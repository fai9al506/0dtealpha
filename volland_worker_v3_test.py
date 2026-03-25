# volland_worker_v3_test.py
"""
TEST worker for SPY DD capture validation.
Exact copy of v2 logic but:
  - No market hours guard (runs anytime for testing)
  - Saves to SEPARATE test tables (volland_v3_test_*)
  - Captures MULTIPLE paradigm responses (SPX + SPY widgets)
  - Heavy diagnostic logging for all API calls
  - No Telegram (console only)
  - Runs ONE capture cycle then exits (safe, no infinite loop)

Usage:
  set DATABASE_URL=...
  set VOLLAND_EMAIL=...
  set VOLLAND_PASSWORD=...
  python volland_worker_v3_test.py

Workspace: hardcoded to new SPY-added workspace for testing.
"""

import os, json, sys, time, traceback
from datetime import datetime, timezone, time as dtime
from urllib.parse import urlparse, parse_qs
import pytz

import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright

# ── Configuration ─────────────────────────────────────────────────────
DB_URL   = os.getenv("DATABASE_URL", "")
EMAIL    = os.getenv("VOLLAND_EMAIL", "")
PASS     = os.getenv("VOLLAND_PASSWORD", "")

# NEW workspace with SPY statistics widget added
WORKSPACE_URL = os.getenv(
    "VOLLAND_WORKSPACE_URL_V3",
    "https://vol.land/app/workspace/69c2d38cce2143e384a8cfa1"
)

WAIT_SEC = float(os.getenv("VOLLAND_WAIT_SEC", "20"))  # extra wait for test

NY = pytz.timezone("US/Eastern")

# ── Database (TEST tables only) ──────────────────────────────────────
def db():
    return psycopg.connect(DB_URL, autocommit=True, row_factory=dict_row)


def ensure_test_tables():
    """Create separate test tables — zero risk to production."""
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS volland_v3_test_snapshots (
          id BIGSERIAL PRIMARY KEY,
          ts TIMESTAMPTZ NOT NULL DEFAULT now(),
          payload JSONB NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS volland_v3_test_exposure_points (
          id BIGSERIAL PRIMARY KEY,
          ts_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
          ticker VARCHAR(20),
          greek VARCHAR(20),
          expiration_option VARCHAR(30),
          strike NUMERIC,
          value NUMERIC,
          current_price NUMERIC,
          last_modified TIMESTAMPTZ,
          source_url TEXT
        );
        """)
    print("[v3-test] Test tables ready.", flush=True)


def save_test_snapshot(payload: dict):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO volland_v3_test_snapshots(payload) VALUES (%s::jsonb)",
            (json.dumps(payload),),
        )


def save_test_exposure_points(points: list, greek: str, ticker: str = "SPX",
                              current_price: float = None, expiration_option: str = None):
    """Insert exposure points into TEST table."""
    if not points:
        return 0
    ts_utc = datetime.now(timezone.utc)
    rows = []
    for pt in points:
        try:
            rows.append((ts_utc, ticker, greek, expiration_option,
                         float(pt.get("x", 0)), float(pt.get("y", 0)), current_price))
        except (ValueError, TypeError):
            pass
    if not rows:
        return 0
    try:
        with db() as conn, conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO volland_v3_test_exposure_points
                (ts_utc, ticker, greek, expiration_option, strike, value, current_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, rows)
        return len(rows)
    except Exception as e:
        print(f"[v3-test] DB error saving {greek}/{ticker} ({len(rows)} pts): {e}", flush=True)
    return 0


# ── Login / session handling (same as v2) ─────────────────────────────
def handle_session_limit_modal(page) -> bool:
    btn = page.locator(
        "button[data-cy='confirmation-modal-confirm-button'], button:has-text('Continue')"
    ).first
    if btn.count() == 0:
        return False
    try:
        btn.wait_for(state="visible", timeout=3000)
        btn.click()
        page.wait_for_timeout(1200)
        print("[v3-test] session modal: clicked Continue", flush=True)
        return True
    except Exception:
        return False


def login_if_needed(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    if "/sign-in" not in page.url and \
       page.locator("input[name='password'], input[type='password']").count() == 0:
        print("[v3-test] Already logged in.", flush=True)
        return

    print("[v3-test] Login required...", flush=True)
    email_box = page.locator(
        "input[data-cy='sign-in-email-input'], input[name='email']"
    ).first
    pwd_box = page.locator(
        "input[data-cy='sign-in-password-input'], input[name='password'], input[type='password']"
    ).first

    email_box.wait_for(state="visible", timeout=90000)
    pwd_box.wait_for(state="visible", timeout=90000)

    email_box.fill(EMAIL)
    pwd_box.fill(PASS)

    page.locator(
        "button:has-text('Log In'), button:has-text('Login'), button[type='submit']"
    ).first.click()
    handle_session_limit_modal(page)

    deadline = time.time() + 90
    while time.time() < deadline:
        handle_session_limit_modal(page)
        if "/sign-in" not in page.url:
            print("[v3-test] Login successful!", flush=True)
            return
        page.wait_for_timeout(500)

    body = ""
    try:
        body = (page.locator("body").inner_text() or "")[:600]
    except Exception:
        pass
    raise RuntimeError(f"Login did not complete. Still on: {page.url}. Body: {body}")


# ── Ticker detection ─────────────────────────────────────────────────
def detect_ticker_from_url(url: str) -> str:
    """Try to detect SPX vs SPY from URL parameters."""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        # Check common param names
        for key in ("ticker", "symbol", "underlying", "asset"):
            if key in params:
                val = params[key][0].upper()
                if "SPY" in val:
                    return "SPY"
                if "SPX" in val:
                    return "SPX"
        # Check path segments
        path_upper = parsed.path.upper()
        if "/SPY/" in path_upper or "/SPY" in path_upper:
            return "SPY"
    except Exception:
        pass
    return None


def detect_ticker_from_data(data: dict) -> str:
    """Detect SPX vs SPY from response data (LIS price range)."""
    # SPX LIS ~ 5000-7000, SPY LIS ~ 500-700
    lis = data.get("lis")
    if isinstance(lis, list) and lis:
        val = lis[0]
        if isinstance(val, (int, float)):
            if val > 1500:
                return "SPX"
            elif val > 100:
                return "SPY"
    # Check aggregatedDeltaDecay magnitude (SPX ~ billions, SPY ~ millions)
    dd = data.get("aggregatedDeltaDecay")
    if isinstance(dd, (int, float)):
        if abs(dd) > 500_000_000:  # > 500M → likely SPX
            return "SPX"
        elif abs(dd) > 0:
            return "SPY"
    return "UNKNOWN"


# ── Statistics formatting (same as v2) ────────────────────────────────
def format_statistics(paradigm_data: dict, spot_vol_data: dict, label: str = "") -> dict:
    stats = {}
    if paradigm_data:
        stats["paradigm"] = paradigm_data.get("paradigm")
        t = paradigm_data.get("target")
        stats["target"] = f"${t:,.0f}" if t is not None else None
        lis = paradigm_data.get("lis")
        if isinstance(lis, list) and len(lis) >= 2:
            stats["lines_in_sand"] = f"${lis[0]:,} - ${lis[-1]:,}"
        elif isinstance(lis, list) and len(lis) == 1:
            stats["lines_in_sand"] = f"${lis[0]:,}"
        elif isinstance(lis, (int, float)):
            stats["lines_in_sand"] = f"${lis:,}"
        else:
            stats["lines_in_sand"] = None
        dd = paradigm_data.get("aggregatedDeltaDecay")
        stats["delta_decay_hedging"] = f"${dd:,}" if dd is not None else None
        vol = paradigm_data.get("totalZeroDteOptionVolume")
        stats["opt_volume"] = f"{vol:,}" if vol is not None else None
        stats["aggregatedCharm"] = paradigm_data.get("aggregatedCharm")
    if spot_vol_data:
        stats["spot_vol_beta"] = spot_vol_data
    return stats


# ── Main test run ─────────────────────────────────────────────────────
def run():
    if not DB_URL or not EMAIL or not PASS:
        raise RuntimeError(
            "Missing env vars: DATABASE_URL / VOLLAND_EMAIL / VOLLAND_PASSWORD"
        )

    ensure_test_tables()
    print(f"[v3-test] Workspace: {WORKSPACE_URL}", flush=True)
    print(f"[v3-test] Wait: {WAIT_SEC}s", flush=True)
    print(f"[v3-test] Time now (ET): {datetime.now(NY).strftime('%H:%M:%S')}", flush=True)
    print("=" * 70, flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            service_workers="block",
        )
        page = context.new_page()
        page.set_default_timeout(90000)

        # ── Capture state ──
        # KEY CHANGE: paradigms is a LIST (not single dict) to capture SPX + SPY
        cycle = {
            "exposures": [],
            "paradigms": [],          # list of {ticker, data, url}
            "spot_vols": [],          # list of {ticker, data, url}
            "zero_captures": 0,
        }

        # Track ALL API requests for diagnostics
        all_api_requests = []
        all_api_responses = []

        def handle_exposure(route, request):
            """Route handler: intercept exposure POST (same as v2)."""
            post_data = request.post_data
            greek, exp_option = "unknown", None
            ticker_hint = None
            if post_data:
                try:
                    pj = json.loads(post_data)
                    greek = pj.get("greek", "unknown")
                    exp_option = pj.get("expirations", {}).get("option")
                    # Check if POST body reveals ticker
                    ticker_hint = pj.get("ticker") or pj.get("symbol")
                    print(f"[v3-diag] exposure POST body keys: {list(pj.keys())}", flush=True)
                    if ticker_hint:
                        print(f"[v3-diag] exposure POST ticker/symbol: {ticker_hint}", flush=True)
                except Exception:
                    pass
            try:
                response = route.fetch()
                body = response.text()
                try:
                    data = json.loads(body)
                    cycle["exposures"].append({
                        "greek": greek,
                        "expiration_option": exp_option,
                        "items": data.get("items", []),
                        "current_price": data.get("currentPrice"),
                        "expirations": data.get("expirations", []),
                        "last_modified": data.get("lastModified"),
                        "ticker_hint": ticker_hint,
                    })
                    n = len(data.get("items", []))
                    price = data.get("currentPrice")
                    print(
                        f"[v3-capture] exposure: {greek}/{exp_option} "
                        f"({n} pts, price={price}, ticker_hint={ticker_hint})",
                        flush=True,
                    )
                except json.JSONDecodeError:
                    pass
                route.fulfill(response=response)
            except Exception as e:
                print(f"[v3-test] exposure route error: {e}", flush=True)
                try:
                    route.continue_()
                except Exception:
                    pass

        def handle_response(response):
            """Response handler: capture ALL paradigm and SVB responses."""
            url = response.url
            if "/data/exposure" in url:
                return  # Already handled by route handler

            # Log ALL API responses for diagnostics
            if ("/data/" in url or "/api/" in url) and response.status == 200:
                all_api_responses.append({
                    "url": url[:200],
                    "status": response.status,
                })

            try:
                # PARADIGM — capture ALL (SPX widget + SPY widget fire separate calls)
                if "/data/paradigms" in url and response.status == 200:
                    data = json.loads(response.text())

                    # Detect ticker
                    ticker_url = detect_ticker_from_url(url)
                    ticker_data = detect_ticker_from_data(data)
                    ticker = ticker_url or ticker_data or "UNKNOWN"

                    cycle["paradigms"].append({
                        "ticker": ticker,
                        "data": data,
                        "url": url,
                        "ticker_from_url": ticker_url,
                        "ticker_from_data": ticker_data,
                    })

                    print(
                        f"\n{'='*50}\n"
                        f"[v3-capture] PARADIGM #{len(cycle['paradigms'])}\n"
                        f"  ticker: {ticker} (url={ticker_url}, data={ticker_data})\n"
                        f"  URL: {url[:150]}\n"
                        f"  paradigm: {data.get('paradigm')}\n"
                        f"  lis: {data.get('lis')}\n"
                        f"  target: {data.get('target')}\n"
                        f"  DD hedging: {data.get('aggregatedDeltaDecay')}\n"
                        f"  charm: {data.get('aggregatedCharm')}\n"
                        f"  volume: {data.get('totalZeroDteOptionVolume')}\n"
                        f"  ALL KEYS: {list(data.keys())}\n"
                        f"{'='*50}",
                        flush=True,
                    )

                # SPOT-VOL-BETA — capture all
                elif "/data/volhacks/spot-vol-beta" in url and response.status == 200:
                    data = json.loads(response.text())
                    ticker_url = detect_ticker_from_url(url)

                    cycle["spot_vols"].append({
                        "ticker": ticker_url or "UNKNOWN",
                        "data": data,
                        "url": url,
                    })
                    print(
                        f"[v3-capture] spot-vol-beta: ticker={ticker_url} "
                        f"corr={data.get('correlation')} url={url[:100]}",
                        flush=True,
                    )

            except Exception as e:
                print(f"[v3-test] response handler error: {e}", flush=True)

        def handle_request_diag(request):
            """Log ALL API requests."""
            url = request.url
            if "/api/" in url or "/data/" in url:
                all_api_requests.append(f"{request.method} {url}")
                print(f"[v3-diag] >> {request.method} {url[:150]}", flush=True)

        # Register handlers
        page.route("**/api/v1/data/exposure", handle_exposure)
        page.on("response", handle_response)
        page.on("request", handle_request_diag)

        # Login
        print("[v3-test] Logging in...", flush=True)
        login_if_needed(page, WORKSPACE_URL)

        # ── CAPTURE ──────────────────────────────────────────────────
        print(f"\n[v3-test] Navigating to workspace...", flush=True)
        cycle["exposures"] = []
        cycle["paradigms"] = []
        cycle["spot_vols"] = []
        cycle["zero_captures"] = 0
        all_api_requests.clear()
        all_api_responses.clear()

        page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)

        if "/sign-in" in page.url:
            raise RuntimeError("Session expired after login!")

        try:
            page.wait_for_load_state("load", timeout=30000)
        except Exception:
            pass

        handle_session_limit_modal(page)

        # Wait longer for all widgets to fire API calls
        print(f"[v3-test] Waiting {WAIT_SEC}s for all widgets to load...", flush=True)
        page.wait_for_timeout(int(WAIT_SEC * 1000))

        # ── SECOND NAVIGATION to trigger refresh ──
        # Sometimes widgets don't fire on first load outside market hours.
        # Do a second goto to ensure both SPX and SPY widgets fire.
        print(f"\n[v3-test] Second navigation (trigger widget refresh)...", flush=True)
        page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)
        try:
            page.wait_for_load_state("load", timeout=30000)
        except Exception:
            pass
        page.wait_for_timeout(int(WAIT_SEC * 1000))

        # ── RESULTS ──────────────────────────────────────────────────
        print("\n" + "=" * 70, flush=True)
        print("[v3-test] CAPTURE RESULTS", flush=True)
        print("=" * 70, flush=True)

        # Exposures
        print(f"\nExposures captured: {len(cycle['exposures'])}", flush=True)
        seen_greeks = {}
        for exp in cycle["exposures"]:
            key = (exp["greek"], exp["expiration_option"])
            seen_greeks[key] = exp
        for (g, eo), exp in seen_greeks.items():
            print(
                f"  {g}/{eo}: {len(exp['items'])} pts, "
                f"price={exp['current_price']}, hint={exp.get('ticker_hint')}",
                flush=True,
            )

        # Paradigms (the key thing we're testing!)
        print(f"\nParadigm responses: {len(cycle['paradigms'])}", flush=True)
        spx_paradigm = None
        spy_paradigm = None
        for i, p_entry in enumerate(cycle["paradigms"]):
            ticker = p_entry["ticker"]
            data = p_entry["data"]
            print(f"\n  Paradigm #{i+1} — {ticker}:", flush=True)
            print(f"    URL: {p_entry['url'][:150]}", flush=True)
            print(f"    paradigm: {data.get('paradigm')}", flush=True)
            print(f"    lis: {data.get('lis')}", flush=True)
            print(f"    DD hedging: {data.get('aggregatedDeltaDecay')}", flush=True)
            print(f"    charm: {data.get('aggregatedCharm')}", flush=True)
            print(f"    volume: {data.get('totalZeroDteOptionVolume')}", flush=True)
            print(f"    target: {data.get('target')}", flush=True)
            print(f"    keys: {list(data.keys())}", flush=True)

            if ticker == "SPX":
                spx_paradigm = data
            elif ticker == "SPY":
                spy_paradigm = data

        # Spot-vol-beta
        print(f"\nSpot-vol-beta responses: {len(cycle['spot_vols'])}", flush=True)
        for sv in cycle["spot_vols"]:
            print(f"  ticker={sv['ticker']}: corr={sv['data'].get('correlation')}", flush=True)

        # All API requests summary
        print(f"\nTotal API requests: {len(all_api_requests)}", flush=True)
        unique_endpoints = {}
        for req in all_api_requests:
            # Extract method + path (no query params)
            parts = req.split(" ", 1)
            if len(parts) == 2:
                method = parts[0]
                try:
                    path = urlparse(parts[1]).path
                except Exception:
                    path = parts[1][:80]
                key = f"{method} {path}"
                unique_endpoints[key] = unique_endpoints.get(key, 0) + 1
        print("\nUnique API endpoints:", flush=True)
        for endpoint, count in sorted(unique_endpoints.items()):
            print(f"  {endpoint} (×{count})", flush=True)

        # ── SAVE TO TEST TABLES ──────────────────────────────────────
        print("\n" + "=" * 70, flush=True)
        print("[v3-test] SAVING TO TEST TABLES", flush=True)
        print("=" * 70, flush=True)

        # Deduplicate exposures (keep last per greek/expiration combo)
        deduped = {}
        for exp in cycle["exposures"]:
            key = (exp["greek"], exp["expiration_option"])
            deduped[key] = exp
        exposures = list(deduped.values())

        # Save exposure points
        total_points = 0
        for exp in exposures:
            greek = exp["greek"]
            exp_option = exp["expiration_option"]
            items = exp["items"]
            cur_price = exp["current_price"]
            db_exp_option = None if (greek == "charm" and exp_option == "TODAY") else exp_option
            count = save_test_exposure_points(
                items, greek=greek, ticker="SPX",
                current_price=cur_price, expiration_option=db_exp_option,
            )
            total_points += count
            print(f"  saved {greek}/{exp_option}: {count} pts", flush=True)

        # Build stats for SPX (backward compatible with v2)
        spx_stats = format_statistics(spx_paradigm, cycle["spot_vols"][0]["data"] if cycle["spot_vols"] else None)

        # Build SPY stats
        spy_stats = format_statistics(spy_paradigm, None, label="SPY") if spy_paradigm else {}

        # Snapshot payload — includes BOTH SPX and SPY
        payload = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "page_url": WORKSPACE_URL,
            "test_mode": True,
            "statistics": spx_stats,          # SPX (same as v2 format)
            "spy_statistics": spy_stats,       # SPY (new!)
            "spy_paradigm_raw": spy_paradigm,  # raw SPY paradigm response
            "exposure_points_saved": total_points,
            "current_price": exposures[0]["current_price"] if exposures else None,
            "paradigm_responses": len(cycle["paradigms"]),
            "captures": {
                "exposure_count": len(exposures),
                "paradigm_count": len(cycle["paradigms"]),
                "spot_vol_count": len(cycle["spot_vols"]),
                "api_requests": len(all_api_requests),
                "exposures_summary": [
                    {"greek": e["greek"], "option": e["expiration_option"], "items": len(e["items"])}
                    for e in exposures
                ],
                "paradigm_tickers": [p["ticker"] for p in cycle["paradigms"]],
            },
        }

        save_test_snapshot(payload)
        print(f"\n[v3-test] Snapshot saved to volland_v3_test_snapshots", flush=True)

        # ── FINAL SUMMARY ────────────────────────────────────────────
        print("\n" + "=" * 70, flush=True)
        print("[v3-test] SUMMARY", flush=True)
        print("=" * 70, flush=True)
        print(f"  Exposures: {len(exposures)} types, {total_points} total points", flush=True)
        print(f"  Paradigm responses: {len(cycle['paradigms'])}", flush=True)
        for p_entry in cycle["paradigms"]:
            t = p_entry["ticker"]
            d = p_entry["data"]
            dd = d.get("aggregatedDeltaDecay")
            dd_str = f"${dd:,}" if dd is not None else "None"
            print(f"    {t}: paradigm={d.get('paradigm')} DD={dd_str} lis={d.get('lis')}", flush=True)

        if spx_paradigm and spy_paradigm:
            spx_dd = spx_paradigm.get("aggregatedDeltaDecay") or 0
            spy_dd = spy_paradigm.get("aggregatedDeltaDecay") or 0
            combined = spx_dd + spy_dd
            print(f"\n  >>> COMBINED DD: SPX({spx_dd:,}) + SPY({spy_dd:,}) = {combined:,}", flush=True)
            print("  >>> SPY capture WORKING!", flush=True)
        elif spy_paradigm:
            print("\n  >>> SPY paradigm captured but no SPX (expected outside market hours)", flush=True)
        elif spx_paradigm:
            print("\n  >>> SPX paradigm captured but NO SPY — check widget setup!", flush=True)
        else:
            print("\n  >>> No paradigm responses — normal outside market hours if widgets are empty.", flush=True)
            print("  >>> The API endpoints and capture mechanism are verified by the request log above.", flush=True)

        print(f"\n  Spot-vol-beta: {len(cycle['spot_vols'])} responses", flush=True)
        print(f"  Total API requests seen: {len(all_api_requests)}", flush=True)

        # Check: did we see paradigm URLs with different tickers?
        paradigm_urls = [r for r in all_api_requests if "paradigm" in r.lower()]
        if paradigm_urls:
            print(f"\n  Paradigm-related URLs ({len(paradigm_urls)}):", flush=True)
            for pu in paradigm_urls:
                print(f"    {pu[:150]}", flush=True)

        print("\n[v3-test] Done. Check volland_v3_test_snapshots for saved data.", flush=True)

        browser.close()


if __name__ == "__main__":
    run()
