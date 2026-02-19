# volland_worker_v2.py
"""
V2 Volland worker: single all-in-one workspace, route-based data capture.
Replaces V1's two-workspace approach (charm + statistics pages).
Same DB tables, same data format — drop-in replacement.

Key differences from V1:
  - 1 vol.land session instead of 2 (avoids 3-device limit)
  - Definitive greek identification from POST body (not heuristic)
  - Captures all 10 exposure types (charm, vanna x4, gamma x4, deltaDecay)
  - Statistics from API response (paradigm endpoint) instead of DOM scraping
  - Spot-vol-beta and aggregatedCharm data (new)
  - Synced to Volland's 120s refresh cycle (no duplicate data)
"""

import os, json, sys, time, traceback
from datetime import datetime, timezone, time as dtime
import pytz
import requests as _requests

import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright

# ── Configuration ─────────────────────────────────────────────────────
DB_URL   = os.getenv("DATABASE_URL", "")
EMAIL    = os.getenv("VOLLAND_EMAIL", "")
PASS     = os.getenv("VOLLAND_PASSWORD", "")
# Falls back to VOLLAND_URL for gradual migration
WORKSPACE_URL = os.getenv("VOLLAND_WORKSPACE_URL", "") or os.getenv("VOLLAND_URL", "")

PULL_EVERY = int(os.getenv("VOLLAND_PULL_EVERY_SEC", "120"))
WAIT_SEC   = float(os.getenv("VOLLAND_WAIT_SEC", "15"))
SYNC_POLL_SEC = int(os.getenv("VOLLAND_SYNC_POLL_SEC", "20"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
ZERO_POINTS_THRESHOLD = 3  # consecutive zero-point cycles before alerting
AUTO_RESTART_THRESHOLD = 5  # consecutive zero-point cycles before browser restart

NY = pytz.timezone("US/Eastern")

def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        resp = _requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            print(f"[volland-telegram] sent: {message[:60]}...", flush=True)
        return resp.status_code == 200
    except Exception as e:
        print(f"[volland-telegram] error: {e}", flush=True)
        return False


def market_open_now() -> bool:
    """Worker operating window: 9:20-16:10 ET, Mon-Fri only."""
    t = datetime.now(NY)
    if t.weekday() >= 5:  # Sat=5, Sun=6
        return False
    return dtime(9, 20) <= t.time() <= dtime(16, 10)

def is_market_hours() -> bool:
    """Actual market hours: 9:30-16:00 ET, Mon-Fri only. Used for alert logic."""
    t = datetime.now(NY)
    if t.weekday() >= 5:
        return False
    return dtime(9, 30) <= t.time() <= dtime(16, 0)


# ── Database ──────────────────────────────────────────────────────────
def db():
    return psycopg.connect(DB_URL, autocommit=True, row_factory=dict_row)


def ensure_tables():
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS volland_snapshots (
          id BIGSERIAL PRIMARY KEY,
          ts TIMESTAMPTZ NOT NULL DEFAULT now(),
          payload JSONB NOT NULL
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_volland_snapshots_ts ON volland_snapshots(ts DESC);")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS volland_exposure_points (
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_volland_exposure_points_ts ON volland_exposure_points(ts_utc DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_volland_exposure_points_greek ON volland_exposure_points(greek);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_volland_ep_greek_ts ON volland_exposure_points(greek, ts_utc DESC);")


def save_snapshot(payload: dict):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO volland_snapshots(payload) VALUES (%s::jsonb)",
            (json.dumps(payload),),
        )


def save_exposure_points(points: list, greek: str, ticker: str = "SPX",
                         current_price: float = None, expiration_option: str = None):
    """Insert exposure points into volland_exposure_points table."""
    if not points:
        return 0
    ts_utc = datetime.now(timezone.utc)
    with db() as conn, conn.cursor() as cur:
        count = 0
        for pt in points:
            try:
                strike = float(pt.get("x", 0))
                value = float(pt.get("y", 0))
                cur.execute("""
                    INSERT INTO volland_exposure_points
                    (ts_utc, ticker, greek, expiration_option, strike, value, current_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (ts_utc, ticker, greek, expiration_option, strike, value, current_price))
                count += 1
            except Exception as e:
                print(f"[exposure] Failed to insert point: {e}", flush=True)
        return count


# ── Login / session handling ──────────────────────────────────────────
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
        print("[login] session modal: clicked Continue", flush=True)
        return True
    except Exception:
        return False


def login_if_needed(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    if "/sign-in" not in page.url and \
       page.locator("input[name='password'], input[type='password']").count() == 0:
        return

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
            return
        page.wait_for_timeout(500)

    body = ""
    try:
        body = (page.locator("body").inner_text() or "")[:600]
    except Exception:
        pass
    raise RuntimeError(f"Login did not complete. Still on: {page.url}. Body: {body}")


# ── Statistics formatting ─────────────────────────────────────────────
def format_statistics(paradigm_data: dict, spot_vol_data: dict) -> dict:
    """
    Format paradigm API data into the same statistics dict V1 produces.
    Backward compatible with main.py's db_volland_stats() parsing.

    V1 format examples (from DOM scraping):
      paradigm:           "BofA-LIS"
      lines_in_sand:      "$6,923 - $6,943"
      delta_decay_hedging: "$7,298,110,681"
      opt_volume:          "1,365,839"

    V2 reads the same data from the paradigm API response:
      {"paradigm":"BofA-LIS","lis":[6923,6943],"target":null,
       "totalZeroDteOptionVolume":1365839,"aggregatedCharm":-109934477,
       "aggregatedDeltaDecay":7298110681}
    """
    stats = {}

    if paradigm_data:
        stats["paradigm"] = paradigm_data.get("paradigm")

        # Target: null or number → "$X,XXX" or None
        t = paradigm_data.get("target")
        stats["target"] = f"${t:,.0f}" if t is not None else None

        # LIS: [6923, 6943] → "$6,923 - $6,943" | [6859] → "$6,859" | 6859 → "$6,859"
        # Paradigm types: BofA/SIDIAL → 2 LIS no target, GEX → LIS + target, AG/Anti-GEX → 1 LIS + target
        lis = paradigm_data.get("lis")
        if isinstance(lis, list) and len(lis) >= 2:
            stats["lines_in_sand"] = f"${lis[0]:,} - ${lis[-1]:,}"
        elif isinstance(lis, list) and len(lis) == 1:
            stats["lines_in_sand"] = f"${lis[0]:,}"
        elif isinstance(lis, (int, float)):
            stats["lines_in_sand"] = f"${lis:,}"
        else:
            stats["lines_in_sand"] = None

        # Delta decay hedging: 7298110681 → "$7,298,110,681"
        dd = paradigm_data.get("aggregatedDeltaDecay")
        stats["delta_decay_hedging"] = f"${dd:,}" if dd is not None else None

        # Volume: 1365839 → "1,365,839"
        vol = paradigm_data.get("totalZeroDteOptionVolume")
        stats["opt_volume"] = f"{vol:,}" if vol is not None else None

        # Aggregated charm — raw number for setup_detector (BofA Scalp)
        stats["aggregatedCharm"] = paradigm_data.get("aggregatedCharm")

    if spot_vol_data:
        stats["spot_vol_beta"] = spot_vol_data

    return stats


# ── Main loop ─────────────────────────────────────────────────────────
def run():
    if not DB_URL or not EMAIL or not PASS or not WORKSPACE_URL:
        raise RuntimeError(
            "Missing env vars: DATABASE_URL / VOLLAND_EMAIL / VOLLAND_PASSWORD / "
            "VOLLAND_WORKSPACE_URL (or VOLLAND_URL)"
        )

    ensure_tables()
    print(f"[volland-v2] Starting. Workspace: {WORKSPACE_URL}", flush=True)
    print(f"[volland-v2] Capture every {PULL_EVERY}s, wait {WAIT_SEC}s, sync poll {SYNC_POLL_SEC}s", flush=True)

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

        # Mutable capture state — reset each cycle, shared via closure
        cycle = {"exposures": [], "paradigm": None, "spot_vol": None, "zero_captures": 0}

        def handle_exposure(route, request):
            """Route handler: intercept exposure POST, fetch response, store both."""
            post_data = request.post_data
            greek, exp_option = "unknown", None
            if post_data:
                try:
                    pj = json.loads(post_data)
                    greek = pj.get("greek", "unknown")
                    exp_option = pj.get("expirations", {}).get("option")
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
                    })
                    n = len(data.get("items", []))
                    if n > 0:
                        print(f"[capture] exposure: {greek}/{exp_option} ({n} pts)", flush=True)
                    else:
                        cycle["zero_captures"] = cycle.get("zero_captures", 0) + 1
                except json.JSONDecodeError:
                    pass
                route.fulfill(response=response)
            except Exception as e:
                print(f"[capture] exposure route error: {e}", flush=True)
                try:
                    route.continue_()
                except Exception:
                    pass

        def handle_response(response):
            """Response handler: capture paradigm and spot-vol-beta GET endpoints."""
            url = response.url
            if "/data/exposure" in url:
                return  # Already handled by route handler
            try:
                if "/data/paradigms/0dte" in url and response.status == 200:
                    cycle["paradigm"] = json.loads(response.text())
                    tgt = cycle['paradigm'].get('target')
                    print(
                        f"[capture] paradigm: {cycle['paradigm'].get('paradigm')} "
                        f"lis={cycle['paradigm'].get('lis')}"
                        f"{f' target={tgt}' if tgt is not None else ''}",
                        flush=True,
                    )
                elif "/data/volhacks/spot-vol-beta" in url and response.status == 200:
                    cycle["spot_vol"] = json.loads(response.text())
                    print(
                        f"[capture] spot-vol-beta: corr={cycle['spot_vol'].get('correlation')}",
                        flush=True,
                    )
            except Exception:
                pass

        # Diagnostic: log API requests to see what the page is actually calling
        _diag_cycle_count = {"n": 0}

        def handle_request_diag(request):
            """Log API requests for diagnostics (first 3 cycles only)."""
            if _diag_cycle_count["n"] > 3:
                return
            url = request.url
            if "/api/" in url or "/data/" in url:
                print(f"[diag] {request.method} {url[:120]}", flush=True)

        def setup_handlers(pg):
            """Register route and response handlers on a page."""
            pg.route("**/api/v1/data/exposure", handle_exposure)
            pg.on("response", handle_response)
            pg.on("request", handle_request_diag)

        def do_full_capture():
            """Navigate to workspace, wait, return deduped (exposures, paradigm, spot_vol)."""
            cycle["exposures"] = []
            cycle["paradigm"] = None
            cycle["spot_vol"] = None
            cycle["zero_captures"] = 0
            _diag_cycle_count["n"] += 1

            page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)

            if "/sign-in" in page.url:
                login_if_needed(page, WORKSPACE_URL)
                cycle["exposures"] = []
                cycle["paradigm"] = None
                cycle["spot_vol"] = None
                cycle["zero_captures"] = 0
                page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)

            # Wait for full page load (scripts, stylesheets) before data wait
            try:
                page.wait_for_load_state("load", timeout=30000)
            except Exception:
                pass  # proceed even if load state times out

            page.wait_for_timeout(int(WAIT_SEC * 1000))
            handle_session_limit_modal(page)

            # Diagnostic: log page state if no data captured
            if not cycle["exposures"] and _diag_cycle_count["n"] <= 3:
                try:
                    title = page.title()
                    url = page.url
                    print(f"[diag] page title={title!r} url={url[:100]}", flush=True)
                except Exception:
                    pass

            # Deduplicate: widgets auto-refresh during wait, keep last per combo
            seen = {}
            for exp in cycle["exposures"]:
                key = (exp["greek"], exp["expiration_option"])
                seen[key] = exp

            zc = cycle.get("zero_captures", 0)
            if zc > 0:
                print(f"[capture] {zc} exposure API calls returned 0 pts", flush=True)

            return list(seen.values()), cycle["paradigm"], cycle["spot_vol"]

        def save_cycle(exposures, paradigm, spot_vol):
            """Format, save to DB, log. Returns (stats, total_points, zero_exposures)."""
            stats = format_statistics(paradigm, spot_vol)

            total_points = 0
            zero_exposures = []
            for exp in exposures:
                greek      = exp["greek"]
                exp_option = exp["expiration_option"]
                items      = exp["items"]
                cur_price  = exp["current_price"]

                db_exp_option = (
                    None if (greek == "charm" and exp_option == "TODAY")
                    else exp_option
                )

                count = save_exposure_points(
                    items, greek=greek, ticker="SPX",
                    current_price=cur_price, expiration_option=db_exp_option,
                )
                total_points += count
                if count == 0:
                    zero_exposures.append(f"{greek}/{exp_option}")

            payload = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "page_url": WORKSPACE_URL,
                "statistics": stats,
                "exposure_points_saved": total_points,
                "current_price": (
                    exposures[0]["current_price"] if exposures else None
                ),
                "captures": {
                    "exposure_count": len(exposures),
                    "exposures_summary": [
                        {
                            "greek": e["greek"],
                            "option": e["expiration_option"],
                            "items": len(e["items"]),
                        }
                        for e in exposures
                    ],
                },
            }

            save_snapshot(payload)

            print(
                f"[volland-v2] saved {payload['ts_utc']} "
                f"exposures={len(exposures)} points={total_points} "
                f"paradigm={stats.get('paradigm', 'N/A')} "
                f"lis={stats.get('lines_in_sand', 'N/A')} "
                f"charm={stats.get('aggregatedCharm', 'N/A')}",
                flush=True,
            )
            return stats, total_points, zero_exposures

        # Register handlers once (persist across navigations)
        setup_handlers(page)

        # Login once
        login_if_needed(page, WORKSPACE_URL)

        # Track Volland's lastModified to detect refreshes
        last_known_modified = ""
        consecutive_zero_pts = 0
        zero_pts_alerted = False
        _was_in_market = False  # Track if we've been active this session

        def get_exposure_lastmodified():
            """Get lastModified from the most recent exposure capture."""
            for exp in reversed(cycle["exposures"]):
                lm = exp.get("last_modified")
                if lm:
                    return lm
            return ""

        while True:
            if not market_open_now():
                if _was_in_market:
                    # Market closed after we were active — exit for fresh start
                    # tomorrow. Railway auto-restarts; new process = fresh
                    # Playwright context = no overnight degradation.
                    print("[volland-v2] Market closed. Exiting for fresh start tomorrow.", flush=True)
                    sys.exit(0)
                # Pre-market: just wait
                last_known_modified = ""
                time.sleep(30)
                continue
            _was_in_market = True

            try:
                # ══════════════════════════════════════════════════════
                # SYNC PHASE: wait for Volland to refresh before first
                # capture of the day. We sit on the workspace page and
                # let widgets auto-refresh (they have their own timer).
                # The route handler captures each refresh — we just
                # watch lastModified for a change.
                # ══════════════════════════════════════════════════════
                if not last_known_modified:
                    # Reload workspace so widgets are fresh (page goes stale overnight)
                    print("[sync] Refreshing workspace page...", flush=True)
                    cycle["exposures"] = []
                    cycle["paradigm"] = None
                    cycle["spot_vol"] = None
                    cycle["zero_captures"] = 0
                    _diag_cycle_count["n"] += 1
                    page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)
                    if "/sign-in" in page.url:
                        login_if_needed(page, WORKSPACE_URL)
                        cycle["exposures"] = []
                        cycle["paradigm"] = None
                        cycle["spot_vol"] = None
                        cycle["zero_captures"] = 0
                        page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)
                    try:
                        page.wait_for_load_state("load", timeout=30000)
                    except Exception:
                        pass
                    page.wait_for_timeout(int(WAIT_SEC * 1000))

                    baseline = get_exposure_lastmodified()
                    print(
                        f"[sync] Waiting for Volland refresh... "
                        f"baseline lastModified={baseline!r}",
                        flush=True,
                    )
                    # Diagnostic: if no data captured during page load, log state
                    if not baseline and _diag_cycle_count["n"] <= 3:
                        try:
                            title = page.title()
                            url = page.url
                            print(f"[diag-sync] page title={title!r} url={url[:100]}", flush=True)
                        except Exception:
                            pass

                    # Wait for widgets to auto-refresh on the page.
                    # page.wait_for_timeout keeps the event loop alive
                    # so route/response handlers fire on widget refreshes.
                    sync_deadline = time.time() + 120  # 2 min max
                    synced = False
                    while market_open_now():
                        if time.time() >= sync_deadline:
                            print("[sync] Timed out after 2m, proceeding to capture", flush=True)
                            last_known_modified = "timeout"
                            synced = True
                            break
                        page.wait_for_timeout(SYNC_POLL_SEC * 1000)
                        current = get_exposure_lastmodified()
                        if current and current != baseline:
                            print(
                                f"[sync] Volland refreshed! "
                                f"lastModified={current!r} (was {baseline!r})",
                                flush=True,
                            )
                            last_known_modified = current
                            synced = True
                            break
                        # Also break if baseline was empty and we now have data
                        if current and not baseline:
                            print(
                                f"[sync] First data available: "
                                f"lastModified={current!r}",
                                flush=True,
                            )
                            last_known_modified = current
                            synced = True
                            break
                    if not synced:
                        # Market closed while waiting
                        continue

                # ══════════════════════════════════════════════════════
                # CAPTURE PHASE: full workspace load + save
                # ══════════════════════════════════════════════════════
                print("[volland-v2] Fetching workspace...", flush=True)
                exposures, paradigm, spot_vol = do_full_capture()

                # Update lastModified from captured data
                for exp in reversed(exposures):
                    lm = exp.get("last_modified")
                    if lm:
                        last_known_modified = lm
                        break

                _stats, _total_pts, _zero_exps = save_cycle(exposures, paradigm, spot_vol)

                # Track 0-point cycles during actual market hours
                if _total_pts == 0 and is_market_hours():
                    consecutive_zero_pts += 1
                    detail = f"exposures={len(exposures)}"
                    if _zero_exps:
                        detail += f" zero=[{', '.join(_zero_exps)}]"
                    print(f"[volland-v2] WARNING: 0 total pts ({consecutive_zero_pts}/{ZERO_POINTS_THRESHOLD}): {detail}", flush=True)
                    if consecutive_zero_pts >= ZERO_POINTS_THRESHOLD and not zero_pts_alerted:
                        zero_pts_alerted = True
                        send_telegram(
                            "⚠️ <b>Volland 0-Point Exposures</b>\n\n"
                            f"{consecutive_zero_pts} consecutive cycles with 0 points.\n"
                            f"Exposures captured: {len(exposures)}\n\n"
                            "Auto-restart will trigger after "
                            f"{AUTO_RESTART_THRESHOLD} cycles."
                        )
                    # Process exit: in-process browser restart doesn't fix the
                    # root cause (degraded Playwright context after extended
                    # uptime). Let Railway restart the entire process — fresh
                    # Python process = fresh Playwright context = working handlers.
                    if consecutive_zero_pts >= AUTO_RESTART_THRESHOLD:
                        print(
                            f"[volland-v2] PROCESS EXIT: {consecutive_zero_pts} "
                            f"consecutive zero-point cycles. Railway will restart fresh.",
                            flush=True,
                        )
                        send_telegram(
                            "🔄 <b>Volland Process Restart</b>\n\n"
                            f"{consecutive_zero_pts} consecutive cycles with 0 points.\n"
                            "Exiting for clean Railway restart (fresh Playwright context)."
                        )
                        time.sleep(5)
                        sys.exit(1)
                elif _total_pts > 0:
                    if consecutive_zero_pts > 0:
                        print(f"[volland-v2] recovered after {consecutive_zero_pts} zero-point cycles", flush=True)
                    consecutive_zero_pts = 0
                    zero_pts_alerted = False

            except Exception as e:
                err_payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "page_url": getattr(page, "url", ""),
                    "error": str(e),
                    "trace": traceback.format_exc()[-4000:],
                }
                try:
                    save_snapshot({"error_event": err_payload})
                except Exception:
                    pass
                print(f"[volland-v2] error: {e}", flush=True)
                traceback.print_exc()

                # Session expiry recovery: try re-login if we're on sign-in page
                try:
                    if "/sign-in" in (page.url or ""):
                        print("[volland-v2] Session expired — attempting re-login...", flush=True)
                        login_if_needed(page, WORKSPACE_URL)
                        last_known_modified = ""  # force re-sync
                        print("[volland-v2] Re-login successful", flush=True)
                except Exception as login_err:
                    print(f"[volland-v2] Re-login failed: {login_err}", flush=True)
                    if is_market_hours():
                        send_telegram(
                            "🔑 <b>Volland Login Failed</b>\n\n"
                            f"Error: <code>{str(login_err)[:200]}</code>\n"
                            "Session may have expired. Check credentials."
                        )

                # Browser crash recovery — exit for clean restart
                if "closed" in str(e).lower() or "Target" in str(e):
                    print("[volland-v2] Browser/page crashed — exiting for clean restart...", flush=True)
                    send_telegram(
                        "💥 <b>Volland Browser Crash</b>\n\n"
                        f"Error: <code>{str(e)[:200]}</code>\n"
                        "Exiting for clean Railway restart."
                    )
                    time.sleep(5)
                    sys.exit(1)

            time.sleep(PULL_EVERY)


if __name__ == "__main__":
    run()
