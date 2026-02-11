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
"""

import os, json, time, traceback
from datetime import datetime, timezone, time as dtime
import pytz

import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright

# ── Configuration ─────────────────────────────────────────────────────
DB_URL   = os.getenv("DATABASE_URL", "")
EMAIL    = os.getenv("VOLLAND_EMAIL", "")
PASS     = os.getenv("VOLLAND_PASSWORD", "")
# Falls back to VOLLAND_URL for gradual migration
WORKSPACE_URL = os.getenv("VOLLAND_WORKSPACE_URL", "") or os.getenv("VOLLAND_URL", "")

PULL_EVERY = int(os.getenv("VOLLAND_PULL_EVERY_SEC", "60"))
WAIT_SEC   = float(os.getenv("VOLLAND_WAIT_SEC", "15"))

NY = pytz.timezone("US/Eastern")


def market_open_now() -> bool:
    t = datetime.now(NY)
    return dtime(9, 20) <= t.time() <= dtime(16, 10)


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

        # LIS: [6923, 6943] → "$6,923 - $6,943"
        lis = paradigm_data.get("lis")
        if isinstance(lis, list) and len(lis) == 2:
            stats["lines_in_sand"] = f"${lis[0]:,} - ${lis[1]:,}"
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
    print(f"[volland-v2] Pull every {PULL_EVERY}s, wait {WAIT_SEC}s per cycle", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(90000)

        # Mutable capture state — reset each cycle, shared via closure
        cycle = {"exposures": [], "paradigm": None, "spot_vol": None}

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
                    print(f"[capture] exposure: {greek}/{exp_option} ({n} pts)", flush=True)
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
                    print(
                        f"[capture] paradigm: {cycle['paradigm'].get('paradigm')} "
                        f"lis={cycle['paradigm'].get('lis')}",
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

        def setup_handlers(pg):
            """Register route and response handlers on a page."""
            pg.route("**/api/v1/data/exposure", handle_exposure)
            pg.on("response", handle_response)

        # Register handlers once (persist across navigations)
        setup_handlers(page)

        # Login once
        login_if_needed(page, WORKSPACE_URL)

        while True:
            if not market_open_now():
                time.sleep(PULL_EVERY)
                continue

            try:
                # ── Reset capture state ──
                cycle["exposures"] = []
                cycle["paradigm"] = None
                cycle["spot_vol"] = None

                # ── Navigate to workspace (triggers all widget API calls) ──
                print("[volland-v2] Fetching workspace...", flush=True)
                page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)

                if "/sign-in" in page.url:
                    login_if_needed(page, WORKSPACE_URL)
                    # Reset — login navigation may have triggered stale captures
                    cycle["exposures"] = []
                    cycle["paradigm"] = None
                    cycle["spot_vol"] = None
                    page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)

                # ── Wait for all widgets to load and fire API calls ──
                page.wait_for_timeout(int(WAIT_SEC * 1000))

                # Dismiss session-limit modal if it appeared late
                handle_session_limit_modal(page)

                # ── Process captured data ──
                exposures = cycle["exposures"]
                paradigm  = cycle["paradigm"]
                spot_vol  = cycle["spot_vol"]

                # Format statistics (backward compatible with V1)
                stats = format_statistics(paradigm, spot_vol)

                # Save exposure points to DB
                total_points = 0
                for exp in exposures:
                    greek      = exp["greek"]
                    exp_option = exp["expiration_option"]
                    items      = exp["items"]
                    cur_price  = exp["current_price"]

                    # Backward compat: 0DTE charm keeps expiration_option=None
                    # so existing queries (WHERE greek='charm') still work
                    db_exp_option = (
                        None if (greek == "charm" and exp_option == "TODAY")
                        else exp_option
                    )

                    count = save_exposure_points(
                        items, greek=greek, ticker="SPX",
                        current_price=cur_price, expiration_option=db_exp_option,
                    )
                    total_points += count

                # Build snapshot payload
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

                # Browser crash recovery
                if "closed" in str(e).lower() or "Target" in str(e):
                    print("[volland-v2] Browser/page crashed — recreating...", flush=True)
                    try:
                        browser.close()
                    except Exception:
                        pass
                    browser = p.chromium.launch(
                        headless=True,
                        args=["--no-sandbox", "--disable-dev-shm-usage"],
                    )
                    page = browser.new_page(viewport={"width": 1400, "height": 900})
                    page.set_default_timeout(90000)
                    setup_handlers(page)
                    login_if_needed(page, WORKSPACE_URL)
                    print("[volland-v2] Browser recreated successfully", flush=True)

            time.sleep(PULL_EVERY)


if __name__ == "__main__":
    run()
