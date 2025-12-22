# volland_worker.py
import os, json, time, traceback, base64, re
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright


# ========= ENV =========
DB_URL   = os.getenv("DATABASE_URL", "")
EMAIL    = os.getenv("VOLLAND_EMAIL", "")
PASS     = os.getenv("VOLLAND_PASSWORD", "")
URL      = os.getenv("VOLLAND_URL", "")  # workspace URL
PULL_EVERY = int(os.getenv("VOLLAND_PULL_EVERY_SEC", "60"))

# sniff network for JSON responses (chart data usually comes as JSON)
SNIFF_SECONDS     = float(os.getenv("VOLLAND_SNIFF_SECONDS", "10"))   # how long to watch responses after page load
MAX_JSON_KB       = int(os.getenv("VOLLAND_MAX_JSON_KB", "900"))      # skip huge JSON
LOG_MATCHED_URLS  = os.getenv("VOLLAND_LOG_MATCHED_URLS", "1") == "1" # print matched JSON URLs


# ========= DB =========
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

def save_snapshot(payload: dict):
    with db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO volland_snapshots(payload) VALUES (%s::jsonb)", (json.dumps(payload),))


# ========= LOGIN HELPERS =========
def handle_session_limit_modal(page) -> bool:
    """
    Volland supports 2 active sessions. On a 3rd device, a confirmation modal appears.
    We must click Continue.
    """
    btn = page.locator(
        "button[data-cy='confirmation-modal-confirm-button'], button:has-text('Continue')"
    ).first

    if btn.count() == 0:
        return False

    try:
        btn.wait_for(state="visible", timeout=3000)
        btn.click()
        page.wait_for_timeout(1200)
        print("[login] session modal: clicked Continue")
        return True
    except Exception:
        return False

def login_if_needed(page):
    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    # already logged in
    if "/sign-in" not in page.url and page.locator("input[name='password'], input[type='password']").count() == 0:
        return

    # wait for login inputs (Volland email is type=text with data-cy)
    email_box = page.locator("input[data-cy='sign-in-email-input'], input[name='email']").first
    pwd_box   = page.locator("input[data-cy='sign-in-password-input'], input[name='password'], input[type='password']").first

    email_box.wait_for(state="visible", timeout=90000)
    pwd_box.wait_for(state="visible", timeout=90000)

    email_box.fill(EMAIL)
    pwd_box.fill(PASS)

    submit = page.locator(
        "button:has-text('Log In'), button:has-text('Login'), button[type='submit']"
    ).first
    submit.click()

    # modal may appear immediately or slightly later
    handle_session_limit_modal(page)

    # SPA login: poll until we leave /sign-in
    deadline = time.time() + 90
    while time.time() < deadline:
        handle_session_limit_modal(page)

        if "/sign-in" not in page.url:
            return

        page.wait_for_timeout(500)

    page.screenshot(path="debug_login_timeout.png", full_page=True)
    body = ""
    try:
        body = (page.locator("body").inner_text() or "")[:500]
    except Exception:
        pass
    raise RuntimeError(f"Login did not redirect after 90s. Still on: {page.url}. Body: {body}")


# ========= NETWORK SNIFFING (REAL DATA) =========
def _safe_json_from_response(resp):
    try:
        ct = (resp.headers.get("content-type") or "").lower()
    except Exception:
        ct = ""
    if "application/json" not in ct and "json" not in ct:
        return None

    try:
        data = resp.json()
    except Exception:
        return None

    # estimate size to avoid saving huge payloads
    try:
        raw = json.dumps(data, ensure_ascii=False)
        kb = len(raw.encode("utf-8")) / 1024.0
    except Exception:
        kb = 0
        raw = None

    if raw is None:
        return None
    if kb > MAX_JSON_KB:
        return None

    return data

def _score_json(obj) -> int:
    """
    Heuristic scoring: prefer objects that look like chart series payloads.
    """
    score = 0
    s = json.dumps(obj, ensure_ascii=False).lower()

    # common chart-ish words
    for w in ["series", "data", "points", "xaxis", "yaxis", "categories", "tooltip", "highcharts", "echarts"]:
        if w in s:
            score += 2

    # Volland-ish words
    for w in ["exposure", "gamma", "vanna", "charm", "dealer", "hedg", "notional", "puts", "calls", "spx", "spot", "strike", "expiration", "expiry"]:
        if w in s:
            score += 2

    # numeric density hint
    score += min(20, s.count("[") + s.count("{"))

    return score

def capture_chart_json(page, sniff_seconds: float):
    """
    Listen to network responses and capture likely JSON used to render charts.
    Returns list of captured items (best-first).
    """
    captured = []

    def on_response(resp):
        try:
            url = resp.url
            if resp.status != 200:
                return

            data = _safe_json_from_response(resp)
            if data is None:
                return

            sc = _score_json(data)

            item = {
                "url": url,
                "status": resp.status,
                "score": sc,
                "json": data
            }
            captured.append(item)

            if LOG_MATCHED_URLS:
                print(f"[sniff] json captured score={sc} url={url}")

        except Exception:
            return

    page.on("response", on_response)

    # Let responses arrive
    end = time.time() + sniff_seconds
    while time.time() < end:
        page.wait_for_timeout(250)

    # stop listening (avoid memory growth)
    try:
        page.remove_listener("response", on_response)
    except Exception:
        pass

    # sort best-first
    captured.sort(key=lambda x: x["score"], reverse=True)

    # keep top 5
    return captured[:5]


# ========= FALLBACK TEXT (NOT REAL VALUES, but helps debugging) =========
def extract_accessible_chart_text(page) -> str:
    try:
        page.wait_for_timeout(1000)
        # often the chart accessibility text is in body; keep it short
        txt = (page.locator("body").inner_text() or "")
        return txt[:20000]
    except Exception:
        return ""


# ========= MAIN LOOP =========
def run():
    if not DB_URL or not EMAIL or not PASS or not URL:
        raise RuntimeError("Missing env vars: DATABASE_URL / VOLLAND_EMAIL / VOLLAND_PASSWORD / VOLLAND_URL")

    ensure_tables()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(90000)

        # login once
        login_if_needed(page)

        while True:
            try:
                # open workspace
                page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(2000)

                # if kicked to sign-in, log in again
                if "/sign-in" in page.url:
                    login_if_needed(page)
                    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                    page.wait_for_timeout(2000)

                # sniff real JSON used by charts
                chart_json = capture_chart_json(page, sniff_seconds=SNIFF_SECONDS)

                # fallback “text” (for debugging only)
                raw_text = extract_accessible_chart_text(page)

                payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "page_url": page.url,
                    "sniff": {
                        "seconds": SNIFF_SECONDS,
                        "count": len(chart_json),
                        "top_urls": [c["url"] for c in chart_json[:3]],
                        "top_scores": [c["score"] for c in chart_json[:3]],
                    },
                    # ✅ this is what you want: real JSON the site fetched (best candidates)
                    "chart_json": chart_json,   # contains [{"url","score","json"}, ...]
                    # debug text (axis labels etc.)
                    "raw_text": raw_text[:4000],
                }

                save_snapshot(payload)

                print(
                    "[volland] saved",
                    payload["ts_utc"],
                    "json_count=",
                    len(chart_json),
                    "top_score=",
                    (chart_json[0]["score"] if chart_json else None)
                )

            except Exception as e:
                err_payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "page_url": getattr(page, "url", ""),
                    "error": str(e),
                    "trace": traceback.format_exc()[-4000:]
                }
                try:
                    save_snapshot({"error_event": err_payload})
                except Exception:
                    pass

                print("[volland] error:", e)
                traceback.print_exc()

            time.sleep(PULL_EVERY)


if __name__ == "__main__":
    run()
