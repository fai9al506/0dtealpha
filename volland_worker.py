# volland_worker.py
import os, json, time, hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

from sqlalchemy import create_engine, text
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

VERSION = "2025-12-23D"

EMAIL = os.getenv("VOLLAND_EMAIL", "")
PASSWORD = os.getenv("VOLLAND_PASSWORD", "")
WORKSPACE_URL = os.getenv("WORKSPACE_URL", "https://vol.land/app/workspace/6787a95cfe7b13a115716f54")
DB_URL = os.getenv("DATABASE_URL", "")

HEADLESS = True
CAPTURE_SECONDS_AFTER_LOAD = int(os.getenv("CAPTURE_WAIT", "60"))

ONLY_CAPTURE_PATHS = ("/api/v1/data/exposure",)

def normalize_db_url(db_url: str) -> str:
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return db_url

def ensure_table(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS volland_exposure (
        id BIGSERIAL PRIMARY KEY,
        ts_utc TIMESTAMPTZ NOT NULL,
        endpoint TEXT NOT NULL,
        ticker TEXT,
        greek TEXT,
        data_type TEXT,
        kind TEXT,
        expirations_opt TEXT,
        expirations TEXT,
        current_price DOUBLE PRECISION,
        last_modified TIMESTAMPTZ,
        body_sha256 TEXT NOT NULL,
        payload JSONB NOT NULL,
        UNIQUE (endpoint, ticker, greek, data_type, kind, expirations_opt, expirations, last_modified, body_sha256)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def parse_meta_from_url(url: str):
    p = urlparse(url)
    qs = parse_qs(p.query)
    def one(k):
        v = qs.get(k)
        return v[0] if v else None
    return {
        "endpoint": p.path,
        "ticker": one("ticker"),
        "greek": one("greek"),
        "data_type": one("type"),
        "kind": one("kind"),
        "expirations_opt": one("expirations[option]") or one("expirations.option") or one("expirationsOption"),
        "expirations": one("expirations[dates]") or one("expirations.dates") or one("dates"),
    }

def to_timestamptz(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None

EMAIL_SELECTORS = [
    'input[type="email"]',
    'input[autocomplete="username"]',
    'input[name*="email" i]',
    'input[id*="email" i]',
    'input[placeholder*="email" i]',
    'input[aria-label*="email" i]',
]
PASS_SELECTORS = [
    'input[type="password"]',
    'input[autocomplete="current-password"]',
    'input[name*="pass" i]',
    'input[id*="pass" i]',
    'input[placeholder*="password" i]',
    'input[aria-label*="password" i]',
]
SUBMIT_BUTTONS = [
    'button:has-text("Sign in")',
    'button:has-text("Log in")',
    'button:has-text("Login")',
    'button:has-text("Continue")',
    'button[type="submit"]',
]

def find_visible_in_any_frame(page, selectors, timeout_ms=20000):
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        for frame in page.frames:
            for sel in selectors:
                loc = frame.locator(sel).first
                try:
                    loc.wait_for(state="visible", timeout=700)
                    return frame, loc
                except Exception:
                    pass
        time.sleep(0.25)
    return None, None

def click_first_available(page, selectors, timeout_ms=8000):
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        for frame in page.frames:
            for sel in selectors:
                loc = frame.locator(sel).first
                try:
                    loc.wait_for(state="visible", timeout=700)
                    loc.click(timeout=2500)
                    return True
                except Exception:
                    pass
        time.sleep(0.25)
    return False

def is_on_workspace(page) -> bool:
    # FIX: check PATH, not full URL (because sign-in contains redirectUri=/app/workspace in query)
    return urlparse(page.url).path.startswith("/app/workspace/")

def ensure_logged_in(page):
    page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(800)
    print(f"[login] landed url={page.url}")

    if is_on_workspace(page):
        print("[login] already authenticated (workspace page)")
        return

    # If session exists, sometimes it auto-redirects after a moment
    try:
        page.wait_for_url("**/app/workspace/**", timeout=8000)
    except Exception:
        pass
    if is_on_workspace(page):
        print("[login] already authenticated (auto-redirect)")
        return

    # Normal login flow
    _, email_loc = find_visible_in_any_frame(page, EMAIL_SELECTORS, timeout_ms=45000)
    if not email_loc:
        raise RuntimeError(f"Email input not found. URL={page.url}")
    email_loc.fill(EMAIL)

    _, pass_loc = find_visible_in_any_frame(page, PASS_SELECTORS, timeout_ms=30000)
    if not pass_loc:
        raise RuntimeError(f"Password input not found. URL={page.url}")
    pass_loc.fill(PASSWORD)

    if not click_first_available(page, SUBMIT_BUTTONS, timeout_ms=12000):
        pass_loc.press("Enter")

    # Wait until we truly reach workspace
    page.wait_for_url("**/app/workspace/**", timeout=60000)
    print(f"[login] after auth url={page.url}")

def main():
    print(f"[boot] VERSION={VERSION}")

    if not EMAIL or not PASSWORD:
        raise SystemExit("Set VOLLAND_EMAIL and VOLLAND_PASSWORD.")
    if not DB_URL:
        raise SystemExit("Set DATABASE_URL.")

    engine = create_engine(normalize_db_url(DB_URL), future=True, pool_pre_ping=True)
    ensure_table(engine)

    captured = []

    def should_capture(resp_url: str) -> bool:
        p = urlparse(resp_url)
        return any(p.path.endswith(x) for x in ONLY_CAPTURE_PATHS)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context()
        context.set_extra_http_headers({"Cache-Control": "no-cache"})
        page = context.new_page()

        def on_response(resp):
            url = resp.url
            if not should_capture(url):
                return
            try:
                if resp.status != 200:
                    return
                ct = (resp.headers.get("content-type") or "").lower()
                if "application/json" not in ct:
                    return
                payload = resp.json()
                print(f"[cap] 200 {urlparse(url).path}")

                payload_str = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
                body_hash = sha256_str(payload_str)

                meta = parse_meta_from_url(url)
                ts_utc = datetime.now(timezone.utc)
                last_modified = to_timestamptz(payload.get("lastModified"))
                current_price = payload.get("currentPrice")

                expirations_list = payload.get("expirations")
                expirations_compact = None
                if isinstance(expirations_list, list) and expirations_list:
                    expirations_compact = ",".join(map(str, expirations_list))

                captured.append({
                    "ts_utc": ts_utc.isoformat(),
                    "endpoint": meta["endpoint"],
                    "ticker": meta["ticker"],
                    "greek": meta["greek"],
                    "data_type": meta["data_type"],
                    "kind": meta["kind"],
                    "expirations_opt": meta["expirations_opt"],
                    "expirations": meta["expirations"] or expirations_compact,
                    "current_price": float(current_price) if current_price is not None else None,
                    "last_modified": last_modified.isoformat() if last_modified else None,
                    "body_sha256": body_hash,
                    "payload": payload,
                })
            except Exception:
                return

        page.on("response", on_response)

        ensure_logged_in(page)

        # Force fresh data requests
        try:
            page.reload(wait_until="domcontentloaded", timeout=60000)
        except PWTimeoutError:
            pass

        # Small UI nudge
        try:
            page.wait_for_timeout(1200)
            page.mouse.move(400, 300)
            page.mouse.wheel(0, 900)
            page.wait_for_timeout(600)
            page.mouse.click(550, 300)
            page.wait_for_timeout(600)
        except Exception:
            pass

        time.sleep(CAPTURE_SECONDS_AFTER_LOAD)
        browser.close()

    if not captured:
        print("[capture] No /api/v1/data/exposure captured.")
        return

    insert_sql = """
    INSERT INTO volland_exposure (
        ts_utc, endpoint, ticker, greek, data_type, kind,
        expirations_opt, expirations, current_price, last_modified,
        body_sha256, payload
    )
    VALUES (
        :ts_utc::timestamptz, :endpoint, :ticker, :greek, :data_type, :kind,
        :expirations_opt, :expirations, :current_price, :last_modified::timestamptz,
        :body_sha256, :payload::jsonb
    )
    ON CONFLICT DO NOTHING;
    """

    with engine.begin() as conn:
        for r in captured:
            conn.execute(
                text(insert_sql),
                {**r, "payload": json.dumps(r["payload"], ensure_ascii=False)},
            )

    print(f"[save] captured={len(captured)} rows (dedupe enabled)")

if __name__ == "__main__":
    main()
