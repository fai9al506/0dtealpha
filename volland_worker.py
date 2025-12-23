# volland_worker.py
# Capture Volland /api/v1/data/exposure responses and store them in Postgres (JSONB).
#
# ENV VARS:
#   VOLLAND_EMAIL, VOLLAND_PASSWORD, WORKSPACE_URL, DATABASE_URL
#
# INSTALL:
#   pip install playwright sqlalchemy psycopg[binary]
#   playwright install chromium

import os
import json
import time
import hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

from sqlalchemy import create_engine, text
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ====== CONFIG ======
EMAIL = os.getenv("VOLLAND_EMAIL", "")
PASSWORD = os.getenv("VOLLAND_PASSWORD", "")
WORKSPACE_URL = os.getenv(
    "WORKSPACE_URL",
    "https://vol.land/app/workspace/6787a95cfe7b13a115716f54"
)
DB_URL = os.getenv("DATABASE_URL", "")

HEADLESS = True
CAPTURE_SECONDS_AFTER_LOAD = 12

ONLY_CAPTURE_PATHS = (
    "/api/v1/data/exposure",
)

# ====== DB HELPERS ======
def normalize_db_url(db_url: str) -> str:
    if not db_url:
        return db_url
    if db_url.startswith("postgresql://"):
        return db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return db_url

def ensure_table(engine):
    sql = """
    CREATE TABLE IF NOT EXISTS volland_exposure (
        id              BIGSERIAL PRIMARY KEY,
        ts_utc          TIMESTAMPTZ NOT NULL,
        endpoint        TEXT NOT NULL,
        ticker          TEXT,
        greek           TEXT,
        data_type       TEXT,
        kind            TEXT,
        expirations_opt TEXT,
        expirations     TEXT,
        current_price   DOUBLE PRECISION,
        last_modified   TIMESTAMPTZ,
        body_sha256     TEXT NOT NULL,
        payload         JSONB NOT NULL,
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

    def one(key):
        v = qs.get(key)
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

# ====== PLAYWRIGHT LOGIN HELPERS ======
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

CONTINUE_BUTTONS = [
    'button:has-text("Continue")',
    'button:has-text("Next")',
    'button:has-text("Proceed")',
    'button:has-text("Submit")',
    'button[type="submit"]',
]

SUBMIT_BUTTONS = [
    'button:has-text("Sign in")',
    'button:has-text("Log in")',
    'button:has-text("Login")',
    'button:has-text("Continue")',
    'button[type="submit"]',
]

def dump_debug(page, prefix="debug_login"):
    try:
        page.screenshot(path=f"{prefix}.png", full_page=True)
    except Exception:
        pass
    try:
        html = page.content()
        with open(f"{prefix}.html", "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

def find_visible_in_any_frame(page, selectors, timeout_ms=20000):
    """
    Returns (frame, locator) for the first selector that becomes visible
    across any frame/iframe. Otherwise returns (None, None).
    """
    deadline = time.time() + (timeout_ms / 1000.0)
    last_err = None

    while time.time() < deadline:
        for frame in page.frames:  # includes main frame
            for sel in selectors:
                loc = frame.locator(sel).first
                try:
                    loc.wait_for(state="visible", timeout=800)
                    return frame, loc
                except Exception as e:
                    last_err = e
                    continue
        time.sleep(0.25)

    return None, None

def click_first_available(page, selectors, timeout_ms=8000):
    deadline = time.time() + (timeout_ms / 1000.0)
    while time.time() < deadline:
        for frame in page.frames:
            for sel in selectors:
                loc = frame.locator(sel).first
                try:
                    loc.wait_for(state="visible", timeout=700)
                    loc.click(timeout=2000)
                    return True
                except Exception:
                    continue
        time.sleep(0.25)
    return False

def robust_login(page, email, password):
    # Go to login (might redirect)
    page.goto("https://vol.land/login", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1500)

    # Helpful logs in Railway
    print(f"[login] url={page.url}")
    try:
        print(f"[login] title={page.title()}")
    except Exception:
        pass

    # Find email field (any selector, any frame)
    frame, email_loc = find_visible_in_any_frame(page, EMAIL_SELECTORS, timeout_ms=45000)
    if not email_loc:
        dump_debug(page, "debug_no_email")
        raise RuntimeError(f"Email input not found. Current URL: {page.url} (see debug_no_email.png/html)")

    email_loc.fill(email)

    # If password not visible yet, try clicking Continue/Next to reveal it
    frame2, pass_loc = find_visible_in_any_frame(page, PASS_SELECTORS, timeout_ms=4000)
    if not pass_loc:
        click_first_available(page, CONTINUE_BUTTONS, timeout_ms=8000)
        frame2, pass_loc = find_visible_in_any_frame(page, PASS_SELECTORS, timeout_ms=30000)

    if not pass_loc:
        dump_debug(page, "debug_no_password")
        raise RuntimeError(f"Password input not found. Current URL: {page.url} (see debug_no_password.png/html)")

    pass_loc.fill(password)

    # Submit
    if not click_first_available(page, SUBMIT_BUTTONS, timeout_ms=12000):
        # last fallback: press Enter in password field
        try:
            pass_loc.press("Enter")
        except Exception:
            dump_debug(page, "debug_no_submit")
            raise RuntimeError(f"Submit button not found. Current URL: {page.url} (see debug_no_submit.png/html)")

    # Wait for post-login navigation
    try:
        page.wait_for_load_state("networkidle", timeout=60000)
    except PWTimeoutError:
        # some apps keep network busy; continue anyway
        pass

    print(f"[login] after submit url={page.url}")

# ====== MAIN CAPTURE ======
def main():
    if not EMAIL or not PASSWORD:
        raise SystemExit("Set VOLLAND_EMAIL and VOLLAND_PASSWORD.")
    if not DB_URL:
        raise SystemExit("Set DATABASE_URL (Postgres).")

    engine = create_engine(normalize_db_url(DB_URL), future=True, pool_pre_ping=True)
    ensure_table(engine)

    captured = []  # save to DB only at end

    def should_capture(response_url: str) -> bool:
        try:
            p = urlparse(response_url)
            return any(p.path.endswith(path) for path in ONLY_CAPTURE_PATHS)
        except Exception:
            return False

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
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

                row = {
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
                }
                captured.append(row)

            except Exception:
                return

        page.on("response", on_response)

        # --- LOGIN ---
        robust_login(page, EMAIL, PASSWORD)

        # --- OPEN WORKSPACE (triggers exposure calls) ---
        page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=60000)
        except PWTimeoutError:
            pass

        time.sleep(CAPTURE_SECONDS_AFTER_LOAD)

        browser.close()

    # ====== SAVE TO DB (ONLY HERE, AT THE END) ======
    if not captured:
        print("[capture] No /data/exposure responses captured.")
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
