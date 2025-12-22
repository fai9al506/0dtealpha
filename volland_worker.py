import os, json, time, traceback
from datetime import datetime, timezone
import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright

DB_URL = os.getenv("DATABASE_URL", "")
EMAIL  = os.getenv("VOLLAND_EMAIL", "")
PASS   = os.getenv("VOLLAND_PASSWORD", "")
URL    = os.getenv("VOLLAND_URL", "")
PULL_EVERY = int(os.getenv("VOLLAND_PULL_EVERY_SEC", "60"))

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

def login_if_needed(page):
    # Go directly to workspace; if not logged in, it should redirect to login
    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    # If a "Sign in / Log in" button exists, click it
    for text in ["Sign in", "Log in", "Login"]:
        btn = page.get_by_role("button", name=text)
        if btn.count() > 0:
            btn.first.click()
            page.wait_for_timeout(1200)
            break

    # If we are already inside the app (no password field), skip
    if page.locator("input[type='password']").count() == 0:
        return

    # Use more flexible selectors for email field
    email_box = (
        page.get_by_label("Email")
        if page.get_by_label("Email").count() > 0
        else page.locator("input[type='email'], input[name='email'], input[autocomplete='email'], input[placeholder*='mail' i]").first
    )

    pwd_box = page.locator("input[type='password']").first

    try:
        email_box.wait_for(timeout=90000)
        pwd_box.wait_for(timeout=90000)
    except Exception:
        # Debug: screenshot + url + title
        print("[login] could not find login fields")
        print("[login] url:", page.url)
        try:
            print("[login] title:", page.title())
        except Exception:
            pass
        page.screenshot(path="debug_login.png", full_page=True)
        raise

    email_box.fill(EMAIL)
    pwd_box.fill(PASS)

    # Submit (try common patterns)
    submit = page.locator("button[type='submit'], button:has-text('Sign in'), button:has-text('Log in')").first
    submit.click()
    page.wait_for_timeout(2500)

def run():
    if not DB_URL or not EMAIL or not PASS or not URL:
        raise RuntimeError("Missing env vars: DATABASE_URL / VOLLAND_EMAIL / VOLLAND_PASSWORD / VOLLAND_URL")

    ensure_tables()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(90000)

        # ✅ robust login
        login_if_needed(page)

        while True:
            try:
                page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(2000)

                payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "raw": "PUT YOUR TOOLTIP DATA HERE"
                }

                save_snapshot(payload)
                print("[volland] saved", payload["ts_utc"])

            except Exception as e:
                print("[volland] error:", e)
                traceback.print_exc()

            time.sleep(PULL_EVERY)

if __name__ == "__main__":
    run()
