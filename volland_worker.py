import os, json, time, traceback
from datetime import datetime, timezone
import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright

DB_URL = os.getenv("DATABASE_URL", "")
EMAIL  = os.getenv("VOLLAND_EMAIL", "")
PASS   = os.getenv("VOLLAND_PASSWORD", "")
URL    = os.getenv("VOLLAND_URL", "")   # your workspace link
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

def run():
    if not DB_URL or not EMAIL or not PASS or not URL:
        raise RuntimeError("Missing env vars: DATABASE_URL / VOLLAND_EMAIL / VOLLAND_PASSWORD / VOLLAND_URL")

    ensure_tables()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        page = browser.new_page(viewport={"width": 1400, "height": 900})

        # login
        page.goto("https://vol.land/app", wait_until="domcontentloaded", timeout=60000)
        page.locator("input[type='email']").first.fill(EMAIL)
        page.locator("input[type='password']").first.fill(PASS)
        page.locator("button[type='submit']").first.click()
        page.wait_for_timeout(2000)

        while True:
            try:
                page.goto(URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(2000)

                # ✅ TODO: replace this block with YOUR working tooltip extraction
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
