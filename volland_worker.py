# volland_capture.py
# Capture Volland /api/v1/data/exposure responses and store them in Postgres (JSONB).
#
# ENV VARS (recommended):
#   VOLLAND_EMAIL, VOLLAND_PASSWORD, WORKSPACE_URL, DATABASE_URL
#
# RUN:
#   python volland_capture.py
#
# REQUIREMENTS:
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
EMAIL = os.getenv("VOLLAND_EMAIL", "")          # <-- set env var
PASSWORD = os.getenv("VOLLAND_PASSWORD", "")    # <-- set env var
WORKSPACE_URL = os.getenv(
    "WORKSPACE_URL",
    "https://vol.land/app/workspace/6787a95cfe7b13a115716f54"
)
DB_URL = os.getenv("DATABASE_URL", "")          # e.g. Railway Postgres

HEADLESS = True
CAPTURE_SECONDS_AFTER_LOAD = 12  # let Volland finish firing API calls
ONLY_CAPTURE_PATHS = (
    "/api/v1/data/exposure",
    # add more endpoints if you want later:
    # "/api/v1/data/summary",
)

# ====== DB HELPERS ======
def normalize_db_url(db_url: str) -> str:
    if not db_url:
        return db_url
    # SQLAlchemy prefers explicit driver
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
    """
    Many Volland endpoints include useful query params.
    We'll try to extract common ones if present.
    """
    p = urlparse(url)
    qs = parse_qs(p.query)

    def one(key):
        v = qs.get(key)
        return v[0] if v else None

    # typical keys (may differ, but harmless if missing)
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
        # Volland gives e.g. "2025-12-22T23:00:00Z"
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None

# ====== MAIN CAPTURE ======
def main():
    if not EMAIL or not PASSWORD:
        raise SystemExit("Set VOLLAND_EMAIL and VOLLAND_PASSWORD env vars.")
    if not DB_URL:
        raise SystemExit("Set DATABASE_URL env var (Postgres).")

    engine = create_engine(normalize_db_url(DB_URL), future=True, pool_pre_ping=True)
    ensure_table(engine)

    captured = []  # we save to DB ONLY at the end

    def should_capture(response_url: str) -> bool:
        try:
            p = urlparse(response_url)
            return any(p.path.endswith(path) for path in ONLY_CAPTURE_PATHS)
        except Exception:
            return False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
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

                # Volland may include expirations list in payload
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
                    "payload": payload,  # keep full JSON
                }
                captured.append(row)

            except Exception:
                # keep it silent (don’t break capture loop)
                return

        page.on("response", on_response)

        # --- LOGIN FLOW ---
        page.goto("https://vol.land/login", wait_until="domcontentloaded")

        # selectors may change; these are common patterns
        try:
            page.fill('input[type="email"]', EMAIL, timeout=15000)
        except PWTimeoutError:
            # fallback
            page.fill('input[name="email"]', EMAIL)

        try:
            page.fill('input[type="password"]', PASSWORD, timeout=15000)
        except PWTimeoutError:
            page.fill('input[name="password"]', PASSWORD)

        # try common login button patterns
        clicked = False
        for sel in [
            'button:has-text("Sign in")',
            'button:has-text("Log in")',
            'button[type="submit"]',
        ]:
            try:
                page.click(sel, timeout=4000)
                clicked = True
                break
            except Exception:
                pass
        if not clicked:
            raise SystemExit("Could not find login button. Update selectors in the script.")

        # wait for app to load (cookies set)
        page.wait_for_load_state("networkidle", timeout=60000)

        # go to your workspace (this triggers the exposure fetch calls)
        page.goto(WORKSPACE_URL, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=60000)

        # give it a little time to fire the exposure calls
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
                {
                    **r,
                    "payload": json.dumps(r["payload"], ensure_ascii=False),
                },
            )

    print(f"[save] captured={len(captured)} rows (dedupe via UNIQUE + ON CONFLICT DO NOTHING)")

if __name__ == "__main__":
    main()
