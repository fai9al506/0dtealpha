"""Pure-HTTP Volland worker — bypasses headless browser detection.

WHY THIS EXISTS:
  2026-05-21: vol.land deployed JS-level bot detection on the workspace page.
  Headless Chromium can log in but the React widget tree refuses to mount,
  so /api/v1/data/exposure POSTs never fire. The previous v2 worker became
  unusable on Railway. Direct API login also fails (409 device verification
  for new IPs/devices).

  HOWEVER the API itself is unprotected when called with a valid JWT. So:
  this worker reads a JWT from env (VOLLAND_JWT), provided by the user
  copy-pasting it from their already-authenticated browser DevTools. The
  JWT expires every ~8h, so user refreshes it once per market day.

ARCHITECTURE:
  - No browser at all. Pure requests-based HTTP loop.
  - Every 120s: fetch SPX/SPY paradigm + spot-vol-beta + 10 exposure greeks.
  - Saves to SAME volland_snapshots + volland_exposure_points tables as v2.
  - Payload schema matches v2's format_statistics() so existing portal code
    + setup_detector code work unchanged.
  - JWT expiry monitored; Telegram alert at <2h remaining + skip cycle on
    expired JWT instead of crashing.

USAGE:
  export VOLLAND_JWT=<jwt copied from browser DevTools>
  export DATABASE_URL=postgres://...
  export VOLLAND_TICKER=SPX     # optional, default SPX
  export TELEGRAM_BOT_TOKEN=...  # optional, for alerts
  export TELEGRAM_CHAT_ID=...    # optional
  python volland_http_worker.py

GETTING THE JWT:
  1. Open vol.land in your normal Chrome (logged-in).
  2. F12 → Network tab → Reload page → click any /api/v1/data/... request.
  3. Headers panel → Request Headers → copy 'authorization' value
     (the part after "Bearer ", a long string starting with "eyJ").
  4. Paste into VOLLAND_JWT env var on Railway and redeploy.
"""
from __future__ import annotations

import os
import sys
import time
import json
import html
import base64
import traceback
import requests
import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config ────────────────────────────────────────────────────────────
JWT = os.environ.get("VOLLAND_JWT", "").strip()
DB_URL = os.environ["DATABASE_URL"]
TICKER = os.environ.get("VOLLAND_TICKER", "SPX").upper()
CYCLE_S = int(os.environ.get("VOLLAND_CYCLE_S", "120"))
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")

API_BASE = "https://api.vol.land"
COMMON_HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Origin": "https://vol.land",
    "Referer": "https://vol.land/",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Greek × expiration combinations the browser-based v2 worker captures.
# 10 total = matches a healthy v2 cycle.
EXPOSURES = [
    ("charm",       "TODAY"),
    ("vanna",       "TODAY"),
    ("vanna",       "THIS_WEEK"),
    ("vanna",       "THIRTY_NEXT_DAYS"),
    ("vanna",       "ALL"),
    ("gamma",       "TODAY"),
    ("gamma",       "THIS_WEEK"),
    ("gamma",       "THIRTY_NEXT_DAYS"),
    ("gamma",       "ALL"),
    ("deltaDecay",  "TODAY"),
]

# Charm/TODAY is stored with NULL expiration_option in v2 (legacy). Match that.
def _db_exp_option(greek: str, exp: str) -> str | None:
    if greek == "charm" and exp == "TODAY":
        return None
    return exp


# ── Telegram alert ────────────────────────────────────────────────────
def send_telegram(msg: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        print(f"[telegram] error: {e}", flush=True)


# ── JWT helpers ───────────────────────────────────────────────────────
def parse_jwt_exp(jwt: str) -> datetime | None:
    """Returns expiry datetime (UTC) for a JWT, or None if can't parse."""
    if not jwt or jwt.count(".") != 2:
        return None
    try:
        # Decode the payload (middle segment); pad base64 properly
        body_b64 = jwt.split(".")[1]
        body_b64 += "=" * (-len(body_b64) % 4)
        body = json.loads(base64.urlsafe_b64decode(body_b64))
        exp = body.get("exp")
        if isinstance(exp, (int, float)):
            return datetime.fromtimestamp(exp, tz=timezone.utc)
    except Exception:
        pass
    return None


# ── DB helpers ────────────────────────────────────────────────────────
def db():
    return psycopg.connect(DB_URL, autocommit=True, row_factory=dict_row)


def ensure_tables():
    """Create tables/indexes if missing. Matches v2 schema exactly."""
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
        DO $$ BEGIN
            ALTER TABLE volland_snapshots ADD COLUMN data_ts TIMESTAMPTZ;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """)
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


def save_snapshot(payload: dict, data_ts=None):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO volland_snapshots(payload, data_ts) VALUES (%s::jsonb, %s)",
            (json.dumps(payload), data_ts),
        )


def save_exposure_points(points: list, greek: str, ticker: str,
                          current_price: float | None, expiration_option: str | None) -> int:
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
    with db() as conn, conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO volland_exposure_points
            (ts_utc, ticker, greek, expiration_option, strike, value, current_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, rows)
    return len(rows)


# ── Formatting (matches v2 format_statistics) ─────────────────────────
def format_statistics(paradigm_data: dict | None, spot_vol_data: dict | None) -> dict:
    """Match v2's format_statistics for full backward compatibility."""
    stats: dict = {}
    if paradigm_data:
        stats["paradigm"] = paradigm_data.get("paradigm")
        t = paradigm_data.get("target")
        if isinstance(t, str):
            try: t = float(t)
            except (ValueError, TypeError): t = None
        if isinstance(t, float) and t != t:
            t = None
        stats["target"] = f"${t:,.0f}" if isinstance(t, (int, float)) else None
        lis = paradigm_data.get("lis")
        if isinstance(lis, list):
            lis = [x for x in lis if isinstance(x, (int, float)) and not (isinstance(x, float) and x != x)]
        if isinstance(lis, list) and len(lis) >= 2:
            stats["lines_in_sand"] = f"${lis[0]:,} - ${lis[-1]:,}"
        elif isinstance(lis, list) and len(lis) == 1:
            stats["lines_in_sand"] = f"${lis[0]:,}"
        elif isinstance(lis, (int, float)):
            stats["lines_in_sand"] = f"${lis:,}"
        else:
            stats["lines_in_sand"] = "(none)"
        dd = paradigm_data.get("aggregatedDeltaDecay")
        if isinstance(dd, str):
            try: dd = float(dd)
            except (ValueError, TypeError): dd = None
        if isinstance(dd, float) and dd != dd:
            dd = None
        stats["delta_decay_hedging"] = f"${dd:,.0f}" if isinstance(dd, (int, float)) else None
        vol = paradigm_data.get("totalZeroDteOptionVolume")
        if isinstance(vol, str):
            try: vol = float(vol)
            except (ValueError, TypeError): vol = None
        if isinstance(vol, float) and vol != vol:
            vol = None
        stats["opt_volume"] = f"{vol:,.0f}" if isinstance(vol, (int, float)) else None
        stats["aggregatedCharm"] = paradigm_data.get("aggregatedCharm")
    if spot_vol_data:
        stats["spot_vol_beta"] = spot_vol_data
    return stats


# ── HTTP fetchers ─────────────────────────────────────────────────────
class AuthError(Exception):
    pass


def auth_headers() -> dict:
    if not JWT:
        raise AuthError("VOLLAND_JWT env var is empty — paste a fresh JWT from browser")
    return {**COMMON_HDRS, "Authorization": f"Bearer {JWT}"}


def http_get(session: requests.Session, path: str, timeout: int = 15) -> dict | None:
    r = session.get(f"{API_BASE}{path}", headers=auth_headers(), timeout=timeout)
    if r.status_code == 401:
        raise AuthError(f"401 from {path} — JWT expired or invalid")
    if r.status_code != 200:
        print(f"[http] GET {path} → {r.status_code} {r.text[:120]}", flush=True)
        return None
    try:
        return r.json()
    except Exception:
        return None


def http_post(session: requests.Session, path: str, body: dict, timeout: int = 20) -> dict | None:
    r = session.post(f"{API_BASE}{path}", headers=auth_headers(), json=body, timeout=timeout)
    if r.status_code == 401:
        raise AuthError(f"401 from {path} — JWT expired or invalid")
    if r.status_code != 200:
        print(f"[http] POST {path} ({body.get('greek')}/{body.get('expirations',{}).get('option')}) → {r.status_code} {r.text[:120]}", flush=True)
        return None
    try:
        return r.json()
    except Exception:
        return None


def fetch_exposures(session: requests.Session) -> list[dict]:
    """Fetch all 10 exposure combos in parallel. Returns list of dicts matching v2 cycle format."""
    results = []
    def _one(greek: str, exp: str):
        body = {"greek": greek, "expirations": {"option": exp}, "ticker": TICKER}
        data = http_post(session, "/api/v1/data/exposure", body, timeout=20)
        if not data:
            return None
        return {
            "greek": greek,
            "expiration_option": exp,
            "items": data.get("items", []),
            "current_price": data.get("currentPrice"),
            "expirations": data.get("expirations", []),
            "last_modified": data.get("lastModified"),
        }
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_one, g, e): (g, e) for g, e in EXPOSURES}
        for fut in as_completed(futs):
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception as e:
                g, ex_o = futs[fut]
                print(f"[fetch] {g}/{ex_o}: {e}", flush=True)
    return results


# ── Main loop ─────────────────────────────────────────────────────────
def run_once(session: requests.Session) -> tuple[bool, int]:
    """One capture cycle. Returns (ok, total_points)."""
    # SPX paradigm
    spx_para = http_get(session, f"/api/v1/data/paradigms/0dte?ticker={TICKER}")
    spy_para = http_get(session, f"/api/v1/data/paradigms/0dte?ticker=SPY")
    spot_vol = http_get(session, f"/api/v1/data/volhacks/spot-vol-beta?ticker={TICKER}")

    # Exposures (parallel)
    exposures = fetch_exposures(session)
    if not exposures:
        print("[cycle] no exposures captured — possibly transient vol.land outage", flush=True)
        return False, 0

    # Stats payload
    stats = format_statistics(spx_para, spot_vol)
    spy_stats = format_statistics(spy_para, None) if spy_para else {}

    # Save exposure points (per-greek)
    total_points = 0
    summary = []
    for exp in exposures:
        n = save_exposure_points(
            exp["items"], greek=exp["greek"], ticker=TICKER,
            current_price=exp.get("current_price"),
            expiration_option=_db_exp_option(exp["greek"], exp["expiration_option"]),
        )
        total_points += n
        summary.append({"greek": exp["greek"], "option": exp["expiration_option"], "items": len(exp["items"])})

    # lastModified
    cycle_modified = ""
    for exp in reversed(exposures):
        lm = exp.get("last_modified")
        if lm:
            cycle_modified = lm
            break
    data_ts_val = None
    if cycle_modified:
        try:
            data_ts_val = datetime.fromisoformat(cycle_modified.replace("Z", "+00:00"))
        except Exception:
            data_ts_val = datetime.now(timezone.utc)

    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "page_url": "http_worker",  # marker
        "statistics": stats,
        "spy_statistics": spy_stats,
        "exposure_points_saved": total_points,
        "current_price": exposures[0]["current_price"] if exposures else None,
        "captures": {"exposure_count": len(exposures), "exposures_summary": summary},
    }
    save_snapshot(payload, data_ts=data_ts_val)

    para_str = stats.get("paradigm") or "N/A"
    lis_str = stats.get("lines_in_sand") or "N/A"
    spy_dd = spy_stats.get("delta_decay_hedging") or "N/A"
    charm = stats.get("aggregatedCharm") or "N/A"
    print(f"[http-worker] saved {payload['ts_utc']} exposures={len(exposures)} points={total_points} "
          f"paradigm={para_str} lis={lis_str} charm={charm} spy_dd={spy_dd}", flush=True)
    return True, total_points


def main():
    print(f"[http-worker] Starting — ticker={TICKER} cycle={CYCLE_S}s", flush=True)
    if not JWT:
        msg = "❌ VOLLAND_JWT env var is empty. Paste a fresh JWT from your browser DevTools."
        print(msg, flush=True)
        send_telegram(msg)
        sys.exit(1)

    exp_dt = parse_jwt_exp(JWT)
    if exp_dt:
        now = datetime.now(timezone.utc)
        remaining_h = (exp_dt - now).total_seconds() / 3600
        print(f"[http-worker] JWT expires {exp_dt.isoformat()} ({remaining_h:.1f}h remaining)", flush=True)
        if remaining_h < 0:
            send_telegram("❌ Volland HTTP worker: JWT already expired. Paste a fresh JWT.")
            sys.exit(1)
        if remaining_h < 2:
            send_telegram(f"⚠️ Volland JWT expires in {remaining_h:.1f}h — refresh soon")
    else:
        print("[http-worker] WARNING: could not parse JWT exp; running without expiry check", flush=True)

    ensure_tables()

    session = requests.Session()
    cycle_n = 0
    consecutive_zero = 0
    expiry_alerted = False
    while True:
        cycle_n += 1
        t0 = time.time()
        try:
            ok, total_pts = run_once(session)
            if total_pts == 0:
                consecutive_zero += 1
                if consecutive_zero == 3:
                    send_telegram(f"⚠️ Volland HTTP: 3 consecutive 0-pt cycles. vol.land may be degraded.")
                if consecutive_zero >= 6:
                    send_telegram(f"🚨 Volland HTTP: 6 consecutive 0-pt cycles. Investigate.")
            else:
                if consecutive_zero >= 3:
                    send_telegram(f"🟢 Volland HTTP: recovered after {consecutive_zero} zero-cycles")
                consecutive_zero = 0
        except AuthError as e:
            print(f"[http-worker] AUTH ERROR: {e}", flush=True)
            send_telegram(f"🔴 Volland JWT failed: {html.escape(str(e))}. Paste a fresh JWT.")
            time.sleep(30)
            continue
        except Exception as e:
            print(f"[http-worker] CYCLE ERROR: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc()
            send_telegram(f"⚠️ Volland HTTP cycle error: {html.escape(type(e).__name__)}: {html.escape(str(e))[:200]}")
            time.sleep(15)
            continue

        # JWT expiry watch — alert once when <2h, again at <30min
        if exp_dt:
            remaining_min = (exp_dt - datetime.now(timezone.utc)).total_seconds() / 60
            if remaining_min < 30 and not expiry_alerted:
                send_telegram(f"🔴 Volland JWT expires in {remaining_min:.0f} min — paste fresh JWT")
                expiry_alerted = True

        elapsed = time.time() - t0
        sleep_for = max(CYCLE_S - elapsed, 5)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
