"""Backfill spx_ohlc_1m with historical 1-min SPX bars from TS barcharts API.
Run via: railway run python _backfill_spx_ohlc.py
"""
import os, sys, requests
from datetime import datetime, date
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

db_url = os.getenv('DATABASE_URL', '').replace('postgres://', 'postgresql://')
if not db_url:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)

engine = create_engine(db_url)

# TS auth
CID = os.getenv("TS_CLIENT_ID", "")
SECRET = os.getenv("TS_CLIENT_SECRET", "")
RTOKEN = os.getenv("TS_REFRESH_TOKEN", "")
BASE = "https://api.tradestation.com/v3"

def get_token():
    r = requests.post("https://signin.tradestation.com/oauth/token", data={
        "grant_type": "refresh_token",
        "client_id": CID,
        "client_secret": SECRET,
        "refresh_token": RTOKEN,
    }, timeout=45)
    return r.json().get("access_token")

token = get_token()
if not token:
    print("ERROR: Failed to get TS token")
    sys.exit(1)

# Pull max bars (API limit varies, try 10000 first then fall back)
for barsback in [10000, 5000, 2000]:
    print(f"Trying barsback={barsback}...")
    r = requests.get(
        f"{BASE}/marketdata/barcharts/$SPX.X",
        headers={"Authorization": f"Bearer {token}"},
        params={"interval": "1", "unit": "Minute", "barsback": str(barsback)},
        timeout=60,
    )
    if r.status_code == 200:
        data = r.json()
        bars = data.get("Bars", [])
        if bars:
            print(f"Got {len(bars)} bars")
            break
    else:
        print(f"  Status {r.status_code}, trying smaller...")
else:
    print("ERROR: All attempts failed")
    sys.exit(1)

# Parse and insert
saved = 0
skipped = 0
with engine.connect() as conn:
    # Check existing count
    existing = conn.execute(text("SELECT COUNT(*) FROM spx_ohlc_1m")).fetchone()[0]
    print(f"Existing rows in spx_ohlc_1m: {existing}")

    for bar in bars:
        ts_raw = bar.get("TimeStamp", "")
        if not ts_raw:
            continue

        bar_ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        bar_et = bar_ts.astimezone(ET)
        trade_date = bar_et.date()

        # Only save market hours bars (9:30-16:00 ET)
        t = bar_et.time()
        from datetime import time as dtime
        if not (dtime(9, 30) <= t <= dtime(16, 0)):
            skipped += 1
            continue

        try:
            conn.execute(text("""
                INSERT INTO spx_ohlc_1m (ts, trade_date, bar_open, bar_high, bar_low, bar_close, volume)
                VALUES (:ts, :td, :o, :h, :l, :c, :v)
                ON CONFLICT (ts) DO NOTHING
            """), {
                "ts": bar_ts, "td": trade_date,
                "o": bar.get("Open"), "h": bar.get("High"),
                "l": bar.get("Low"), "c": bar.get("Close"),
                "v": bar.get("TotalVolume", 0),
            })
            saved += 1
        except Exception as e:
            print(f"  Error on {ts_raw}: {e}")

    conn.commit()

    # Summary
    total = conn.execute(text("SELECT COUNT(*) FROM spx_ohlc_1m")).fetchone()[0]
    dates = conn.execute(text("""
        SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date)
        FROM spx_ohlc_1m
    """)).fetchone()

    print(f"\nBackfill complete:")
    print(f"  Saved: {saved} bars (skipped {skipped} non-market-hours)")
    print(f"  Total in table: {total}")
    print(f"  Date range: {dates[0]} to {dates[1]} ({dates[2]} trading days)")
