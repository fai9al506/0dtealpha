"""
Verify the "fast" GEX Long trades — are they real or data artifacts?
Check the actual price path resolution around those entries.
"""
import os
import numpy as np
import pandas as pd
from datetime import time as dtime, date
from sqlalchemy import create_engine, text
import pytz

NY = pytz.timezone("US/Eastern")
DB_URL = os.environ.get("DATABASE_URL", "")
engine = create_engine(DB_URL)


def parse_dollar(s):
    if not s or s in ('None', 'null', '', 'Terms Of Service', 'Undefined'):
        return None
    s = s.replace('$', '').replace(',', '').strip()
    try:
        return float(s)
    except:
        return None


# The "fast" trades we need to verify (hit target in 0-2 min)
FAST_TRADES = [
    # (date, time_et, spot, target_pts=15)
    ("2026-02-17", "15:37", 6842.9),  # Trade 7: 1 min
    ("2026-02-18", "12:56", 6900.4),  # Trade 8: 0 min
    ("2026-02-18", "13:42", 6889.8),  # Trade 9: 0 min
    ("2026-02-20", "10:57", 6903.1),  # Trade 10: 1 min
    ("2026-02-23", "09:40", 6894.3),  # Trade 11: 0 min
    ("2026-02-26", "10:51", 6889.6),  # Trade 12: 1 min
    ("2026-02-26", "11:44", 6883.8),  # Trade 13: 1 min
    ("2026-03-02", "13:33", 6875.7),  # Trade 15: 0 min
    ("2026-03-02", "15:46", 6874.1),  # Trade 16: 0 min
]


def check_price_data(trade_date, entry_time_et, entry_spot):
    """Check what price data points exist around the entry time."""
    print(f"\n{'='*80}")
    print(f"  {trade_date} @ {entry_time_et} ET — Entry spot: {entry_spot:.1f} — Target: {entry_spot + 15:.1f}")
    print(f"{'='*80}")

    # 1. Chain snapshots around entry (2-min intervals)
    q = text("""
        SELECT ts, spot FROM chain_snapshots
        WHERE ts::date = :dt AND spot IS NOT NULL AND spot > 0
        ORDER BY ts
    """)
    with engine.connect() as conn:
        chain = pd.read_sql(q, conn, params={"dt": trade_date})
    chain['ts'] = pd.to_datetime(chain['ts'], utc=True)
    chain['et'] = chain['ts'].dt.tz_convert(NY)

    print(f"\n  Chain snapshots on {trade_date}: {len(chain)} total")
    print(f"  Interval: ~{chain['ts'].diff().median().total_seconds():.0f}s between snapshots")

    # Show chain prices around entry (±15 min)
    entry_approx = pd.Timestamp(f"{trade_date} {entry_time_et}", tz=NY)
    window = chain[(chain['et'] >= entry_approx - pd.Timedelta('15min')) &
                   (chain['et'] <= entry_approx + pd.Timedelta('30min'))]

    if len(window) > 0:
        print(f"\n  Chain prices around entry (±15m before, +30m after):")
        for _, row in window.iterrows():
            et_str = row['et'].strftime('%H:%M:%S')
            price = row['spot']
            dist_from_entry = price - entry_spot
            target_hit = "  <<<< TARGET HIT" if price >= entry_spot + 15 else ""
            elapsed = (row['et'] - entry_approx).total_seconds() / 60
            print(f"    {et_str}  SPX={price:.2f}  (from entry: {dist_from_entry:+.1f} pts, "
                  f"elapsed: {elapsed:+.0f}m){target_hit}")
    else:
        print(f"  NO chain data found in window!")

    # 2. ES 1-min bars around entry
    q2 = text("""
        SELECT ts, bar_open_price, bar_high_price, bar_low_price, bar_close_price
        FROM es_delta_bars
        WHERE trade_date = :dt
        ORDER BY ts
    """)
    with engine.connect() as conn:
        es_bars = pd.read_sql(q2, conn, params={"dt": trade_date})

    if len(es_bars) > 0:
        es_bars['ts'] = pd.to_datetime(es_bars['ts'], utc=True)
        es_bars['et'] = es_bars['ts'].dt.tz_convert(NY)

        window_es = es_bars[(es_bars['et'] >= entry_approx - pd.Timedelta('5min')) &
                            (es_bars['et'] <= entry_approx + pd.Timedelta('30min'))]

        print(f"\n  ES 1-min bars on {trade_date}: {len(es_bars)} total")
        if len(window_es) > 0:
            print(f"  ES bars around entry:")
            for _, row in window_es.iterrows():
                et_str = row['et'].strftime('%H:%M:%S')
                h = row['bar_high_price']
                l = row['bar_low_price']
                high_from_entry = h - entry_spot
                elapsed = (row['et'] - entry_approx).total_seconds() / 60
                target_hit = "  <<<< HIGH >= TARGET" if h >= entry_spot + 15 else ""
                print(f"    {et_str}  H={h:.2f} L={l:.2f} "
                      f"(high from entry: {high_from_entry:+.1f}, elapsed: {elapsed:+.0f}m){target_hit}")
        else:
            print(f"  NO ES bars in window")
    else:
        print(f"\n  NO ES 1-min bars for {trade_date}")

    # 3. Volland snapshot that triggered the signal
    q3 = text("""
        SELECT ts,
               payload->'statistics'->>'paradigm' as paradigm,
               payload->'statistics'->>'lines_in_sand' as lis,
               payload->'statistics'->>'target' as target
        FROM volland_snapshots
        WHERE ts::date = :dt
        AND payload->'statistics'->>'paradigm' LIKE 'GEX%'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        vol = pd.read_sql(q3, conn, params={"dt": trade_date})
    vol['ts'] = pd.to_datetime(vol['ts'], utc=True)
    vol['et'] = vol['ts'].dt.tz_convert(NY)

    vol_near = vol[(vol['et'] >= entry_approx - pd.Timedelta('5min')) &
                   (vol['et'] <= entry_approx + pd.Timedelta('5min'))]
    if len(vol_near) > 0:
        print(f"\n  Volland snapshots near entry:")
        for _, row in vol_near.iterrows():
            et_str = row['et'].strftime('%H:%M:%S')
            print(f"    {et_str}  paradigm={row['paradigm']}  LIS={row['lis']}  target={row['target']}")


def main():
    print("VERIFYING FAST GEX LONG TRADES")
    print("These trades reportedly hit +15pt target in 0-2 minutes.")
    print("Checking actual price data resolution to confirm or debunk.\n")

    for dt, tm, spot in FAST_TRADES:
        check_price_data(dt, tm, spot)

    print(f"\n\n{'='*80}")
    print("CONCLUSION")
    print("="*80)
    print("If chain snapshots are ~2min apart and ES bars are 1-min,")
    print("a '0 min' exit means the NEXT data point after entry already shows")
    print("price >= target. The actual move could have taken 30s-2min.")
    print("")
    print("Key question: Did price ACTUALLY reach entry+15 within that window,")
    print("or was the entry spot ALREADY stale (price had already moved)?")
    print("Check if the chain spot at entry time matches nearby chain snapshots.")


if __name__ == "__main__":
    main()
