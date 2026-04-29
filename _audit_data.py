import os
from sqlalchemy import create_engine, text
db = os.getenv('DATABASE_URL', '').replace('postgres://', 'postgresql://')
engine = create_engine(db)
with engine.connect() as conn:
    r = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               setup_name, direction, grade, score, spot, paradigm,
               greek_alignment, vix, overvix,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               outcome_stop_level, outcome_target_level, outcome_elapsed_min, outcome_first_event
        FROM setup_log
        WHERE setup_name = 'Skew Charm' AND outcome_result = 'WIN'
          AND (ts AT TIME ZONE 'America/New_York')::date >= '2026-03-20'
        ORDER BY id DESC LIMIT 1
    """)).fetchone()
    print("SAMPLE SC WIN TRADE:")
    cols = ['id','ts_et','setup_name','direction','grade','score','spot','paradigm',
            'greek_alignment','vix','overvix',
            'outcome_result','outcome_pnl','outcome_max_profit','outcome_max_loss',
            'outcome_stop_level','outcome_target_level','outcome_elapsed_min','outcome_first_event']
    for c, v in zip(cols, r):
        print(f"  {c:25s} = {v}")

    # What can we reconstruct from chain_snapshots?
    r2 = conn.execute(text("""
        SELECT COUNT(*) as n,
               MIN(ts AT TIME ZONE 'America/New_York') as first_ts,
               MAX(ts AT TIME ZONE 'America/New_York') as last_ts,
               COUNT(DISTINCT (ts AT TIME ZONE 'America/New_York')::date) as days
        FROM chain_snapshots
    """)).fetchone()
    print(f"\nCHAIN_SNAPSHOTS: {r2[0]} rows, {r2[3]} days, {r2[1].date()} to {r2[2].date()}")

    # setup_log date range
    r3 = conn.execute(text("""
        SELECT MIN((ts AT TIME ZONE 'America/New_York')::date),
               MAX((ts AT TIME ZONE 'America/New_York')::date),
               COUNT(DISTINCT (ts AT TIME ZONE 'America/New_York')::date)
        FROM setup_log WHERE outcome_result IS NOT NULL
    """)).fetchone()
    print(f"SETUP_LOG outcomes: {r3[2]} days, {r3[0]} to {r3[1]}")

    # Can we backfill OHLC from TS API?
    print(f"\nTS barcharts API supports barsback up to ~10,000 for 1-min bars")
    print(f"  10,000 bars / 390 per day = ~25 trading days")
    print(f"  Could backfill from ~Feb 17 to today in one API call")

    # What columns would make analysis easy?
    print(f"\nPROPOSED NEW COLUMNS FOR setup_log:")
    print(f"  filter_version  TEXT    -- 'V12', 'V11', etc. at time of signal")
    print(f"  trail_sl        FLOAT   -- initial SL pts at time of trade (14, 20, etc)")
    print(f"  trail_activation FLOAT  -- trail activation pts")
    print(f"  trail_gap       FLOAT   -- trail gap pts")
    print(f"  exit_price      FLOAT   -- actual SPX exit price")
    print(f"  data_quality    TEXT    -- 'clean', 'stale', 'outage'")
