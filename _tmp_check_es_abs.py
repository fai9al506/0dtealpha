"""Check ES Absorption trade #147 in detail"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

with engine.begin() as conn:
    # 1. Get es_range_bars column names
    cols = conn.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'es_range_bars' ORDER BY ordinal_position
    """)).all()
    print("es_range_bars columns:", [c[0] for c in cols])

    # Trade #147 details (already known):
    # ES entry: 6867.0, bearish
    # Target: 6857.0 (ES - 10), Stop: 6879.0 (ES + 12)
    # Outcome: WIN +10.0, elapsed 5 min
    es_entry = 6867.0
    target = 6857.0
    stop = 6879.0

    # 2. Get ES range bars using correct column names (probably in payload/JSONB or quoted)
    bars = conn.execute(text("""
        SELECT * FROM es_range_bars
        WHERE trade_date = '2026-02-19' AND symbol = '@ES'
        ORDER BY bar_idx ASC
        LIMIT 5
    """)).mappings().all()

    if bars:
        print("\nSample bar keys:", list(bars[0].keys()))

    # 3. Get bars around the trade time
    print(f"\n=== ES RANGE BARS around 18:53 UTC ===")
    bars = conn.execute(text("""
        SELECT * FROM es_range_bars
        WHERE trade_date = '2026-02-19' AND symbol = '@ES'
        ORDER BY bar_idx ASC
    """)).mappings().all()

    print(f"Total bars on Feb 19: {len(bars)}")

    # Find bars near the trade timestamp
    for b in bars:
        ts_start = b.get('ts_start') or b.get('ts')
        bar_high = b.get('high') or b.get('bar_high')
        bar_low = b.get('low') or b.get('bar_low')
        bar_open = b.get('bar_open') or b.get('open_price')
        bar_close = b.get('bar_close') or b.get('close_price') or b.get('close')
        bar_idx = b.get('bar_idx')
        volume = b.get('volume')
        delta = b.get('delta')

        # Show all bars from bar_idx that includes our trade onward
        # The trade was at 18:53:38 UTC
        if ts_start and str(ts_start) >= '2026-02-19 18:40':
            marker = ""
            if bar_low is not None and float(bar_low) <= target:
                marker += " ** TARGET HIT **"
            if bar_high is not None and float(bar_high) >= stop:
                marker += " ** STOP HIT **"
            print(f"  bar#{bar_idx} ts={ts_start} "
                  f"O={bar_open} H={bar_high} L={bar_low} C={bar_close} "
                  f"vol={volume} d={delta}{marker}")

    # 4. Check if ES price actually reached 6857 (target) in 5 minutes
    # The live tracker uses ES range bar close price, not range bar H/L
    # Let me also check how the live tracker resolves ES Absorption outcomes
    print(f"\n=== KEY QUESTION: Did ES actually reach {target}? ===")
    print(f"ES entry: {es_entry}, bearish target: {target} (ES - 10)")
    print(f"SPX spot at trade time: 6851.18")
    print(f"Note: ES and SPX differ! ES was at 6867 while SPX was at 6851")
    print(f"Difference: ES - SPX = {es_entry - 6851.18:.2f}")

    # Also show the SPX path during this time
    print(f"\n=== SPX PLAYBACK 18:50 - 19:10 UTC ===")
    prices = conn.execute(text("""
        SELECT ts, spot FROM playback_snapshots
        WHERE ts >= '2026-02-19 18:50:00' AND ts <= '2026-02-19 19:10:00'
        ORDER BY ts ASC
    """)).mappings().all()
    for p in prices:
        print(f"  {p['ts']} SPX={float(p['spot']):.1f}")
