"""Query pre-market range bars from today."""
import os
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
with e.connect() as c:
    # Check schema
    cols = c.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'es_range_bars' ORDER BY ordinal_position"
    )).fetchall()
    print("Columns:", [r[0] for r in cols])

    # Get today's bars including pre-market
    rows = c.execute(text("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume, bar_delta, cvd_close, ts_start, ts_end, range_pts
        FROM es_range_bars
        WHERE source = 'rithmic'
          AND ts_start >= '2026-03-24 00:00:00+00'
          AND ts_start <= '2026-03-24 14:30:00+00'
        ORDER BY bar_idx
        LIMIT 80
    """)).fetchall()
    print(f"\nFound {len(rows)} bars (pre-market + early RTH)")
    for r in rows:
        ts_str = str(r[8])[:19] if r[8] else "?"
        rng = r[10] if r[10] else 5
        print(f"idx={r[0]:>4} O={r[1]:>8.2f} H={r[2]:>8.2f} L={r[3]:>8.2f} "
              f"C={r[4]:>8.2f} vol={r[5]:>5} delta={r[6]:>+6} "
              f"cvd={r[7]:>+8.0f} rng={rng} start={ts_str}")
