import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Check what strikes look like in vanna exposure
r = c.execute(text("""
    SELECT strike, value::float, expiration_option
    FROM volland_exposure_points
    WHERE greek='vanna' AND ts_utc::date='2026-03-05' AND expiration_option='THIS_WEEK'
    ORDER BY abs(value::float) DESC LIMIT 10
""")).fetchall()
print("Top vanna strikes (Mar 5, THIS_WEEK):", flush=True)
for x in r:
    print(f"  strike={x[0]} val={x[1]:.0f} tf={x[2]}", flush=True)

# SPX spot
r2 = c.execute(text("SELECT spot_price FROM chain_snapshots WHERE created_at::date='2026-03-05' ORDER BY created_at DESC LIMIT 1")).fetchone()
print(f"SPX spot: {r2[0]}", flush=True)

# ES range bar price range
r3 = c.execute(text("""
    SELECT min(bar_low), max(bar_high) FROM es_range_bars
    WHERE trade_date='2026-03-05' AND source='rithmic'
""")).fetchone()
print(f"ES bar range: {r3[0]} - {r3[1]}", flush=True)

# Check error from backtest script
import sys, traceback
sys.path.insert(0, '.')
try:
    from app.setup_detector import _vp_find_swings, _vp_detect_divergences

    bars = c.execute(text("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume, bar_delta, cumulative_delta AS cvd,
               ts_start, ts_end, status
        FROM es_range_bars
        WHERE trade_date = '2026-03-05' AND source = 'rithmic'
        ORDER BY bar_idx ASC
    """)).mappings().all()
    bars = [dict(r) for r in bars]
    print(f"Bars: {len(bars)}", flush=True)

    swings = _vp_find_swings(bars, pivot_n=2)
    print(f"Swings found: {len(swings)}", flush=True)

    divs = _vp_detect_divergences(bars, swings)
    print(f"Divergences: {len(divs) if divs else 0}", flush=True)
    if divs:
        for d in divs[:5]:
            print(f"  idx={d['bar_idx']} dir={d['direction']} price={d['price']:.1f} pattern={d.get('pattern','?')}", flush=True)
except Exception as ex:
    traceback.print_exc()

c.close()
