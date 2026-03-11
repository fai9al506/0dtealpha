"""Debug why CVD Divergence never fires — test with time gate bypassed"""
import os, sys
from sqlalchemy import create_engine, text
from unittest.mock import patch
from datetime import datetime, time as dtime, timezone, timedelta
sys.path.insert(0, '.')

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

rows = c.execute(text("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume, bar_delta,
           bar_buy_volume, bar_sell_volume, cvd_close, ts_start
    FROM es_range_bars
    WHERE trade_date = '2026-03-10' AND source = 'rithmic'
    ORDER BY bar_idx
""")).fetchall()

bars = []
for r in rows:
    bars.append({
        'idx': r[0], 'open': float(r[1]), 'high': float(r[2]),
        'low': float(r[3]), 'close': float(r[4]), 'volume': int(r[5]),
        'delta': int(r[6]), 'buy_volume': int(r[7]), 'sell_volume': int(r[8]),
        'cvd': int(r[9]), 'status': 'closed',
    })
c.close()
print("Total bars: %d" % len(bars))

# Monkey-patch datetime.now in setup_detector to return market hours
import app.setup_detector as sd
_original_now = datetime.now

class FakeDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        # Return 12:00 ET to pass the time gate
        from zoneinfo import ZoneInfo
        ny = ZoneInfo("America/New_York")
        return datetime(2026, 3, 10, 12, 0, 0, tzinfo=ny)

sd.datetime = FakeDatetime

from app.setup_detector import evaluate_absorption, _cooldown_absorption

settings = {'absorption_enabled': True, 'abs_pivot_n': 2, 'abs_max_trigger_dist': 40}

signals_found = 0
for i in range(20, len(bars)):
    _cooldown_absorption.clear()
    subset = bars[:i+1]
    result = evaluate_absorption(subset, None, settings, spx_spot=None)
    if result:
        signals_found += 1
        bar = bars[i]
        print("  #%3d bar#%-4d price=%.2f: %s %-8s [%s] score=%d pattern=%s" % (
            signals_found, bar['idx'], bar['close'], result['setup_name'],
            result['direction'], result.get('grade', '?'), result.get('score', 0),
            result.get('pattern', '?')))
        if signals_found >= 30:
            print("  ... (capping)")
            break

print("\nTotal signals found: %d (scanning %d bar positions)" % (signals_found, len(bars)-20))

# Restore
sd.datetime = datetime
