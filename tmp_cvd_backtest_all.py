"""Backtest CVD Divergence on all available dates with RTH filter"""
import os, sys
from sqlalchemy import create_engine, text
from datetime import datetime, time as dtime
sys.path.insert(0, '.')

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Get all available dates
dates = c.execute(text("""
    SELECT DISTINCT trade_date FROM es_range_bars
    WHERE source = 'rithmic'
    ORDER BY trade_date
""")).fetchall()
dates = [r[0] for r in dates]
print("Available dates: %s" % ", ".join(str(d) for d in dates))

# Monkey-patch time gate in setup_detector
import app.setup_detector as sd

# We'll set fake time per-signal check based on bar timestamp
from zoneinfo import ZoneInfo
NY = ZoneInfo("America/New_York")

class FakeDT(datetime):
    _fake_time = dtime(12, 0)
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 3, 10, cls._fake_time.hour, cls._fake_time.minute, 0, tzinfo=NY)

sd.datetime = FakeDT

from app.setup_detector import evaluate_absorption, _cooldown_absorption

settings = {'absorption_enabled': True, 'abs_pivot_n': 2, 'abs_max_trigger_dist': 40}
SL = 8
TARGET = 10

# RTH window in UTC (EDT: 9:30-16:00 ET = 13:30-20:00 UTC)
# Time gate in evaluate_absorption: 10:00-15:30 ET = 14:00-19:30 UTC
RTH_START_H = 14  # 10:00 ET in UTC (EDT)
RTH_END_H = 19    # 15:00 ET in UTC (EDT)

grand_total = {'w': 0, 'l': 0, 'pnl': 0, 'n': 0}

for trade_date in dates:
    rows = c.execute(text("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume, bar_delta,
               bar_buy_volume, bar_sell_volume, cvd_close, ts_start, ts_end
        FROM es_range_bars
        WHERE trade_date = :d AND source = 'rithmic'
        ORDER BY bar_idx
    """), {"d": str(trade_date)}).fetchall()

    bars = []
    for r in rows:
        ts_end = str(r[11]) if r[11] else ""
        bars.append({
            'idx': r[0], 'open': float(r[1]), 'high': float(r[2]),
            'low': float(r[3]), 'close': float(r[4]), 'volume': int(r[5]),
            'delta': int(r[6]), 'buy_volume': int(r[7]), 'sell_volume': int(r[8]),
            'cvd': int(r[9]), 'status': 'closed',
            'ts_end': ts_end,
        })

    if len(bars) < 30:
        print("\n%s: only %d bars, skipping" % (trade_date, len(bars)))
        continue

    # Reset cooldowns for each day
    _cooldown_absorption.clear()

    signals = []
    for i in range(20, len(bars)):
        subset = bars[:i+1]
        result = evaluate_absorption(subset, None, settings, spx_spot=None)
        if result:
            bar = bars[i]
            ts = bar.get('ts_end', '')
            # Filter to RTH only (10:00-15:30 ET)
            try:
                if ts:
                    ts_dt = datetime.fromisoformat(ts.replace('+00:00', '+00:00'))
                    if hasattr(ts_dt, 'hour'):
                        utc_h = ts_dt.hour
                        if utc_h < RTH_START_H or utc_h >= RTH_END_H:
                            continue
            except Exception:
                pass

            signals.append({
                'bar_idx': bar['idx'],
                'price': bar['close'],
                'direction': result['direction'],
                'pattern': result.get('pattern', '?'),
                'grade': result.get('grade', '?'),
                'ts': ts,
            })

    # Forward simulate
    day_w, day_l, day_pnl = 0, 0, 0.0
    print("\n" + "=" * 95)
    print("%s  (%d bars, %d RTH signals)" % (trade_date, len(bars), len(signals)))
    print("=" * 95)

    for sig in signals:
        entry = sig['price']
        is_long = sig['direction'] == 'bullish'
        forward = [b for b in bars if b['idx'] > sig['bar_idx']]

        outcome = "OPEN"
        pnl = 0.0
        mfe, mae = 0.0, 0.0

        for fb in forward:
            fav = (fb['high'] - entry) if is_long else (entry - fb['low'])
            adv = (entry - fb['low']) if is_long else (fb['high'] - entry)
            mfe = max(mfe, fav)
            mae = max(mae, adv)
            if adv >= SL:
                outcome = "LOSS"
                pnl = -SL
                break
            if fav >= TARGET:
                outcome = "WIN"
                pnl = TARGET
                break

        if outcome == "WIN": day_w += 1
        elif outcome == "LOSS": day_l += 1
        day_pnl += pnl

        ts_short = sig['ts'][11:19] if len(sig['ts']) > 11 else "?"
        print("  bar#%-4d %s  %-8s %-18s [%s] @%.2f  -> %-5s %+.0f (MFE=%.1f MAE=%.1f)" % (
            sig['bar_idx'], ts_short, sig['direction'], sig['pattern'],
            sig['grade'], entry, outcome, pnl, mfe, mae))

    if signals:
        wr = day_w / (day_w + day_l) * 100 if day_w + day_l else 0
        print("-" * 60)
        print("  %s: %d signals | %dW/%dL | WR %.0f%% | %+.1f pts | $%+.0f (10 MES)" % (
            trade_date, len(signals), day_w, day_l, wr, day_pnl, day_pnl * 50))
        grand_total['w'] += day_w
        grand_total['l'] += day_l
        grand_total['pnl'] += day_pnl
        grand_total['n'] += len(signals)
    else:
        print("  No RTH signals")

c.close()

print("\n" + "=" * 95)
print("GRAND TOTAL (all dates, RTH only, SL=%d T=%d)" % (SL, TARGET))
print("=" * 95)
gt = grand_total
wr = gt['w'] / (gt['w'] + gt['l']) * 100 if gt['w'] + gt['l'] else 0
print("%d signals | %dW/%dL | WR %.0f%% | %+.1f pts | $%+.0f (10 MES)" % (
    gt['n'], gt['w'], gt['l'], wr, gt['pnl'], gt['pnl'] * 50))

sd.datetime = datetime
