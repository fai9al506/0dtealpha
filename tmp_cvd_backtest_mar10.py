"""Backtest CVD Divergence on Mar 10 RTH session with proper cooldowns"""
import os, sys
from sqlalchemy import create_engine, text
from datetime import datetime, time as dtime, timedelta
sys.path.insert(0, '.')

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

rows = c.execute(text("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume, bar_delta,
           bar_buy_volume, bar_sell_volume, cvd_close, ts_start, ts_end
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
        'ts_start': str(r[10]), 'ts_end': str(r[11]),
    })
c.close()

print("Total bars: %d (idx %d-%d)" % (len(bars), bars[0]['idx'], bars[-1]['idx']))

# Monkey-patch time gate
import app.setup_detector as sd
_orig_dt = sd.datetime

class FakeDT(datetime):
    @classmethod
    def now(cls, tz=None):
        from zoneinfo import ZoneInfo
        return datetime(2026, 3, 10, 12, 0, 0, tzinfo=ZoneInfo("America/New_York"))

sd.datetime = FakeDT

from app.setup_detector import evaluate_absorption, _cooldown_absorption

settings = {'absorption_enabled': True, 'abs_pivot_n': 2, 'abs_max_trigger_dist': 40}

# Run with REAL cooldowns (not clearing between each bar)
_cooldown_absorption.clear()
signals = []
SL = 8
TARGET = 10

for i in range(20, len(bars)):
    subset = bars[:i+1]
    result = evaluate_absorption(subset, None, settings, spx_spot=None)
    if result:
        bar = bars[i]
        signals.append({
            'bar_idx': bar['idx'],
            'price': bar['close'],
            'direction': result['direction'],
            'pattern': result.get('pattern', '?'),
            'grade': result.get('grade', '?'),
            'score': result.get('score', 0),
            'ts': bar.get('ts_end', ''),
        })

print("\n" + "=" * 90)
print("CVD DIVERGENCE SIGNALS — Mar 10 (with cooldowns)")
print("=" * 90)
print("%-5s %-8s %-22s %-8s %-18s %s %s" % ("#", "Bar", "Time", "Dir", "Pattern", "Grd", "Entry"))
print("-" * 90)

# Forward simulate each signal
results = []
for i, sig in enumerate(signals):
    entry_price = sig['price']
    direction = sig['direction']
    is_long = direction == 'bullish'

    # Find forward bars after this signal
    entry_idx = sig['bar_idx']
    forward_bars = [b for b in bars if b['idx'] > entry_idx]

    outcome = "OPEN"
    exit_price = entry_price
    max_fav = 0
    max_adv = 0

    for fb in forward_bars:
        if is_long:
            fav = fb['high'] - entry_price
            adv = entry_price - fb['low']
        else:
            fav = entry_price - fb['low']
            adv = fb['high'] - entry_price

        max_fav = max(max_fav, fav)
        max_adv = max(max_adv, adv)

        # Check stop first
        if adv >= SL:
            outcome = "LOSS"
            exit_price = entry_price - SL if is_long else entry_price + SL
            break
        # Check target
        if fav >= TARGET:
            outcome = "WIN"
            exit_price = entry_price + TARGET if is_long else entry_price - TARGET
            break

    pnl = exit_price - entry_price if is_long else entry_price - exit_price
    results.append({
        'outcome': outcome,
        'pnl': pnl,
        'max_fav': max_fav,
        'max_adv': max_adv,
    })

    ts_short = sig['ts'][11:19] if len(sig['ts']) > 11 else sig['ts']
    print("%-5d %-8d %-22s %-8s %-18s [%s]  %.2f  -> %-6s %+.1f (MFE=%.1f MAE=%.1f)" % (
        i+1, sig['bar_idx'], ts_short, direction, sig['pattern'],
        sig['grade'], entry_price, outcome, pnl, max_fav, max_adv))

print("\n" + "=" * 90)
print("SUMMARY (SL=%d, T=%d)" % (SL, TARGET))
print("=" * 90)
wins = sum(1 for r in results if r['outcome'] == 'WIN')
losses = sum(1 for r in results if r['outcome'] == 'LOSS')
total_pnl = sum(r['pnl'] for r in results)
print("Signals: %d  |  %dW / %dL  |  WR: %.0f%%  |  Net: %+.1f pts" % (
    len(results), wins, losses,
    wins/(wins+losses)*100 if wins+losses else 0, total_pnl))
print("Avg P&L: %+.1f pts  |  10 MES = $%+.0f" % (
    total_pnl/len(results) if results else 0, total_pnl * 10 * 5))

sd.datetime = _orig_dt
