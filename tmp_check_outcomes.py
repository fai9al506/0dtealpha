"""Check today's ES Absorption outcomes — what they SHOULD be."""
import os, sys, psycopg
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from psycopg.rows import dict_row
from datetime import datetime, timezone

c = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, row_factory=dict_row)

# Get today's absorption trades
trades = c.execute("""
    SELECT id, setup_name, direction, score, abs_es_price, abs_vol_ratio,
           ts AT TIME ZONE 'America/New_York' as ts_et, ts
    FROM setup_log
    WHERE ts::date = '2026-02-27' AND setup_name = 'ES Absorption'
    ORDER BY id
""").fetchall()

# Get today's ES range bars
bars = c.execute("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, ts_start, ts_end, status
    FROM es_range_bars
    WHERE trade_date = '2026-02-27' AND source = 'rithmic' AND status = 'closed'
    ORDER BY bar_idx ASC
""").fetchall()

print(f"Trades: {len(trades)}, Bars: {len(bars)}")
print()

import pytz
NY = pytz.timezone("America/New_York")

for t in trades:
    es_entry = float(t['abs_es_price'])
    direction = t['direction']
    is_long = direction.lower() in ('long', 'bullish')
    ts = t['ts']

    ten_pt = es_entry + 10 if is_long else es_entry - 10
    initial_stop = es_entry - 12 if is_long else es_entry + 12

    # Find signal bar
    signal_bar_idx = None
    for b in bars:
        bar_start = b['ts_start']
        if hasattr(bar_start, 'tzinfo') and bar_start.tzinfo is None:
            bar_start = NY.localize(bar_start)
        if bar_start <= ts:
            signal_bar_idx = b['bar_idx']
        else:
            break

    if signal_bar_idx is None:
        print(f"#{t['id']} {direction} @ {es_entry} - NO SIGNAL BAR FOUND")
        continue

    # Walk bars
    hit_10pt = False
    max_profit = 0.0
    max_loss = 0.0
    trail_stop = initial_stop
    trail_peak = 0.0
    trail_exit_pnl = None
    first_event = None

    for b in bars:
        if b['bar_idx'] <= signal_bar_idx:
            continue
        bh = float(b['bar_high'])
        bl = float(b['bar_low'])

        if is_long:
            ph = bh - es_entry
            pl = bl - es_entry
        else:
            ph = es_entry - bl
            pl = es_entry - bh

        if ph > max_profit:
            max_profit = ph
        if pl < max_loss:
            max_loss = pl

        # T1
        if not hit_10pt:
            if is_long and bh >= ten_pt:
                hit_10pt = True
                if first_event is None:
                    first_event = "10pt"
            elif not is_long and bl <= ten_pt:
                hit_10pt = True
                if first_event is None:
                    first_event = "10pt"

        # Trail
        if trail_exit_pnl is None:
            if ph > trail_peak:
                trail_peak = ph
            if trail_peak >= 10:
                trail_lock = max(trail_peak - 5, 0)
                if is_long:
                    new_stop = es_entry + trail_lock
                    if new_stop > trail_stop:
                        trail_stop = new_stop
                else:
                    new_stop = es_entry - trail_lock
                    if new_stop < trail_stop:
                        trail_stop = new_stop

            if is_long and bl <= trail_stop:
                trail_exit_pnl = round(trail_stop - es_entry, 2)
                if first_event is None:
                    first_event = "stop"
            elif not is_long and bh >= trail_stop:
                trail_exit_pnl = round(es_entry - trail_stop, 2)
                if first_event is None:
                    first_event = "stop"

    # Determine T1/T2
    t1r = "WIN" if hit_10pt else ("LOSS" if (trail_exit_pnl is not None and trail_exit_pnl < 0) else "PENDING")
    if trail_exit_pnl is not None:
        t2r = "WIN" if trail_exit_pnl > 0 else "LOSS"
    elif hit_10pt:
        t2r = "TRAILING"
    else:
        t2r = "PENDING"

    # Overall
    if hit_10pt:
        if trail_exit_pnl is not None:
            overall_pnl = round((10.0 + trail_exit_pnl) / 2, 1)
        else:
            overall_pnl = 10.0
        overall = "WIN"
    elif trail_exit_pnl is not None and trail_exit_pnl > 0:
        overall = "WIN"
        overall_pnl = trail_exit_pnl
    elif trail_exit_pnl is not None and trail_exit_pnl <= 0:
        overall = "LOSS"
        overall_pnl = trail_exit_pnl
    else:
        overall = "OPEN"
        overall_pnl = max_profit

    dir_arrow = "^" if is_long else "v"
    print(f"#{t['id']} {dir_arrow} @ {es_entry} vol={t['abs_vol_ratio']} | T1={t1r} T2={t2r} trail_peak={trail_peak:.1f} trail_exit={trail_exit_pnl} | maxP={max_profit:.1f} maxL={max_loss:.1f} | OVERALL: {overall} pnl={overall_pnl:.1f} | {t['ts_et'].strftime('%H:%M')}")

c.close()
