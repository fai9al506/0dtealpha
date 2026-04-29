"""
Manual verification of Mar 26 SB2 Absorption trade outcomes
using REAL Rithmic ES range bar data.

KEY FINDINGS from earlier runs:
1. abs_details is NULL for all SB2 signals (no bar_idx stored)
2. Same bar_idx has DUPLICATE rows (overnight + market hours) in rithmic source
3. Must use signal TIME to find correct entry bar, not bar_idx

SB2 trail: BE@10, activation=20, gap=10, initial SL=8
Split-target: T1 flag at +10, final P&L = (10 + raw_pnl) / 2 if T1 hit
"""

import os
from datetime import datetime, time as dtime
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

# Step 1: Get all Mar 26 SB2 signals
print("=" * 120)
print("STEP 1: Mar 26 SB2 Absorption signals from setup_log")
print("=" * 120)

with engine.connect() as conn:
    signals = conn.execute(text("""
        SELECT id, setup_name, direction, score, grade,
               abs_es_price, abs_details,
               outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_max_loss,
               ts AT TIME ZONE 'America/New_York' as signal_ts
        FROM setup_log
        WHERE setup_name = 'SB2 Absorption'
          AND (ts AT TIME ZONE 'America/New_York')::date = '2026-03-26'
        ORDER BY ts
    """)).mappings().all()

print(f"Found {len(signals)} SB2 signals on Mar 26\n")
for s in signals:
    print(f"  ID={s['id']}  Dir={s['direction']:7s}  ES={s['abs_es_price']}  "
          f"Grade={s['grade']}  Score={s['score']:.0f}  "
          f"Time={s['signal_ts'].strftime('%H:%M:%S')}  "
          f"DB: {s['outcome_result']} {s['outcome_pnl']:+.1f}pts")

# Step 2: Get MARKET HOURS Rithmic range bars only (filter by time, not just source)
print("\n" + "=" * 120)
print("STEP 2: Mar 26 Rithmic range bars (MARKET HOURS ONLY)")
print("=" * 120)

with engine.connect() as conn:
    # Get ALL rithmic bars, then filter to market hours by timestamp
    all_bars = conn.execute(text("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               ts_start AT TIME ZONE 'America/New_York' as bar_start,
               ts_end AT TIME ZONE 'America/New_York' as bar_end
        FROM es_range_bars
        WHERE trade_date = '2026-03-26' AND source = 'rithmic'
        ORDER BY ts_end
    """)).mappings().all()

# Filter to market hours bars (after 9:25 ET to catch pre-open bars)
bars = [b for b in all_bars if b['bar_end'] and b['bar_end'].time() >= dtime(9, 25)]

print(f"Total rithmic bars: {len(all_bars)}, Market hours bars: {len(bars)}")
if bars:
    print(f"  First: end={bars[0]['bar_end'].strftime('%H:%M:%S')} "
          f"idx={bars[0]['bar_idx']} "
          f"OHLC={bars[0]['bar_open']}/{bars[0]['bar_high']}/{bars[0]['bar_low']}/{bars[0]['bar_close']}")
    print(f"  Last:  end={bars[-1]['bar_end'].strftime('%H:%M:%S')} "
          f"idx={bars[-1]['bar_idx']} "
          f"OHLC={bars[-1]['bar_open']}/{bars[-1]['bar_high']}/{bars[-1]['bar_low']}/{bars[-1]['bar_close']}")

# Step 3: Simulate each trade
print("\n" + "=" * 120)
print("STEP 3: Trail simulation using TIME-based bar selection")
print("=" * 120)

SL_PTS = 8
TRAIL_BE_TRIGGER = 10
TRAIL_ACTIVATION = 20
TRAIL_GAP = 10

results = []

for s in signals:
    sig_id = s['id']
    direction = s['direction']
    is_long = direction in ('long', 'bullish')
    entry = s['abs_es_price']
    signal_time = s['signal_ts']

    if entry is None:
        results.append({
            'id': sig_id, 'direction': direction, 'entry': None,
            'signal_time': signal_time,
            'db_result': s['outcome_result'], 'db_pnl': s['outcome_pnl'],
            'real_result': 'NO_ENTRY', 'real_pnl': 0, 'match': False
        })
        continue

    if is_long:
        stop_lvl = entry - SL_PTS
    else:
        stop_lvl = entry + SL_PTS

    print(f"\n  ID={sig_id}  {'LONG' if is_long else 'SHORT':5s}  Entry={entry:.2f}  "
          f"InitStop={stop_lvl:.2f}  Time={signal_time.strftime('%H:%M:%S')}")

    # Find bars AFTER signal time (by ts_end, since we need completed bars)
    max_fav = 0.0
    seen_high = entry
    seen_low = entry
    t1_hit = False
    result_type = None
    pnl = 0.0
    resolution_detail = ""

    for bar in bars:
        # Only process bars that ENDED after signal time
        if bar['bar_end'] <= signal_time:
            continue

        bh = bar['bar_high']
        bl = bar['bar_low']

        # Update seen extremes
        if bh is not None:
            seen_high = max(seen_high, bh)
        if bl is not None:
            seen_low = min(seen_low, bl)

        # Compute favorable excursion
        if is_long:
            fav = seen_high - entry
        else:
            fav = entry - seen_low
        if fav > max_fav:
            max_fav = fav

        # Track T1 hit
        if max_fav >= 10 and not t1_hit:
            t1_hit = True

        # Trail logic
        trail_lock = None
        if max_fav >= TRAIL_ACTIVATION:
            trail_lock = max_fav - TRAIL_GAP
        elif max_fav >= TRAIL_BE_TRIGGER:
            trail_lock = 0  # breakeven

        if trail_lock is not None:
            if is_long:
                new_stop = entry + trail_lock
                if new_stop > stop_lvl:
                    stop_lvl = new_stop
            else:
                new_stop = entry - trail_lock
                if new_stop < stop_lvl:
                    stop_lvl = new_stop

        # Check trailing stop hit
        if is_long:
            if seen_low <= stop_lvl:
                raw_pnl = stop_lvl - entry
                if t1_hit:
                    pnl = round((10.0 + raw_pnl) / 2, 1)
                    result_type = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "EXPIRED")
                else:
                    pnl = round(raw_pnl, 1)
                    result_type = "WIN" if stop_lvl >= entry else "LOSS"
                resolution_detail = (f"stop_hit @ end={bar['bar_end'].strftime('%H:%M:%S')} "
                    f"H={bh:.2f} L={bl:.2f} stop={stop_lvl:.2f} fav={max_fav:.1f}")
                break
        else:
            if seen_high >= stop_lvl:
                raw_pnl = entry - stop_lvl
                if t1_hit:
                    pnl = round((10.0 + raw_pnl) / 2, 1)
                    result_type = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "EXPIRED")
                else:
                    pnl = round(raw_pnl, 1)
                    result_type = "WIN" if stop_lvl <= entry else "LOSS"
                resolution_detail = (f"stop_hit @ end={bar['bar_end'].strftime('%H:%M:%S')} "
                    f"H={bh:.2f} L={bl:.2f} stop={stop_lvl:.2f} fav={max_fav:.1f}")
                break

        # Market close
        if bar['bar_end'].time() >= dtime(16, 0):
            break

    if result_type is None:
        result_type = 'EXPIRED'
        last_bar = None
        for bar in reversed(bars):
            if bar['bar_end'] > signal_time and bar['bar_end'].time() <= dtime(16, 5):
                last_bar = bar
                break
        if last_bar:
            close_px = last_bar['bar_close']
            raw_pnl = (close_px - entry) if is_long else (entry - close_px)
            if t1_hit:
                pnl = round((10.0 + raw_pnl) / 2, 1)
            else:
                pnl = round(raw_pnl, 1)
            resolution_detail = f"EXPIRED @ close={close_px:.2f}"

    print(f"    REAL: {result_type} {pnl:+.1f}pts  max_fav={max_fav:.1f} t1={'Y' if t1_hit else 'N'} "
          f"seen_H={seen_high:.2f} seen_L={seen_low:.2f}")
    print(f"    DB:   {s['outcome_result']} {s['outcome_pnl']:+.1f}pts  "
          f"maxP={s['outcome_max_profit']} maxL={s['outcome_max_loss']}")
    if resolution_detail:
        print(f"    Detail: {resolution_detail}")

    results.append({
        'id': sig_id, 'direction': direction, 'entry': entry,
        'signal_time': signal_time,
        'db_result': s['outcome_result'], 'db_pnl': s['outcome_pnl'],
        'db_max_profit': s['outcome_max_profit'], 'db_max_loss': s['outcome_max_loss'],
        'db_target': s['outcome_target_level'], 'db_stop': s['outcome_stop_level'],
        'real_result': result_type, 'real_pnl': pnl,
        'max_fav': max_fav, 't1_hit': t1_hit,
        'seen_high': seen_high, 'seen_low': seen_low,
        'match': s['outcome_result'] == result_type,
    })

# Step 4: Comparison table
print("\n" + "=" * 120)
print("STEP 4: COMPARISON TABLE")
print("=" * 120)

header = (f"{'ID':>6} | {'Dir':>7} | {'Entry':>9} | {'Time':>8} | "
          f"{'DB Res':>7} | {'DB PnL':>7} | "
          f"{'REAL Res':>8} | {'R PnL':>6} | {'MaxFav':>6} | {'T1':>3} | "
          f"{'seen_H':>8} | {'seen_L':>8} | {'Match':>5}")
print(header)
print("-" * len(header))

matches = 0
for r in results:
    entry_str = f"{r['entry']:.2f}" if r['entry'] else "N/A"
    time_str = r['signal_time'].strftime('%H:%M:%S')
    db_pnl_str = f"{r['db_pnl']:+.1f}" if r['db_pnl'] is not None else "N/A"
    m = "YES" if r['match'] else "NO"
    if r['match']:
        matches += 1

    print(f"{r['id']:>6} | {r['direction']:>7} | {entry_str:>9} | {time_str:>8} | "
          f"{r['db_result'] or 'None':>7} | {db_pnl_str:>7} | "
          f"{r['real_result']:>8} | {r['real_pnl']:>+6.1f} | "
          f"{r['max_fav']:>6.1f} | {'Y' if r.get('t1_hit') else 'N':>3} | "
          f"{r['seen_high']:>8.2f} | {r['seen_low']:>8.2f} | "
          f"{m:>5}")

print(f"\n{'=' * 80}")
print(f"MATCHES: {matches}/{len(results)}")
print(f"{'=' * 80}")

# P&L totals
db_total = sum(r['db_pnl'] for r in results if r['db_pnl'] is not None)
real_total = sum(r['real_pnl'] for r in results)
db_wins = sum(1 for r in results if r['db_result'] == 'WIN')
db_losses = sum(1 for r in results if r['db_result'] == 'LOSS')
real_wins = sum(1 for r in results if r['real_result'] == 'WIN')
real_losses = sum(1 for r in results if r['real_result'] == 'LOSS')
real_expired = sum(1 for r in results if r['real_result'] == 'EXPIRED')

print(f"\nDB:   {db_wins}W/{db_losses}L  total={db_total:+.1f} pts")
print(f"REAL: {real_wins}W/{real_losses}L/{real_expired}E  total={real_total:+.1f} pts")
print(f"Delta: {real_total - db_total:+.1f} pts")

# Mismatch details
mismatches = [r for r in results if not r['match']]
if mismatches:
    print(f"\n{'=' * 80}")
    print(f"MISMATCH DETAILS ({len(mismatches)} trades)")
    print(f"{'=' * 80}")
    for r in mismatches:
        print(f"\n  ID={r['id']} {r['direction'].upper()}")
        print(f"    Entry={r['entry']:.2f}  Time={r['signal_time'].strftime('%H:%M:%S')}")
        print(f"    DB:   result={r['db_result']}, pnl={r['db_pnl']}, "
              f"maxP={r['db_max_profit']}, maxL={r['db_max_loss']}")
        print(f"    REAL: result={r['real_result']}, pnl={r['real_pnl']}, "
              f"max_fav={r['max_fav']:.1f}, seen_H={r['seen_high']:.2f}, seen_L={r['seen_low']:.2f}")
