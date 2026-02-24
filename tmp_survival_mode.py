"""Survival mode analysis: tighter SL + BE stop simulations
Run via: railway run --service 0dtealpha -- python tmp_survival_mode.py"""
from sqlalchemy import create_engine, text
import os, sys

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

rows = c.execute(text("""
SELECT s.id, s.ts AT TIME ZONE 'America/New_York' as t,
       s.setup_name, s.direction, s.paradigm,
       s.outcome_result, s.outcome_pnl, s.outcome_max_profit, s.outcome_max_loss,
       s.outcome_first_event,
       EXTRACT(HOUR FROM s.ts AT TIME ZONE 'America/New_York') as hour,
       (SELECT payload->'statistics'->'spot_vol_beta'->>'correlation'
        FROM volland_snapshots v WHERE v.ts <= s.ts AND payload->'statistics' IS NOT NULL
        ORDER BY v.ts DESC LIMIT 1) as svb
FROM setup_log s
WHERE s.outcome_result IS NOT NULL
ORDER BY s.id
""")).fetchall()

def cat(p):
    if not p: return 'OTHER'
    if 'MESSY' in p or 'EXTREME' in p or 'SIDIAL' in p: return 'MESSY/SIDIAL'
    if 'BOFA' in p or 'BofA' in p: return 'BOFA'
    if 'AG' in p: return 'AG'
    if 'GEX' in p: return 'GEX'
    return 'OTHER'

trades = []
for r in rows:
    svb_val = float(r[11]) if r[11] else None
    trades.append({
        'id': r[0], 'time': r[1], 'setup': r[2], 'dir': r[3],
        'paradigm': r[4] or '-', 'result': r[5], 'pnl': r[6],
        'maxP': r[7], 'maxL': r[8], 'first_event': r[9],
        'hour': int(r[10]), 'svb': svb_val
    })

def passes_filter(t):
    if t['setup'] == 'GEX Long': return False
    if t['setup'] == 'DD Exhaustion':
        if t['hour'] >= 14: return False
        if cat(t['paradigm']) == 'BOFA': return False
        if t['svb'] is not None and t['svb'] >= 1.0: return False
    return True

all_trades = trades
filtered_trades = [t for t in trades if passes_filter(t)]

def simulate(bucket, be_trigger=None, max_sl=None):
    """Simulate BE stop and/or tighter SL on a set of trades.

    be_trigger: if maxP >= this, stop moves to breakeven (0)
    max_sl: cap the stop loss at this many pts (e.g. 12 means max loss = -12)

    Returns sim results for each trade.
    """
    sim_w = 0; sim_l = 0; sim_be = 0; sim_e = 0; sim_pnl = 0
    sim_trades = []

    for t in bucket:
        maxP = t['maxP'] if t['maxP'] is not None else 0
        maxL = t['maxL'] if t['maxL'] is not None else 0  # maxL is negative
        orig_pnl = t['pnl']
        orig_result = t['result']

        new_pnl = orig_pnl
        new_result = orig_result

        if orig_result == 'EXPIRED':
            sim_e += 1
            sim_pnl += orig_pnl
            continue

        # --- TIGHTER SL ---
        # If we have a max SL and the trade's adverse excursion exceeded it,
        # the tighter stop would have been hit.
        if max_sl is not None and maxL is not None and abs(maxL) >= max_sl:
            if orig_result == 'LOSS':
                # Loss is capped at the tighter SL
                new_pnl = -max_sl
                new_result = 'LOSS'
            elif orig_result == 'WIN':
                # WIN but adverse excursion hit the tighter stop FIRST?
                # We can't be 100% sure of path, but if maxL >= max_sl,
                # the stop would have been hit at some point.
                # Conservative: count as stopped out at -max_sl
                new_pnl = -max_sl
                new_result = 'LOSS'

        # --- BE STOP ---
        # If maxP >= be_trigger, the stop moved to breakeven.
        # Any trade that had maxP >= trigger but ended as LOSS -> now BE ($0)
        # WINs stay as wins (they reached target after reaching BE trigger)
        if be_trigger is not None and maxP >= be_trigger:
            if new_result == 'LOSS':
                # Stop was at BE, so instead of losing, we exit at $0
                new_pnl = 0
                new_result = 'BE'

        if new_result == 'WIN':
            sim_w += 1
        elif new_result == 'LOSS':
            sim_l += 1
        elif new_result == 'BE':
            sim_be += 1

        sim_pnl += new_pnl
        sim_trades.append({**t, 'sim_pnl': new_pnl, 'sim_result': new_result})

    total_decisions = sim_w + sim_l
    wr = sim_w / total_decisions * 100 if total_decisions else 0

    # Max drawdown
    running = 0; peak = 0; max_dd = 0
    for t in sorted(sim_trades, key=lambda x: x['time']):
        running += t['sim_pnl']
        if running > peak: peak = running
        dd = peak - running
        if dd > max_dd: max_dd = dd

    # Max consecutive losses
    streak = 0; max_streak = 0
    for t in sorted(sim_trades, key=lambda x: x['time']):
        if t['sim_result'] == 'LOSS':
            streak += 1
            if streak > max_streak: max_streak = streak
        else:
            streak = 0

    return {
        'trades': len(bucket), 'w': sim_w, 'l': sim_l, 'be': sim_be, 'e': sim_e,
        'wr': wr, 'pnl': sim_pnl, 'max_dd': max_dd, 'max_streak': max_streak,
        'detail': sim_trades
    }


# ============================================================
# SECTION 1: BE STOP SIMULATIONS (no SL change)
# ============================================================
print("\n" + "="*95)
print("BREAKEVEN STOP SIMULATIONS")
print("="*95)
print("Rule: when trade reaches +X pts profit, move stop to $0 (breakeven)")
print("If trade later reverses, exit at $0 instead of full loss")

for label, bucket in [("ALL TRADES (no filter)", all_trades), ("FILTERED TRADES", filtered_trades)]:
    print("\n--- %s (%d trades) ---" % (label, len(bucket)))

    # Actual baseline
    actual = simulate(bucket)
    print("\n  %-22s  %3d trades  %2dW/%2dL/%dBE/%dE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f  MaxL-streak=%d" % (
        "ACTUAL (no BE)", actual['trades'], actual['w'], actual['l'], actual['be'], actual['e'],
        actual['wr'], actual['pnl'], actual['max_dd'], actual['max_streak']))

    for be in [5, 7, 10, 12, 15]:
        s = simulate(bucket, be_trigger=be)
        delta = s['pnl'] - actual['pnl']
        print("  %-22s  %3d trades  %2dW/%2dL/%dBE/%dE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f  MaxL-streak=%d  (%+.1f)" % (
            "BE @ +%d pts" % be, s['trades'], s['w'], s['l'], s['be'], s['e'],
            s['wr'], s['pnl'], s['max_dd'], s['max_streak'], delta))

# Show which losses would be saved by BE@7
print("\n--- LOSSES SAVED BY BE@+7 (all trades) ---")
for t in all_trades:
    if t['result'] == 'LOSS' and t['maxP'] is not None and t['maxP'] >= 7:
        print("  #%-4d %-18s %5s  %-15s  maxP=%+5.1f  loss=%+6.1f  -> saved!" % (
            t['id'], t['setup'], t['dir'], t['paradigm'], t['maxP'], t['pnl']))


# ============================================================
# SECTION 2: TIGHTER SL SIMULATIONS (no BE)
# ============================================================
print("\n\n" + "="*95)
print("TIGHTER STOP LOSS SIMULATIONS")
print("="*95)
print("Rule: cap maximum loss per trade at X pts (regardless of setup's original stop)")
print("WARNING: tighter SL can also stop out eventual winners that dipped first")

for label, bucket in [("ALL TRADES (no filter)", all_trades), ("FILTERED TRADES", filtered_trades)]:
    print("\n--- %s (%d trades) ---" % (label, len(bucket)))

    actual = simulate(bucket)
    print("\n  %-22s  %3d trades  %2dW/%2dL/%dBE/%dE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f" % (
        "ACTUAL (no cap)", actual['trades'], actual['w'], actual['l'], actual['be'], actual['e'],
        actual['wr'], actual['pnl'], actual['max_dd']))

    for sl in [8, 10, 12, 15]:
        s = simulate(bucket, max_sl=sl)
        delta = s['pnl'] - actual['pnl']
        # Count how many wins got stopped out
        wins_stopped = sum(1 for t in s['detail'] if t['result'] == 'WIN' and t['sim_result'] == 'LOSS')
        print("  %-22s  %3d trades  %2dW/%2dL/%dBE/%dE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f  (%+.1f)  [%d wins stopped out]" % (
            "Max SL = %d pts" % sl, s['trades'], s['w'], s['l'], s['be'], s['e'],
            s['wr'], s['pnl'], s['max_dd'], delta, wins_stopped))

# Show wins that would be stopped out by SL=12
print("\n--- WINS STOPPED OUT BY SL=12 (all trades) ---")
for t in all_trades:
    if t['result'] == 'WIN' and t['maxL'] is not None and abs(t['maxL']) >= 12:
        print("  #%-4d %-18s %5s  maxL=%+6.1f  then WON %+6.1f  -> stopped at -12.0" % (
            t['id'], t['setup'], t['dir'], t['maxL'], t['pnl']))


# ============================================================
# SECTION 3: COMBINED — BE + TIGHTER SL
# ============================================================
print("\n\n" + "="*95)
print("COMBINED: BE STOP + TIGHTER SL (SURVIVAL MODE)")
print("="*95)
print("Best of both: cap downside AND move to BE on favorable move")

for label, bucket in [("ALL TRADES (no filter)", all_trades), ("FILTERED TRADES", filtered_trades)]:
    print("\n--- %s (%d trades) ---" % (label, len(bucket)))

    actual = simulate(bucket)
    print("\n  %-30s  %2dW/%2dL/%dBE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f  MaxL-streak=%d" % (
        "ACTUAL", actual['w'], actual['l'], actual['be'],
        actual['wr'], actual['pnl'], actual['max_dd'], actual['max_streak']))

    combos = [
        (5, 15, "BE@5 + SL=15"),
        (5, 12, "BE@5 + SL=12"),
        (5, 10, "BE@5 + SL=10"),
        (7, 15, "BE@7 + SL=15"),
        (7, 12, "BE@7 + SL=12"),
        (7, 10, "BE@7 + SL=10"),
        (10, 15, "BE@10 + SL=15"),
        (10, 12, "BE@10 + SL=12"),
    ]

    for be, sl, combo_label in combos:
        s = simulate(bucket, be_trigger=be, max_sl=sl)
        delta = s['pnl'] - actual['pnl']
        wins_stopped = sum(1 for t in s['detail'] if t['result'] == 'WIN' and t['sim_result'] == 'LOSS')
        print("  %-30s  %2dW/%2dL/%dBE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f  MaxL-streak=%d  (%+.1f) [%d wins killed]" % (
            combo_label, s['w'], s['l'], s['be'],
            s['wr'], s['pnl'], s['max_dd'], s['max_streak'], delta, wins_stopped))


# ============================================================
# SECTION 4: PER-SETUP BREAKDOWN for best combos
# ============================================================
print("\n\n" + "="*95)
print("PER-SETUP BREAKDOWN — KEY SCENARIOS")
print("="*95)

setups = sorted(set(t['setup'] for t in all_trades))
scenarios = [
    ("ACTUAL", None, None),
    ("BE@7", 7, None),
    ("BE@5 + SL=12", 5, 12),
    ("BE@7 + SL=15", 7, 15),
]

for setup in setups:
    bucket = [t for t in all_trades if t['setup'] == setup]
    if len(bucket) < 3:
        continue

    print("\n  %s (%d trades):" % (setup, len(bucket)))
    for slabel, be, sl in scenarios:
        s = simulate(bucket, be_trigger=be, max_sl=sl)
        wins_stopped = sum(1 for t in s['detail'] if t['result'] == 'WIN' and t['sim_result'] == 'LOSS')
        extra = "  [%d wins killed]" % wins_stopped if wins_stopped else ""
        print("    %-20s  %2dW/%2dL/%dBE/%dE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f%s" % (
            slabel, s['w'], s['l'], s['be'], s['e'],
            s['wr'], s['pnl'], s['max_dd'], extra))


# ============================================================
# SECTION 5: ULTIMATE COMBO — Filters + Survival
# ============================================================
print("\n\n" + "="*95)
print("ULTIMATE COMBO: FILTERS + SURVIVAL MODE")
print("="*95)
print("Proposed filters (disable GEX, DD<14:00/noBofa/SVB<1) + BE/SL combos")

actual_all = simulate(all_trades)
actual_filt = simulate(filtered_trades)

print("\n  %-40s  %3d trades  %2dW/%2dL/%dBE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f" % (
    "Baseline (all, no filter, no BE/SL)",
    actual_all['trades'], actual_all['w'], actual_all['l'], actual_all['be'],
    actual_all['wr'], actual_all['pnl'], actual_all['max_dd']))

print("  %-40s  %3d trades  %2dW/%2dL/%dBE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f" % (
    "Filters only (no BE/SL)",
    actual_filt['trades'], actual_filt['w'], actual_filt['l'], actual_filt['be'],
    actual_filt['wr'], actual_filt['pnl'], actual_filt['max_dd']))

best_combos = [
    ("Filters + BE@5", 5, None),
    ("Filters + BE@7", 7, None),
    ("Filters + BE@5 + SL=12", 5, 12),
    ("Filters + BE@7 + SL=12", 7, 12),
    ("Filters + BE@7 + SL=15", 7, 15),
    ("Filters + BE@10 + SL=15", 10, 15),
]

for combo_label, be, sl in best_combos:
    s = simulate(filtered_trades, be_trigger=be, max_sl=sl)
    delta = s['pnl'] - actual_all['pnl']
    wins_stopped = sum(1 for t in s['detail'] if t['result'] == 'WIN' and t['sim_result'] == 'LOSS')
    print("  %-40s  %3d trades  %2dW/%2dL/%dBE  WR=%4.0f%%  PnL=%+8.1f  MaxDD=%5.1f  (%+.1f vs baseline) [%d wins killed]" % (
        combo_label, s['trades'], s['w'], s['l'], s['be'],
        s['wr'], s['pnl'], s['max_dd'], delta, wins_stopped))

sys.stdout.flush()
c.close()
