"""
Analyze macro trend filters to prevent fighting strong trends.
Key question: if alignment was +2/+3 earlier in session, should we block shorts?
"""
import os
from sqlalchemy import create_engine, text
from collections import defaultdict

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Pull all trades with alignment
rows = c.execute(text("""
    SELECT ts::date as d, to_char(ts, 'HH24:MI') as t,
           setup_name, direction, grade, outcome_result, outcome_pnl,
           greek_alignment, spot, score
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    ORDER BY ts
""")).fetchall()
c.close()

print(f"Total resolved trades: {len(rows)}")

# Build per-day alignment timeline
day_trades = defaultdict(list)
for r in rows:
    day_trades[str(r[0])].append({
        'time': r[1], 'setup': r[2], 'dir': r[3], 'grade': r[4],
        'outcome': r[5], 'pnl': float(r[6] or 0), 'align': r[7],
        'spot': float(r[8] or 0), 'score': float(r[9] or 0)
    })

# --- FILTER IDEA 1: Block shorts when session-peak alignment >= +2 (and vice versa) ---
# For each trade, check max alignment seen BEFORE this trade in same session
print("\n" + "="*80)
print("FILTER 1: Block shorts when session-max alignment >= +2 before trade")
print("         Block longs when session-min alignment <= -2 before trade")
print("="*80)

for threshold in [2, 3]:
    baseline_pnl = 0
    filtered_pnl = 0
    blocked_trades = []
    passed_trades = []

    for day, trades in sorted(day_trades.items()):
        alignments_seen = []
        for t in trades:
            # Check macro trend before this trade
            max_align = max(alignments_seen) if alignments_seen else 0
            min_align = min(alignments_seen) if alignments_seen else 0

            baseline_pnl += t['pnl']

            is_short = t['dir'] in ('short', 'bearish')
            is_long = t['dir'] in ('long', 'bullish')

            blocked = False
            if is_short and max_align >= threshold:
                blocked = True
            elif is_long and min_align <= -threshold:
                blocked = True

            if blocked:
                blocked_trades.append(t)
            else:
                filtered_pnl += t['pnl']
                passed_trades.append(t)

            if t['align'] is not None:
                alignments_seen.append(t['align'])

    blocked_w = sum(1 for t in blocked_trades if 'WIN' in str(t['outcome']))
    blocked_l = sum(1 for t in blocked_trades if 'LOSS' in str(t['outcome']))
    blocked_pnl = sum(t['pnl'] for t in blocked_trades)
    passed_w = sum(1 for t in passed_trades if 'WIN' in str(t['outcome']))
    passed_l = sum(1 for t in passed_trades if 'LOSS' in str(t['outcome']))

    print(f"\n  Threshold: alignment {'>=+' if threshold > 0 else ''}{threshold} / <=-{threshold}")
    print(f"  Baseline: {len(rows)} trades, PnL={baseline_pnl:+.1f}")
    print(f"  Passed:   {len(passed_trades)} trades, {passed_w}W/{passed_l}L, PnL={filtered_pnl:+.1f}")
    print(f"  Blocked:  {len(blocked_trades)} trades, {blocked_w}W/{blocked_l}L, PnL={blocked_pnl:+.1f}")
    print(f"  Improvement: {filtered_pnl - baseline_pnl:+.1f} pts")

# --- FILTER IDEA 2: Block counter-trend when CURRENT alignment is strong ---
print("\n" + "="*80)
print("FILTER 2: Block counter-trend at current alignment thresholds")
print("="*80)

for align_thresh in [1, 2, 3]:
    filtered_pnl = 0
    blocked = []

    for r in rows:
        is_short = r[3] in ('short', 'bearish')
        is_long = r[3] in ('long', 'bullish')
        pnl = float(r[6] or 0)
        align = r[7] if r[7] is not None else 0

        block = False
        if is_short and align >= align_thresh:
            block = True
        elif is_long and align <= -align_thresh:
            block = True

        if block:
            blocked.append({'pnl': pnl, 'outcome': r[5], 'setup': r[2], 'align': align})
        else:
            filtered_pnl += pnl

    blocked_w = sum(1 for t in blocked if 'WIN' in str(t['outcome']))
    blocked_l = sum(1 for t in blocked if 'LOSS' in str(t['outcome']))
    blocked_pnl = sum(t['pnl'] for t in blocked)

    print(f"\n  Block shorts when align >= +{align_thresh}, longs when align <= -{align_thresh}")
    print(f"  Blocked: {len(blocked)} trades, {blocked_w}W/{blocked_l}L, PnL={blocked_pnl:+.1f}")
    print(f"  Remaining PnL: {filtered_pnl:+.1f} (delta: {-blocked_pnl:+.1f})")

    # Break down blocked by setup
    setup_blocked = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0, 'n': 0})
    for t in blocked:
        s = setup_blocked[t['setup']]
        s['n'] += 1
        s['pnl'] += t['pnl']
        if 'WIN' in str(t['outcome']): s['w'] += 1
        if 'LOSS' in str(t['outcome']): s['l'] += 1
    for setup, s in sorted(setup_blocked.items(), key=lambda x: x[1]['pnl']):
        print(f"    {setup:20s}: {s['n']} trades, {s['w']}W/{s['l']}L, PnL={s['pnl']:+.1f}")

# --- FILTER IDEA 3: Per-setup analysis of counter-trend losses ---
print("\n" + "="*80)
print("FILTER 3: Per-setup counter-trend analysis (shorts at align>0, longs at align<0)")
print("="*80)

setup_stats = defaultdict(lambda: {'with_trend': {'w': 0, 'l': 0, 'pnl': 0},
                                     'counter_trend': {'w': 0, 'l': 0, 'pnl': 0}})
for r in rows:
    is_short = r[3] in ('short', 'bearish')
    is_long = r[3] in ('long', 'bullish')
    pnl = float(r[6] or 0)
    align = r[7] if r[7] is not None else 0

    counter = False
    if is_short and align > 0:
        counter = True
    elif is_long and align < 0:
        counter = True

    bucket = 'counter_trend' if counter else 'with_trend'
    s = setup_stats[r[2]][bucket]
    s['pnl'] += pnl
    if 'WIN' in str(r[5]): s['w'] += 1
    if 'LOSS' in str(r[5]): s['l'] += 1

for setup in sorted(setup_stats.keys()):
    s = setup_stats[setup]
    wt = s['with_trend']
    ct = s['counter_trend']
    wt_n = wt['w'] + wt['l']
    ct_n = ct['w'] + ct['l']
    wt_wr = wt['w']/wt_n*100 if wt_n else 0
    ct_wr = ct['w']/ct_n*100 if ct_n else 0
    print(f"\n  {setup}:")
    print(f"    With trend:    {wt_n:3d} trades, {wt_wr:.0f}% WR, PnL={wt['pnl']:+.1f}")
    print(f"    Counter trend: {ct_n:3d} trades, {ct_wr:.0f}% WR, PnL={ct['pnl']:+.1f}")

# --- FILTER IDEA 4: Alignment magnitude gate ---
# Only trade when |alignment| >= 1 in your direction
print("\n" + "="*80)
print("FILTER 4: Require alignment >= +1 for longs, <= -1 for shorts")
print("="*80)

filtered_pnl = 0
blocked_trades = []
for r in rows:
    is_short = r[3] in ('short', 'bearish')
    is_long = r[3] in ('long', 'bullish')
    pnl = float(r[6] or 0)
    align = r[7] if r[7] is not None else 0

    passes = False
    if is_long and align >= 1:
        passes = True
    elif is_short and align <= -1:
        passes = True

    if passes:
        filtered_pnl += pnl
    else:
        blocked_trades.append({'pnl': pnl, 'outcome': r[5], 'setup': r[2], 'dir': r[3], 'align': align})

blocked_w = sum(1 for t in blocked_trades if 'WIN' in str(t['outcome']))
blocked_l = sum(1 for t in blocked_trades if 'LOSS' in str(t['outcome']))
blocked_pnl = sum(t['pnl'] for t in blocked_trades)
total_pnl = sum(float(r[6] or 0) for r in rows)

print(f"  Baseline: {len(rows)} trades, PnL={total_pnl:+.1f}")
print(f"  Passed:   {len(rows)-len(blocked_trades)} trades, PnL={filtered_pnl:+.1f}")
print(f"  Blocked:  {len(blocked_trades)} trades, {blocked_w}W/{blocked_l}L, PnL={blocked_pnl:+.1f}")
print(f"  Improvement: {filtered_pnl - total_pnl:+.1f} pts")

# Show what today (Mar 9) would look like with Filter 4
print("\n--- Mar 9 with Filter 4 ---")
mar9 = day_trades.get('2026-03-09', [])
for t in mar9:
    is_short = t['dir'] in ('short', 'bearish')
    is_long = t['dir'] in ('long', 'bullish')
    align = t['align'] if t['align'] is not None else 0
    passes = (is_long and align >= 1) or (is_short and align <= -1)
    status = "PASS" if passes else "BLOCK"
    print(f"  {t['time']} {t['setup']:20s} {t['dir']:5s} align={align:+d} {status:5s} {t['outcome']:15s} {t['pnl']:+.1f}")

mar9_passed = sum(t['pnl'] for t in mar9 if
    ((t['dir'] in ('long','bullish') and (t['align'] or 0) >= 1) or
     (t['dir'] in ('short','bearish') and (t['align'] or 0) <= -1)))
mar9_total = sum(t['pnl'] for t in mar9)
print(f"  Today baseline: {mar9_total:+.1f}, with filter: {mar9_passed:+.1f}")
