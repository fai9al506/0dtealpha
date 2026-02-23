"""Deep analysis of conflicting trades — was the opposite signal a correct reversal?"""
import os, sys
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT id, setup_name, direction, grade, score,
               spot, target, outcome_target_level, outcome_stop_level,
               outcome_result, outcome_pnl, outcome_first_event,
               outcome_max_profit, outcome_max_loss, outcome_elapsed_min, ts
        FROM setup_log
        WHERE ts >= '2026-02-17'
        ORDER BY ts
    """)).mappings().all()

    # Also get SPX spot history for P&L-at-time-of-conflict estimation
    # We'll use the spot field from setup_log entries as proxy timestamps

trades = []
for r in rows:
    fired = r['ts'].replace(tzinfo=None)
    result = r['outcome_result']
    elapsed = r['outcome_elapsed_min'] or 0
    end = fired + timedelta(minutes=elapsed) if result else None
    dir_norm = 'long' if r['direction'].lower() in ('long', 'bullish') else 'short'

    trades.append({
        'id': r['id'], 'setup': r['setup_name'], 'direction': dir_norm,
        'grade': r['grade'], 'spot': r['spot'],
        'target_lvl': r['outcome_target_level'], 'stop_lvl': r['outcome_stop_level'],
        'fired': fired, 'end': end, 'result': result,
        'pnl': r['outcome_pnl'] or 0, 'max_profit': r['outcome_max_profit'] or 0,
        'max_loss': r['outcome_max_loss'] or 0, 'elapsed': elapsed,
    })

# Build spot timeline from all trade entries (spot at time of firing)
spot_timeline = [(t['fired'], t['spot']) for t in trades if t['spot']]
spot_timeline.sort()

def estimate_spot_at(ts):
    """Find the closest spot price to a given timestamp."""
    best = None
    best_diff = float('inf')
    for t, s in spot_timeline:
        diff = abs((t - ts).total_seconds())
        if diff < best_diff:
            best_diff = diff
            best = s
    return best

def estimate_unrealized_pnl(trade, at_time):
    """Estimate unrealized P&L of a trade at a given time."""
    spot_then = estimate_spot_at(at_time)
    if not spot_then or not trade['spot']:
        return None
    if trade['direction'] == 'long':
        return spot_then - trade['spot']
    else:
        return trade['spot'] - spot_then

# Find conflicts: opposite direction, overlapping time
conflicts = []
for i, a in enumerate(trades):
    for b in trades[i+1:]:
        if a['direction'] == b['direction']:
            continue
        a_start = a['fired']
        a_end = a['end'] or datetime(2026, 12, 31)
        b_start = b['fired']
        b_end = b['end'] or datetime(2026, 12, 31)
        if a_start < b_end and b_start < a_end:
            overlap_min = ((min(a_end, b_end) - max(a_start, b_start)).total_seconds()) / 60
            # Estimate trade A's unrealized P&L when trade B fires
            a_unrealized = estimate_unrealized_pnl(a, b_start)
            conflicts.append({
                'a': a, 'b': b, 'overlap_min': overlap_min,
                'a_unrealized_at_b': a_unrealized,
            })

# Group conflicts by unique first trade (A) to avoid duplicate analysis
# For each long-running trade A, list all opposing trades B that fired during it
by_first_trade = defaultdict(list)
for c in conflicts:
    by_first_trade[c['a']['id']].append(c)

print(f"=== DEEP CONFLICT ANALYSIS — {len(conflicts)} overlaps across {len(by_first_trade)} holding trades ===\n")

for a_id in sorted(by_first_trade.keys()):
    group = by_first_trade[a_id]
    a = group[0]['a']
    end_str = a['end'].strftime('%H:%M') if a['end'] else 'OPEN'
    day = a['fired'].strftime('%m/%d')

    print(f"{'='*80}")
    print(f"HOLDING: #{a['id']} {a['setup']:20s} {a['direction'].upper():5s} "
          f"| {day} {a['fired'].strftime('%H:%M')}-{end_str} UTC "
          f"| entry={a['spot']:.1f} | {a['result'] or 'OPEN':7s} {a['pnl']:+.1f}pts "
          f"| maxP={a['max_profit']:+.1f} maxL={a['max_loss']:+.1f} | {a['elapsed']:.0f}min")
    print(f"  target={a['target_lvl']} stop={a['stop_lvl']}")
    print()

    for c in sorted(group, key=lambda x: x['b']['fired']):
        b = c['b']
        b_end_str = b['end'].strftime('%H:%M') if b['end'] else 'OPEN'
        a_unreal = c['a_unrealized_at_b']
        a_unreal_str = f"{a_unreal:+.1f}pts" if a_unreal is not None else "?"

        # Was B's signal correct? (did B end as WIN?)
        b_correct = b['result'] == 'WIN'
        b_marker = "CORRECT" if b_correct else ("WRONG" if b['result'] == 'LOSS' else b['result'] or 'OPEN')

        # Would closing A at B's fire time have been better than A's final result?
        if a_unreal is not None and a['result']:
            close_early_better = a_unreal > a['pnl'] if a['result'] != 'WIN' else False
            # For WIN trades: was unrealized already positive? Could have locked profit
            if a['result'] == 'WIN' and a_unreal > 0:
                close_assessment = f"A was +{a_unreal:.1f} (final={a['pnl']:+.1f}) — early close {'worse' if a_unreal < a['pnl'] else 'similar/better'}"
            elif a['result'] == 'LOSS':
                close_assessment = f"A was {a_unreal:+.1f} (ended LOSS {a['pnl']:+.1f}) — early close {'SAVES money' if a_unreal > a['pnl'] else 'similar'}"
            else:
                close_assessment = f"A was {a_unreal:+.1f} at this point"
        else:
            close_assessment = ""

        print(f"  >> OPPOSING: #{b['id']} {b['setup']:20s} {b['direction'].upper():5s} "
              f"| {b['fired'].strftime('%H:%M')}-{b_end_str} | {b['result'] or 'OPEN':7s} {b['pnl']:+.1f}pts "
              f"| Signal: {b_marker}")
        print(f"     A's unrealized when B fired: {a_unreal_str} | overlap: {c['overlap_min']:.0f}min")
        if close_assessment:
            print(f"     {close_assessment}")
        print()

# Summary statistics
print(f"\n{'='*80}")
print("=== SUMMARY ===\n")

# Count by pattern
pattern_counts = defaultdict(lambda: {'total': 0, 'b_correct': 0, 'a_would_save': 0})
for c in conflicts:
    a, b = c['a'], c['b']
    pattern = f"{a['setup']}({a['direction']}) vs {b['setup']}({b['direction']})"
    p = pattern_counts[pattern]
    p['total'] += 1
    if b['result'] == 'WIN':
        p['b_correct'] += 1
    a_unreal = c['a_unrealized_at_b']
    if a_unreal is not None and a['result'] == 'LOSS' and a_unreal > a['pnl']:
        p['a_would_save'] += 1

print(f"{'Pattern':<55s} {'Count':>5s} {'B correct':>10s} {'Would save A':>12s}")
print('-' * 85)
for pattern in sorted(pattern_counts.keys(), key=lambda x: pattern_counts[x]['total'], reverse=True):
    p = pattern_counts[pattern]
    print(f"{pattern:<55s} {p['total']:>5d} {p['b_correct']:>10d} {p['a_would_save']:>12d}")

# Key question: when B fires opposite and is correct, would "close A + enter B" improve total?
print(f"\n\n=== KEY QUESTION: If we closed A and entered B on every conflict ===\n")
total_a_pnl_actual = 0
total_b_pnl_actual = 0
total_a_early_close = 0
seen_a = set()
seen_b = set()
for c in conflicts:
    a, b = c['a'], c['b']
    if a['id'] not in seen_a and a['result']:
        total_a_pnl_actual += a['pnl']
        seen_a.add(a['id'])
    if b['id'] not in seen_b and b['result']:
        total_b_pnl_actual += b['pnl']
        seen_b.add(b['id'])
    if c['a_unrealized_at_b'] is not None and a['id'] not in seen_b:  # avoid double-counting
        pass  # handled per-trade below

# Per unique A trade: compare actual A result vs closing when first B fires
print(f"{'Trade A':<45s} {'A actual':>10s} {'A@close':>10s} {'Delta':>10s} {'First B result':>15s}")
print('-' * 95)
total_actual = 0
total_early = 0
for a_id in sorted(by_first_trade.keys()):
    group = by_first_trade[a_id]
    a = group[0]['a']
    if not a['result']:
        continue
    first_b = min(group, key=lambda x: x['b']['fired'])
    a_actual = a['pnl']
    a_early = first_b['a_unrealized_at_b']
    if a_early is None:
        continue
    delta = a_early - a_actual
    b = first_b['b']
    b_res = f"{b['result'] or 'OPEN'} {b['pnl']:+.1f}" if b['result'] else 'OPEN'

    total_actual += a_actual
    total_early += a_early
    marker = " <<< SAVE" if delta > 2 else (" <<< WORSE" if delta < -2 else "")
    print(f"#{a['id']} {a['setup']:20s} {a['direction']:5s} {a['result']:7s} "
          f"{a_actual:>+10.1f} {a_early:>+10.1f} {delta:>+10.1f} {b_res:>15s}{marker}")

print('-' * 95)
print(f"{'TOTAL':<45s} {total_actual:>+10.1f} {total_early:>+10.1f} {total_early - total_actual:>+10.1f}")
