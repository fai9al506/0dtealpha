"""Analyze swing pair staleness across all 31 trades with abs_details."""
import json
from collections import defaultdict

data = json.load(open('abs_data.json'))

print("=" * 120)
print("SWING PAIR STALENESS ANALYSIS — All ES Absorption trades with swing data")
print("=" * 120)

print(f"\n{'ID':>4} | {'Dir':>8} | {'Pattern':>22} | {'Res':>7} | {'PnL':>6} | {'Sw2>Trig':>9} | {'Sw1>Sw2':>8} | {'Sw1>Trig':>9} | {'Sw Price vs Entry':>20} | Notes")
print("-" * 150)

# Track staleness vs outcome
stale_bins = defaultdict(lambda: {'w': 0, 'l': 0, 'e': 0, 'pnl': 0.0, 'trades': []})

for t in data:
    abs_d = t.get('abs_details', {})
    best = abs_d.get('best_swing', {})
    if not best:
        continue

    ref_sw = best.get('ref_swing', {})
    rec_sw = best.get('swing', {})
    trigger_idx = abs_d.get('bar_idx')

    if not trigger_idx or not ref_sw.get('bar_idx') or not rec_sw.get('bar_idx'):
        continue

    sw1_idx = ref_sw['bar_idx']
    sw2_idx = rec_sw['bar_idx']
    sw2_to_trig = trigger_idx - sw2_idx
    sw1_to_sw2 = sw2_idx - sw1_idx
    sw1_to_trig = trigger_idx - sw1_idx

    # How far is price from the swing pair?
    sw_type = ref_sw.get('type', '')
    pattern = abs_d.get('pattern', '')
    direction = t['direction']
    es_price = t.get('es_price') or 0

    if sw_type in ('H', 'L'):
        sw2_price = rec_sw.get('price', 0)
        price_dist = abs(es_price - sw2_price) if es_price and sw2_price else 0
    else:
        price_dist = 0

    # Check if divergence already played out
    # For bearish (buy_exhaustion/absorption): swing highs, if current price < swing high → move happened
    # For bullish (sell_exhaustion/absorption): swing lows, if current price > swing low → move happened
    already_resolved = False
    note = ''
    if direction == 'bearish' and sw_type == 'H':
        sw2_price = rec_sw.get('price', 0)
        if es_price and sw2_price and es_price < sw2_price - 5:
            already_resolved = True
            note = f"STALE: price {es_price:.0f} already {sw2_price - es_price:.0f}pts below swing H {sw2_price:.0f}"
    elif direction == 'bullish' and sw_type == 'L':
        sw2_price = rec_sw.get('price', 0)
        if es_price and sw2_price and es_price > sw2_price + 5:
            already_resolved = True
            note = f"STALE: price {es_price:.0f} already {es_price - sw2_price:.0f}pts above swing L {sw2_price:.0f}"
    elif direction == 'bullish' and sw_type == 'Z':
        note = 'zone-revisit'
    elif direction == 'bearish' and sw_type == 'Z':
        note = 'zone-revisit'

    result = t['result'] or 'OPEN'
    pnl = t['pnl']

    # Classify staleness
    if sw2_to_trig <= 10:
        bucket = 'fresh (0-10)'
    elif sw2_to_trig <= 20:
        bucket = 'moderate (11-20)'
    elif sw2_to_trig <= 30:
        bucket = 'stale (21-30)'
    else:
        bucket = 'very stale (31+)'

    stale_bins[bucket]['pnl'] += pnl
    stale_bins[bucket]['trades'].append(t['id'])
    if result == 'WIN': stale_bins[bucket]['w'] += 1
    elif result == 'LOSS': stale_bins[bucket]['l'] += 1
    else: stale_bins[bucket]['e'] += 1

    flag = ' *** RESOLVED ***' if already_resolved else ''
    print(f"#{t['id']:>4} | {direction:>8} | {pattern:>22} | {result:>7} | {pnl:>+6.1f} | {sw2_to_trig:>5} bars | {sw1_to_sw2:>5} bars | {sw1_to_trig:>5} bars | dist={price_dist:>5.1f} | {note}{flag}")

# Summary by staleness bucket
print(f"\n{'='*80}")
print("STALENESS vs OUTCOME")
print(f"{'='*80}\n")
print(f"{'Bucket':<25} | {'Trades':>6} | {'W/L/E':>10} | {'WR':>6} | {'PnL':>8} | {'Avg':>6}")
print("-" * 75)
for bucket in ['fresh (0-10)', 'moderate (11-20)', 'stale (21-30)', 'very stale (31+)']:
    s = stale_bins[bucket]
    n = s['w'] + s['l'] + s['e']
    if n == 0: continue
    wl = s['w'] + s['l']
    wr = round(100 * s['w'] / wl, 1) if wl else 0
    avg = s['pnl'] / n if n else 0
    print(f"{bucket:<25} | {n:>6} | {s['w']}W/{s['l']}L/{s['e']}E | {wr:>5.1f}% | {s['pnl']:>+8.1f} | {avg:>+6.1f}")

# Check how many already-resolved divergences exist and their outcomes
print(f"\n{'='*80}")
print("ALREADY-RESOLVED DIVERGENCES (price moved away from swing)")
print(f"{'='*80}\n")
resolved_w = resolved_l = resolved_e = resolved_pnl = 0
fresh_w = fresh_l = fresh_e = fresh_pnl = 0
for t in data:
    abs_d = t.get('abs_details', {})
    best = abs_d.get('best_swing', {})
    if not best: continue
    ref_sw = best.get('ref_swing', {})
    rec_sw = best.get('swing', {})
    trigger_idx = abs_d.get('bar_idx')
    if not trigger_idx: continue

    sw_type = ref_sw.get('type', '')
    direction = t['direction']
    es_price = t.get('es_price') or 0
    sw2_price = rec_sw.get('price', 0)
    result = t['result'] or 'OPEN'
    pnl = t['pnl']

    resolved = False
    if direction == 'bearish' and sw_type == 'H' and es_price and sw2_price:
        if es_price < sw2_price - 5: resolved = True
    elif direction == 'bullish' and sw_type == 'L' and es_price and sw2_price:
        if es_price > sw2_price + 5: resolved = True

    if resolved:
        resolved_pnl += pnl
        if result == 'WIN': resolved_w += 1
        elif result == 'LOSS': resolved_l += 1
        else: resolved_e += 1
    elif sw_type in ('H', 'L'):  # only count swing-to-swing (not zones)
        fresh_pnl += pnl
        if result == 'WIN': fresh_w += 1
        elif result == 'LOSS': fresh_l += 1
        else: fresh_e += 1

r_wl = resolved_w + resolved_l
f_wl = fresh_w + fresh_l
print(f"RESOLVED (stale): {resolved_w + resolved_l + resolved_e} trades | {resolved_w}W/{resolved_l}L/{resolved_e}E | WR={round(100*resolved_w/r_wl,1) if r_wl else 0}% | PnL={resolved_pnl:+.1f}")
print(f"FRESH (active):   {fresh_w + fresh_l + fresh_e} trades | {fresh_w}W/{fresh_l}L/{fresh_e}E | WR={round(100*fresh_w/f_wl,1) if f_wl else 0}% | PnL={fresh_pnl:+.1f}")

print(f"\n{'='*80}")
print("CONCLUSION")
print(f"{'='*80}")
