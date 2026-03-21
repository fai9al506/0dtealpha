"""VIX vs Trade Outcomes Analysis — from API data."""
import json, sys
from collections import defaultdict

with open('gex_analysis_tmp.json') as f:
    data = json.load(f)

outcomes = data['setup_outcomes']
print(f"Total records: {len(outcomes)}", flush=True)

# Filter to trades with VIX data and WIN/LOSS
trades = [r for r in outcomes if r.get('vix') is not None and r['result'] in ('WIN', 'LOSS')]
print(f"Trades with VIX + WIN/LOSS: {len(trades)}", flush=True)

expired = [r for r in outcomes if r.get('vix') is not None and r['result'] == 'EXPIRED']
print(f"EXPIRED with VIX: {len(expired)}", flush=True)

no_vix = [r for r in outcomes if r.get('vix') is None]
no_vix_dates = sorted(set(r['date'] for r in no_vix))
print(f"Trades without VIX data: {len(no_vix)} (dates: {no_vix_dates})", flush=True)

vix_vals = [r['vix'] for r in trades]
print(f"VIX range: {min(vix_vals):.2f} - {max(vix_vals):.2f}", flush=True)

ov_count = sum(1 for r in trades if r.get('overvix') is not None)
print(f"Trades with overvix: {ov_count} / {len(trades)}", flush=True)

# ====================================================================
print()
print("=" * 80)
print("SECTION A: VIX BUCKET ANALYSIS -- LONG TRADES")
print("=" * 80)

longs = [t for t in trades if t['direction'] in ('long', 'bullish')]
print(f"Total long trades with VIX: {len(longs)}")

buckets = [(0, 18, "<18"), (18, 20, "18-20"), (20, 22, "20-22"), (22, 24, "22-24"), (24, 26, "24-26"), (26, 100, ">26")]

for lo, hi, label in buckets:
    bucket = [t for t in longs if lo <= t['vix'] < hi]
    if not bucket:
        print(f"\n  VIX {label}: 0 trades")
        continue
    wins = sum(1 for t in bucket if t['result'] == 'WIN')
    losses = len(bucket) - wins
    pnl = sum(t['pnl'] for t in bucket)
    wr = wins / len(bucket) * 100
    avg_pnl = pnl / len(bucket)
    print(f"\n  VIX {label}: {len(bucket)} trades | WR={wr:.1f}% ({wins}W/{losses}L) | PnL={pnl:+.1f} pts | Avg={avg_pnl:+.2f}")

    # Per-setup breakdown
    setup_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0})
    for t in bucket:
        k = t['setup_name']
        if t['result'] == 'WIN':
            setup_stats[k]['w'] += 1
        else:
            setup_stats[k]['l'] += 1
        setup_stats[k]['pnl'] += t['pnl']
    for s in sorted(setup_stats.keys()):
        d = setup_stats[s]
        n = d['w'] + d['l']
        print(f"    {s}: {n}t {d['w']}W/{d['l']}L ({d['w']/n*100:.0f}%WR) {d['pnl']:+.1f}pts")


# ====================================================================
print()
print("=" * 80)
print("SECTION B: VIX BUCKET ANALYSIS -- SHORT TRADES")
print("=" * 80)

shorts = [t for t in trades if t['direction'] in ('short', 'bearish')]
print(f"Total short trades with VIX: {len(shorts)}")

for lo, hi, label in buckets:
    bucket = [t for t in shorts if lo <= t['vix'] < hi]
    if not bucket:
        print(f"\n  VIX {label}: 0 trades")
        continue
    wins = sum(1 for t in bucket if t['result'] == 'WIN')
    losses = len(bucket) - wins
    pnl = sum(t['pnl'] for t in bucket)
    wr = wins / len(bucket) * 100
    avg_pnl = pnl / len(bucket)
    print(f"\n  VIX {label}: {len(bucket)} trades | WR={wr:.1f}% ({wins}W/{losses}L) | PnL={pnl:+.1f} pts | Avg={avg_pnl:+.2f}")

    setup_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0})
    for t in bucket:
        k = t['setup_name']
        if t['result'] == 'WIN':
            setup_stats[k]['w'] += 1
        else:
            setup_stats[k]['l'] += 1
        setup_stats[k]['pnl'] += t['pnl']
    for s in sorted(setup_stats.keys()):
        d = setup_stats[s]
        n = d['w'] + d['l']
        print(f"    {s}: {n}t {d['w']}W/{d['l']}L ({d['w']/n*100:.0f}%WR) {d['pnl']:+.1f}pts")


# ====================================================================
print()
print("=" * 80)
print("SECTION C: OPTIMAL VIX THRESHOLD FOR LONG TRADES")
print("=" * 80)
print("Testing: block longs when VIX > threshold")
print()

base_w = sum(1 for t in longs if t['result'] == 'WIN')
base_pnl = sum(t['pnl'] for t in longs)
header = f"{'Threshold':<14} {'Trades':<8} {'Blocked':<9} {'WR':<8} {'PnL':<12} {'AvgPnL':<10} {'Blocked Detail'}"
print(header)
print(f"{'No filter':<14} {len(longs):<8} {0:<9} {base_w/len(longs)*100:.1f}%   {base_pnl:+.1f}     {base_pnl/len(longs):+.2f}      n/a")

for thresh in [18, 19, 20, 21, 22, 23, 24, 25, 26]:
    kept = [t for t in longs if t['vix'] <= thresh]
    blocked = [t for t in longs if t['vix'] > thresh]
    if not kept:
        print(f"VIX<={thresh:<7} {0:<8} {len(blocked):<9} n/a      n/a          n/a        n/a")
        continue
    w = sum(1 for t in kept if t['result'] == 'WIN')
    pnl = sum(t['pnl'] for t in kept)
    bl_pnl = sum(t['pnl'] for t in blocked)
    bl_w = sum(1 for t in blocked if t['result'] == 'WIN')
    bl_wr = f"{bl_w/len(blocked)*100:.1f}%" if blocked else "n/a"
    print(f"VIX<={thresh:<7} {len(kept):<8} {len(blocked):<9} {w/len(kept)*100:.1f}%   {pnl:+.1f}     {pnl/len(kept):+.2f}      {len(blocked)}t {bl_pnl:+.1f}pts ({bl_wr}WR)")

# With overvix override
print()
print("--- With Overvix Override (block longs VIX > threshold UNLESS overvix >= +2) ---")
print()
header2 = f"{'Threshold':<14} {'Trades':<8} {'Blocked':<9} {'WR':<8} {'PnL':<12} {'Blocked Detail'}"
print(header2)

for thresh in [18, 19, 20, 21, 22, 23, 24, 25, 26]:
    kept = [t for t in longs if t['vix'] <= thresh or (t.get('overvix') is not None and t['overvix'] >= 2)]
    blocked = [t for t in longs if t['vix'] > thresh and (t.get('overvix') is None or t['overvix'] < 2)]
    if not kept:
        print(f"VIX<={thresh:<7} {0:<8} {len(blocked):<9} n/a      n/a          n/a")
        continue
    w = sum(1 for t in kept if t['result'] == 'WIN')
    pnl = sum(t['pnl'] for t in kept)
    bl_pnl = sum(t['pnl'] for t in blocked)
    print(f"VIX<={thresh:<7} {len(kept):<8} {len(blocked):<9} {w/len(kept)*100:.1f}%   {pnl:+.1f}     {len(blocked)}t {bl_pnl:+.1f}pts blocked")


# ====================================================================
print()
print("=" * 80)
print("SECTION D: OVERVIX ANALYSIS")
print("=" * 80)

ov_trades = [t for t in trades if t.get('overvix') is not None]
print(f"Trades with overvix data: {len(ov_trades)} / {len(trades)}")

if ov_trades:
    ov_vals = [t['overvix'] for t in ov_trades]
    print(f"Overvix range: {min(ov_vals):.2f} to {max(ov_vals):.2f}")
    print(f"Overvix mean: {sum(ov_vals)/len(ov_vals):.2f}")

# D1: Long trades at VIX > 20
print()
print("--- D1: Long trades at VIX > 20, split by overvix ---")
longs_high_vix = [t for t in longs if t['vix'] > 20]
print(f"Total long trades VIX > 20: {len(longs_high_vix)}")

if longs_high_vix:
    ov_high = [t for t in longs_high_vix if t.get('overvix') is not None and t['overvix'] >= 2]
    ov_low = [t for t in longs_high_vix if t.get('overvix') is None or t['overvix'] < 2]

    if ov_high:
        w = sum(1 for t in ov_high if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_high)
        print(f"  Overvix >= +2: {len(ov_high)}t | WR={w/len(ov_high)*100:.1f}% | PnL={pnl:+.1f}")
        for t in sorted(ov_high, key=lambda x: x['date']):
            print(f"    {t['date']} {t['setup_name']:20s} VIX={t['vix']:.1f} OV={t['overvix']:+.1f} align={t['alignment']} -> {t['result']} {t['pnl']:+.1f}")
    else:
        print("  Overvix >= +2: 0 trades")

    if ov_low:
        w = sum(1 for t in ov_low if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_low)
        print(f"  Overvix < +2 (or null): {len(ov_low)}t | WR={w/len(ov_low)*100:.1f}% | PnL={pnl:+.1f}")
        for t in sorted(ov_low, key=lambda x: x['date']):
            ov_str = f"{t['overvix']:+.1f}" if t.get('overvix') is not None else "null"
            print(f"    {t['date']} {t['setup_name']:20s} VIX={t['vix']:.1f} OV={ov_str} align={t['alignment']} -> {t['result']} {t['pnl']:+.1f}")
    else:
        print("  Overvix < +2: 0 trades")

# D2: Overvix buckets for longs
print()
print("--- D2: Overvix buckets (LONG trades, all VIX levels) ---")
ov_longs = [t for t in longs if t.get('overvix') is not None]
print(f"Long trades with overvix: {len(ov_longs)}")

ov_buckets = [(-100, -2, "< -2"), (-2, 0, "-2 to 0"), (0, 2, "0 to +2"), (2, 100, ">= +2")]
for lo, hi, label in ov_buckets:
    bucket = [t for t in ov_longs if lo <= t['overvix'] < hi]
    if not bucket:
        print(f"  Overvix {label}: 0 trades")
        continue
    w = sum(1 for t in bucket if t['result'] == 'WIN')
    pnl = sum(t['pnl'] for t in bucket)
    avg_vix = sum(t['vix'] for t in bucket) / len(bucket)
    print(f"  Overvix {label}: {len(bucket)}t | WR={w/len(bucket)*100:.1f}% | PnL={pnl:+.1f} | AvgVIX={avg_vix:.1f}")

# D3: Overvix buckets for shorts
print()
print("--- D3: Overvix buckets (SHORT trades, all VIX levels) ---")
ov_shorts = [t for t in shorts if t.get('overvix') is not None]
print(f"Short trades with overvix: {len(ov_shorts)}")

for lo, hi, label in ov_buckets:
    bucket = [t for t in ov_shorts if lo <= t['overvix'] < hi]
    if not bucket:
        print(f"  Overvix {label}: 0 trades")
        continue
    w = sum(1 for t in bucket if t['result'] == 'WIN')
    pnl = sum(t['pnl'] for t in bucket)
    avg_vix = sum(t['vix'] for t in bucket) / len(bucket)
    print(f"  Overvix {label}: {len(bucket)}t | WR={w/len(bucket)*100:.1f}% | PnL={pnl:+.1f} | AvgVIX={avg_vix:.1f}")

# D4: VIX thresholds with overvix cross-analysis
print()
print("--- D4: Long trades above each VIX level, overvix split ---")
for vix_thresh in [20, 22, 24, 26]:
    above = [t for t in longs if t['vix'] > vix_thresh]
    above_ov = [t for t in above if t.get('overvix') is not None]
    if not above:
        print(f"  VIX>{vix_thresh}: 0 trades")
        continue
    ov_pos = [t for t in above_ov if t['overvix'] >= 2]
    ov_neg = [t for t in above_ov if t['overvix'] < 2]
    ov_null = [t for t in above if t.get('overvix') is None]
    all_w = sum(1 for t in above if t['result'] == 'WIN')
    all_pnl = sum(t['pnl'] for t in above)
    print(f"  VIX>{vix_thresh} ALL: {len(above)}t WR={all_w/len(above)*100:.1f}% PnL={all_pnl:+.1f}")
    if ov_pos:
        w = sum(1 for t in ov_pos if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_pos)
        print(f"    overvix >= +2: {len(ov_pos)}t WR={w/len(ov_pos)*100:.1f}% PnL={pnl:+.1f}")
    else:
        print(f"    overvix >= +2: 0 trades")
    if ov_neg:
        w = sum(1 for t in ov_neg if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_neg)
        print(f"    overvix <  +2: {len(ov_neg)}t WR={w/len(ov_neg)*100:.1f}% PnL={pnl:+.1f}")
    else:
        print(f"    overvix <  +2: 0 trades")
    if ov_null:
        w = sum(1 for t in ov_null if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_null)
        print(f"    overvix null:  {len(ov_null)}t WR={w/len(ov_null)*100:.1f}% PnL={pnl:+.1f}")


# ====================================================================
print()
print("=" * 80)
print("SECTION E: DAILY VIX + PnL SUMMARY")
print("=" * 80)

daily = defaultdict(lambda: {'vix_avg': [], 'overvix_avg': [], 'long_pnl': 0, 'short_pnl': 0,
                               'long_n': 0, 'short_n': 0, 'long_w': 0, 'short_w': 0})
for t in trades:
    d = daily[t['date']]
    d['vix_avg'].append(t['vix'])
    if t.get('overvix') is not None:
        d['overvix_avg'].append(t['overvix'])
    if t['direction'] in ('long', 'bullish'):
        d['long_pnl'] += t['pnl']
        d['long_n'] += 1
        if t['result'] == 'WIN':
            d['long_w'] += 1
    else:
        d['short_pnl'] += t['pnl']
        d['short_n'] += 1
        if t['result'] == 'WIN':
            d['short_w'] += 1

print(f"{'Date':<12} {'AvgVIX':<8} {'AvgOV':<8} {'LN':<5} {'LWR':<7} {'LPnL':<10} {'SN':<5} {'SWR':<7} {'SPnL':<10} {'Total'}")
for date in sorted(daily.keys()):
    d = daily[date]
    avg_vix = sum(d['vix_avg']) / len(d['vix_avg'])
    avg_ov = sum(d['overvix_avg']) / len(d['overvix_avg']) if d['overvix_avg'] else None
    ov_str = f"{avg_ov:+.1f}" if avg_ov is not None else "n/a"
    l_wr = f"{d['long_w']/d['long_n']*100:.0f}%" if d['long_n'] > 0 else "n/a"
    s_wr = f"{d['short_w']/d['short_n']*100:.0f}%" if d['short_n'] > 0 else "n/a"
    total = d['long_pnl'] + d['short_pnl']
    print(f"{date:<12} {avg_vix:<8.1f} {ov_str:<8} {d['long_n']:<5} {l_wr:<7} {d['long_pnl']:<+10.1f} {d['short_n']:<5} {s_wr:<7} {d['short_pnl']:<+10.1f} {total:+.1f}")


# ====================================================================
print()
print("=" * 80)
print("SECTION F: OPTIMAL VIX THRESHOLD FOR SHORT TRADES")
print("=" * 80)
print("Testing: block shorts when VIX > threshold")
print()

base_w = sum(1 for t in shorts if t['result'] == 'WIN')
base_pnl = sum(t['pnl'] for t in shorts)
print(f"{'Threshold':<14} {'Trades':<8} {'Blocked':<9} {'WR':<8} {'PnL':<12} {'Blocked Detail'}")
print(f"{'No filter':<14} {len(shorts):<8} {0:<9} {base_w/len(shorts)*100:.1f}%   {base_pnl:+.1f}     n/a")

for thresh in [18, 19, 20, 21, 22, 23, 24, 25, 26]:
    kept = [t for t in shorts if t['vix'] <= thresh]
    blocked = [t for t in shorts if t['vix'] > thresh]
    if not kept:
        continue
    w = sum(1 for t in kept if t['result'] == 'WIN')
    pnl = sum(t['pnl'] for t in kept)
    bl_pnl = sum(t['pnl'] for t in blocked)
    bl_w = sum(1 for t in blocked if t['result'] == 'WIN')
    bl_wr = f"{bl_w/len(blocked)*100:.1f}%" if blocked else "n/a"
    print(f"VIX<={thresh:<7} {len(kept):<8} {len(blocked):<9} {w/len(kept)*100:.1f}%   {pnl:+.1f}     {len(blocked)}t {bl_pnl:+.1f}pts ({bl_wr}WR)")


# ====================================================================
print()
print("=" * 80)
print("SECTION G: OVERVIX MEAN REVERSION AT LOWER VIX LEVELS")
print("=" * 80)
print("Does overvix >= +2 predict better long outcomes at VIX < 26?")
print()

ov_longs_low = [t for t in longs if t.get('overvix') is not None and t['vix'] <= 26]
if ov_longs_low:
    ov_high = [t for t in ov_longs_low if t['overvix'] >= 2]
    ov_low = [t for t in ov_longs_low if t['overvix'] < 2]

    if ov_high:
        w = sum(1 for t in ov_high if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_high)
        avg_vix = sum(t['vix'] for t in ov_high) / len(ov_high)
        print(f"VIX<=26 + overvix>=+2: {len(ov_high)}t | WR={w/len(ov_high)*100:.1f}% | PnL={pnl:+.1f} | AvgVIX={avg_vix:.1f}")
    else:
        print("VIX<=26 + overvix>=+2: 0 trades")

    if ov_low:
        w = sum(1 for t in ov_low if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_low)
        avg_vix = sum(t['vix'] for t in ov_low) / len(ov_low)
        print(f"VIX<=26 + overvix< +2: {len(ov_low)}t | WR={w/len(ov_low)*100:.1f}% | PnL={pnl:+.1f} | AvgVIX={avg_vix:.1f}")
    else:
        print("VIX<=26 + overvix< +2: 0 trades")

# By VIX sub-bucket
print()
for vix_lo, vix_hi, vix_label in [(0,20,"VIX<20"), (20,22,"VIX 20-22"), (22,24,"VIX 22-24"), (24,26,"VIX 24-26")]:
    sub = [t for t in longs if t.get('overvix') is not None and vix_lo <= t['vix'] < vix_hi]
    if not sub:
        print(f"  {vix_label}: 0 trades with overvix")
        continue
    ov_h = [t for t in sub if t['overvix'] >= 2]
    ov_l = [t for t in sub if t['overvix'] < 2]

    print(f"  {vix_label} ({len(sub)} trades):")
    if ov_h:
        w = sum(1 for t in ov_h if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_h)
        print(f"    overvix>=+2: {len(ov_h)}t WR={w/len(ov_h)*100:.1f}% PnL={pnl:+.1f}")
    else:
        print(f"    overvix>=+2: 0t")
    if ov_l:
        w = sum(1 for t in ov_l if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_l)
        print(f"    overvix< +2: {len(ov_l)}t WR={w/len(ov_l)*100:.1f}% PnL={pnl:+.1f}")
    else:
        print(f"    overvix< +2: 0t")


print("\n\nDone.", flush=True)
