"""VIX vs Trade Outcomes Analysis — complete breakdown."""
import os, sys, json
from sqlalchemy import create_engine, text
from collections import defaultdict

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Fetch ALL trades with VIX and overvix
rows = c.execute(text("""
    SELECT (sl.ts AT TIME ZONE 'America/New_York')::date as d,
           sl.setup_name, sl.direction, sl.grade, sl.score,
           sl.greek_alignment, sl.spot_vol_beta,
           sl.outcome_result, sl.outcome_pnl,
           sl.vix, sl.overvix
    FROM setup_log sl
    WHERE sl.ts >= '2026-02-05'
      AND sl.outcome_result IS NOT NULL
    ORDER BY sl.ts
""")).fetchall()
c.close()

print(f"Total trades fetched: {len(rows)}", flush=True)

# Count how many have VIX data
vix_count = sum(1 for r in rows if r.vix is not None)
overvix_count = sum(1 for r in rows if r.overvix is not None)
print(f"Trades with VIX: {vix_count}, with overvix: {overvix_count}", flush=True)

# Filter to trades with VIX data and WIN/LOSS results
trades = []
for r in rows:
    if r.vix is not None and r.outcome_result in ('WIN', 'LOSS'):
        trades.append({
            'date': str(r.d),
            'setup': r.setup_name,
            'dir': r.direction,
            'grade': r.grade,
            'alignment': r.greek_alignment,
            'svb': float(r.spot_vol_beta) if r.spot_vol_beta is not None else None,
            'result': r.outcome_result,
            'pnl': float(r.outcome_pnl) if r.outcome_pnl else 0,
            'vix': float(r.vix),
            'overvix': float(r.overvix) if r.overvix is not None else None
        })

print(f"\nTrades with VIX + WIN/LOSS: {len(trades)}", flush=True)

# Also count EXPIRED
expired_trades = []
for r in rows:
    if r.vix is not None and r.outcome_result == 'EXPIRED':
        expired_trades.append({
            'date': str(r.d),
            'setup': r.setup_name,
            'dir': r.direction,
            'result': 'EXPIRED',
            'pnl': float(r.outcome_pnl) if r.outcome_pnl else 0,
            'vix': float(r.vix),
            'overvix': float(r.overvix) if r.overvix is not None else None
        })
print(f"EXPIRED trades with VIX: {len(expired_trades)}", flush=True)

# Show VIX range
vix_vals = [t['vix'] for t in trades]
if vix_vals:
    print(f"VIX range: {min(vix_vals):.2f} - {max(vix_vals):.2f}", flush=True)

# VIX distribution
print("\n--- VIX Distribution (all WIN/LOSS trades) ---")
for lo, hi, label in [(0,18,"<18"), (18,20,"18-20"), (20,22,"20-22"), (22,24,"22-24"), (24,26,"24-26"), (26,100,">26")]:
    n = sum(1 for t in trades if lo <= t['vix'] < hi)
    print(f"  VIX {label}: {n} trades")

# ====================================================================
# SECTION A: VIX BUCKET ANALYSIS FOR LONG TRADES
# ====================================================================
print("\n" + "="*80)
print("SECTION A: VIX BUCKET ANALYSIS — LONG TRADES")
print("="*80)

longs = [t for t in trades if t['dir'] in ('long', 'bullish')]
print(f"Total long trades with VIX: {len(longs)}")

buckets = [(0,18,"<18"), (18,20,"18-20"), (20,22,"20-22"), (22,24,"22-24"), (24,26,"24-26"), (26,100,">26")]

for lo, hi, label in buckets:
    bucket = [t for t in longs if lo <= t['vix'] < hi]
    if not bucket:
        print(f"\n  VIX {label}: 0 trades")
        continue
    wins = sum(1 for t in bucket if t['result'] == 'WIN')
    losses = len(bucket) - wins
    pnl = sum(t['pnl'] for t in bucket)
    wr = wins/len(bucket)*100
    print(f"\n  VIX {label}: {len(bucket)} trades | WR={wr:.1f}% ({wins}W/{losses}L) | PnL={pnl:+.1f} pts")

    # Per-setup breakdown
    setup_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0})
    for t in bucket:
        k = t['setup']
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
# SECTION B: VIX BUCKET ANALYSIS FOR SHORT TRADES
# ====================================================================
print("\n" + "="*80)
print("SECTION B: VIX BUCKET ANALYSIS — SHORT TRADES")
print("="*80)

shorts = [t for t in trades if t['dir'] in ('short', 'bearish')]
print(f"Total short trades with VIX: {len(shorts)}")

for lo, hi, label in buckets:
    bucket = [t for t in shorts if lo <= t['vix'] < hi]
    if not bucket:
        print(f"\n  VIX {label}: 0 trades")
        continue
    wins = sum(1 for t in bucket if t['result'] == 'WIN')
    losses = len(bucket) - wins
    pnl = sum(t['pnl'] for t in bucket)
    wr = wins/len(bucket)*100
    print(f"\n  VIX {label}: {len(bucket)} trades | WR={wr:.1f}% ({wins}W/{losses}L) | PnL={pnl:+.1f} pts")

    setup_stats = defaultdict(lambda: {'w': 0, 'l': 0, 'pnl': 0})
    for t in bucket:
        k = t['setup']
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
# SECTION C: OPTIMAL VIX THRESHOLD FOR LONGS
# ====================================================================
print("\n" + "="*80)
print("SECTION C: OPTIMAL VIX THRESHOLD FOR LONG TRADES")
print("="*80)
print("Testing: block longs when VIX > threshold")
print(f"{'Threshold':<12} {'Trades':<8} {'Blocked':<9} {'WR':<8} {'PnL':<12} {'Avg PnL':<10}")

# Baseline (no filter)
base_w = sum(1 for t in longs if t['result'] == 'WIN')
base_pnl = sum(t['pnl'] for t in longs)
print(f"{'No filter':<12} {len(longs):<8} {0:<9} {base_w/len(longs)*100:.1f}%  {base_pnl:+.1f}     {base_pnl/len(longs):+.2f}")

for thresh in [18, 19, 20, 21, 22, 23, 24, 25, 26]:
    kept = [t for t in longs if t['vix'] <= thresh]
    blocked = [t for t in longs if t['vix'] > thresh]
    if not kept:
        print(f"VIX<={thresh:<5} {0:<8} {len(blocked):<9} n/a      n/a          n/a")
        continue
    w = sum(1 for t in kept if t['result'] == 'WIN')
    pnl = sum(t['pnl'] for t in kept)
    bl_pnl = sum(t['pnl'] for t in blocked)
    print(f"VIX<={thresh:<5} {len(kept):<8} {len(blocked):<9} {w/len(kept)*100:.1f}%  {pnl:+.1f}     {pnl/len(kept):+.2f}   (blocked: {len(blocked)}t {bl_pnl:+.1f}pts)")

# Also test "block longs when VIX > threshold UNLESS overvix >= +2"
print("\n--- With Overvix Override (VIX > threshold blocked UNLESS overvix >= +2) ---")
print(f"{'Threshold':<12} {'Trades':<8} {'Blocked':<9} {'WR':<8} {'PnL':<12}")

for thresh in [18, 19, 20, 21, 22, 23, 24, 25, 26]:
    kept = [t for t in longs if t['vix'] <= thresh or (t['overvix'] is not None and t['overvix'] >= 2)]
    blocked = [t for t in longs if t['vix'] > thresh and (t['overvix'] is None or t['overvix'] < 2)]
    if not kept:
        print(f"VIX<={thresh:<5} {0:<8} {len(blocked):<9} n/a      n/a")
        continue
    w = sum(1 for t in kept if t['result'] == 'WIN')
    pnl = sum(t['pnl'] for t in kept)
    bl_pnl = sum(t['pnl'] for t in blocked)
    print(f"VIX<={thresh:<5} {len(kept):<8} {len(blocked):<9} {w/len(kept)*100:.1f}%  {pnl:+.1f}     (blocked: {len(blocked)}t {bl_pnl:+.1f}pts)")


# ====================================================================
# SECTION D: OVERVIX ANALYSIS
# ====================================================================
print("\n" + "="*80)
print("SECTION D: OVERVIX ANALYSIS")
print("="*80)

# How many trades have overvix?
ov_trades = [t for t in trades if t['overvix'] is not None]
print(f"Trades with overvix data: {len(ov_trades)} / {len(trades)}")

if ov_trades:
    ov_vals = [t['overvix'] for t in ov_trades]
    print(f"Overvix range: {min(ov_vals):.2f} to {max(ov_vals):.2f}")
    print(f"Overvix mean: {sum(ov_vals)/len(ov_vals):.2f}")

# D1: Long trades, VIX > 20, split by overvix
print("\n--- D1: Long trades at VIX > 20 ---")
longs_high_vix = [t for t in longs if t['vix'] > 20]
print(f"Total long trades with VIX > 20: {len(longs_high_vix)}")

if longs_high_vix:
    ov_high = [t for t in longs_high_vix if t['overvix'] is not None and t['overvix'] >= 2]
    ov_low = [t for t in longs_high_vix if t['overvix'] is None or t['overvix'] < 2]

    if ov_high:
        w = sum(1 for t in ov_high if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_high)
        print(f"  Overvix >= +2: {len(ov_high)}t | WR={w/len(ov_high)*100:.1f}% | PnL={pnl:+.1f}")
        for t in ov_high:
            print(f"    {t['date']} {t['setup']:20s} VIX={t['vix']:.1f} OV={t['overvix']:+.1f} align={t['alignment']} → {t['result']} {t['pnl']:+.1f}")
    else:
        print("  Overvix >= +2: 0 trades")

    if ov_low:
        w = sum(1 for t in ov_low if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_low)
        print(f"  Overvix < +2 (or null): {len(ov_low)}t | WR={w/len(ov_low)*100:.1f}% | PnL={pnl:+.1f}")
        for t in ov_low:
            ov_str = f"{t['overvix']:+.1f}" if t['overvix'] is not None else "null"
            print(f"    {t['date']} {t['setup']:20s} VIX={t['vix']:.1f} OV={ov_str} align={t['alignment']} → {t['result']} {t['pnl']:+.1f}")
    else:
        print("  Overvix < +2: 0 trades")

# D2: Overvix buckets across ALL VIX levels (longs only)
print("\n--- D2: Overvix buckets (LONG trades only, all VIX levels) ---")
ov_longs = [t for t in longs if t['overvix'] is not None]
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

# D3: Same for shorts
print("\n--- D3: Overvix buckets (SHORT trades only, all VIX levels) ---")
ov_shorts = [t for t in shorts if t['overvix'] is not None]
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

# D4: Detailed view: VIX > threshold at each overvix level for longs
print("\n--- D4: Long trades at each VIX level with overvix override analysis ---")
for vix_thresh in [20, 22, 24, 26]:
    above = [t for t in longs if t['vix'] > vix_thresh and t['overvix'] is not None]
    if not above:
        print(f"\n  VIX>{vix_thresh}: 0 trades with overvix data")
        continue
    ov_pos = [t for t in above if t['overvix'] >= 2]
    ov_neg = [t for t in above if t['overvix'] < 2]
    print(f"\n  VIX>{vix_thresh} ({len(above)} trades):")
    if ov_pos:
        w = sum(1 for t in ov_pos if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_pos)
        print(f"    overvix >= +2: {len(ov_pos)}t WR={w/len(ov_pos)*100:.1f}% PnL={pnl:+.1f}")
    if ov_neg:
        w = sum(1 for t in ov_neg if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in ov_neg)
        print(f"    overvix <  +2: {len(ov_neg)}t WR={w/len(ov_neg)*100:.1f}% PnL={pnl:+.1f}")

# ====================================================================
# SECTION E: DAILY VIX + PnL SUMMARY
# ====================================================================
print("\n" + "="*80)
print("SECTION E: DAILY VIX LEVEL & TRADE PnL")
print("="*80)

daily = defaultdict(lambda: {'vix_avg': [], 'long_pnl': 0, 'short_pnl': 0, 'long_n': 0, 'short_n': 0, 'long_w': 0, 'short_w': 0})
for t in trades:
    d = daily[t['date']]
    d['vix_avg'].append(t['vix'])
    if t['dir'] in ('long', 'bullish'):
        d['long_pnl'] += t['pnl']
        d['long_n'] += 1
        if t['result'] == 'WIN': d['long_w'] += 1
    else:
        d['short_pnl'] += t['pnl']
        d['short_n'] += 1
        if t['result'] == 'WIN': d['short_w'] += 1

print(f"{'Date':<12} {'AvgVIX':<8} {'LongN':<7} {'LongWR':<8} {'LongPnL':<10} {'ShortN':<7} {'ShortWR':<9} {'ShortPnL':<10} {'Total'}")
for date in sorted(daily.keys()):
    d = daily[date]
    avg_vix = sum(d['vix_avg']) / len(d['vix_avg'])
    l_wr = f"{d['long_w']/d['long_n']*100:.0f}%" if d['long_n'] > 0 else "n/a"
    s_wr = f"{d['short_w']/d['short_n']*100:.0f}%" if d['short_n'] > 0 else "n/a"
    total = d['long_pnl'] + d['short_pnl']
    print(f"{date:<12} {avg_vix:<8.1f} {d['long_n']:<7} {l_wr:<8} {d['long_pnl']:<+10.1f} {d['short_n']:<7} {s_wr:<9} {d['short_pnl']:<+10.1f} {total:+.1f}")


print("\n\nDone.", flush=True)
