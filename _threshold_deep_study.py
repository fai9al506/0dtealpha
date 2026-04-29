"""Deep threshold study: fine sweep, distance-weighting, concentration metrics."""
import pickle, json, bisect
from collections import defaultdict
from sqlalchemy import create_engine, text
import statistics as st
import os

with open('enriched_shorts.pkl','rb') as f: R = pickle.load(f)

# For SC+DD (the setups the filter applies to)
SUB = [r for r in R if r['setup'] in ('Skew Charm','DD Exhaustion')]
print(f"Sample: {len(SUB)} SC+DD signals (Mar 1 - Apr 15)")

def stats(arr):
    if not arr: return (0,0,0.0)
    w = sum(1 for r in arr if r['out']=='WIN')
    return len(arr), w, sum(r['pnl'] for r in arr)

# 1) Full distribution of plus_above_v
vals = sorted([r['plus_above_v'] for r in SUB])
print(f"\nplus_above_v distribution (SC+DD, n={len(vals)}):")
for pct in [10,25,40,50,60,70,75,80,85,90,95,99]:
    i = int(len(vals)*pct/100)
    print(f"  p{pct}: {vals[i]:+.1f}M")
print(f"  max: {vals[-1]:+.1f}M")

# 2) Fine threshold sweep — every 5M
print('\n=== FINE SWEEP: threshold on plus_above_v (SC+DD only) ===')
print(f"{'Thr (M)':<8} {'Block':<6} {'Wins':<5} {'WR':<5} {'BlkPnL':<8} {'After':<8} {'Delta':<7}")
baseline = stats(SUB)
print(f"  BASELINE: {baseline[0]}t {baseline[1]}W={baseline[1]/baseline[0]*100:.0f}% {baseline[2]:+.1f}")
best_thr, best_delta = None, -999
for thr in range(20, 155, 5):
    blk = [r for r in SUB if r['plus_above_v']>=thr]
    n,w,p = stats(SUB); bn,bw,bp = stats(blk)
    if bn == 0:
        print(f"  >={thr:<5} 0 blocks"); continue
    delta = -bp
    star = " *" if delta > best_delta else ""
    print(f"  >={thr:<5} {bn:<6} {bw:<5} {bw/bn*100:<5.0f} {bp:<+8.1f} {p-bp:<+8.1f} {delta:<+7.1f}{star}")
    if delta > best_delta:
        best_thr, best_delta = thr, delta

print(f"\nBest raw threshold: >={best_thr}M -> delta {best_delta:+.1f}")

# 3) Distance-weighted metric: size / distance
print('\n=== ALTERNATIVE 1: magnet "pull" = size / distance ===')
# magnet pull = plus_above_v / max(plus_above_k - spot, 1) — bigger when close AND strong
for r in SUB:
    k = r['plus_above_k']
    if k and r['plus_above_v'] > 0:
        dist = max(k - r['spot'], 1)
        r['pull'] = r['plus_above_v'] / dist  # M per pt
    else:
        r['pull'] = 0

vals = sorted([r['pull'] for r in SUB])
print(f"pull distribution: p25={vals[len(vals)//4]:.2f}  p50={vals[len(vals)//2]:.2f}  p75={vals[len(vals)*3//4]:.2f}  p90={vals[len(vals)*9//10]:.2f}  p95={vals[len(vals)*95//100]:.2f}  max={vals[-1]:.2f}")
print(f"{'Thr':<8} {'Block':<6} {'Wins':<5} {'WR':<5} {'BlkPnL':<8} {'After':<8}")
for thr in [3, 5, 8, 10, 15, 20, 30, 50, 80]:
    blk = [r for r in SUB if r['pull']>=thr]
    if not blk:
        print(f"  >={thr:<5} 0 blocks"); continue
    n,w,p = stats(SUB); bn,bw,bp = stats(blk)
    print(f"  >={thr:<5} {bn:<6} {bw:<5} {bw/bn*100:<5.0f} {bp:<+8.1f} {p-bp:<+8.1f}")

# 4) Size + distance combined filter (raw magnitude above threshold AND within N pts)
print('\n=== ALTERNATIVE 2: size >= T AND distance =< D (close magnet) ===')
for size_t in [40, 50, 60, 70]:
    for dist_t in [10, 15, 20, 30]:
        blk = [r for r in SUB if r['plus_above_v']>=size_t and r['plus_above_k'] and (r['plus_above_k']-r['spot'])<=dist_t]
        if not blk: continue
        n,w,p = stats(SUB); bn,bw,bp = stats(blk)
        print(f"  size>={size_t}M AND dist<={dist_t}: {bn}t {bw}W={bw/bn*100:.0f}% bp={bp:+.1f} after={p-bp:+.1f} (delta {-bp:+.1f})")

# 5) Normalize by total chain gamma (concentration)
print('\n=== ALTERNATIVE 3: top magnet as % of abs(net_gex) ===')
for r in SUB:
    if r['net_gex'] and abs(r['net_gex']) > 0:
        r['pct'] = r['plus_above_v'] / abs(r['net_gex']) * 100
    else:
        r['pct'] = 0
pct_vals = sorted([r['pct'] for r in SUB])
print(f"pct distribution: p50={pct_vals[len(pct_vals)//2]:.1f}%  p75={pct_vals[len(pct_vals)*3//4]:.1f}%  p90={pct_vals[len(pct_vals)*9//10]:.1f}%  max={pct_vals[-1]:.1f}%")
for thr in [10, 15, 20, 25, 30, 40, 50]:
    blk = [r for r in SUB if r['pct']>=thr]
    if not blk: continue
    n,w,p = stats(SUB); bn,bw,bp = stats(blk)
    print(f"  pct>={thr}%: {bn}t {bw}W={bw/bn*100:.0f}% bp={bp:+.1f} after={p-bp:+.1f}")

# 6) Detailed look at 40-70M band (what the user thinks might be a useful signal)
print('\n=== DEEP DIVE: 40-70M band (signals in this range) ===')
band = [r for r in SUB if 40 <= r['plus_above_v'] < 70]
print(f"In band: {len(band)} trades")
for setup in ['Skew Charm','DD Exhaustion']:
    s_band = [r for r in band if r['setup']==setup]
    if s_band:
        n,w,p = stats(s_band)
        print(f"  {setup}: {n}t {w}W={w/n*100:.0f}% WR {p:+.1f} pts")

# 7) Full per-setup with multiple thresholds
print('\n=== PER-SETUP FINE THRESHOLD SWEEP ===')
for setup in ['Skew Charm','DD Exhaustion']:
    sub = [r for r in SUB if r['setup']==setup]
    n0,w0,p0 = stats(sub)
    print(f"\n{setup} baseline: {n0}t {w0}W={w0/n0*100:.0f}% WR {p0:+.1f}")
    print(f"  {'Thr':<6} {'Block':<6} {'WR':<5} {'BlkPnL':<8} {'After':<8} {'Delta':<6}")
    for thr in range(30, 131, 10):
        blk = [r for r in sub if r['plus_above_v']>=thr]
        if not blk: continue
        bn,bw,bp = stats(blk)
        print(f"  >={thr:<4}  {bn:<6} {bw/bn*100:<5.0f} {bp:<+8.1f} {p0-bp:<+8.1f} {-bp:<+6.1f}")
