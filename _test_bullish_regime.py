"""Test bullish-regime filter: net +GEX, strong +GEX above spot, net -charm."""
import pickle
from collections import defaultdict

with open('enriched_shorts.pkl','rb') as f: R = pickle.load(f)

print(f"Total V12-fix signals: {len(R)}\n")

# Distribution of features
import statistics as st
net_gex_vals = [r['net_gex'] for r in R]
plus_above_vals = [r['plus_above_v'] for r in R]
charm_vals = [r['net_charm'] for r in R if r['net_charm'] is not None]
print('=== FEATURE DISTRIBUTIONS ===')
print(f"net_gex       : min {min(net_gex_vals):+.0f}  p25 {st.quantiles(net_gex_vals,n=4)[0]:+.0f}  median {st.median(net_gex_vals):+.0f}  p75 {st.quantiles(net_gex_vals,n=4)[2]:+.0f}  max {max(net_gex_vals):+.0f}")
print(f"plus_above_v  : min {min(plus_above_vals):+.0f}  p25 {st.quantiles(plus_above_vals,n=4)[0]:+.0f}  median {st.median(plus_above_vals):+.0f}  p75 {st.quantiles(plus_above_vals,n=4)[2]:+.0f}  max {max(plus_above_vals):+.0f}")
if charm_vals:
    print(f"net_charm     : min {min(charm_vals):+.2f}  p25 {st.quantiles(charm_vals,n=4)[0]:+.2f}  median {st.median(charm_vals):+.2f}  p75 {st.quantiles(charm_vals,n=4)[2]:+.2f}  max {max(charm_vals):+.2f}")
    print(f"signals w/ charm data: {len(charm_vals)}/{len(R)}")

def stats(arr):
    if not arr: return (0,0,0.0,0.0)
    w = sum(1 for r in arr if r['out']=='WIN')
    pnl = sum(r['pnl'] for r in arr)
    return len(arr), w, pnl, w/len(arr)*100

def ap(name, fn):
    blk = [r for r in R if fn(r)]
    n,w,p,wr = stats(R); bn,bw,bp,bwr = stats(blk)
    print(f"  {name}: block {bn}t {bw}W={bwr:.0f}% bp={bp:+.1f} | after {p-bp:+.1f} (delta {-bp:+.1f})")

base_n, base_w, base_p, _ = stats(R)
print(f"\n=== BASELINE: {base_n}t  +{base_p:.1f} pts  WR={base_w/base_n*100:.0f}% ===")

print('\n=== USER THESIS (one at a time): BULLISH REGIME BLOCKS ===')
ap('A: net_gex>0 (any positive net)',        lambda r: r['net_gex']>0)
ap('B: plus_above_v>=30 (M)',                lambda r: r['plus_above_v']>=30)
ap('C: plus_above_v>=50 (M strong)',          lambda r: r['plus_above_v']>=50)
ap('D: plus_above_v>=80 (M very strong)',     lambda r: r['plus_above_v']>=80)
ap('E: net_charm<0 (bullish hedging)',        lambda r: r['net_charm'] is not None and r['net_charm']<0)
ap('F: net_charm<-50',                        lambda r: r['net_charm'] is not None and r['net_charm']<-50)
ap('G: net_charm<-100',                       lambda r: r['net_charm'] is not None and r['net_charm']<-100)

print('\n=== USER THESIS: 3 CONDITIONS COMBINED ===')
# Threshold sweep
for gt in [0, 20, 50]:
    for pa in [30, 50, 80]:
        for ct in [0, -50, -100]:
            def fn(r, gt=gt, pa=pa, ct=ct):
                if r['net_gex']<=gt: return False
                if r['plus_above_v']<pa: return False
                if r['net_charm'] is None: return False
                if r['net_charm']>=ct: return False
                return True
            ap(f'gex>{gt} AND +above>={pa}M AND charm<{ct}', fn)

print('\n=== TWO-OF-THREE COMBOS ===')
ap('net_gex>0 AND plus_above>=50',           lambda r: r['net_gex']>0 and r['plus_above_v']>=50)
ap('net_gex>0 AND charm<0',                   lambda r: r['net_gex']>0 and (r['net_charm'] is not None and r['net_charm']<0))
ap('plus_above>=50 AND charm<0',              lambda r: r['plus_above_v']>=50 and (r['net_charm'] is not None and r['net_charm']<0))

print('\n=== PER-SETUP: best single-condition ===')
for setup in ['Skew Charm','DD Exhaustion','AG Short']:
    sub = [r for r in R if r['setup']==setup]
    if not sub: continue
    n,w,p,_ = stats(sub)
    print(f'\n  {setup}: base {n}t {p:+.1f} ({w/n*100:.0f}%)')
    for name, fn in [
        ('gex>0', lambda r: r['net_gex']>0),
        ('+above>=50', lambda r: r['plus_above_v']>=50),
        ('charm<0', lambda r: r['net_charm'] is not None and r['net_charm']<0),
        ('gex>0 AND +above>=50 AND charm<0',
         lambda r: r['net_gex']>0 and r['plus_above_v']>=50 and (r['net_charm'] is not None and r['net_charm']<0)),
    ]:
        blk = [r for r in sub if fn(r)]
        bn,bw,bp,bwr = stats(blk)
        print(f"    block {name:<40}: {bn}t {bw}W={bwr:.0f}% bp={bp:+.1f} | after {p-bp:+.1f}")

# Daily PnL with best candidate (triple-combo at any threshold)
print('\n=== DAILY PNL: triple combo gex>0 AND +above>=50 AND charm<0 ===')
by_date = defaultdict(list)
for r in R: by_date[r['date']].append(r)
print(f"{'Date':<12} {'#sig':<5} {'Base':<8} {'Blk':<4} {'Blk$':<8} {'After':<7}")
tot_base_p = tot_blk_p = 0.0; tot_base_n = tot_blk_n = 0
for d in sorted(by_date.keys()):
    arr = by_date[d]
    n,w,p,_ = stats(arr)
    blk = [r for r in arr if r['net_gex']>0 and r['plus_above_v']>=50 and (r['net_charm'] is not None and r['net_charm']<0)]
    bn,bw,bp,_ = stats(blk)
    print(f"{str(d):<12} {n:<5} {p:<+8.1f} {bn:<4} {bp:<+8.1f} {p-bp:<+7.1f}")
    tot_base_p += p; tot_base_n += n; tot_blk_p += bp; tot_blk_n += bn
print('-'*60)
print(f"{'TOTAL':<12} {tot_base_n:<5} {tot_base_p:<+8.1f} {tot_blk_n:<4} {tot_blk_p:<+8.1f} {tot_base_p-tot_blk_p:<+7.1f}")

# Stability
print('\n=== STABILITY triple combo (median split) ===')
R_sorted = sorted(R, key=lambda x: x['ts'])
mid = len(R_sorted)//2
for half, label in [(R_sorted[:mid],'H1'), (R_sorted[mid:],'H2')]:
    blk = [r for r in half if r['net_gex']>0 and r['plus_above_v']>=50 and (r['net_charm'] is not None and r['net_charm']<0)]
    n,w,p,_ = stats(half); bn,bw,bp,_ = stats(blk)
    d1,d2 = half[0]['date'], half[-1]['date']
    print(f"  {label} {d1} -> {d2}: {n}t base {p:+.1f} | block {bn}({bw}W) bp={bp:+.1f} | after {p-bp:+.1f}")
