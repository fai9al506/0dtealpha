"""Drill into the clean signal: strong +GEX above spot >= 80M."""
import pickle
from collections import defaultdict

with open('enriched_shorts.pkl','rb') as f: R = pickle.load(f)

def stats(arr):
    if not arr: return (0,0,0.0,0.0)
    w = sum(1 for r in arr if r['out']=='WIN')
    pnl = sum(r['pnl'] for r in arr)
    return len(arr), w, pnl, w/len(arr)*100

def ap(name, arr, fn):
    blk = [r for r in arr if fn(r)]
    n,w,p,wr = stats(arr); bn,bw,bp,bwr = stats(blk)
    print(f"  {name}: blk {bn}({bw}W={bwr:.0f}%) bp={bp:+.1f} | after {p-bp:+.1f} (delta {-bp:+.1f})")

print('=== PLUS-ABOVE THRESHOLD SWEEP (single condition) ===')
for thr in [40, 50, 60, 70, 80, 90, 100, 120, 150]:
    ap(f'plus_above>={thr}', R, lambda r, t=thr: r['plus_above_v']>=t)

print('\n=== PER-SETUP @ plus_above>=80 (STANDALONE) ===')
for setup in ['Skew Charm','DD Exhaustion','AG Short']:
    sub = [r for r in R if r['setup']==setup]
    if not sub: continue
    n,w,p,_ = stats(sub)
    print(f'\n  {setup}: base {n}t {p:+.1f} ({w/n*100:.0f}%)')
    ap(f'    plus_above>=80', sub, lambda r: r['plus_above_v']>=80)
    ap(f'    plus_above>=60', sub, lambda r: r['plus_above_v']>=60)
    ap(f'    plus_above>=100', sub, lambda r: r['plus_above_v']>=100)

print('\n=== DAILY PNL: plus_above>=80 (full sample) ===')
by_date = defaultdict(list)
for r in R: by_date[r['date']].append(r)
print(f"{'Date':<12} {'#sig':<5} {'Base':<8} {'Blk':<4} {'Blk$':<8} {'After':<8}")
T_p=T_n=0; B_p=B_n=0
for d in sorted(by_date.keys()):
    arr = by_date[d]
    n,w,p,_ = stats(arr)
    blk = [r for r in arr if r['plus_above_v']>=80]
    bn,bw,bp,_ = stats(blk)
    print(f"{str(d):<12} {n:<5} {p:<+8.1f} {bn:<4} {bp:<+8.1f} {p-bp:<+8.1f}")
    T_p+=p; T_n+=n; B_p+=bp; B_n+=bn
print('-'*56)
print(f"{'TOTAL':<12} {T_n:<5} {T_p:<+8.1f} {B_n:<4} {B_p:<+8.1f} {T_p-B_p:<+8.1f}")

print('\n=== STABILITY @ plus_above>=80 ===')
R_s = sorted(R, key=lambda x: x['ts'])
mid = len(R_s)//2
for half, label in [(R_s[:mid],'H1'), (R_s[mid:],'H2')]:
    blk = [r for r in half if r['plus_above_v']>=80]
    n,w,p,_ = stats(half); bn,bw,bp,_ = stats(blk)
    d1,d2 = half[0]['date'], half[-1]['date']
    print(f"  {label} {d1} -> {d2}: {n}t base {p:+.1f} | block {bn}({bw}W) bp={bp:+.1f} | after {p-bp:+.1f}")

print('\n=== BLOCKED TRADES DETAIL @ plus_above>=80 ===')
blocked = sorted([r for r in R if r['plus_above_v']>=80], key=lambda x: x['ts'])
for r in blocked:
    print(f"  {r['ts_et'].strftime('%m-%d %H:%M')} {r['setup']:<15} {r['grade'] or '-':<4} spot={r['spot']:.0f} +aboveK={r['plus_above_k'] or 0:.0f}@{r['plus_above_v']:+.0f}M net_gex={r['net_gex']:+.0f} charm={(r['net_charm'] or 0)/1e6:+.0f}M pnl={r['pnl']:+.1f} {r['out']}")

# Important: check SC only with threshold 80
print('\n=== SC ONLY: plus_above threshold sweep ===')
sc = [r for r in R if r['setup']=='Skew Charm']
for thr in [40, 50, 60, 70, 80, 100, 120, 150]:
    blk = [r for r in sc if r['plus_above_v']>=thr]
    n,w,p,_ = stats(sc); bn,bw,bp,bwr = stats(blk)
    print(f"  SC plus_above>={thr}: blk {bn}({bw}W={bwr:.0f}%) bp={bp:+.1f} | after {p-bp:+.1f}")

print('\n=== DD ONLY: plus_above threshold sweep ===')
dd = [r for r in R if r['setup']=='DD Exhaustion']
for thr in [40, 50, 60, 70, 80, 100, 120, 150]:
    blk = [r for r in dd if r['plus_above_v']>=thr]
    n,w,p,_ = stats(dd); bn,bw,bp,bwr = stats(blk)
    print(f"  DD plus_above>={thr}: blk {bn}({bw}W={bwr:.0f}%) bp={bp:+.1f} | after {p-bp:+.1f}")

print('\n=== AG ONLY: plus_above threshold sweep ===')
ag = [r for r in R if r['setup']=='AG Short']
for thr in [40, 50, 60, 70, 80, 100, 120, 150]:
    blk = [r for r in ag if r['plus_above_v']>=thr]
    n,w,p,_ = stats(ag); bn,bw,bp,bwr = stats(blk)
    print(f"  AG plus_above>={thr}: blk {bn}({bw}W={bwr:.0f}%) bp={bp:+.1f} | after {p-bp:+.1f}")
