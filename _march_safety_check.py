"""Safety check: does ≥70M filter harm March performance?"""
import pickle
from collections import defaultdict

with open('enriched_shorts.pkl','rb') as f: R = pickle.load(f)

march = [r for r in R if r['date'].month == 3 and r['setup'] in ('Skew Charm','DD Exhaustion')]

def stats(arr):
    if not arr: return (0,0,0.0)
    w = sum(1 for r in arr if r['out']=='WIN')
    return len(arr), w, sum(r['pnl'] for r in arr)

by_date = defaultdict(list)
for r in march: by_date[r['date']].append(r)

print(f"{'Date':<12} {'Day':<4} {'#sig':<5} {'Base':<9} {'Blk':<4} {'Blk$':<9} {'After':<9} {'Delta':<8}")
print('-'*70)
Tb_p = Tblk_p = 0.0; Tb_n = Tblk_n = 0; Tb_w = 0
for d in sorted(by_date.keys()):
    arr = by_date[d]
    n,w,p = stats(arr)
    blk = [r for r in arr if r['plus_above_v']>=70]
    bn,bw,bp = stats(blk)
    after = p - bp; delta = -bp
    print(f"{str(d):<12} {d.strftime('%a'):<4} {n:<5} {p:<+9.1f} {bn:<4} {bp:<+9.1f} {after:<+9.1f} {delta:<+8.1f}")
    Tb_p += p; Tb_n += n; Tb_w += w; Tblk_p += bp; Tblk_n += bn
print('-'*70)
print(f"{'TOTAL':<12} {'':<4} {Tb_n:<5} {Tb_p:<+9.1f} {Tblk_n:<4} {Tblk_p:<+9.1f} {Tb_p-Tblk_p:<+9.1f} {-Tblk_p:<+8.1f}")

print(f"\nMarch SC+DD total: {Tb_n}t, {Tb_w}W={Tb_w/Tb_n*100:.0f}% WR, base={Tb_p:+.1f}")
print(f"Blocks: {Tblk_n}t ({Tblk_n/Tb_n*100:.1f}% of signals)")

# Show blocked March trades if any
print('\n=== MARCH BLOCKED TRADES (if any) ===')
march_blk = sorted([r for r in march if r['plus_above_v']>=70], key=lambda x: x['ts'])
if not march_blk:
    print("  NONE — filter did not activate once in March.")
else:
    for r in march_blk:
        print(f"  {r['ts_et'].strftime('%m-%d %H:%M')} {r['setup']:<15} spot={r['spot']:.0f} magnet@{r['plus_above_k'] or 0:.0f} size={r['plus_above_v']:+.0f}M pnl={r['pnl']:+.1f} {r['out']}")

# Distribution of plus_above_v in March vs April
print('\n=== plus_above_v DISTRIBUTION: March vs April ===')
mar_v = [r['plus_above_v'] for r in march]
apr = [r for r in R if r['date'].month == 4 and r['setup'] in ('Skew Charm','DD Exhaustion')]
apr_v = [r['plus_above_v'] for r in apr]
import statistics as st
print(f"  March ({len(mar_v)}): median={st.median(mar_v):+.0f}M  p75={st.quantiles(mar_v,n=4)[2]:+.0f}M  max={max(mar_v):+.0f}M")
print(f"  April ({len(apr_v)}): median={st.median(apr_v):+.0f}M  p75={st.quantiles(apr_v,n=4)[2]:+.0f}M  max={max(apr_v):+.0f}M")

mar_ge70 = sum(1 for v in mar_v if v>=70)
apr_ge70 = sum(1 for v in apr_v if v>=70)
print(f"\n  March signals with plus_above>=70M: {mar_ge70}/{len(mar_v)} ({mar_ge70/len(mar_v)*100:.1f}%)")
print(f"  April signals with plus_above>=70M: {apr_ge70}/{len(apr_v)} ({apr_ge70/len(apr_v)*100:.1f}%)")
