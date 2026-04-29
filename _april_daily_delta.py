"""April-only daily delta with strong +GEX above filter (≥70M) on SC+DD."""
import pickle
from collections import defaultdict
from datetime import datetime

with open('enriched_shorts.pkl','rb') as f: R = pickle.load(f)

# April only, SC+DD (not AG)
april = [r for r in R if r['date'].month == 4 and r['setup'] in ('Skew Charm','DD Exhaustion')]

def stats(arr):
    if not arr: return (0,0,0.0,0.0)
    w = sum(1 for r in arr if r['out']=='WIN')
    return len(arr), w, sum(r['pnl'] for r in arr), 0

by_date = defaultdict(list)
for r in april: by_date[r['date']].append(r)

print(f"{'Date':<12} {'Day':<4} {'#sig':<5} {'Base':<9} {'Blk':<4} {'Blk$':<9} {'After':<9} {'Delta':<8}")
print('-'*70)
T_base_p = T_blk_p = 0.0; T_base_n = T_blk_n = 0
for d in sorted(by_date.keys()):
    arr = by_date[d]
    n,w,p,_ = stats(arr)
    blk = [r for r in arr if r['plus_above_v']>=70]
    bn,bw,bp,_ = stats(blk)
    after = p - bp
    delta = -bp
    wday = d.strftime('%a')
    print(f"{str(d):<12} {wday:<4} {n:<5} {p:<+9.1f} {bn:<4} {bp:<+9.1f} {after:<+9.1f} {delta:<+8.1f}")
    T_base_p += p; T_base_n += n; T_blk_p += bp; T_blk_n += bn
print('-'*70)
print(f"{'TOTAL':<12} {'':<4} {T_base_n:<5} {T_base_p:<+9.1f} {T_blk_n:<4} {T_blk_p:<+9.1f} {T_base_p-T_blk_p:<+9.1f} {-T_blk_p:<+8.1f}")

# Show blocked trades for April
print('\n=== APRIL BLOCKED TRADES DETAIL ===')
apr_blk = sorted([r for r in april if r['plus_above_v']>=70], key=lambda x: x['ts'])
for r in apr_blk:
    k = r['plus_above_k'] or 0
    dist = k - r['spot'] if k else 0
    print(f"  {r['ts_et'].strftime('%m-%d %H:%M')} {r['setup']:<15} {r['grade'] or '-':<4} spot={r['spot']:.0f} magnet@{k:.0f}(+{dist:.0f}pts) size={r['plus_above_v']:+.0f}M pnl={r['pnl']:+.1f} {r['out']}")
