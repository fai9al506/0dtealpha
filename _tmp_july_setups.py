import os, sys
sys.path.insert(0, '.')
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; load = g['load']
v16sb = FILTERS['v16sb']; isLongf = g['isLongf']

july = load('2026-07')

def blk(sn_dir_rows):
    n = len(sn_dir_rows); taken = sum(1 for l in sn_dir_rows if v16sb(l))
    return taken, n

from collections import defaultdict
agg = defaultdict(list)
for l in july:
    d = 'LONG ' if isLongf(l['direction']) else 'SHORT'
    agg[(d, l['setup_name'] or '?')].append(l)

print("=== JULY 2026 — every setup, both metrics (chain now validated for EXECUTED trades) ===")
print(f"{'dir':5} {'setup':18} {'n':>4} {'taken':>5} {'chain':>8} {'mes':>8} {'WR%':>4}  note")
def wr(rows):
    dec = [l for l in rows if l['outcome_result'] in ('WIN','LOSS','EXPIRED','TIMEOUT')]
    return 100*sum(1 for l in dec if l['outcome_result']=='WIN')/max(1,len(dec))
rowsout = []
for (d, sn), rows in agg.items():
    ch = sum(float(l['outcome_pnl']) for l in rows if l['outcome_pnl'] is not None)
    me = sum(float(l['mes_sim_outcome_pnl']) for l in rows if l['mes_sim_outcome_pnl'] is not None)
    taken, n = blk(rows)
    rowsout.append((d, sn, n, taken, ch, me, wr(rows)))
for d, sn, n, taken, ch, me, w in sorted(rowsout, key=lambda x: x[4]):
    note = ''
    if taken == 0 and n > 0: note = '<< BLOCKED by v16-sb'
    elif taken < n: note = f'({taken}/{n} taken)'
    print(f"{d:5} {sn[:18]:18} {n:>4} {taken:>5} {ch:>8.1f} {me:>8.1f} {w:>4.0f}  {note}")

print("\n=== What v16-sb ACTUALLY executed in July (the real book) ===")
taken = [l for l in july if v16sb(l)]
ch = sum(float(l['outcome_pnl']) for l in taken if l['outcome_pnl'] is not None)
me = sum(float(l['mes_sim_outcome_pnl']) for l in taken if l['mes_sim_outcome_pnl'] is not None)
byset = defaultdict(lambda: [0,0.0,0.0])
for l in taken:
    d='L' if isLongf(l['direction']) else 'S'
    k=f"{d} {l['setup_name']}"
    byset[k][0]+=1
    if l['outcome_pnl'] is not None: byset[k][1]+=float(l['outcome_pnl'])
    if l['mes_sim_outcome_pnl'] is not None: byset[k][2]+=float(l['mes_sim_outcome_pnl'])
for k in sorted(byset, key=lambda x:byset[x][1]):
    n,c,m=byset[k]; print(f"  {k[:22]:22} n={n:3d} chain={c:+7.1f} mes={m:+7.1f}")
print(f"  TOTAL v16-sb July: n={len(taken)} chain={ch:+.1f} mes={me:+.1f}")

print("\n=== The blocked SC/DD shorts: chain vs MES (sims DISAGREE = red flag) + high-vol behavior ===")
for label, months in [('JULY', ['2026-07']), ('APR+MAY high-vol', ['2026-04','2026-05'])]:
    rows=[]
    for m in months: rows+=load(m)
    for sn in ['Skew Charm','DD Exhaustion']:
        sh=[l for l in rows if (l['setup_name']==sn) and not isLongf(l['direction']) and not v16sb(l)]
        ch=sum(float(l['outcome_pnl']) for l in sh if l['outcome_pnl'] is not None)
        me=sum(float(l['mes_sim_outcome_pnl']) for l in sh if l['mes_sim_outcome_pnl'] is not None)
        print(f"  {label:16} SHORT {sn:14} n={len(sh):3d} chain={ch:+7.1f} mes={me:+7.1f}  gap={ch-me:+.0f}")
