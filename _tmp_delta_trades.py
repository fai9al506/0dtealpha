import os, sys
sys.path.insert(0, '.')
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; load = g['load']
v7 = FILTERS['v7']; v16sb = FILTERS['v16sb']; v16 = FILTERS['v16']

from collections import defaultdict
for label, months in [('JUN+JUL (low-vol)', ['2026-06', '2026-07']), ('APR+MAY (high-vol war)', ['2026-04', '2026-05'])]:
    rows = []
    for m in months: rows += load(m)
    # trades v7 takes but v16-sb blocks
    delta = [l for l in rows if v7(l) and not v16sb(l)]
    print(f"\n===== {label}: trades v7 TAKES but v16-SB BLOCKS =====")
    agg = defaultdict(lambda: [0, 0.0, 0.0])
    for l in delta:
        d = 'LONG' if l['direction'] in ('long', 'bullish') else 'SHORT'
        k = f"{d} {l['setup_name']}"
        agg[k][0] += 1
        if l['outcome_pnl'] is not None: agg[k][1] += float(l['outcome_pnl'])
        if l['mes_sim_outcome_pnl'] is not None: agg[k][2] += float(l['mes_sim_outcome_pnl'])
    tot_c = tot_m = 0
    for k in sorted(agg, key=lambda x: -agg[x][1]):
        n, ch, me = agg[k]; tot_c += ch; tot_m += me
        print(f"  {k[:26]:26} n={n:3d}  chain={ch:+7.1f}  mes={me:+7.1f}")
    print(f"  {'TOTAL delta (v7 minus v16sb)':26} chain={tot_c:+7.1f}  mes={tot_m:+7.1f}")
    # and what v16sb takes that v7 does NOT (the longs v7's align>=2 drops)
    delta2 = [l for l in rows if v16sb(l) and not v7(l)]
    c2 = sum(float(l['outcome_pnl']) for l in delta2 if l['outcome_pnl'] is not None)
    m2 = sum(float(l['mes_sim_outcome_pnl']) for l in delta2 if l['mes_sim_outcome_pnl'] is not None)
    print(f"  (reverse: v16sb takes but v7 blocks: n={len(delta2)} chain={c2:+.1f} mes={m2:+.1f})")
