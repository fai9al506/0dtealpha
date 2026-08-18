import os, sys
sys.path.insert(0, '.')
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; load = g['load']
v16sb = FILTERS['v16sb']; isLongf = g['isLongf']
from collections import defaultdict
MES = 5.0; COMM = 1.0

def block(months, title):
    rows = []
    for m in months: rows += load(m)
    taken = [l for l in rows if v16sb(l)]
    byset = defaultdict(lambda: [0, 0.0, 0.0, 0, 0])  # n, chain, mes, wins, dec
    for l in taken:
        d = 'L' if isLongf(l['direction']) else 'S'
        k = f"{d} {l['setup_name']}"
        byset[k][0] += 1
        if l['outcome_pnl'] is not None: byset[k][1] += float(l['outcome_pnl'])
        if l['mes_sim_outcome_pnl'] is not None: byset[k][2] += float(l['mes_sim_outcome_pnl'])
        if l['outcome_result'] in ('WIN','LOSS','EXPIRED','TIMEOUT'):
            byset[k][4] += 1
            if l['outcome_result'] == 'WIN': byset[k][3] += 1
    print(f"\n===== V16-SB executed book: {title} =====")
    print(f"{'setup':22} {'n':>4} {'chainPts':>8} {'chain$':>8} {'WR%':>4}")
    tc = tn = 0
    for k in sorted(byset, key=lambda x: byset[x][1]):
        n, c, m, w, dec = byset[k]
        wr = 100 * w / max(1, dec)
        print(f"{k[:22]:22} {n:>4} {c:>8.1f} {c*MES-n*COMM:>8.0f} {wr:>4.0f}")
        tc += c; tn += n
    print(f"{'TOTAL':22} {tn:>4} {tc:>8.1f} {tc*MES-tn*COMM:>8.0f}")
    print(f"   (chain$ = pts*$5 - $1/trade comm, 1 MES; chain validated ~broker post-S217)")

block(['2026-07'], 'JULY 2026')
block(['2026-04','2026-05','2026-06','2026-07'], 'APR-JUL 2026 (4 months)')
block(['2026-05','2026-06','2026-07'], 'MAY-JUL 2026 (post-war-peak)')
