import os, sys, psycopg2
from zoneinfo import ZoneInfo
sys.path.insert(0, '.')
import importlib.util
spec = importlib.util.spec_from_file_location("fc", "_tmp_filter_compare.py")
# reuse filter fns by exec
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; load = g['load']

print("=== APR-JUL AGGREGATE (matched mes population) ===")
print(f"{'filt':6} {'nMes':>5} {'chain(m)':>9} {'mesSum':>9} {'mes/trade':>10} {'WR%':>5}")
allrows = []
for mo in ['2026-04', '2026-05', '2026-06', '2026-07']:
    allrows += load(mo)
for fn, fx in FILTERS.items():
    sel = [l for l in allrows if fx(l)]
    m = [l for l in sel if l['mes_sim_outcome_pnl'] is not None and l['outcome_pnl'] is not None]
    chainM = sum(float(l['outcome_pnl']) for l in m)
    mesM = sum(float(l['mes_sim_outcome_pnl']) for l in m)
    dec = [l for l in m if l['outcome_result'] in ('WIN', 'LOSS', 'EXPIRED', 'TIMEOUT')]
    wr = 100 * sum(1 for l in dec if l['outcome_result'] == 'WIN') / max(1, len(dec))
    per = mesM / max(1, len(m))
    print(f"{fn:6} {len(m):>5} {chainM:>9.1f} {mesM:>9.1f} {per:>10.2f} {wr:>5.0f}")
