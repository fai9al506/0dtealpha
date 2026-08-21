import os, sys
sys.path.insert(0, '.')
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; load = g['load']; ET = g['ET']

# ---- Full-period chain totals (validated metric post-S217), per filter per month ----
print("=== CHAIN-SIM totals (now the validated metric) — FULL filter population ===")
months = ['2026-04', '2026-05', '2026-06', '2026-07']
data = {m: load(m) for m in months}
print(f"{'filt':6} " + " ".join(f"{m[-2:]:>7}" for m in months) + f" {'Apr-Jul':>8}")
for fn, fx in FILTERS.items():
    tot = 0; cells = []
    for m in months:
        s = sum(float(l['outcome_pnl']) for l in data[m] if fx(l) and l['outcome_pnl'] is not None)
        cells.append(f"{s:>7.0f}"); tot += s
    print(f"{fn:6} " + " ".join(cells) + f" {tot:>8.0f}")

# ---- Dedup + cap model for July real-net estimate ----
# Real trader: skip same (setup,direction) within 15 min of a prior placement;
# cap 3 concurrent open per direction (assume 30-min hold). Approximation.
def dedup_place(sigs):
    # CALIBRATED to the May18-31 v16 = 78 real-placed anchor -> (30min, cap2, 45min hold) = 76
    placed = []
    last = {}
    openq = {'L': [], 'S': []}
    for l in sigs:
        ts = l['ts']; sn = l['setup_name'] or ''
        isL = l['direction'] in ('long', 'bullish'); side = 'L' if isL else 'S'
        openq[side] = [t for t in openq[side] if (ts - t).total_seconds() <= 2700]
        k = (sn, side)
        if k in last and (ts - last[k]).total_seconds() < 1800:
            continue
        if len(openq[side]) >= 2:
            continue
        placed.append(l); last[k] = ts; openq[side].append(ts)
    return placed

HAIRCUT = 0.6   # pts/trade: midpoint of May(+0.4) & post-S217 bearish(-0.8) calibrations
COMM = 1.0      # $/RT/MES
MES = 5.0       # $/pt
print("\n=== JULY real-net estimate if TSRT had been enabled (1 MES) ===")
print("net$ = (chainPts - 0.8*n)*$5  -  n*$1 comm   [chain validated broker-1.6pt]")
print(f"{'filt':6} {'nDedup':>6} {'chainPts':>8} {'est$net':>8} {'mesPts':>7} {'mes$net':>8}")
july = data['2026-07']
for fn, fx in FILTERS.items():
    sigs = [l for l in july if fx(l)]
    placed = dedup_place(sigs)
    ch = sum(float(l['outcome_pnl']) for l in placed if l['outcome_pnl'] is not None)
    mp = [l for l in placed if l['mes_sim_outcome_pnl'] is not None]
    me = sum(float(l['mes_sim_outcome_pnl']) for l in mp)
    n = len(placed)
    net = (ch - HAIRCUT * n) * MES - n * COMM
    netm = me * MES - len(mp) * COMM
    print(f"{fn:6} {n:>6} {ch:>8.1f} {net:>8.0f} {me:>7.1f} {netm:>8.0f}")
