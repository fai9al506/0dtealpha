import os, sys, psycopg2
sys.path.insert(0, '.')
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; isLongf = g['isLongf']
v16sb = FILTERS['v16sb']
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True; cur = c.cursor()
ET = g['ET']; MES = 5.0; COMM = 1.0

# richer load incl volland magnet fields
COLS = ("id, setup_name, direction, greek_alignment, grade, paradigm, vix, overvix, ts, "
        "v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side, basket_pct, "
        "outcome_pnl, mes_sim_outcome_pnl, outcome_result, spot_vol_beta, vanna_regime, "
        "vanna_all, spot, max_plus_gex")
KEYS = [k.strip() for k in COLS.split(',')]
def load(m):
    cur.execute(f"SELECT {COLS} FROM setup_log WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')=%s ORDER BY ts", (m,))
    return [dict(zip(KEYS, r)) for r in cur.fetchall()]
months = ['2026-03','2026-04','2026-05','2026-06','2026-07']
data = {m: load(m) for m in months}

# distributions to sanity-check thresholds
import statistics
allrows = [l for m in months for l in data[m]]
svb = [float(l['spot_vol_beta']) for l in allrows if l['spot_vol_beta'] is not None]
print(f"spot_vol_beta: n={len(svb)} of {len(allrows)}  min={min(svb):.2f} med={statistics.median(svb):.2f} max={max(svb):.2f}")
vr = set(str(l['vanna_regime']) for l in allrows if l['vanna_regime'] is not None)
print("vanna_regime values:", vr)

def longs(rows): return [l for l in rows if v16sb(l) and isLongf(l['direction'])]
def dol(sel): return sum(float(l['outcome_pnl'])*MES - COMM for l in sel if l['outcome_pnl'] is not None)

# candidate LONG gates (block the long if gate returns True)
def gate_overvix(l): return (l['overvix'] if l['overvix'] is not None else 0) < -2.0
def gate_svb(l):     return (l['spot_vol_beta'] is not None and float(l['spot_vol_beta']) > 1.0)  # undervixed
def gate_gexvoid(l):  # TS-GEX void: no +GEX strike within 15pt above spot
    sp=l['spot']; mp=l['max_plus_gex']
    if sp is None or mp is None: return False
    return not (sp < mp <= sp+15)
def gate_vanna_neg_above(l):
    # desk: vanna negative above spot = no upside magnet. proxy via vanna_regime label if present
    vr=str(l['vanna_regime'] or '')
    return 'NEG' in vr.upper() or 'BEAR' in vr.upper()

GATES = {'overvix<-2':gate_overvix, 'svb>1(undervix)':gate_svb, 'TSgexVoid':gate_gexvoid, 'vannaRegNeg':gate_vanna_neg_above}

print("\n=== LONG-side $ per month: baseline vs each gate applied (block long if gate True) ===")
print(f"{'gate':16}" + "".join(f"{m[-2:]:>9}" for m in months) + f"{'HIvol(MAM)':>11}{'LOvol(JJ)':>10}")
base_by = {m: dol(longs(data[m])) for m in months}
print(f"{'BASELINE':16}" + "".join(f"{base_by[m]:>9.0f}" for m in months) +
      f"{base_by['2026-03']+base_by['2026-04']+base_by['2026-05']:>11.0f}{base_by['2026-06']+base_by['2026-07']:>10.0f}")
for gn, gf in GATES.items():
    row = {}
    for m in months:
        kept = [l for l in longs(data[m]) if not gf(l)]
        row[m] = dol(kept)
    hi = row['2026-03']+row['2026-04']+row['2026-05']; lo = row['2026-06']+row['2026-07']
    print(f"{gn:16}" + "".join(f"{row[m]:>9.0f}" for m in months) + f"{hi:>11.0f}{lo:>10.0f}")

print("\n=== interpretation aid: how many longs each gate blocks, and their $ ===")
for gn, gf in GATES.items():
    blocked = [l for m in months for l in longs(data[m]) if gf(l)]
    print(f"  {gn:16} blocks {len(blocked):3d} longs worth ${dol(blocked):>7.0f} (removing them = +/- that)")

print("\n=== ROBUSTNESS: gate must HELP low-vol (Jun-Jul) WITHOUT hurting high-vol (Mar-May) ===")
for gn, gf in GATES.items():
    d_hi = sum(dol([l for l in longs(data[m]) if not gf(l)]) for m in ['2026-03','2026-04','2026-05']) - (base_by['2026-03']+base_by['2026-04']+base_by['2026-05'])
    d_lo = sum(dol([l for l in longs(data[m]) if not gf(l)]) for m in ['2026-06','2026-07']) - (base_by['2026-06']+base_by['2026-07'])
    verdict = 'PASS' if (d_lo>0 and d_hi>=-50) else ('low-vol help but HIGH-VOL COST' if d_lo>0 else 'no low-vol help')
    print(f"  {gn:16} dHIvol={d_hi:>+7.0f}  dLOvol={d_lo:>+7.0f}   -> {verdict}")
