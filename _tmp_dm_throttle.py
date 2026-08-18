import os, sys, psycopg2
from collections import defaultdict
sys.path.insert(0, '.')
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; isLongf = g['isLongf']; ET = g['ET']
v16sb = FILTERS['v16sb']
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True; cur = c.cursor()
MES = 5.0; COMM = 1.0
COLS = ("id, setup_name, direction, greek_alignment, grade, paradigm, vix, overvix, ts, "
        "v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side, basket_pct, "
        "outcome_pnl, mes_sim_outcome_pnl, outcome_result, spot_vol_beta")
KEYS = [k.strip() for k in COLS.split(',')]
def load(m):
    cur.execute(f"SELECT {COLS} FROM setup_log WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')=%s ORDER BY ts",(m,))
    return [dict(zip(KEYS, r)) for r in cur.fetchall()]
months = ['2026-03','2026-04','2026-05','2026-06','2026-07']
data = {m: load(m) for m in months}

# does spot_vol_beta separate low-vol-GOOD (May) from low-vol-BAD (Jul)?
print("=== spot_vol_beta by month (DM regime switch) ===")
for m in months:
    b = [float(l['spot_vol_beta']) for l in data[m] if l['spot_vol_beta'] is not None]
    neg = 100*sum(1 for x in b if x < 0)/len(b) if b else 0
    print(f"  {m}: avg_svb={sum(b)/len(b):+.2f}  %days_svb<0={neg:.0f}%  n={len(b)}")

def longs(rows): return [l for l in rows if v16sb(l) and isLongf(l['direction'])]
def dol(sel): return sum(float(l['outcome_pnl'])*MES-COMM for l in sel if l['outcome_pnl'] is not None)

# Throttle A: skip LONGS when signal-time spot_vol_beta < 0
print("\n=== Throttle A: skip longs when spot_vol_beta < 0 (per signal) ===")
print(f"{'month':8}{'baseLong$':>10}{'gatedLong$':>11}{'delta':>8}")
for m in months:
    base = dol(longs(data[m]))
    gated = dol([l for l in longs(data[m]) if not (l['spot_vol_beta'] is not None and float(l['spot_vol_beta']) < 0)])
    print(f"{m:8}{base:>10.0f}{gated:>11.0f}{gated-base:>+8.0f}")

# Throttle B: persistence - skip longs when PRIOR-DAY avg spot_vol_beta < 0 (regime confirmed)
print("\n=== Throttle B: skip longs when PRIOR trading-day avg spot_vol_beta < 0 (persistence) ===")
# build daily avg svb across all months
dayavg = {}
for m in months:
    tmp = defaultdict(list)
    for l in data[m]:
        if l['spot_vol_beta'] is not None:
            d = l['ts'].astimezone(ET).date()
            tmp[d].append(float(l['spot_vol_beta']))
    for d, v in tmp.items(): dayavg[d] = sum(v)/len(v)
alldays = sorted(dayavg)
prevday = {alldays[i]: alldays[i-1] for i in range(1, len(alldays))}
def prior_neg(l):
    d = l['ts'].astimezone(ET).date()
    pd = prevday.get(d)
    return pd is not None and dayavg.get(pd, 0) < 0
print(f"{'month':8}{'baseLong$':>10}{'gatedLong$':>11}{'delta':>8}")
for m in months:
    base = dol(longs(data[m]))
    gated = dol([l for l in longs(data[m]) if not prior_neg(l)])
    print(f"{m:8}{base:>10.0f}{gated:>11.0f}{gated-base:>+8.0f}")

print("\nROBUSTNESS (throttle B): HIvol(Mar-May) vs LOvol(Jun-Jul) long-$ delta")
hi = sum(dol([l for l in longs(data[m]) if not prior_neg(l)]) - dol(longs(data[m])) for m in ['2026-03','2026-04','2026-05'])
lo = sum(dol([l for l in longs(data[m]) if not prior_neg(l)]) - dol(longs(data[m])) for m in ['2026-06','2026-07'])
print(f"  dHIvol={hi:+.0f}  dLOvol={lo:+.0f}  -> {'ROBUST' if lo>0 and hi>=-50 else 'still costs high-vol' if lo>0 else 'no help'}")

# ---- Credit-spread proxy for SHORT setups in low-vol ----
print("\n=== Credit-spread opportunity: low-vol (Jun+Jul) SHORT setups outcome mix ===")
lowvol = data['2026-06'] + data['2026-07']
shorts = [l for l in lowvol if v16sb(l) and not isLongf(l['direction'])]
mix = defaultdict(int)
for l in shorts:
    mix[l['outcome_result']] += 1
tot = sum(mix.values())
print(f"  taken shorts n={tot}: " + ", ".join(f"{k}={v}({100*v/tot:.0f}%)" for k,v in sorted(mix.items())))
print("  Directional MES: WIN=+target, LOSS=-stop, EXPIRED~0.  Credit-spread: WIN+EXPIRED collect credit; only LOSS(breach) pays out.")
exp_rate = (mix.get('EXPIRED',0)+mix.get('TIMEOUT',0))/max(1,tot)
print(f"  EXPIRED/flat rate = {100*exp_rate:.0f}% -> these are ~$0 directional but WINS for a credit spread (theta).")
