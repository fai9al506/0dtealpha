import os, sys, psycopg2
sys.path.insert(0, '.')
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; load = g['load']
v16 = FILTERS['v16']; v16sb = FILTERS['v16sb']; isLongf = g['isLongf']
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True; cur = c.cursor()
MES = 5.0; COMM = 1.0
months = ['2026-03', '2026-04', '2026-05', '2026-06', '2026-07']
data = {m: load(m) for m in months}

# ---- (4/low-vol) VIX + overvix by month ----
print("=== VIX / overvix by month (define 'low-vol') ===")
for m in months:
    vs = [float(l['vix']) for l in data[m] if l['vix'] is not None]
    ov = [float(l['overvix']) for l in data[m] if l['overvix'] is not None]
    print(f"  {m}: avgVIX={sum(vs)/len(vs):.1f}  minVIX={min(vs):.1f} maxVIX={max(vs):.1f}  avgOvervix(VIX-VIX3M)={sum(ov)/len(ov):+.2f}")

# ---- ($ per setup per month, WITH vs WITHOUT SB gate) ----
def setup_dollars(rows, fx):
    from collections import defaultdict
    d = defaultdict(float)
    for l in rows:
        if fx(l) and l['outcome_pnl'] is not None:
            k = ('L ' if isLongf(l['direction']) else 'S ') + (l['setup_name'] or '?')
            d[k] += float(l['outcome_pnl']) * MES - COMM
    return d

SETUPS = ['L Skew Charm','S Skew Charm','L DD Exhaustion','L ES Absorption','S ES Absorption',
          'S AG Short','L GEX Long','L VIX Divergence','L Vanna Pivot Bounce','S Vanna Pivot Bounce']
print("\n=== $ per setup per month: v16-BASE (no gate) vs v16-SB (gate) ===  [1 MES, chain$]")
hdr = "setup".ljust(20) + "".join(f"{m[-2:]:>16}" for m in months)
print(hdr); print("(each cell: base / sb)")
base = {m: setup_dollars(data[m], v16) for m in months}
sb = {m: setup_dollars(data[m], v16sb) for m in months}
for s in SETUPS:
    cells = []
    for m in months:
        b = base[m].get(s, 0); v = sb[m].get(s, 0)
        cells.append(f"{b:>7.0f}/{v:<7.0f}")
    print(f"{s:20}" + "".join(f"{c:>16}" for c in cells))
# totals
print(f"{'TOTAL':20}" + "".join(f"{sum(base[m].values()):>7.0f}/{sum(sb[m].values()):<7.0f}"[:16].rjust(16) for m in months))

# ---- (3) ES Abs short per month ----
print("\n=== (3) ES Absorption SHORT by month (is it a loser everywhere?) — v16-sb taken ===")
for m in months:
    sel = [l for l in data[m] if v16sb(l) and l['setup_name']=='ES Absorption' and not isLongf(l['direction'])]
    ch = sum(float(l['outcome_pnl']) for l in sel if l['outcome_pnl'] is not None)
    dec=[l for l in sel if l['outcome_result'] in ('WIN','LOSS','EXPIRED','TIMEOUT')]
    wr=100*sum(1 for l in dec if l['outcome_result']=='WIN')/max(1,len(dec))
    print(f"  {m}: n={len(sel):2d} chain$={ch*MES-len(sel)*COMM:>7.0f} WR={wr:.0f}%")

# ---- (1) What Volland magnet fields exist on setup_log + exposure availability ----
print("\n=== Volland magnet data available for the long-gate ===")
cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='setup_log'
   AND (column_name ILIKE '%vanna%' OR column_name ILIKE '%charm%' OR column_name ILIKE '%gex%'
        OR column_name ILIKE '%dd%' OR column_name ILIKE '%gamma%' OR column_name ILIKE '%lis%'
        OR column_name ILIKE '%magnet%' OR column_name ILIKE '%spot%') ORDER BY 1""")
print("  setup_log magnet-ish cols:", [r[0] for r in cur.fetchall()])
cur.execute("""SELECT count(*), min(ts AT TIME ZONE 'America/New_York')::date, max(ts AT TIME ZONE 'America/New_York')::date
   FROM volland_exposure_points WHERE exposure_type ILIKE '%vanna%'""")
r=cur.fetchone(); print(f"  volland_exposure_points (vanna): rows={r[0]} range {r[1]}..{r[2]}")
cur.execute("SELECT DISTINCT exposure_type FROM volland_exposure_points LIMIT 30")
print("  exposure_types:", sorted(set(r[0] for r in cur.fetchall())))
