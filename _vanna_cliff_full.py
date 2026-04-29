"""Vanna cliff on FULL vanna-era sample (Feb 11 - Apr 16, 46 days)."""
import psycopg2
from collections import defaultdict
import statistics

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

cur.execute("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl, greek_alignment,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-11' AND '2026-04-16'
  AND direction IN ('short','bearish')
  AND setup_name IN ('Skew Charm','DD Exhaustion','AG Short')
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""")
all_t = cur.fetchall()
print(f"All SC/DD/AG shorts (full vanna era): {len(all_t)}")

# Apply V12-fix POST-HOC
v12 = []
for t in all_t:
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, h, m = t
    if setup == 'Skew Charm' and grade not in ('A+', 'A', 'B'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and paradigm and paradigm.startswith('GEX-LIS'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and ((h == 14 and m >= 30) or h == 15): continue
    if setup == 'DD Exhaustion' and align == 0: continue
    v12.append(t)
print(f"After V12-fix post-hoc: {len(v12)}")

def get_cliff(ts, spot):
    cur.execute("""
    WITH latest_ts AS (
      SELECT MAX(ts_utc) as mts FROM volland_exposure_points
      WHERE greek='vanna' AND expiration_option='THIS_WEEK' AND ts_utc <= %s
        AND ts_utc >= %s - interval '15 minutes'
    )
    SELECT strike, value FROM volland_exposure_points
    WHERE greek='vanna' AND expiration_option='THIS_WEEK'
      AND ts_utc = (SELECT mts FROM latest_ts)
    ORDER BY strike
    """, (ts, ts))
    pts = cur.fetchall()
    if not pts: return None
    near = [(float(s), float(v)) for s, v in pts if abs(float(s) - float(spot)) <= 50]
    if len(near) < 2: return None
    near.sort()
    crossings = []
    for i in range(1, len(near)):
        s0, v0 = near[i-1]
        s1, v1 = near[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1 - v0 != 0:
                crossings.append(s0 + (-v0/(v1-v0))*(s1-s0))
    if not crossings: return None
    return min(crossings, key=lambda s: abs(s - float(spot)))

print("Computing cliff for each trade...", flush=True)
results = []
for i, t in enumerate(v12):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, h, m = t
    cliff = get_cliff(ts, spot)
    results.append({'id': tid, 'setup': setup, 'spot': float(spot),
                    'cliff': cliff, 'outcome': outcome,
                    'pnl': float(pnl) if pnl else 0})
    if (i+1) % 50 == 0:
        print(f"  ...{i+1}/{len(v12)}", flush=True)

with_cliff = [r for r in results if r['cliff'] is not None]
print(f"Got cliff: {len(with_cliff)}/{len(results)}")

def fmt(name, rs):
    if not rs: return f"  {name:<18} (empty)"
    w = sum(1 for r in rs if r['outcome'] == 'WIN')
    l = sum(1 for r in rs if r['outcome'] == 'LOSS')
    pnl = sum(r['pnl'] for r in rs)
    wr = 100.0 * w / max(1, w + l)
    return f"  {name:<18} n={len(rs):>3}  W={w:>3} L={l:>3}  WR={wr:>5.1f}%  pnl={pnl:+7.1f}  avg={pnl/len(rs):+.2f}"

# Above vs below
above = [r for r in with_cliff if r['cliff'] > r['spot']]
below = [r for r in with_cliff if r['cliff'] <= r['spot']]
print()
print("=== FULL ERA WEEKLY CLIFF: above vs below spot (V12-fix shorts) ===")
print(fmt('cliff ABOVE', above))
print(fmt('cliff BELOW', below))

# Bucket
print()
print("=== By cliff-to-spot distance ===")
buckets = [(-100, -20, '<-20'), (-20, -10, '-20:-10'), (-10, -3, '-10:-3'),
           (-3, 3, 'at spot'), (3, 10, '+3:+10'), (10, 20, '+10:+20'), (20, 100, '>+20')]
for lo, hi, label in buckets:
    rs = [r for r in with_cliff if lo <= r['cliff']-r['spot'] < hi]
    print(fmt(label, rs))

# Per-setup breakdown for above vs below
print()
print("=== By setup (cliff above vs below) ===")
for setup in ['Skew Charm', 'DD Exhaustion', 'AG Short']:
    sub = [r for r in with_cliff if r['setup'] == setup]
    ab = [r for r in sub if r['cliff'] > r['spot']]
    be = [r for r in sub if r['cliff'] <= r['spot']]
    print(f"  {setup}:")
    print(fmt('    above', ab))
    print(fmt('    below', be))

# Monthly stability check — is signal consistent across months?
print()
print("=== Stability: monthly above vs below ===")
import datetime
cur.execute("""
SELECT id, EXTRACT(YEAR FROM (ts AT TIME ZONE 'America/New_York'))::int as y,
       EXTRACT(MONTH FROM (ts AT TIME ZONE 'America/New_York'))::int as mo
FROM setup_log WHERE id = ANY(%s)
""", ([r['id'] for r in with_cliff],))
month_map = {tid: (y, mo) for tid, y, mo in cur.fetchall()}
by_month = defaultdict(list)
for r in with_cliff:
    by_month[month_map.get(r['id'], (0, 0))].append(r)

for mk in sorted(by_month.keys()):
    rs = by_month[mk]
    ab = [r for r in rs if r['cliff'] > r['spot']]
    be = [r for r in rs if r['cliff'] <= r['spot']]
    pnl_ab = sum(r['pnl'] for r in ab)
    pnl_be = sum(r['pnl'] for r in be)
    print(f"  {mk[0]}-{mk[1]:02d}: above n={len(ab):>3} pnl={pnl_ab:+7.1f} | below n={len(be):>3} pnl={pnl_be:+7.1f}")
