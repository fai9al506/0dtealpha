"""Vanna cliff v2: try weekly vanna + near-spot filtering."""
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
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-30' AND '2026-04-16'
  AND direction IN ('short','bearish')
  AND setup_name IN ('Skew Charm','DD Exhaustion','AG Short')
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""")
all_trades = cur.fetchall()
trades = []
for t in all_trades:
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, h, m = t
    if setup == 'Skew Charm' and grade not in ('A+', 'A', 'B'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and paradigm and paradigm.startswith('GEX-LIS'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and ((h == 14 and m >= 30) or h == 15): continue
    if setup == 'DD Exhaustion' and align == 0: continue
    trades.append(t)
print(f"V12-fix eligible shorts: {len(trades)}")

def get_cliff_peak(ts, spot, exp='TODAY', near_only=True):
    """Compute cliff + peak, optionally restricted to strikes near spot."""
    cur.execute("""
    WITH latest_ts AS (
      SELECT MAX(ts_utc) as mts FROM volland_exposure_points
      WHERE greek = 'vanna' AND expiration_option = %s AND ts_utc <= %s
        AND ts_utc >= %s - interval '15 minutes'
    )
    SELECT strike, value FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = %s
      AND ts_utc = (SELECT mts FROM latest_ts)
    ORDER BY strike
    """, (exp, ts, ts, exp))
    pts = cur.fetchall()
    if not pts: return (None, None)
    vdata = [(float(s), float(v)) for s, v in pts]
    # Filter near-spot: within +/- 50 pts
    if near_only:
        near = [(s, v) for s, v in vdata if abs(s - float(spot)) <= 50]
    else:
        near = vdata
    if not near: return (None, None)
    # Cliff: sign change in per-strike vanna CLOSEST to spot
    vd = sorted(near)
    cliff_strikes = []
    for i in range(1, len(vd)):
        s0, v0 = vd[i-1]
        s1, v1 = vd[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            # interpolate
            if v1 - v0 != 0:
                ratio = -v0 / (v1 - v0)
                cliff_strikes.append(s0 + ratio * (s1 - s0))
    cliff = min(cliff_strikes, key=lambda s: abs(s - float(spot))) if cliff_strikes else None
    # Peak of |vanna| in near-spot band
    peak = max(near, key=lambda x: abs(x[1]))[0]
    return (cliff, peak)

print("Computing v2 cliff (per-strike sign flip, within +/-50 pts of spot)...", flush=True)
results_today = []
results_week = []
for i, t in enumerate(trades):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, h, m = t
    cliff_t, peak_t = get_cliff_peak(ts, spot, 'TODAY', near_only=True)
    cliff_w, peak_w = get_cliff_peak(ts, spot, 'THIS_WEEK', near_only=True)
    if (i+1) % 20 == 0:
        print(f"  ...{i+1}/{len(trades)}", flush=True)
    r = {'id': tid, 'setup': setup, 'spot': float(spot),
         'cliff_t': cliff_t, 'peak_t': peak_t,
         'cliff_w': cliff_w, 'peak_w': peak_w,
         'outcome': outcome, 'pnl': float(pnl) if pnl else 0}
    results_today.append(r)

def bin_by(key, buckets):
    out = defaultdict(list)
    for r in results_today:
        if r[key] is None: continue
        d = r[key] - r['spot']
        for lo, hi, label in buckets:
            if lo <= d < hi:
                out[label].append(r); break
    return out

def fmt(name, rs):
    if not rs: return f"  {name:<12} (empty)"
    w = sum(1 for r in rs if r['outcome'] == 'WIN')
    l = sum(1 for r in rs if r['outcome'] == 'LOSS')
    pnl = sum(r['pnl'] for r in rs)
    wr = 100.0 * w / max(1, w + l)
    return f"  {name:<12} n={len(rs):>3}  W={w:>3} L={l:>3}  WR={wr:>5.1f}%  pnl={pnl:+7.1f}  avg={pnl/len(rs):+.2f}"

buckets = [(-100, -20, '<-20'), (-20, -10, '-20:-10'), (-10, -3, '-10:-3'),
           (-3, 3, 'at spot'), (3, 10, '+3:+10'), (10, 20, '+10:+20'), (20, 100, '>+20')]

print()
print("=== SHORT outcomes by TODAY-vanna CLIFF (near-spot sign flip) ===")
for _, _, label in buckets:
    print(fmt(label, bin_by('cliff_t', buckets).get(label, [])))

print()
print("=== SHORT outcomes by WEEKLY-vanna CLIFF (near-spot sign flip) ===")
for _, _, label in buckets:
    print(fmt(label, bin_by('cliff_w', buckets).get(label, [])))

print()
print("=== SHORT outcomes by TODAY-vanna PEAK (|max| in +/-50 pts) ===")
for _, _, label in buckets:
    print(fmt(label, bin_by('peak_t', buckets).get(label, [])))

print()
print("Coverage:")
print(f"  Today cliff found: {sum(1 for r in results_today if r['cliff_t'] is not None)}/{len(results_today)}")
print(f"  Weekly cliff found: {sum(1 for r in results_today if r['cliff_w'] is not None)}/{len(results_today)}")
print(f"  Today peak found: {sum(1 for r in results_today if r['peak_t'] is not None)}/{len(results_today)}")

# Cliff above vs below for shorts - weekly
print()
print("=== WEEKLY CLIFF: above vs below spot (shorts) ===")
above_w = [r for r in results_today if r['cliff_w'] is not None and r['cliff_w'] > r['spot']]
below_w = [r for r in results_today if r['cliff_w'] is not None and r['cliff_w'] <= r['spot']]
print(fmt('above', above_w))
print(fmt('below', below_w))

# For winners vs losers, mean cliff_w distance
wins = [r for r in results_today if r['outcome']=='WIN' and r['cliff_w'] is not None]
losses = [r for r in results_today if r['outcome']=='LOSS' and r['cliff_w'] is not None]
if wins: print(f"  Wins mean cliff_w offset: {statistics.mean([r['cliff_w']-r['spot'] for r in wins]):+.2f}")
if losses: print(f"  Losses mean cliff_w offset: {statistics.mean([r['cliff_w']-r['spot'] for r in losses]):+.2f}")
