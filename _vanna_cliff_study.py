"""Vanna cliff prototype: does 'cumulative vanna sign-flip strike' act as S/R?

For each V12-fix short (SC/DD/AG), compute:
  - cliff: strike where ascending cumulative vanna crosses zero (closest to spot)
  - peak: strike with largest |vanna|
Then bin outcomes by distance.
"""
import psycopg2
from collections import defaultdict

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

# 1. V12-fix candidate shorts
cur.execute("""
SELECT id, ts, setup_name, grade, paradigm, spot, lis,
       outcome_result, outcome_pnl,
       greek_alignment,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-30' AND '2026-04-16'
  AND direction IN ('short','bearish')
  AND setup_name IN ('Skew Charm','DD Exhaustion','AG Short')
  AND outcome_result IS NOT NULL
  AND spot IS NOT NULL
ORDER BY ts
""")
all_trades = cur.fetchall()
print(f"Total candidate shorts: {len(all_trades)}")

# Apply V12-fix filter to get actually traded shorts
trades = []
for t in all_trades:
    tid, ts, setup, grade, paradigm, spot, lis, outcome, pnl, align, h, m = t
    # V12-fix filter for shorts
    if setup == 'Skew Charm' and grade not in ('A+', 'A', 'B'):
        continue  # grade gate
    if setup in ('Skew Charm', 'DD Exhaustion') and paradigm and paradigm.startswith('GEX-LIS'):
        continue
    if setup in ('Skew Charm', 'DD Exhaustion') and ((h == 14 and m >= 30) or h == 15):
        continue
    if setup == 'DD Exhaustion' and align == 0:
        continue
    trades.append(t)
print(f"V12-fix eligible: {len(trades)}")

# 2. For each trade, get the nearest vanna_TODAY snapshot using a fast DISTINCT ON query
print("Pulling nearest vanna snapshots for each trade...")
cliff_map = {}
peak_map = {}
for i, t in enumerate(trades):
    tid, ts, setup, grade, paradigm, spot, lis, outcome, pnl, align, h, m = t
    cur.execute("""
    WITH latest_ts AS (
      SELECT MAX(ts_utc) as mts FROM volland_exposure_points
      WHERE greek = 'vanna' AND expiration_option = 'TODAY' AND ts_utc <= %s
        AND ts_utc >= %s - interval '15 minutes'
    )
    SELECT strike, value FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'TODAY'
      AND ts_utc = (SELECT mts FROM latest_ts)
    ORDER BY strike
    """, (ts, ts))
    pts = cur.fetchall()
    if not pts:
        continue
    vdata = [(float(s), float(v)) for s, v in pts]
    vdata.sort()
    # Cliff: where ascending cumulative vanna crosses zero (closest to spot)
    cum = 0
    prev_s = None
    prev_cum = 0
    crossings = []
    for s, v in vdata:
        new_cum = prev_cum + v
        if prev_s is not None and ((prev_cum > 0 and new_cum < 0) or (prev_cum < 0 and new_cum > 0)):
            if new_cum - prev_cum != 0:
                ratio = -prev_cum / (new_cum - prev_cum)
                crossings.append(prev_s + ratio * (s - prev_s))
        prev_s = s
        prev_cum = new_cum
    if crossings:
        cliff_map[tid] = min(crossings, key=lambda x: abs(x - float(spot)))
    # Peak
    peak_map[tid] = max(vdata, key=lambda x: abs(x[1]))[0]
    if (i+1) % 20 == 0:
        print(f"  ...{i+1}/{len(trades)}")
print(f"Got cliff for {len(cliff_map)}/{len(trades)} trades")

# 3. Analysis
results = []
for t in trades:
    tid, ts, setup, grade, paradigm, spot, lis, outcome, pnl, align, h, m = t
    results.append({
        'id': tid, 'setup': setup, 'spot': float(spot),
        'lis': float(lis) if lis else None,
        'cliff': cliff_map.get(tid),
        'peak': peak_map.get(tid),
        'outcome': outcome, 'pnl': float(pnl) if pnl else 0,
    })

def bin_by(key, buckets):
    out = defaultdict(list)
    for r in results:
        if r[key] is None:
            continue
        d = r[key] - r['spot']
        for lo, hi, label in buckets:
            if lo <= d < hi:
                out[label].append(r)
                break
    return out

def fmt(name, rs):
    if not rs: return f"  {name:<12} (empty)"
    w = sum(1 for r in rs if r['outcome'] == 'WIN')
    l = sum(1 for r in rs if r['outcome'] == 'LOSS')
    e = sum(1 for r in rs if r['outcome'] == 'EXPIRED')
    pnl = sum(r['pnl'] for r in rs)
    wr = 100.0 * w / max(1, w + l)
    return f"  {name:<12} n={len(rs):>3}  W={w:>3} L={l:>3} E={e:>2}  WR={wr:>5.1f}%  pnl={pnl:+7.1f}  avg={pnl/len(rs):+.2f}"

buckets = [(-500, -30, '<-30'), (-30, -15, '-30:-15'), (-15, -5, '-15:-5'),
           (-5, 5, '-5:+5'), (5, 15, '+5:+15'), (15, 30, '+15:+30'), (30, 500, '>+30')]

print()
print("=== SHORT outcomes by VANNA CLIFF distance (spot -> cliff) ===")
for _, _, label in buckets:
    rs = bin_by('cliff', buckets).get(label, [])
    print(fmt(label, rs))

print()
print("=== SHORT outcomes by LIS distance (spot -> LIS) ===")
for _, _, label in buckets:
    rs = bin_by('lis', buckets).get(label, [])
    print(fmt(label, rs))

print()
print("=== SHORT outcomes by |VANNA PEAK| distance (spot -> peak) ===")
for _, _, label in buckets:
    rs = bin_by('peak', buckets).get(label, [])
    print(fmt(label, rs))

# Sign: cliff above vs below spot for shorts
print()
print("=== SHORT: cliff ABOVE vs BELOW spot ===")
above = [r for r in results if r['cliff'] is not None and r['cliff'] > r['spot']]
below = [r for r in results if r['cliff'] is not None and r['cliff'] <= r['spot']]
print(fmt('cliff ABOVE', above))
print(fmt('cliff BELOW', below))

print()
print("=== SHORT: cliff closer than LIS? ===")
has_both = [r for r in results if r['cliff'] is not None and r['lis'] is not None]
cliff_closer = [r for r in has_both if abs(r['cliff']-r['spot']) < abs(r['lis']-r['spot'])]
lis_closer = [r for r in has_both if abs(r['lis']-r['spot']) <= abs(r['cliff']-r['spot'])]
print(fmt('cliff closer', cliff_closer))
print(fmt('LIS closer', lis_closer))

# Average distance
import statistics
cliff_dists = [abs(r['cliff']-r['spot']) for r in has_both]
lis_dists = [abs(r['lis']-r['spot']) for r in has_both]
print(f"  Mean |cliff-spot| = {statistics.mean(cliff_dists):.1f} pts")
print(f"  Mean |LIS-spot|   = {statistics.mean(lis_dists):.1f} pts")
