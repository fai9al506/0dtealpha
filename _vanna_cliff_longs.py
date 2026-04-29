"""Test cliff signal on LONGS + explore peak-vanna as bidirectional filter."""
import psycopg2
from collections import defaultdict

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

# Pull LONGS from V12-fix era (alignment>=+2 AND (SC OR vix<=22 OR overvix>=+2))
cur.execute("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
       greek_alignment, vix, overvix, direction
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-11' AND '2026-04-16'
  AND direction IN ('long','bullish')
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""")
all_longs = cur.fetchall()

v12_longs = []
for t in all_longs:
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx = t
    # V12-fix long filter
    if align is None or align < 2: continue
    vix_f = float(vix) if vix else None
    ovx_f = float(ovx) if ovx else None
    # Allow if SC OR VIX<=22 OR overvix>=+2
    sc_exempt = setup == 'Skew Charm'
    vix_ok = vix_f is not None and vix_f <= 22
    ovx_ok = ovx_f is not None and ovx_f >= 2
    if not (sc_exempt or vix_ok or ovx_ok): continue
    v12_longs.append(t)
print(f"V12-fix eligible LONGS (Feb 11 - Apr 16): {len(v12_longs)}")
long_baseline = sum(float(t[7] or 0) for t in v12_longs)
print(f"Long baseline PnL: {long_baseline:+.1f}")

def fetch_near(ts, spot, exp='THIS_WEEK', band=50):
    cur.execute("""
    WITH latest_ts AS (
      SELECT MAX(ts_utc) as mts FROM volland_exposure_points
      WHERE greek='vanna' AND expiration_option=%s AND ts_utc <= %s
        AND ts_utc >= %s - interval '15 minutes'
    )
    SELECT strike, value FROM volland_exposure_points
    WHERE greek='vanna' AND expiration_option=%s
      AND ts_utc = (SELECT mts FROM latest_ts)
    ORDER BY strike
    """, (exp, ts, ts, exp))
    pts = cur.fetchall()
    if not pts: return None
    near = [(float(s), float(v)) for s, v in pts if abs(float(s) - float(spot)) <= band]
    return near if len(near) >= 2 else None

def basic_cliff(near):
    s0 = sorted(near)
    crossings = []
    for i in range(1, len(s0)):
        x0, v0 = s0[i-1]; x1, v1 = s0[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1 - v0 != 0: crossings.append(x0 + (-v0/(v1-v0))*(x1-x0))
    return crossings

def peak_strike(near):
    return max(near, key=lambda x: abs(x[1]))[0]

print("Computing cliff + peak for each long...", flush=True)
results = []
for i, t in enumerate(v12_longs):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx = t
    near = fetch_near(ts, spot)
    if not near:
        results.append({'setup': setup, 'outcome': outcome, 'pnl': float(pnl or 0),
                        'cliff_side': 'NODATA', 'peak_side': 'NODATA', 'spot': float(spot)})
        continue
    cs = basic_cliff(near)
    cliff_side = None
    if cs:
        nearest = min(cs, key=lambda s: abs(s - float(spot)))
        cliff_side = 'ABOVE' if nearest > float(spot) else 'BELOW'
    pk = peak_strike(near)
    peak_side = 'ABOVE' if pk > float(spot) else 'BELOW'
    results.append({'setup': setup, 'outcome': outcome, 'pnl': float(pnl or 0),
                    'cliff_side': cliff_side, 'peak_side': peak_side, 'spot': float(spot)})
    if (i+1) % 50 == 0: print(f"  ...{i+1}/{len(v12_longs)}", flush=True)

def fmt(name, rs):
    if not rs: return f"  {name:<20} (empty)"
    w = sum(1 for r in rs if r['outcome']=='WIN')
    l = sum(1 for r in rs if r['outcome']=='LOSS')
    pnl = sum(r['pnl'] for r in rs)
    wr = 100*w/max(1,w+l)
    return f"  {name:<20} n={len(rs):>3} W={w:>3} L={l:>3} WR={wr:>5.1f}% pnl={pnl:+7.1f} avg={pnl/len(rs):+.2f}"

print()
print("=== LONGS by cliff side (THIS_WEEK basic) ===")
for side in ['ABOVE', 'BELOW', 'NODATA', None]:
    if side is None: continue
    rs = [r for r in results if r['cliff_side'] == side]
    print(fmt(f"cliff {side}", rs))

print()
print("=== LONGS by cliff side x setup ===")
for setup in sorted(set(r['setup'] for r in results)):
    for side in ['ABOVE', 'BELOW']:
        rs = [r for r in results if r['setup']==setup and r['cliff_side']==side]
        if rs:
            print(fmt(f"{setup[:15]:<15} {side}", rs))

print()
print("=== LONGS by peak |vanna| side ===")
for side in ['ABOVE', 'BELOW']:
    rs = [r for r in results if r['peak_side']==side]
    print(fmt(f"peak {side}", rs))

# Intersection: cliff AND peak both agree
print()
print("=== LONGS: cliff+peak concordance ===")
for cs in ['ABOVE', 'BELOW']:
    for ps in ['ABOVE', 'BELOW']:
        rs = [r for r in results if r['cliff_side']==cs and r['peak_side']==ps]
        label = f"cliff={cs},peak={ps}"
        if rs: print(fmt(label, rs))

# ---- Back to SHORTS: compare with improvement magnitude ----
print()
print("=" * 70)
print("COMBINED BIDIRECTIONAL FILTER EVALUATION")
print("=" * 70)

# Block DD-shorts when cliff-above, ADD longs when cliff-above
# (actually this is conceptual — we don't automatically add longs since they require other setups.
# But we can compute: if we were to trust cliff-above-long as a bias, how much extra?)

dd_above_saved = 106.3  # from earlier
print(f"Short-side savings (DD-above block): +{dd_above_saved:.1f}")

long_total = sum(r['pnl'] for r in results)
long_above = [r for r in results if r['cliff_side'] == 'ABOVE']
long_below = [r for r in results if r['cliff_side'] == 'BELOW']
print(f"Long baseline: +{long_total:.1f}")
print(f"Long when cliff ABOVE: {sum(r['pnl'] for r in long_above):+.1f} ({len(long_above)}t)")
print(f"Long when cliff BELOW: {sum(r['pnl'] for r in long_below):+.1f} ({len(long_below)}t)")

# Skew Charm longs (SC has longs too — check separate)
sc_longs = [r for r in results if r['setup']=='Skew Charm']
sc_above = [r for r in sc_longs if r['cliff_side']=='ABOVE']
sc_below = [r for r in sc_longs if r['cliff_side']=='BELOW']
print()
print(f"SC LONGS baseline: {sum(r['pnl'] for r in sc_longs):+.1f}")
print(f"SC LONGS cliff ABOVE: {sum(r['pnl'] for r in sc_above):+.1f} ({len(sc_above)}t)")
print(f"SC LONGS cliff BELOW: {sum(r['pnl'] for r in sc_below):+.1f} ({len(sc_below)}t)")
