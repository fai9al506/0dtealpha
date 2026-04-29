"""Exhaustive cliff-definition sweep for max edge."""
import psycopg2
from collections import defaultdict

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

cur.execute("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
       greek_alignment, vix, overvix,
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
v12 = []
for t in all_t:
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, h, m = t
    if setup == 'Skew Charm' and grade not in ('A+', 'A', 'B'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and paradigm and paradigm.startswith('GEX-LIS'): continue
    if setup in ('Skew Charm', 'DD Exhaustion') and ((h == 14 and m >= 30) or h == 15): continue
    if setup == 'DD Exhaustion' and align == 0: continue
    v12.append(t)
print(f"Baseline V12-fix shorts: {len(v12)}")
baseline_pnl = sum(float(t[7] or 0) for t in v12)
print(f"Baseline PnL: {baseline_pnl:+.1f}")

def fetch_near(ts, spot, exp, band):
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

def cliff_basic(near):
    """First definition: any sign flip closest to spot."""
    if not near: return None
    crossings = []
    n = sorted(near)
    for i in range(1, len(n)):
        s0, v0 = n[i-1]; s1, v1 = n[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1 - v0 != 0: crossings.append(s0 + (-v0/(v1-v0))*(s1-s0))
    return crossings

def cliff_magnitude(near, min_mag=100):
    """Only sign flips where both sides have |val| >= min_mag."""
    if not near: return None
    crossings = []
    n = sorted(near)
    for i in range(1, len(n)):
        s0, v0 = n[i-1]; s1, v1 = n[i]
        if abs(v0) < min_mag or abs(v1) < min_mag: continue
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1 - v0 != 0: crossings.append(s0 + (-v0/(v1-v0))*(s1-s0))
    return crossings

def cliff_peak(near):
    """Strike with max |vanna|."""
    if not near: return None
    return [max(near, key=lambda x: abs(x[1]))[0]]

def cliff_net_signed(near):
    """Strike where cumulative (bottom-up) sum crosses 0."""
    if not near: return None
    s0 = sorted(near)
    cum = 0
    prev_s = None; prev_cum = 0
    crossings = []
    for s, v in s0:
        nc = prev_cum + v
        if prev_s is not None and ((prev_cum > 0 and nc < 0) or (prev_cum < 0 and nc > 0)):
            if nc - prev_cum != 0:
                crossings.append(prev_s + (-prev_cum/(nc-prev_cum))*(s - prev_s))
        prev_s = s; prev_cum = nc
    return crossings

def run_filter(v12, cliff_fn, exp, band, descr):
    """Apply: block setup when cliff side is 'bad'. Try both sides to see which is bad."""
    results = []
    for t in v12:
        tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, h, m = t
        near = fetch_near(ts, spot, exp, band)
        if near is None:
            results.append({'setup': setup, 'outcome': outcome, 'pnl': float(pnl or 0), 'side': 'NODATA', 'spot': float(spot)})
            continue
        cliffs = cliff_fn(near)
        if not cliffs:
            results.append({'setup': setup, 'outcome': outcome, 'pnl': float(pnl or 0), 'side': 'NONE', 'spot': float(spot)})
            continue
        # nearest to spot
        nearest = min(cliffs, key=lambda s: abs(s - float(spot)))
        side = 'ABOVE' if nearest > float(spot) else 'BELOW'
        results.append({'setup': setup, 'outcome': outcome, 'pnl': float(pnl or 0), 'side': side,
                        'spot': float(spot), 'cliff': nearest, 'dist': nearest - float(spot)})

    print(f"\n--- {descr} ---")
    # Aggregate by side x setup
    for grp in ['ABOVE', 'BELOW', 'NONE', 'NODATA']:
        sub = [r for r in results if r['side'] == grp]
        w = sum(1 for r in sub if r['outcome'] == 'WIN')
        l = sum(1 for r in sub if r['outcome'] == 'LOSS')
        pnl = sum(r['pnl'] for r in sub)
        wr = 100*w/max(1,w+l)
        print(f"  {grp:<8} n={len(sub):>3} W={w:>3} L={l:>3} WR={wr:>5.1f}% pnl={pnl:+7.1f}")

    # Per setup x side
    print("  per-setup:")
    for setup_name in ['Skew Charm', 'DD Exhaustion', 'AG Short']:
        for grp in ['ABOVE', 'BELOW']:
            sub = [r for r in results if r['setup'] == setup_name and r['side'] == grp]
            if not sub: continue
            w = sum(1 for r in sub if r['outcome']=='WIN')
            l = sum(1 for r in sub if r['outcome']=='LOSS')
            pnl = sum(r['pnl'] for r in sub)
            wr = 100*w/max(1,w+l)
            print(f"    {setup_name:<16} {grp:<6} n={len(sub):>3} W={w:>3} L={l:>3} WR={wr:>5.1f}% pnl={pnl:+7.1f}")
    return results

# Sweep 1: different expirations, basic cliff
for exp in ['TODAY', 'THIS_WEEK', 'THIRTY_NEXT_DAYS', 'ALL']:
    run_filter(v12, cliff_basic, exp, 50, f"expiration={exp}, basic cliff, band=50")

# Sweep 2: different bands on weekly
for band in [20, 30, 100]:
    run_filter(v12, cliff_basic, 'THIS_WEEK', band, f"expiration=THIS_WEEK, basic cliff, band={band}")

# Sweep 3: magnitude-filtered on weekly (noise-filter)
for min_mag in [50, 200, 500, 1000]:
    run_filter(v12, lambda n, m=min_mag: cliff_magnitude(n, m), 'THIS_WEEK', 50, f"THIS_WEEK, mag>={min_mag}, band=50")

# Sweep 4: net signed cumulative on weekly
run_filter(v12, cliff_net_signed, 'THIS_WEEK', 50, "THIS_WEEK cumulative crossing, band=50")
run_filter(v12, cliff_net_signed, 'THIS_WEEK', 200, "THIS_WEEK cumulative crossing, band=200")

# Sweep 5: peak |vanna| strike on weekly
run_filter(v12, cliff_peak, 'THIS_WEEK', 50, "THIS_WEEK peak |vanna|, band=50")
run_filter(v12, cliff_peak, 'THIS_WEEK', 100, "THIS_WEEK peak |vanna|, band=100")
