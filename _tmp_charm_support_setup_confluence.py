"""Does proximity to the dominant below-spot charm bar explain existing setup outcomes?

For every setup_log signal with a resolved outcome (Apr 1 - Jun 3, current-era):
  - charm snapshot nearest signal ts (within 5 min), NULL-exp SPX
  - S_sup = below-spot strike with max |charm|, require >= 0.6*gmax and >= 10M
  - dist = spot - S_sup
Buckets: NEAR (0-15 pts above bar), MID (15-40), FAR (>40 or no qualifying bar)
Question: do SHORTS entered NEAR the bar underperform (bounce risk)?
          do LONGS entered NEAR outperform?
DB actual outcomes only (outcome_pnl), no simulation.
"""
import os
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
from statistics import median
import bisect

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

# top-10 bars per snapshot, computed server-side (dominant >=0.6*gmax is always in top-10)
cur.execute("""
    SELECT ts_utc, strike, value FROM (
        SELECT ts_utc, strike, value,
               row_number() OVER (PARTITION BY ts_utc ORDER BY abs(value) DESC) rn
        FROM volland_exposure_points
        WHERE greek='charm' AND ticker='SPX' AND expiration_option IS NULL
          AND ts_utc >= '2026-04-01'
    ) x WHERE rn <= 10
""")
snaps = defaultdict(list)
for ts, strike, val in cur.fetchall():
    snaps[ts].append((float(strike), float(val)))
snap_times = sorted(snaps.keys())
print(f"charm snapshots loaded: {len(snap_times)}")

cur.execute("""
    SELECT id, setup_name, direction, ts, spot, outcome_result, outcome_pnl
    FROM setup_log
    WHERE ts >= '2026-04-01' AND outcome_result IN ('WIN','LOSS','EXPIRED')
      AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    ORDER BY ts
""")
rows = cur.fetchall()
print(f"resolved signals Apr 1 - now: {len(rows)}")

def classify(ts, spot):
    i = bisect.bisect_left(snap_times, ts)
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(snap_times):
            dtv = abs((snap_times[j] - ts).total_seconds())
            if dtv <= 300 and (best is None or dtv < best[0]):
                best = (dtv, snap_times[j])
    if not best:
        return None, None
    pts = snaps[best[1]]
    gmax = max(abs(v) for _, v in pts)
    below = [(s, v) for s, v in pts if s <= spot]
    if not below:
        return "FAR", None
    s_sup, v_sup = max(below, key=lambda x: abs(x[1]))
    if abs(v_sup) < max(0.6 * gmax, 10e6):
        return "FAR", None
    return None, spot - s_sup

agg = defaultdict(lambda: defaultdict(list))
for sid, name, direction, ts, spot, res, pnl in rows:
    tag, dist = classify(ts, float(spot))
    if tag is None and dist is None:
        continue  # no charm snapshot near signal
    if dist is None:
        bucket = "FAR/none"
    elif dist <= 15:
        bucket = "NEAR(0-15)"
    elif dist <= 40:
        bucket = "MID(15-40)"
    else:
        bucket = "FAR/none"
    d = (direction or "").upper()
    side = "LONG" if d in ("LONG", "BULLISH", "BUY") else "SHORT"
    agg[side][bucket].append((res, float(pnl), name))

for side in ("SHORT", "LONG"):
    print(f"\n=== {side}S vs dominant below-spot charm bar ===")
    for bucket in ("NEAR(0-15)", "MID(15-40)", "FAR/none"):
        xs = agg[side].get(bucket, [])
        if not xs:
            continue
        w = sum(1 for r, _, _ in xs if r == "WIN")
        l = sum(1 for r, _, _ in xs if r == "LOSS")
        tot = sum(p for _, p, _ in xs)
        print(f"  {bucket:11s} n={len(xs):4d}  WR={100*w/max(w+l,1):3.0f}%  total {tot:+8.1f} pts  avg {tot/len(xs):+.2f}")
c.close()
