"""User's full Jun-3 signature as a long blocker:
  AG-* paradigm  AND  spot >25 pts above Volland target  AND  near-spot charm empty
  (near_max within +/-15 pts < 0.4 * global max |charm|)
vs the same condition WITHOUT the charm-empty leg, on ALL long signals Apr 1+ (portal pts).
Also: was May 15 charm-near-spot populated? (the discriminator check)
"""
import os, re
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
import bisect

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

# volland paradigm/target with carry-forward
cur.execute("""
    SELECT ts, payload->'statistics'->>'paradigm', payload->'statistics'->>'target'
    FROM volland_snapshots WHERE ts >= '2026-01-19' ORDER BY ts
""")
vt, vp, vtgt = [], [], []
lt = (None, None); lp = (None, None)
for ts, para, tgt in cur.fetchall():
    val = None
    if tgt:
        m = re.search(r"[\d,]+", tgt)
        if m: val = float(m.group().replace(",", ""))
    if val is not None: lt = (ts, val)
    elif lt[0] is not None and (ts - lt[0]).total_seconds() <= 1200: val = lt[1]
    if para is not None: lp = (ts, para)
    elif lp[0] is not None and (ts - lp[0]).total_seconds() <= 1200: para = lp[1]
    vt.append(ts); vp.append(para); vtgt.append(val)

# charm top-10 per snapshot (server-side)
cur.execute("""
    SELECT ts_utc, strike, value FROM (
        SELECT ts_utc, strike, value,
               row_number() OVER (PARTITION BY ts_utc ORDER BY abs(value) DESC) rn
        FROM volland_exposure_points
        WHERE greek='charm' AND ticker='SPX' AND expiration_option IS NULL
          AND ts_utc >= '2026-01-19'
    ) x WHERE rn <= 10
""")
csnaps = defaultdict(list)
for ts, s, v in cur.fetchall():
    csnaps[ts].append((float(s), float(v)))
ctimes = sorted(csnaps.keys())

def near_at(idx_ts, spot):
    i = bisect.bisect_left(ctimes, idx_ts)
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(ctimes):
            d = abs((ctimes[j] - idx_ts).total_seconds())
            if d <= 360 and (best is None or d < best[0]):
                best = (d, j)
    if best is None:
        return None
    pts = csnaps[ctimes[best[1]]]
    gmax = max(abs(v) for _, v in pts)
    near = max((abs(v) for s, v in pts if abs(s - spot) <= 15), default=0.0)
    return near < 0.4 * gmax  # True = near-spot EMPTY

def volland_at(ts):
    i = bisect.bisect_left(vt, ts)
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(vt):
            d = abs((vt[j] - ts).total_seconds())
            if d <= 360 and (best is None or d < best[0]):
                best = (d, j)
    return (None, None) if best is None else (vp[best[1]], vtgt[best[1]])

cur.execute("""
    SELECT setup_name, ts, spot, outcome_pnl
    FROM setup_log
    WHERE ts >= '2026-01-19' AND lower(direction) IN ('long','bullish','buy')
      AND outcome_result IN ('WIN','LOSS','EXPIRED') AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    ORDER BY ts
""")
groups = defaultdict(list)
days = defaultdict(lambda: defaultdict(float))
for name, ts, spot, pnl in cur.fetchall():
    para, tgt = volland_at(ts)
    if para is None or tgt is None or not para.startswith("AG"):
        continue
    spot = float(spot); pnl = float(pnl)
    dist = spot - tgt
    if dist <= 25:
        groups["AG dist<=25 (control)"].append(pnl)
        continue
    empty = near_at(ts, spot)
    if empty is None:
        groups["AG dist>25, charm n/a"].append(pnl)
    elif empty:
        groups["FULL SIG: AG + >25 + charm-empty"].append(pnl)
        days[ts.astimezone(ET).date()]["sig"] += pnl
    else:
        groups["AG + >25 but charm NEAR-SPOT present"].append(pnl)
        days[ts.astimezone(ET).date()]["nosig"] += pnl

for g, xs in sorted(groups.items()):
    w = sum(1 for p in xs if p > 0.2); l = sum(1 for p in xs if p < -0.2)
    print(f"{g:38s} n={len(xs):4d}  WR={100*w/max(w+l,1):3.0f}%  total {sum(xs):+8.1f} pts  avg {sum(xs)/len(xs):+.2f}")
print("\nper-day (sig = full signature, nosig = charm present):")
for d, v in sorted(days.items()):
    print(f"  {d}: {dict(v)}")
c.close()
