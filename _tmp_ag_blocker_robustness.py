"""Robustness: (1) list the 7 placed AG->30 longs by date.
(2) Same dist-above-target gradient on ALL setup_log long signals Apr 1+ (portal outcomes,
    points not $) -- feature-predictiveness test on a much larger n.
"""
import os, json, re
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
import bisect

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("""
    SELECT ts, payload->'statistics'->>'paradigm', payload->'statistics'->>'target'
    FROM volland_snapshots WHERE ts >= '2026-04-01' ORDER BY ts
""")
vt, vp, vtgt = [], [], []
last_t = (None, None); last_p = (None, None)
for ts, para, tgt in cur.fetchall():
    val = None
    if tgt:
        m = re.search(r"[\d,]+", tgt)
        if m:
            val = float(m.group().replace(",", ""))
    if val is not None:
        last_t = (ts, val)
    elif last_t[0] is not None and (ts - last_t[0]).total_seconds() <= 1200:
        val = last_t[1]
    if para is not None:
        last_p = (ts, para)
    elif last_p[0] is not None and (ts - last_p[0]).total_seconds() <= 1200:
        para = last_p[1]
    vt.append(ts); vp.append(para); vtgt.append(val)

def volland_at(ts):
    i = bisect.bisect_left(vt, ts)
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(vt):
            d = abs((vt[j] - ts).total_seconds())
            if d <= 360 and (best is None or d < best[0]):
                best = (d, j)
    return (None, None) if best is None else (vp[best[1]], vtgt[best[1]])

# (1) the 7 placed AG >30 longs
cur.execute("""
    SELECT r.setup_log_id, r.state, l.setup_name, l.ts, l.spot, l.outcome_pnl
    FROM real_trade_orders r JOIN setup_log l ON l.id = r.setup_log_id
    WHERE l.ts >= '2026-04-01' AND lower(l.direction) IN ('long','bullish','buy')
    ORDER BY l.ts
""")
print("=== placed AG-paradigm longs >30 above target ===")
for lid, state, name, ts, spot, pnl in cur.fetchall():
    para, tgt = volland_at(ts)
    if not para or not para.startswith("AG") or tgt is None or spot is None:
        continue
    dist = float(spot) - tgt
    if dist > 30:
        st = state if isinstance(state, dict) else json.loads(state or "{}")
        fill = st.get("fill_price"); cp = st.get("close_fill_price")
        usd = (float(cp) - float(fill)) * 5.0 if (fill and cp) else (float(pnl) * 5.0 if pnl is not None else None)
        print(f"  {ts.astimezone(ET).strftime('%m-%d %H:%M')} lid {lid} {name:14s} dist={dist:+.1f} ${usd:+.2f}")

# (2) ALL long signals, portal outcome points
cur.execute("""
    SELECT setup_name, ts, spot, outcome_pnl
    FROM setup_log
    WHERE ts >= '2026-04-01' AND lower(direction) IN ('long','bullish','buy')
      AND outcome_result IN ('WIN','LOSS','EXPIRED') AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    ORDER BY ts
""")
agg = defaultdict(list)
day_dist = defaultdict(lambda: defaultdict(list))
for name, ts, spot, pnl in cur.fetchall():
    para, tgt = volland_at(ts)
    if para is None or tgt is None:
        continue
    grp = "AG" if para.startswith("AG") else "nonAG"
    dist = float(spot) - tgt
    if dist <= 0:
        b = "tgt>=spot"
    elif dist <= 10:
        b = "0-10"
    elif dist <= 20:
        b = "10-20"
    elif dist <= 30:
        b = "20-30"
    else:
        b = ">30"
    agg[(grp, b)].append(float(pnl))
    if grp == "AG" and b == ">30":
        day_dist[ts.astimezone(ET).date()][name].append(float(pnl))

print("\n=== ALL long signals (portal pts, Apr 1+) ===")
for grp in ("AG", "nonAG"):
    print(f"  {grp}:")
    for b in ("tgt>=spot", "0-10", "10-20", "20-30", ">30"):
        xs = agg.get((grp, b))
        if not xs:
            continue
        w = sum(1 for p in xs if p > 0.2); l = sum(1 for p in xs if p < -0.2)
        print(f"    {b:9s} n={len(xs):4d}  WR={100*w/max(w+l,1):3.0f}%  total {sum(xs):+8.1f} pts  avg {sum(xs)/len(xs):+.2f}")

print("\nAG >30 signals by day (n days = era spread check):")
for d, names in sorted(day_dist.items()):
    tot = sum(sum(v) for v in names.values())
    n = sum(len(v) for v in names.values())
    print(f"  {d}: n={n} {dict((k, round(sum(v),1)) for k, v in names.items())} tot={tot:+.1f}")
c.close()
