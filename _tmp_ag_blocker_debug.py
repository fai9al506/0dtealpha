"""Why did 83 longs land in no-data? Count causes + check coverage by month."""
import os, json, re
import psycopg2
from zoneinfo import ZoneInfo
from collections import Counter, defaultdict
import bisect

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("""
    SELECT ts, payload->'statistics'->>'paradigm', payload->'statistics'->>'target'
    FROM volland_snapshots WHERE ts >= '2026-04-01' ORDER BY ts
""")
vt, vp, vtgt = [], [], []
for ts, para, tgt in cur.fetchall():
    val = None
    if tgt:
        m = re.search(r"[\d,]+", tgt)
        if m:
            val = float(m.group().replace(",", ""))
    vt.append(ts); vp.append(para); vtgt.append(val)
print(f"volland snapshots: {len(vt)}")
mon = defaultdict(lambda: [0, 0, 0])
for i, t in enumerate(vt):
    m = t.strftime("%Y-%m")
    mon[m][0] += 1
    if vp[i]: mon[m][1] += 1
    if vtgt[i] is not None: mon[m][2] += 1
for m, (n, p, g) in sorted(mon.items()):
    print(f"  {m}: snaps={n}  with_paradigm={p}  with_target={g}")

cur.execute("""
    SELECT r.setup_log_id, l.ts, l.spot, l.direction
    FROM real_trade_orders r JOIN setup_log l ON l.id = r.setup_log_id
    WHERE l.ts >= '2026-04-01' AND lower(l.direction) IN ('long','bullish','buy')
    ORDER BY l.ts
""")
causes = Counter()
for lid, ts, spot, d in cur.fetchall():
    i = bisect.bisect_left(vt, ts)
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(vt):
            dd = abs((vt[j] - ts).total_seconds())
            if best is None or dd < best[0]:
                best = (dd, j)
    if best is None:
        causes["no snapshot at all"] += 1; continue
    dsec, j = best
    if dsec > 360:
        causes[f"gap>{6}min"] += 1
        continue
    if spot is None:
        causes["spot null"] += 1
    elif vtgt[j] is None:
        causes["target unparsed"] += 1
    elif vp[j] is None:
        causes["paradigm null"] += 1
    else:
        causes["OK"] += 1
print("\nlong-signal match causes:", dict(causes))
c.close()
