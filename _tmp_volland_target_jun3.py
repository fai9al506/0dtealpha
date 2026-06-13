"""Did our own volland_snapshots have the AG target near 7550 on Jun 3 morning?"""
import os, json
import psycopg2
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("""
    SELECT ts, payload FROM volland_snapshots
    WHERE ts BETWEEN '2026-06-03 13:30+00' AND '2026-06-03 17:00+00'
    ORDER BY ts
""")
rows = cur.fetchall()
print(f"snapshots: {len(rows)}")
seen = 0
for ts, payload in rows:
    p = payload if isinstance(payload, dict) else json.loads(payload)
    stats = p.get("statistics") or p.get("stats") or {}
    t = ts.astimezone(ET).strftime("%H:%M")
    keys = list(stats.keys()) if isinstance(stats, dict) else []
    if seen == 0:
        print("payload top-level keys:", list(p.keys()))
        print("stats keys:", keys)
    # print every 30 min approx
    if seen % 15 == 0:
        para = stats.get("paradigm") or p.get("paradigm")
        lis = stats.get("lis") or stats.get("LIS")
        tgt = {k: v for k, v in stats.items() if "target" in k.lower()} if isinstance(stats, dict) else {}
        print(f"  {t} ET  paradigm={para}  LIS={lis}  targets={tgt}")
    seen += 1
c.close()
