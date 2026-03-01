import os, psycopg2, psycopg2.extras, json, sys
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

out = []

cur.execute("SELECT NOW(), NOW() AT TIME ZONE 'America/New_York' as et_now")
r = cur.fetchone()
out.append(f"DB time UTC: {r['now']}")
out.append(f"DB time ET:  {r['et_now']}")
out.append("")

out.append("Last 10 trades:")
cur.execute("""
SELECT id, setup_name, direction, grade,
       ts AT TIME ZONE 'America/New_York' as time_et,
       (ts AT TIME ZONE 'America/New_York')::date as trade_date_et,
       outcome_result, outcome_pnl
FROM setup_log ORDER BY ts DESC LIMIT 10
""")
for r in cur.fetchall():
    d = dict(r)
    for k,v in d.items():
        if hasattr(v,'isoformat'): d[k]=v.isoformat()
    out.append(json.dumps(d, default=str))

out.append("")
out.append("Trades with UTC date = 2026-02-27:")
cur.execute("""
SELECT id, setup_name, ts, ts AT TIME ZONE 'America/New_York' as time_et
FROM setup_log WHERE ts::date = '2026-02-27' ORDER BY ts
""")
rows = cur.fetchall()
if not rows:
    out.append("(none)")
else:
    for r in rows:
        out.append(str(dict(r)))

conn.close()
print("\n".join(out), flush=True)
