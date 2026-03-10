import psycopg2, os, json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("""
SELECT id, comments, abs_details
FROM setup_log
WHERE setup_name = 'ES Absorption' AND abs_details IS NOT NULL
  AND comments LIKE '%%Zone%%'
ORDER BY ts DESC LIMIT 2
""")
for r in cur.fetchall():
    print("ID=%s" % r[0])
    print("comments:", r[1][:200] if r[1] else "None")
    det = r[2] if isinstance(r[2], dict) else json.loads(r[2]) if r[2] else {}
    print("abs_details keys:", list(det.keys()))
    best = det.get("best_swing", {})
    print("best_swing:", json.dumps(best, indent=2, default=str))
    print("---")
conn.close()
