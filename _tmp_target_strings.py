import os
import psycopg2
from collections import Counter
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("""
    SELECT payload->'statistics'->>'target' AS t, count(*)
    FROM volland_snapshots WHERE ts >= '2026-04-01'
    GROUP BY 1 ORDER BY 2 DESC LIMIT 15
""")
for t, n in cur.fetchall():
    print(repr(t), n)
c.close()
