import sys; sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import os, psycopg
conn = psycopg.connect(os.environ['DATABASE_URL'])
rows = conn.execute("""
    SELECT id, ts::date as d,
           abs_details IS NOT NULL as has_det,
           comments IS NOT NULL AND comments != '' as has_com,
           comments
    FROM setup_log
    WHERE setup_name = 'ES Absorption'
    ORDER BY id DESC
    LIMIT 20
""").fetchall()
for r in rows:
    print(f"#{r[0]} {r[1]} det={r[2]} com={r[3]} | {(r[4] or '')[:150]}")
