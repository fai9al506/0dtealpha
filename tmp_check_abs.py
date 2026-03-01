import sys; sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import os, psycopg
conn = psycopg.connect(os.environ['DATABASE_URL'])
rows = conn.execute("""
    SELECT id, comments, abs_details::text
    FROM setup_log
    WHERE setup_name = 'ES Absorption'
      AND ts::date >= '2026-02-24' AND ts::date <= '2026-02-28'
    ORDER BY id
    LIMIT 5
""").fetchall()
print(f"{len(rows)} rows")
for r in rows:
    print(f"#{r[0]}")
    print(f"  comments: {(r[1] or '')[:200]}")
    print(f"  abs_details: {(r[2] or 'NULL')[:400]}")
    print()
