import sys; sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import os, psycopg
conn = psycopg.connect(os.environ['DATABASE_URL'])
rows = conn.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'volland_exposure_points'
    ORDER BY ordinal_position
""").fetchall()
for r in rows:
    print(f'{r[0]:<30} {r[1]}')

print("\n--- Sample row ---")
sample = conn.execute("""
    SELECT * FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'ALL'
    ORDER BY ts DESC LIMIT 1
""").fetchone()
if sample:
    for i, col in enumerate([r[0] for r in rows]):
        val = str(sample[i])[:100] if sample[i] is not None else "NULL"
        print(f"  {col}: {val}")
