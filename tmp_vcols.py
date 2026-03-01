import psycopg2, os, sys
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='volland_snapshots' ORDER BY ordinal_position")
for r in cur.fetchall():
    sys.stdout.write(r[0]+'\n')
# Also get a sample row
cur.execute("SELECT * FROM volland_snapshots ORDER BY ts DESC LIMIT 1")
cols = [d[0] for d in cur.description]
row = cur.fetchone()
if row:
    for c, v in zip(cols, row):
        sys.stdout.write(f'  {c}: {str(v)[:100]}\n')
sys.stdout.flush()
conn.close()
