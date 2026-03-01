import psycopg2, os, sys
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='es_range_bars' ORDER BY ordinal_position")
for r in cur.fetchall():
    sys.stdout.write(r[0]+'\n')
sys.stdout.flush()
conn.close()
