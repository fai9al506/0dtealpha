import psycopg2, os, sys, json
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("""
SELECT ts AT TIME ZONE 'America/New_York', payload
FROM volland_snapshots
WHERE payload::text LIKE '%paradigm%'
ORDER BY ts DESC LIMIT 1
""")
row = cur.fetchone()
if row:
    sys.stdout.write(f'ts: {row[0]}\n')
    p = row[1]
    # Find statistics-related keys
    if isinstance(p, dict):
        for k in sorted(p.keys()):
            v = p[k]
            if isinstance(v, dict):
                sys.stdout.write(f'\n  {k}:\n')
                for k2, v2 in v.items():
                    sys.stdout.write(f'    {k2}: {str(v2)[:120]}\n')
            else:
                sys.stdout.write(f'  {k}: {str(v)[:120]}\n')
sys.stdout.flush()
conn.close()
