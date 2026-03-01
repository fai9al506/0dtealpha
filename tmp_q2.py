import psycopg2, os, sys
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("SELECT id, ts AT TIME ZONE 'America/New_York', setup_name, direction, grade, outcome_result, outcome_pnl, outcome_max_profit FROM setup_log ORDER BY ts DESC LIMIT 10")
sys.stdout.write('Latest 10 trades:\n')
for r in cur.fetchall():
    sys.stdout.write(f'  {r[0]:>4} {str(r[1]):>26} {r[2]:>20} {r[3]:>6} {r[4]:>4} | {str(r[5]):>10} pnl={str(r[6]):>6} maxP={str(r[7]):>6}\n')
cur.execute("SELECT max(ts) AT TIME ZONE 'America/New_York' FROM setup_log")
sys.stdout.write(f'\nLatest trade timestamp (ET): {cur.fetchone()[0]}\n')
cur.execute("SELECT now() AT TIME ZONE 'America/New_York'")
sys.stdout.write(f'Current time (ET): {cur.fetchone()[0]}\n')
sys.stdout.flush()
conn.close()
