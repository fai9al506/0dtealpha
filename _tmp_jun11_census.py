"""Jun 11 afternoon: did ANY long setup fire during the 14:00-16:00 rally, and
was it blocked by the filter? (User: 'bullish day, we got nothing of it')."""
import os, sys, psycopg2
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("""SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
                      real_trade_skip_reason, paradigm
   FROM setup_log
   WHERE (ts AT TIME ZONE 'America/New_York')::date='2026-06-11'
     AND (ts AT TIME ZONE 'America/New_York')::time >= '12:00'
   ORDER BY ts""")
print(f"{'et':6} {'setup':16} {'dir':8} {'gr':3} {'skip_reason':28} {'paradigm'}")
for et,setup,d,g,skip,par in cur.fetchall():
    print(f"{str(et)[11:16]:6} {setup:16} {str(d):8} {str(g):3} {str(skip):28} {str(par)}")
# count of long signals all day vs placed
cur.execute("""SELECT direction, count(*),
   sum(CASE WHEN real_trade_skip_reason IS NULL THEN 1 ELSE 0 END) placed_ish
   FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date='2026-06-11'
   GROUP BY direction""")
print("\nAll-day signal census Jun 11 (direction, total signals, no-skip):")
for d,n,p in cur.fetchall():
    print(f"  {str(d):8} total={n:>3} no_skip_reason={p}")
conn.close()
