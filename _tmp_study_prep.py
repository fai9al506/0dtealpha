import os, psycopg
from datetime import date
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
# chain_snapshots resolution on a sample day
cur.execute("""SELECT count(*), min(ts), max(ts) FROM chain_snapshots WHERE ts::date='2026-06-11'""")
print("chain_snapshots 6/11:", cur.fetchone())
cur.execute("""SELECT ts, spot FROM chain_snapshots WHERE ts::date='2026-06-11' AND ts AT TIME ZONE 'America/New_York' >= '2026-06-11 12:38' ORDER BY ts LIMIT 8""")
print("sample spot series ~12:38 ET:")
for r in cur.fetchall(): print("  ",r[0], r[1])
# population: real trail trades SC/AG/DD with fill
cur.execute("""
  SELECT s.setup_name, count(*) FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id=s.id
  WHERE s.setup_name IN ('Skew Charm','AG Short','DD Exhaustion')
    AND (r.state->>'fill_price') IS NOT NULL
    AND s.ts::date >= '2026-04-15'
  GROUP BY 1""")
print("\npopulation (real trail trades since Apr 15):")
for r in cur.fetchall(): print("  ",r[0],r[1])
