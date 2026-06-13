import os, psycopg, json
from collections import Counter
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
cur.execute("SELECT state->>'close_reason' cr, count(*) FROM real_trade_orders GROUP BY 1 ORDER BY 2 DESC")
for cr,n in cur.fetchall(): print(f"  {n:>4}  {cr}")
