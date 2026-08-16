import os, psycopg2
c=psycopg2.connect(os.environ["DATABASE_URL"]); cur=c.cursor()
cur.execute("""SELECT column_name,data_type FROM information_schema.columns WHERE table_name='real_trade_orders' ORDER BY ordinal_position""")
print("=== real_trade_orders cols ===")
for r in cur.fetchall(): print(r)
cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='setup_log' AND column_name IN ('real_trade_skip_reason','live_pass','outcome_pnl','outcome_result','direction','grade','setup_name','ts','spot')""")
print("=== setup_log relevant cols ===")
for r in cur.fetchall(): print(r)
cur.close(); c.close()
