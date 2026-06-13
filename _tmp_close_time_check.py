"""Find real close timestamps: dump full state JSONB keys for Jun 3 lids + setup_log columns."""
import os, json
import psycopg2
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='setup_log' AND column_name ILIKE '%outcome%'")
print("setup_log outcome cols:", [r[0] for r in cur.fetchall()])
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='real_trade_orders'")
print("real_trade_orders cols:", [r[0] for r in cur.fetchall()])

cur.execute("SELECT state FROM real_trade_orders WHERE setup_log_id = 3508")
st = cur.fetchone()[0]
st = st if isinstance(st, dict) else json.loads(st)
print("\nlid 3508 state keys:", sorted(st.keys()))
for k, v in sorted(st.items()):
    s = str(v)
    print(f"  {k}: {s[:200]}")
c.close()
