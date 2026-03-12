import os, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Full state for the 3 trades with close_price=0
for sid in [624, 626, 654]:
    row = c.execute(text(f"SELECT state FROM options_trade_orders WHERE setup_log_id = {sid}")).fetchone()
    if row:
        s = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        print(f"=== #{sid} ===", flush=True)
        for k, v in sorted(s.items()):
            print(f"  {k}: {v}", flush=True)
        print(flush=True)

c.close()
