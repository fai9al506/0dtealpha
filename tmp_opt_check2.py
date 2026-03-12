import os, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

rows = c.execute(text("SELECT setup_log_id, state FROM options_trade_orders WHERE setup_log_id IN (678, 696, 714) ORDER BY setup_log_id")).fetchall()
for r in rows:
    print(f"=== #{r[0]} ===", flush=True)
    s = r[1] if isinstance(r[1], dict) else json.loads(r[1])
    for k, v in s.items():
        print(f"  {k}: {v}", flush=True)
    print(flush=True)

# Also check columns
cols = c.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='options_trade_orders' ORDER BY ordinal_position")).fetchall()
print("COLUMNS:", [r[0] for r in cols], flush=True)

# Check if OPTIONS_TRADE_ENABLED
print("\nChecking recent Railway logs would tell us if options trader is enabled...", flush=True)
print("Entry prices suggest option premiums ($1.55-$10.20) - so _find_strike works", flush=True)

c.close()
