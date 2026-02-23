"""Clean auto_trade_orders table — mark all as closed."""
import os
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
with engine.begin() as conn:
    rows = conn.execute(text("SELECT setup_log_id, state->>'status' as status, state->>'setup_name' as name FROM auto_trade_orders")).mappings().all()
    for r in rows:
        print(f"  id={r['setup_log_id']} status={r['status']} name={r['name']}", flush=True)
    result = conn.execute(text("UPDATE auto_trade_orders SET state = jsonb_set(state, '{status}', '\"closed\"') WHERE state->>'status' != 'closed'"))
    print(f"Updated {result.rowcount} rows to closed", flush=True)
print("Done", flush=True)
