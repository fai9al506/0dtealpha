import os
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    # First look at schema of state
    row = conn.execute(text("""
        SELECT state FROM real_trade_orders
        WHERE setup_log_id = 1898 LIMIT 1
    """)).fetchone()
    if row:
        import json
        st = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        print("Sample state keys:", sorted(st.keys()))
        print("Sample state:", json.dumps(st, indent=2, default=str)[:1200])
