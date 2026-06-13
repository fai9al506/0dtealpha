"""Broker truth for Jun 5 from tsrt_daily_stmt (S204: /historicalorders+/orders FIFO)."""
import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    row = c.execute(text("SELECT * FROM tsrt_daily_stmt WHERE day = '2026-06-05'")).mappings().fetchone()
    if not row:
        print("no row for 2026-06-05")
    else:
        for k, v in row.items():
            if k == "trades":
                tr = v if isinstance(v, list) else json.loads(v or "[]")
                print(f"trades ({len(tr)}):")
                for t in tr:
                    print("  ", json.dumps(t)[:220])
            else:
                print(f"{k} = {v}")
