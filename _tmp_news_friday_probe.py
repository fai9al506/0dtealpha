import os
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
with eng.connect() as c:
    cols = c.execute(text("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = 'economic_events' ORDER BY ordinal_position
    """)).fetchall()
    print("schema:")
    for col in cols:
        print(f"  {col[0]} {col[1]}")

    n = c.execute(text("SELECT COUNT(*), MIN(event_date), MAX(event_date) FROM economic_events")).fetchone() \
        if any(col[0] == 'event_date' for col in cols) else None
    if n:
        print(f"\ntotal rows: {n[0]}, range {n[1]} -> {n[2]}")

    # try generic select for the week of Jun 1-5
    rows = c.execute(text("SELECT * FROM economic_events LIMIT 3")).fetchall()
    print("\nsample rows:")
    for r in rows:
        print(" ", r)
