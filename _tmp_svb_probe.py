import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
with eng.connect() as c:
    cols = c.execute(text("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = 'volland_snapshots' ORDER BY ordinal_position
    """)).fetchall()
    print("volland_snapshots cols:")
    for r in cols:
        print(f"  {r[0]} {r[1]}")

    # latest row — inspect spot-vol-beta payload if present
    row = c.execute(text("""
        SELECT * FROM volland_snapshots ORDER BY ts DESC LIMIT 1
    """)).mappings().fetchone()
    if row:
        print("\nlatest row keys/values (truncated):")
        for k, v in row.items():
            s = str(v)
            if len(s) > 200:
                s = s[:200] + "..."
            print(f"  {k} = {s}")
