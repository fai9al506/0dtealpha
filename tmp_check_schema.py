import os
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])
with engine.connect() as c:
    r = c.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'chain_snapshots' ORDER BY ordinal_position"))
    print("chain_snapshots columns:")
    for row in r:
        print(f"  {row[0]}")

    r = c.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'volland_snapshots' ORDER BY ordinal_position"))
    print("\nvolland_snapshots columns:")
    for row in r:
        print(f"  {row[0]}")

    # quick sample
    r = c.execute(text("SELECT * FROM chain_snapshots LIMIT 1"))
    cols = r.keys()
    print(f"\nchain_snapshots keys: {list(cols)}")
