import sqlalchemy as sa, os

e = sa.create_engine(os.environ['DATABASE_URL'])
with e.connect() as c:
    r = c.execute(sa.text("SELECT payload FROM volland_snapshots ORDER BY ts DESC LIMIT 1")).scalar()
    stats = r.get("statistics", {})
    print("Statistics keys:", list(stats.keys()))
    for k, v in stats.items():
        print(f"  {k}: {v}")
