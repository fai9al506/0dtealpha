import os, sqlalchemy as sa

eng = sa.create_engine(os.environ["DATABASE_URL"])
with eng.connect() as c:
    r = c.execute(sa.text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='chain_snapshots' AND column_name='vix3m'"
    )).fetchall()
    print("vix3m exists:", bool(r))

    rows = c.execute(sa.text("""
        SELECT pid, usename, state, wait_event_type, wait_event,
               now()-xact_start AS xact_age, now()-query_start AS query_age,
               left(query,150) AS q
        FROM pg_stat_activity
        WHERE datname='railway' AND pid <> pg_backend_pid()
        ORDER BY xact_start NULLS LAST
    """)).fetchall()
    for row in rows:
        print(dict(row._mapping))
