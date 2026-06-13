import os, sqlalchemy as sa

eng = sa.create_engine(os.environ["DATABASE_URL"])
with eng.connect() as c:
    rows = c.execute(sa.text("""
        SELECT l.pid, l.mode, l.granted, a.state,
               now()-a.xact_start AS xact_age, left(a.query,100) AS q
        FROM pg_locks l
        JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE l.relation = 'chain_snapshots'::regclass
    """)).fetchall()
    print("--- locks on chain_snapshots ---")
    for row in rows:
        print(dict(row._mapping))

    rows = c.execute(sa.text("""
        SELECT pid, pg_blocking_pids(pid) AS blocked_by, state, left(query,100) AS q
        FROM pg_stat_activity
        WHERE cardinality(pg_blocking_pids(pid)) > 0
    """)).fetchall()
    print("--- blocked sessions ---")
    for row in rows:
        print(dict(row._mapping))
