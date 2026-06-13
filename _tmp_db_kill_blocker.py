import os, sqlalchemy as sa

eng = sa.create_engine(os.environ["DATABASE_URL"])
with eng.connect() as c:
    # terminate any idle-in-transaction session holding a lock on chain_snapshots
    rows = c.execute(sa.text("""
        SELECT DISTINCT l.pid
        FROM pg_locks l
        JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE l.relation = 'chain_snapshots'::regclass
          AND a.state = 'idle in transaction'
          AND a.pid <> pg_backend_pid()
    """)).fetchall()
    for (pid,) in rows:
        ok = c.execute(sa.text("SELECT pg_terminate_backend(:p)"), {"p": pid}).scalar()
        print(f"terminated pid {pid}: {ok}")
    if not rows:
        print("no blocker found")
