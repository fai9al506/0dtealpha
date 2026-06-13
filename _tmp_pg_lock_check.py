import sqlalchemy as sa

e = sa.create_engine("postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway")
with e.connect() as c:
    rows = c.execute(sa.text("""
        SELECT pid, state, application_name, client_addr,
               now() - xact_start  AS xact_age,
               now() - state_change AS in_state_for,
               left(query, 110) AS query
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND state IS NOT NULL
          AND state <> 'idle'
        ORDER BY xact_start NULLS LAST
    """)).mappings().all()
    if not rows:
        print("no active / idle-in-transaction sessions")
    for r in rows:
        print(dict(r))

    # who held locks on chain_snapshots recently? show all sessions touching it now
    locks = c.execute(sa.text("""
        SELECT l.pid, a.state, a.client_addr, a.application_name,
               now() - a.xact_start AS xact_age, l.mode, left(a.query, 90) AS query
        FROM pg_locks l
        JOIN pg_class cl ON cl.oid = l.relation
        JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE cl.relname = 'chain_snapshots' AND l.pid <> pg_backend_pid()
    """)).mappings().all()
    print("--- locks on chain_snapshots ---")
    if not locks:
        print("none")
    for r in locks:
        print(dict(r))
