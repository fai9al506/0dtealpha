import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"])
c.autocommit = True
cur = c.cursor()
cur.execute("""
    SELECT pid, state, usename, application_name,
           now() - xact_start AS xact_age, now() - query_start AS query_age,
           left(query, 120)
    FROM pg_stat_activity
    WHERE datname = current_database() AND pid <> pg_backend_pid()
    ORDER BY xact_start NULLS LAST
""")
for r in cur.fetchall():
    print(r)
print("\n-- locks on chain_snapshots --")
cur.execute("""
    SELECT l.pid, l.mode, l.granted, a.state, now()-a.xact_start, left(a.query,100)
    FROM pg_locks l
    JOIN pg_class cl ON cl.oid = l.relation
    JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE cl.relname = 'chain_snapshots'
""")
for r in cur.fetchall():
    print(r)
