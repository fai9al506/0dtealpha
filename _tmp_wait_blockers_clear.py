"""Poll until the charm_stream analysis sessions release their locks, then exit 0."""
import os, time, psycopg2

while True:
    try:
        c = psycopg2.connect(os.environ["DATABASE_URL"])
        c.autocommit = True
        cur = c.cursor()
        cur.execute("""
            SELECT count(*) FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND state = 'idle in transaction'
        """)
        n = cur.fetchone()[0]
        c.close()
        if n == 0:
            print("blockers cleared")
            break
    except Exception as e:
        print(f"check error (retrying): {e}", flush=True)
    time.sleep(60)
