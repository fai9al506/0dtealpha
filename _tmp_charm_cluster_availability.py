import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("""
    SELECT ts_utc::date d, count(DISTINCT ts_utc) snaps, count(*) pts
    FROM volland_exposure_points
    WHERE greek='charm' AND ticker='SPX' AND expiration_option IS NULL
    GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall()
print(f"days with NULL-exp charm: {len(rows)}, first={rows[0][0]}, last={rows[-1][0]}")
# also the TODAY labeled era (older schema?)
cur.execute("""
    SELECT min(ts_utc)::date, max(ts_utc)::date, count(DISTINCT ts_utc::date)
    FROM volland_exposure_points
    WHERE greek='charm' AND ticker='SPX' AND expiration_option='TODAY'
""")
print("TODAY-exp charm era:", cur.fetchall())
c.close()
