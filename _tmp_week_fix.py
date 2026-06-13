import os
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
DAYS = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"]

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date AS d,
               (ARRAY_AGG(spot ORDER BY ts))[1] AS o, MAX(spot) AS h, MIN(spot) AS l,
               (ARRAY_AGG(spot ORDER BY ts DESC))[1] AS cl
        FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date = ANY(:days) AND spot IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """), {"days": DAYS}).fetchall()
    print("=== SPX per day ===")
    for r in rows:
        o, h, l, cl = (float(x) for x in r[1:])
        print(f"  {r[0]}  O {o:.0f}  H {h:.0f}  L {l:.0f}  C {cl:.0f}   net {cl-o:+.0f}  range {h-l:.0f}")

    print("\n=== paradigm mode per day (from setup_log signals) ===")
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date AS d, paradigm, COUNT(*)
        FROM setup_log
        WHERE (ts AT TIME ZONE 'America/New_York')::date = ANY(:days) AND paradigm IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """), {"days": DAYS}).fetchall()
    cur = None
    for r in rows:
        if str(r[0]) != cur:
            cur = str(r[0])
            print(f"\n  {cur}: ", end="")
        print(f"{r[1]}({r[2]}) ", end="")
    print()
