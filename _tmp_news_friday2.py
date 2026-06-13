import os
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
with eng.connect() as c:
    r = c.execute(text("SELECT COUNT(*), MIN(ts), MAX(ts), MAX(fetched_at) FROM economic_events")).fetchone()
    print(f"rows={r[0]} ts range {r[1]} -> {r[2]} last_fetch={r[3]}")

    print("\n=== USD events Jun 1-5 2026 (ET times) ===")
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') AS et, title, impact, forecast, previous, actual
        FROM economic_events
        WHERE country = 'USD'
          AND (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-06-01' AND '2026-06-05'
        ORDER BY ts
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]} | {r[2]:<7} | {r[1]:<45} | fcst={r[3]} prev={r[4]} actual={r[5]}")

    print("\n=== High-impact ANY country Jun 5 ===")
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') AS et, country, title, impact, forecast, previous, actual
        FROM economic_events
        WHERE impact ILIKE 'high%'
          AND (ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
        ORDER BY ts
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]} | {r[1]} | {r[3]} | {r[2]} | fcst={r[4]} prev={r[5]} actual={r[6]}")
