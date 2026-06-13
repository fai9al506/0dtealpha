import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
with eng.connect() as c:
    # Friday's correlation through the day
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') AS et,
               payload->'statistics'->'spot_vol_beta' AS svb
        FROM volland_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
          AND payload->'statistics' ? 'spot_vol_beta'
        ORDER BY ts
    """)).fetchall()
    print(f"Jun 5: {len(rows)} snapshots with spot_vol_beta")
    step = max(1, len(rows)//15)
    for r in rows[::step]:
        svb = r[1] if isinstance(r[1], dict) else json.loads(r[1] or "{}")
        print(f"  {r[0].strftime('%H:%M')}  corr={svb.get('correlation')}  vixEvents={svb.get('vixEvents')}")
    if rows:
        last = rows[-1]
        svb = last[1] if isinstance(last[1], dict) else json.loads(last[1] or "{}")
        print(f"  LAST {last[0].strftime('%H:%M')}  corr={svb.get('correlation')}  vixEvents={svb.get('vixEvents')}")

    # has vixEvents EVER been non-empty?
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') AS et,
               payload->'statistics'->'spot_vol_beta'->'vixEvents' AS ev
        FROM volland_snapshots
        WHERE jsonb_array_length(COALESCE(payload->'statistics'->'spot_vol_beta'->'vixEvents', '[]'::jsonb)) > 0
        ORDER BY ts DESC LIMIT 8
    """)).fetchall()
    print(f"\nnon-empty vixEvents rows: {len(rows)}")
    for r in rows:
        print(f"  {r[0]}  {str(r[1])[:400]}")

    # daily closing correlation, last 30 sessions (last snapshot of each day)
    rows = c.execute(text("""
        SELECT DISTINCT ON ((ts AT TIME ZONE 'America/New_York')::date)
               (ts AT TIME ZONE 'America/New_York')::date AS d,
               payload->'statistics'->'spot_vol_beta'->>'correlation' AS corr
        FROM volland_snapshots
        WHERE payload->'statistics' ? 'spot_vol_beta'
          AND ts > NOW() - INTERVAL '45 days'
        ORDER BY (ts AT TIME ZONE 'America/New_York')::date, ts DESC
    """)).fetchall()
    print("\nEOD correlation last sessions:")
    for r in rows:
        print(f"  {r[0]}  {r[1]}")
