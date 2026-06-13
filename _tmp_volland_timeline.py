"""Compare Volland snapshot pattern between yesterday and today."""
import os
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DATABASE_URL"])
with eng.connect() as c:
    # Yesterday's last 5 saves
    print("=== Yesterday's last 5 healthy saves (2026-05-20 EOD) ===")
    r = c.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' AS et,
               (payload->'statistics'->>'paradigm') AS paradigm,
               (payload->>'exposure_points_saved')::int AS pts
        FROM volland_snapshots
        WHERE ts >= '2026-05-20 19:30:00+00' AND ts < '2026-05-21 00:00:00+00'
        ORDER BY ts DESC LIMIT 5
    """))
    for row in r: print(dict(row._mapping))

    print("\n=== Today's first 10 saves (2026-05-21 from session start) ===")
    r = c.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' AS et,
               (payload->'statistics'->>'paradigm') AS paradigm,
               (payload->>'exposure_points_saved')::int AS pts
        FROM volland_snapshots
        WHERE ts >= '2026-05-21 12:00:00+00'
        ORDER BY ts ASC LIMIT 12
    """))
    for row in r: print(dict(row._mapping))

    print("\n=== Gap detection — biggest gap between consecutive saves today ===")
    r = c.execute(text("""
        WITH t AS (
            SELECT ts, LAG(ts) OVER (ORDER BY ts) AS prev_ts
            FROM volland_snapshots
            WHERE ts >= '2026-05-21 12:00:00+00'
        )
        SELECT (ts AT TIME ZONE 'America/New_York') AS et,
               EXTRACT(EPOCH FROM (ts - prev_ts))::int AS gap_seconds
        FROM t
        WHERE prev_ts IS NOT NULL
        ORDER BY (ts - prev_ts) DESC
        LIMIT 5
    """))
    for row in r: print(dict(row._mapping))

    print("\n=== Last healthy save BEFORE the broken-state window ===")
    r = c.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' AS et,
               (payload->'statistics'->>'paradigm') AS paradigm
        FROM volland_snapshots
        WHERE ts < '2026-05-21 12:00:00+00'
          AND (payload->>'exposure_points_saved')::int > 0
        ORDER BY ts DESC LIMIT 1
    """))
    for row in r: print(dict(row._mapping))
