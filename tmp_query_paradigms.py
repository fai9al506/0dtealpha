import os, json
import sqlalchemy as sa

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as c:
    rows = c.execute(sa.text("""
        SELECT
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            array_agg(DISTINCT payload->'statistics'->>'paradigm') as paradigms
        FROM volland_snapshots
        WHERE payload->'statistics'->>'paradigm' IS NOT NULL
          AND payload->'statistics'->>'paradigm' != ''
        GROUP BY (ts AT TIME ZONE 'America/New_York')::date
        ORDER BY trade_date
    """)).fetchall()

    for r in rows:
        print(f'{r.trade_date}: {r.paradigms}')
