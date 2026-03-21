import os, sqlalchemy as sa

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as c:
    # Get FOMC/major economic events
    rows = c.execute(sa.text("""
        SELECT ts::date as event_date, ts::time as event_time, title, impact, country
        FROM economic_events
        WHERE title ILIKE '%fomc%' OR title ILIKE '%fed%' OR title ILIKE '%CPI%'
           OR title ILIKE '%NFP%' OR title ILIKE '%employment%' OR title ILIKE '%payroll%'
           OR title ILIKE '%PPI%' OR title ILIKE '%GDP%' OR title ILIKE '%retail sales%'
           OR title ILIKE '%PCE%' OR title ILIKE '%interest rate%'
           OR impact = 'high' OR impact = 'High'
        ORDER BY ts
    """)).fetchall()
    for r in rows:
        print(f'{r.event_date} {r.event_time}: {r.title} [{r.impact}] ({r.country})')
    print(f'\nTotal: {len(rows)} events')

    # Also show distinct dates with high-impact events
    rows2 = c.execute(sa.text("""
        SELECT DISTINCT ts::date as event_date, COUNT(*) as cnt
        FROM economic_events
        WHERE impact ILIKE '%high%' OR title ILIKE '%fomc%' OR title ILIKE '%fed%'
        GROUP BY ts::date
        ORDER BY ts::date
    """)).fetchall()
    print(f'\nDistinct high-impact dates: {len(rows2)}')
    for r in rows2:
        print(f'  {r.event_date}: {r.cnt} events')
