import os, sqlalchemy as sa

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as c:
    # List all tables
    rows = c.execute(sa.text("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)).fetchall()
    print('All tables:')
    for r in rows:
        print(f'  {r.table_name}')

    # Get volland_snapshots columns
    rows2 = c.execute(sa.text("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = 'volland_snapshots'
        ORDER BY ordinal_position
    """)).fetchall()
    print('\nvolland_snapshots columns:')
    for r in rows2:
        print(f'  {r.column_name} ({r.data_type})')

    # Get setup_log columns
    rows3 = c.execute(sa.text("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = 'setup_log'
        ORDER BY ordinal_position
    """)).fetchall()
    print('\nsetup_log columns:')
    for r in rows3:
        print(f'  {r.column_name} ({r.data_type})')

    # Check if economic_events exists and get its columns
    rows4 = c.execute(sa.text("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = 'economic_events'
        ORDER BY ordinal_position
    """)).fetchall()
    if rows4:
        print('\neconomic_events columns:')
        for r in rows4:
            print(f'  {r.column_name} ({r.data_type})')
    else:
        print('\nNo economic_events table found')
