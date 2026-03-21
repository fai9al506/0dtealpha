import os, sqlalchemy as sa

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as c:
    rows = c.execute(sa.text(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'setup_log' ORDER BY ordinal_position"
    )).fetchall()
    for r in rows:
        print(f'{r.column_name}: {r.data_type}')
