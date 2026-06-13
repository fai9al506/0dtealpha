import os, json
from sqlalchemy import create_engine, text
engine=create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    cols=[c[0] for c in conn.execute(text("""SELECT column_name FROM information_schema.columns
        WHERE table_name='volland_exposure_points' ORDER BY ordinal_position""")).fetchall()]
    print("volland_exposure_points cols:", ", ".join(cols))
    # distinct exposure types (the charm, vanna x4, gamma x4, deltaDecay)
    try:
        types=conn.execute(text("SELECT DISTINCT exposure_type FROM volland_exposure_points ORDER BY 1")).fetchall()
        print("\nexposure_type values:", [t[0] for t in types])
    except Exception as e:
        print("no exposure_type col:", e)
    # sample a recent snapshot's points
    print("\n--- sample rows (latest ts) ---")
    rows=conn.execute(text("""SELECT * FROM volland_exposure_points ORDER BY ts DESC LIMIT 6""")).mappings().fetchall()
    for r in rows:
        print({k:(str(v)[:40]) for k,v in dict(r).items()})
    # coverage
    cov=conn.execute(text("""SELECT MIN((ts AT TIME ZONE 'America/New_York')::date), MAX((ts AT TIME ZONE 'America/New_York')::date), COUNT(*) FROM volland_exposure_points""")).fetchone()
    print("\ncoverage:", cov)
