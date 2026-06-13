import os, json
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    cols=[c[0] for c in conn.execute(text("""
        SELECT column_name FROM information_schema.columns WHERE table_name='volland_snapshots' ORDER BY ordinal_position""")).fetchall()]
    print("volland_snapshots cols:", ", ".join(cols))
    # sample latest payload keys
    row=conn.execute(text("SELECT payload FROM volland_snapshots ORDER BY ts DESC LIMIT 1")).fetchone()
    if row:
        p=row[0]
        if not isinstance(p,dict):
            try: p=json.loads(p)
            except: p={}
        print("\npayload top-level keys:", list(p.keys()) if isinstance(p,dict) else type(p))
        stats=p.get('statistics') if isinstance(p,dict) else None
        if isinstance(stats,dict):
            print("\nstatistics keys:", list(stats.keys()))
            for k,v in stats.items():
                sv=str(v)[:80]
                print(f"   {k}: {sv}")
    # date coverage
    cov=conn.execute(text("""SELECT MIN((ts AT TIME ZONE 'America/New_York')::date), MAX((ts AT TIME ZONE 'America/New_York')::date), COUNT(*) FROM volland_snapshots""")).fetchone()
    print("\nvolland coverage:", cov)
