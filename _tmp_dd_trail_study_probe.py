import os
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"], isolation_level="AUTOCOMMIT")
with eng.connect() as c:
    cols = c.execute(text("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name='setup_log' ORDER BY ordinal_position
    """)).fetchall()
    print([f"{a}" for a,b in cols])
    # peek one DD row's abs_details for align field
    r = c.execute(text("""
        SELECT abs_details FROM setup_log
        WHERE setup_name='DD Exhaustion' AND ts>='2026-05-01' AND abs_details IS NOT NULL
        ORDER BY ts DESC LIMIT 1
    """)).fetchone()
    print("\nabs_details sample:", str(r[0])[:800] if r else None)
