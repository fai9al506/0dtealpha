"""Fix ES Absorption trade #147: elapsed time wrong (5 min, should be ~44 min)
The WIN +10 is correct (ES target was hit before stop), but was resolved
prematurely via SPX fallback."""
import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

with engine.begin() as conn:
    before = conn.execute(text("""
        SELECT id, outcome_result, outcome_pnl, outcome_elapsed_min
        FROM setup_log WHERE id = 147
    """)).mappings().first()
    print(f"BEFORE: #{before['id']} result={before['outcome_result']} PNL={before['outcome_pnl']} elapsed={before['outcome_elapsed_min']}min")

    conn.execute(text("""
        UPDATE setup_log SET outcome_elapsed_min = 44 WHERE id = 147
    """))

    after = conn.execute(text("""
        SELECT id, outcome_result, outcome_pnl, outcome_elapsed_min
        FROM setup_log WHERE id = 147
    """)).mappings().first()
    print(f"AFTER:  #{after['id']} result={after['outcome_result']} PNL={after['outcome_pnl']} elapsed={after['outcome_elapsed_min']}min")
