import os
from sqlalchemy import create_engine, text
db = os.getenv('DATABASE_URL', '').replace('postgres://', 'postgresql://')
engine = create_engine(db)
with engine.connect() as conn:
    r = conn.execute(text("""
        UPDATE setup_log
        SET outcome_result = NULL, outcome_pnl = NULL,
            outcome_max_profit = NULL, outcome_max_loss = NULL,
            outcome_elapsed_min = NULL, outcome_first_event = NULL,
            outcome_target_level = NULL
        WHERE id IN (1252, 1256, 1279, 1288)
        RETURNING id
    """))
    cleared = r.fetchall()
    conn.commit()
    print(f"Cleared {len(cleared)} Mar 26 SC outcomes: {[r[0] for r in cleared]}")

    r2 = conn.execute(text("""
        SELECT id, outcome_result FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND (ts AT TIME ZONE 'America/New_York')::date = '2026-03-26'
    """)).fetchall()
    print(f"Mar 26 SC trades now: {[(r[0], r[1]) for r in r2]}")
