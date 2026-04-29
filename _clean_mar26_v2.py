import os
from sqlalchemy import create_engine, text
db = os.getenv('DATABASE_URL', '').replace('postgres://', 'postgresql://')
engine = create_engine(db)
with engine.connect() as conn:
    r = conn.execute(text("SELECT id, outcome_result FROM setup_log WHERE id IN (1252, 1256, 1279, 1288)")).fetchall()
    print("Before:", [(x[0], x[1]) for x in r])

    conn.execute(text("""
        UPDATE setup_log
        SET outcome_result = NULL, outcome_pnl = NULL,
            outcome_max_profit = NULL, outcome_max_loss = NULL,
            outcome_elapsed_min = NULL, outcome_first_event = NULL,
            outcome_target_level = NULL
        WHERE id IN (1252, 1256, 1279, 1288)
    """))
    conn.commit()

    r2 = conn.execute(text("SELECT id, outcome_result FROM setup_log WHERE id IN (1252, 1256, 1279, 1288)")).fetchall()
    print("After:", [(x[0], x[1]) for x in r2])

    r3 = conn.execute(text("SELECT COUNT(*) FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IN ('WIN','LOSS','EXPIRED')")).fetchone()
    print(f"SC with outcomes: {r3[0]}")
