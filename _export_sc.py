import os, csv
from sqlalchemy import create_engine, text

db = os.getenv('DATABASE_URL', '').replace('postgres://', 'postgresql://')
print(f"DB: {db[:40]}...")
engine = create_engine(db)

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               setup_name, direction, grade, score, spot,
               paradigm, greek_alignment, vix, overvix,
               outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss,
               outcome_elapsed_min, outcome_first_event,
               outcome_target_level, outcome_stop_level
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND outcome_result IN ('WIN','LOSS','EXPIRED')
        ORDER BY id
    """)).fetchall()

print(f"Exported {len(rows)} SC trades")
with open('exports/sc_trades_full.csv', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['id','ts_et','setup_name','direction','grade','score','spot',
                'paradigm','greek_alignment','vix','overvix',
                'outcome_result','outcome_pnl','outcome_max_profit','outcome_max_loss',
                'outcome_elapsed_min','outcome_first_event','outcome_target_level','outcome_stop_level'])
    for r in rows:
        w.writerow(list(r))
print("Saved to exports/sc_trades_full.csv")
print(f"First: {rows[0][1]}")
print(f"Last: {rows[-1][1]}")
