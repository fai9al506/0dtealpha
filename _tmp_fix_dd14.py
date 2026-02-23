"""Fix DD Exhaustion trade #139: should be LOSS -12 (initial stop was hit but missed by live tracker)"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

# Show current state
with engine.begin() as conn:
    before = conn.execute(text("""
        SELECT id, outcome_result, outcome_pnl, outcome_stop_level
        FROM setup_log WHERE id = 139
    """)).mappings().first()
    print(f"BEFORE: ID=139 result={before['outcome_result']} pnl={before['outcome_pnl']} stop={before['outcome_stop_level']}")

    # Fix: initial stop was 6860.9 + 12 = 6872.9
    # PNL = entry - stop = 6860.9 - 6872.9 = -12.0
    conn.execute(text("""
        UPDATE setup_log
        SET outcome_result = 'LOSS',
            outcome_pnl = -12.0,
            outcome_stop_level = 6872.9
        WHERE id = 139
    """))

    after = conn.execute(text("""
        SELECT id, outcome_result, outcome_pnl, outcome_stop_level
        FROM setup_log WHERE id = 139
    """)).mappings().first()
    print(f"AFTER:  ID=139 result={after['outcome_result']} pnl={after['outcome_pnl']} stop={after['outcome_stop_level']}")

    # Show updated DD Exhaustion totals
    dd_totals = conn.execute(text("""
        SELECT outcome_result, COUNT(*) as cnt, SUM(outcome_pnl) as total_pnl
        FROM setup_log
        WHERE setup_name = 'DD Exhaustion' AND outcome_result IS NOT NULL
        GROUP BY outcome_result
        ORDER BY outcome_result
    """)).mappings().all()
    print("\nDD Exhaustion updated totals:")
    grand = 0
    for t in dd_totals:
        print(f"  {t['outcome_result']}: {t['cnt']} trades, PNL={t['total_pnl']}")
        grand += float(t['total_pnl'] or 0)
    print(f"  NET: {grand:+.1f}")

    # Show all setup totals
    all_totals = conn.execute(text("""
        SELECT setup_name, outcome_result, COUNT(*) as cnt, SUM(outcome_pnl) as total_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY setup_name, outcome_result
        ORDER BY setup_name, outcome_result
    """)).mappings().all()
    print("\nAll setup totals:")
    current_setup = None
    setup_net = 0
    grand_total = 0
    for t in all_totals:
        if t['setup_name'] != current_setup:
            if current_setup:
                print(f"    NET: {setup_net:+.1f}")
                grand_total += setup_net
            current_setup = t['setup_name']
            setup_net = 0
            print(f"\n  {current_setup}:")
        pnl = float(t['total_pnl'] or 0)
        setup_net += pnl
        print(f"    {t['outcome_result']}: {t['cnt']} trades, PNL={pnl:+.1f}")
    if current_setup:
        print(f"    NET: {setup_net:+.1f}")
        grand_total += setup_net
    print(f"\n  GRAND TOTAL: {grand_total:+.1f}")
