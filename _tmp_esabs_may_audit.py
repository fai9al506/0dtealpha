import os
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DATABASE_URL"])

with eng.connect() as c:
    print("=== ES Abs May 2026 (all live signals) ===")
    r = c.execute(text("""
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) wins,
            SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) losses,
            SUM(CASE WHEN outcome_result='EXPIRED' THEN 1 ELSE 0 END) expired,
            ROUND(SUM(outcome_pnl)::numeric,1) total_pnl,
            ROUND(AVG(outcome_pnl)::numeric,2) avg_pnl
        FROM setup_log
        WHERE setup_name='ES Absorption'
          AND created_at >= '2026-05-01' AND created_at < '2026-06-01'
          AND outcome_result IS NOT NULL
    """))
    for row in r:
        print(dict(row._mapping))

    print("\n=== ES Abs May 2026 by direction ===")
    r = c.execute(text("""
        SELECT direction,
            COUNT(*) n,
            SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) wins,
            ROUND(SUM(outcome_pnl)::numeric,1) pnl
        FROM setup_log
        WHERE setup_name='ES Absorption'
          AND created_at >= '2026-05-01' AND created_at < '2026-06-01'
          AND outcome_result IS NOT NULL
        GROUP BY direction
    """))
    for row in r:
        print(dict(row._mapping))

    print("\n=== ES Abs May 2026 by grade ===")
    r = c.execute(text("""
        SELECT grade,
            COUNT(*) n,
            ROUND(SUM(outcome_pnl)::numeric,1) pnl,
            ROUND(AVG(outcome_pnl)::numeric,1) avg
        FROM setup_log
        WHERE setup_name='ES Absorption'
          AND created_at >= '2026-05-01' AND created_at < '2026-06-01'
          AND outcome_result IS NOT NULL
        GROUP BY grade
        ORDER BY grade
    """))
    for row in r:
        print(dict(row._mapping))

    print("\n=== ES Abs by month (Feb-May 2026) ===")
    r = c.execute(text("""
        SELECT TO_CHAR(created_at, 'YYYY-MM') ym,
            COUNT(*) n,
            SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) wins,
            ROUND(SUM(outcome_pnl)::numeric,1) pnl,
            ROUND(AVG(outcome_pnl)::numeric,2) avg
        FROM setup_log
        WHERE setup_name='ES Absorption'
          AND created_at >= '2026-02-01' AND created_at < '2026-06-01'
          AND outcome_result IS NOT NULL
        GROUP BY ym
        ORDER BY ym
    """))
    for row in r:
        print(dict(row._mapping))

    print("\n=== ES Abs May 2026 trade detail (chronological) ===")
    r = c.execute(text("""
        SELECT id,
            created_at AT TIME ZONE 'America/New_York' AS et,
            direction,
            grade,
            paradigm,
            ROUND(outcome_pnl::numeric,1) pnl,
            outcome_result result
        FROM setup_log
        WHERE setup_name='ES Absorption'
          AND created_at >= '2026-05-01' AND created_at < '2026-06-01'
          AND outcome_result IS NOT NULL
        ORDER BY created_at
    """))
    for row in r:
        print(dict(row._mapping))
