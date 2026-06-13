"""ES Abs post-C6 (2026-05-06) audit + TSRT real comparison."""
import os
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DATABASE_URL"])

with eng.connect() as c:
    # 1) Find real-trade table
    print("=== tables containing 'real' or 'trade' ===")
    r = c.execute(text("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public'
          AND (table_name ILIKE '%real%' OR table_name ILIKE '%trade%')
        ORDER BY table_name
    """))
    print([row[0] for row in r])

    # 2) Post-C6 portal stats
    print("\n=== ES Abs post-C6 portal (chain-sim) May 6 - May 21 ===")
    r = c.execute(text("""
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) wins,
            SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) losses,
            SUM(CASE WHEN outcome_result='EXPIRED' THEN 1 ELSE 0 END) expired,
            SUM(CASE WHEN outcome_result='WIN' AND outcome_pnl=0 THEN 1 ELSE 0 END) be_snaps,
            ROUND(SUM(outcome_pnl)::numeric,1) total_pnl,
            ROUND(AVG(outcome_pnl)::numeric,2) avg_pnl,
            ROUND(MIN(outcome_pnl)::numeric,1) worst,
            ROUND(MAX(outcome_pnl)::numeric,1) best
        FROM setup_log
        WHERE setup_name='ES Absorption'
          AND ts >= '2026-05-06' AND ts < '2026-05-22'
          AND outcome_result IS NOT NULL
    """))
    for row in r:
        print(dict(row._mapping))

    # 3) Pre-C6 same window length (Apr 21 - May 5)
    print("\n=== ES Abs pre-C6 portal Apr 21 - May 5 (fixed T=10) ===")
    r = c.execute(text("""
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) wins,
            SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) losses,
            SUM(CASE WHEN outcome_result='EXPIRED' THEN 1 ELSE 0 END) expired,
            SUM(CASE WHEN outcome_result='WIN' AND outcome_pnl=0 THEN 1 ELSE 0 END) be_snaps,
            ROUND(SUM(outcome_pnl)::numeric,1) total_pnl,
            ROUND(AVG(outcome_pnl)::numeric,2) avg_pnl,
            ROUND(MIN(outcome_pnl)::numeric,1) worst,
            ROUND(MAX(outcome_pnl)::numeric,1) best
        FROM setup_log
        WHERE setup_name='ES Absorption'
          AND ts >= '2026-04-21' AND ts < '2026-05-06'
          AND outcome_result IS NOT NULL
    """))
    for row in r:
        print(dict(row._mapping))

    # 4) Post-C6 MES-sim (closer to real broker)
    print("\n=== ES Abs post-C6 MES-sim (S55) May 6 - May 21 ===")
    r = c.execute(text("""
        SELECT
            COUNT(*) with_mes,
            SUM(CASE WHEN mes_sim_outcome_result='WIN' THEN 1 ELSE 0 END) wins,
            SUM(CASE WHEN mes_sim_outcome_result='LOSS' THEN 1 ELSE 0 END) losses,
            SUM(CASE WHEN mes_sim_outcome_result='WIN' AND mes_sim_outcome_pnl=0 THEN 1 ELSE 0 END) be_snaps,
            ROUND(SUM(mes_sim_outcome_pnl)::numeric,1) total_pnl,
            ROUND(AVG(mes_sim_outcome_pnl)::numeric,2) avg_pnl
        FROM setup_log
        WHERE setup_name='ES Absorption'
          AND ts >= '2026-05-06' AND ts < '2026-05-22'
          AND mes_sim_outcome_pnl IS NOT NULL
    """))
    for row in r:
        print(dict(row._mapping))

    # 5) ES Abs skip reasons (why not all signals reached TSRT)
    print("\n=== ES Abs real_trade_skip_reason breakdown (May 6-21) ===")
    r = c.execute(text("""
        SELECT real_trade_skip_reason,
            COUNT(*) n
        FROM setup_log
        WHERE setup_name='ES Absorption'
          AND ts >= '2026-05-06' AND ts < '2026-05-22'
          AND outcome_result IS NOT NULL
        GROUP BY real_trade_skip_reason
        ORDER BY n DESC
    """))
    for row in r:
        print(dict(row._mapping))
