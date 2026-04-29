import sqlalchemy

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine = sqlalchemy.create_engine(DB_URL)

queries = {}

queries["Q1_overall"] = """
SELECT COUNT(*) as total,
       COUNT(*) FILTER (WHERE outcome_result='WIN') as wins,
       COUNT(*) FILTER (WHERE outcome_result='LOSS') as losses,
       COUNT(*) FILTER (WHERE outcome_result='EXPIRED') as expired,
       ROUND(SUM(outcome_pnl)::numeric, 1) as pnl,
       ROUND(100.0 * COUNT(*) FILTER (WHERE outcome_result='WIN') / NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')), 0), 1) as wr,
       MIN(ts::date) as first, MAX(ts::date) as last
FROM setup_log
WHERE setup_name = 'ES Absorption' AND direction = 'bearish' AND outcome_result IS NOT NULL
"""

queries["Q2_by_grade"] = """
SELECT grade, COUNT(*) as total,
       COUNT(*) FILTER (WHERE outcome_result='WIN') as wins,
       COUNT(*) FILTER (WHERE outcome_result='LOSS') as losses,
       ROUND(SUM(outcome_pnl)::numeric, 1) as pnl,
       ROUND(100.0 * COUNT(*) FILTER (WHERE outcome_result='WIN') / NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')), 0), 1) as wr
FROM setup_log WHERE setup_name = 'ES Absorption' AND direction = 'bearish' AND outcome_result IS NOT NULL
GROUP BY grade ORDER BY pnl DESC
"""

queries["Q3_by_paradigm"] = """
SELECT paradigm, COUNT(*) as total,
       COUNT(*) FILTER (WHERE outcome_result='WIN') as wins,
       COUNT(*) FILTER (WHERE outcome_result='LOSS') as losses,
       ROUND(SUM(outcome_pnl)::numeric, 1) as pnl,
       ROUND(100.0 * COUNT(*) FILTER (WHERE outcome_result='WIN') / NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')), 0), 1) as wr
FROM setup_log WHERE setup_name = 'ES Absorption' AND direction = 'bearish' AND outcome_result IS NOT NULL
GROUP BY paradigm ORDER BY pnl DESC
"""

queries["Q4_by_alignment"] = """
SELECT greek_alignment, COUNT(*) as total,
       COUNT(*) FILTER (WHERE outcome_result='WIN') as wins,
       COUNT(*) FILTER (WHERE outcome_result='LOSS') as losses,
       ROUND(SUM(outcome_pnl)::numeric, 1) as pnl,
       ROUND(100.0 * COUNT(*) FILTER (WHERE outcome_result='WIN') / NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')), 0), 1) as wr
FROM setup_log WHERE setup_name = 'ES Absorption' AND direction = 'bearish' AND outcome_result IS NOT NULL
GROUP BY greek_alignment ORDER BY greek_alignment
"""

queries["Q5_by_hour"] = """
SELECT EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York')::int as hour_et,
       COUNT(*) as total,
       COUNT(*) FILTER (WHERE outcome_result='WIN') as wins,
       COUNT(*) FILTER (WHERE outcome_result='LOSS') as losses,
       ROUND(SUM(outcome_pnl)::numeric, 1) as pnl,
       ROUND(100.0 * COUNT(*) FILTER (WHERE outcome_result='WIN') / NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')), 0), 1) as wr
FROM setup_log WHERE setup_name = 'ES Absorption' AND direction = 'bearish' AND outcome_result IS NOT NULL
GROUP BY hour_et ORDER BY hour_et
"""

queries["Q6_gate_combos"] = """
SELECT 'A_all' as gate, COUNT(*) as t, COUNT(*) FILTER (WHERE outcome_result='WIN') as w, COUNT(*) FILTER (WHERE outcome_result='LOSS') as l, ROUND(SUM(outcome_pnl)::numeric,1) as pnl, ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1) as wr
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL
UNION ALL
SELECT 'B_no_GEX_LIS', COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'), ROUND(SUM(outcome_pnl)::numeric,1), ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL AND (paradigm IS NULL OR paradigm != 'GEX-LIS')
UNION ALL
SELECT 'C_grade_A+A', COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'), ROUND(SUM(outcome_pnl)::numeric,1), ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL AND grade IN ('A+','A')
UNION ALL
SELECT 'D_grade_A+AB', COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'), ROUND(SUM(outcome_pnl)::numeric,1), ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL AND grade IN ('A+','A','B')
UNION ALL
SELECT 'E_align_neg2', COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'), ROUND(SUM(outcome_pnl)::numeric,1), ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL AND greek_alignment <= -2
UNION ALL
SELECT 'F_gradeAB_align-1', COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'), ROUND(SUM(outcome_pnl)::numeric,1), ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL AND grade IN ('A+','A','B') AND greek_alignment <= -1
UNION ALL
SELECT 'G_noLIS_gradeAB', COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'), ROUND(SUM(outcome_pnl)::numeric,1), ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL AND (paradigm IS NULL OR paradigm != 'GEX-LIS') AND grade IN ('A+','A','B')
UNION ALL
SELECT 'H_align_neg1', COUNT(*), COUNT(*) FILTER (WHERE outcome_result='WIN'), COUNT(*) FILTER (WHERE outcome_result='LOSS'), ROUND(SUM(outcome_pnl)::numeric,1), ROUND(100.0*COUNT(*) FILTER (WHERE outcome_result='WIN')/NULLIF(COUNT(*) FILTER (WHERE outcome_result IN ('WIN','LOSS')),0),1)
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL AND greek_alignment <= -1
ORDER BY gate
"""

queries["Q7_running_pnl"] = """
SELECT ts::date as trade_date, SUM(outcome_pnl) as daily_pnl,
       SUM(SUM(outcome_pnl)) OVER (ORDER BY ts::date) as running_pnl
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL
GROUP BY ts::date ORDER BY ts::date
"""

queries["Q8_gateF_running_pnl"] = """
SELECT ts::date as trade_date, SUM(outcome_pnl) as daily_pnl,
       SUM(SUM(outcome_pnl)) OVER (ORDER BY ts::date) as running_pnl
FROM setup_log WHERE setup_name='ES Absorption' AND direction='bearish' AND outcome_result IS NOT NULL
AND grade IN ('A+','A','B') AND greek_alignment <= -1
GROUP BY ts::date ORDER BY ts::date
"""

with engine.connect() as conn:
    for name, sql in queries.items():
        print(f"===== {name} =====")
        r = conn.execute(sqlalchemy.text(sql))
        cols = list(r.keys())
        print(" | ".join(str(c) for c in cols))
        print("-" * 80)
        for row in r:
            print(" | ".join(str(v) for v in row))
        print()
