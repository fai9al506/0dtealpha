from sqlalchemy import create_engine, text
db_url = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(db_url)

V12_SC = """
CASE
    WHEN grade IN ('C', 'LOG') THEN false
    WHEN (ts AT TIME ZONE 'America/New_York')::time >= '14:30'
         AND (ts AT TIME ZONE 'America/New_York')::time < '15:00' THEN false
    WHEN (ts AT TIME ZONE 'America/New_York')::time >= '15:30' THEN false
    WHEN direction = 'long' AND COALESCE(greek_alignment, 0) < 2 THEN false
    WHEN direction = 'short' AND paradigm = 'GEX-LIS' THEN false
    ELSE true
END
"""

with engine.connect() as conn:
    # 1. SC daily V12 - ALL TIME
    r = conn.execute(text(f"""
        WITH filtered AS (
            SELECT *, (ts AT TIME ZONE 'America/New_York')::date as trade_date,
                {V12_SC} as passes_v12
            FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IS NOT NULL
        )
        SELECT trade_date, COUNT(*), SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END),
            SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END),
            ROUND(SUM(CASE WHEN outcome_result='WIN' THEN 1.0 ELSE 0 END) /
                NULLIF(SUM(CASE WHEN outcome_result IN ('WIN','LOSS') THEN 1 ELSE 0 END), 0) * 100, 1),
            ROUND(SUM(COALESCE(outcome_pnl, 0))::numeric, 1)
        FROM filtered WHERE passes_v12 = true
        GROUP BY trade_date ORDER BY trade_date
    """))
    print("=== SC DAILY V12 ===")
    cum = 0
    for row in r:
        cum += float(row[5])
        print(f"{row[0]}|{row[1]}|{row[2]}W|{row[3]}L|WR={row[4]}|PnL={row[5]}|cum={round(cum,1)}")

    print("---")

    # 2. SC weekly ALL TIME with context
    r2 = conn.execute(text(f"""
        WITH filtered AS (
            SELECT *, (ts AT TIME ZONE 'America/New_York')::date as trade_date,
                {V12_SC} as passes_v12
            FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IS NOT NULL
        )
        SELECT date_trunc('week', trade_date)::date as wk, COUNT(*),
            SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END),
            SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END),
            ROUND(SUM(CASE WHEN outcome_result='WIN' THEN 1.0 ELSE 0 END) /
                NULLIF(SUM(CASE WHEN outcome_result IN ('WIN','LOSS') THEN 1 ELSE 0 END), 0) * 100, 1),
            ROUND(SUM(COALESCE(outcome_pnl, 0))::numeric, 1),
            ROUND(AVG(vix)::numeric, 1),
            ROUND(AVG(COALESCE(outcome_max_profit, 0))::numeric, 1),
            SUM(CASE WHEN outcome_pnl > 20 THEN 1 ELSE 0 END) as big_wins,
            SUM(CASE WHEN direction='long' THEN 1 ELSE 0 END) as longs,
            SUM(CASE WHEN direction='short' THEN 1 ELSE 0 END) as shorts
        FROM filtered WHERE passes_v12 = true
        GROUP BY wk ORDER BY wk
    """))
    print("=== SC WEEKLY V12 ALL TIME ===")
    print("WEEK|TOTAL|W|L|WR|PNL|VIX|AVG_MFE|BIG_WINS|LONGS|SHORTS")
    for row in r2:
        print(f"{row[0]}|{row[1]}|{row[2]}|{row[3]}|{row[4]}|{row[5]}|{row[6]}|{row[7]}|{row[8]}|{row[9]}|{row[10]}")

    print("---")

    # 3. Big winners (>20 pts) - are they concentrated in week 1?
    r3 = conn.execute(text(f"""
        WITH filtered AS (
            SELECT *, (ts AT TIME ZONE 'America/New_York')::date as trade_date,
                {V12_SC} as passes_v12
            FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IS NOT NULL
        )
        SELECT trade_date, direction, grade, outcome_pnl, outcome_max_profit,
               ROUND(outcome_elapsed_min::numeric, 0) as elapsed, vix, paradigm
        FROM filtered WHERE passes_v12 = true AND outcome_pnl > 20
        ORDER BY trade_date
    """))
    print("=== SC BIG WINNERS >20pts (V12) ===")
    for row in r3:
        print(f"{row[0]}|{row[1]}|{row[2]}|PnL={row[3]}|MFE={row[4]}|{row[5]}min|VIX={row[6]}|{row[7]}")

    print("---")

    # 4. Week 1 without big winners - still profitable?
    r4 = conn.execute(text(f"""
        WITH filtered AS (
            SELECT *, (ts AT TIME ZONE 'America/New_York')::date as trade_date,
                {V12_SC} as passes_v12
            FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IS NOT NULL
                AND ts >= '2026-03-02' AND ts < '2026-03-07'
        )
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as losses,
            ROUND(SUM(COALESCE(outcome_pnl, 0))::numeric, 1) as total_pnl,
            ROUND(SUM(CASE WHEN outcome_pnl <= 20 THEN COALESCE(outcome_pnl, 0) ELSE 0 END)::numeric, 1) as pnl_excl_big,
            SUM(CASE WHEN outcome_pnl > 20 THEN 1 ELSE 0 END) as big_win_count,
            ROUND(SUM(CASE WHEN outcome_pnl > 20 THEN outcome_pnl ELSE 0 END)::numeric, 1) as big_win_pnl
        FROM filtered WHERE passes_v12 = true
    """))
    row = r4.fetchone()
    print(f"=== WEEK 1 DECOMPOSITION ===")
    print(f"Total: {row[0]}t, {row[1]}W/{row[2]}L, PnL={row[3]}")
    print(f"Big wins (>20pts): {row[5]} trades = {row[6]} pts")
    print(f"Without big wins: {row[4]} pts")

    print("---")

    # 5. If we remove ALL big winners from ALL weeks, is SC still profitable?
    r5 = conn.execute(text(f"""
        WITH filtered AS (
            SELECT *, (ts AT TIME ZONE 'America/New_York')::date as trade_date,
                date_trunc('week', (ts AT TIME ZONE 'America/New_York')::date)::date as wk,
                {V12_SC} as passes_v12
            FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IS NOT NULL
                AND ts >= '2026-03-01' AND ts < '2026-04-07'
        )
        SELECT wk,
            COUNT(*) as total,
            ROUND(SUM(COALESCE(outcome_pnl, 0))::numeric, 1) as full_pnl,
            ROUND(SUM(CASE WHEN outcome_pnl <= 15 THEN COALESCE(outcome_pnl, 0) ELSE 0 END)::numeric, 1) as pnl_no_big,
            SUM(CASE WHEN outcome_pnl > 15 THEN 1 ELSE 0 END) as big_cnt,
            ROUND(SUM(CASE WHEN outcome_pnl > 15 THEN outcome_pnl ELSE 0 END)::numeric, 1) as big_pnl
        FROM filtered WHERE passes_v12 = true
        GROUP BY wk ORDER BY wk
    """))
    print("=== SC: REMOVE BIG WINS (>15pts) FROM EACH WEEK ===")
    print("WEEK|TOTAL|FULL_PNL|NO_BIG_PNL|BIG_COUNT|BIG_PNL")
    total_full = 0
    total_nobig = 0
    for row in r5:
        total_full += float(row[2])
        total_nobig += float(row[3])
        print(f"{row[0]}|{row[1]}|{row[2]}|{row[3]}|{row[4]}|{row[5]}")
    print(f"TOTAL||{round(total_full,1)}|{round(total_nobig,1)}||")

    print("---")

    # 6. Consistency check: how many GREEN days vs RED days per week
    r6 = conn.execute(text(f"""
        WITH filtered AS (
            SELECT *, (ts AT TIME ZONE 'America/New_York')::date as trade_date,
                date_trunc('week', (ts AT TIME ZONE 'America/New_York')::date)::date as wk,
                {V12_SC} as passes_v12
            FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IS NOT NULL
                AND ts >= '2026-02-01' AND ts < '2026-04-07'
        ),
        daily AS (
            SELECT trade_date, wk,
                ROUND(SUM(COALESCE(outcome_pnl, 0))::numeric, 1) as day_pnl
            FROM filtered WHERE passes_v12 = true
            GROUP BY trade_date, wk
        )
        SELECT wk,
            COUNT(*) as trading_days,
            SUM(CASE WHEN day_pnl > 0 THEN 1 ELSE 0 END) as green_days,
            SUM(CASE WHEN day_pnl <= 0 THEN 1 ELSE 0 END) as red_days,
            ROUND(AVG(day_pnl)::numeric, 1) as avg_day_pnl,
            ROUND(MIN(day_pnl)::numeric, 1) as worst_day,
            ROUND(MAX(day_pnl)::numeric, 1) as best_day
        FROM daily
        GROUP BY wk ORDER BY wk
    """))
    print("=== SC GREEN/RED DAYS PER WEEK ===")
    print("WEEK|DAYS|GREEN|RED|AVG_DAY|WORST|BEST")
    for row in r6:
        print(f"{row[0]}|{row[1]}|{row[2]}|{row[3]}|{row[4]}|{row[5]}|{row[6]}")
