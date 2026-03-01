"""ES Absorption trade analysis queries against Railway PostgreSQL."""
import psycopg2
import os
import sys

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set", flush=True)
    sys.exit(1)

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

def run_query(title, sql):
    print(flush=True)
    print('=' * 140, flush=True)
    print(title, flush=True)
    print('=' * 140, flush=True)
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()

    print(f"({len(rows)} rows)", flush=True)
    print(flush=True)

    # Calculate column widths
    col_widths = [len(c) for c in cols]
    for r in rows:
        for i, v in enumerate(r):
            col_widths[i] = max(col_widths[i], len(str(v)))

    # Print formatted table
    header = ' | '.join(c.ljust(col_widths[i]) for i, c in enumerate(cols))
    print(header, flush=True)
    print('-' * len(header), flush=True)
    for r in rows:
        print(' | '.join(str(v).ljust(col_widths[i]) for i, v in enumerate(r)), flush=True)

    return rows

# Query 1: All ES Absorption trades with outcome details
run_query(
    "QUERY 1: All ES Absorption trades with outcome details",
    """
    SELECT id,
           ts AT TIME ZONE 'America/New_York' as time_et,
           direction, grade, score,
           outcome_result, outcome_pnl,
           outcome_first_event,
           outcome_max_profit, outcome_max_loss,
           outcome_elapsed_min,
           abs_vol_ratio, abs_es_price,
           spot
    FROM setup_log
    WHERE setup_name = 'ES Absorption'
    AND outcome_result IS NOT NULL
    ORDER BY ts
    """
)

# Query 2: Trades that reached +10 pts max profit
run_query(
    "QUERY 2: Trades that reached +10 pts max profit - what was final PnL?",
    """
    SELECT id,
           ts AT TIME ZONE 'America/New_York' as time_et,
           direction, outcome_result, outcome_pnl,
           outcome_max_profit, outcome_max_loss,
           outcome_first_event,
           ROUND((outcome_max_profit - outcome_pnl)::numeric, 1) as profit_left_on_table
    FROM setup_log
    WHERE setup_name = 'ES Absorption'
    AND outcome_result IS NOT NULL
    AND outcome_max_profit >= 10
    ORDER BY outcome_max_profit DESC
    """
)

# Query 3: All trades - max profit vs captured
run_query(
    "QUERY 3: All trades - max profit vs captured profit",
    """
    SELECT id,
           ts AT TIME ZONE 'America/New_York' as time_et,
           direction, grade,
           outcome_result, outcome_pnl,
           outcome_max_profit, outcome_max_loss,
           CASE WHEN outcome_max_profit >= 10 THEN 'reached_10' ELSE 'under_10' END as reached_target,
           ROUND((outcome_max_profit - COALESCE(outcome_pnl, 0))::numeric, 1) as unrealized_beyond_exit
    FROM setup_log
    WHERE setup_name = 'ES Absorption'
    AND outcome_result IS NOT NULL
    ORDER BY ts
    """
)

# Query 4: Summary stats
run_query(
    "QUERY 4: Summary stats - trailing potential",
    """
    SELECT
        COUNT(*) as total_trades,
        SUM(CASE WHEN outcome_max_profit >= 10 THEN 1 ELSE 0 END) as reached_10,
        SUM(CASE WHEN outcome_max_profit >= 15 THEN 1 ELSE 0 END) as reached_15,
        SUM(CASE WHEN outcome_max_profit >= 20 THEN 1 ELSE 0 END) as reached_20,
        SUM(CASE WHEN outcome_max_profit >= 30 THEN 1 ELSE 0 END) as reached_30,
        SUM(CASE WHEN outcome_max_profit >= 50 THEN 1 ELSE 0 END) as reached_50,
        ROUND(AVG(outcome_max_profit)::numeric, 1) as avg_max_profit,
        ROUND(AVG(outcome_pnl)::numeric, 1) as avg_actual_pnl,
        ROUND(SUM(outcome_pnl)::numeric, 1) as total_actual_pnl,
        ROUND(SUM(outcome_max_profit)::numeric, 1) as total_theoretical_max
    FROM setup_log
    WHERE setup_name = 'ES Absorption' AND outcome_result IS NOT NULL
    """
)

# Query 5: Winners with large max profits (>=20)
run_query(
    "QUERY 5: Winners with large max profits (>=20) - timeline",
    """
    SELECT id,
           ts AT TIME ZONE 'America/New_York' as entry_time,
           (ts + (outcome_elapsed_min || ' minutes')::interval) AT TIME ZONE 'America/New_York' as approx_exit_time,
           direction,
           outcome_result,
           outcome_pnl,
           outcome_max_profit,
           outcome_max_loss,
           spot as entry_spot,
           outcome_elapsed_min
    FROM setup_log
    WHERE setup_name = 'ES Absorption'
    AND outcome_max_profit >= 20
    ORDER BY outcome_max_profit DESC
    """
)

# Query 6: Exit behavior analysis
run_query(
    "QUERY 6: Exit behavior analysis - what happened after reaching +10",
    """
    SELECT id,
           ts AT TIME ZONE 'America/New_York' as time_et,
           direction,
           outcome_result, outcome_pnl,
           outcome_max_profit,
           outcome_max_loss,
           CASE
               WHEN outcome_max_profit >= 10 AND outcome_pnl = 10 THEN 'exited_at_target'
               WHEN outcome_max_profit >= 10 AND outcome_pnl > 10 THEN 'trailed_past_target'
               WHEN outcome_max_profit >= 10 AND outcome_pnl < 10 THEN 'gave_back_after_target'
               ELSE 'never_reached_target'
           END as exit_behavior,
           ROUND((outcome_max_profit - 10)::numeric, 1) as additional_profit_available,
           outcome_elapsed_min
    FROM setup_log
    WHERE setup_name = 'ES Absorption'
    AND outcome_result IS NOT NULL
    ORDER BY outcome_max_profit DESC
    """
)

conn.close()
print(flush=True)
print("ALL QUERIES COMPLETE", flush=True)
