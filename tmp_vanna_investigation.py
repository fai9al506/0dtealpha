"""Deeper vanna investigation - magnitude analysis and better cross-ref."""
import os
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]

def run_query(label, sql):
    print(f"\n{'='*80}")
    print(f"QUERY {label}")
    print(f"{'='*80}")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        if cur.description:
            cols = [d.name for d in cur.description]
            print(f"Columns: {cols}")
            print(f"Rows: {len(rows)}")
            print("-" * 80)
            for row in rows:
                print(row)
        else:
            print("No results")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        conn.close()

# Query 8 fixed: GEX Long trades with higher-tenor vanna detail
run_query("8 - GEX Long trades with higher-tenor vanna detail", """
WITH gex_trades AS (
    SELECT id, ts, outcome_result, outcome_pnl, outcome_max_profit,
           paradigm, spot, grade
    FROM setup_log
    WHERE setup_name = 'GEX Long'
    AND outcome_result IS NOT NULL
),
trade_vanna AS (
    SELECT g.id,
           g.ts AT TIME ZONE 'America/New_York' as time_et,
           g.outcome_result,
           g.outcome_pnl,
           g.outcome_max_profit,
           g.paradigm,
           g.grade,
           ROUND(g.spot::numeric, 1) as spot,
           (SELECT ROUND(SUM(vep.value)::numeric, 0)
            FROM volland_exposure_points vep
            WHERE vep.greek = 'vanna' AND vep.expiration_option = 'ALL'
            AND vep.ts_utc BETWEEN g.ts - interval '5 minutes' AND g.ts + interval '5 minutes'
           ) as vanna_all,
           (SELECT ROUND(SUM(vep.value)::numeric, 0)
            FROM volland_exposure_points vep
            WHERE vep.greek = 'vanna' AND vep.expiration_option = 'TODAY'
            AND vep.ts_utc BETWEEN g.ts - interval '5 minutes' AND g.ts + interval '5 minutes'
           ) as vanna_today
    FROM gex_trades g
)
SELECT id, time_et, outcome_result, outcome_pnl, outcome_max_profit,
       paradigm, grade, spot,
       vanna_all, vanna_today,
       COALESCE(vanna_all, 0) - COALESCE(vanna_today, 0) as higher_tenor_vanna,
       CASE
           WHEN COALESCE(vanna_all, 0) - COALESCE(vanna_today, 0) < 0 THEN 'NEG'
           ELSE 'POS'
       END as ht_sign
FROM trade_vanna
ORDER BY time_et;
""")

# Query 14: Vanna ALL magnitude buckets for GEX Long
run_query("14 - GEX Long WR by vanna_ALL magnitude bucket", """
WITH gex_trades AS (
    SELECT id, ts, outcome_result, outcome_pnl
    FROM setup_log
    WHERE setup_name = 'GEX Long'
    AND outcome_result IS NOT NULL
),
trade_vanna AS (
    SELECT g.id,
           g.outcome_result,
           g.outcome_pnl,
           (SELECT SUM(vep.value)
            FROM volland_exposure_points vep
            WHERE vep.greek = 'vanna' AND vep.expiration_option = 'ALL'
            AND vep.ts_utc BETWEEN g.ts - interval '5 minutes' AND g.ts + interval '5 minutes'
           ) as vanna_all
    FROM gex_trades g
)
SELECT
    CASE
        WHEN vanna_all IS NULL THEN 'NO_DATA'
        WHEN vanna_all < -5e9 THEN 'VERY_NEG (<-5B)'
        WHEN vanna_all < -1e9 THEN 'MOD_NEG (-5B to -1B)'
        WHEN vanna_all < 0 THEN 'SLIGHT_NEG (-1B to 0)'
        WHEN vanna_all < 1e9 THEN 'SLIGHT_POS (0 to +1B)'
        WHEN vanna_all < 5e9 THEN 'MOD_POS (+1B to +5B)'
        ELSE 'VERY_POS (>+5B)'
    END as vanna_bucket,
    COUNT(*) as trades,
    SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses,
    SUM(CASE WHEN outcome_result = 'EXPIRED' THEN 1 ELSE 0 END) as expired,
    ROUND(100.0 * SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) as win_rate,
    ROUND(SUM(outcome_pnl)::numeric, 1) as total_pnl,
    ROUND(AVG(outcome_pnl)::numeric, 1) as avg_pnl
FROM trade_vanna
GROUP BY 1
ORDER BY 1;
""")

# Query 15: Higher-tenor vanna magnitude buckets
run_query("15 - GEX Long WR by higher-tenor (ALL-TODAY) magnitude bucket", """
WITH gex_trades AS (
    SELECT id, ts, outcome_result, outcome_pnl
    FROM setup_log
    WHERE setup_name = 'GEX Long'
    AND outcome_result IS NOT NULL
),
trade_vanna AS (
    SELECT g.id,
           g.outcome_result,
           g.outcome_pnl,
           (SELECT SUM(vep.value)
            FROM volland_exposure_points vep
            WHERE vep.greek = 'vanna' AND vep.expiration_option = 'ALL'
            AND vep.ts_utc BETWEEN g.ts - interval '5 minutes' AND g.ts + interval '5 minutes'
           ) as vanna_all,
           (SELECT SUM(vep.value)
            FROM volland_exposure_points vep
            WHERE vep.greek = 'vanna' AND vep.expiration_option = 'TODAY'
            AND vep.ts_utc BETWEEN g.ts - interval '5 minutes' AND g.ts + interval '5 minutes'
           ) as vanna_today
    FROM gex_trades g
),
categorized AS (
    SELECT *,
           COALESCE(vanna_all, 0) - COALESCE(vanna_today, 0) as ht_vanna
    FROM trade_vanna
)
SELECT
    CASE
        WHEN vanna_all IS NULL THEN 'NO_DATA'
        WHEN ht_vanna < -5e9 THEN 'VERY_NEG (<-5B)'
        WHEN ht_vanna < -1e9 THEN 'MOD_NEG (-5B to -1B)'
        WHEN ht_vanna < 0 THEN 'SLIGHT_NEG (-1B to 0)'
        WHEN ht_vanna < 1e9 THEN 'SLIGHT_POS (0 to +1B)'
        WHEN ht_vanna < 5e9 THEN 'MOD_POS (+1B to +5B)'
        ELSE 'VERY_POS (>+5B)'
    END as ht_vanna_bucket,
    COUNT(*) as trades,
    SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses,
    SUM(CASE WHEN outcome_result = 'EXPIRED' THEN 1 ELSE 0 END) as expired,
    ROUND(100.0 * SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) as win_rate,
    ROUND(SUM(outcome_pnl)::numeric, 1) as total_pnl,
    ROUND(AVG(outcome_pnl)::numeric, 1) as avg_pnl
FROM categorized
GROUP BY 1
ORDER BY 1;
""")

# Query 16: Check how vanna data aligns in time - do ALL and TODAY always come together?
run_query("16 - Vanna timestamp alignment check (do ALL+TODAY arrive together?)", """
WITH per_snap AS (
    SELECT ts_utc,
           bool_or(expiration_option = 'ALL') as has_all,
           bool_or(expiration_option = 'TODAY') as has_today,
           bool_or(expiration_option = 'THIS_WEEK') as has_week,
           bool_or(expiration_option = 'THIRTY_NEXT_DAYS') as has_30d,
           COUNT(DISTINCT expiration_option) as exp_types
    FROM volland_exposure_points
    WHERE greek = 'vanna'
    GROUP BY ts_utc
)
SELECT exp_types, has_all, has_today, has_week, has_30d, COUNT(*) as snapshot_count
FROM per_snap
GROUP BY exp_types, has_all, has_today, has_week, has_30d
ORDER BY snapshot_count DESC;
""")

# Query 17: For the +/-5min window, how many vanna snapshots typically match?
run_query("17 - Vanna snapshot count within 5min of each GEX Long trade", """
WITH gex_trades AS (
    SELECT id, ts
    FROM setup_log
    WHERE setup_name = 'GEX Long'
    AND outcome_result IS NOT NULL
    AND ts >= '2026-02-11'  -- only after vanna data started
)
SELECT g.id,
       g.ts AT TIME ZONE 'America/New_York' as time_et,
       (SELECT COUNT(DISTINCT ts_utc) FROM volland_exposure_points
        WHERE greek = 'vanna' AND expiration_option = 'ALL'
        AND ts_utc BETWEEN g.ts - interval '5 minutes' AND g.ts + interval '5 minutes'
       ) as all_snap_count,
       (SELECT COUNT(DISTINCT ts_utc) FROM volland_exposure_points
        WHERE greek = 'vanna' AND expiration_option = 'TODAY'
        AND ts_utc BETWEEN g.ts - interval '5 minutes' AND g.ts + interval '5 minutes'
       ) as today_snap_count
FROM gex_trades g
ORDER BY g.ts;
""")

# Query 18: Use CLOSEST single snapshot instead of sum-within-window
run_query("18 - GEX Long with CLOSEST vanna snapshot (not window sum)", """
WITH gex_trades AS (
    SELECT id, ts, outcome_result, outcome_pnl, outcome_max_profit,
           paradigm, spot, grade
    FROM setup_log
    WHERE setup_name = 'GEX Long'
    AND outcome_result IS NOT NULL
    AND ts >= '2026-02-11'
)
SELECT g.id,
       g.ts AT TIME ZONE 'America/New_York' as time_et,
       g.outcome_result,
       g.outcome_pnl,
       g.paradigm,
       g.grade,
       ROUND(g.spot::numeric, 1) as spot,
       (SELECT ROUND(SUM(v.value)::numeric, 0)
        FROM volland_exposure_points v
        WHERE v.greek = 'vanna' AND v.expiration_option = 'ALL'
        AND v.ts_utc = (
            SELECT ts_utc FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'ALL'
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts_utc - g.ts)))
            LIMIT 1
        )
       ) as closest_vanna_all,
       (SELECT ROUND(SUM(v.value)::numeric, 0)
        FROM volland_exposure_points v
        WHERE v.greek = 'vanna' AND v.expiration_option = 'TODAY'
        AND v.ts_utc = (
            SELECT ts_utc FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'TODAY'
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts_utc - g.ts)))
            LIMIT 1
        )
       ) as closest_vanna_today,
       (SELECT ROUND(SUM(v.value)::numeric, 0)
        FROM volland_exposure_points v
        WHERE v.greek = 'vanna' AND v.expiration_option = 'THIRTY_NEXT_DAYS'
        AND v.ts_utc = (
            SELECT ts_utc FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'THIRTY_NEXT_DAYS'
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts_utc - g.ts)))
            LIMIT 1
        )
       ) as closest_vanna_30d
FROM gex_trades g
ORDER BY g.ts;
""")

# Query 19: For each trade, get closest snapshot with LATERAL for efficiency
run_query("19 - GEX Long + closest vanna ALL using lateral join", """
WITH gex_trades AS (
    SELECT id, ts, outcome_result, outcome_pnl, outcome_max_profit,
           paradigm, spot, grade
    FROM setup_log
    WHERE setup_name = 'GEX Long'
    AND outcome_result IS NOT NULL
    AND ts >= '2026-02-11'
),
vanna_snaps AS (
    SELECT ts_utc, SUM(value) as total_value
    FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'ALL'
    GROUP BY ts_utc
)
SELECT g.id,
       g.ts AT TIME ZONE 'America/New_York' as time_et,
       g.outcome_result,
       g.outcome_pnl,
       g.paradigm,
       g.grade,
       ROUND(g.spot::numeric, 1) as spot,
       ROUND(v.total_value::numeric, 0) as vanna_all,
       v.ts_utc AT TIME ZONE 'America/New_York' as vanna_ts,
       ROUND(EXTRACT(EPOCH FROM (v.ts_utc - g.ts)) / 60.0, 1) as offset_min
FROM gex_trades g
CROSS JOIN LATERAL (
    SELECT vs.ts_utc, vs.total_value
    FROM vanna_snaps vs
    WHERE vs.ts_utc BETWEEN g.ts - interval '10 minutes' AND g.ts + interval '10 minutes'
    ORDER BY ABS(EXTRACT(EPOCH FROM (vs.ts_utc - g.ts)))
    LIMIT 1
) v
ORDER BY g.ts;
""")

# Query 20: Same but for ALL setups (not just GEX Long) - broader pattern check
run_query("20 - ALL setup types WR by vanna_ALL sign", """
WITH trades AS (
    SELECT id, ts, setup_name, outcome_result, outcome_pnl
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    AND ts >= '2026-02-11'
),
vanna_snaps AS (
    SELECT ts_utc, SUM(value) as total_value
    FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'ALL'
    GROUP BY ts_utc
),
trade_vanna AS (
    SELECT t.id, t.setup_name, t.outcome_result, t.outcome_pnl,
           v.total_value as vanna_all
    FROM trades t
    CROSS JOIN LATERAL (
        SELECT vs.ts_utc, vs.total_value
        FROM vanna_snaps vs
        WHERE vs.ts_utc BETWEEN t.ts - interval '10 minutes' AND t.ts + interval '10 minutes'
        ORDER BY ABS(EXTRACT(EPOCH FROM (vs.ts_utc - t.ts)))
        LIMIT 1
    ) v
)
SELECT setup_name,
    CASE WHEN vanna_all < 0 THEN 'NEGATIVE' ELSE 'POSITIVE' END as vanna_sign,
    COUNT(*) as trades,
    SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
    ROUND(100.0 * SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1) as win_rate,
    ROUND(SUM(outcome_pnl)::numeric, 1) as total_pnl,
    ROUND(AVG(outcome_pnl)::numeric, 1) as avg_pnl
FROM trade_vanna
GROUP BY setup_name, CASE WHEN vanna_all < 0 THEN 'NEGATIVE' ELSE 'POSITIVE' END
ORDER BY setup_name, vanna_sign;
""")
