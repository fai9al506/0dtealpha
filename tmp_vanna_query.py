import psycopg2

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
TZ = "America/New_York"

def run_query(label, sql):
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    conn.close()

    print(f"\n{'='*130}")
    print(f"  {label}")
    print(f"{'='*130}")

    if not rows:
        print("(no rows)")
        return

    widths = [max(len(str(c)), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(cols)]

    header = ' | '.join(f'{c:>{w}}' for c, w in zip(cols, widths))
    print(header)
    print('-' * len(header))
    for r in rows:
        print(' | '.join(f'{str(v):>{w}}' for v, w in zip(r, widths)))
    print(f"\n({len(rows)} rows)")

CLOSING_PRICE_CTE = f"""
closing_price AS (
    SELECT (ts AT TIME ZONE '{TZ}')::date as trade_date, spot as close_price
    FROM (
        SELECT ts, spot,
               ROW_NUMBER() OVER (PARTITION BY (ts AT TIME ZONE '{TZ}')::date ORDER BY ts DESC) as rn
        FROM chain_snapshots
        WHERE EXTRACT(HOUR FROM ts AT TIME ZONE '{TZ}') BETWEEN 15 AND 16
          AND spot IS NOT NULL
    ) sub WHERE rn = 1
)
"""

def snap_cte(name, hour_start, hour_end=None):
    if hour_end is not None:
        hf = f"EXTRACT(HOUR FROM ts_utc AT TIME ZONE '{TZ}') BETWEEN {hour_start} AND {hour_end}"
    else:
        hf = f"EXTRACT(HOUR FROM ts_utc AT TIME ZONE '{TZ}') >= {hour_start}"
    return f"""
{name} AS (
    SELECT (ts_utc AT TIME ZONE '{TZ}')::date as trade_date, MAX(ts_utc) as snap_ts
    FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'TODAY'
      AND {hf}
    GROUP BY (ts_utc AT TIME ZONE '{TZ}')::date
)"""

def mav_cte(snap_name):
    return f"""
max_abs_vanna AS (
    SELECT ms.trade_date, vep.strike, vep.value, vep.current_price,
           ABS(vep.value) as abs_val,
           vep.strike - vep.current_price as vs_spot,
           CASE WHEN vep.value > 0 THEN 'GREEN' ELSE 'RED' END as color,
           CASE WHEN vep.strike >= vep.current_price THEN 'ABOVE' ELSE 'BELOW' END as position,
           ROW_NUMBER() OVER (PARTITION BY ms.trade_date ORDER BY ABS(vep.value) DESC) as rn
    FROM {snap_name} ms
    JOIN volland_exposure_points vep ON vep.ts_utc = ms.snap_ts
        AND vep.greek = 'vanna' AND vep.expiration_option = 'TODAY'
    WHERE vep.strike BETWEEN vep.current_price - 50 AND vep.current_price + 50
)"""

# Q1: Midday
run_query("QUERY 1: Midday (13-14 ET) - Controlling Vanna vs Close", f"""
WITH {snap_cte('midday_snap', 13, 14)},
{mav_cte('midday_snap')},
{CLOSING_PRICE_CTE}
SELECT v.trade_date, v.strike as ctrl_vanna, ROUND(v.value::numeric/1e6, 1) as val_M, v.color,
       ROUND(v.current_price::numeric, 1) as spot_mid,
       ROUND(v.vs_spot::numeric, 1) as vs_spot,
       ROUND(cp.close_price::numeric, 1) as close_px,
       ROUND(ABS(v.strike - cp.close_price)::numeric, 1) as dist,
       CASE WHEN ABS(v.strike - cp.close_price) <= 5 THEN 'PIN'
            WHEN ABS(v.strike - cp.close_price) <= 10 THEN 'NEAR'
            ELSE 'MISS' END as result
FROM max_abs_vanna v
LEFT JOIN closing_price cp ON v.trade_date = cp.trade_date
WHERE v.rn = 1
ORDER BY v.trade_date
""")

# Q2: Morning
run_query("QUERY 2: Morning (11-12 ET) - Controlling Vanna vs Close", f"""
WITH {snap_cte('morning_snap', 11, 12)},
{mav_cte('morning_snap')},
{CLOSING_PRICE_CTE}
SELECT v.trade_date, v.strike as ctrl_vanna, ROUND(v.value::numeric/1e6, 1) as val_M, v.color,
       ROUND(v.current_price::numeric, 1) as spot_am,
       ROUND(v.vs_spot::numeric, 1) as vs_spot,
       ROUND(cp.close_price::numeric, 1) as close_px,
       ROUND(ABS(v.strike - cp.close_price)::numeric, 1) as dist,
       CASE WHEN ABS(v.strike - cp.close_price) <= 5 THEN 'PIN'
            WHEN ABS(v.strike - cp.close_price) <= 10 THEN 'NEAR'
            ELSE 'MISS' END as result
FROM max_abs_vanna v
LEFT JOIN closing_price cp ON v.trade_date = cp.trade_date
WHERE v.rn = 1
ORDER BY v.trade_date
""")

# Q3: EOD
run_query("QUERY 3: EOD (15+ ET) - Controlling Vanna vs Close", f"""
WITH {snap_cte('eod_snap', 15)},
{mav_cte('eod_snap')},
{CLOSING_PRICE_CTE}
SELECT v.trade_date, v.strike as ctrl_vanna, ROUND(v.value::numeric/1e6, 1) as val_M, v.color,
       ROUND(v.current_price::numeric, 1) as spot_eod,
       ROUND(v.vs_spot::numeric, 1) as vs_spot,
       ROUND(cp.close_price::numeric, 1) as close_px,
       ROUND(ABS(v.strike - cp.close_price)::numeric, 1) as dist,
       CASE WHEN ABS(v.strike - cp.close_price) <= 5 THEN 'PIN'
            WHEN ABS(v.strike - cp.close_price) <= 10 THEN 'NEAR'
            ELSE 'MISS' END as result
FROM max_abs_vanna v
LEFT JOIN closing_price cp ON v.trade_date = cp.trade_date
WHERE v.rn = 1
ORDER BY v.trade_date
""")

# Q4: Top 3 midday
run_query("QUERY 4: Top 3 Absolute Vanna Strikes Near Spot (Midday 13-14 ET)", f"""
WITH {snap_cte('midday_snap', 13, 14)},
{mav_cte('midday_snap')}
SELECT trade_date, rn as rank, strike, ROUND(value::numeric/1e6, 1) as val_M, color,
       ROUND(current_price::numeric, 1) as spot,
       ROUND(vs_spot::numeric, 1) as vs_spot,
       ROUND(abs_val::numeric/1e6, 1) as abs_M
FROM max_abs_vanna
WHERE rn <= 3
ORDER BY trade_date, rn
""")

# Q5: Mar 4 intraday
run_query("QUERY 5: March 4 Intraday - Controlling Vanna Throughout Day", f"""
WITH all_snaps AS (
    SELECT DISTINCT ts_utc
    FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'TODAY'
      AND (ts_utc AT TIME ZONE '{TZ}')::date = '2026-03-04'
      AND EXTRACT(HOUR FROM ts_utc AT TIME ZONE '{TZ}') BETWEEN 9 AND 16
),
max_abs AS (
    SELECT s.ts_utc,
           to_char(s.ts_utc AT TIME ZONE '{TZ}', 'HH24:MI') as et_time,
           vep.strike, vep.value,
           ABS(vep.value) as abs_val,
           vep.current_price as spot,
           vep.strike - vep.current_price as vs_spot,
           CASE WHEN vep.value > 0 THEN 'GREEN' ELSE 'RED' END as color,
           ROW_NUMBER() OVER (PARTITION BY s.ts_utc ORDER BY ABS(vep.value) DESC) as rn
    FROM all_snaps s
    JOIN volland_exposure_points vep ON vep.ts_utc = s.ts_utc
        AND vep.greek = 'vanna' AND vep.expiration_option = 'TODAY'
    WHERE vep.strike BETWEEN vep.current_price - 50 AND vep.current_price + 50
)
SELECT et_time, strike as ctrl_strike, ROUND(value::numeric/1e6, 1) as val_M, color,
       ROUND(spot::numeric, 1) as spot, ROUND(vs_spot::numeric, 1) as vs_spot
FROM max_abs
WHERE rn = 1
ORDER BY ts_utc
""")

# Q6: Summary stats
run_query("QUERY 6: Summary Stats - Pin Rates by Method", f"""
WITH {snap_cte('midday_snap', 13, 14)},
vanna_mid AS (
    SELECT ms.trade_date, vep.strike,
           ROW_NUMBER() OVER (PARTITION BY ms.trade_date ORDER BY ABS(vep.value) DESC) as rn
    FROM midday_snap ms
    JOIN volland_exposure_points vep ON vep.ts_utc = ms.snap_ts
        AND vep.greek = 'vanna' AND vep.expiration_option = 'TODAY'
    WHERE vep.strike BETWEEN vep.current_price - 50 AND vep.current_price + 50
),
{snap_cte('eod_snap', 15).strip()},
vanna_eod AS (
    SELECT ms.trade_date, vep.strike,
           ROW_NUMBER() OVER (PARTITION BY ms.trade_date ORDER BY ABS(vep.value) DESC) as rn
    FROM eod_snap ms
    JOIN volland_exposure_points vep ON vep.ts_utc = ms.snap_ts
        AND vep.greek = 'vanna' AND vep.expiration_option = 'TODAY'
    WHERE vep.strike BETWEEN vep.current_price - 50 AND vep.current_price + 50
),
{snap_cte('morning_snap', 11, 12).strip()},
vanna_am AS (
    SELECT ms.trade_date, vep.strike,
           ROW_NUMBER() OVER (PARTITION BY ms.trade_date ORDER BY ABS(vep.value) DESC) as rn
    FROM morning_snap ms
    JOIN volland_exposure_points vep ON vep.ts_utc = ms.snap_ts
        AND vep.greek = 'vanna' AND vep.expiration_option = 'TODAY'
    WHERE vep.strike BETWEEN vep.current_price - 50 AND vep.current_price + 50
),
{CLOSING_PRICE_CTE},
am_results AS (
    SELECT v.trade_date, ABS(v.strike - cp.close_price) as dist
    FROM vanna_am v JOIN closing_price cp ON v.trade_date = cp.trade_date WHERE v.rn = 1
),
mid_results AS (
    SELECT v.trade_date, ABS(v.strike - cp.close_price) as dist
    FROM vanna_mid v JOIN closing_price cp ON v.trade_date = cp.trade_date WHERE v.rn = 1
),
eod_results AS (
    SELECT v.trade_date, ABS(v.strike - cp.close_price) as dist
    FROM vanna_eod v JOIN closing_price cp ON v.trade_date = cp.trade_date WHERE v.rn = 1
)
SELECT 'Vanna MaxAbs Morning' as method,
       COUNT(*) as days,
       SUM(CASE WHEN dist <= 5 THEN 1 ELSE 0 END) as pin5,
       ROUND(100.0 * SUM(CASE WHEN dist <= 5 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct5,
       SUM(CASE WHEN dist <= 10 THEN 1 ELSE 0 END) as pin10,
       ROUND(100.0 * SUM(CASE WHEN dist <= 10 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct10,
       ROUND(AVG(dist)::numeric, 1) as avg_dist
FROM am_results
UNION ALL
SELECT 'Vanna MaxAbs Midday',
       COUNT(*),
       SUM(CASE WHEN dist <= 5 THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN dist <= 5 THEN 1 ELSE 0 END) / COUNT(*), 1),
       SUM(CASE WHEN dist <= 10 THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN dist <= 10 THEN 1 ELSE 0 END) / COUNT(*), 1),
       ROUND(AVG(dist)::numeric, 1)
FROM mid_results
UNION ALL
SELECT 'Vanna MaxAbs EOD',
       COUNT(*),
       SUM(CASE WHEN dist <= 5 THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN dist <= 5 THEN 1 ELSE 0 END) / COUNT(*), 1),
       SUM(CASE WHEN dist <= 10 THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN dist <= 10 THEN 1 ELSE 0 END) / COUNT(*), 1),
       ROUND(AVG(dist)::numeric, 1)
FROM eod_results
""")

# Q7: Color + Position breakdown
run_query("QUERY 7: Pin Rate by Color + Position (Midday)", f"""
WITH {snap_cte('midday_snap', 13, 14)},
{mav_cte('midday_snap')},
{CLOSING_PRICE_CTE}
SELECT v.color, v.position,
       COUNT(*) as days,
       SUM(CASE WHEN ABS(v.strike - cp.close_price) <= 5 THEN 1 ELSE 0 END) as pin5,
       SUM(CASE WHEN ABS(v.strike - cp.close_price) <= 10 THEN 1 ELSE 0 END) as pin10,
       ROUND(AVG(ABS(v.strike - cp.close_price))::numeric, 1) as avg_dist
FROM max_abs_vanna v
JOIN closing_price cp ON v.trade_date = cp.trade_date
WHERE v.rn = 1
GROUP BY v.color, v.position
ORDER BY v.color, v.position
""")
