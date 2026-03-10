import psycopg2

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB)
cur = conn.cursor()
TZ = "America/New_York"

CTE = f"""
WITH midday_snap AS (
    SELECT ts_utc::date as trade_date, MAX(ts_utc) as snap_ts
    FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'TODAY'
      AND EXTRACT(HOUR FROM ts_utc AT TIME ZONE '{TZ}') BETWEEN 13 AND 14
    GROUP BY ts_utc::date
),
max_abs_vanna AS (
    SELECT ms.trade_date, vep.strike as pin, vep.value, vep.current_price,
           ABS(vep.value) as abs_val,
           CASE WHEN vep.value > 0 THEN 'GREEN' ELSE 'RED' END as color,
           ROW_NUMBER() OVER (PARTITION BY ms.trade_date ORDER BY ABS(vep.value) DESC) as rn
    FROM midday_snap ms
    JOIN volland_exposure_points vep ON vep.ts_utc = ms.snap_ts
        AND vep.greek = 'vanna' AND vep.expiration_option = 'TODAY'
    WHERE vep.strike BETWEEN vep.current_price - 50 AND vep.current_price + 50
),
closing_price AS (
    SELECT ts::date as trade_date, MAX(spot) as close_price
    FROM chain_snapshots
    WHERE EXTRACT(HOUR FROM ts AT TIME ZONE '{TZ}') BETWEEN 15 AND 16
    GROUP BY ts::date
),
base AS (
    SELECT v.trade_date, v.pin, v.color, v.abs_val, cp.close_price,
           ABS(cp.close_price - v.pin)::numeric as dist
    FROM max_abs_vanna v
    JOIN closing_price cp ON v.trade_date = cp.trade_date
    WHERE v.rn = 1
)
"""

def print_table(title, cols, rows):
    print(f"\n=== {title} ===")
    widths = [max(len(str(c)), max((len(str(r[i])) for r in rows), default=0)) + 2 for i, c in enumerate(cols)]
    header = " | ".join(str(c).rjust(w) for c, w in zip(cols, widths))
    print(header)
    print("-" * len(header))
    for r in rows:
        print(" | ".join(str(x).rjust(w) for x, w in zip(r, widths)))
    print(f"({len(rows)} rows)")

# Query 2: Butterfly P&L by day
cur.execute(CTE + """
SELECT trade_date, pin, color, ROUND(close_price::numeric, 1) as close_px, ROUND(dist, 1) as dist,
       ROUND(GREATEST(0, 5 - dist) - 1.0, 2) as fly5_pnl,
       ROUND(GREATEST(0, 10 - dist) - 2.0, 2) as fly10_pnl,
       ROUND(GREATEST(0, 15 - dist) - 3.0, 2) as fly15_pnl,
       ROUND(GREATEST(0, 20 - dist) - 4.0, 2) as fly20_pnl
FROM base
ORDER BY trade_date
""")
print_table("QUERY 2: Butterfly P&L by Day (5/10/15/20-pt wings)",
            [d[0] for d in cur.description], cur.fetchall())

# Query 3: Butterfly summary
cur.execute(CTE + """
SELECT
  '5pt Fly ($1 debit)' as strategy, COUNT(*) as trades,
  SUM(CASE WHEN GREATEST(0, 5 - dist) - 1.0 > 0 THEN 1 ELSE 0 END) as wins,
  ROUND(100.0 * SUM(CASE WHEN GREATEST(0, 5 - dist) - 1.0 > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct,
  ROUND(SUM(GREATEST(0, 5 - dist) - 1.0), 2) as total_pnl,
  ROUND(AVG(GREATEST(0, 5 - dist) - 1.0), 2) as avg_pnl,
  ROUND(SUM(CASE WHEN GREATEST(0, 5 - dist) - 1.0 > 0 THEN GREATEST(0, 5 - dist) - 1.0 ELSE 0 END), 2) as gross_profit,
  ROUND(ABS(SUM(CASE WHEN GREATEST(0, 5 - dist) - 1.0 <= 0 THEN GREATEST(0, 5 - dist) - 1.0 ELSE 0 END)), 2) as gross_loss
FROM base
UNION ALL
SELECT '10pt Fly ($2 debit)', COUNT(*),
  SUM(CASE WHEN GREATEST(0, 10 - dist) - 2.0 > 0 THEN 1 ELSE 0 END),
  ROUND(100.0 * SUM(CASE WHEN GREATEST(0, 10 - dist) - 2.0 > 0 THEN 1 ELSE 0 END) / COUNT(*), 1),
  ROUND(SUM(GREATEST(0, 10 - dist) - 2.0), 2), ROUND(AVG(GREATEST(0, 10 - dist) - 2.0), 2),
  ROUND(SUM(CASE WHEN GREATEST(0, 10 - dist) - 2.0 > 0 THEN GREATEST(0, 10 - dist) - 2.0 ELSE 0 END), 2),
  ROUND(ABS(SUM(CASE WHEN GREATEST(0, 10 - dist) - 2.0 <= 0 THEN GREATEST(0, 10 - dist) - 2.0 ELSE 0 END)), 2)
FROM base
UNION ALL
SELECT '15pt Fly ($3 debit)', COUNT(*),
  SUM(CASE WHEN GREATEST(0, 15 - dist) - 3.0 > 0 THEN 1 ELSE 0 END),
  ROUND(100.0 * SUM(CASE WHEN GREATEST(0, 15 - dist) - 3.0 > 0 THEN 1 ELSE 0 END) / COUNT(*), 1),
  ROUND(SUM(GREATEST(0, 15 - dist) - 3.0), 2), ROUND(AVG(GREATEST(0, 15 - dist) - 3.0), 2),
  ROUND(SUM(CASE WHEN GREATEST(0, 15 - dist) - 3.0 > 0 THEN GREATEST(0, 15 - dist) - 3.0 ELSE 0 END), 2),
  ROUND(ABS(SUM(CASE WHEN GREATEST(0, 15 - dist) - 3.0 <= 0 THEN GREATEST(0, 15 - dist) - 3.0 ELSE 0 END)), 2)
FROM base
UNION ALL
SELECT '20pt Fly ($4 debit)', COUNT(*),
  SUM(CASE WHEN GREATEST(0, 20 - dist) - 4.0 > 0 THEN 1 ELSE 0 END),
  ROUND(100.0 * SUM(CASE WHEN GREATEST(0, 20 - dist) - 4.0 > 0 THEN 1 ELSE 0 END) / COUNT(*), 1),
  ROUND(SUM(GREATEST(0, 20 - dist) - 4.0), 2), ROUND(AVG(GREATEST(0, 20 - dist) - 4.0), 2),
  ROUND(SUM(CASE WHEN GREATEST(0, 20 - dist) - 4.0 > 0 THEN GREATEST(0, 20 - dist) - 4.0 ELSE 0 END), 2),
  ROUND(ABS(SUM(CASE WHEN GREATEST(0, 20 - dist) - 4.0 <= 0 THEN GREATEST(0, 20 - dist) - 4.0 ELSE 0 END)), 2)
FROM base
""")
print_table("QUERY 3: Butterfly Summary Stats",
            [d[0] for d in cur.description], cur.fetchall())

# Query 4: Iron Condor P&L by day
cur.execute(CTE + """
SELECT trade_date, pin, ROUND(close_price::numeric, 1) as close_px, ROUND(dist, 1) as dist,
       ROUND(CASE WHEN dist <= 3 THEN 3.50 WHEN dist <= 13 THEN 3.50 - (dist - 3) ELSE -6.50 END, 2) as ic_tight_pnl,
       ROUND(CASE WHEN dist <= 5 THEN 4.00 WHEN dist <= 15 THEN 4.00 - (dist - 5) ELSE -6.00 END, 2) as ic_std_pnl,
       ROUND(CASE WHEN dist <= 10 THEN 5.00 WHEN dist <= 20 THEN 5.00 - (dist - 10) ELSE -5.00 END, 2) as ic_wide_pnl
FROM base
ORDER BY trade_date
""")
print_table("QUERY 4: Iron Condor P&L by Day",
            [d[0] for d in cur.description], cur.fetchall())

# Query 5: Iron Condor Summary
cur.execute(CTE + """
SELECT 'Tight IC +/-3/+/-13 ($3.50 cr)' as strategy, COUNT(*) as trades,
  SUM(CASE WHEN CASE WHEN dist <= 3 THEN 3.50 WHEN dist <= 13 THEN 3.50-(dist-3) ELSE -6.50 END > 0 THEN 1 ELSE 0 END) as wins,
  ROUND(100.0 * SUM(CASE WHEN CASE WHEN dist <= 3 THEN 3.50 WHEN dist <= 13 THEN 3.50-(dist-3) ELSE -6.50 END > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct,
  ROUND(SUM(CASE WHEN dist <= 3 THEN 3.50 WHEN dist <= 13 THEN 3.50-(dist-3) ELSE -6.50 END), 2) as total_pnl,
  ROUND(AVG(CASE WHEN dist <= 3 THEN 3.50 WHEN dist <= 13 THEN 3.50-(dist-3) ELSE -6.50 END), 2) as avg_pnl
FROM base
UNION ALL
SELECT 'Std IC +/-5/+/-15 ($4 cr)', COUNT(*),
  SUM(CASE WHEN CASE WHEN dist <= 5 THEN 4.00 WHEN dist <= 15 THEN 4.00-(dist-5) ELSE -6.00 END > 0 THEN 1 ELSE 0 END),
  ROUND(100.0 * SUM(CASE WHEN CASE WHEN dist <= 5 THEN 4.00 WHEN dist <= 15 THEN 4.00-(dist-5) ELSE -6.00 END > 0 THEN 1 ELSE 0 END) / COUNT(*), 1),
  ROUND(SUM(CASE WHEN dist <= 5 THEN 4.00 WHEN dist <= 15 THEN 4.00-(dist-5) ELSE -6.00 END), 2),
  ROUND(AVG(CASE WHEN dist <= 5 THEN 4.00 WHEN dist <= 15 THEN 4.00-(dist-5) ELSE -6.00 END), 2)
FROM base
UNION ALL
SELECT 'Wide IC +/-10/+/-20 ($5 cr)', COUNT(*),
  SUM(CASE WHEN CASE WHEN dist <= 10 THEN 5.00 WHEN dist <= 20 THEN 5.00-(dist-10) ELSE -5.00 END > 0 THEN 1 ELSE 0 END),
  ROUND(100.0 * SUM(CASE WHEN CASE WHEN dist <= 10 THEN 5.00 WHEN dist <= 20 THEN 5.00-(dist-10) ELSE -5.00 END > 0 THEN 1 ELSE 0 END) / COUNT(*), 1),
  ROUND(SUM(CASE WHEN dist <= 10 THEN 5.00 WHEN dist <= 20 THEN 5.00-(dist-10) ELSE -5.00 END), 2),
  ROUND(AVG(CASE WHEN dist <= 10 THEN 5.00 WHEN dist <= 20 THEN 5.00-(dist-10) ELSE -5.00 END), 2)
FROM base
""")
print_table("QUERY 5: Iron Condor Summary Stats",
            [d[0] for d in cur.description], cur.fetchall())

# Query 6: GREEN vs RED filter
cur.execute(CTE + """
SELECT color,
  COUNT(*) as trades,
  ROUND(SUM(GREATEST(0, 15 - dist) - 3.0), 2) as fly15_total,
  ROUND(AVG(GREATEST(0, 15 - dist) - 3.0), 2) as fly15_avg,
  SUM(CASE WHEN GREATEST(0, 15 - dist) - 3.0 > 0 THEN 1 ELSE 0 END) as fly15_wins,
  ROUND(SUM(CASE WHEN dist <= 10 THEN 5.00 WHEN dist <= 20 THEN 5.00-(dist-10) ELSE -5.00 END), 2) as ic_wide_total,
  ROUND(AVG(CASE WHEN dist <= 10 THEN 5.00 WHEN dist <= 20 THEN 5.00-(dist-10) ELSE -5.00 END), 2) as ic_wide_avg,
  SUM(CASE WHEN CASE WHEN dist <= 10 THEN 5.00 WHEN dist <= 20 THEN 5.00-(dist-10) ELSE -5.00 END > 0 THEN 1 ELSE 0 END) as ic_wide_wins,
  ROUND(AVG(dist), 1) as avg_dist,
  ROUND(MIN(dist), 1) as min_dist,
  ROUND(MAX(dist), 1) as max_dist
FROM base
GROUP BY color
ORDER BY color
""")
print_table("QUERY 6: GREEN vs RED Filter",
            [d[0] for d in cur.description], cur.fetchall())

# Query 7: Strength x Color
cur.execute(CTE + """
SELECT
  CASE WHEN abs_val >= 50000000 THEN 'Strong (>=50M)'
       WHEN abs_val >= 30000000 THEN 'Medium (30-50M)'
       ELSE 'Weak (<30M)' END as strength,
  color,
  COUNT(*) as trades,
  ROUND(AVG(dist), 1) as avg_dist,
  SUM(CASE WHEN dist <= 10 THEN 1 ELSE 0 END) as within_10,
  ROUND(SUM(GREATEST(0, 15 - dist) - 3.0), 2) as fly15_pnl,
  ROUND(SUM(CASE WHEN dist <= 10 THEN 5.00 WHEN dist <= 20 THEN 5.00-(dist-10) ELSE -5.00 END), 2) as ic_wide_pnl
FROM base
GROUP BY
  CASE WHEN abs_val >= 50000000 THEN 'Strong (>=50M)'
       WHEN abs_val >= 30000000 THEN 'Medium (30-50M)'
       ELSE 'Weak (<30M)' END,
  color
ORDER BY strength, color
""")
print_table("QUERY 7: Strength x Color Breakdown",
            [d[0] for d in cur.description], cur.fetchall())

conn.close()
print("\nDone.")
