import psycopg2, os
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Get all ES range bars from session
print("=== ES 5-pt Range Bars (full session) ===")
cur.execute("""
SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
       bar_volume, bar_delta, bar_buy_volume, bar_sell_volume,
       cumulative_delta, cvd_close,
       ts_start AT TIME ZONE 'America/New_York' as start_et,
       ts_end AT TIME ZONE 'America/New_York' as end_et,
       status
FROM es_range_bars
WHERE trade_date = '2026-02-19'
  AND range_pts = 5.0
ORDER BY bar_idx ASC
""")
rows = cur.fetchall()
print(f"Found {len(rows)} bars total")
# Focus on bars around the 10:15-10:40 area and print all bars
for r in rows:
    start_str = r[11].strftime('%H:%M:%S') if r[11] else '?'
    end_str = r[12].strftime('%H:%M:%S') if r[12] else '?'
    # Highlight bars near 10:28 timeframe
    marker = ""
    if r[11] and r[11].hour == 10 and 20 <= r[11].minute <= 35:
        marker = " <<<<<<"
    elif r[12] and r[12].hour == 10 and 20 <= r[12].minute <= 35:
        marker = " <<<<<<"
    print(f"#{r[0]:>3} | {start_str}-{end_str} | O={r[1]:.2f} H={r[2]:.2f} L={r[3]:.2f} C={r[4]:.2f} | "
          f"vol={r[5]:>6} d={r[6]:>+6} buy={r[7]:>6} sell={r[8]:>6} | "
          f"cvd={r[9]:>+8} cvd_c={r[10]:>+8} | {r[13]}{marker}")

conn.close()
