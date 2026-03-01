import os, psycopg2
c = psycopg2.connect(os.environ['DATABASE_URL'])
r = c.cursor()

# Find this trade
r.execute("""SELECT id, ts AT TIME ZONE 'America/New_York', spot, abs_es_price, direction,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
       outcome_target_level, outcome_stop_level
FROM setup_log WHERE setup_name='ES Absorption' AND ts::date='2026-02-26'
ORDER BY ts DESC LIMIT 5""")
for row in r.fetchall():
    print(f"ID={row[0]} {row[1]} SPX={row[2]} ES={row[3]} {row[4]} | {row[5]} {row[6]} mp={row[7]} ml={row[8]} {row[9]}min | tgt={row[10]} stp={row[11]}")

# What was actual Rithmic bar at 13:38 ET (18:38 UTC)?
print("\n--- Rithmic bars around 13:38 ET ---")
r.execute("SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, ts_start, ts_end FROM es_range_bars WHERE trade_date=%s AND source=%s AND ts_end BETWEEN %s AND %s ORDER BY bar_idx",
          ('2026-02-26', 'rithmic', '2026-02-26T18:35:00Z', '2026-02-26T18:42:00Z'))
for row in r.fetchall():
    print(f"  bar={row[0]} O={row[1]} H={row[2]} L={row[3]} C={row[4]} | {row[5]} - {row[6]}")

# What is live_since_idx? Check latest bars
r.execute("SELECT max(bar_idx) FROM es_range_bars WHERE trade_date=%s AND source=%s", ('2026-02-26', 'rithmic'))
print(f"\nMax rithmic bar_idx: {r.fetchone()[0]}")

# Check Rithmic bar with close=6882.0
r.execute("SELECT bar_idx, bar_close, ts_end FROM es_range_bars WHERE trade_date=%s AND source=%s AND bar_close=6882.0",
          ('2026-02-26', 'rithmic'))
print(f"Rithmic bars with close=6882.0: {r.fetchall()}")

c.close()
