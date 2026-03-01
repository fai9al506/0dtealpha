import os, psycopg2
c = psycopg2.connect(os.environ['DATABASE_URL'])
r = c.cursor()

# Remaining absorption trades today
r.execute("SELECT id, ts, direction, abs_es_price, outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss, outcome_target_level, outcome_stop_level FROM setup_log WHERE setup_name='ES Absorption' AND ts::date='2026-02-26' ORDER BY id")
for row in r.fetchall():
    print(f"ID={row[0]} {row[1]} {row[2]} ES={row[3]} | {row[4]} {row[5]} mp={row[6]} ml={row[7]} tgt={row[8]} stp={row[9]}")

# Verify #270: SHORT at 6896.75, target=6886.75 (entry-10)
# Check if ES actually reached 6886.75 after 12:11 ET
print("\n--- Trade #270 verification ---")
r.execute("""SELECT bar_idx, bar_low, bar_close, ts_end FROM es_range_bars
             WHERE trade_date='2026-02-26' AND source='rithmic'
             AND ts_end >= '2026-02-26T17:11:30Z'
             ORDER BY bar_idx LIMIT 20""")
for row in r.fetchall():
    lo = row[1]
    mark = " *** TARGET HIT" if lo <= 6886.75 else ""
    print(f"  bar {row[0]}: low={lo} close={row[2]} {row[3]}{mark}")

c.close()
