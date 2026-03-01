import os, psycopg2
c = psycopg2.connect(os.environ['DATABASE_URL'])
r = c.cursor()
r.execute('SELECT id, ts, spot, abs_es_price, direction, outcome_target_level, outcome_stop_level, outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss FROM setup_log WHERE id IN (275, 278, 280) ORDER BY id')
for row in r.fetchall():
    print(f"ID={row[0]} ts={row[1]} SPX={row[2]} ES={row[3]} {row[4]}")
    print(f"  tgt={row[5]} stp={row[6]} | {row[7]} {row[8]} mp={row[9]} ml={row[10]}")
r.execute('SELECT bar_idx, bar_close, ts_end FROM es_range_bars WHERE trade_date=%s AND source=%s AND bar_idx IN (199, 206)', ('2026-02-26', 'rithmic'))
for row in r.fetchall():
    print(f"  bar {row[0]}: close={row[1]} ts_end={row[2]}")
r.execute('SELECT bar_idx, bar_close, ts_end FROM es_range_bars WHERE trade_date=%s AND source=%s AND ts_end <= %s ORDER BY bar_idx DESC LIMIT 1', ('2026-02-26', 'rithmic', '2026-02-26T18:38:17Z'))
print(f"Last bar at 13:38: {r.fetchone()}")
c.close()
