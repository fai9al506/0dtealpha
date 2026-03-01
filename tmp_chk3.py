import os, psycopg2
c = psycopg2.connect(os.environ['DATABASE_URL'])
r = c.cursor()

# Trade #280: ts=18:56:56 UTC = 13:56 ET, ES=6904.75
# What bar was at 6904.75?
r.execute("SELECT bar_idx, bar_close, ts_end FROM es_range_bars WHERE trade_date=%s AND source=%s AND bar_close=%s",
          ('2026-02-26', 'rithmic', 6904.75))
print(f"Bars with close=6904.75: {r.fetchall()}")

# What was the last bar at 13:56 ET (18:56 UTC)?
r.execute("SELECT bar_idx, bar_close, ts_end FROM es_range_bars WHERE trade_date=%s AND source=%s AND ts_end <= %s ORDER BY bar_idx DESC LIMIT 1",
          ('2026-02-26', 'rithmic', '2026-02-26T18:56:56Z'))
print(f"Last bar at 13:56: {r.fetchone()}")

# Max bar at that point?
r.execute("SELECT max(bar_idx) FROM es_range_bars WHERE trade_date=%s AND source=%s AND ts_end <= %s",
          ('2026-02-26', 'rithmic', '2026-02-26T18:56:56Z'))
print(f"Max bar_idx at 13:56: {r.fetchone()}")

# Trade #275: ml=-91.5 -- fix it
# What's the actual max loss for this trade?
r.execute("SELECT abs_es_price, direction, outcome_stop_level FROM setup_log WHERE id=275")
row = r.fetchone()
print(f"\n#275: ES={row[0]} dir={row[1]} stop={row[2]}")
r.execute("""SELECT MAX(bar_high), MIN(bar_low) FROM es_range_bars
             WHERE trade_date='2026-02-26' AND source='rithmic'
             AND ts_end >= '2026-02-26T18:10:12Z' AND ts_end <= '2026-02-26T18:31:00Z'""")
hl = r.fetchone()
print(f"  Bars from 13:10 to 13:31 ET: high={hl[0]} low={hl[1]}")
# entry=6879.25, SHORT, so max_loss = max_high - entry
if hl[0]:
    print(f"  Actual max_loss = {6879.25 - hl[0]:.2f}")

c.close()
