import os, psycopg2, pytz
from datetime import datetime

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# DST check first
et = pytz.timezone('US/Eastern')
dt = et.localize(datetime(2026, 3, 3, 12, 0))
offset = dt.utcoffset().total_seconds() / 3600
print(f"=== DST CHECK: March 3 2026 = {dt.strftime('%Z')} (UTC{'+' if offset>=0 else ''}{int(offset)}) ===\n")

# 1. Rithmic range bars 9:44-9:52 ET
print("=== RITHMIC 5pt RANGE BARS 9:44-9:52 ET (our data) ===")
cur.execute("""
    SELECT bar_idx, ts_start, ts_end, bar_open, bar_high, bar_low, bar_close, bar_volume
    FROM es_range_bars WHERE trade_date='2026-03-03' AND source='rithmic'
    AND ts_start >= '2026-03-03 14:44:00+00' AND ts_end <= '2026-03-03 14:52:00+00'
    ORDER BY bar_idx
""")
for r in cur.fetchall():
    s = r[1].astimezone(et).strftime("%H:%M:%S")
    e = r[2].astimezone(et).strftime("%H:%M:%S")
    print(f"  #{r[0]} {s}-{e}  O={r[3]} H={r[4]} L={r[5]} C={r[6]}  vol={r[7]}")

# 2. TradeStation ES 1-min bars (completely independent source)
print("\n=== TRADESTATION @ES 1-MIN BARS 9:44-9:52 ET ===")
cur.execute("""
    SELECT ts, bar_open_price, bar_high_price, bar_low_price, bar_close_price, bar_volume
    FROM es_delta_bars WHERE ts::date='2026-03-03'
    AND ts >= '2026-03-03 14:44:00+00' AND ts <= '2026-03-03 14:52:00+00'
    ORDER BY ts
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        t = r[0].astimezone(et).strftime("%H:%M:%S") if r[0].tzinfo else r[0].strftime("%H:%M:%S")
        print(f"  {t}  O={r[1]} H={r[2]} L={r[3]} C={r[4]}  vol={r[5]}")
else:
    print("  (no data)")

# 3. SPX spot from chain snapshots
print("\n=== SPX SPOT (chain_snapshots) 9:40-9:55 ET ===")
cur.execute("""
    SELECT ts, spot FROM chain_snapshots
    WHERE ts::date='2026-03-03'
    AND ts >= '2026-03-03 14:40:00+00' AND ts <= '2026-03-03 14:55:00+00'
    ORDER BY ts
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        t = r[0].astimezone(et).strftime("%H:%M:%S") if r[0].tzinfo else r[0].strftime("%H:%M:%S")
        print(f"  {t}  SPX={r[1]}")
else:
    print("  (no data)")

# 4. Live (TS websocket) range bars at same time
print("\n=== LIVE (TS WebSocket) RANGE BARS 9:44-9:52 ET ===")
cur.execute("""
    SELECT bar_idx, ts_start, ts_end, bar_open, bar_high, bar_low, bar_close, bar_volume
    FROM es_range_bars WHERE trade_date='2026-03-03' AND source='live'
    AND ts_start >= '2026-03-03 14:44:00+00' AND ts_end <= '2026-03-03 14:52:00+00'
    ORDER BY bar_idx
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        s = r[1].astimezone(et).strftime("%H:%M:%S")
        e = r[2].astimezone(et).strftime("%H:%M:%S")
        print(f"  #{r[0]} {s}-{e}  O={r[3]} H={r[4]} L={r[5]} C={r[6]}  vol={r[7]}")
else:
    print("  (no data)")

# 5. What was the first range bar of RTH? (bar closest to 9:30 ET)
print("\n=== FIRST RTH BAR (nearest 9:30 ET) ===")
cur.execute("""
    SELECT bar_idx, ts_start, bar_open, bar_high, bar_low, bar_close
    FROM es_range_bars WHERE trade_date='2026-03-03' AND source='rithmic'
    AND ts_start >= '2026-03-03 14:29:00+00' AND ts_start <= '2026-03-03 14:31:00+00'
    ORDER BY bar_idx LIMIT 3
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        s = r[1].astimezone(et).strftime("%H:%M:%S")
        print(f"  #{r[0]} {s} O={r[2]} H={r[3]} L={r[4]} C={r[5]}")
else:
    print("  (no bars near 9:30)")

# 6. What was the session open bar?
print("\n=== SESSION OPEN (first bars) ===")
cur.execute("""
    SELECT bar_idx, ts_start, bar_open, bar_high, bar_low, bar_close
    FROM es_range_bars WHERE trade_date='2026-03-03' AND source='rithmic'
    ORDER BY bar_idx LIMIT 3
""")
for r in cur.fetchall():
    s = r[1].astimezone(et).strftime("%H:%M:%S")
    print(f"  #{r[0]} {s} O={r[2]} H={r[3]} L={r[4]} C={r[5]}")

conn.close()
