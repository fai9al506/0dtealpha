"""Identify today's 8 silent-skip whitelist signals."""
import os, psycopg2

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
    SELECT sl.id, sl.ts, sl.setup_name, sl.direction, sl.grade, sl.paradigm,
           sl.greek_alignment, sl.notified, sl.real_trade_skip_reason
    FROM setup_log sl
    LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
    WHERE sl.ts::date = '2026-05-20'
      AND sl.real_trade_skip_reason IS NULL
      AND rto.setup_log_id IS NULL
      AND sl.setup_name IN ('Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion','VIX Divergence','GEX Long')
    ORDER BY sl.ts
""")
print("Today's silent-skip whitelist signals:")
for row in cur.fetchall():
    sid, ts, name, dir_, grade, para, align, notified, skip = row
    et = ts.strftime("%H:%M ET")
    print(f"  lid={sid} {et} {name} {dir_} g={grade} p={para} align={align} notified={notified}")

cur.close(); c.close()
