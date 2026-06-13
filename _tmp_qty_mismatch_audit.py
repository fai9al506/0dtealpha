"""Investigate 2026-05-22 QTY MISMATCH alerts on 210VYX65 around 15:29 ET."""
import psycopg2, json
from datetime import datetime

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB)
cur = conn.cursor()

print("=" * 90)
print("ALL real_trade_orders on 210VYX65 today (2026-05-22):")
print("=" * 90)
cur.execute("""
    SELECT setup_log_id,
           state->>'setup_name', state->>'direction', state->>'quantity',
           state->>'status', state->>'fill_price', state->>'close_fill_price',
           state->>'close_reason',
           state->>'ts_placed', state->>'ts_closed',
           state->>'atomic_bracket',
           state->>'entry_order_id', state->>'stop_order_id', state->>'target_order_id',
           created_at, updated_at
    FROM real_trade_orders
    WHERE state->>'account_id' = '210VYX65'
      AND created_at::date = '2026-05-22'
    ORDER BY setup_log_id
""")
for r in cur.fetchall():
    print(f"\nlid={r[0]}  {r[1]} {r[2]}  qty={r[3]}  status={r[4]}  atomic={r[10]}")
    print(f"  fill={r[5]} close={r[6]} reason={r[7]}")
    print(f"  ts_placed={r[8]} ts_closed={r[9]}")
    print(f"  entry_oid={r[11]} stop_oid={r[12]} target_oid={r[13]}")
    print(f"  row_created={r[14]} row_updated={r[15]}")

print("\n" + "=" * 90)
print("setup_log LONGS today, 14:00-16:00 ET:")
print("=" * 90)
cur.execute("""
    SELECT id, setup_name, direction, grade,
           (ts AT TIME ZONE 'America/New_York') as et_ts,
           outcome_result, outcome_pnl, real_trade_skip_reason
    FROM setup_log
    WHERE ts::date = '2026-05-22'
      AND direction = 'long'
      AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '14:00' AND '16:00'
    ORDER BY id
""")
for r in cur.fetchall():
    print(f"lid={r[0]:5d} {r[1]:28s} g={r[3] or '-':4s} et={r[4]}  "
          f"out={r[5] or '-':10s} pnl={r[6] or 0:6.2f}  skip={r[7] or '-'}")

print("\n" + "=" * 90)
print("ALL setup_log LONGS today (full day):")
print("=" * 90)
cur.execute("""
    SELECT id, setup_name, direction, grade,
           (ts AT TIME ZONE 'America/New_York') as et_ts,
           outcome_result, outcome_pnl, real_trade_skip_reason
    FROM setup_log
    WHERE ts::date = '2026-05-22'
      AND direction = 'long'
    ORDER BY id
""")
for r in cur.fetchall():
    print(f"lid={r[0]:5d} {r[1]:28s} g={r[3] or '-':4s} et={r[4]}  "
          f"out={r[5] or '-':10s} pnl={r[6] or 0:6.2f}  skip={r[7] or '-'}")

conn.close()
