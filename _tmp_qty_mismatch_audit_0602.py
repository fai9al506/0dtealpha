"""Investigate 2026-06-02 QTY MISMATCH + GHOST on 210VYX65 (SC/DD long cluster)."""
import psycopg2, json
from datetime import datetime

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB)
cur = conn.cursor()

print("=" * 100)
print("ALL real_trade_orders on 210VYX65 today (2026-06-02):")
print("=" * 100)
cur.execute("""
    SELECT setup_log_id,
           state->>'setup_name', state->>'direction', state->>'quantity',
           state->>'status', state->>'fill_price', state->>'close_fill_price',
           state->>'close_reason', state->>'ts_placed', state->>'ts_closed',
           state->>'atomic_bracket',
           state->>'entry_order_id', state->>'stop_order_id', state->>'target_order_id',
           state->>'closing_in_progress', state->>'close_telegram_sent',
           created_at, updated_at, state
    FROM real_trade_orders
    WHERE state->>'account_id' = '210VYX65'
      AND created_at::date = '2026-06-02'
    ORDER BY setup_log_id
""")
rows = cur.fetchall()
for r in rows:
    print(f"\nlid={r[0]}  {r[1]} {r[2]}  qty={r[3]}  status={r[4]}  atomic={r[10]}")
    print(f"  fill={r[5]} close={r[6]} reason={r[7]}")
    print(f"  ts_placed={r[8]} ts_closed={r[9]}")
    print(f"  entry_oid={r[11]} stop_oid={r[12]} target_oid={r[13]}")
    print(f"  closing_in_progress={r[14]} close_tel_sent={r[15]}")
    print(f"  row_created={r[16]} row_updated={r[17]}")
    st = r[18]
    # dump any trail / s131 / extra keys
    extra = {k: v for k, v in st.items() if k in (
        'internal_trail', 's131_trail', 'current_stop', 'be_moved',
        'trail_locked', 'force_released', 'mfe', 'max_fav', 'entry_price',
        'spx_entry', 'es_entry', 'target_pts', 'stop_pts', 'broker_qty_last')}
    if extra:
        print(f"  extra={json.dumps(extra, default=str)}")

print("\n" + "=" * 100)
print("setup_log for these lids (full detail):")
print("=" * 100)
lids = [r[0] for r in rows]
if lids:
    cur.execute("""
        SELECT id, setup_name, direction, grade,
               (ts AT TIME ZONE 'America/New_York') as et_ts,
               outcome_result, outcome_pnl, real_trade_skip_reason,
               close_fill_price, close_reason, real_trade_pnl
        FROM setup_log
        WHERE id = ANY(%s)
        ORDER BY id
    """, (lids,))
    for r in cur.fetchall():
        print(f"lid={r[0]:5d} {r[1]:18s} {r[2]:5s} g={r[3] or '-':4s} et={r[4]}  "
              f"out={r[5] or '-':10s} pnl={r[6] or 0:6.2f} skip={r[7] or '-'}")
        print(f"        close_fill={r[8]} close_reason={r[9]} real_pnl={r[10]}")

print("\n" + "=" * 100)
print("ALL setup_log LONGS today (full day):")
print("=" * 100)
cur.execute("""
    SELECT id, setup_name, direction, grade,
           (ts AT TIME ZONE 'America/New_York') as et_ts,
           outcome_result, outcome_pnl, real_trade_skip_reason
    FROM setup_log
    WHERE ts::date = '2026-06-02' AND direction = 'long'
    ORDER BY id
""")
for r in cur.fetchall():
    print(f"lid={r[0]:5d} {r[1]:18s} g={r[3] or '-':4s} et={r[4]}  "
          f"out={r[5] or '-':10s} pnl={r[6] or 0:6.2f} skip={r[7] or '-'}")

conn.close()
