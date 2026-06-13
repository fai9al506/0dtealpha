"""Part B: setup_log detail + current open-position check for 210VYX65."""
import psycopg2, json
DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB); cur = conn.cursor()

# what columns exist on setup_log?
cur.execute("""SELECT column_name FROM information_schema.columns
               WHERE table_name='setup_log' ORDER BY ordinal_position""")
cols = [r[0] for r in cur.fetchall()]
print("setup_log columns:", cols)

lids = [3465, 3468, 3469, 3471]
cur.execute("""
    SELECT id, setup_name, direction, grade,
           (ts AT TIME ZONE 'America/New_York') as et_ts,
           outcome_result, outcome_pnl, real_trade_skip_reason
    FROM setup_log WHERE id = ANY(%s) ORDER BY id
""", (lids,))
print("\n--- setup_log for cluster lids ---")
for r in cur.fetchall():
    print(f"lid={r[0]} {r[1]:14s} {r[2]:5s} g={r[3] or '-'} et={r[4]} "
          f"out={r[5] or '-'} pnl={r[6] or 0} skip={r[7] or '-'}")

# Full state JSON for the still-open lid 3471 and ghost 3468
print("\n--- full state JSON for 3471 (still filled?) and 3468 (ghost) ---")
cur.execute("""SELECT setup_log_id, state, updated_at FROM real_trade_orders
               WHERE setup_log_id = ANY(%s) ORDER BY setup_log_id""", ([3468, 3471],))
for r in cur.fetchall():
    print(f"\n=== lid {r[0]}  (row updated {r[2]}) ===")
    print(json.dumps(r[1], indent=2, default=str))

conn.close()
