"""When did atomic path start? Find the time gap between last SEQ and first ATOMIC."""
import os, psycopg2, json
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
    SELECT setup_log_id, state, created_at
    FROM real_trade_orders
    WHERE created_at::date = '2026-05-20'
    ORDER BY setup_log_id
""")
print("lid  | created_ET            | path | setup")
print("-" * 80)
last_seq = None
first_atomic = None
for sid, state, created in cur.fetchall():
    if isinstance(state, str): state = json.loads(state)
    atomic = state.get("atomic_bracket")
    et = created.astimezone(ET)
    path = "ATOMIC" if atomic else "  SEQ "
    setup = state.get("setup_name")
    print(f"{sid} | {et.strftime('%H:%M:%S ET')}        | {path} | {setup}")
    if atomic and first_atomic is None:
        first_atomic = (sid, et)
    if not atomic:
        last_seq = (sid, et)

print()
if last_seq and first_atomic:
    gap = (first_atomic[1] - last_seq[1]).total_seconds()
    print(f"Last SEQ:     lid={last_seq[0]}  at {last_seq[1].strftime('%H:%M:%S ET')}")
    print(f"First ATOMIC: lid={first_atomic[0]}  at {first_atomic[1].strftime('%H:%M:%S ET')}")
    print(f"Gap between: {gap:.0f} seconds ({gap/60:.1f} min)")
    print(f"→ Service was restarting somewhere in this window")

cur.close(); c.close()
