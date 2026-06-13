import os, json
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"])
with eng.connect() as c:
    rows = c.execute(text("""
        SELECT setup_log_id, state FROM real_trade_orders
        WHERE (state->>'status') NOT IN ('closed','cancelled')
        ORDER BY setup_log_id DESC LIMIT 10
    """)).fetchall()
for lid, st in rows:
    s = st if isinstance(st, dict) else json.loads(st)
    print(f"--- lid={lid} setup={s.get('setup_name')} dir={s.get('direction')} status={s.get('status')}")
    keys = ["entry_fill_price","mes_entry","stop_price","current_stop","initial_stop","trail_active",
            "max_favorable","stop_order_id","spx_entry","spx_stop","spx_current_stop","be_done",
            "trail_locked","internal_trail","s131_trail","target_pts","stop_pts"]
    for k in keys:
        if k in s: print(f"    {k} = {s[k]}")
    # print any key containing 'trail' or 'stop' not already shown
    for k,v in s.items():
        if ("trail" in k or "stop" in k) and k not in keys:
            print(f"    {k} = {v}")
