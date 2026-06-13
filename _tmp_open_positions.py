"""Find currently open trades — multiple sources."""
import os, json
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"])
with eng.connect() as c:
    print("=== setup_log: today's trades with NULL outcome ===")
    r = c.execute(text("""
        SELECT id, setup_name, direction, grade, paradigm,
               ts AT TIME ZONE 'America/New_York' AS et,
               spot, abs_es_price, outcome_result,
               (SELECT state FROM real_trade_orders WHERE setup_log_id = sl.id) AS rt_state
        FROM setup_log sl
        WHERE ts::date = '2026-05-21'
          AND outcome_result IS NULL
        ORDER BY ts DESC
    """)).fetchall()
    print(f"Found: {len(r)}\n")
    for row in r:
        d = dict(row._mapping)
        rt = d["rt_state"] if isinstance(d["rt_state"], dict) else (json.loads(d["rt_state"]) if d["rt_state"] else None)
        print(f"  lid={d['id']}  {d['et'].strftime('%H:%M:%S')}  {d['setup_name']} {d['direction']} grade={d['grade']} para={d['paradigm']}  spot={d['spot']}  es={d['abs_es_price']}")
        if rt:
            print(f"    real_trade status={rt.get('status')}  acct={rt.get('account_id')}  fill={rt.get('fill_price')}  stop={rt.get('current_stop')}  oids=(entry={rt.get('entry_order_id')}, stop={rt.get('stop_order_id')}, target={rt.get('target_order_id')})  closing={rt.get('closing_in_progress', False)}")

    print("\n=== ALL real_trade_orders today (any status) ===")
    r = c.execute(text("""
        SELECT setup_log_id, state->>'status' AS status, state->>'account_id' AS acct,
               state->>'fill_price' AS fill, state->>'close_reason' AS close_reason,
               created_at AT TIME ZONE 'America/New_York' AS et
        FROM real_trade_orders
        WHERE created_at >= '2026-05-21 13:00:00+00'
        ORDER BY created_at DESC
    """)).fetchall()
    print(f"Found: {len(r)}\n")
    for row in r:
        print(dict(row._mapping))
