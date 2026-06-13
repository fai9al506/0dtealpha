"""S166/S167 — investigate close_fill_price anomalies on lids 3031, 3033, 3039, 3051."""
import os, json
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"])

LIDS = [3031, 3033, 3039, 3051]

with eng.connect() as c:
    for lid in LIDS:
        print(f"\n========== lid={lid} ==========")
        # setup_log
        sl = c.execute(text("""
            SELECT id, setup_name, direction, paradigm, grade,
                   ts AT TIME ZONE 'America/New_York' AS et,
                   spot, abs_es_price,
                   outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
                   outcome_first_event, exit_price
            FROM setup_log WHERE id = :lid
        """), {"lid": lid}).fetchone()
        if not sl:
            print(f"  not found")
            continue
        d = dict(sl._mapping)
        print(f"  setup={d['setup_name']} dir={d['direction']} grade={d['grade']} para={d['paradigm']}")
        print(f"  et={d['et']} spot={d['spot']} abs_es={d['abs_es_price']}")
        print(f"  outcome: result={d['outcome_result']} pnl={d['outcome_pnl']} mfe={d['outcome_max_profit']} mae={d['outcome_max_loss']} first={d['outcome_first_event']} exit={d['exit_price']}")

        # real_trade_orders.state
        rt = c.execute(text("""
            SELECT state, created_at, updated_at FROM real_trade_orders WHERE setup_log_id = :lid
        """), {"lid": lid}).fetchone()
        if not rt:
            print(f"  no real_trade_orders row")
            continue
        st = dict(rt._mapping)["state"]
        if not isinstance(st, dict):
            st = json.loads(st)
        # Key fields
        for k in ("status", "direction", "fill_price", "max_favorable",
                  "current_stop", "be_triggered", "trail_active",
                  "stop_fill_price", "target_fill_price", "close_fill_price",
                  "close_reason", "entry_order_id", "stop_order_id",
                  "target_order_id", "close_order_id", "atomic_bracket",
                  "ts_placed"):
            v = st.get(k)
            if v is not None:
                print(f"  state.{k} = {v}")
