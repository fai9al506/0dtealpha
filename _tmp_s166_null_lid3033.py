"""Null out the corrupted close_fill_price on lid=3033 (S166 bug).

Sets state.close_fill_price=NULL and adds a marker so analytics queries don't
mis-attribute P&L from the impossible +$200 fill. The trade IS closed (broker
confirms account flat), we just don't have a trustworthy close price stored.

User can later check TradeStation statement to find the real close fill price.
"""
import os, json
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"])
LID = 3033

with eng.begin() as c:
    r = c.execute(text("SELECT state FROM real_trade_orders WHERE setup_log_id = :lid"),
                  {"lid": LID}).fetchone()
    if not r:
        print(f"lid={LID} not found")
        raise SystemExit
    st = r[0] if isinstance(r[0], dict) else json.loads(r[0])
    print(f"BEFORE: close_fill_price = {st.get('close_fill_price')}")

    # Nullify the corrupted value, add marker
    st["close_fill_price_PRE_S166_FIX"] = st.get("close_fill_price")  # archive
    st["close_fill_price"] = None
    st["close_fill_price_corrupted"] = True
    st["s166_marker"] = "Original close_fill_price 7418.75 was impossible (+40pt above entry on a LOSS). Nulled 2026-05-21. Check TS statement for real close fill on close_oid=1266617981."

    c.execute(text("UPDATE real_trade_orders SET state = :st WHERE setup_log_id = :lid"),
              {"st": json.dumps(st), "lid": LID})
    print(f"AFTER: close_fill_price = None, archived as close_fill_price_PRE_S166_FIX")
    print(f"  s166_marker = {st['s166_marker']}")
