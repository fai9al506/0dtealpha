import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
with e.connect() as c:
    # Check if setup 290,291,292 have auto_trade_orders entries
    rows = c.execute(text("""
        SELECT setup_log_id, state::text
        FROM auto_trade_orders
        WHERE setup_log_id IN (290, 291, 292)
    """)).mappings().all()
    print("=== AUTO_TRADE_ORDERS for 290,291,292 ===")
    for r in rows:
        print(f'id={r["setup_log_id"]}|{r["state"][:400]}')

    if not rows:
        print("NONE FOUND - no auto_trade_orders for these setup_log_ids")

    # Check all auto_trade_orders with status != closed
    rows2 = c.execute(text("""
        SELECT setup_log_id, state::text
        FROM auto_trade_orders
        WHERE state->>'status' != 'closed'
    """)).mappings().all()
    print("\n=== ALL OPEN AUTO_TRADE_ORDERS ===")
    for r in rows2:
        print(f'id={r["setup_log_id"]}|{r["state"][:400]}')
    if not rows2:
        print("NONE - all orders are closed")

    # Check #289 details (DD Short that was placed at 15:26)
    rows3 = c.execute(text("""
        SELECT setup_log_id, state::text
        FROM auto_trade_orders
        WHERE setup_log_id = 289
    """)).mappings().all()
    print("\n=== AUTO_TRADE_ORDER #289 (DD Short at 15:26) ===")
    for r in rows3:
        print(r["state"][:600])
