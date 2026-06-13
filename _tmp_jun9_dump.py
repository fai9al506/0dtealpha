import os, json
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT sl.id,
               (sl.ts AT TIME ZONE 'America/New_York')::text as et_ts,
               sl.setup_name, sl.direction, sl.grade,
               rto.state
        FROM setup_log sl
        JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = DATE '2026-06-09'
        ORDER BY sl.ts ASC
    """)).fetchall()

print(f"Jun 9 real trades: {len(rows)}\n")
for r in rows:
    sid, ets, setup, direction, grade, st = r
    if not isinstance(st, dict):
        try: st = json.loads(st)
        except Exception: st = {}
    print(f"=== lid {sid} {ets[11:16]} {setup} {direction} {grade} acct={st.get('account_id')} ===")
    keys = ['status','close_reason','fill_price','entry_fill_price','stop_fill_price',
            'close_fill_price','target_fill_price','target_price','stop_price',
            'realized_pnl','pnl','broker_pnl','qty','filled_qty',
            'stop_fill_price_pre_fifo_reconcile','close_fill_price_pre_fifo_reconcile']
    for k in keys:
        if k in st and st[k] is not None:
            print(f"    {k}: {st[k]}")
    print()
