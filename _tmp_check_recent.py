import os, psycopg2, json

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
    SELECT setup_log_id, state, created_at, updated_at
    FROM real_trade_orders
    WHERE setup_log_id >= 3035
    ORDER BY setup_log_id
""")
rows = cur.fetchall()
print(f"=== {len(rows)} real_trade_orders rows since lid 3035 ===\n")
for sid, state, created, updated in rows:
    if isinstance(state, str):
        state = json.loads(state)
    print(f"lid={sid}  created={created}  updated={updated}")
    print(f"  setup_name:    {state.get('setup_name')}")
    print(f"  direction:     {state.get('direction')}")
    print(f"  account_id:    {state.get('account_id')}")
    print(f"  atomic_bracket:{state.get('atomic_bracket')}")
    print(f"  quantity:      {state.get('quantity')}")
    print(f"  status:        {state.get('status')}")
    print(f"  fill_price:    {state.get('fill_price')}")
    print(f"  signal_es:     {state.get('signal_es_price')}")
    print(f"  current_stop:  {state.get('current_stop')}")
    print(f"  target_price:  {state.get('target_price')}")
    print(f"  entry_oid:     {state.get('entry_order_id')}")
    print(f"  stop_oid:      {state.get('stop_order_id')}")
    print(f"  target_oid:    {state.get('target_order_id')}")
    print(f"  close_fill:    {state.get('close_fill_price')}")
    print(f"  close_reason:  {state.get('close_reason')}")
    print(f"  max_favorable: {state.get('max_favorable')}")
    print()

cur.execute("""
    SELECT id, ts, setup_name, direction, grade, paradigm, greek_alignment,
           notified, real_trade_skip_reason
    FROM setup_log
    WHERE id >= 3035
    ORDER BY id
""")
rows = cur.fetchall()
print(f"=== {len(rows)} setup_log rows since 3035 ===\n")
for r in rows:
    sid, ts, name, dir_, grade, para, align, notified, skip = r
    print(f"lid={sid}  {ts.strftime('%H:%M:%S')}  {name}  {dir_}  grade={grade}  align={align}  para={para}")
    print(f"           notified={notified}  skip_reason={skip}")
    print()

cur.close(); c.close()
