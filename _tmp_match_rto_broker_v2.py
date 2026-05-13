"""Match real_trade_orders to broker fills using state.entry_order_id and stop_order_id (not FIFO).
This is the CORRECT way — uses OrderID stored in real_trade_orders.state."""
import json
import psycopg2
from datetime import datetime, timezone
from collections import defaultdict

# Load all broker orders + extract fills with OrderID
with open('_tmp_tsrt/tsrt_historical_orders.json') as f:
    orders = json.load(f)

# Build by OrderID lookup (with execution price + fill time)
fill_by_oid = {}
for o in orders:
    legs = o.get('Legs', [])
    for leg in legs:
        q = int(leg.get('ExecQuantity') or 0)
        if q == 0:
            continue
        fill_by_oid[str(o.get('OrderID'))] = {
            'account': o.get('_account'),
            'fill_time': o.get('ClosedDateTime') or o.get('OpenedDateTime'),
            'side': leg.get('BuyOrSell'),
            'price': float(leg.get('ExecutionPrice') or 0),
            'order_type': o.get('OrderType'),
            'status': o.get('Status'),
            'commission': float(o.get('CommissionFee') or 0),
        }

print(f'Total filled orders: {len(fill_by_oid)}')

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()
cur.execute("SELECT setup_log_id, state, created_at FROM real_trade_orders ORDER BY created_at")
rtos = cur.fetchall()

ids = [r[0] for r in rtos]
cur.execute("SELECT id, outcome_pnl, outcome_result, setup_name, grade, direction, outcome_max_profit, paradigm FROM setup_log WHERE id = ANY(%s)", (ids,))
portal = {r[0]: r for r in cur.fetchall()}

MES_PT = 5.0
matched = []
problems = []

for sid, st, c in rtos:
    direction = st.get('direction')
    is_long = direction in ('long', 'bullish')

    # Get OrderIDs from state
    entry_oid = st.get('entry_order_id')
    stop_oid = st.get('stop_order_id')
    target_oid = st.get('target_order_id')

    e_fill = fill_by_oid.get(str(entry_oid)) if entry_oid else None
    s_fill = fill_by_oid.get(str(stop_oid)) if stop_oid else None
    t_fill = fill_by_oid.get(str(target_oid)) if target_oid else None

    # Determine actual exit (stop or target)
    exit_fill = None
    exit_via = None
    if s_fill and s_fill['status'] in ('FLL', 'FLP'):
        exit_fill = s_fill
        exit_via = 'stop'
    elif t_fill and t_fill['status'] in ('FLL', 'FLP'):
        exit_fill = t_fill
        exit_via = 'target'

    if not e_fill:
        problems.append((sid, 'no_entry_fill', st.get('close_reason')))
        # Use state values as fallback
        broker_entry = st.get('fill_price') or 0
        broker_exit = st.get('stop_fill_price') or st.get('close_fill_price') or 0
        broker_pts = (broker_exit - broker_entry) if is_long else (broker_entry - broker_exit) if broker_exit else 0
        broker_exit_time = str(st.get('updated_at') or '')
    else:
        broker_entry = e_fill['price']
        if exit_fill:
            broker_exit = exit_fill['price']
            broker_pts = (broker_exit - broker_entry) if is_long else (broker_entry - broker_exit)
            broker_exit_time = exit_fill['fill_time']
        else:
            # No exit fill via known OIDs — check close_fill_price in state
            close_fill = st.get('close_fill_price')
            if close_fill:
                broker_exit = close_fill
                broker_pts = (broker_exit - broker_entry) if is_long else (broker_entry - broker_exit)
                broker_exit_time = 'state.close_fill_price'
                exit_via = 'close_fill_state'
            else:
                broker_exit = None
                broker_pts = 0
                broker_exit_time = 'unknown'
                exit_via = 'unknown'
                problems.append((sid, 'no_exit_fill', st.get('close_reason')))

    p = portal.get(sid)
    matched.append({
        'lid': sid,
        'created_at': str(c),
        'setup_name': st.get('setup_name'),
        'direction': direction,
        'account': st.get('account_id'),
        'fill_db': st.get('fill_price'),
        'stop_fill_db': st.get('stop_fill_price'),
        'close_fill_db': st.get('close_fill_price'),
        'close_reason': st.get('close_reason'),
        'broker_entry': broker_entry,
        'broker_exit': broker_exit,
        'broker_pts': broker_pts,
        'broker_usd': broker_pts * MES_PT if broker_pts else 0,
        'broker_exit_time': broker_exit_time,
        'exit_via': exit_via,
        'portal_pnl': p[1] if p else None,
        'portal_result': p[2] if p else None,
        'portal_grade': p[4] if p else None,
        'portal_paradigm': p[7] if p else None,
        'portal_mfe': p[6] if p else None,
        'entry_order_id': entry_oid,
        'stop_order_id': stop_oid,
        'target_order_id': target_oid,
    })

print(f'Total RTOs: {len(rtos)}  Matched: {len(matched) - sum(1 for p in problems if p[1] == "no_entry_fill")}  Problems: {len(problems)}')
print()
print('=== PROBLEM RTOs ===')
for sid, reason, cr in problems:
    print(f'  lid={sid} {reason} close_reason={cr}')

# Save
with open('_tmp_rto_broker_matched_v2.json', 'w') as f:
    json.dump(matched, f, indent=2, default=str)

# Compute total broker P&L from this OID-based match
total_pts = sum(m['broker_pts'] for m in matched if m['broker_pts'])
total_portal = sum((m['portal_pnl'] or 0) for m in matched)
print(f'\nTotal via OID match: {total_pts:+.2f} pt = ${5*total_pts:+,.2f}')
print(f'Total portal pnl:     {total_portal:+.2f} pt = ${5*total_portal:+,.2f}')
print(f'Gap (portal - real):  {total_portal-total_pts:+.2f} pt = ${5*(total_portal-total_pts):+,.2f}')
print(f'Saved _tmp_rto_broker_matched_v2.json')
