"""Match real_trade_orders to broker fills:
1. Entry: state.entry_order_id (always reliable)
2. Exit: state.stop_order_id OR target_order_id (if filled). Otherwise find next opposite-side fill on same account after entry time.
"""
import json
import psycopg2
from datetime import datetime, timezone
from collections import defaultdict

# Load broker orders
with open('_tmp_tsrt/tsrt_historical_orders.json') as f:
    orders = json.load(f)

# Build OID lookup of filled orders
fill_by_oid = {}
fills_chrono = []  # sorted list (account, fill_time, side, price, oid)
for o in orders:
    legs = o.get('Legs', [])
    for leg in legs:
        q = int(leg.get('ExecQuantity') or 0)
        if q == 0:
            continue
        ft = o.get('ClosedDateTime') or o.get('OpenedDateTime')
        rec = {
            'oid': str(o.get('OrderID')),
            'account': o.get('_account'),
            'fill_time': ft,
            'side': leg.get('BuyOrSell'),
            'price': float(leg.get('ExecutionPrice') or 0),
            'order_type': o.get('OrderType'),
            'status': o.get('Status'),
            'commission': float(o.get('CommissionFee') or 0),
        }
        fill_by_oid[rec['oid']] = rec
        fills_chrono.append(rec)
fills_chrono.sort(key=lambda r: (r['account'], r['fill_time'] or ''))

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()
cur.execute("SELECT setup_log_id, state, created_at FROM real_trade_orders ORDER BY created_at")
rtos = cur.fetchall()
ids = [r[0] for r in rtos]
cur.execute("SELECT id, outcome_pnl, outcome_result, setup_name, grade, direction, outcome_max_profit, paradigm FROM setup_log WHERE id = ANY(%s)", (ids,))
portal = {r[0]: r for r in cur.fetchall()}

# Pre-build account-keyed entry OIDs for skipping already-paired entries when looking for exits
used_oids = set()

MES_PT = 5.0
matched = []
problems = []

# To avoid double-counting fills used as exits for another lid: track used closing oids
already_paired_exits = set()

for sid, st, c in rtos:
    direction = st.get('direction')
    is_long = direction in ('long', 'bullish')
    entry_side = 'Buy' if is_long else 'Sell'
    exit_side = 'Sell' if is_long else 'Buy'
    acct = st.get('account_id')

    entry_oid = str(st.get('entry_order_id') or '')
    stop_oid = str(st.get('stop_order_id') or '')
    target_oid = str(st.get('target_order_id') or '')

    e_fill = fill_by_oid.get(entry_oid)
    s_fill = fill_by_oid.get(stop_oid) if stop_oid else None
    t_fill = fill_by_oid.get(target_oid) if target_oid else None

    exit_fill = None
    exit_via = None
    if s_fill and s_fill['status'] in ('FLL', 'FLP') and s_fill['oid'] not in already_paired_exits:
        exit_fill = s_fill
        exit_via = 'stop_oid'
    elif t_fill and t_fill['status'] in ('FLL', 'FLP') and t_fill['oid'] not in already_paired_exits:
        exit_fill = t_fill
        exit_via = 'target_oid'

    # Fallback: find next opposite-side fill on same account AFTER entry time
    if not exit_fill and e_fill:
        entry_ft = e_fill['fill_time'] or ''
        # Look for next exit_side fill after entry, same account, not already paired
        for f in fills_chrono:
            if f['account'] != acct:
                continue
            if (f['fill_time'] or '') <= entry_ft:
                continue
            if f['side'] != exit_side:
                continue
            if f['oid'] in already_paired_exits:
                continue
            # Skip the entry_oid itself
            if f['oid'] == entry_oid:
                continue
            exit_fill = f
            exit_via = 'next_opposite'
            break

    if exit_fill:
        already_paired_exits.add(exit_fill['oid'])

    if not e_fill:
        problems.append((sid, 'no_entry_fill', st.get('close_reason')))
        broker_entry = st.get('fill_price') or 0
        broker_exit = st.get('stop_fill_price') or st.get('close_fill_price') or 0
        broker_pts = (broker_exit - broker_entry) if is_long else (broker_entry - broker_exit) if broker_exit else 0
        broker_exit_time = 'no_entry_fill'
    elif not exit_fill:
        problems.append((sid, 'no_exit_match', st.get('close_reason')))
        broker_entry = e_fill['price']
        # Use state close fill as last resort
        cf = st.get('close_fill_price') or st.get('stop_fill_price')
        broker_exit = cf or e_fill['price']  # fallback flat
        broker_pts = (broker_exit - broker_entry) if is_long else (broker_entry - broker_exit)
        broker_exit_time = 'state_fallback'
        exit_via = 'state_fallback'
    else:
        broker_entry = e_fill['price']
        broker_exit = exit_fill['price']
        broker_pts = (broker_exit - broker_entry) if is_long else (broker_entry - broker_exit)
        broker_exit_time = exit_fill['fill_time']

    p = portal.get(sid)
    matched.append({
        'lid': sid,
        'created_at': str(c),
        'setup_name': st.get('setup_name'),
        'direction': direction,
        'account': acct,
        'fill_db': st.get('fill_price'),
        'stop_fill_db': st.get('stop_fill_price'),
        'close_fill_db': st.get('close_fill_price'),
        'close_reason': st.get('close_reason'),
        'broker_entry': broker_entry,
        'broker_exit': broker_exit,
        'broker_pts': broker_pts,
        'broker_usd': broker_pts * MES_PT,
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

print(f'Total RTOs: {len(rtos)}  Problems: {len(problems)}')
for sid, reason, cr in problems:
    print(f'  lid={sid} {reason} close_reason={cr}')

with open('_tmp_rto_broker_matched_v3.json', 'w') as f:
    json.dump(matched, f, indent=2, default=str)

total_pts = sum(m['broker_pts'] for m in matched if m['broker_pts'])
total_portal = sum((m['portal_pnl'] or 0) for m in matched)
print()
print(f'Total broker pts:  {total_pts:+.2f} = ${5*total_pts:+,.2f}')
print(f'Total portal pts:  {total_portal:+.2f} = ${5*total_portal:+,.2f}')
print(f'Gap (portal-real): {total_portal-total_pts:+.2f} = ${5*(total_portal-total_pts):+,.2f}')

# Verify against tsrt_realized.json totals
with open('_tmp_tsrt/tsrt_realized.json') as f:
    tsrt = json.load(f)
print()
print(f'TSRT broker truth (FIFO): {tsrt["total_gross_pnl_usd"]:+,.2f} ({len(tsrt["matched_trades"])} matched)')

# Verify lid=2707
for m in matched:
    if m['lid'] == 2707:
        print()
        print('=== lid=2707 verified ===')
        print(json.dumps(m, indent=2, default=str))
