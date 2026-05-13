"""Match all real_trade_orders to broker matched_trades for the FULL TSRT era."""
import json
import psycopg2
from datetime import datetime, timezone

with open('_tmp_tsrt/tsrt_realized.json') as f:
    d = json.load(f)
broker = d['matched_trades']

# Parse broker entry times to UTC
for t in broker:
    t['_entry_dt'] = datetime.fromisoformat(t['entry_time'].replace('Z', '+00:00'))

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

cur.execute("SELECT setup_log_id, state, created_at FROM real_trade_orders ORDER BY created_at")
rtos = cur.fetchall()
ids = [r[0] for r in rtos]
cur.execute(
    "SELECT id, outcome_pnl, outcome_result, setup_name, grade, direction, outcome_max_profit FROM setup_log WHERE id = ANY(%s)",
    (ids,),
)
portal = {r[0]: r for r in cur.fetchall()}

used_broker = set()
matched = []
unmatched = []

for sid, st, c in rtos:
    direction = st.get('direction')
    expected_entry_side = 'Sell' if direction in ('short', 'bearish') else 'Buy'
    acct = st.get('account_id')
    c_utc = c if c.tzinfo else c.replace(tzinfo=timezone.utc)

    # Find broker trade with matching account, side, and entry_time within 90s of created_at
    best = None
    for i, bt in enumerate(broker):
        if i in used_broker:
            continue
        if bt['account'] != acct:
            continue
        if bt['entry_side'] != expected_entry_side:
            continue
        diff_s = abs((bt['_entry_dt'] - c_utc).total_seconds())
        if diff_s > 120:
            continue
        if best is None or diff_s < best[1]:
            best = (i, diff_s, bt)

    if best is None:
        unmatched.append((sid, st, c))
        continue

    used_broker.add(best[0])
    matched.append((sid, st, c, best[2]))

print(f'RTOs: {len(rtos)}  matched_to_broker: {len(matched)}  unmatched: {len(unmatched)}')
print(f'Broker trades: {len(broker)}  matched: {len(used_broker)}  orphan: {len(broker)-len(used_broker)}')
print()

print('=== UNMATCHED RTOs ===')
for sid, st, c in unmatched:
    print(f'  lid={sid:4d} {c} dir={st.get("direction")} acct={st.get("account_id")} reason={st.get("close_reason")} fill={st.get("fill_price")} stop_fill={st.get("stop_fill_price")}')

print()
print('=== ORPHAN BROKER TRADES (no RTO match) ===')
for i, bt in enumerate(broker):
    if i not in used_broker:
        print(f'  {bt["entry_time"][:19]} {bt["entry_side"]} {bt["entry_price"]} -> {bt["exit_time"][:19]} @ {bt["exit_price"]} pts={bt["pts"]:+.2f} acct={bt["account"]}')

# Save the matched list
out = []
for sid, st, c, bt in matched:
    p = portal.get(sid)
    out.append({
        'lid': sid,
        'created_at': str(c),
        'setup_name': st.get('setup_name'),
        'direction': st.get('direction'),
        'account': st.get('account_id'),
        'fill_db': st.get('fill_price'),
        'stop_fill_db': st.get('stop_fill_price'),
        'close_reason': st.get('close_reason'),
        'broker_entry': bt['entry_price'],
        'broker_exit': bt['exit_price'],
        'broker_pts': bt['pts'],
        'broker_usd': bt['usd_gross'],
        'broker_exit_time': bt['exit_time'],
        'portal_pnl': p[1] if p else None,
        'portal_result': p[2] if p else None,
        'portal_grade': p[4] if p else None,
        'portal_mfe': p[6] if p else None,
    })

with open('_tmp_rto_broker_matched.json', 'w') as f:
    json.dump(out, f, indent=2, default=str)
print(f'\nSaved _tmp_rto_broker_matched.json ({len(out)} pairs)')
