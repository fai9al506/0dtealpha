"""TSRT Ledger v2 — authoritative broker PnL joined to setup_log."""
import json, psycopg2
from collections import defaultdict
from datetime import date

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
MES_PT = 5.0
COMM_PER_RT = 1.00

V12FIX = date(2026, 3, 29)
QTY_FIX = date(2026, 4, 8)
V13_DATE = date(2026, 4, 17)
TODAY = date(2026, 4, 22)

with open('./_tmp_tsrt/tsrt_realized.json') as f:
    real = json.load(f)
matched_rts = real['matched_trades']
print(f'Loaded {len(matched_rts)} broker RTs')

entry_to_rt = {rt['entry_order_id']: rt for rt in matched_rts}

conn = psycopg2.connect(DB)
cur = conn.cursor()
cur.execute("""
SELECT r.setup_log_id, r.state, s.setup_name, s.direction, s.paradigm, s.grade,
       s.outcome_pnl, s.outcome_result,
       (s.ts AT TIME ZONE 'America/New_York')::date as trade_date,
       s.ts
FROM real_trade_orders r
LEFT JOIN setup_log s ON s.id = r.setup_log_id
ORDER BY s.ts
""")
rto_rows = cur.fetchall()
cur.close(); conn.close()
print(f'Loaded {len(rto_rows)} real_trade_orders DB rows')

enriched = []
no_broker_match = []
for sid, state, setup, direction, paradigm, grade, outcome_pnl, outcome_result, trade_date, ts in rto_rows:
    state = state or {}
    entry_order_id = state.get('entry_order_id')
    account = state.get('account_id')
    broker_rt = entry_to_rt.get(entry_order_id) if entry_order_id else None
    portal_pts = float(outcome_pnl) if outcome_pnl is not None else 0.0
    rec = {
        'setup_log_id': sid, 'setup': setup, 'direction': direction,
        'paradigm': paradigm, 'grade': grade,
        'date': trade_date, 'account': account,
        'portal_pts': portal_pts, 'portal_usd': portal_pts * MES_PT,
        'outcome_result': outcome_result,
        'close_reason': state.get('close_reason'),
        'state_fill': state.get('fill_price'),
        'entry_order_id': entry_order_id,
    }
    if broker_rt:
        rec['broker_pts'] = broker_rt['pts']
        rec['broker_gross_usd'] = broker_rt['usd_gross']
        rec['broker_net_usd'] = broker_rt['usd_gross'] - COMM_PER_RT
        rec['broker_entry'] = broker_rt['entry_price']
        rec['broker_exit'] = broker_rt['exit_price']
    else:
        rec['broker_pts'] = None
        rec['broker_gross_usd'] = None
        rec['broker_net_usd'] = None
        no_broker_match.append(sid)
    enriched.append(rec)

print(f'\nDB rows matched to broker: {sum(1 for r in enriched if r["broker_pts"] is not None)}')
print(f'DB rows WITHOUT broker match: {len(no_broker_match)} -> {no_broker_match}')

matched_entry_ids = set(r['entry_order_id'] for r in enriched if r['entry_order_id'])
orphan_rts = [rt for eid, rt in entry_to_rt.items() if eid not in matched_entry_ids]
print(f'Orphan broker RTs (no DB row): {len(orphan_rts)}')
for rt in orphan_rts:
    print(f'  acct={rt["account"]} oid={rt["entry_order_id"]} {rt["entry_time"][:10]} {rt["entry_side"]} @ {rt["entry_price"]} -> {rt["exit_price"]} gross=${rt["usd_gross"]:+.2f}')

def era_of(d):
    if d is None: return 'unknown'
    if d < V12FIX: return 'pre_V12fix'
    if d < QTY_FIX: return 'V12fix_bugs'
    if d < V13_DATE: return 'V12fix_postfix'
    return 'V13'

by_era = defaultdict(list)
for r in enriched:
    by_era[era_of(r['date'])].append(r)

def summarize(lst):
    n = len(lst)
    pp = sum(r['portal_pts'] for r in lst)
    pu = sum(r['portal_usd'] for r in lst)
    bg = sum(r['broker_gross_usd'] for r in lst if r['broker_gross_usd'] is not None)
    bn = sum(r['broker_net_usd'] for r in lst if r['broker_net_usd'] is not None)
    bp = sum(r['broker_pts'] for r in lst if r['broker_pts'] is not None)
    wins = sum(1 for r in lst if r['broker_net_usd'] is not None and r['broker_net_usd'] > 0)
    losses = sum(1 for r in lst if r['broker_net_usd'] is not None and r['broker_net_usd'] < 0)
    bes = sum(1 for r in lst if r['broker_net_usd'] is not None and r['broker_net_usd'] == 0)
    cap = (bg / pu * 100) if pu != 0 else float('nan')
    wr = (wins / max(wins+losses, 1) * 100)
    return dict(n=n, portal_pts=pp, portal_usd=pu, broker_pts=bp,
                broker_gross=bg, broker_net=bn, capture=cap, wins=wins,
                losses=losses, bes=bes, wr=wr)

print('\n' + '='*120)
print('PER-ERA - broker realized (authoritative)')
print('='*120)
print(f'{"Era":<20} {"N":>4} {"Portal pts":>11} {"Portal $":>10} {"Broker pts":>11} '
      f'{"Gross $":>10} {"Net $":>10} {"Cap %":>7} {"W/L/BE":>10} {"WR":>7}')
for era in ['pre_V12fix', 'V12fix_bugs', 'V12fix_postfix', 'V13']:
    lst = by_era.get(era, [])
    if not lst:
        print(f'{era:<20} {0:>4}  (no trades)')
        continue
    a = summarize(lst)
    print(f'{era:<20} {a["n"]:>4} {a["portal_pts"]:>+11.2f} {a["portal_usd"]:>+10.1f} '
          f'{a["broker_pts"]:>+11.2f} {a["broker_gross"]:>+10.2f} {a["broker_net"]:>+10.2f} '
          f'{a["capture"]:>6.1f}%  {a["wins"]:>2}/{a["losses"]:>2}/{a["bes"]:>2}  {a["wr"]:>6.1f}%')
a = summarize(enriched)
print(f'{"OVERALL":<20} {a["n"]:>4} {a["portal_pts"]:>+11.2f} {a["portal_usd"]:>+10.1f} '
      f'{a["broker_pts"]:>+11.2f} {a["broker_gross"]:>+10.2f} {a["broker_net"]:>+10.2f} '
      f'{a["capture"]:>6.1f}%  {a["wins"]:>2}/{a["losses"]:>2}/{a["bes"]:>2}  {a["wr"]:>6.1f}%')

current = [r for r in enriched if r['date'] and r['date'] >= V12FIX]
cur_postfix = [r for r in current if r['date'] >= QTY_FIX]
print('\n' + '='*120)
print('CURRENT SYSTEM (V12-fix onward, 3/29+)')
print('='*120)
a = summarize(current)
print(f'All-in (3/29+):      n={a["n"]}  broker_net=${a["broker_net"]:+.2f}  '
      f'portal=${a["portal_usd"]:+.1f}  cap={a["capture"]:.1f}%  '
      f'WR={a["wr"]:.1f}% ({a["wins"]}/{a["losses"]}/{a["bes"]})')
a = summarize(cur_postfix)
print(f'Post-bug (4/8+):     n={a["n"]}  broker_net=${a["broker_net"]:+.2f}  '
      f'portal=${a["portal_usd"]:+.1f}  cap={a["capture"]:.1f}%  '
      f'WR={a["wr"]:.1f}% ({a["wins"]}/{a["losses"]}/{a["bes"]})')

print('\n' + '='*120)
print('PER-SETUP BREAKDOWN - V12-fix era (3/29+, all-in)')
print('='*120)
by_setup = defaultdict(list)
for r in current:
    by_setup[(r['setup'], r['direction'])].append(r)
print(f'{"Setup":<16} {"Dir":<8} {"N":>4} {"Portal pts":>11} {"Portal $":>10} {"Broker pts":>11} '
      f'{"Gross $":>10} {"Net $":>10} {"Cap %":>7} {"WR":>7}')
for key, lst in sorted(by_setup.items()):
    a = summarize(lst)
    setup, dirx = key
    print(f'{str(setup)[:16]:<16} {str(dirx)[:8]:<8} {a["n"]:>4} {a["portal_pts"]:>+11.2f} '
          f'{a["portal_usd"]:>+10.1f} {a["broker_pts"]:>+11.2f} {a["broker_gross"]:>+10.2f} '
          f'{a["broker_net"]:>+10.2f} {a["capture"]:>6.1f}% {a["wr"]:>6.1f}%')

print('\n' + '='*120)
print('PER-SETUP BREAKDOWN - V12-fix POST-BUG era (4/8+)')
print('='*120)
by_setup2 = defaultdict(list)
for r in cur_postfix:
    by_setup2[(r['setup'], r['direction'])].append(r)
print(f'{"Setup":<16} {"Dir":<8} {"N":>4} {"Portal pts":>11} {"Portal $":>10} {"Broker pts":>11} '
      f'{"Gross $":>10} {"Net $":>10} {"Cap %":>7} {"WR":>7}')
for key, lst in sorted(by_setup2.items()):
    a = summarize(lst)
    setup, dirx = key
    print(f'{str(setup)[:16]:<16} {str(dirx)[:8]:<8} {a["n"]:>4} {a["portal_pts"]:>+11.2f} '
          f'{a["portal_usd"]:>+10.1f} {a["broker_pts"]:>+11.2f} {a["broker_gross"]:>+10.2f} '
          f'{a["broker_net"]:>+10.2f} {a["capture"]:>6.1f}% {a["wr"]:>6.1f}%')

print('\n' + '='*120)
print('BALANCE RECONCILIATION')
print('='*120)
bal = real['current_balances']
total_eq = real['total_equity_now']
total_net = real['total_net_pnl_usd']
print(f'Broker equity:   210VYX65=${bal["210VYX65"]["equity"]:,.2f} + 210VYX91=${bal["210VYX91"]["equity"]:,.2f} = ${total_eq:,.2f}')
print(f'Broker realized: gross=${real["total_gross_pnl_usd"]:+.2f}  comm=${real["total_commission_usd"]:.2f}  NET=${total_net:+.2f}')
print(f'Implied funding: ${total_eq:,.2f} - (${total_net:+.2f}) = ${total_eq - total_net:,.2f}')
print()
for acct, info in real['per_account'].items():
    eq = bal[acct]['equity']
    funding = eq - info['net_pnl_usd']
    role = 'longs' if acct == '210VYX65' else 'shorts'
    print(f'  {acct} ({role}): trades={info["trades"]} gross=${info["gross_pnl_usd"]:+.2f} '
          f'comm=${info["commission_usd"]:.2f} NET=${info["net_pnl_usd"]:+.2f} '
          f'equity=${eq:,.2f} implied_funding=${funding:,.2f}')

print('\nPRIOR PASS said NET -$661. Truth: NET +$51.25. Error gap: $712.')
print('Error sources:')
print('  1. Fee assumption: prior $3/RT x 60 = $180. Actual $1/RT x 60 = $60. Error +$120.')
print('  2. Fill-based exit approximation: WIN/ghost/eod_flatten used current_stop not real fill. Error ~$592.')

divergences = []
for r in enriched:
    if r['broker_gross_usd'] is None: continue
    diff = r['broker_gross_usd'] - r['portal_usd']
    divergences.append((r['setup_log_id'], r['date'], r['setup'], r['direction'],
                        r['portal_pts'], r['broker_pts'], diff, r['close_reason']))
divergences.sort(key=lambda x: abs(x[6]), reverse=True)
print('\nTop 10 portal-vs-broker divergences:')
for d in divergences[:10]:
    print(f'  id={d[0]} {d[1]} {d[2]:<12} {d[3]:<6} portal={d[4]:+.2f}pts '
          f'broker={d[5]:+.2f}pts diff=${d[6]:+.2f} reason={d[7]}')

with open('./_tmp_tsrt/tsrt_ledger.json', 'w') as f:
    json.dump({'enriched': enriched, 'orphan_rts': orphan_rts}, f, indent=2, default=str)
print('\nSaved ./_tmp_tsrt/tsrt_ledger.json')
