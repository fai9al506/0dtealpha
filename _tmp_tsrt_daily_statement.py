"""TSRT daily statement post-V16-fixes (since 2026-05-19, first session after V16.1 deploy).
Broker truth from /historicalorders (S62 rule). FIFO RT matching per account.
Daily net P&L (gross - commission), ending capital reconstructed backward from
current equity (assumes no deposits/withdrawals in window).
"""
import os, json, requests
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
MES_PT = 5.0
SINCE = "2026-05-15"  # pull a few days before May 19 so FIFO opens cleanly
ERA_START = "2026-05-19"

resp = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
})
token = resp.json()['access_token']
headers = {'Authorization': f'Bearer {token}'}
BASE = 'https://api.tradestation.com/v3'

all_orders = []
for acct_id in ['210VYX65', '210VYX91']:
    url = f'{BASE}/brokerage/accounts/{acct_id}/historicalorders?since={SINCE}&pageSize=600'
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f'[{acct_id}] HTTP {r.status_code}: {r.text[:200]}')
        continue
    orders = r.json().get('Orders', [])
    print(f'[{acct_id}] {len(orders)} historical orders since {SINCE}')
    for o in orders:
        o['_account'] = acct_id
        all_orders.append(o)

def to_et(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace('Z', '+00:00')).astimezone(ET)

fills = []
for o in all_orders:
    if o.get('Status') not in ('FLL', 'FLP'):
        continue
    comm = float(o.get('CommissionFee') or 0)
    for leg in o.get('Legs', []):
        q = int(leg.get('ExecQuantity') or 0)
        if q == 0:
            continue
        fills.append({
            'account': o['_account'],
            'order_id': o.get('OrderID'),
            'time': o.get('ClosedDateTime') or o.get('OpenedDateTime'),
            'side': leg.get('BuyOrSell'),
            'qty': q,
            'price': float(leg.get('ExecutionPrice') or 0),
            'commission': comm,
        })

fills.sort(key=lambda x: (x['account'], x['time'] or ''))
print(f'Total fills: {len(fills)}')

by_account = defaultdict(list)
for f in fills:
    by_account[f['account']].append(f)

trades = []
day_comm = defaultdict(float)  # commission charged per ET day (by order close day)
for acct, flist in by_account.items():
    position = 0
    queue = []
    for f in flist:
        sign = 1 if f['side'] == 'Buy' else -1
        d = to_et(f['time']).date().isoformat()
        day_comm[d] += f['commission']
        remaining = f['qty']
        while remaining > 0:
            if position == 0 or (position > 0) == (sign > 0):
                queue.append({'side': f['side'], 'price': f['price'], 'time': f['time']})
                position += sign
                remaining -= 1
            else:
                e = queue.pop(0)
                pts = (f['price'] - e['price']) if e['side'] == 'Buy' else (e['price'] - f['price'])
                trades.append({
                    'account': acct,
                    'dir': 'LONG' if e['side'] == 'Buy' else 'SHORT',
                    'entry_et': to_et(e['time']).strftime('%Y-%m-%d %H:%M'),
                    'exit_et': to_et(f['time']).strftime('%Y-%m-%d %H:%M'),
                    'day': to_et(f['time']).date().isoformat(),
                    'entry': e['price'], 'exit': f['price'],
                    'pts': round(pts, 2), 'usd_gross': round(pts * MES_PT, 2),
                })
                position += sign
                remaining -= 1
    if position != 0:
        print(f'[{acct}] WARNING open position {position}, unmatched {queue}')

# group by day
days = defaultdict(lambda: {'gross': 0.0, 'trades': []})
for t in trades:
    days[t['day']]['gross'] += t['usd_gross']
    days[t['day']]['trades'].append(t)

# current balances
total_eq = 0.0
for acct_id in ['210VYX65', '210VYX91']:
    r = requests.get(f'{BASE}/brokerage/accounts/{acct_id}/balances', headers=headers)
    b = r.json().get('Balances', [{}])[0]
    eq = float(b.get('Equity', 0))
    print(f'[{acct_id}] equity now ${eq:,.2f}')
    total_eq += eq
print(f'TOTAL EQUITY NOW: ${total_eq:,.2f}')

all_days = sorted(set(list(days.keys()) + list(day_comm.keys())))
era_days = [d for d in all_days if d >= ERA_START]

# net per day, then reconstruct ending capital backward from current equity
rows = []
for d in era_days:
    gross = days[d]['gross'] if d in days else 0.0
    comm = day_comm.get(d, 0.0)
    rows.append({'day': d, 'gross': gross, 'comm': comm, 'net': gross - comm,
                 'n_trades': len(days[d]['trades']) if d in days else 0})

run = total_eq
for r_ in reversed(rows):
    r_['ending'] = run
    run -= r_['net']
pre_era_start_capital = run

print(f'\n=== DAILY STATEMENT (post-V16-fixes, era start {ERA_START}) ===')
print(f'Starting capital (both accounts): ${pre_era_start_capital:,.2f}')
print(f"{'Day':<12}{'Trades':>7}{'Gross':>10}{'Comm':>8}{'Net P&L':>10}{'Ending':>12}")
for r_ in rows:
    print(f"{r_['day']:<12}{r_['n_trades']:>7}{r_['gross']:>+10.2f}{r_['comm']:>8.2f}{r_['net']:>+10.2f}{r_['ending']:>12,.2f}")
era_net = sum(r_['net'] for r_ in rows)
print(f'\nEra net: ${era_net:+,.2f}  over {len(rows)} days with activity records')

print('\n=== PER-DAY TRADE DETAIL ===')
for d in era_days:
    if d not in days:
        continue
    print(f'\n--- {d} ---')
    for t in sorted(days[d]['trades'], key=lambda x: x['entry_et']):
        print(f"  [{t['account']}] {t['dir']:<5} in {t['entry_et'][11:]} out {t['exit_et'][11:]}  "
              f"{t['entry']:.2f} -> {t['exit']:.2f}  {t['pts']:+.2f} pts  ${t['usd_gross']:+.2f}")

out = {'era_start': ERA_START, 'starting_capital': pre_era_start_capital,
       'total_equity_now': total_eq, 'rows': rows, 'trades': trades}
with open('./_tmp_tsrt/tsrt_daily_statement.json', 'w') as f:
    json.dump(out, f, indent=2, default=str)
print('\nSaved ./_tmp_tsrt/tsrt_daily_statement.json')
