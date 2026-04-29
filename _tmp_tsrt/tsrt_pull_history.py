"""Pull full TSRT fill history from /historicalorders for both accounts.
Computes realized P&L from matched fills (entry + exit).
Each account is single-contract; commissions are per order (CommissionFee field)."""
import os, json, requests
from collections import defaultdict

resp = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
})
token = resp.json().get('access_token')
headers = {'Authorization': f'Bearer {token}'}
BASE = 'https://api.tradestation.com/v3'

MES_PT = 5.0

all_orders = []
for acct_id in ['210VYX65', '210VYX91']:
    since = '2026-03-01'
    url = f'{BASE}/brokerage/accounts/{acct_id}/historicalorders?since={since}&pageSize=600'
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f'[{acct_id}] historicalorders HTTP {r.status_code}: {r.text[:300]}')
        continue
    orders = r.json().get('Orders', [])
    print(f'[{acct_id}] retrieved {len(orders)} historical orders since {since}')
    for o in orders:
        o['_account'] = acct_id
        all_orders.append(o)

with open('./_tmp_tsrt/tsrt_historical_orders.json', 'w') as f:
    json.dump(all_orders, f, indent=2, default=str)

# Extract fills (ExecQuantity > 0)
fills = []
for o in all_orders:
    if o.get('Status') not in ('FLL', 'FLP'):
        continue
    legs = o.get('Legs', [])
    order_commission = float(o.get('CommissionFee') or 0)
    for leg in legs:
        q = int(leg.get('ExecQuantity') or 0)
        if q == 0:
            continue
        fills.append({
            'account': o['_account'],
            'order_id': o.get('OrderID'),
            'time': o.get('ClosedDateTime') or o.get('OpenedDateTime'),
            'symbol': leg.get('Symbol'),
            'side': leg.get('BuyOrSell'),  # 'Buy' or 'Sell'
            'qty': q,
            'price': float(leg.get('ExecutionPrice') or 0),
            'commission': order_commission,
        })

fills.sort(key=lambda x: (x['account'], x['time'] or ''))
print(f'\nTotal fills: {len(fills)}')

# Per-account FIFO matching
by_account = defaultdict(list)
for f in fills:
    by_account[f['account']].append(f)

total_gross = 0.0
total_comm = 0.0
per_account = {}
trades_matched = []

for acct, flist in by_account.items():
    position = 0  # +1 long, -1 short
    entry_queue = []
    gross = 0.0
    comm = 0.0
    for f in flist:
        sign = 1 if f['side'] == 'Buy' else -1
        qty_signed = f['qty'] * sign
        comm += f['commission']
        # For each unit
        remaining = f['qty']
        while remaining > 0:
            if position == 0:
                # open new
                entry_queue.append({'side': f['side'], 'price': f['price'], 'time': f['time'], 'order_id': f['order_id']})
                position += sign
                remaining -= 1
            elif (position > 0 and sign > 0) or (position < 0 and sign < 0):
                # adding same side
                entry_queue.append({'side': f['side'], 'price': f['price'], 'time': f['time'], 'order_id': f['order_id']})
                position += sign
                remaining -= 1
            else:
                # closing against entry queue
                entry = entry_queue.pop(0)
                pts = (f['price'] - entry['price']) if entry['side'] == 'Buy' else (entry['price'] - f['price'])
                usd = pts * MES_PT
                gross += usd
                trades_matched.append({
                    'account': acct,
                    'entry_time': entry['time'], 'entry_side': entry['side'], 'entry_price': entry['price'],
                    'exit_time': f['time'], 'exit_side': f['side'], 'exit_price': f['price'],
                    'pts': pts, 'usd_gross': usd,
                    'entry_order_id': entry['order_id'], 'exit_order_id': f['order_id'],
                })
                position += sign
                remaining -= 1
    per_account[acct] = {'gross_pnl_usd': gross, 'commission_usd': comm, 'net_pnl_usd': gross - comm,
                        'trades': len([t for t in trades_matched if t['account'] == acct]),
                        'orders_filled': len(flist), 'open_position': position, 'open_lots': len(entry_queue)}
    total_gross += gross
    total_comm += comm
    print(f'\n[{acct}]  fills={len(flist)}  matched_RTs={per_account[acct]["trades"]}  gross=${gross:+.2f}  '
          f'comm=${comm:.2f}  NET=${gross-comm:+.2f}  open_pos={position}')
    if position != 0:
        print(f'   UNMATCHED LOTS: {entry_queue}')

print(f'\n=== TOTAL ===')
print(f'Gross realized:   ${total_gross:+.2f}')
print(f'Commissions:      ${total_comm:.2f}')
print(f'NET realized:     ${total_gross - total_comm:+.2f}')
print(f'Matched RTs:      {len(trades_matched)}')

# Current balances
bal_info = {}
for acct_id in ['210VYX65', '210VYX91']:
    r = requests.get(f'{BASE}/brokerage/accounts/{acct_id}/balances', headers=headers)
    if r.status_code == 200:
        b = r.json().get('Balances', [{}])
        b = b[0] if isinstance(b, list) and b else b
        bal_info[acct_id] = {
            'cash': float(b.get('CashBalance', 0)),
            'equity': float(b.get('Equity', 0)),
            'buying_power': float(b.get('BuyingPower', 0)),
            'realized_today': float(b.get('BalanceDetail', {}).get('RealizedProfitLoss', 0)),
            'unrealized': float(b.get('BalanceDetail', {}).get('UnrealizedProfitLoss', 0)),
        }
print(f'\n=== CURRENT BALANCES ===')
total_eq = 0.0
for acct, info in bal_info.items():
    print(f'[{acct}] cash=${info["cash"]:,.2f}  equity=${info["equity"]:,.2f}  realized_today=${info["realized_today"]:+.2f}  unrealized=${info["unrealized"]:+.2f}')
    total_eq += info['equity']
print(f'TOTAL EQUITY: ${total_eq:,.2f}')

out = {
    'total_gross_pnl_usd': total_gross,
    'total_commission_usd': total_comm,
    'total_net_pnl_usd': total_gross - total_comm,
    'per_account': per_account,
    'current_balances': bal_info,
    'total_equity_now': total_eq,
    'matched_trades': trades_matched,
    'fills_count': len(fills),
}
with open('./_tmp_tsrt/tsrt_realized.json', 'w') as f:
    json.dump(out, f, indent=2, default=str)
print('\nSaved ./_tmp_tsrt/tsrt_realized.json')
