import os, requests, json, sys, time
sys.stdout.reconfigure(encoding='utf-8')

resp = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
})
token = resp.json()['access_token']
h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

pos = requests.get('https://sim-api.tradestation.com/v3/brokerage/accounts/SIM2609239F/positions', headers=h).json()
positions = pos.get('Positions', [])
print(f"Open positions: {len(positions)}", flush=True)

for p in positions:
    qty_raw = p['Quantity']
    qty_abs = str(abs(int(qty_raw)))
    ls = p['LongShort']
    sym = p['Symbol']
    print(f"  {ls} {qty_raw}x {sym} avg@{p['AveragePrice']} unrealizedPnL={p.get('UnrealizedProfitLoss')}", flush=True)

    close_side = 'Buy' if ls == 'Short' else 'Sell'
    print(f"  Closing: {close_side} {qty_abs} {sym}...", flush=True)
    close = requests.post('https://sim-api.tradestation.com/v3/orderexecution/orders',
        headers=h, json={
            'AccountID': 'SIM2609239F', 'Symbol': sym, 'Quantity': qty_abs,
            'OrderType': 'Market', 'TradeAction': close_side,
            'TimeInForce': {'Duration': 'DAY'}, 'Route': 'Intelligent',
        })
    print(f"  Result: {close.status_code} {close.text[:500]}", flush=True)

time.sleep(2)
pos2 = requests.get('https://sim-api.tradestation.com/v3/brokerage/accounts/SIM2609239F/positions', headers=h).json()
remaining = pos2.get('Positions', [])
print(f"\nAfter close: {len(remaining)} positions", flush=True)
if remaining:
    for p in remaining:
        print(f"  STILL OPEN: {p['LongShort']} {p['Quantity']}x {p['Symbol']}", flush=True)

bal = requests.get('https://sim-api.tradestation.com/v3/brokerage/accounts/SIM2609239F/balances', headers=h).json()
b = bal.get('Balances', [{}])
if isinstance(b, list) and b: b = b[0]
print(f"\nBalance: Equity={b.get('Equity')} Cash={b.get('CashBalance')} TodayPnL={b.get('TodaysProfitLoss')}", flush=True)
