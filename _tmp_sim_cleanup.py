import os, requests
r = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'scope': 'openid profile MarketData ReadAccount Trade OptionSpreads offline_access',
}, timeout=15)
tok = r.json()['access_token']
H = {'Authorization': f'Bearer {tok}'}
o = requests.get('https://sim-api.tradestation.com/v3/brokerage/accounts/SIM2609239F/orders', headers=H, timeout=10).json()
for ord_ in o.get('Orders', []):
    if ord_.get('Status') in ('OPN', 'ACK', 'DON'):
        oid = ord_.get('OrderID')
        rr = requests.delete(f'https://sim-api.tradestation.com/v3/orderexecution/orders/{oid}', headers=H, timeout=10)
        print(f'cancel order {oid}: {rr.status_code}')
p = requests.get('https://sim-api.tradestation.com/v3/brokerage/accounts/SIM2609239F/positions', headers=H, timeout=10).json()
for pos in p.get('Positions', []):
    qty = int(pos.get('Quantity', 0))
    if qty != 0:
        side = 'Sell' if qty > 0 else 'Buy'
        sym = pos.get('Symbol')
        cp = {'AccountID': 'SIM2609239F', 'Symbol': sym, 'Quantity': str(abs(qty)),
              'OrderType': 'Market', 'TradeAction': side, 'TimeInForce': {'Duration': 'DAY'}, 'Route': 'Intelligent'}
        rr = requests.post('https://sim-api.tradestation.com/v3/orderexecution/orders',
                           headers={**H, 'Content-Type': 'application/json'}, json=cp, timeout=15)
        print(f'flatten {sym} {side} {abs(qty)}: {rr.status_code} {rr.text[:200]}')
print('cleanup done')
