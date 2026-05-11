import os, requests, json
r = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'scope': 'openid profile MarketData ReadAccount Trade OptionSpreads offline_access',
}, timeout=15)
tok = r.json()['access_token']
H = {'Authorization': f'Bearer {tok}'}

bal = requests.get('https://sim-api.tradestation.com/v3/brokerage/accounts/SIM2609239F/balances', headers=H, timeout=10).json()
b = bal.get('Balances', [{}])[0]
detail = b.get('BalanceDetail', {}) or {}
print('=== SIM2609239F BALANCE (NORMAL ordergroup, 1 MES open) ===')
print(f"Cash:           ${float(b.get('CashBalance',0)):,.2f}")
print(f"BuyingPower:    ${float(b.get('BuyingPower',0)):,.2f}")
print(f"Equity:         ${float(b.get('Equity',0)):,.2f}")
print(f"InitialMargin:    ${float(detail.get('InitialMargin',0)):,.2f}")
print(f"DayTradeMargin:   ${float(detail.get('DayTradeMargin',0)):,.2f}")
print(f"MaintenanceMargin:${float(detail.get('MaintenanceMargin',0)):,.2f}")
print(f"RequiredMargin:   ${float(detail.get('RequiredMargin',0)):,.2f}")
print(f"DayTradeExcess:   ${float(detail.get('DayTradeExcess',0)):,.2f}")
print(f"OvernightBuyingPower: ${float(detail.get('OvernightBuyingPower',0)):,.2f}")

pos = requests.get('https://sim-api.tradestation.com/v3/brokerage/accounts/SIM2609239F/positions', headers=H, timeout=10).json()
print()
print('=== Positions ===')
for p in pos.get('Positions', []):
    print(f"{p.get('Symbol')}: qty={p.get('Quantity')} avg={p.get('AveragePrice')} long_short={p.get('LongShort')}")

orders = requests.get('https://sim-api.tradestation.com/v3/brokerage/accounts/SIM2609239F/orders', headers=H, timeout=10).json()
print()
print('=== Open Orders ===')
for o in orders.get('Orders', []):
    if o.get('Status') in ('OPN','ACK','DON'):
        print(f"id={o.get('OrderID')} {o.get('Type')} {o.get('Status')} qty={o.get('Quantity')} limit={o.get('LimitPrice','')} stop={o.get('StopPrice','')}")
