"""Quick test: place 1 MES on SIM to diagnose rejection."""
import requests, json, os, time

# Get a fresh token
r = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'scope': 'openid profile MarketData ReadAccount Trade OptionSpreads offline_access',
}, timeout=15)
tok = r.json()
at = tok.get('access_token', '')
print('token_ok:', bool(at))
print('scopes:', tok.get('scope', ''))

# List all accounts
r2 = requests.get('https://api.tradestation.com/v3/brokerage/accounts',
                   headers={'Authorization': f'Bearer {at}'}, timeout=10)
accts = r2.json().get('Accounts', [])
for a in accts:
    print(f"  account: {a.get('AccountID')} type={a.get('AccountType')} status={a.get('Status')}")

# Try placing 1 MES on SIM
payload = {
    'AccountID': 'SIM2609239F',
    'Symbol': '@MES',
    'Quantity': '1',
    'OrderType': 'Market',
    'TradeAction': 'Buy',
    'TimeInForce': {'Duration': 'DAY'},
    'Route': 'Intelligent',
}
print(f"\nPlacing: {json.dumps(payload)}")
r3 = requests.post('https://sim-api.tradestation.com/v3/orderexecution/orders',
                    headers={'Authorization': f'Bearer {at}', 'Content-Type': 'application/json'},
                    json=payload, timeout=10)
print(f'place_status: {r3.status_code}')
print(f'place_resp: {r3.text[:500]}')

# Wait and check order status
if r3.status_code == 200:
    oid = r3.json().get('Orders', [{}])[0].get('OrderID', '')
    print(f'order_id: {oid}')
    time.sleep(3)
    r4 = requests.get(f'https://sim-api.tradestation.com/v3/brokerage/accounts/SIM2609239F/orders',
                      headers={'Authorization': f'Bearer {at}'}, timeout=10)
    print(f'orders_status: {r4.status_code}')
    for o in r4.json().get('Orders', []):
        if o.get('OrderID') == oid:
            print(f'ORDER DETAIL:\n{json.dumps(o, indent=2)[:1000]}')
            break
    else:
        print(f'Order {oid} not found in {len(r4.json().get("Orders", []))} orders')
    # Cancel
    requests.delete(f'https://sim-api.tradestation.com/v3/orderexecution/orders/{oid}',
                    headers={'Authorization': f'Bearer {at}', 'Content-Type': 'application/json'}, timeout=5)
    print('cancelled')
