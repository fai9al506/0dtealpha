"""Close short 5 MESH26 position."""
import requests, os, time, sys

r = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'scope': 'openid profile MarketData ReadAccount Trade OptionSpreads offline_access',
}, timeout=15)
at = r.json().get('access_token', '')
print(f'token: {bool(at)}', flush=True)
headers = {'Authorization': f'Bearer {at}', 'Content-Type': 'application/json'}
SIM = 'https://sim-api.tradestation.com/v3'
ACCT = 'SIM2609239F'

# Close short 5 MESH26 (Buy to cover)
payload = {
    'AccountID': ACCT, 'Symbol': 'MESH26', 'Quantity': '5',
    'OrderType': 'Market', 'TradeAction': 'Buy',
    'TimeInForce': {'Duration': 'DAY'}, 'Route': 'Intelligent',
}
print(f'Placing: Buy 5 MESH26 Market', flush=True)
cr = requests.post(f'{SIM}/orderexecution/orders', headers=headers, json=payload, timeout=10)
print(f'close status: {cr.status_code}', flush=True)
print(f'close resp: {cr.text[:400]}', flush=True)

time.sleep(3)

# Check positions
pr = requests.get(f'{SIM}/brokerage/accounts/{ACCT}/positions', headers=headers, timeout=10)
print(f'positions status: {pr.status_code}', flush=True)
pos = pr.json().get('Positions', [])
print(f'positions count: {len(pos)}', flush=True)
for p in pos:
    print(f'  {p.get("Symbol")} qty={p.get("Quantity")} {p.get("LongShort")}', flush=True)

# Check balance
br = requests.get(f'{SIM}/brokerage/accounts/{ACCT}/balances', headers=headers, timeout=10)
bal = br.json().get('Balances', [{}])[0]
print(f'Buying Power: {bal.get("BuyingPower")}', flush=True)
print(f'Init Margin: {bal.get("BalanceDetail", {}).get("InitialMargin")}', flush=True)
