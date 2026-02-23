"""Clean up orphaned orders and positions on SIM account."""
import requests, json, os

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
print(f'token_ok: {bool(at)}')
headers = {'Authorization': f'Bearer {at}', 'Content-Type': 'application/json'}

SIM = 'https://sim-api.tradestation.com/v3'
ACCT = 'SIM2609239F'

# 1. Check balance
print('\n=== BALANCE ===')
r = requests.get(f'{SIM}/brokerage/accounts/{ACCT}/balances', headers=headers, timeout=10)
if r.status_code == 200:
    bal = r.json().get('Balances', [{}])[0]
    print(f"  Cash: {bal.get('CashBalance')}")
    print(f"  Buying Power: {bal.get('BuyingPower')}")
    print(f"  Init Margin: {bal.get('BalanceDetail', {}).get('InitialMargin')}")
    print(f"  DayTrade Margin: {bal.get('BalanceDetail', {}).get('DayTradeMargin')}")
else:
    print(f"  balance error: {r.status_code} {r.text[:200]}")

# 2. Check positions
print('\n=== POSITIONS ===')
r = requests.get(f'{SIM}/brokerage/accounts/{ACCT}/positions', headers=headers, timeout=10)
if r.status_code == 200:
    positions = r.json().get('Positions', [])
    if not positions:
        print('  No open positions')
    for p in positions:
        print(f"  {p.get('Symbol')} qty={p.get('Quantity')} side={p.get('LongShort')} "
              f"avg={p.get('AveragePrice')} pnl={p.get('UnrealizedProfitLoss')}")
else:
    print(f"  positions error: {r.status_code} {r.text[:200]}")

# 3. Check open orders
print('\n=== OPEN ORDERS ===')
r = requests.get(f'{SIM}/brokerage/accounts/{ACCT}/orders', headers=headers, timeout=10)
open_orders = []
if r.status_code == 200:
    for o in r.json().get('Orders', []):
        st = o.get('Status', '')
        if st in ('OPN', 'ACK', 'DON', 'QUE'):  # Open/active statuses
            open_orders.append(o)
            legs = o.get('Legs', [{}])
            sym = legs[0].get('Symbol', '?') if legs else '?'
            side = legs[0].get('BuyOrSell', '?') if legs else '?'
            qty = legs[0].get('QuantityOrdered', '?') if legs else '?'
            print(f"  OrderID={o.get('OrderID')} Status={st} Type={o.get('OrderType')} "
                  f"{side} {qty} {sym} Stop={o.get('StopPrice', '-')} Limit={o.get('LimitPrice', '-')}")
    if not open_orders:
        print('  No open orders')
else:
    print(f"  orders error: {r.status_code} {r.text[:200]}")

# 4. Cancel all open orders
if open_orders:
    print(f'\n=== CANCELLING {len(open_orders)} OPEN ORDERS ===')
    for o in open_orders:
        oid = o.get('OrderID')
        cr = requests.delete(f'{SIM}/orderexecution/orders/{oid}', headers=headers, timeout=5)
        print(f"  cancel {oid}: {cr.status_code}")

# 5. Close any positions
if r.status_code == 200:
    r2 = requests.get(f'{SIM}/brokerage/accounts/{ACCT}/positions', headers=headers, timeout=10)
    if r2.status_code == 200:
        for p in r2.json().get('Positions', []):
            qty = p.get('Quantity', '0')
            sym = p.get('Symbol')
            ls = p.get('LongShort', '')
            close_side = 'Sell' if ls == 'Long' else 'Buy'
            if int(qty) > 0:
                print(f'\n  Closing {sym} {qty} {ls}...')
                cr = requests.post(f'{SIM}/orderexecution/orders', headers=headers, json={
                    'AccountID': ACCT, 'Symbol': sym, 'Quantity': str(qty),
                    'OrderType': 'Market', 'TradeAction': close_side,
                    'TimeInForce': {'Duration': 'DAY'}, 'Route': 'Intelligent',
                }, timeout=10)
                print(f"  close: {cr.status_code} {cr.text[:200]}")

# 6. Re-check balance after cleanup
import time; time.sleep(2)
print('\n=== BALANCE AFTER CLEANUP ===')
r = requests.get(f'{SIM}/brokerage/accounts/{ACCT}/balances', headers=headers, timeout=10)
if r.status_code == 200:
    bal = r.json().get('Balances', [{}])[0]
    print(f"  Cash: {bal.get('CashBalance')}")
    print(f"  Buying Power: {bal.get('BuyingPower')}")
    print(f"  Init Margin: {bal.get('BalanceDetail', {}).get('InitialMargin')}")
