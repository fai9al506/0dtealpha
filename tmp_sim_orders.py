"""Pull today's filled orders with full details from TS SIM."""
import os, requests, json
from datetime import datetime

r = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
}, timeout=15)
at = r.json()['access_token']

SIM_BASE = "https://sim-api.tradestation.com/v3"
FUTURES_ACCOUNT = "SIM2609239F"
headers = {"Authorization": f"Bearer {at}"}
today = datetime.now().strftime("%Y-%m-%d")

# Get balance
r2 = requests.get(f"{SIM_BASE}/brokerage/accounts/{FUTURES_ACCOUNT}/balances",
    headers=headers, timeout=10)
bal = r2.json().get("Balances", [{}])[0] if r2.status_code == 200 else {}
print(f"FUTURES ACCOUNT: {FUTURES_ACCOUNT}")
print(f"  Cash: ${bal.get('CashBalance', '?')}")
print(f"  Equity: ${bal.get('Equity', '?')}")

# Get orders - dump first one to see field names
r4 = requests.get(f"{SIM_BASE}/brokerage/accounts/{FUTURES_ACCOUNT}/orders",
    headers=headers, params={"since": f"{today}T00:00:00Z"}, timeout=15)
orders = r4.json().get("Orders", []) if r4.status_code == 200 else []

# Print first order to see all fields
if orders:
    print(f"\nSample order keys: {list(orders[0].keys())}")
    # Print first filled order in full
    for o in orders:
        if o.get("Status") == "FLL" or "Filled" in str(o.get("StatusDescription", "")):
            print(f"\nSample filled order:")
            print(json.dumps(o, indent=2))
            break

print(f"\n{'='*100}")
print(f"Total orders: {len(orders)}")
filled = sorted([o for o in orders if "FLL" in str(o.get("Status","")) or "Filled" in str(o.get("StatusDescription",""))],
    key=lambda x: x.get("ClosedDateTime", x.get("OpenedDateTime","")))

# Build trade pairs
print(f"Filled: {len(filled)}\n")

# Show all filled with whatever buy/sell field exists
for o in filled:
    legs = o.get("Legs", [{}])
    leg = legs[0] if legs else {}
    side = leg.get("BuyOrSell", o.get("BuyOrSell", o.get("TradeAction", "?")))
    qty = leg.get("QuantityOrdered", o.get("Quantity", o.get("QuantityOrdered", "?")))
    symbol = leg.get("Symbol", o.get("Symbol", "?"))
    otype = o.get("OrderType", "?")
    fill_price = o.get("FilledPrice", leg.get("ExecPrice", "?"))
    oid = o.get("OrderID", "?")
    closed = o.get("ClosedDateTime", o.get("OpenedDateTime", ""))
    try:
        t = datetime.fromisoformat(closed.replace("Z", "+00:00"))
        time_str = t.strftime("%H:%M:%S")
    except:
        time_str = "?"
    print(f"  {time_str:>10} {side:>8} {str(qty):>4} {symbol:>10} {otype:>14} {str(fill_price):>10} {oid:>12}")
