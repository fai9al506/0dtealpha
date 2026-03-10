"""Check raw order data for March 6 SIM accounts."""
import os, json, requests

resp = requests.post("https://signin.tradestation.com/oauth/token", data={
    "grant_type": "refresh_token",
    "client_id": os.environ["TS_CLIENT_ID"],
    "client_secret": os.environ["TS_CLIENT_SECRET"],
    "refresh_token": os.environ["TS_REFRESH_TOKEN"],
})
token = resp.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

FUTURES_SIM = "SIM2609239F"
OPTIONS_SIM = "SIM2609238M"

for acct, label in [(FUTURES_SIM, "FUTURES SIM"), (OPTIONS_SIM, "OPTIONS SIM")]:
    print(f"\n=== {label} ({acct}) ===", flush=True)
    resp = requests.get(
        f"https://sim-api.tradestation.com/v3/brokerage/accounts/{acct}/orders",
        headers=headers,
        params={"since": "2026-03-06", "pageSize": "600"}
    )
    data = resp.json()
    orders = data.get("Orders", [])

    # Count by status
    statuses = {}
    for o in orders:
        s = o.get("StatusDescription", o.get("Status", "?"))
        statuses[s] = statuses.get(s, 0) + 1
    print(f"Status distribution: {statuses}", flush=True)

    # Print first 5 orders with all relevant fields
    for i, o in enumerate(sorted(orders, key=lambda x: x.get("OpenedDateTime", ""))[:5]):
        legs = o.get("Legs", [{}])
        leg = legs[0] if legs else {}
        print(f"\nOrder {i+1}:", flush=True)
        print(f"  Status: {o.get('Status')} / {o.get('StatusDescription')}", flush=True)
        print(f"  Type: {o.get('OrderType')}", flush=True)
        print(f"  Opened: {o.get('OpenedDateTime')}", flush=True)
        print(f"  Closed: {o.get('ClosedDateTime')}", flush=True)
        print(f"  Symbol: {leg.get('Symbol')}", flush=True)
        print(f"  Action: {leg.get('BuyOrSell')}", flush=True)
        print(f"  Qty: {leg.get('QuantityOrdered')} / Exec: {leg.get('ExecQuantity')}", flush=True)
        print(f"  ExecPrice: {leg.get('ExecPrice')}", flush=True)
        print(f"  LimitPrice: {o.get('LimitPrice')}", flush=True)
        print(f"  StopPrice: {o.get('StopPrice')}", flush=True)
        print(f"  Commission: {o.get('CommissionFee')}", flush=True)
        print(f"  GroupName: {o.get('GroupName')}", flush=True)
        print(f"  FilledPrice: {leg.get('FilledPrice', 'N/A')}", flush=True)

    # Print ALL orders briefly
    print(f"\n--- ALL {len(orders)} ORDERS ---", flush=True)
    for o in sorted(orders, key=lambda x: x.get("OpenedDateTime", "")):
        legs = o.get("Legs", [{}])
        leg = legs[0] if legs else {}
        ts = o.get("ClosedDateTime", o.get("OpenedDateTime", ""))[:19]
        sym = leg.get("Symbol", "?")
        act = leg.get("BuyOrSell", "?")
        qty = leg.get("ExecQuantity", leg.get("QuantityOrdered", "?"))
        price = leg.get("ExecPrice", "?")
        status = o.get("StatusDescription", o.get("Status", "?"))
        otype = o.get("OrderType", "?")
        print(f"  {ts}  {status:12}  {act:4}  {qty:>3}x  {sym:24}  @{price}  {otype}", flush=True)

# Also get account balances
print("\n=== ACCOUNT BALANCES ===", flush=True)
for acct, label in [(FUTURES_SIM, "FUTURES SIM"), (OPTIONS_SIM, "OPTIONS SIM")]:
    resp = requests.get(
        f"https://sim-api.tradestation.com/v3/brokerage/accounts/{acct}/balances",
        headers=headers
    )
    if resp.status_code == 200:
        bal = resp.json().get("Balances", [{}])
        if bal:
            b = bal[0] if isinstance(bal, list) else bal
            print(f"{label}: CashBalance=${b.get('CashBalance')}, Equity=${b.get('Equity')}, RealizedPnL=${b.get('RealizedProfitLoss')}, TodayPnL=${b.get('TodaysProfitLoss')}", flush=True)
    else:
        print(f"{label}: {resp.status_code}", flush=True)
