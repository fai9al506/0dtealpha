"""Pull historical orders with actual fill prices for both accounts."""
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

# 1. Futures SIM - find origin of the SHORT 10 MESH26 position
print("=== FUTURES SIM - ALL HISTORICAL ORDERS ===", flush=True)
resp = requests.get(
    f"https://sim-api.tradestation.com/v3/brokerage/accounts/{FUTURES_SIM}/historicalorders",
    headers=headers,
    params={"since": "2026-03-01", "pageSize": "600"}
)
if resp.status_code == 200:
    data = resp.json()
    orders = data.get("Orders", data.get("HistoricalOrders", []))
    print(f"Total historical orders: {len(orders)}", flush=True)
    for o in sorted(orders, key=lambda x: x.get("ClosedDateTime", "")):
        legs = o.get("Legs", [{}])
        leg = legs[0] if legs else {}
        print(f"  {o.get('ClosedDateTime', '?')[:19]}  {o.get('StatusDescription', '?'):10}  "
              f"{leg.get('BuyOrSell', '?'):4}  {leg.get('ExecQuantity', '?'):>3}x  "
              f"{leg.get('Symbol', '?'):12}  fill@{o.get('FilledPrice', leg.get('ExecutionPrice', 'N/A'))}  "
              f"{o.get('OrderType', '?')}  comm=${o.get('CommissionFee', 0)}  "
              f"oid={o.get('OrderID', '?')}", flush=True)
else:
    print(f"Error: {resp.status_code} {resp.text[:200]}", flush=True)

# 2. Options SIM - full historical with fill prices
print("\n=== OPTIONS SIM - HISTORICAL ORDERS WITH FILL PRICES ===", flush=True)
resp = requests.get(
    f"https://sim-api.tradestation.com/v3/brokerage/accounts/{OPTIONS_SIM}/historicalorders",
    headers=headers,
    params={"since": "2026-03-01", "pageSize": "600"}
)
if resp.status_code == 200:
    data = resp.json()
    orders = data.get("Orders", data.get("HistoricalOrders", []))
    print(f"Total historical orders: {len(orders)}", flush=True)

    # Group by date
    by_date = {}
    for o in orders:
        dt = o.get("ClosedDateTime", "")[:10]
        by_date.setdefault(dt, []).append(o)

    total_pnl = 0
    total_comm = 0
    for dt in sorted(by_date.keys()):
        print(f"\n  --- {dt} ---", flush=True)
        day_orders = sorted(by_date[dt], key=lambda x: x.get("ClosedDateTime", ""))

        # Track buys/sells for P&L calculation
        holdings = {}  # symbol -> [(qty, price)]

        for o in day_orders:
            legs = o.get("Legs", [{}])
            leg = legs[0] if legs else {}
            sym = leg.get("Symbol", "?")
            act = leg.get("BuyOrSell", "?")
            qty = int(leg.get("ExecQuantity", 0))
            fill_price = float(o.get("FilledPrice", leg.get("ExecutionPrice", 0)) or 0)
            comm = float(o.get("CommissionFee", 0))
            open_close = leg.get("OpenOrClose", "?")
            total_comm += comm

            print(f"    {o.get('ClosedDateTime', '?')[:19]}  {act:4}  {qty}x  {sym:24}  "
                  f"fill@${fill_price:.2f}  {open_close}  comm=${comm:.0f}", flush=True)

            # Simple P&L: Buy(Open) = cost, Sell(Close) = revenue
            if act == "Buy" and open_close == "Open":
                holdings.setdefault(sym, []).append((qty, fill_price))
            elif act == "Sell" and open_close == "Close":
                if sym in holdings and holdings[sym]:
                    entry_qty, entry_price = holdings[sym].pop(0)
                    pnl = (fill_price - entry_price) * qty * 100
                    total_pnl += pnl
                    print(f"      >>> P&L: entry@${entry_price:.2f} exit@${fill_price:.2f} = ${pnl:+.0f}", flush=True)

    print(f"\n=== OPTIONS TOTAL P&L: ${total_pnl:+.0f} ===", flush=True)
    print(f"=== TOTAL COMMISSIONS: ${total_comm:.0f} ===", flush=True)
    print(f"=== NET P&L: ${total_pnl - total_comm:+.0f} ===", flush=True)
    print(f"=== TS REPORTED REALIZED P&L: -$1,942 ===", flush=True)
else:
    print(f"Error: {resp.status_code} {resp.text[:200]}", flush=True)
