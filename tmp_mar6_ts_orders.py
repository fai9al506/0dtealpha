"""Pull TS SIM orders for March 6 to get actual execution P&L."""
import os, json, requests
from datetime import datetime, timedelta

# Get TS access token
resp = requests.post("https://signin.tradestation.com/oauth/token", data={
    "grant_type": "refresh_token",
    "client_id": os.environ["TS_CLIENT_ID"],
    "client_secret": os.environ["TS_CLIENT_SECRET"],
    "refresh_token": os.environ["TS_REFRESH_TOKEN"],
})
token = resp.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# Get SIM futures orders for March 6
# Futures SIM account
FUTURES_SIM = "SIM2609239F"
OPTIONS_SIM = "SIM2609238M"

for acct, label in [(FUTURES_SIM, "FUTURES SIM"), (OPTIONS_SIM, "OPTIONS SIM")]:
    print(f"\n=== {label} ({acct}) ===", flush=True)
    resp = requests.get(
        f"https://sim-api.tradestation.com/v3/brokerage/accounts/{acct}/orders",
        headers=headers,
        params={"since": "2026-03-06", "pageSize": "600"}
    )
    if resp.status_code != 200:
        print(f"Error: {resp.status_code} {resp.text[:200]}", flush=True)
        continue

    data = resp.json()
    orders = data.get("Orders", [])
    print(f"Total orders: {len(orders)}", flush=True)

    # Filter for March 6 only
    mar6_orders = []
    for o in orders:
        ot = o.get("OpenedDateTime", "")
        if ot.startswith("2026-03-06"):
            mar6_orders.append(o)

    print(f"March 6 orders: {len(mar6_orders)}", flush=True)

    # Compute P&L using FIFO
    positions = []  # list of (qty, price, side)
    fills = []
    total_realized = 0
    total_commission = 0

    for o in sorted(mar6_orders, key=lambda x: x.get("ClosedDateTime", x.get("OpenedDateTime", ""))):
        status = o.get("Status", "")
        if status not in ("FLL", "Filled"):
            # Count other statuses
            continue

        legs = o.get("Legs", [])
        if not legs:
            continue

        leg = legs[0]
        symbol = leg.get("Symbol", "")
        action = leg.get("BuyOrSell", "")
        qty_filled = int(leg.get("QuantityOrdered", leg.get("ExecQuantity", 0)))
        fill_price = float(leg.get("ExecPrice", 0))
        commission = float(o.get("CommissionFee", 0))
        total_commission += commission

        fills.append({
            "time": o.get("ClosedDateTime", o.get("OpenedDateTime", "")),
            "symbol": symbol,
            "action": action,
            "qty": qty_filled,
            "price": fill_price,
            "commission": commission,
            "status": status,
            "type": o.get("OrderType", ""),
        })

    print(f"Filled orders: {len(fills)}", flush=True)

    # Print fills
    for f in fills:
        print(f"  {f['time'][:19]}  {f['action']:4}  {f['qty']:3}x  {f['symbol']:20}  @{f['price']:.2f}  ${f['commission']:.2f}  {f['type']}", flush=True)

    # FIFO position tracking
    pos_qty = 0  # positive = long, negative = short
    pos_cost = 0.0
    realized_pnl = 0.0

    multiplier = 5.0 if "MES" in (fills[0]["symbol"] if fills else "") or "ES" in (fills[0]["symbol"] if fills else "") else 100.0

    for f in fills:
        qty = f["qty"]
        price = f["price"]

        if f["action"] in ("Buy", "BuyToCover"):
            trade_qty = qty
        else:  # Sell, SellShort
            trade_qty = -qty

        if (pos_qty >= 0 and trade_qty > 0) or (pos_qty <= 0 and trade_qty < 0):
            # Adding to position
            pos_cost = (pos_cost * abs(pos_qty) + price * abs(trade_qty)) / (abs(pos_qty) + abs(trade_qty)) if (abs(pos_qty) + abs(trade_qty)) > 0 else price
            pos_qty += trade_qty
        else:
            # Closing position
            close_qty = min(abs(trade_qty), abs(pos_qty))
            if pos_qty > 0:
                pnl = (price - pos_cost) * close_qty * multiplier
            else:
                pnl = (pos_cost - price) * close_qty * multiplier
            realized_pnl += pnl

            remaining = abs(trade_qty) - close_qty
            if remaining > 0:
                # Flipped direction
                pos_qty = trade_qty + (trade_qty // abs(trade_qty)) * (abs(pos_qty) - close_qty) if trade_qty != 0 else 0
                pos_cost = price
            else:
                pos_qty += trade_qty
                if pos_qty == 0:
                    pos_cost = 0

    print(f"\nRealized P&L: ${realized_pnl:.2f}", flush=True)
    print(f"Commission: ${total_commission:.2f}", flush=True)
    print(f"Net P&L: ${realized_pnl - total_commission:.2f}", flush=True)
    print(f"Remaining position: {pos_qty}", flush=True)
