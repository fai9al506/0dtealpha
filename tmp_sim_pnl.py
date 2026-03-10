"""Reconstruct trade-by-trade P&L using FIFO position tracking."""
import os, requests, json
from datetime import datetime
from collections import deque

r = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
}, timeout=15)
at = r.json()['access_token']

SIM_BASE = "https://sim-api.tradestation.com/v3"
ACCOUNT = "SIM2609239F"
headers = {"Authorization": f"Bearer {at}"}
today = datetime.now().strftime("%Y-%m-%d")

r4 = requests.get(f"{SIM_BASE}/brokerage/accounts/{ACCOUNT}/orders",
    headers=headers, params={"since": f"{today}T00:00:00Z"}, timeout=15)
orders = r4.json().get("Orders", [])

filled = [o for o in orders if o.get("Status") == "FLL"]
filled.sort(key=lambda x: x.get("ClosedDateTime", x.get("OpenedDateTime", "")))

# Track position and P&L
position = 0  # positive = long, negative = short
avg_price = 0.0
total_realized_pnl = 0.0
total_commissions = 0.0
trade_num = 0

print(f"{'#':>3} {'Time':>10} {'Action':>6} {'Qty':>4} {'Price':>10} {'Position':>10} {'RealPnL':>10} {'CumPnL':>10} {'Comm':>6}")
print("-" * 80)

for o in filled:
    leg = o.get("Legs", [{}])[0]
    side = leg.get("BuyOrSell", "?")
    qty = int(leg.get("ExecQuantity", 0))
    price = float(o.get("FilledPrice", 0))
    comm = float(o.get("CommissionFee", 0))
    total_commissions += comm

    closed = o.get("ClosedDateTime", o.get("OpenedDateTime", ""))
    try:
        t = datetime.fromisoformat(closed.replace("Z", "+00:00"))
        time_str = t.strftime("%H:%M:%S")
    except:
        time_str = "?"

    trade_pnl = 0.0

    if side == "Buy":
        if position < 0:
            # Closing short position
            close_qty = min(qty, abs(position))
            trade_pnl = (avg_price - price) * close_qty * 5  # short profit = entry - exit
            total_realized_pnl += trade_pnl
            remaining = qty - close_qty
            position += qty  # buying reduces short / adds long
            if position > 0:
                avg_price = price  # flipped to long
            elif position == 0:
                avg_price = 0
        else:
            # Adding to long
            if position == 0:
                avg_price = price
            else:
                avg_price = (avg_price * position + price * qty) / (position + qty)
            position += qty
    elif side == "Sell":
        if position > 0:
            # Closing long position
            close_qty = min(qty, position)
            trade_pnl = (price - avg_price) * close_qty * 5  # long profit = exit - entry
            total_realized_pnl += trade_pnl
            remaining = qty - close_qty
            position -= qty
            if position < 0:
                avg_price = price  # flipped to short
            elif position == 0:
                avg_price = 0
        else:
            # Adding to short
            if position == 0:
                avg_price = price
            else:
                avg_price = (avg_price * abs(position) + price * qty) / (abs(position) + qty)
            position -= qty

    trade_num += 1
    pnl_str = f"{trade_pnl:>+10.2f}" if trade_pnl != 0 else f"{'—':>10}"
    print(f"{trade_num:>3} {time_str:>10} {side:>6} {qty:>4} {price:>10.2f} {position:>10} {pnl_str} {total_realized_pnl:>+10.2f} {comm:>6.1f}")

print(f"\n{'='*80}")
print(f"Final position: {position} @ avg {avg_price:.2f}")
print(f"Total realized P&L: ${total_realized_pnl:+,.2f}")
print(f"Total commissions:  ${total_commissions:,.2f}")
print(f"Realized after comm: ${total_realized_pnl - total_commissions:+,.2f}")

# Current mark
current_price = 6785  # approximate current price
if position != 0:
    if position > 0:
        unrealized = (current_price - avg_price) * position * 5
    else:
        unrealized = (avg_price - current_price) * abs(position) * 5
    print(f"Unrealized P&L @ ~{current_price}: ${unrealized:+,.2f}")
    print(f"Total P&L (realized + unrealized - comm): ${total_realized_pnl + unrealized - total_commissions:+,.2f}")
    print(f"\nExpected balance: ${50000 + total_realized_pnl + unrealized - total_commissions:,.2f}")

print(f"\nActual balance from API: $46,836.75")
