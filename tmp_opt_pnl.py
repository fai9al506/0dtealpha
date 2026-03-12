"""Pull options SIM account orders from TS API and compute P&L."""
import os, requests, json
from datetime import datetime, timedelta
from collections import defaultdict

# Auth
r = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
}, timeout=15)
at = r.json()['access_token']

SIM_BASE = "https://sim-api.tradestation.com/v3"
OPT_ACCOUNT = "SIM2609238M"
headers = {"Authorization": f"Bearer {at}"}

# Pull orders since Mar 5 (when options trader started)
since = "2026-03-01T00:00:00Z"
r2 = requests.get(f"{SIM_BASE}/brokerage/accounts/{OPT_ACCOUNT}/orders",
    headers=headers, params={"since": since}, timeout=15)
orders = r2.json().get("Orders", [])
print(f"Total orders from API: {len(orders)}", flush=True)

filled = [o for o in orders if o.get("Status") == "FLL"]
filled.sort(key=lambda x: x.get("ClosedDateTime", x.get("OpenedDateTime", "")))
print(f"Filled orders: {len(filled)}", flush=True)

# Also get account balances
r3 = requests.get(f"{SIM_BASE}/brokerage/accounts/{OPT_ACCOUNT}/balances",
    headers=headers, timeout=15)
balances = r3.json().get("Balances", [{}])
if balances:
    b = balances[0]
    print(f"\nACCOUNT BALANCE:", flush=True)
    print(f"  Cash: ${float(b.get('CashBalance', 0)):,.2f}", flush=True)
    print(f"  Equity: ${float(b.get('Equity', 0)):,.2f}", flush=True)
    print(f"  Market Value: ${float(b.get('MarketValue', 0)):,.2f}", flush=True)
    print(f"  Today P&L: ${float(b.get('TodaysProfitLoss', 0)):,.2f}", flush=True)
    print(f"  Unrealized P&L: ${float(b.get('UnrealizedProfitLoss', 0)):,.2f}", flush=True)
    print(f"  Realized P&L: ${float(b.get('RealProfitLoss', b.get('RealizedProfitLoss', 0))):,.2f}", flush=True)

# Get positions
r4 = requests.get(f"{SIM_BASE}/brokerage/accounts/{OPT_ACCOUNT}/positions",
    headers=headers, timeout=15)
positions = r4.json().get("Positions", [])
print(f"\nOpen positions: {len(positions)}", flush=True)
for p in positions:
    print(f"  {p.get('Symbol')} qty={p.get('Quantity')} avg={p.get('AveragePrice')} P&L={p.get('UnrealizedProfitLoss')}", flush=True)

# Trade log
print(f"\n{'#':>3} {'Date':>10} {'Time':>8} {'Action':>6} {'Qty':>4} {'Symbol':<25} {'Price':>8} {'Comm':>6} {'OrderID':>12}", flush=True)
print("-" * 95, flush=True)

daily_pnl = defaultdict(float)
daily_comm = defaultdict(float)
daily_trades = defaultdict(int)

for i, o in enumerate(filled):
    leg = o.get("Legs", [{}])[0]
    side = leg.get("BuyOrSell", "?")
    qty = int(leg.get("ExecQuantity", leg.get("QuantityOrdered", 0)))
    symbol = leg.get("Symbol", o.get("Symbol", "?"))
    price = float(o.get("FilledPrice", 0))
    comm = float(o.get("CommissionFee", 0))
    oid = o.get("OrderID", "?")

    closed = o.get("ClosedDateTime", o.get("OpenedDateTime", ""))
    try:
        t = datetime.fromisoformat(closed.replace("Z", "+00:00"))
        date_str = t.strftime("%Y-%m-%d")
        time_str = t.strftime("%H:%M:%S")
    except:
        date_str = "?"
        time_str = "?"

    daily_comm[date_str] += comm
    daily_trades[date_str] += 1

    print(f"{i+1:>3} {date_str:>10} {time_str:>8} {side:>6} {qty:>4} {symbol:<25} ${price:>7.2f} ${comm:>5.2f} {oid:>12}", flush=True)

# FIFO P&L by symbol
print(f"\n\n=== FIFO P&L RECONSTRUCTION ===", flush=True)

# Group fills by symbol
by_symbol = defaultdict(list)
for o in filled:
    leg = o.get("Legs", [{}])[0]
    symbol = leg.get("Symbol", o.get("Symbol", "?"))
    side = leg.get("BuyOrSell", "?")
    qty = int(leg.get("ExecQuantity", leg.get("QuantityOrdered", 0)))
    price = float(o.get("FilledPrice", 0))
    comm = float(o.get("CommissionFee", 0))
    closed = o.get("ClosedDateTime", o.get("OpenedDateTime", ""))
    by_symbol[symbol].append({'side': side, 'qty': qty, 'price': price, 'comm': comm, 'time': closed})

total_realized = 0
total_comm_all = 0
for sym in sorted(by_symbol.keys()):
    fills = by_symbol[sym]
    # Simple: buys are opens, sells are closes for calls
    buys = [f for f in fills if f['side'] == 'Buy']
    sells = [f for f in fills if f['side'] == 'Sell']

    buy_qty = sum(f['qty'] for f in buys)
    sell_qty = sum(f['qty'] for f in sells)
    buy_cost = sum(f['price'] * f['qty'] for f in buys)
    sell_proceeds = sum(f['price'] * f['qty'] for f in sells)
    comm_total = sum(f['comm'] for f in fills)

    if buy_qty > 0 and sell_qty > 0:
        matched = min(buy_qty, sell_qty)
        avg_buy = buy_cost / buy_qty
        avg_sell = sell_proceeds / sell_qty if sell_qty > 0 else 0
        pnl = (avg_sell - avg_buy) * matched * 100  # SPX options $100 multiplier
        total_realized += pnl
        total_comm_all += comm_total

        date = fills[0]['time'][:10] if fills[0]['time'] else '?'
        print(f"  {sym:<25} buy {buy_qty}@${avg_buy:.2f} sell {sell_qty}@${avg_sell:.2f} P&L=${pnl:+.0f} comm=${comm_total:.2f} [{date}]", flush=True)

print(f"\nTotal Realized P&L: ${total_realized:+,.0f}", flush=True)
print(f"Total Commissions: ${total_comm_all:,.2f}", flush=True)
print(f"Net P&L: ${total_realized - total_comm_all:+,.0f}", flush=True)
