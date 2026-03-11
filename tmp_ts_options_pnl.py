"""Calculate options SIM P&L from TS API order fills - Mar 10+ only (post-reset)"""
import os, sys, requests, json
from collections import defaultdict

# Get credentials from env
token_resp = requests.post("https://signin.tradestation.com/oauth/token", json={
    "grant_type": "refresh_token",
    "client_id": os.environ["TS_CLIENT_ID"],
    "client_secret": os.environ["TS_CLIENT_SECRET"],
    "refresh_token": os.environ["TS_REFRESH_TOKEN"],
})
access_token = token_resp.json()["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}
SIM_BASE = "https://sim-api.tradestation.com/v3"
ACCOUNT = "SIM2609238M"

# Balance
r = requests.get(f"{SIM_BASE}/brokerage/accounts/{ACCOUNT}/balances", headers=headers)
if r.ok:
    bal = r.json()
    for b in bal.get("Balances", []):
        print("Equity: $%s  Day P&L: $%s  Cash: $%s" % (
            b.get("Equity", "?"), b.get("TodaysProfitLoss", "?"), b.get("CashBalance", "?")))

# Today's orders (these are the post-reset Mar 10 trades)
r = requests.get(f"{SIM_BASE}/brokerage/accounts/{ACCOUNT}/orders", headers=headers)
if not r.ok:
    print("Error: %s %s" % (r.status_code, r.text[:200]))
    sys.exit(1)

orders = r.json().get("Orders", [])
print("Orders from API: %d" % len(orders))

# Parse filled orders
filled = []
rejected = 0
for o in orders:
    status = o.get("Status", "?")
    if status == "REJ":
        rejected += 1
        continue
    if status not in ("FLL", "FPR"):
        continue
    legs = o.get("Legs", [])
    if not legs:
        continue
    leg = legs[0]
    symbol = leg.get("Symbol", "?")
    action = leg.get("BuyOrSell", "?")
    qty_str = leg.get("QuantityOrdered", o.get("Quantity", "1"))
    qty = int(qty_str) if qty_str else 1
    fill_price = float(o.get("FilledPrice", "0"))
    closed_dt = o.get("ClosedDateTime", "")
    oid = o.get("OrderID", "?")
    filled.append({
        "oid": oid, "symbol": symbol, "action": action,
        "qty": qty, "fill_price": fill_price, "closed_dt": closed_dt,
    })

filled.sort(key=lambda x: x["closed_dt"])
print("Filled: %d, Rejected: %d" % (len(filled), rejected))
print()

# Track positions per symbol with proper average cost management
positions = defaultdict(lambda: {"qty": 0, "cost": 0.0})
trades = []

for f in filled:
    sym = f["symbol"]
    action = f["action"]
    qty = f["qty"]
    price = f["fill_price"]
    dt = f["closed_dt"]
    pos = positions[sym]

    if action == "Buy":
        if pos["qty"] >= 0:
            # Opening/adding long
            pos["cost"] += price * qty
            pos["qty"] += qty
        else:
            # Closing short (shouldn't happen for our strategy)
            avg_entry = pos["cost"] / abs(pos["qty"])
            pnl = (avg_entry - price) * qty * 100
            trades.append({"symbol": sym, "pnl": pnl, "dt": dt, "entry": avg_entry, "exit": price, "action": "cover"})
            pos["cost"] -= avg_entry * qty  # reduce cost proportionally
            pos["qty"] += qty
            if pos["qty"] == 0:
                pos["cost"] = 0.0
    else:  # Sell
        if pos["qty"] > 0:
            # Closing long
            avg_entry = pos["cost"] / pos["qty"]
            pnl = (price - avg_entry) * qty * 100
            trades.append({"symbol": sym, "pnl": pnl, "dt": dt, "entry": avg_entry, "exit": price, "action": "sell"})
            # Fix: reduce cost proportionally for partial closes
            remaining = pos["qty"] - qty
            if remaining > 0:
                pos["cost"] = avg_entry * remaining
            else:
                pos["cost"] = 0.0
            pos["qty"] = remaining
        else:
            # Opening short (shouldn't happen)
            pos["cost"] += price * qty
            pos["qty"] -= qty

# Print all fills with running P&L
print("=" * 95)
print("%-20s %-28s %5s %3s  %8s  %8s" % ("Time (UTC)", "Symbol", "Side", "Qty", "Price", "P&L"))
print("=" * 95)

running_pnl = 0
trade_idx = 0
for f in filled:
    sym = f["symbol"]
    action = f["action"]
    price = f["fill_price"]
    dt = f["closed_dt"][11:19] if len(f["closed_dt"]) > 11 else f["closed_dt"]
    side = "BUY" if action == "Buy" else "SELL"

    # Check if this sell closed a position
    pnl_str = ""
    if action != "Buy" and trade_idx < len(trades):
        t = trades[trade_idx]
        if t["dt"] == f["closed_dt"] and t["symbol"] == sym:
            running_pnl += t["pnl"]
            pnl_str = "$%+.0f" % t["pnl"]
            trade_idx += 1

    print("  %-20s %-28s %5s %3d  $%-7.2f  %s" % (dt, sym, side, f["qty"], price, pnl_str))

print()
print("=" * 95)
print("SUMMARY")
print("=" * 95)

# By session
morning = [t for t in trades if t["dt"] < "2026-03-10T17:00:00Z"]  # Before 12:00 ET
afternoon = [t for t in trades if t["dt"] >= "2026-03-10T17:00:00Z"]

def summarize(label, tlist):
    if not tlist:
        return
    w = sum(1 for t in tlist if t["pnl"] > 0)
    l = sum(1 for t in tlist if t["pnl"] < 0)
    total = sum(t["pnl"] for t in tlist)
    print("  %-20s %dW/%dL  $%+,.0f" % (label, w, l, total))

summarize("Morning (pre-12 ET)", morning)
summarize("Afternoon (12+ ET)", afternoon)
print()

total_pnl = sum(t["pnl"] for t in trades)
wins = sum(1 for t in trades if t["pnl"] > 0)
losses = sum(1 for t in trades if t["pnl"] < 0)
print("  Computed round-trips: %d trades, %dW/%dL, $%+,.0f" % (len(trades), wins, losses, total_pnl))
print("  Account equity: $55,390 (started $50,000) = $+5,390")
if abs(total_pnl - 5390) > 100:
    print("  ** GAP: $%.0f unaccounted (commissions, rounding, or trades not in API)" % (5390 - total_pnl))

# Remaining open
print("\n  Remaining positions:")
for sym, pos in positions.items():
    if pos["qty"] != 0:
        print("    %s: qty=%d, avg=$%.2f" % (sym, pos["qty"], pos["cost"]/abs(pos["qty"]) if pos["qty"] else 0))

sys.stdout.flush()
