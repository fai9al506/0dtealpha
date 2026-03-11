"""Query TS SIM API for options account orders/history - comprehensive"""
import os, sys, requests, json

# Get fresh token
TS_CLIENT_ID = os.environ.get("TS_CLIENT_ID")
TS_CLIENT_SECRET = os.environ.get("TS_CLIENT_SECRET")
TS_REFRESH_TOKEN = os.environ.get("TS_REFRESH_TOKEN")

if not all([TS_CLIENT_ID, TS_CLIENT_SECRET, TS_REFRESH_TOKEN]):
    import subprocess
    result = subprocess.run(["railway", "variables", "-s", "0dtealpha", "--json"],
                          capture_output=True, text=True)
    env = json.loads(result.stdout)
    TS_CLIENT_ID = env.get("TS_CLIENT_ID")
    TS_CLIENT_SECRET = env.get("TS_CLIENT_SECRET")
    TS_REFRESH_TOKEN = env.get("TS_REFRESH_TOKEN")

token_resp = requests.post("https://signin.tradestation.com/oauth/token", json={
    "grant_type": "refresh_token",
    "client_id": TS_CLIENT_ID,
    "client_secret": TS_CLIENT_SECRET,
    "refresh_token": TS_REFRESH_TOKEN,
})
token_data = token_resp.json()
access_token = token_data.get("access_token")
if not access_token:
    print("Token error:", token_data)
    sys.exit(1)

headers = {"Authorization": f"Bearer {access_token}"}
SIM_BASE = "https://sim-api.tradestation.com/v3"
ACCOUNT = "SIM2609238M"

print("=" * 80)
print("OPTIONS SIM ACCOUNT: %s" % ACCOUNT)
print("=" * 80)

# 1. Account balance
print("\n-- BALANCE --")
r = requests.get(f"{SIM_BASE}/brokerage/accounts/{ACCOUNT}/balances", headers=headers)
if r.ok:
    bal = r.json()
    for b in bal.get("Balances", []):
        print("  Cash: $%s  Equity: $%s  Day P&L: $%s  Unrealized: $%s" % (
            b.get("CashBalance", "?"), b.get("Equity", "?"),
            b.get("TodaysProfitLoss", "?"), b.get("UnrealizedProfitLoss", "?")))
else:
    print("  Error: %s %s" % (r.status_code, r.text[:200]))

# 2. Open positions
print("\n-- OPEN POSITIONS --")
r = requests.get(f"{SIM_BASE}/brokerage/accounts/{ACCOUNT}/positions", headers=headers)
if r.ok:
    pos = r.json()
    positions = pos.get("Positions", [])
    if positions:
        for p in positions:
            print("  %s  qty=%s  avg=%s  last=%s  P&L=$%s" % (
                p.get("Symbol"), p.get("Quantity"), p.get("AveragePrice"),
                p.get("Last"), p.get("UnrealizedProfitLoss")))
    else:
        print("  No open positions")
else:
    print("  Error: %s %s" % (r.status_code, r.text[:200]))

# 3. Today's orders (active/recent)
print("\n-- TODAY'S ORDERS (/orders) --")
r = requests.get(f"{SIM_BASE}/brokerage/accounts/{ACCOUNT}/orders", headers=headers)
if r.ok:
    data = r.json()
    orders = data.get("Orders", [])
    print("  Found %d orders" % len(orders))
    for o in orders:
        legs = o.get("Legs", [])
        symbol = legs[0].get("Symbol", "?") if legs else "?"
        action = legs[0].get("BuyOrSell", "?") if legs else "?"
        print("  %s  %-30s %-5s  qty=%s  fill=$%s  [%s]  %s" % (
            o.get("OrderID", "?"), symbol, action,
            o.get("Quantity", "?"), o.get("FilledPrice", "?"),
            o.get("Status", "?"), o.get("ClosedDateTime", "")))
else:
    print("  Error: %s %s" % (r.status_code, r.text[:200]))

# 4. Historical orders - try different date ranges
for since_date in ["2026-03-01", "2026-03-05", "2026-03-09", "2026-03-10"]:
    print("\n-- HISTORICAL ORDERS (since=%s) --" % since_date)
    r = requests.get(f"{SIM_BASE}/brokerage/accounts/{ACCOUNT}/historicalorders?since={since_date}",
                    headers=headers)
    if r.ok:
        data = r.json()
        orders = data.get("Orders", [])
        print("  Found %d orders" % len(orders))
        for o in orders[:5]:  # Show first 5
            legs = o.get("Legs", [])
            symbol = legs[0].get("Symbol", "?") if legs else "?"
            action = legs[0].get("BuyOrSell", "?") if legs else "?"
            print("  %s  %-30s %-5s  qty=%s  fill=$%s  [%s]  %s" % (
                o.get("OrderID", "?"), symbol, action,
                o.get("Quantity", "?"), o.get("FilledPrice", "?"),
                o.get("Status", "?"), o.get("ClosedDateTime", "")))
        if len(orders) > 5:
            print("  ... and %d more" % (len(orders) - 5))
    else:
        print("  Error: %s %s" % (r.status_code, r.text[:200]))

# 5. Try the LIVE API for historical orders (SIM API may not store them)
print("\n-- HISTORICAL ORDERS via LIVE API (since=2026-03-01) --")
LIVE_BASE = "https://api.tradestation.com/v3"
r = requests.get(f"{LIVE_BASE}/brokerage/accounts/{ACCOUNT}/historicalorders?since=2026-03-01",
                headers=headers)
if r.ok:
    data = r.json()
    orders = data.get("Orders", [])
    print("  Found %d orders" % len(orders))
    for o in orders[:10]:
        legs = o.get("Legs", [])
        symbol = legs[0].get("Symbol", "?") if legs else "?"
        action = legs[0].get("BuyOrSell", "?") if legs else "?"
        print("  %s  %-30s %-5s  qty=%s  fill=$%s  [%s]  %s" % (
            o.get("OrderID", "?"), symbol, action,
            o.get("Quantity", "?"), o.get("FilledPrice", "?"),
            o.get("Status", "?"), o.get("ClosedDateTime", "")))
    if len(orders) > 10:
        print("  ... and %d more" % (len(orders) - 10))
else:
    print("  Error: %s %s" % (r.status_code, r.text[:200]))

sys.stdout.flush()
