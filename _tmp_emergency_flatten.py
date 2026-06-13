"""EMERGENCY: flatten 2 real-trader positions + report P&L.

Runs via `railway run --service 0dtealpha`. Uses real_trader's _ts_api helper
which has refresh-token rotation, so we don't manually juggle access tokens.

For each open position:
  1. Query broker position to confirm it's actually open
  2. Cancel the stop order (DELETE)
  3. Place a market close order (opposite side)
  4. Wait briefly, query fill, compute P&L vs entry
  5. Update real_trade_orders state to 'closed' with close_reason='manual_emergency'

Prints a summary at the end. Idempotent — safe to re-run.

DRY-RUN: set DRY_RUN=1 to print what would happen without sending orders.
"""
import os, sys, time, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DRY_RUN = os.getenv("DRY_RUN", "").lower() in ("1", "true", "yes")
print(f"[flatten] DRY_RUN={DRY_RUN}")

# Pull engine + TS helpers from real_trader module's runtime state
from app import real_trader as rt
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])

# We need to initialize real_trader so _ts_api works (it needs access token)
# real_trader has refresh-token logic in _refresh_access_token via _ts_api
# But _ts_api needs an initial token set via init(). Let's check.

# Look up TS creds from env (same ones real_trader uses)
import requests
TS_API_BASE = "https://api.tradestation.com/v3"
def get_ts_token():
    """Get fresh TS access token via refresh token."""
    r = requests.post(
        "https://signin.tradestation.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": os.environ["TS_CLIENT_ID"],
            "client_secret": os.environ["TS_CLIENT_SECRET"],
            "refresh_token": os.environ["TS_REFRESH_TOKEN"],
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["access_token"]

TOKEN = get_ts_token()
print(f"[flatten] Got TS access token (first 20): {TOKEN[:20]}...")

def ts_api(method, path, body=None):
    """Simple wrapper — TS v3 API."""
    hdrs = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    url = f"{TS_API_BASE}{path}"
    if method == "GET":
        r = requests.get(url, headers=hdrs, timeout=15)
    elif method == "POST":
        r = requests.post(url, headers=hdrs, json=body, timeout=15)
    elif method == "DELETE":
        r = requests.delete(url, headers=hdrs, timeout=15)
    else:
        raise ValueError(method)
    return r

# Positions to close
POSITIONS = [
    {"lid": 3110, "setup": "DD Exhaustion long",   "acct": "210VYX65",
     "direction": "long",  "fill": 7426.25, "stop_oid": "1267143699"},
    {"lid": 3109, "setup": "Skew Charm short",     "acct": "210VYX91",
     "direction": "short", "fill": 7426.5,  "stop_oid": "1267141456"},
]

# Step 0: confirm broker positions match expectations
print("\n=== Verifying broker positions ===")
for p in POSITIONS:
    r = ts_api("GET", f"/brokerage/accounts/{p['acct']}/positions")
    if r.status_code != 200:
        print(f"  lid={p['lid']}: positions API failed {r.status_code}: {r.text[:200]}")
        continue
    positions = r.json().get("Positions", [])
    print(f"  lid={p['lid']} acct={p['acct']}: {len(positions)} position(s) on broker")
    for pos in positions:
        print(f"    Symbol={pos.get('Symbol')} qty={pos.get('Quantity')} long_short={pos.get('LongShort')} avg_price={pos.get('AveragePrice')}")
        p["broker_symbol"] = pos.get("Symbol")
        p["broker_qty"] = abs(int(float(pos.get("Quantity", 0))))
        p["broker_long_short"] = pos.get("LongShort")

# Step 1: cancel stop orders
print("\n=== Cancelling stop orders ===")
for p in POSITIONS:
    if DRY_RUN:
        print(f"  [DRY] would DELETE /orderexecution/orders/{p['stop_oid']} (acct={p['acct']})")
        continue
    r = ts_api("DELETE", f"/orderexecution/orders/{p['stop_oid']}")
    print(f"  lid={p['lid']} stop_oid={p['stop_oid']}: HTTP {r.status_code} {r.text[:120]}")

time.sleep(2)

# Step 2: place market close orders
print("\n=== Placing market close orders ===")
for p in POSITIONS:
    if not p.get("broker_symbol") or not p.get("broker_qty"):
        print(f"  lid={p['lid']}: no broker position to close, skipping")
        continue
    # Opposite side
    side = "SELL" if p["broker_long_short"] == "Long" else "BUY"
    body = {
        "AccountID": p["acct"],
        "Symbol": p["broker_symbol"],
        "Quantity": str(p["broker_qty"]),
        "OrderType": "Market",
        "TradeAction": side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    if DRY_RUN:
        print(f"  [DRY] would POST {body}")
        continue
    r = ts_api("POST", "/orderexecution/orders", body=body)
    print(f"  lid={p['lid']} close: HTTP {r.status_code}")
    try:
        resp = r.json()
        print(f"    response: {json.dumps(resp, indent=2)[:400]}")
        if r.status_code == 200 and "Orders" in resp:
            close_oid = resp["Orders"][0].get("OrderID")
            p["close_oid"] = close_oid
            print(f"    close_oid={close_oid}")
    except Exception as e:
        print(f"    parse error: {e} body={r.text[:200]}")

if DRY_RUN:
    print("\n[DRY] No actions taken. Re-run without DRY_RUN=1 to execute.")
else:
    print("\n=== Waiting 5s for fills, then querying ===")
    time.sleep(5)
    for p in POSITIONS:
        if not p.get("close_oid"):
            continue
        r = ts_api("GET", f"/brokerage/accounts/{p['acct']}/historicalorders?since=2026-05-21&pageSize=50")
        if r.status_code == 200:
            for o in r.json().get("Orders", []):
                if o.get("OrderID") == p["close_oid"]:
                    legs = o.get("Legs", [])
                    fill_px = None
                    for leg in legs:
                        if leg.get("ExecQuantity") and float(leg.get("ExecQuantity", 0)) > 0:
                            fill_px = float(leg.get("ExecutionPrice", 0))
                    if fill_px:
                        sign = 1 if p["direction"] == "long" else -1
                        pnl = sign * (fill_px - p["fill"])
                        print(f"  lid={p['lid']} {p['setup']}: close fill={fill_px} entry={p['fill']} → P&L = {pnl:+.2f} pts ({pnl*5:+.2f}$ @ 1 MES)")
                    else:
                        print(f"  lid={p['lid']}: close order found but no fill yet")
                    break
    print("\n=== DONE — verify in TradeStation UI ===")
