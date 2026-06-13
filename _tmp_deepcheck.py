"""Deeper dive: open position state, BP utilization, weird close prices."""
import os, requests, json, psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
TS_BASE = "https://api.tradestation.com/v3"


def refresh_token():
    r = requests.post("https://signin.tradestation.com/oauth/token", data={
        "grant_type": "refresh_token",
        "client_id": os.environ["TS_CLIENT_ID"],
        "client_secret": os.environ["TS_CLIENT_SECRET"],
        "refresh_token": os.environ["TS_REFRESH_TOKEN"],
    }, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def ts_get(p, t):
    r = requests.get(TS_BASE + p, headers={"Authorization": f"Bearer {t}"}, timeout=10)
    return r.json() if r.text else None


token = refresh_token()

print("=== OPEN POSITION STATE (lid=3057) ===")
c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()
cur.execute("SELECT setup_log_id, state, created_at, updated_at FROM real_trade_orders WHERE setup_log_id = 3057")
row = cur.fetchone()
if row:
    sid, state, created, updated = row
    if isinstance(state, str): state = json.loads(state)
    print(f"  setup_log_id:    {sid}")
    print(f"  created_at:      {created}")
    print(f"  updated_at:      {updated}")
    for k in ("setup_name","direction","account_id","atomic_bracket","quantity",
              "status","fill_price","signal_es_price","current_stop","target_price",
              "trail_only","trail_active","be_triggered","max_favorable",
              "entry_order_id","stop_order_id","target_order_id"):
        print(f"  {k:<18}{state.get(k)}")

print("\n=== SHORTS ACCT POSITION + ORDERS at TS ===")
pos = ts_get("/brokerage/accounts/210VYX91/positions", token)
print("Positions:", json.dumps(pos, indent=2, default=str))
orders = ts_get("/brokerage/accounts/210VYX91/orders", token)
if orders:
    open_orders = [o for o in orders.get("Orders", []) if o.get("Status") not in ("FLL","CAN","REJ","BRC","BRO","UCN")]
    print(f"\nOpen orders ({len(open_orders)}):")
    for o in open_orders:
        print(f"  oid={o.get('OrderID')} {o.get('Symbol')} {o.get('OrderType')} qty={o.get('Quantity')} "
              f"action={o.get('LegFillAction') or (o.get('Legs') or [{}])[0].get('BuyOrSell')} "
              f"price={o.get('LimitPrice') or o.get('StopPrice')} status={o.get('Status')}")

print("\n=== WEIRD: lid=3033 ES Abs bullish, close=7418.75 vs fill=7378.75 (close way ABOVE fill) ===")
cur.execute("SELECT state FROM real_trade_orders WHERE setup_log_id = 3033")
row = cur.fetchone()
if row:
    state = row[0]
    if isinstance(state, str): state = json.loads(state)
    print(json.dumps(state, indent=2, default=str))

print("\n=== WEIRD: lid=3039 bullish [ATOMIC], close=7416.5 vs fill=7420.25 (close BELOW fill on long, but reason=WIN) ===")
cur.execute("SELECT state FROM real_trade_orders WHERE setup_log_id = 3039")
row = cur.fetchone()
if row:
    state = row[0]
    if isinstance(state, str): state = json.loads(state)
    print(json.dumps(state, indent=2, default=str))

print("\n=== LONGS ACCT: any open orders? (acct showed FLAT but checking for stranded orders) ===")
orders = ts_get("/brokerage/accounts/210VYX65/orders", token)
if orders:
    open_orders = [o for o in orders.get("Orders", []) if o.get("Status") not in ("FLL","CAN","REJ","BRC","BRO","UCN")]
    print(f"Open orders on LONGS: {len(open_orders)}")
    for o in open_orders:
        print(f"  oid={o.get('OrderID')} {o.get('Symbol')} {o.get('OrderType')} qty={o.get('Quantity')} "
              f"price={o.get('LimitPrice') or o.get('StopPrice')} status={o.get('Status')}")

cur.close(); c.close()
