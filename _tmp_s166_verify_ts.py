"""Pull TS historicalorders for lid=3033 close_oid to verify true broker fill."""
import os, requests, json
from datetime import datetime, timezone, timedelta

# Get TS access token
r = requests.post(
    "https://signin.tradestation.com/oauth/token",
    data={
        "grant_type": "refresh_token",
        "client_id": os.environ["TS_CLIENT_ID"],
        "client_secret": os.environ["TS_CLIENT_SECRET"],
        "refresh_token": os.environ["TS_REFRESH_TOKEN"],
    },
    timeout=30,
)
TOKEN = r.json()["access_token"]
hdrs = {"Authorization": f"Bearer {TOKEN}"}

ACCT = "210VYX65"  # longs account
CLOSE_OID = "1266617981"  # lid=3033 close
ENTRY_OID = "1266598484"  # lid=3033 entry
STOP_OID = "1266598516"   # lid=3033 stop

# Pull historicalorders since 2026-05-19
since = "05-19-2026"
r = requests.get(
    f"https://api.tradestation.com/v3/brokerage/accounts/{ACCT}/historicalorders?since={since}&pageSize=600",
    headers=hdrs, timeout=30,
)
print(f"Status: {r.status_code}")
orders = r.json().get("Orders", [])
print(f"Got {len(orders)} historical orders\n")

TARGETS = {ENTRY_OID: "ENTRY", STOP_OID: "STOP", CLOSE_OID: "CLOSE"}
for o in orders:
    oid = o.get("OrderID")
    if oid in TARGETS:
        print(f"\n=== {TARGETS[oid]} (OID={oid}) ===")
        print(f"  Status: {o.get('Status')}")
        print(f"  OrderType: {o.get('OrderType')}")
        print(f"  Opened: {o.get('OpenedDateTime')}")
        print(f"  Closed: {o.get('ClosedDateTime')}")
        for leg in o.get("Legs", []):
            print(f"  Leg: BuyOrSell={leg.get('BuyOrSell')} QtyOrd={leg.get('QuantityOrdered')} QtyExec={leg.get('ExecQuantity')} ExecPrice={leg.get('ExecutionPrice')} Symbol={leg.get('Symbol')}")
