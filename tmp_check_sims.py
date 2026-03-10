"""Check actual TS SIM account status via API"""
import os, requests, json

# Get fresh token via OAuth
CID = os.environ.get("TS_CLIENT_ID", "")
CSEC = os.environ.get("TS_CLIENT_SECRET", "")
RTOK = os.environ.get("TS_REFRESH_TOKEN", "")

r = requests.post("https://signin.tradestation.com/oauth/token", data={
    "grant_type": "refresh_token",
    "client_id": CID,
    "client_secret": CSEC,
    "refresh_token": RTOK
})
if not r.ok:
    print(f"TOKEN ERROR: {r.status_code} {r.text[:200]}")
    exit()
token = r.json()["access_token"]
print("Token OK")

headers = {"Authorization": f"Bearer {token}"}
BASE = "https://sim-api.tradestation.com/v3"

for acct in ["SIM2609239F", "SIM2609238M"]:
    print(f"\n{'='*60}")
    print(f"ACCOUNT: {acct}")
    print(f"{'='*60}")

    # Balances - dump raw
    r = requests.get(f"{BASE}/brokerage/accounts/{acct}/balances", headers=headers)
    if r.ok:
        b = r.json()
        print("\n  BALANCES:")
        for k, v in sorted(b.items()):
            if isinstance(v, (int, float)) and v != 0:
                print(f"    {k}: {v}")
            elif isinstance(v, str) and v and k not in ('AccountID', 'AccountType', 'Currency'):
                print(f"    {k}: {v}")
    else:
        print(f"  Balance error: {r.status_code}")

    # Positions
    r2 = requests.get(f"{BASE}/brokerage/accounts/{acct}/positions", headers=headers)
    if r2.ok:
        pos = r2.json()
        if isinstance(pos, list) and pos:
            print(f"\n  POSITIONS:")
            for p in pos:
                print(f"    {json.dumps(p, indent=4)[:300]}")
        elif isinstance(pos, dict) and pos.get('Positions'):
            print(f"\n  POSITIONS:")
            for p in pos['Positions']:
                print(f"    {json.dumps(p, indent=4)[:300]}")
        else:
            print(f"\n  No positions")

    # Orders - dump with full field names
    r3 = requests.get(f"{BASE}/brokerage/accounts/{acct}/orders", headers=headers)
    if r3.ok:
        data = r3.json()
        orders = data.get('Orders', data) if isinstance(data, dict) else data
        if isinstance(orders, list) and orders:
            # Print first order raw to see field names
            print(f"\n  SAMPLE ORDER KEYS: {list(orders[0].keys())}")
            print(f"\n  ORDERS ({len(orders)} total, showing last 25):")
            for o in orders[-25:]:
                legs = o.get('Legs', [{}])
                leg = legs[0] if legs else {}
                sym = leg.get('Symbol', o.get('Symbol', '?'))
                bs = leg.get('BuyOrSell', o.get('BuyOrSell', '?'))
                qty = leg.get('QuantityOrdered', o.get('Quantity', '?'))
                filled = o.get('FilledPrice', leg.get('ExecPrice', '?'))
                status = o.get('StatusDescription', o.get('Status', '?'))
                otype = o.get('OrderType', o.get('Type', '?'))
                opened = o.get('OpenedDateTime', '?')[:16]
                print(f"    {opened} {otype:12s} {bs:5s} {sym:15s} qty={qty} filled={filled} status={status}")
