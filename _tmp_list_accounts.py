"""List all TradeStation accounts (live + sim)"""
import os, requests

CID = os.getenv("TS_CLIENT_ID", "")
SECRET = os.getenv("TS_CLIENT_SECRET", "")
RTOKEN = os.getenv("TS_REFRESH_TOKEN", "")

if not all([CID, SECRET, RTOKEN]):
    print("Missing TS env vars")
    exit(1)

# Get access token
r = requests.post("https://signin.tradestation.com/oauth/token", data={
    "grant_type": "refresh_token",
    "refresh_token": RTOKEN,
    "client_id": CID,
    "client_secret": SECRET,
}, timeout=15)
tok = r.json()
access_token = tok["access_token"]
print(f"Auth OK (expires in {tok.get('expires_in')}s)\n")

headers = {"Authorization": f"Bearer {access_token}"}

# List accounts on LIVE API
print("=== LIVE API accounts ===")
r = requests.get("https://api.tradestation.com/v3/brokerage/accounts",
                  headers=headers, timeout=10)
if r.status_code == 200:
    accounts = r.json().get("Accounts", [])
    for a in accounts:
        print(f"  AccountID: {a.get('AccountID')}  Name: {a.get('Name')}  "
              f"Type: {a.get('AccountType')}  Status: {a.get('Status')}  "
              f"Currency: {a.get('Currency')}")
        # Check account details
        aid = a.get('AccountID')
        r2 = requests.get(f"https://api.tradestation.com/v3/brokerage/accounts/{aid}/balances",
                          headers=headers, timeout=10)
        if r2.status_code == 200:
            bal = r2.json().get("Balances", [])
            for b in bal:
                print(f"    Balance: CashBalance={b.get('CashBalance')} "
                      f"MarketValue={b.get('MarketValue')} "
                      f"AccountType={b.get('AccountType')}")
    if not accounts:
        print("  (no accounts found)")
else:
    print(f"  Error: {r.status_code} {r.text[:200]}")

# List accounts on SIM API
print("\n=== SIM API accounts ===")
r = requests.get("https://sim-api.tradestation.com/v3/brokerage/accounts",
                  headers=headers, timeout=10)
if r.status_code == 200:
    accounts = r.json().get("Accounts", [])
    for a in accounts:
        print(f"  AccountID: {a.get('AccountID')}  Name: {a.get('Name')}  "
              f"Type: {a.get('AccountType')}  Status: {a.get('Status')}  "
              f"Currency: {a.get('Currency')}")
    if not accounts:
        print("  (no accounts found)")
else:
    print(f"  Error: {r.status_code} {r.text[:200]}")
