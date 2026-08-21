import os, requests, json
cid=os.environ["TS_CLIENT_ID"]; cs=os.environ["TS_CLIENT_SECRET"]; rt=os.environ["TS_REFRESH_TOKEN"]
tok=requests.post("https://signin.tradestation.com/oauth/token",
    data={"grant_type":"refresh_token","client_id":cid,"client_secret":cs,"refresh_token":rt},timeout=30).json()["access_token"]
d=requests.get("https://api.tradestation.com/v3/brokerage/accounts",
    headers={"Authorization":f"Bearer {tok}"},timeout=30).json()
for a in d.get("Accounts",[]):
    print(f"  {a.get('AccountID')}  type={a.get('AccountType')}  status={a.get('Status')}  alias={a.get('Alias')}")
print("errors:", d.get("Errors"))
r=requests.get("https://api.tradestation.com/v3/brokerage/accounts/210XFR64/balances",
    headers={"Authorization":f"Bearer {tok}"},timeout=30)
print("210XFR64 raw:", r.status_code, r.text[:400])
