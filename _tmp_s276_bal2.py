import os, requests
cid=os.environ["TS_CLIENT_ID"]; cs=os.environ["TS_CLIENT_SECRET"]; rt=os.environ["TS_REFRESH_TOKEN"]
tok=requests.post("https://signin.tradestation.com/oauth/token",
    data={"grant_type":"refresh_token","client_id":cid,"client_secret":cs,"refresh_token":rt},timeout=30).json()["access_token"]
for a in ["210FALDE"]:
    d=requests.get(f"https://api.tradestation.com/v3/brokerage/accounts/{a}/balances",
        headers={"Authorization":f"Bearer {tok}"},timeout=30).json()
    b=(d.get("Balances") or [{}])[0]
    bd=b.get("BalanceDetail") or {}
    print(f"{a}: equity={b.get('Equity')} cash={b.get('CashBalance')} bp={b.get('BuyingPower')} "
          f"realized={b.get('TodaysProfitLoss')} initmargin={bd.get('InitialMargin')}")
    p=requests.get(f"https://api.tradestation.com/v3/brokerage/accounts/{a}/positions",
        headers={"Authorization":f"Bearer {tok}"},timeout=30).json()
    print(f"   positions: {len(p.get('Positions') or [])}")
