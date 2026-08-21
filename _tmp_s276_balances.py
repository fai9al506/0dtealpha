import os, requests
cid=os.environ["TS_CLIENT_ID"]; cs=os.environ["TS_CLIENT_SECRET"]; rt=os.environ["TS_REFRESH_TOKEN"]
r=requests.post("https://signin.tradestation.com/oauth/token",
    data={"grant_type":"refresh_token","client_id":cid,"client_secret":cs,"refresh_token":rt},timeout=30)
tok=r.json().get("access_token")
print("token ok:", bool(tok))
for a in ["210VYX65","210VYX91","210XFR64"]:
    try:
        d=requests.get(f"https://api.tradestation.com/v3/brokerage/accounts/{a}/balances",
            headers={"Authorization":f"Bearer {tok}"},timeout=30).json()
        b=d.get("Balances") or []
        b=b[0] if isinstance(b,list) and b else (b if isinstance(b,dict) else {})
        bod=b.get("BalanceDetail") or {}
        print(f"  {a}: equity={b.get('Equity')} cash={b.get('CashBalance')} "
              f"bp={b.get('BuyingPower')} mktval={b.get('MarketValue')} "
              f"initmargin={bod.get('InitialMargin')} daytrademargin={bod.get('DayTradeMargin')} "
              f"errors={d.get('Errors')}")
    except Exception as e:
        print(f"  {a}: ERR {str(e)[:120]}")
