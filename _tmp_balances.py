import os, requests
cid=os.environ["TS_CLIENT_ID"]; sec=os.environ["TS_CLIENT_SECRET"]; rt=os.environ["TS_REFRESH_TOKEN"]
r=requests.post("https://signin.tradestation.com/oauth/token",
   data={"grant_type":"refresh_token","client_id":cid,"client_secret":sec,"refresh_token":rt},timeout=20)
tok=r.json().get("access_token")
if not tok: print("TOKEN ERR", r.status_code, str(r.json())[:200]); raise SystemExit
h={"Authorization":f"Bearer {tok}"}
b=requests.get("https://api.tradestation.com/v3/brokerage/accounts/210VYX65,210VYX91/balances",headers=h,timeout=20).json()
for acc in b.get("Balances",[]):
    d=acc.get("BalanceDetail",{})
    nm = "LONGS" if acc.get("AccountID")=="210VYX65" else "SHORTS"
    print(f"{nm} {acc.get('AccountID')}:  Equity=${float(acc.get('Equity',0)):,.2f}  CashBalance=${float(acc.get('CashBalance',0)):,.2f}  BuyingPower=${float(acc.get('BuyingPower',0)):,.2f}")
    print(f"    InitialMargin=${float(d.get('InitialMargin',0)):,.2f}  DayTradeMargin=${float(d.get('DayTradeMargin',0)):,.2f}  RealizedPnL=${float(d.get('RealizedProfitLoss',0)):,.2f}")
