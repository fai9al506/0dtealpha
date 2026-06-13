import os, requests
AUTH="https://signin.tradestation.com"; BASE="https://api.tradestation.com/v3"
CID,SEC,RTOK=os.getenv("TS_CLIENT_ID"),os.getenv("TS_CLIENT_SECRET"),os.getenv("TS_REFRESH_TOKEN")
TOK=requests.post(f"{AUTH}/oauth/token",data={"grant_type":"refresh_token","refresh_token":RTOK,"client_id":CID,"client_secret":SEC,"scope":"openid profile MarketData ReadAccount Trade OptionSpreads offline_access"},timeout=20).json()["access_token"]
H={"Authorization":f"Bearer {TOK}"}
for acct in ("210VYX65","210VYX91"):
    b=requests.get(f"{BASE}/brokerage/accounts/{acct}/balances",headers=H,timeout=15).json()
    for bal in b.get("Balances",[]):
        d=bal.get("BalanceDetail",{})
        print(f"{acct}: RealizedPnL={d.get('RealizedProfitLoss')} Unrealized={d.get('UnrealizedProfitLoss')} Cash={bal.get('CashBalance')}")
    pos=requests.get(f"{BASE}/brokerage/accounts/{acct}/positions",headers=H,timeout=15).json().get("Positions",[])
    print(f"   open: {'FLAT' if not pos else [(p['Symbol'],p['Quantity'],p['LongShort']) for p in pos]}")
