import os, requests, json
from datetime import datetime, timezone, timedelta
AUTH="https://signin.tradestation.com"; BASE="https://api.tradestation.com/v3"; ACCT="210VYX65"
CID,SEC,RTOK=os.getenv("TS_CLIENT_ID"),os.getenv("TS_CLIENT_SECRET"),os.getenv("TS_REFRESH_TOKEN")
TOK=requests.post(f"{AUTH}/oauth/token",data={"grant_type":"refresh_token","refresh_token":RTOK,"client_id":CID,"client_secret":SEC,"scope":"openid profile MarketData ReadAccount Trade OptionSpreads offline_access"},timeout=20).json()["access_token"]
H={"Authorization":f"Bearer {TOK}"}
def et(s):
    if not s: return ""
    try:
        dt=datetime.fromisoformat(s.replace("Z","+00:00")); return dt.astimezone(timezone(timedelta(hours=-4))).strftime("%H:%M:%S")
    except: return s
oo=requests.get(f"{BASE}/brokerage/accounts/{ACCT}/orders",headers=H,timeout=20).json()
rows=[]
for o in oo.get("Orders",[]):
    leg=(o.get("Legs") or [{}])[0]
    if "MES" not in leg.get("Symbol","").upper(): continue
    rows.append((o.get("OrderID"),o,leg))
rows.sort(key=lambda x:x[0])
print("OID            open     close    side qty type        status fill      stop")
for oid,o,leg in rows:
    fp=leg.get("ExecutionPrice") or o.get("FilledPrice") or "-"
    print(f"{oid} {et(o.get('OpenedDateTime')):>8} {et(o.get('ClosedDateTime')):>8} "
          f"{leg.get('BuyOrSell'):4s} {leg.get('ExecQuantity') or leg.get('QuantityOrdered')} "
          f"{o.get('OrderType'):11s} {o.get('Status'):4s} {str(fp):>9} {o.get('StopPrice') or '-'}")
