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

# 1) positions
pos=requests.get(f"{BASE}/brokerage/accounts/{ACCT}/positions",headers=H,timeout=20).json()
print("=== POSITIONS ===")
net_qty = 0
for p in pos.get("Positions",[]):
    q = int(float(p.get("Quantity",0)))
    side = p.get("LongShort")
    if side == "Short": net_qty -= abs(q)
    else: net_qty += abs(q)
    print(f"{p.get('Symbol')} {side} qty={q} avg={p.get('AveragePrice')} unrealPL={p.get('UnrealizedProfitLoss')}")
if not pos.get("Positions"): print("(none)")
print(f"NET MES QTY: {net_qty}")

# 2) open/active orders
oo=requests.get(f"{BASE}/brokerage/accounts/{ACCT}/orders",headers=H,timeout=20).json()
print("\n=== ACTIVE ORDERS (Received/Queued/Open) ===")
active=[]
for o in oo.get("Orders",[]):
    st=o.get("Status")
    leg=(o.get("Legs") or [{}])[0]
    if "MES" not in leg.get("Symbol","").upper(): continue
    if st in ("ACK","REC","DON","OPN","QUE","RCV"):  # broad
        active.append((o,leg))
for o,leg in active:
    print(f"OID={o.get('OrderID')} opened={et(o.get('OpenedDateTime'))} {leg.get('BuyOrSell')} qty={leg.get('QuantityOrdered')} type={o.get('OrderType')} status={o.get('Status')} ({o.get('StatusDescription')}) stop={o.get('StopPrice') or '-'} limit={o.get('LimitPrice') or '-'}")
if not active: print("(none)")

# also dump today's all MES orders short
print("\n=== ALL MES ORDERS TODAY ===")
rows=[]
for o in oo.get("Orders",[]):
    leg=(o.get("Legs") or [{}])[0]
    if "MES" not in leg.get("Symbol","").upper(): continue
    rows.append((o.get("OrderID"),o,leg))
rows.sort(key=lambda x:x[0])
for oid,o,leg in rows:
    fp=leg.get("ExecutionPrice") or o.get("FilledPrice") or "-"
    print(f"{oid} {et(o.get('OpenedDateTime')):>8} {et(o.get('ClosedDateTime')):>8} {leg.get('BuyOrSell'):4s} q={leg.get('ExecQuantity') or leg.get('QuantityOrdered')} {o.get('OrderType'):11s} {o.get('Status'):4s} fill={str(fp):>9} stop={o.get('StopPrice') or '-'}")
