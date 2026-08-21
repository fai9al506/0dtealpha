import os, requests
cid=os.environ["TS_CLIENT_ID"]; sec=os.environ["TS_CLIENT_SECRET"]; rt=os.environ["TS_REFRESH_TOKEN"]
tok=requests.post("https://signin.tradestation.com/oauth/token",data={"grant_type":"refresh_token","client_id":cid,"client_secret":sec,"refresh_token":rt},timeout=20).json().get("access_token")
h={"Authorization":f"Bearer {tok}"}
for acct in ["210VYX65","210VYX91"]:
    pos=requests.get(f"https://api.tradestation.com/v3/brokerage/accounts/{acct}/positions",headers=h,timeout=20).json().get("Positions",[])
    print(f"\n{acct} POSITIONS: {len(pos)}")
    for p in pos: print(f"  {p.get('Symbol')} {p.get('LongShort')} qty={p.get('Quantity')} avg={p.get('AveragePrice')} openPnL={p.get('UnrealizedProfitLoss')}")
    orders=requests.get(f"https://api.tradestation.com/v3/brokerage/accounts/{acct}/orders",headers=h,timeout=20).json().get("Orders",[])
    working=[o for o in orders if o.get("StatusDescription") in ("Received","Queued","Open") or o.get("Status") in ("ACK","OPN")]
    print(f"  WORKING ORDERS: {len(working)}")
    for o in working:
        leg=(o.get('Legs') or [{}])[0]
        print(f"  oid={o.get('OrderID')} {o.get('OrderType')} {leg.get('BuyOrSell')} qty{leg.get('QuantityOrdered')} {leg.get('Symbol')} stop={o.get('StopPrice')} limit={o.get('LimitPrice')} status={o.get('StatusDescription')}")
