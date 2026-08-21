import os, requests, json
cid=os.environ["TS_CLIENT_ID"]; cs=os.environ["TS_CLIENT_SECRET"]; rt=os.environ["TS_REFRESH_TOKEN"]
tok=requests.post("https://signin.tradestation.com/oauth/token",
    data={"grant_type":"refresh_token","client_id":cid,"client_secret":cs,"refresh_token":rt},timeout=30).json()["access_token"]
H={"Authorization":f"Bearer {tok}"}
d=requests.get("https://api.tradestation.com/v3/brokerage/accounts/210VYX65/positions",headers=H,timeout=30).json()
print("LONG ACCT positions:", [(p.get("Symbol"),p.get("Quantity"),p.get("AveragePrice"),p.get("LongShort")) for p in d.get("Positions") or []])
d2=requests.get("https://api.tradestation.com/v3/brokerage/accounts/210VYX91/positions",headers=H,timeout=30).json()
print("SHORT ACCT positions:", [(p.get("Symbol"),p.get("Quantity"),p.get("AveragePrice"),p.get("LongShort")) for p in d2.get("Positions") or []])
o=requests.get("https://api.tradestation.com/v3/brokerage/accounts/210VYX65/orders",headers=H,timeout=30).json()
for x in o.get("Orders") or []:
    print(f"  ORDER {x.get('OrderID')} {x.get('Status')} {x.get('StatusDescription')} "
          f"type={x.get('OrderType')} qty={x.get('Quantity')} filled={x.get('FilledQuantity')} "
          f"limit={x.get('LimitPrice')} stop={x.get('StopPrice')} sym={(x.get('Legs') or [{}])[0].get('Symbol')}")
