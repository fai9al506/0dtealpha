import os,requests
cid=os.environ['TS_CLIENT_ID'];sec=os.environ['TS_CLIENT_SECRET'];rt=os.environ['TS_REFRESH_TOKEN']
tok=requests.post('https://signin.tradestation.com/oauth/token',data={'grant_type':'refresh_token','client_id':cid,'client_secret':sec,'refresh_token':rt},timeout=20).json().get('access_token')
b=requests.get('https://api.tradestation.com/v3/brokerage/accounts/210VYX65,210VYX91/balances',headers={'Authorization':f'Bearer {tok}'},timeout=20).json()
mon={'210VYX65':1763.00,'210VYX91':2996.92}
tot=0
for a in b.get('Balances',[]):
    aid=a.get('AccountID');eq=float(a.get('Equity',0));nm='LONGS' if aid=='210VYX65' else 'SHORTS'
    print(f'{nm} {aid}: equity now ${eq:,.2f}  vs Mon-close ${mon[aid]:,.2f}  = Tue day ${eq-mon[aid]:+,.2f}');tot+=eq-mon[aid]
print(f'TOTAL Tue day (broker truth): ${tot:+,.2f}')
