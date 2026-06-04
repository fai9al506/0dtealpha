import os, requests
r = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
}, timeout=30)
token = r.json()['access_token']
H = {'Authorization': f'Bearer {token}'}
B = 'https://api.tradestation.com/v3'
for acct in ['210VYX65', '210VYX91']:
    b = requests.get(f'{B}/brokerage/accounts/{acct}/balances', headers=H, timeout=15).json()['Balances'][0]
    d = b.get('BalanceDetail', {})
    print(acct, 'equity=', b.get('Equity'), 'realized_today=', d.get('RealizedProfitLoss'),
          'unrealized=', d.get('UnrealizedProfitLoss'), 'todays=', b.get('TodaysProfitLoss'))
    o = requests.get(f'{B}/brokerage/accounts/{acct}/orders?pageSize=20', headers=H, timeout=15).json().get('Orders', [])
    fll = [x for x in o if x.get('Status') in ('FLL', 'FLP')]
    print('  today filled orders:', len(fll))
    for x in fll[:6]:
        leg = x.get('Legs', [{}])[0]
        print('   ', x.get('ClosedDateTime'), leg.get('BuyOrSell'), leg.get('ExecQuantity'), '@', leg.get('ExecutionPrice'))
