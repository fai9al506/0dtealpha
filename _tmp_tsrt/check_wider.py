"""Check historical orders with wider date window in case earlier trades exist."""
import os, requests, json
resp = requests.post('https://signin.tradestation.com/oauth/token', data={
    'grant_type': 'refresh_token',
    'client_id': os.environ['TS_CLIENT_ID'],
    'client_secret': os.environ['TS_CLIENT_SECRET'],
    'refresh_token': os.environ['TS_REFRESH_TOKEN'],
})
token = resp.json().get('access_token')
headers = {'Authorization': f'Bearer {token}'}
BASE = 'https://api.tradestation.com/v3'

for acct_id in ['210VYX65', '210VYX91']:
    since = '2026-01-23'
    url = f'{BASE}/brokerage/accounts/{acct_id}/historicalorders?since={since}&pageSize=600'
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        orders = r.json().get('Orders', [])
        earliest = min((o.get('OpenedDateTime', '') for o in orders), default='none')
        print(f'{acct_id}: {len(orders)} orders since {since}, earliest={earliest}')
    else:
        print(f'{acct_id}: HTTP {r.status_code} {r.text[:200]}')
