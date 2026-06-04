"""S204 local test: run the production tsrt_weekly_report path end-to-end,
but intercept the Telegram send and write the HTML to disk instead
(local ISP blocks api.telegram.org; preview gets relay-sent separately).
Side effect (intended): pre-populates tsrt_daily_stmt before Friday's first cron.
"""
import os, requests
from sqlalchemy import create_engine
from app import tsrt_weekly_report as wr

def get_token():
    r = requests.post('https://signin.tradestation.com/oauth/token', data={
        'grant_type': 'refresh_token',
        'client_id': os.environ['TS_CLIENT_ID'],
        'client_secret': os.environ['TS_CLIENT_SECRET'],
        'refresh_token': os.environ['TS_REFRESH_TOKEN'],
    }, timeout=30)
    return r.json()['access_token']

captured = {}
def fake_send(html, fname, caption):
    with open(fname, 'w', encoding='utf-8') as f:
        f.write(html)
    captured['fname'] = fname
    captured['caption'] = caption
    print(f'CAPTURED: {fname} ({len(html)//1024} KB)')
    print(f'CAPTION:\n{caption}')
    return True

wr._send_document = fake_send
engine = create_engine(os.environ['DATABASE_URL'], pool_pre_ping=True)
wr.init(engine, get_token)
wr.run_weekly()
assert captured.get('fname'), 'report was not generated'
print('S204 local test PASSED')
