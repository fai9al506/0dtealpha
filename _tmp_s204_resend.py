"""S204 one-off resend: run the production weekly statement path with the REAL
Telegram send (now routed to the Trades channel). Intended to be run ON RAILWAY
via `railway ssh -s 0dtealpha "python _tmp_s204_resend.py"` — local ISP blocks
api.telegram.org. Idempotent: tsrt_daily_stmt rows are upserted.
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


print(f"target chat: {wr.TEL_RES_CHAT}")
engine = create_engine(os.environ['DATABASE_URL'], pool_pre_ping=True)
wr.init(engine, get_token)
wr.run_weekly()
print('resend done')
