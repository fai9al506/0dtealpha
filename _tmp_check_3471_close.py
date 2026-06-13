"""Confirm 210VYX65 flat + how lid 3471 closed + final day realized P&L."""
import os, requests, json
AUTH="https://signin.tradestation.com"; BASE="https://api.tradestation.com/v3"; ACCT="210VYX65"
CID,SEC,RTOK=os.getenv("TS_CLIENT_ID"),os.getenv("TS_CLIENT_SECRET"),os.getenv("TS_REFRESH_TOKEN")
TOK=requests.post(f"{AUTH}/oauth/token",data={"grant_type":"refresh_token","refresh_token":RTOK,"client_id":CID,"client_secret":SEC,"scope":"openid profile MarketData ReadAccount Trade OptionSpreads offline_access"},timeout=20).json()["access_token"]
H={"Authorization":f"Bearer {TOK}"}

p=requests.get(f"{BASE}/brokerage/accounts/{ACCT}/positions",headers=H,timeout=15).json()
pos=p.get("Positions",[])
print("POSITIONS:", "FLAT ✓" if not pos else pos)

b=requests.get(f"{BASE}/brokerage/accounts/{ACCT}/balances",headers=H,timeout=15).json()
for bal in b.get("Balances",[]):
    d=bal.get("BalanceDetail",{})
    print(f"Realized day P&L: {d.get('RealizedProfitLoss')}  Cash: {bal.get('CashBalance')}")

# 3471 from DB
import psycopg2
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
cur=psycopg2.connect(DB).cursor()
cur.execute("SELECT state->>'status', state->>'fill_price', state->>'close_fill_price', state->>'close_reason', updated_at FROM real_trade_orders WHERE setup_log_id=3471")
r=cur.fetchone()
if r:
    print(f"lid 3471: status={r[0]} entry={r[1]} close={r[2]} reason={r[3]} updated={r[4]}")
