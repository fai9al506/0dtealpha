import os, json, psycopg2, requests
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
cur.execute("""SELECT rto.setup_log_id, rto.state, sl.setup_name, sl.direction
   FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
   WHERE date(sl.ts AT TIME ZONE 'America/New_York')='2026-06-29' ORDER BY sl.ts""")
print("=== per-lid (pre-FIFO own-exit) ===")
tot=0
for sid,st,nm,dr in cur.fetchall():
    s=st if isinstance(st,dict) else json.loads(st); il=dr in ('long','bullish')
    en=s.get('fill_price'); ex=s.get('close_fill_price_pre_fifo_reconcile') or s.get('close_fill_price'); q=s.get('quantity') or 1
    pnl=((ex-en) if il else (en-ex))*q*5 if (en and ex) else None
    acct=s.get('account_id','')[-4:]
    print(f"  lid {sid} {nm:<14} {dr:<8} {acct} entry={en} exit={ex} qty={q} -> ${pnl:+.1f}" if pnl is not None else f"  lid {sid} {nm} OPEN/incomplete")
    if pnl: tot+=pnl
print(f"  per-lid total: ${tot:+.1f}")
c.close()
# live balances vs this morning (longs $1664.20 / shorts $3061.72)
cid=os.environ["TS_CLIENT_ID"]; sec=os.environ["TS_CLIENT_SECRET"]; rt=os.environ["TS_REFRESH_TOKEN"]
tok=requests.post("https://signin.tradestation.com/oauth/token",data={"grant_type":"refresh_token","client_id":cid,"client_secret":sec,"refresh_token":rt},timeout=20).json().get("access_token")
b=requests.get("https://api.tradestation.com/v3/brokerage/accounts/210VYX65,210VYX91/balances",headers={"Authorization":f"Bearer {tok}"},timeout=20).json()
print("\n=== live broker equity vs this AM ===")
am={"210VYX65":1664.20,"210VYX91":3061.72}
for acc in b.get("Balances",[]):
    aid=acc.get("AccountID"); eq=float(acc.get("Equity",0)); d=acc.get("BalanceDetail",{})
    nm="LONGS" if aid=="210VYX65" else "SHORTS"
    print(f"  {nm} {aid}: equity ${eq:,.2f}  (AM ${am[aid]:,.2f}, day {eq-am[aid]:+.2f})  openPnL=${float(d.get('UnrealizedProfitLoss',0) or 0):,.2f}  DayMargin=${float(d.get('DayTradeMargin',0)):,.0f}")
