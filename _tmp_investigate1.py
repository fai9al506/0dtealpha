"""Re-investigate user challenges:
B) April 'real trades' — what are they? (user: no real trading until May)
F) Exit mechanism by month — did the safety-SL get hit more in June? (S131 capture)
A) per-day PORTAL vs BROKER in loss window — did portal win while broker lost?
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# inspect state keys + setup_log columns
cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='setup_log' ORDER BY ordinal_position""")
print("setup_log cols:", [r[0] for r in cur.fetchall()])
cur.execute("""SELECT rto.state FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date='2026-06-09' LIMIT 1""")
s=cur.fetchone()
if s:
    st=s[0] if isinstance(s[0],dict) else json.loads(s[0])
    print("\nstate keys:", list(st.keys()))

# B) April real trades — dates, accounts, what account_id
print("\n=== B) 'real' trades by month with account + earliest dates ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date < '2026-05-19' ORDER BY sl.ts""")
acct_month=defaultdict(lambda: defaultdict(int))
earliest={}
for d,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    acct=st.get('account') or st.get('account_id') or '?'
    m=str(d)[:7]
    acct_month[m][acct]+=1
    earliest.setdefault(acct, str(d))
for m in sorted(acct_month):
    print(f"  {m}: "+", ".join(f"{a}={n}" for a,n in acct_month[m].items()))
print("  earliest per account:", earliest)

# F) exit mechanism by month: count close_reason / how trade ended
print("\n=== F) exit mechanism by month (how did real trades close?) ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, rto.state, sl.close_reason, sl.outcome_result
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date >= '2026-04-15' ORDER BY sl.ts""")
mech=defaultdict(lambda: defaultdict(int))
for d,state,creason,res in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    m=str(d)[:7]
    # infer: did it close via stop_fill or close_fill?
    if st.get('stop_fill_price') is not None: how='STOP_hit'
    elif st.get('close_fill_price') is not None: how='portal_exit/market'
    else: how='other'
    mech[m][how]+=1
    mech[m][f"reason:{creason}"]+=0  # placeholder
for m in sorted(mech):
    d=mech[m]
    tot=d.get('STOP_hit',0)+d.get('portal_exit/market',0)+d.get('other',0)
    print(f"  {m}: STOP_hit={d.get('STOP_hit',0)} portal_exit/market={d.get('portal_exit/market',0)} other={d.get('other',0)}  (stop%={d.get('STOP_hit',0)/tot*100:.0f}%)")

# close_reason distribution by month
print("\n=== close_reason distribution by month ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, sl.close_reason, count(*)
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date >= '2026-04-15'
   GROUP BY d, sl.close_reason ORDER BY d""")
cr=defaultdict(lambda: defaultdict(int))
for d,reason,n in cur.fetchall():
    cr[str(d)[:7]][str(reason)]+=n
for m in sorted(cr):
    print(f"  {m}: "+", ".join(f"{k}={v}" for k,v in sorted(cr[m].items(), key=lambda x:-x[1])))
conn.close()
