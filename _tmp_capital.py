import os, psycopg, json
from datetime import date
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
START=4896.99; ERA=date(2026,5,19)
# statement table (FIFO-reconciled broker truth) since era
cur.execute("SELECT day,net FROM tsrt_daily_stmt WHERE day>=%s ORDER BY day",(ERA,))
stmt=cur.fetchall()
stmt_net=sum(float(n) for _,n in stmt)
last_stmt_day=max(d for d,_ in stmt)
print(f"Statement table: {len(stmt)} days, {stmt[0][0]} -> {last_stmt_day}, net sum {stmt_net:+.2f}")
# days AFTER last_stmt_day, computed from real fills (gross - $1/RT)
cur.execute("""SELECT s.ts::date d, s.direction, r.state FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id=s.id
  WHERE s.ts::date > %s AND (r.state->>'fill_price') IS NOT NULL ORDER BY s.ts""",(last_stmt_day,))
from collections import defaultdict
days=defaultdict(lambda:[0.0,0])
for d,dirn,st in cur.fetchall():
    s=st if isinstance(st,dict) else json.loads(st)
    e=s.get("fill_price"); x=s.get("close_fill_price") or s.get("stop_fill_price")
    if not e or not x: continue
    lng=str(dirn).lower() in ("long","bullish")
    g=((x-e) if lng else (e-x))*5
    days[d][0]+=g; days[d][1]+=1
recent_net=0.0
print(f"\nDays not yet in statement table (computed gross - $1/RT):")
for d in sorted(days):
    g,n=days[d]; net=g-n*1
    recent_net+=net
    print(f"  {d}: gross ${g:+.0f}  comm ${n}  net ${net:+.0f}  ({n} RT)")
cap = START + stmt_net + recent_net
print(f"\n  era start (2026-05-19): ${START:,.2f}")
print(f"  + statement net (thru {last_stmt_day}): ${stmt_net:+,.2f}")
print(f"  + recent computed net: ${recent_net:+,.2f}")
print(f"  = CURRENT CAPITAL ≈ ${cap:,.2f}")
# 3-day drawdown
last3=sorted(days)[-3:]
dd=sum(days[d][0]-days[d][1] for d in last3)
print(f"\n  last 3 days ({last3[0]}->{last3[-1]}) net: ${dd:+.0f}")
