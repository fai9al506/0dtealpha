"""1) Stop-loss blowout detail: nominal stop_pts vs actual fill (slippage) by month.
   2) Wrong-side-of-trend rate by month (shorts on up days, longs on down days).
   3) Accurate intraday breaker test: per-day min cumulative (does tighter breaker hit winners?).
   4) Realtime size-down: VIX-at-open + rolling-range (no look-ahead).
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','DD Exhaustion')
def fields(state,direction):
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    spct=st.get('stop_pts'); cr=str(st.get('close_reason'))
    if f is None or e is None: return None
    f,e=float(f),float(e)
    pts=(e-f) if direction in ('long','bullish') else (f-e)
    return {'pts':pts,'fill':f,'exit':e,'stop_pts':spct,'cr':cr}

# 1) stop slippage: for stop_filled trades, actual loss vs nominal -stop_pts
print("=== 1) stop_filled: nominal vs actual (slippage) by month ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name IN %s AND (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-04-15'
   ORDER BY sl.ts""",(WL,))
mo=defaultdict(lambda:{'n':0,'nom':0.0,'act':0.0})
for d,direction,state in cur.fetchall():
    fl=fields(state,direction)
    if not fl or fl['cr']!='stop_filled': continue
    m=str(d)[:7]; a=mo[m]; a['n']+=1
    a['act']+=fl['pts']
    if fl['stop_pts'] is not None: a['nom']+= -abs(float(fl['stop_pts']))
for m in sorted(mo):
    a=mo[m]
    print(f"  {m}: stops={a['n']:>3}  nominal_sum={a['nom']:>+7.1f}  actual_sum={a['act']:>+7.1f}  "
          f"slippage={a['act']-a['nom']:>+7.1f}  (avg_actual={a['act']/a['n']:.1f})")

# 2) wrong-side-of-trend: day net (close-open). long aligned if day up; short aligned if day down.
cur.execute("""WITH d AS (SELECT (ts AT TIME ZONE 'America/New_York')::date dd, spot,
                  ts AT TIME ZONE 'America/New_York' et FROM chain_snapshots
                  WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::date>='2026-04-15')
   SELECT dd,(array_agg(spot ORDER BY et DESC))[1]-(array_agg(spot ORDER BY et))[1] FROM d GROUP BY dd""")
trend={str(dd):float(v or 0) for dd,v in cur.fetchall()}
print("\n=== 2) wrong-side-of-day-trend rate + P&L by month ===")
cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York')::date d, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name IN %s AND (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-04-15'
   ORDER BY sl.ts""",(WL,))
ws=defaultdict(lambda:{'right_n':0,'right_pts':0.0,'wrong_n':0,'wrong_pts':0.0})
for d,direction,state in cur.fetchall():
    fl=fields(state,direction)
    if not fl: continue
    ds=str(d); m=ds[:7]; tr=trend.get(ds,0)
    is_long = direction in ('long','bullish')
    aligned = (is_long and tr>0) or ((not is_long) and tr<0)
    a=ws[m]
    if abs(tr)<10: continue  # skip flat days (no clear trend)
    if aligned: a['right_n']+=1; a['right_pts']+=fl['pts']
    else: a['wrong_n']+=1; a['wrong_pts']+=fl['pts']
for m in sorted(ws):
    a=ws[m]; tot=a['right_n']+a['wrong_n']
    wrate=a['wrong_n']/tot*100 if tot else 0
    print(f"  {m}: wrong-side {a['wrong_n']:>3}/{tot} ({wrate:>3.0f}%) wrong_pts={a['wrong_pts']:>+7.1f} | right {a['right_n']:>3} right_pts={a['right_pts']:>+7.1f}")

# 3) accurate intraday min cumulative per day (broker FIFO trades by exit_et)
print("\n=== 3) intraday min-cumulative per day (does -150/-200 breaker hit winners?) ===")
cur.execute("""SELECT day, net, trades FROM tsrt_daily_stmt WHERE day>='2026-05-19' ORDER BY day""")
for day,net,trades in cur.fetchall():
    t=trades if isinstance(trades,list) else (json.loads(trades) if trades else [])
    t=sorted(t,key=lambda x:x.get('exit_et') or '')
    cum=0.0; mn=0.0
    for it in t: cum+=float(it.get('usd') or 0); mn=min(mn,cum)
    net=float(net or 0)
    flag=''
    if net>0 and mn<=-150: flag=' <WINNER dips below -150!'
    elif net>0 and mn<=-100: flag=' <winner dipped below -100'
    print(f"  {str(day)} final={net:>+8.1f}  intraday_min={mn:>+8.1f}{flag}")
conn.close()
