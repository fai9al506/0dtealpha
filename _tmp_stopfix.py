"""Validate the two Leak-#2 fixes on ALL post-V16 stop_filled trades, using the
REAL ES path (vps_es_range_bars) for ES adverse excursion and chain for SPX.

Fix1 = safety stop triggers on SPX (smooth) crossing the SPX-equiv stop level.
Fix2 = widen the ES safety stop by +4 pts (e.g. 14->18).
For a stop that 'survives' (wasn't really hit), counterfactual outcome = portal P&L.
For a genuine reversal (adverse >= stop), Fix2 costs the extra width.
"""
import os, sys, json, psycopg2
import pandas as pd
from datetime import timedelta
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'], df['P&L']))
dur=dict(zip(df['ID'], df['Duration (min)']))
conn=psycopg2.connect(os.environ['DATABASE_URL']); cur=conn.cursor()

cur.execute("""SELECT sl.id, sl.ts, sl.spot, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
trades=[]
for sid,ts,spot,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    if str(st.get('close_reason'))!='stop_filled': continue
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None or spot is None: continue
    trades.append({'id':sid,'ts':ts,'spx':float(spot),'es':float(f),'dir':direction,
                   'broker':((float(e)-float(f)) if direction in('long','bullish') else (float(f)-float(e))),
                   'stop_pts':float(st.get('stop_pts') or 14),'portal':float(port.get(sid,0)),
                   'dur':float(dur.get(sid,30) or 30)})

def es_mae(ts,dur_min,entry,is_long):
    end=ts+timedelta(minutes=max(dur_min,3)+2)
    cur.execute("""SELECT min(bar_low),max(bar_high) FROM vps_es_range_bars
       WHERE symbol='@ES' AND range_pts=5 AND ts_start>=%s AND ts_start<=%s""",(ts,end))
    lo,hi=cur.fetchone()
    if lo is None: return None
    return (entry-float(lo)) if is_long else (float(hi)-entry)  # adverse pts

def spx_mae(ts,dur_min,entry,is_long):
    end=ts+timedelta(minutes=max(dur_min,3)+2)
    cur.execute("""SELECT min(spot),max(spot) FROM chain_snapshots WHERE spot IS NOT NULL
       AND ts>=%s AND ts<=%s""",(ts,end))
    lo,hi=cur.fetchone()
    if lo is None: return None
    return (entry-float(lo)) if is_long else (float(hi)-entry)

base=fix1=fix2=0.0
recov1=recov2=cost2=0
for t in trades:
    is_long=t['dir'] in ('long','bullish')
    emae=es_mae(t['ts'],t['dur'],t['es'],is_long)
    smae=spx_mae(t['ts'],t['dur'],t['spx'],is_long)
    base+=t['broker']
    # Fix1: SPX-based — survive if SPX adverse < stop_pts
    if smae is not None and smae < t['stop_pts']:
        fix1+=t['portal']; recov1+=1
    else:
        fix1+=t['broker']
    # Fix2: +4 wider ES stop
    if emae is not None and emae < t['stop_pts']+4:
        fix2+=t['portal']; recov2+=1
    else:
        fix2+= -(t['stop_pts']+4); cost2+=1   # genuine stop now costs the wider width

print(f"post-V16 stop_filled trades: {len(trades)}")
print(f"  BASELINE stop P&L        = {base:+.1f} pts (${base*5:+.0f})")
print(f"  FIX1 SPX-based stop       = {fix1:+.1f} pts (${fix1*5:+.0f})   recovered {recov1} wick-stops  Δ${ (fix1-base)*5:+.0f}")
print(f"  FIX2 +4 wider ES stop     = {fix2:+.1f} pts (${fix2*5:+.0f})   recovered {recov2}, {cost2} genuine stops now wider  Δ${ (fix2-base)*5:+.0f}")
conn.close()
