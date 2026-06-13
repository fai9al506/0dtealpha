"""FINAL combined counterfactual with the unified fix.
Fix S (SPX-based exits) = make BOTH initial stop and trailing exit trigger on SPX:
   - trail_market_exit trades -> ride portal
   - stop_filled trades where SPX never crossed the stop level -> ride portal
Fix Z (size-down) = halve when after 2 consecutive stops OR VIX-at-open>=19.
Split: WIN era (May19-Jun04) vs LOSS window (Jun05-12).
"""
import os, sys, json, psycopg2
import pandas as pd
from datetime import timedelta
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'], df['P&L'])); dur=dict(zip(df['ID'], df['Duration (min)']))
conn=psycopg2.connect(os.environ['DATABASE_URL']); cur=conn.cursor()
cur.execute("""SELECT sl.id, sl.ts,(sl.ts AT TIME ZONE 'America/New_York') et, sl.spot, sl.vix, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
T=[]
for sid,ts,et,spot,vix,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    is_long=direction in ('long','bullish')
    b=(float(e)-float(f)) if is_long else (float(f)-float(e))
    T.append({'id':sid,'ts':ts,'day':str(et)[:10],'spx':float(spot) if spot else None,
              'dir':direction,'is_long':is_long,'broker':b,'portal':float(port.get(sid,0)),
              'cr':str(st.get('close_reason')),'stop_pts':float(st.get('stop_pts') or 14),
              'dur':float(dur.get(sid,30) or 30),'vix':float(vix) if vix else None,
              'stop':str(st.get('close_reason'))=='stop_filled'})
firstvix={}
for t in T: firstvix.setdefault(t['day'], t['vix'])
def spx_mae(ts,d,entry,is_long):
    if entry is None: return None
    end=ts+timedelta(minutes=max(d,3)+2)
    cur.execute("SELECT min(spot),max(spot) FROM chain_snapshots WHERE spot IS NOT NULL AND ts>=%s AND ts<=%s",(ts,end))
    lo,hi=cur.fetchone()
    if lo is None: return None
    return (entry-float(lo)) if is_long else (float(hi)-entry)
# precompute spx survival for stop trades
for t in T:
    t['survives']=False
    if t['cr']=='stop_filled':
        m=spx_mae(t['ts'],t['dur'],t['spx'],t['is_long'])
        t['survives']= (m is not None and m < t['stop_pts'])

def val_base(t,c): return t['broker']
def val_S(t,c):
    if t['cr']=='outcome_close_trail_market_exit': return t['portal']
    if t['cr']=='stop_filled' and t['survives']: return t['portal']
    return t['broker']
def val_SZ(t,c):
    v=val_S(t,c)
    if c>=2 or (firstvix.get(t['day']) or 0)>=19: v*=0.5
    return v

def run(fn):
    win=loss=0.0; byday=defaultdict(list)
    for t in T: byday[t['day']].append(t)
    for day,trs in byday.items():
        c=0
        for t in trs:
            v=fn(t,c)*5
            if day>='2026-06-05': loss+=v
            else: win+=v
            c=c+1 if t['stop'] else 0
    return win,loss

print("Per-lid broker $ (size as traded). WIN era=May19-Jun04, LOSS window=Jun05-12\n")
print(f"{'scenario':44}{'WIN era':>10}{'LOSS window':>13}{'TOTAL':>9}")
for name,fn in [("BASELINE (what actually happened)",val_base),
                ("+ Fix S (all exits SPX-based: Leak#1+#2)",val_S),
                ("+ Fix S & size-down (Leak fixes + protect)",val_SZ)]:
    w,l=run(fn); print(f"{name:44}{w:>+10.0f}{l:>+13.0f}{w+l:>+9.0f}")
conn.close()
