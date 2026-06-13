"""Post-V16 matrix: sizing scheme x (before/after SPX-exit fixes).
Schemes: baseline 1/1/1 ; A = contradict0.5/neutral1/confirm2 ; B = 0/0/1 (only confirmed).
Before fixes = actual broker P&L. After fixes = SPX-based exits (val_S).
Split WIN era (May19-Jun4) vs LOSS window (Jun5-12). $ at *5 per lid.
"""
import os, sys, json, psycopg2, bisect
import pandas as pd
from datetime import timedelta
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
port=dict(zip(df['ID'],df['P&L'])); dur=dict(zip(df['ID'],df['Duration (min)']))
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor()
cur.execute("""SELECT sl.id, sl.ts,(sl.ts AT TIME ZONE 'America/New_York') et, sl.spot, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
cur2=conn.cursor()
cur.execute("SELECT et,basket_pct FROM semi_basket ORDER BY et")
sb=[(r[0],float(r[1])) for r in cur.fetchall()]; sb_t=[x[0] for x in sb]
def basket_at(t):
    i=bisect.bisect_right(sb_t,t)-1; return sb[i][1] if i>=0 else None
def spx_mae(ts,d,entry,is_long):
    if entry is None: return None
    cur2.execute("SELECT min(spot),max(spot) FROM chain_snapshots WHERE spot IS NOT NULL AND ts>=%s AND ts<=%s",
                 (ts,ts+timedelta(minutes=max(d,3)+2)))
    lo,hi=cur2.fetchone()
    if lo is None: return None
    return (entry-float(lo)) if is_long else (float(hi)-entry)
cur.execute("""SELECT sl.id, sl.ts,(sl.ts AT TIME ZONE 'America/New_York') et, sl.spot, sl.direction, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY sl.ts""")
T=[]
for sid,ts,et,spot,direction,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    is_long=direction in ('long','bullish')
    broker=(float(e)-float(f)) if is_long else (float(f)-float(e))
    p=float(port.get(sid,0)); cr=str(st.get('close_reason')); sp=float(st.get('stop_pts') or 14)
    # after-fix value
    if cr=='outcome_close_trail_market_exit': fixed=p
    elif cr=='stop_filled':
        m=spx_mae(ts,float(dur.get(sid,30) or 30),float(spot) if spot else None,is_long)
        fixed=p if (m is not None and m<sp) else broker
    else: fixed=broker
    # alignment
    b=basket_at(et.replace(tzinfo=None))
    if b is None: al='no_data'
    elif abs(b)<0.15: al='neutral'
    else: al='confirm' if (b>0)==is_long else 'contradict'
    T.append({'day':str(et)[:10],'broker':broker,'fixed':fixed,'al':al})

SCHEMES={'baseline 1/1/1':{'confirm':1,'neutral':1,'contradict':1,'no_data':1},
         'A 0.5/1/2':{'confirm':2,'neutral':1,'contradict':0.5,'no_data':1},
         'B 0/0/1 (confirmed only)':{'confirm':1,'neutral':0,'contradict':0,'no_data':1}}
def calc(valkey, mults):
    w=l=0.0
    for t in T:
        v=t[valkey]*mults[t['al']]*5
        if t['day']>='2026-06-05': l+=v
        else: w+=v
    return w,l
print("Post-V16 ($ per lid). WIN=May19-Jun4, LOSS=Jun5-12\n")
print(f"{'scheme':28}{'':3}{'WIN':>8}{'LOSS':>9}{'TOTAL':>9}")
for sn,mm in SCHEMES.items():
    for label,vk in [('BEFORE fixes','broker'),('AFTER fixes','fixed')]:
        w,l=calc(vk,mm)
        print(f"{sn:28}{label:13}{w:>+8.0f}{l:>+9.0f}{w+l:>+9.0f}")
    print()
# how many trades scheme B keeps/drops
from collections import Counter
c=Counter(t['al'] for t in T)
print("alignment counts post-V16:",dict(c),"-> B keeps confirm+no_data =",c['confirm']+c['no_data'],"of",len(T))
conn.close()
