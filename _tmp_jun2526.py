# -*- coding: utf-8 -*-
"""Jun-25 & Jun-26: if open 0/1/2 were deployed vs current open 0/0/1. Verified base, chain pts @1MES, dense 5m basket."""
import os, sys, pandas as pd, numpy as np, yfinance as yf
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
        f"WHERE (ts AT TIME ZONE 'America/New_York')::date IN ('2026-06-25','2026-06-26') AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base)
if len(df)==0:
    print("NO V16-base trades found for Jun-25/26 (check data freshness)"); sys.exit()
df['long']=df['direction'].isin(['long','bullish']); df['pts']=df['outcome_pnl'].astype(float); df['d']=df['et'].dt.date
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='30d',interval='5m',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1); parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
df=df.sort_values('et').reset_index(drop=True)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
k=pd.Series(np.where(df['b_now'].isna()|(df['b_now'].abs()<0.15),'neu',np.where((df['b_now']>0)==df['long'],'conf','fight')),index=df.index)
df['m001']=np.where(k=='conf',1.0,0.0)           # open 0/0/1 (current live)
df['m012']=np.where(k=='fight',0.0,np.where(k=='conf',2.0,1.0))  # open 0/1/2
print(f"V16-base coverage Jun-25/26: {len(df)} trades  basket-coverage {df['b_now'].notna().mean()*100:.0f}%\n")
for d,g in df.groupby('d'):
    n1=int((g['m001']>0).sum()); n2=int((g['m012']>0).sum())
    p1=(g['pts']*g['m001']).sum()*5; p2=(g['pts']*g['m012']).sum()*5
    print(f"{d} ({['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][pd.Timestamp(d).weekday()]}):  V16-base {len(g)}t/{g['pts'].sum()*5:+.0f}$")
    print(f"    open 0/0/1 (current): {n1} trades  ${p1:+.0f}")
    print(f"    open 0/1/2 (proposed):{n2} trades  ${p2:+.0f}   delta vs 0/0/1: ${p2-p1:+.0f}")
# totals
n1=int((df['m001']>0).sum()); n2=int((df['m012']>0).sum())
p1=(df['pts']*df['m001']).sum()*5; p2=(df['pts']*df['m012']).sum()*5
print(f"\nBOTH DAYS:  open 0/0/1 = {n1}t ${p1:+.0f}   |   open 0/1/2 = {n2}t ${p2:+.0f}   |   delta ${p2-p1:+.0f}")
