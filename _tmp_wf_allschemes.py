# -*- coding: utf-8 -*-
"""Compare baseline vs OPEN(SB) vs MOM30(SB), each under 0/0/1, 0/1/1, 0/1/2 (+ref 0.5/1/2 sizing).
Walk-forward: lock threshold on April($), test May-Jun unseen. Report $, vsBase, capital-adj vsBase, Ret/DD, DD, trades."""
import os, psycopg2, pandas as pd, numpy as np, yfinance as yf
from datetime import timedelta
import warnings; warnings.filterwarnings('ignore')
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1)
    parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
df=pd.read_sql("""SELECT id, ts AT TIME ZONE 'America/New_York' et, setup_name nm, direction dir,
   outcome_pnl cpnl, outcome_result res FROM setup_log WHERE live_pass=true
   AND ts AT TIME ZONE 'America/New_York'>='2026-03-27' AND outcome_pnl IS NOT NULL ORDER BY ts""",c); c.close()
df['et']=pd.to_datetime(df['et']).dt.tz_localize(None); df['long']=df['dir'].isin(['long','bullish'])
df=df.sort_values('et'); keep=[]; last={}
for _,r in df.iterrows():
    k=(r['nm'],r['long'])
    if k in last and (r['et']-last[k])<timedelta(minutes=15): continue
    last[k]=r['et']; keep.append(r)
df=pd.DataFrame(keep).sort_values('et').reset_index(drop=True)
df['d']=df['et'].dt.date; df['pnl']=df['cpnl'].astype(float)*5; df['mo']=pd.to_datetime(df['et']).dt.to_period('M').astype(str)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(minutes=30); tt=tt.sort_values('q')
df['mom30']=df['b_now']-pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min')).sort_index()['bp'].reindex(df.index).values

def cls(r,ref,th):
    v=r['b_now'] if ref=='open' else r['mom30']
    if pd.isna(v) or abs(v)<th: return 'neu'
    return 'conf' if (v>0)==r['long'] else 'fight'
SCHEMES={'0/0/1':{'fight':0,'neu':0,'conf':1},'0/1/1':{'fight':0,'neu':1,'conf':1},
         '0/1/2':{'fight':0,'neu':1,'conf':2},'0.5/1/2':{'fight':0.5,'neu':1,'conf':2}}
train=df['mo']=='2026-04'; test=df['mo'].isin(['2026-05','2026-06'])
ths=[0.10,0.15,0.20,0.30]
def daydd(pnl,days):
    cum=pnl.groupby(days).sum().sort_index().cumsum(); return (cum.cummax()-cum).max()
base_test=df.loc[test,'pnl'].sum(); base_dd=daydd(df.loc[test,'pnl'],df.loc[test,'d'])
print(f"TEST=May-Jun  Baseline: ${base_test:.0f}  DD=${base_dd:.0f}  Ret/DD={base_test/base_dd:.1f}  (all {int(test.sum())} trades)\n")
print(f"{'ref':>6} {'scheme':<8}{'th':>5}{'test$':>8}{'vsBase':>8}{'capAdj':>8}{'avgCap':>8}{'Ret/DD':>8}{'DD':>7}{'trades':>8}")
for ref in ('open','mom30'):
    for sch,mp in SCHEMES.items():
        # lock th on April by $
        best=None
        for th in ths:
            cl=df.apply(lambda r:cls(r,ref,th),axis=1); mult=cl.map(mp)
            apr=(df.loc[train,'pnl']*mult[train]).sum()
            if best is None or apr>best[1]: best=(th,apr,mult)
        th,_,mult=best
        pt=df.loc[test,'pnl']*mult[test]; tot=pt.sum(); avgcap=mult[test].mean()
        dd=daydd(pt,df.loc[test,'d']); ntk=int((mult[test]>0).sum())
        capadj=(tot/avgcap)/base_test if avgcap>0 else 0
        print(f"{ref:>6} {sch:<8}{th:>5}{tot:>8.0f}{tot/base_test:>7.2f}x{capadj:>7.2f}x{avgcap:>8.2f}{tot/dd if dd>0 else 0:>8.1f}{dd:>7.0f}{ntk:>5}/{int(test.sum())}")
