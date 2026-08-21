# -*- coding: utf-8 -*-
"""Walk-forward, MOMENTUM vs OPEN reference under BOTH scalings:
   - SIZING (2x conf /1x neu /0.5x fight)
   - GATE 0/0/1 (take conf only; skip neutral AND contradicted)
Lock threshold on April ($), test May-Jun. mom window=30 (from prior lock)."""
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

def signal(r,ref,th):
    v=r['b_now'] if ref=='open' else r['mom30']
    if pd.isna(v) or abs(v)<th: return 'neutral'
    return 'conf' if (v>0)==r['long'] else 'fight'
def metrics(sub,pnlcol):
    tot=sub[pnlcol].sum(); cum=sub.groupby('d')[pnlcol].sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
    return tot,(tot/dd if dd>0 else np.nan),dd

def build(ref,th):
    s=df.apply(lambda r:signal(r,ref,th),axis=1)
    size=s.map({'conf':2.0,'neutral':1.0,'fight':0.5})
    gate=s.map({'conf':1.0,'neutral':0.0,'fight':0.0})  # 0/0/1
    return df['pnl']*size, df['pnl']*gate, gate

# LOCK threshold on April by $ for each (ref, scaling)
train=df['mo']=='2026-04'; test=df['mo'].isin(['2026-05','2026-06'])
ths=[0.10,0.15,0.20,0.30]
print("Lock threshold on APRIL ($), then TEST May-Jun (unseen). base$ test=",f"{df.loc[test,'pnl'].sum():.0f}\n")
basecol='pnl'
for ref in ('open','mom30'):
    for scaling in ('SIZE','GATE 0/0/1'):
        best=None
        for th in ths:
            sz,gt,gate=build(ref,th)
            col=sz if scaling=='SIZE' else gt
            tmp=df.assign(x=col)
            apr=tmp.loc[train,'x'].sum()
            if best is None or apr>best[1]: best=(th,apr,col,gate)
        th,apr,col,gate=best
        tmp=df.assign(x=col)
        tot,rdd,dd=metrics(tmp[test],'x'); base,_,bdd=metrics(df[test],'pnl')
        ntaken=int(gate[test].sum()) if scaling.startswith('GATE') else len(df[test])
        ntot=int(test.sum())
        print(f"{ref:>6} {scaling:<11} th={th:<4} | TEST ${tot:.0f}  vsBase={tot/base:.2f}x  Ret/DD={rdd:.1f}  DD={dd:.0f}  trades={ntaken}/{ntot}")
