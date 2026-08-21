# -*- coding: utf-8 -*-
"""POST-V16 (May18+) clean compare: V16-baseline vs SB open 0/0/1 vs SB mom 0/0/1 vs SB mom 0/1/2.
Universe = TRUE V16-base via canonical app/live_filter.passes_v16 (NOT live_pass, which is already SB for June).
Dense yfinance basket for all schemes (apples-to-apples). chain $ @1MES, 15-min dedup."""
import os, sys, pandas as pd, numpy as np, yfinance as yf
from datetime import timedelta
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf

eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
        f"WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-05-18' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
# TRUE V16-base universe (no basket gate)
base=[dict(r) for r in rows if lf.passes_v16(r, gaps)]
for r in base:
    et=r['ts'].astimezone(lf.ET); r['et']=et.replace(tzinfo=None)
df=pd.DataFrame(base)
df['long']=df['direction'].isin(['long','bullish']); df['pnl']=df['outcome_pnl'].astype(float)*5
df['res']=df['outcome_result']
# 15-min dedup per (setup,side)
df=df.sort_values('et'); keep=[]; last={}
for _,r in df.iterrows():
    k=(r['setup_name'],r['long'])
    if k in last and (r['et']-last[k])<timedelta(minutes=15): continue
    last[k]=r['et']; keep.append(r)
df=pd.DataFrame(keep).sort_values('et').reset_index(drop=True); df['d']=df['et'].dt.date

# dense basket
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1)
    parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(minutes=30); tt=tt.sort_values('q')
df['mom30']=df['b_now']-pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min')).sort_index()['bp'].reindex(df.index).values

def gate(ref,th):  # 0/0/1: 1 if confirmed else 0
    v=df['b_now'] if ref=='open' else df['mom30']
    return np.where(v.isna()|(v.abs()<th),0.0,np.where((v>0)==df['long'],1.0,0.0))
def s012(th):      # 0/1/2 momentum
    v=df['mom30']
    return np.where(v.isna()|(v.abs()<th),1.0,np.where((v>0)==df['long'],2.0,0.0))
df['m_base']=1.0; df['m_open001']=gate('open',0.15); df['m_mom001']=gate('mom30',0.10); df['m_mom012']=s012(0.15)

def metr(col):
    pnl=df['pnl']*df[col]; tot=pnl.sum(); cap=df[col].mean(); ntk=int((df[col]>0).sum())
    w=((df['res']=='WIN')&(df[col]>0)).sum(); wr=100*w/ntk if ntk else 0
    cum=pnl.groupby(df['d']).sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
    return tot,cap,ntk,wr,dd,(tot/dd if dd>0 else np.nan)
bt=df['pnl'].sum()
print(f"POST-V16 May18->{df['d'].max()}  TRUE V16-base universe = {len(df)} trades  (base $@1MES = {bt:.0f})\n")
print(f"{'scheme':<22}{'$':>8}{'vsBase':>8}{'capAdj':>8}{'trades':>8}{'WR':>6}{'DD':>7}{'Ret/DD':>8}")
for lab,col in [('V16-baseline','m_base'),('SB open 0/0/1','m_open001'),('SB mom 0/0/1','m_mom001'),('SB mom 0/1/2','m_mom012')]:
    tot,cap,ntk,wr,dd,rdd=metr(col); capadj=(tot/cap)/bt if cap>0 else 0
    print(f"{lab:<22}{tot:>8.0f}{tot/bt:>7.2f}x{capadj:>7.2f}x{ntk:>6}/{len(df)}{wr:>5.0f}%{dd:>7.0f}{rdd:>8.1f}")
