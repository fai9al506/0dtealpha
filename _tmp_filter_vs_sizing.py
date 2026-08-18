# -*- coding: utf-8 -*-
"""Separate FILTER effect (which trades, 1x) from SIZING effect (how much, 2x conf).
Verified V16-base (passes_v16 VPB-fixed), 3-month Mar27-Jun24, chain pts, 1h basket."""
import os, sys, pandas as pd, numpy as np, yfinance as yf, json
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
        f"WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-27' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base); df['long']=df['direction'].isin(['long','bullish']); df['pts']=df['outcome_pnl'].astype(float)
df['d']=df['et'].dt.date
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='90d',interval='1h',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1); parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
df=df.sort_values('et').reset_index(drop=True)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('75min'))['bp'].values
tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(hours=2); tt=tt.sort_values('q')
df['mom']=df['b_now']-pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('90min')).sort_index()['bp'].reindex(df.index).values
def cls(ref): 
    v=df['b_now'] if ref=='open' else df['mom']; th=0.15
    return pd.Series(np.where(v.isna()|(v.abs()<th),'neu',np.where((v>0)==df['long'],'conf','fight')),index=df.index)
df['k_open']=cls('open'); df['k_mom']=cls('mom')
def mult(kcol,mp): return df[kcol].map(mp)
def metr(col):
    pts=(df['pts']*col); tot=pts.sum(); cap=col.mean(); ntk=int((col>0).sum())
    w=((df['outcome_result']=='WIN')&(col>0)).sum(); wr=100*w/ntk if ntk else 0
    cum=pts.groupby(df['d']).sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
    return tot,cap,ntk,wr,dd*5
bt=df['pts'].sum()
def line(lab,col):
    tot,cap,ntk,wr,dd=metr(col); return f"{lab:<24}{tot:>7.0f}{tot/bt if bt else 0:>7.2f}x{(tot/cap)/bt if cap and bt else 0:>7.2f}x{ntk:>6}/{len(df)}{wr:>4.0f}%{dd:>8.0f}"
hdr=f"{'scheme':<24}{'pts':>7}{'vsBase':>8}{'capAdj':>8}{'trades':>9}{'WR':>5}{'maxDD$':>8}"
GATE={'fight':0,'neu':0,'conf':1}; SZ012={'fight':0,'neu':1,'conf':2}; SZ_half={'fight':0.5,'neu':1,'conf':2}; KEEP112={'fight':1,'neu':1,'conf':2}
print(f"3-mo verified base = {len(df)} trades, baseline {bt:+.0f} pts\n")
print("A) FILTER ONLY (1x sizing — pure 'which trades'):")
print(hdr); print(line('baseline (take all)',df['m_base'] if 'm_base' in df else pd.Series(1.0,index=df.index)))
print(line('open  0/0/1 (filter)',mult('k_open',GATE))); print(line('mom   0/0/1 (filter)',mult('k_mom',GATE)))
print("\nB) SIZING on the OPEN filter (same trades kept, vary bet size):")
print(hdr)
print(line('open 0/0/1 (no size)',mult('k_open',GATE)))
print(line('open 1/1/2 (size only)',mult('k_open',KEEP112)))
print(line('open 0/1/2 (filt+size)',mult('k_open',SZ012)))
print(line('open 0.5/1/2',mult('k_open',SZ_half)))
print("\nC) for reference — mom sizing:")
print(hdr); print(line('mom 0/1/2',mult('k_mom',SZ012)))
