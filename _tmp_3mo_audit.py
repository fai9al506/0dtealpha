# -*- coding: utf-8 -*-
"""(A) 3-month backtest (Mar27-Jun24) four schemes on VERIFIED base (passes_v16 VPB-fixed, chain pts, dense basket).
(B) June: open 0/0/1 on STAMPED basket (live) vs DENSE basket (ideal) -> pins live magnitude.
AUDIT anchor: Jun15-24 base must = portal -231.1."""
import os, sys, pandas as pd, numpy as np, yfinance as yf
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
df=pd.DataFrame(base)
df['long']=df['direction'].isin(['long','bullish']); df['pts']=df['outcome_pnl'].astype(float)
df['stamped']=pd.to_numeric(df['basket_pct'],errors='coerce')
df['d']=df['et'].dt.date; df['mo']=pd.to_datetime(df['et']).dt.to_period('M').astype(str)
# AUDIT anchor
anc=df[(df['et']>=pd.Timestamp('2026-06-15'))&(df['et']<pd.Timestamp('2026-06-25'))]
print(f"AUDIT anchor Jun15-24 base: {len(anc)} trades, {anc['pts'].sum():+.1f} pts  (portal=-231.1, must match)")
# dense basket
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='90d',interval='1h',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1); parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
df=df.sort_values('et').reset_index(drop=True)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('75min'))['bp'].values
tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(hours=2); tt=tt.sort_values('q')
df['mom30']=df['b_now']-pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('90min')).sort_index()['bp'].reindex(df.index).values
def gate(v,th):  return np.where(v.isna()|(v.abs()<th),0.0,np.where((v>0)==df['long'],1.0,0.0))
def s012(v,th): return np.where(v.isna()|(v.abs()<th),1.0,np.where((v>0)==df['long'],2.0,0.0))
df['m_base']=1.0; df['m_open001']=gate(df['b_now'],0.15); df['m_mom001']=gate(df['mom30'],0.10); df['m_mom012']=s012(df['mom30'],0.15)
def metr(col):
    pts=(df['pts']*df[col]); tot=pts.sum(); cap=df[col].mean(); ntk=int((df[col]>0).sum())
    w=((df['outcome_result']=='WIN')&(df[col]>0)).sum(); wr=100*w/ntk if ntk else 0
    cum=pts.groupby(df['d']).sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
    return tot,cap,ntk,wr,dd
bt=df['pts'].sum()
print(f"\n(A) 3-MONTH Mar27-Jun24 [1h basket; mom=2h-coarse], VERIFIED base = {len(df)} trades, baseline {bt:+.0f} pts")
print(f"{'scheme':<18}{'pts':>8}{'vsBase':>8}{'capAdj':>8}{'trades':>9}{'WR':>5}{'maxDD$':>8}")
for lab,col in [('V16-baseline','m_base'),('SB open 0/0/1','m_open001'),('SB mom 0/0/1','m_mom001'),('SB mom 0/1/2','m_mom012')]:
    tot,cap,ntk,wr,dd=metr(col); print(f"{lab:<18}{tot:>8.0f}{tot/bt if bt else 0:>7.2f}x{(tot/cap)/bt if cap and bt else 0:>7.2f}x{ntk:>6}/{len(df)}{wr:>4.0f}%{dd*5:>8.0f}")
print("by month (pts): base / open001 / mom001 / mom012")
for mo in sorted(df['mo'].unique()):
    g=df[df['mo']==mo]; print(f"  {mo}: {(g['pts']).sum():+.0f} / {(g['pts']*g['m_open001']).sum():+.0f} / {(g['pts']*g['m_mom001']).sum():+.0f} / {(g['pts']*g['m_mom012']).sum():+.0f}  (n={len(g)})")

# (B) June: stamped (live) vs dense (ideal) for open 0/0/1
jun=df[df['mo']=='2026-06'].copy()
jun['g_stamped']=gate(jun['stamped'],0.15) if False else np.where(jun['stamped'].isna()|(jun['stamped'].abs()<0.15),0.0,np.where((jun['stamped']>0)==jun['long'],1.0,0.0))
jun['g_dense']=np.where(jun['b_now'].isna()|(jun['b_now'].abs()<0.15),0.0,np.where((jun['b_now']>0)==jun['long'],1.0,0.0))
print(f"\n(B) JUNE open 0/0/1 — LIVE stamped basket vs IDEAL dense basket (n={len(jun)} June trades)")
print(f"  base June: {jun['pts'].sum():+.0f} pts")
print(f"  STAMPED (what live used): {(jun['pts']*jun['g_stamped']).sum():+.0f} pts, took {int(jun['g_stamped'].sum())} trades  (stamped present: {jun['stamped'].notna().sum()}/{len(jun)})")
print(f"  DENSE (ideal full data):  {(jun['pts']*jun['g_dense']).sum():+.0f} pts, took {int(jun['g_dense'].sum())} trades")
