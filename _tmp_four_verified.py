# -*- coding: utf-8 -*-
"""Four-scheme on VERIFIED V16-base (passes_v16 VPB-fixed, no dedup, chain pts). Post-V16 May18+.
Schemes: baseline / SB open 0/0/1 / SB mom 0/0/1 / SB mom 0/1/2. Dense yfinance basket (full coverage)."""
import os, sys, pandas as pd, numpy as np, yfinance as yf
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
        f"WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-05-18' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]   # VPB now excluded by the fix
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base)
df['long']=df['direction'].isin(['long','bullish']); df['pts']=df['outcome_pnl'].astype(float)
df['d']=df['et'].dt.date; df['mo']=pd.to_datetime(df['et']).dt.to_period('M').astype(str)
print(f"VERIFIED V16-base May18+: {len(df)} trades (VPB excluded), setups={df['setup_name'].nunique()}")
# dense basket
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1); parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
df=df.sort_values('et').reset_index(drop=True)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(minutes=30); tt=tt.sort_values('q')
df['mom30']=df['b_now']-pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min')).sort_index()['bp'].reindex(df.index).values
def gate(ref,th):
    v=df['b_now'] if ref=='open' else df['mom30']
    return np.where(v.isna()|(v.abs()<th),0.0,np.where((v>0)==df['long'],1.0,0.0))
def s012(th):
    v=df['mom30']; return np.where(v.isna()|(v.abs()<th),1.0,np.where((v>0)==df['long'],2.0,0.0))
df['m_base']=1.0; df['m_open001']=gate('open',0.15); df['m_mom001']=gate('mom30',0.10); df['m_mom012']=s012(0.15)
def metr(col):
    pts=(df['pts']*df[col]); tot=pts.sum(); cap=df[col].mean(); ntk=int((df[col]>0).sum())
    w=((df['outcome_result']=='WIN')&(df[col]>0)).sum(); wr=100*w/ntk if ntk else 0
    cum=pts.groupby(df['d']).sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
    return tot,cap,ntk,wr,dd
bt=df['pts'].sum()
print(f"\n{'scheme':<20}{'pts':>8}{'$@1MES':>8}{'vsBase':>8}{'capAdj':>8}{'trades':>9}{'WR':>5}{'maxDD$':>8}")
for lab,col in [('V16-baseline','m_base'),('SB open 0/0/1','m_open001'),('SB mom 0/0/1','m_mom001'),('SB mom 0/1/2','m_mom012')]:
    tot,cap,ntk,wr,dd=metr(col); capadj=(tot/cap)/bt if (cap>0 and bt!=0) else 0
    print(f"{lab:<20}{tot:>8.1f}{tot*5:>8.0f}{tot/bt if bt else 0:>7.2f}x{capadj:>7.2f}x{ntk:>6}/{len(df)}{wr:>4.0f}%{dd*5:>8.0f}")
# month split
print("\nby month (pts):")
for mo in sorted(df['mo'].unique()):
    g=df[df['mo']==mo]
    r=[(g['pts']*g[c]).sum() for c in ['m_base','m_open001','m_mom001','m_mom012']]
    print(f"  {mo}: base={r[0]:+.0f}  open001={r[1]:+.0f}  mom001={r[2]:+.0f}  mom012={r[3]:+.0f}  (n={len(g)})")
