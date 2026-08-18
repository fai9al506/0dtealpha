# -*- coding: utf-8 -*-
"""POST-V16 only (May18 -> today): baseline vs open 0/0/1 vs mom 0/0/1 vs mom 0/1/2. Frozen April thresholds."""
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
   AND ts AT TIME ZONE 'America/New_York'>='2026-05-18' AND outcome_pnl IS NOT NULL ORDER BY ts""",c); c.close()
df['et']=pd.to_datetime(df['et']).dt.tz_localize(None); df['long']=df['dir'].isin(['long','bullish'])
df=df.sort_values('et'); keep=[]; last={}
for _,r in df.iterrows():
    k=(r['nm'],r['long'])
    if k in last and (r['et']-last[k])<timedelta(minutes=15): continue
    last[k]=r['et']; keep.append(r)
df=pd.DataFrame(keep).sort_values('et').reset_index(drop=True)
df['d']=df['et'].dt.date; df['pnl']=df['cpnl'].astype(float)*5
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(minutes=30); tt=tt.sort_values('q')
df['mom30']=df['b_now']-pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min')).sort_index()['bp'].reindex(df.index).values
def cls(r,ref,th):
    v=r['b_now'] if ref=='open' else r['mom30']
    if pd.isna(v) or abs(v)<th: return 'neu'
    return 'conf' if (v>0)==r['long'] else 'fight'
def mult_col(ref,th,mp): 
    return df.apply(lambda r:cls(r,ref,th),axis=1).map(mp)
SC={'open001':('open',0.20,{'fight':0,'neu':0,'conf':1}),
    'open011':('open',0.20,{'fight':0,'neu':1,'conf':1}),
    'mom011':('mom30',0.30,{'fight':0,'neu':1,'conf':1}),
    'mom001':('mom30',0.10,{'fight':0,'neu':0,'conf':1}),
    'mom012':('mom30',0.15,{'fight':0,'neu':1,'conf':2})}
df['m_base']=1.0
for k,(ref,th,mp) in SC.items(): df['m_'+k]=mult_col(ref,th,mp)
def metr(col):
    pnl=df['pnl']*df[col]; tot=pnl.sum(); cap=df[col].mean()
    cum=pnl.groupby(df['d']).sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
    ntk=int((df[col]>0).sum())
    return tot,cap,dd,(tot/dd if dd>0 else np.nan),ntk
print(f"POST-V16  May 18 -> {df['d'].max()}   {len(df)} trades  ({df['long'].sum()}L/{(~df['long']).sum()}S)")
print(f"\n{'scheme':<20}{'$':>8}{'vsBase':>8}{'capAdj':>8}{'avgCap':>8}{'Ret/DD':>8}{'DD':>7}{'trades':>8}")
bt=df['pnl'].sum()
for lab,col in [('Baseline','m_base'),('open 0/0/1','m_open001'),('open 0/1/1','m_open011'),('mom 0/0/1','m_mom001'),('mom 0/1/1','m_mom011'),('mom 0/1/2','m_mom012')]:
    t,cap,dd,rdd,ntk=metr(col); capadj=(t/cap)/bt if cap>0 else 0
    print(f"{lab:<20}{t:>8.0f}{t/bt:>7.2f}x{capadj:>7.2f}x{cap:>8.2f}{rdd:>8.1f}{dd:>7.0f}{ntk:>5}/{len(df)}")

print("\n--- daily $ (each scheme), worst & best days ---")
dd=pd.DataFrame({lab:(df['pnl']*df[col]).groupby(df['d']).sum()
                 for lab,col in [('base','m_base'),('open011','m_open011'),('mom001','m_mom001'),('mom011','m_mom011'),('mom012','m_mom012')]}).round(0).fillna(0)
print(dd.sort_values('base').head(5).to_string())
print("...")
print(dd.sort_values('base').tail(4).to_string())
print(f"\nTOTAL row: {dd.sum().to_dict()}")
