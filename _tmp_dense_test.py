# -*- coding: utf-8 -*-
import os, psycopg2, pandas as pd, numpy as np, yfinance as yf
from datetime import timedelta
import warnings; warnings.filterwarnings('ignore')
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
# 1) dense 5-min basket path from yfinance
px=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)['Close']
px=px.tz_convert('America/New_York')
px=px.between_time('09:30','15:59')
px['day']=px.index.date
parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]
    pct=(g[TK]-o)/o*100
    b=pct.mean(axis=1)
    parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
print(f"dense basket rows: {len(bk)}  {bk['bt'].min()} -> {bk['bt'].max()}")

# 2) V16 trades
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
df['d']=df['et'].dt.date; df['pnl']=df['cpnl'].astype(float)*5

df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
def prior(win):
    tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(minutes=win); tt=tt.sort_values('q')
    r=pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min'))
    return r.sort_index()['bp'].reindex(df.index).values
df['mom15']=df['b_now']-prior(15); df['mom30']=df['b_now']-prior(30)
print(f"trades {len(df)} ({df['long'].sum()}L/{(~df['long']).sum()}S)  dense coverage {df['b_now'].notna().mean()*100:.0f}%")

D=0.15
def m_open(r):
    b=r['b_now']
    if pd.isna(b) or abs(b)<D: return 1.0
    return 2.0 if (b>0)==r['long'] else 0.5
def m_shortsonly(r):
    return 1.0 if r['long'] else m_open(r)
def m_mom(r,col,th):
    mm=r[col]
    if pd.isna(mm) or abs(mm)<th: return 1.0
    return 2.0 if (mm>0)==r['long'] else 0.5
def m_mom_long_open_short(r,col,th):   # momentum for longs, open-level for shorts (hybrid)
    if r['long']: return m_mom(r,col,th)
    return m_open(r)

df['m_base']=1.0
df['m_semi']=df.apply(m_open,axis=1)
df['m_shorts']=df.apply(m_shortsonly,axis=1)
for th in (0.10,0.20,0.30):
    df[f'm_mom15_{th}']=df.apply(lambda r:m_mom(r,'mom15',th),axis=1)
df['m_mom30_0.2']=df.apply(lambda r:m_mom(r,'mom30',0.2),axis=1)
df['m_hybrid']=df.apply(lambda r:m_mom_long_open_short(r,'mom15',0.2),axis=1)

def stats(col):
    pnl=df['pnl']*df[col]; tot=pnl.sum(); cap=df[col].mean()
    cum=pnl.groupby(df['d']).sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
    return tot,cap,dd,(tot/dd if dd>0 else np.nan)
print(f"\n{'scheme':<24}{'total$':>9}{'avgCap':>8}{'maxDD':>8}{'Ret/DD':>8}{'vsBase':>8}")
bt=stats('m_base')[0]
for nm,col in [('Baseline','m_base'),('Semi(open,both)*study','m_semi'),('#2 Shorts-only','m_shorts'),
   ('#1 Mom15 th.10','m_mom15_0.1'),('#1 Mom15 th.20','m_mom15_0.2'),('#1 Mom15 th.30','m_mom15_0.3'),
   ('#1 Mom30 th.20','m_mom30_0.2'),('Hybrid mom-L/open-S','m_hybrid')]:
    t,cap,dd,rdd=stats(col); print(f"{nm:<24}{t:>9.0f}{cap:>8.2f}{dd:>8.0f}{rdd:>8.1f}{t/bt:>7.2f}x")

# WR diagnostic: confirmed-LONG win rate under open vs momentum
def cwr(mask): 
    s=df[mask & df['long']]; return len(s),(s['res']=='WIN').mean()*100 if len(s) else 0
n_o,wr_o=cwr(df['b_now']>D); n_m,wr_m=cwr(df['mom15']>0.2)
print(f"\nConfirmed-LONG WR: open-anchored n={n_o} WR={wr_o:.0f}%  |  momentum15 n={n_m} WR={wr_m:.0f}%  (neutral baseline ~64%)")
# yesterday
y=df[df['d']==pd.to_datetime('2026-06-23').date()&df['long'] if False else df['d']==pd.to_datetime('2026-06-23').date()]
yl=y[y['long']]
print(f"\nYesterday longs: base=${(yl['pnl']).sum():.0f}  semi(open)=${(yl['pnl']*yl['m_semi']).sum():.0f}  mom15.20=${(yl['pnl']*yl['m_mom15_0.2']).sum():.0f}")
print(f"  yesterday long b_now mean={yl['b_now'].mean():.2f}  mom15 mean={yl['mom15'].mean():.2f}")

print("\n================ CAREFUL STUDY: shorts symmetric + era stability ================")
# SHORT-side diagnostic (symmetric to longs)
sh=df[~df['long']]
def swr(mask): s=sh[mask]; return len(s),((s['res']=='WIN').mean()*100 if len(s) else 0)
no,wo=swr(sh['b_now']<-D); nm,wm=swr(sh['mom15']<-0.2)
print(f"Confirmed-SHORT WR: open-anchored n={no} WR={wo:.0f}%  |  momentum15 n={nm} WR={wm:.0f}%")

# Symmetric TAILS
print("\nTAILS (open-anchor confirms WRONG; does momentum fight them?):")
# long tail: green-from-open but rolling DOWN (V-top fade)
lt=df[df['long'] & (df['b_now']>D) & (df['mom15']<0)]
# short tail: red-from-open but rolling UP (V-bottom bounce) -- the user's point
st=df[~df['long'] & (df['b_now']<-D) & (df['mom15']>0)]
for lab,t in [("LONG tail (green-open, rolling down)",lt),("SHORT tail (red-open, rolling up)",st)]:
    if len(t)==0: print(f"  {lab}: n=0"); continue
    base=t['pnl'].sum(); semi=(t['pnl']*t.apply(m_open,axis=1)).sum(); mom=(t['pnl']*t.apply(lambda r:m_mom(r,'mom15',0.2),axis=1)).sum()
    print(f"  {lab}: n={len(t)} WR={(t['res']=='WIN').mean()*100:.0f}%  base=${base:+.0f}  open-semi=${semi:+.0f}  momentum=${mom:+.0f}")

# ERA STABILITY (kill test): per-month baseline vs semi(open) vs mom15.20 (both dirs)
df['mo']=pd.to_datetime(df['et']).dt.to_period('M').astype(str)
print(f"\n{'month':<10}{'n':>5}{'base$':>9}{'semiOpen$':>11}{'mom15.20$':>11}{'mom-vs-base':>12}")
for mo in sorted(df['mo'].unique()):
    g=df[df['mo']==mo]
    b=g['pnl'].sum(); s=(g['pnl']*g['m_semi']).sum(); m=(g['pnl']*g['m_mom15_0.2']).sum()
    print(f"{mo:<10}{len(g):>5}{b:>9.0f}{s:>11.0f}{m:>11.0f}{m-b:>+12.0f}")
b=df['pnl'].sum(); s=(df['pnl']*df['m_semi']).sum(); m=(df['pnl']*df['m_mom15_0.2']).sum()
print(f"{'TOTAL':<10}{len(df):>5}{b:>9.0f}{s:>11.0f}{m:>11.0f}{m-b:>+12.0f}")

# DD per month-cum for the two finalists
def maxdd(col):
    cum=(df['pnl']*df[col]).groupby(df['d']).sum().sort_index().cumsum(); return (cum.cummax()-cum).max()
print(f"\nMaxDD: baseline=${maxdd('m_base'):.0f}  semi(open)=${maxdd('m_semi'):.0f}  mom15.20=${maxdd('m_mom15_0.2'):.0f}  mom30.20=${maxdd('m_mom30_0.2'):.0f}")
