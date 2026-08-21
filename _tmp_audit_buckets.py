# -*- coding: utf-8 -*-
"""AUDIT: does the basket SEPARATE winners from losers? Per-trade outcome by conf/neutral/fight bucket,
for OPEN and MOMENTUM(30m) reference. If conf avg >> fight avg => real selection. If flat => just volume."""
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
def klass(v,islong,th):
    if pd.isna(v) or abs(v)<th: return 'neutral'
    return 'CONF' if (v>0)==islong else 'fight'
def audit(ref,th,sub,title):
    col='b_now' if ref=='open' else 'mom30'
    s=sub.copy(); s['k']=s.apply(lambda r:klass(r[col],r['long'],th),axis=1)
    print(f"\n  [{ref} ref, th={th}] {title}  (n={len(s)})")
    print(f"    {'bucket':<9}{'n':>5}{'WR':>6}{'totPnl$':>10}{'avg/trade':>11}")
    for kk in ['CONF','neutral','fight']:
        g=s[s['k']==kk]; n=len(g)
        if n==0: print(f"    {kk:<9}{0:>5}"); continue
        print(f"    {kk:<9}{n:>5}{(g['res']=='WIN').mean()*100:>5.0f}%{g['pnl'].sum():>10.0f}{g['pnl'].mean():>11.2f}")
    cf=s[s['k']=='CONF']['pnl'].mean(); fg=s[s['k']=='fight']['pnl'].mean()
    print(f"    >> selection edge (CONF avg - fight avg) = {cf-fg:+.2f} $/trade")

post=df[df['mo'].isin(['2026-05','2026-06'])]
full=df
for ref,th in [('open',0.15),('mom30',0.10)]:
    audit(ref,th,full,"FULL Mar27-Jun24")
    audit(ref,th,post,"POST-V16 May18+")
print("\n--- KEY: are the FIGHT trades (what 0/1/2 skips) net-negative? are CONF (what gets 2x) net-positive? ---")
# Jun13+ LOSING window bucket check + 1/1/2 vs 0/1/2 vs 0/0/1
jun=df[df['et']>=pd.Timestamp('2026-06-13')]
print(f"\n=== Jun13+ (the losing window user cited), n={len(jun)} ===")
for ref,th in [('open',0.15),('mom30',0.10)]:
    col='b_now' if ref=='open' else 'mom30'
    s=jun.copy(); s['k']=s.apply(lambda r:klass(r[col],r['long'],th),axis=1)
    print(f" [{ref}]")
    for kk in ['CONF','neutral','fight']:
        g=s[s['k']==kk]; 
        if len(g): print(f"    {kk:<9}n={len(g):<3} WR={(g['res']=='WIN').mean()*100:.0f}%  tot={g['pnl'].sum():+.0f}  avg={g['pnl'].mean():+.2f}")
        else: print(f"    {kk:<9}n=0")

print("\n=== schemes incl 1/1/2 (keep fighters) — FULL Mar27+ & POST-V16, mom30 th0.10 ===")
def mult(r,mp,th=0.10):
    v=r['mom30']
    if pd.isna(v) or abs(v)<th: k='neutral'
    else: k='CONF' if (v>0)==r['long'] else 'fight'
    return mp[k]
MPS={'0/0/1':{'CONF':1,'neutral':0,'fight':0},'0/1/1':{'CONF':1,'neutral':1,'fight':0},
     '0/1/2':{'CONF':2,'neutral':1,'fight':0},'1/1/2':{'CONF':2,'neutral':1,'fight':1},
     '0.5/1/2':{'CONF':2,'neutral':1,'fight':0.5},'baseline':{'CONF':1,'neutral':1,'fight':1}}
for win,sub in [('FULL',df),('POST-V16',df[df['mo'].isin(['2026-05','2026-06'])])]:
    base=sub['pnl'].sum()
    print(f" {win} (base ${base:.0f}):")
    for nm,mp in MPS.items():
        m=sub.apply(lambda r:mult(r,mp),axis=1); pnl=(sub['pnl']*m)
        cum=pnl.groupby(sub['d']).sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
        cap=m.mean(); tot=pnl.sum()
        print(f"    {nm:<9} ${tot:>6.0f}  vsBase={tot/base:.2f}x  capAdj={(tot/cap)/base:.2f}x  Ret/DD={tot/dd if dd>0 else 0:.1f}  DD={dd:.0f}")
