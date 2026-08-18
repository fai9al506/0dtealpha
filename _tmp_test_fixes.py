# -*- coding: utf-8 -*-
"""Test #1 (momentum-anchored basket) and #2 (shorts-only sizing) vs Baseline + current Semi(open).
Same V16 set (live_pass, all setups), chain $ @1MES, 15min dedup, Mar16+ (basket coverage)."""
import os, psycopg2, pandas as pd, numpy as np
from datetime import timedelta
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
df=pd.read_sql("""SELECT id, ts AT TIME ZONE 'America/New_York' et, setup_name nm, direction dir,
   basket_pct, outcome_pnl cpnl, outcome_result res
   FROM setup_log WHERE live_pass=true
   AND ts AT TIME ZONE 'America/New_York'>='2026-03-16' AND outcome_pnl IS NOT NULL ORDER BY ts""",c)
bk=pd.read_sql("SELECT et AT TIME ZONE 'America/New_York' bt, basket_pct bp FROM semi_basket ORDER BY et",c); c.close()
df['et']=pd.to_datetime(df['et']).dt.tz_localize(None); bk['bt']=pd.to_datetime(bk['bt']).dt.tz_localize(None)
df['long']=df['dir'].isin(['long','bullish'])
# 15min dedup per (setup,side)
df=df.sort_values('et'); keep=[]; last={}
for _,r in df.iterrows():
    k=(r['nm'],r['long'])
    if k in last and (r['et']-last[k])<timedelta(minutes=15): continue
    last[k]=r['et']; keep.append(r)
df=pd.DataFrame(keep).sort_values('et').reset_index(drop=True)
df['d']=df['et'].dt.date; df['pnl']=df['cpnl'].astype(float)*5
bk=bk.sort_values('bt')
# basket now, and 15/30 min prior (momentum = now - prior; open cancels)
def asof(times):
    t=pd.DataFrame({'et':times}).sort_values('et')
    r=pd.merge_asof(t,bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('20min'))
    return r.set_index('et')['bp']
df['b_now']=df['et'].map(asof(df['et']))
df['b_15'] =df['et'].map(lambda x: None)  # fill below
df=df.sort_values('et')
df['b_15']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward')['bp'].values
# prior-window lookups
def prior(win):
    tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(minutes=win)
    tt=tt.sort_values('q')
    r=pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('40min'))
    return r.sort_index()['bp'].reindex(df.index).values
df['b_p15']=prior(15); df['b_p30']=prior(30)
df['mom15']=df['b_now']-df['b_p15']; df['mom30']=df['b_now']-df['b_p30']

D=0.15  # open-level deadband
def mult_open(r):
    b=r['b_now']
    if pd.isna(b) or abs(b)<D: return 1.0
    return 2.0 if (b>0)==r['long'] else 0.5
def mult_open_shortsonly(r):
    if r['long']: return 1.0
    return mult_open(r)
def mult_mom(r,col,th):
    m=r[col]
    if pd.isna(m) or abs(m)<th: return 1.0
    rising=m>0
    return 2.0 if (rising)==r['long'] else 0.5

def stats(pnl_col, mult_col):
    pnl=(df['pnl']*df[mult_col])
    tot=pnl.sum(); cap=df[mult_col].mean()
    cum=pnl.groupby(df['d']).sum().sort_index().cumsum()
    dd=(cum.cummax()-cum).max()
    return tot, cap, dd, (tot/dd if dd>0 else float('nan'))

df['m_base']=1.0
df['m_semi']=df.apply(mult_open,axis=1)
df['m_shortsonly']=df.apply(mult_open_shortsonly,axis=1)
for th in (0.05,0.10,0.20):
    df[f'm_mom15_{th}']=df.apply(lambda r:mult_mom(r,'mom15',th),axis=1)
    df[f'm_mom30_{th}']=df.apply(lambda r:mult_mom(r,'mom30',th),axis=1)
# momentum shorts-only at best th later

print(f"V16 set Mar16+: {len(df)} trades ({df['long'].sum()} long / {(~df['long']).sum()} short), basket coverage {df['b_now'].notna().mean()*100:.0f}%")
print(f"\n{'scheme':<22}{'total$':>9}{'avgCap':>8}{'maxDD$':>8}{'Ret/DD':>8}{'vsBase':>8}")
base_tot=None
for name,col in [('Baseline','m_base'),('Semi(open,both)','m_semi'),
                 ('#2 Shorts-only','m_shortsonly'),
                 ('#1 Mom15 th.05','m_mom15_0.05'),('#1 Mom15 th.10','m_mom15_0.1'),('#1 Mom15 th.20','m_mom15_0.2'),
                 ('#1 Mom30 th.10','m_mom30_0.1')]:
    if col not in df.columns: col=col.replace('.1','0.1').replace('.05','0.05').replace('.2','0.2')
    t,cap,dd,rdd=stats('pnl',col)
    if base_tot is None: base_tot=t
    print(f"{name:<22}{t:>9.0f}{cap:>8.2f}{dd:>8.0f}{rdd:>8.1f}{t/base_tot:>7.2f}x")
