# -*- coding: utf-8 -*-
"""Backtest the 3 DEGRADING block rules: full-history + mes-sim + per-month vs the 30d alert."""
import os, psycopg2, pandas as pd, numpy as np
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
df=pd.read_sql("""SELECT id, ts AT TIME ZONE 'America/New_York' et, setup_name nm, direction dir, paradigm,
   greek_alignment al, outcome_pnl cpnl, mes_sim_outcome_pnl mpnl, outcome_result res
   FROM setup_log WHERE ts AT TIME ZONE 'America/New_York' >= '2026-02-01' AND outcome_pnl IS NOT NULL""", c)
c.close()
df['et']=pd.to_datetime(df['et']); df['mo']=df['et'].dt.to_period('M').astype(str)
df['hr']=df['et'].dt.hour; df['long']=df['dir'].isin(['long','bullish'])
d30=df['et']>= (df['et'].max()-pd.Timedelta(days=30))
def report(mask, name):
    g=df[mask]
    def stat(s):
        n=len(s); 
        if n==0: return (0,0,0,0,0)
        wr=(s['res']=='WIN').mean()*100; ch=s['cpnl'].sum()*5
        m=s[s['mpnl'].notna()]; me=m['mpnl'].sum()*5
        return (n,wr,ch,me,len(m))
    n,wr,ch,me,mn=stat(g); n30,wr30,ch30,_,_=stat(g[d30.loc[g.index] if False else g['et']>=(df['et'].max()-pd.Timedelta(days=30))])
    print(f"\n### {name}")
    print(f"  30d (vs alert): n={n30} WR={wr30:.0f}% chain=${ch30:+.0f}")
    print(f"  FULL history:   n={n} WR={wr:.0f}% chain=${ch:+.0f}  | mes(n={mn})=${me:+.0f}")
    print(f"  per-month chain$ (blocked-would-be):")
    for mo in sorted(g['mo'].unique()):
        s=g[g['mo']==mo]; mm=s[s['mpnl'].notna()]
        print(f"    {mo}: n={len(s):<3} chain=${s['cpnl'].sum()*5:+6.0f}  mes=${mm['mpnl'].sum()*5:+6.0f}  WR={(s['res']=='WIN').mean()*100:.0f}%")
# R5: SC long GEX-LIS (all alignments)
report((df['nm']=='Skew Charm')&(df['long'])&(df['paradigm']=='GEX-LIS'), "R5: SC long GEX-LIS block")
# R10: ES Abs bearish hr>=14
report((df['nm']=='ES Absorption')&(~df['long'])&(df['hr']>=14), "R10: ES Abs bearish PM (hr>=14) block")
# SIDIAL: long SIDIAL-EXTREME hr14
report((df['long'])&(df['paradigm']=='SIDIAL-EXTREME')&(df['hr']==14), "SIDIAL-EXT: long SIDIAL-EXTREME hr14 block")
