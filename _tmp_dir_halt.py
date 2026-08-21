# -*- coding: utf-8 -*-
"""Directional loss-halt sim, full history, overlap-aware (uses CLOSE times).
Rule: per day/direction, after N realized losses among TAKEN trades, skip further same-dir entries.
Kill test = era stability. Judged on mes_sim ($ truth, Apr15+) AND chain (full Feb-Jun context)."""
import os, psycopg2, pandas as pd, numpy as np
conn=psycopg2.connect(os.environ["DATABASE_URL"])
df=pd.read_sql("""
  SELECT id, ts AT TIME ZONE 'America/New_York' AS et, direction,
         outcome_elapsed_min em, outcome_result res, outcome_pnl cpnl,
         mes_sim_outcome_pnl mpnl, mes_sim_outcome_result mres
  FROM setup_log
  WHERE live_pass=true AND ts AT TIME ZONE 'America/New_York' >= '2026-02-01'
    AND outcome_result IS NOT NULL
  ORDER BY ts""",conn)
conn.close()
df['et']=pd.to_datetime(df['et']); df['d']=df['et'].dt.date
df['month']=df['et'].dt.to_period('M').astype(str)
df['dir']=df['direction'].apply(lambda x:'long' if x in('long','bullish') else 'short')
df['close']=df['et']+pd.to_timedelta(df['em'].fillna(0),unit='m')

def sim(sub, N, pnl_col, res_col):
    """return (taken_pnl, baseline_pnl, n_taken, n_base) for one day+dir"""
    sub=sub.sort_values('et')
    taken=[]  # list of (close, isloss)
    tp=0.0; nt=0
    for _,r in sub.iterrows():
        lost = sum(1 for c,isl in taken if isl and c<=r['et'])
        if lost>=N:   # halt
            continue
        p=r[pnl_col]
        if pd.isna(p): p=0.0
        taken.append((r['close'], r[res_col]=='LOSS'))
        tp+=p; nt+=1
    return tp, nt

def run(pnl_col,res_col,label,frame):
    print(f"\n=== {label} | direction halt on LONGS ===")
    base=frame[frame['dir']=='long']
    base_by_mo=base.groupby('month')[pnl_col].sum()
    print(f"{'N':<4}{'month':<10}{'base_pnl':>10}{'halt_pnl':>10}{'delta(saved)':>14}{'base_n':>8}{'halt_n':>8}")
    for N in [2,3,4]:
        tot_b=tot_h=0
        for mo in sorted(base['month'].unique()):
            mb=base[base['month']==mo]
            bpnl=mb[pnl_col].fillna(0).sum(); bn=len(mb)
            hp=hn=0
            for d,g in mb.groupby('d'):
                tp,nt=sim(g,N,pnl_col,res_col); hp+=tp; hn+=nt
            tot_b+=bpnl; tot_h+=hp
            print(f"{N:<4}{mo:<10}{bpnl:>10.0f}{hp:>10.0f}{bpnl-hp:>14.0f}{bn:>8}{hn:>8}")
        print(f"{N:<4}{'TOTAL':<10}{tot_b:>10.0f}{tot_h:>10.0f}{tot_b-tot_h:>14.0f}")

# chain full history
run('cpnl','res','CHAIN (Feb-Jun, all live_pass longs)', df)
# mes truth, restrict to mes-covered rows
m=df[df['mpnl'].notna()].copy()
run('mpnl','mres','MES-SIM (Apr15+ covered rows only)', m)
