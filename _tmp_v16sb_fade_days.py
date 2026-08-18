# -*- coding: utf-8 -*-
"""Is yesterday's pattern (basket-CONFIRMED longs that net-lose on a fade day) IN the V16-SB backtest window?
Join semi_basket (Mar16+) to V16 longs, classify confirm/neutral/contradict like the live gate, per-month + per-day net."""
import os, psycopg2, pandas as pd, numpy as np
conn=psycopg2.connect(os.environ["DATABASE_URL"])
longs=pd.read_sql("""SELECT id, ts AT TIME ZONE 'America/New_York' et, direction,
   outcome_result res, outcome_pnl cpnl, mes_sim_outcome_pnl mpnl, mes_sim_outcome_result mres
   FROM setup_log WHERE live_pass=true AND direction in ('long','bullish')
   AND ts AT TIME ZONE 'America/New_York'>='2026-03-16' AND outcome_result IS NOT NULL
   ORDER BY ts""",conn)
bk=pd.read_sql("""SELECT et AT TIME ZONE 'America/New_York' bt, basket_pct FROM semi_basket
   WHERE et AT TIME ZONE 'America/New_York'>='2026-03-16' ORDER BY et""",conn)
conn.close()
longs['et']=pd.to_datetime(longs['et']).dt.tz_localize(None); bk['bt']=pd.to_datetime(bk['bt']).dt.tz_localize(None)
longs['d']=longs['et'].dt.date; longs['mo']=longs['et'].dt.to_period('M').astype(str)
# attach most-recent basket <= signal (within 15min), same as live gate behaviour
bk=bk.sort_values('bt')
longs=longs.sort_values('et')
m=pd.merge_asof(longs, bk, left_on='et', right_on='bt', direction='backward', tolerance=pd.Timedelta('15min'))
def cls(b):
    if pd.isna(b): return 'no_data(taken)'
    if abs(b)<0.15: return 'neutral(skip)'
    return 'confirm(take)' if b>0 else 'contradict(skip)'
m['gate']=m['basket_pct'].apply(cls)
# V16-SB TAKES confirm + no_data(fail-open). neutral/contradict skipped.
m['taken']= m['gate'].isin(['confirm(take)','no_data(taken)'])
print("=== basket gate classification of V16 LONGS (Mar16-Jun) ===")
g=m.groupby('gate').agg(n=('id','size'), chain=('cpnl','sum'),
   wr=('res',lambda s:(s=='WIN').mean()*100)).round(0)
print(g.to_string())
print("\n=== V16-SB (confirm+fail-open longs) net by month — chain & mes(covered) ===")
t=m[m['taken']]
for mo in sorted(m['mo'].unique()):
    sub=t[t['mo']==mo]; mm=sub[sub['mpnl'].notna()]
    print(f" {mo}: taken_n={len(sub):<4} chain {(sub['res']=='WIN').mean()*100:4.0f}%/{sub['cpnl'].sum():+7.0f}p | mes(n={len(mm)}) {sub['mpnl'].sum():+7.0f}p")
print("\n=== Days where CONFIRMED longs net-LOST (yesterday's pattern) ===")
day=t.groupby('d').agg(n=('id','size'), chain=('cpnl','sum'),
   mes=('mpnl',lambda s:s.dropna().sum())).reset_index()
bad=day[day['chain']<0].sort_values('chain')
print(f"confirmed-long-net-loss days: {len(bad)} of {len(day)} trading days")
print(bad.tail(12).to_string(index=False))
