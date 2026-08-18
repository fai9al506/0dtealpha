# -*- coding: utf-8 -*-
"""S309 - V21 projection at 1, 5 and 10 contracts, with the $300 daily breaker
scaled proportionally (it is a FIXED dollar amount and does not scale itself)."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
N_SESS=117; MARGIN=265.0; SAFE=0.70; SAR=3.75
CAL={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c); moves=lf.load_prev_moves(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')

def run(mult):
    daily=-300.0*mult
    openp,last,out=[],{},[]; realized=0.0; d0=None; closed=[]; peak_l=peak_s=0
    for r in rows:
        t=r['et']
        if t.date()!=d0: d0=t.date(); realized=0.0; closed=[]
        for ct,nv in [x for x in closed if x[0]<=t]: realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not lf.passes_v20(r,gaps): continue
        if lf.v21_blocks(r,moves): continue
        if realized<=daily: continue
        openp=[p for p in openp if p[0]>t]
        n=sum(1 for p in openp if p[1]==r['is_long'])
        if n >= (2 if r['is_long'] else 3): continue
        k=(r['setup_name'],r['is_long'])
        if k in last and (t-last[k]).total_seconds()<90: continue
        sib=[p for p in openp if p[1]==r['is_long'] and p[3]==r['setup_name']]
        if len(sib)>=2 and r.get('spot'):
            sgn=1.0 if r['is_long'] else -1.0
            if sum((float(r['spot'])-p[2])*sgn for p in sib)<0: continue
        last[k]=t
        v=r.get('basket_pct')
        q=(1 if v is None or abs(float(v))<DEAD else (2 if ((float(v)>0)==r['is_long']) else 1))*mult
        pts=float(r['outcome_pnl']); net=(pts-HAIR)*q*DPP-FEE*q
        ct=t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct,r['is_long'],float(r['spot']) if r.get('spot') else 0.0,r['setup_name'],q))
        closed.append((ct,net)); out.append({'d':t.date(),'net':net})
        cl=sum(p[4] for p in openp if p[1]); cs=sum(p[4] for p in openp if not p[1])
        peak_l,peak_s=max(peak_l,cl),max(peak_s,cs)
    o=pd.DataFrame(out); o['mo']=pd.to_datetime(o['d']).dt.strftime('%Y-%m')
    return o, peak_l, peak_s

print("="*118)
print("V21 AT 1, 5 AND 10 CONTRACTS  (the $300 daily breaker scaled with size)")
print("="*118)
print(f"  {'size':10s}{'breaker':>9s}{'$/mo':>10s}{'SAR/mo':>10s}{'worst mo':>10s}{'best mo':>10s}"
      f"{'MaxDD':>10s}{'worst wk':>10s}{'worst day':>11s}")
res={}
for m in (1,2,5,10):
    o,pl,ps=run(m); res[m]=(o,pl,ps)
    per=o.groupby('mo')['net'].sum()/pd.Series(CAL).reindex(sorted(o['mo'].unique()))*21
    dd=o.groupby('d')['net'].sum(); eq=dd.cumsum()
    mo=o['net'].sum()/N_SESS*21
    lbl={1:'1 MES',2:'2 MES',5:'5 MES',10:'1 ES (10)'}[m]
    print(f"  {lbl:10s}{-300*m:>+9,.0f}{mo:>+10,.0f}{mo*SAR:>10,.0f}{per.min():>+10,.0f}{per.max():>+10,.0f}"
          f"{float((eq-eq.cummax()).min()):>+10,.0f}{dd.rolling(5).sum().min():>+10,.0f}{dd.min():>+11,.0f}")
print()
print("="*118)
print("WHAT EACH SIZE REQUIRES")
print("="*118)
print(f"  {'size':10s}{'peak long':>11s}{'peak short':>12s}{'margin $':>11s}{'need equity':>13s}"
      f"{'40-pt gap':>11s}{'% of $6,076':>13s}")
for m in (1,2,5,10):
    o,pl,ps=res[m]
    marg=(pl+ps)*MARGIN
    gap=(pl+ps)*40*DPP
    lbl={1:'1 MES',2:'2 MES',5:'5 MES',10:'1 ES (10)'}[m]
    print(f"  {lbl:10s}{pl:>11.0f}{ps:>12.0f}{marg:>11,.0f}{marg/SAFE:>13,.0f}{gap:>+11,.0f}"
          f"{gap/6076.27*100:>12.0f}%")
print()
print("  (peak long and peak short are separate ACCOUNTS - margin is not shared)")
