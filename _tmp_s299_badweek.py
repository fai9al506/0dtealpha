# -*- coding: utf-8 -*-
"""S299 - the 2026-06-05 -> 06-12 streak: 6 sessions, all red, -$1,414 real money.
What does the CURRENT configuration do to that exact window?"""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
DAILY=-300.0
A,B='2026-06-05','2026-06-12'
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='{A}' AND (ts AT TIME ZONE 'America/New_York')<'{B} 23:59'
  AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
brk=pd.read_sql(text(f"select day, net from tsrt_daily_stmt where day between '{A}' and '{B}' order by day"),c)
c.close()
brk['net']=brk['net'].astype(float)
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')
def is_fri(r): return r['et'].weekday()==4
def esabs_low(r): return r['setup_name']=='ES Absorption' and (r.get('vix') is None or float(r['vix'])<20)

def run(pred, daily=True):
    openp,last,out=[],{},[]; realized=0.0; day=None; closed=[]
    for r in rows:
        t=r['et']
        if t.date()!=day: day=t.date(); realized=0.0; closed=[]
        for ct,nv in [x for x in closed if x[0]<=t]: realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not pred(r): continue
        if daily and realized<=DAILY: continue
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
        q=1 if v is None or abs(float(v))<DEAD else (2 if ((float(v)>0)==r['is_long']) else 1)
        pts=float(r['outcome_pnl']); net=(pts-HAIR)*q*DPP-FEE*q
        ct=t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct,r['is_long'],float(r['spot']) if r.get('spot') else 0.0,r['setup_name'],q))
        closed.append((ct,net))
        out.append({'d':t.date(),'setup':r['setup_name'],'long':r['is_long'],'fri':is_fri(r),
                    'q':q,'pts':pts,'net':net,'vix':pd.to_numeric(r.get('vix'),errors='coerce')})
    return pd.DataFrame(out)

v16=run(lambda r: lf.passes_v16(r,gaps))
v20=run(lambda r: lf.passes_v20(r,gaps))
print("="*92)
print(f"THE BAD WEEK: {A} -> {B}   (6 sessions, ALL RED, broker ${brk['net'].sum():+,.2f})")
print("="*92)
print(f"  {'day':12s}{'BROKER':>10s}{'V16 sim':>10s}{'V20 sim':>10s}{'V20 trades':>12s}")
for d in sorted(set(v16['d'])|set(brk['day'])):
    bv=brk[brk['day']==d]['net']
    a1=v16[v16['d']==d]['net'].sum(); b1=v20[v20['d']==d]['net'].sum()
    n=len(v20[v20['d']==d])
    print(f"  {str(d):12s}{(bv.iloc[0] if len(bv) else 0):>+10,.0f}{a1:>+10,.0f}{b1:>+10,.0f}{n:>12d}")
print(f"  {'TOTAL':12s}{brk['net'].sum():>+10,.0f}{v16['net'].sum():>+10,.0f}{v20['net'].sum():>+10,.0f}{len(v20):>12d}")
print()
print("="*92); print("WHAT V20 REMOVES FROM THIS WEEK"); print("="*92)
fri=v16[v16['fri']]; es=v16[(v16['setup']=='ES Absorption')&((v16['vix'].isna())|(v16['vix']<20))]
print(f"  Fridays deleted            : {len(fri):3d} trades  ${fri['net'].sum():+8,.0f}")
print(f"  ES Absorption below VIX 20 : {len(es):3d} trades  ${es['net'].sum():+8,.0f}")
print()
print("="*92); print("WHO LOST THE MONEY THAT WEEK (V16 book, by setup)"); print("="*92)
print(f"  {'setup':18s}{'n':>5s}{'net $':>10s}{'WR':>6s}   |  {'under V20':>10s}")
for s,g in v16.groupby('setup'):
    g2=v20[v20['setup']==s]
    print(f"  {s:18s}{len(g):>5d}{g['net'].sum():>+10,.0f}{(g['pts']>0).mean()*100:>5.0f}%   |  "
          f"{g2['net'].sum():>+10,.0f} ({len(g2)}t)")
print()
print("="*92); print("AND THE SAME WEEK WITH THE SEPTEMBER SIZING (R1d)"); print("="*92)
def run_r1d():
    openp,last,out=[],{},[]; realized=0.0; day=None; closed=[]
    for r in rows:
        t=r['et']
        if t.date()!=day: day=t.date(); realized=0.0; closed=[]
        for ct,nv in [x for x in closed if x[0]<=t]: realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not lf.passes_v20(r,gaps): continue
        if realized<=DAILY: continue
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
        q=1 if v is None or abs(float(v))<DEAD else (2 if ((float(v)>0)==r['is_long']) else 1)
        if r['setup_name']=='Skew Charm' and not r['is_long'] and n>=1: q=max(q, 3 if n>=2 else 2)
        pts=float(r['outcome_pnl']); net=(pts-HAIR)*q*DPP-FEE*q
        ct=t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct,r['is_long'],float(r['spot']) if r.get('spot') else 0.0,r['setup_name'],q))
        closed.append((ct,net)); out.append({'d':t.date(),'net':net})
    return pd.DataFrame(out)
r1=run_r1d()
print(f"  V20 + R1d over the same 6 sessions: ${r1['net'].sum():+,.0f}   "
      f"(worst day ${r1.groupby('d')['net'].sum().min():+,.0f})")
