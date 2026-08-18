# -*- coding: utf-8 -*-
"""S302 - AUDIT the relief-rally rule before anyone trades it.
The user's worry: "when the market goes down sometimes it goes MORE down, and we would
be skipping good shorts." That is the right question. Eight checks."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
DAILY=-300.0; N_SESS=117
CAL={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
px=pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d, bar_open, bar_close
    from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-02-19' order by ts"""),c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
px['d']=pd.to_datetime(px['d']).dt.date
g=px.groupby('d')
day=pd.DataFrame({'open':g['bar_open'].first(),'close':g['bar_close'].last()}).reset_index()
day['ret']=(day['close']-day['open'])/day['open']*100
day['prev']=day['ret'].shift(1)
PR=dict(zip(day['d'],day['prev']))
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')



CAL2={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}
def run(skipfn, mult=1.0, daily=-300.0):
    openp,last,out=[],{},[]; realized=0.0; d0=None; closed=[]
    for r in rows:
        t=r['et']
        if t.date()!=d0: d0=t.date(); realized=0.0; closed=[]
        for ct,nv in [x for x in closed if x[0]<=t]: realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not lf.passes_v20(r,gaps): continue
        if realized<=daily: continue
        if skipfn(r, PR.get(t.date())): continue
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
    o=pd.DataFrame(out); o['mo']=pd.to_datetime(o['d']).dt.strftime('%Y-%m'); return o
NONE=lambda r,p: False
def rule(r,p):
    if r['is_long'] or p is None: return False
    v=r.get('vix'); return v is not None and p < -0.8 and float(v) < 24
BAD=(pd.Timestamp('2026-06-05').date(), pd.Timestamp('2026-06-12').date())
print('='*100)
print('THE JUNE BAD WEEK AT DIFFERENT SIZES  (breaker scaled with size, as it must be)')
print('='*100)
print(f"  {'size':10s}{'breaker':>10s}{'V20 week':>12s}{'V21 week':>12s}{'saved':>11s}{'V20 MaxDD':>12s}{'V21 MaxDD':>12s}")
for mult,lab in [(1,'1 MES'),(2,'2 MES'),(5,'5 MES'),(10,'1 ES (10x)'),(20,'2 ES (20x)')]:
    dl=-300.0*mult
    a=run(NONE,mult,dl); b=run(rule,mult,dl)
    aw=a[(a['d']>=BAD[0])&(a['d']<=BAD[1])]['net'].sum()
    bw=b[(b['d']>=BAD[0])&(b['d']<=BAD[1])]['net'].sum()
    ad=a.groupby('d')['net'].sum().cumsum(); bd=b.groupby('d')['net'].sum().cumsum()
    print(f'  {lab:10s}{dl:>+10,.0f}{aw:>+12,.0f}{bw:>+12,.0f}{bw-aw:>+11,.0f}'
          f"{float((ad-ad.cummax()).min()):>+12,.0f}{float((bd-bd.cummax()).min()):>+12,.0f}")
print()
print('='*100)
print('AND IF THE BREAKER IS LEFT AT $300 WHILE SIZE GROWS')
print('='*100)
print(f"  {'size':10s}{'breaker':>10s}{'trades taken':>14s}{'$/mo':>10s}   note")
for mult,lab in [(1,'1 MES'),(2,'2 MES'),(10,'1 ES (10x)')]:
    a=run(NONE,mult,-300.0)
    print(f'  {lab:10s}{-300:>+10,.0f}{len(a):>14d}{a["net"].sum()/117*21:>+10,.0f}'
          f"   {'fine' if mult==1 else 'breaker trips on the FIRST bad trade'}")
