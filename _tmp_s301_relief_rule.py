# -*- coding: utf-8 -*-
"""S301 - turn the relief-rally finding into a rule and test it like every other:
full replay (V20 + cap + dedup + S203 + $300 breaker), month by month, LOMO."""
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
px=pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d, ts at time zone 'America/New_York' et,
    bar_open, bar_close from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-02-19' order by ts"""),c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
px['d']=pd.to_datetime(px['d']).dt.date
g=px.groupby('d')
day=pd.DataFrame({'open':g['bar_open'].first(),'close':g['bar_close'].last()}).reset_index()
day['ret']=(day['close']-day['open'])/day['open']*100
day['prev']=day['ret'].shift(1); day['prev2']=day['ret'].shift(2)
PR=dict(zip(day['d'],day['prev'])); PR2=dict(zip(day['d'],day['prev2']))
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')

def run(skip):
    openp,last,out=[],{},[]; realized=0.0; d0=None; closed=[]
    for r in rows:
        t=r['et']
        if t.date()!=d0: d0=t.date(); realized=0.0; closed=[]
        for ct,nv in [x for x in closed if x[0]<=t]: realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not lf.passes_v20(r,gaps): continue
        if realized<=DAILY: continue
        if skip(r, PR.get(t.date()), PR2.get(t.date())): continue
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
        closed.append((ct,net)); out.append({'d':t.date(),'net':net,'long':r['is_long']})
    df=pd.DataFrame(out); df['mo']=pd.to_datetime(df['d']).dt.strftime('%Y-%m'); return df

NONE=lambda r,p,p2: False
def mk(thr):  return lambda r,p,p2: (not r['is_long']) and p is not None and p < thr
TWO  = lambda r,p,p2: (not r['is_long']) and p is not None and p2 is not None and p<-0.3 and p2<-0.3
RULES={'V20 as-is':NONE,'skip shorts if prev < -0.3%':mk(-0.3),'skip shorts if prev < -0.5%':mk(-0.5),
       'skip shorts if prev < -0.8%':mk(-0.8),'skip shorts if prev < -1.0%':mk(-1.0),
       'skip shorts after TWO down days':TWO}
print("="*112)
print("THE RELIEF-RALLY RULE — full replay, V20 + cap + S203 + $300 breaker")
print("="*112)
print(f"  {'rule':34s}{'trades':>8s}{'$/mo':>9s}{'min mo':>9s}{'max mo':>9s}{'MaxDD':>9s}{'worst day':>11s}{'worst wk':>10s}")
res={}
for k,f in RULES.items():
    df=run(f); res[k]=df
    per=df.groupby('mo')['net'].sum()/pd.Series(CAL).reindex(sorted(df['mo'].unique()))*21
    dd=df.groupby('d')['net'].sum(); eq=dd.cumsum()
    print(f"  {k:34s}{len(df):>8d}{df['net'].sum()/N_SESS*21:>+9,.0f}{per.min():>+9,.0f}{per.max():>+9,.0f}"
          f"{float((eq-eq.cummax()).min()):>+9,.0f}{dd.min():>+11,.0f}{dd.rolling(5).sum().min():>+10,.0f}")
print()
print("="*112); print("MONTH BY MONTH — the leave-one-month-out test"); print("="*112)
base=res['V20 as-is'].groupby('mo')['net'].sum()
print(f"  {'month':9s}" + "".join(f"{k.split('prev')[-1].strip() if 'prev' in k else ('TWO' if 'TWO' in k else 'as-is'):>14s}" for k in RULES))
for m in sorted(CAL):
    line=f"  {m:9s}"
    for k in RULES: line+=f"{res[k].groupby('mo')['net'].sum().get(m,0):>+14,.0f}"
    print(line)
print(f"  {'wins/6':9s}" + "".join(
    f"{sum(1 for m in CAL if res[k].groupby('mo')['net'].sum().get(m,0) >= base.get(m,0)):>13d}/6" for k in RULES))
print()
print("="*112); print("AND THE BAD WEEK (2026-06-05 -> 06-12)"); print("="*112)
for k in RULES:
    d=res[k]; w=d[(d['d']>=pd.Timestamp('2026-06-05').date())&(d['d']<=pd.Timestamp('2026-06-12').date())]
    print(f"  {k:34s} ${w['net'].sum():+8,.0f}   ({len(w)} trades)")
