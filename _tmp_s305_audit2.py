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



import itertools
rng=np.random.default_rng(20260817)
CAL2={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}

def run(skipset=None, skipfn=None):
    openp,last,out=[],{},[]; realized=0.0; d0=None; closed=[]
    for r in rows:
        t=r['et']
        if t.date()!=d0: d0=t.date(); realized=0.0; closed=[]
        for ct,nv in [x for x in closed if x[0]<=t]: realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not lf.passes_v20(r,gaps): continue
        if realized<=DAILY: continue
        if skipset is not None and r['id'] in skipset: continue
        if skipfn is not None and skipfn(r, PR.get(t.date())): continue
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
        closed.append((ct,net)); out.append({'d':t.date(),'net':net})
    o=pd.DataFrame(out); o['mo']=pd.to_datetime(o['d']).dt.strftime('%Y-%m'); return o

def rule(r,p):
    if r['is_long'] or p is None: return False
    v=r.get('vix')
    return v is not None and p < -0.8 and float(v) < 24

def stats(o):
    dd=o.groupby('d')['net'].sum(); eq=dd.cumsum()
    return (o['net'].sum()/117*21, float((eq-eq.cummax()).min()), dd.rolling(5).sum().min(),
            (o.groupby('mo')['net'].sum()/pd.Series(CAL2).reindex(sorted(o['mo'].unique()))*21).min())

base=run(); r21=run(skipfn=rule)
print('='*98); print('AUDIT 1 - HOW MANY DAYS DOES THE RULE ACTUALLY TOUCH?'); print('='*98)
skipped=[r for r in rows if lf.passes_v20(r,gaps) and rule(r, PR.get(r['et'].date()))]
days=sorted({r['et'].date() for r in skipped})
print(f'  trades skipped: {len(skipped)}   on {len(days)} separate days   out of 117 sessions')
print('  the days, and what those shorts were worth:')
for d in days:
    g=[r for r in skipped if r['et'].date()==d]
    p=sum(float(x['outcome_pnl']) for x in g)
    print(f'    {d}  {len(g):2d} shorts  {p:+7.1f} pts   prev day {PR.get(d):+.2f}%   vix ~{np.mean([float(x["vix"]) for x in g]):.1f}')
print()
print('='*98); print('AUDIT 2 - IS THE DRAWDOWN GAIN FROM ONE DAY?'); print('='*98)
b=base.groupby('d')['net'].sum(); a=r21.groupby('d')['net'].sum()
diff=(a-b).dropna(); diff=diff[diff.abs()>1]
print(f'  days whose P&L changed at all: {len(diff)}')
for d,v in diff.sort_values().items():
    print(f'    {d}  {v:+8.0f}')
print()
print('='*98); print('AUDIT 3 - SIGNIFICANCE OF THE SKIPPED COHORT'); print('='*98)
pts=np.array([float(r['outcome_pnl']) for r in skipped])
se=pts.std(ddof=1)/np.sqrt(len(pts))
print(f'  n={len(pts)}  mean {pts.mean():+.2f} pt  sd {pts.std(ddof=1):.2f}  SE {se:.2f}  t={pts.mean()/se:+.2f}')
print(f'  win rate {(pts>0).mean()*100:.0f}%   vs the book-wide short WR')
allsh=[float(r['outcome_pnl']) for r in rows if lf.passes_v20(r,gaps) and not r['is_long']]
print(f'  all V20 shorts: n={len(allsh)} mean {np.mean(allsh):+.2f} pt WR {(np.array(allsh)>0).mean()*100:.0f}%')
print()
print('='*98); print('AUDIT 4 - PLACEBO. Remove 27 RANDOM shorts, 500 times. Where does the rule rank?'); print('='*98)
shorts=[r['id'] for r in rows if lf.passes_v20(r,gaps) and not r['is_long']]
bm,bdd,bwk,bmin=stats(base); rm,rdd,rwk,rmin=stats(r21)
sims=[]
for i in range(500):
    ss=set(rng.choice(shorts, size=len(skipped), replace=False).tolist())
    sims.append(stats(run(skipset=ss)))
sims=np.array(sims)
print(f'  {"metric":14s}{"V20":>10s}{"rule":>10s}{"random mean":>14s}{"random sd":>12s}{"rule beats":>12s}')
for i,(nm,rv,bv,better) in enumerate([('$/month',rm,bm,True),('MaxDD',rdd,bdd,True),
                                       ('worst week',rwk,bwk,True),('min month',rmin,bmin,True)]):
    col=sims[:,i]
    pct=(rv>col).mean()*100 if better else (rv<col).mean()*100
    print(f'  {nm:14s}{bv:>+10,.0f}{rv:>+10,.0f}{col.mean():>+14,.0f}{col.std():>12,.0f}{pct:>11.0f}%')
print()
print('  (a real rule should beat ~95%+ of random removals of the same size)')
