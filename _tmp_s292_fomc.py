# -*- coding: utf-8 -*-
"""S292 - the room says 0DTE structure fails on FOMC days (fundamentals drive it).
Test it on our book: skip FOMC day / the day after / both. Replay includes the S203
underwater guard so the baseline is honest."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
N_SESS=117; CAL={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
fomc=[r[0] for r in c.execute(text("""select distinct (ts at time zone 'America/New_York')::date
   from economic_events where title in ('FOMC Statement','Federal Funds Rate','FOMC Press Conference')
   order by 1""")).all()]
sess=[r[0] for r in c.execute(text("""select distinct (ts at time zone 'America/New_York')::date d
   from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-03-01' order by d""")).all()]
c.close()
fomc=[d for d in fomc if d >= sess[0]]
after={sess[i+1] for i,d in enumerate(sess) if d in fomc and i+1 < len(sess)}
print(f"FOMC decision days in the window: {len(fomc)}  ->  {[str(d) for d in fomc]}")
print(f"days after            : {sorted(str(d) for d in after)}")
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')
def confirms(r):
    v=r.get('basket_pct')
    if v is None: return False
    v=float(v); return abs(v)>=DEAD and ((v>0)==r['is_long'])
def is_sc_short(r): return r['setup_name']=='Skew Charm' and not r['is_long']
basket=lambda r: 2 if confirms(r) else 1
def run(qtyfn, skipdays=frozenset(), breaker=0):
    openp,last,out=[],{},[]; streak={}; day=None
    for r in rows:
        if not lf.passes_v20(r,gaps): continue
        t=r['et']
        if t.date() in skipdays: continue
        if t.date()!=day: day=t.date(); streak={}
        openp=[p for p in openp if p[0]>t]
        n=sum(1 for p in openp if p[1]==r['is_long'])
        if n >= (2 if r['is_long'] else 3): continue
        k=(r['setup_name'],r['is_long'])
        if k in last and (t-last[k]).total_seconds()<90: continue
        if breaker and streak.get(k,0)>=breaker: continue
        sib=[p for p in openp if p[1]==r['is_long'] and p[3]==r['setup_name']]
        if len(sib)>=2 and r.get('spot'):
            sgn=1.0 if r['is_long'] else -1.0
            if sum((float(r['spot'])-p[2])*sgn for p in sib) < 0: continue
        last[k]=t; q=qtyfn(r,n); pts=float(r['outcome_pnl'])
        openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),
                      r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        streak[k]=streak.get(k,0)+1 if pts<=-13.9 else 0
        out.append({'d':t.date(),'setup':r['setup_name'],'q':q,'pts':pts,'net':(pts-HAIR)*q*DPP-FEE*q})
    df=pd.DataFrame(out); df['mo']=pd.to_datetime(df['d']).dt.strftime('%Y-%m'); return df
R0 = lambda r,n: basket(r)
R1D= lambda r,n: max(basket(r), 3 if (is_sc_short(r) and n>=2) else (2 if (is_sc_short(r) and n>=1) else 1))
print()
print("="*112); print("A. HOW DOES THE BOOK ACTUALLY DO ON FOMC DAYS?"); print("="*112)
b=run(R0); b['isf']=b['d'].isin(fomc); b['isa']=b['d'].isin(after)
for lbl,s in [('FOMC decision days',b[b['isf']]),('day AFTER FOMC',b[b['isa']]),
              ('every other day',b[~b['isf']&~b['isa']])]:
    dd=s.groupby('d')['net'].sum()
    print(f"  {lbl:22s} {len(dd):3d} days  {len(s):4d} trades  ${s['net'].sum():+8,.0f}  "
          f"${dd.mean() if len(dd) else 0:+7.0f}/day  green {(dd>0).mean()*100 if len(dd) else 0:3.0f}%")
print()
print("="*112); print("B. WHAT IF WE SKIP THEM?  (all include the S203 guard)"); print("="*112)
OPTS={'take everything (today)':frozenset(),'skip FOMC day':frozenset(fomc),
      'skip the day AFTER':frozenset(after),'skip BOTH':frozenset(fomc)|frozenset(after)}
print(f"  {'option':26s}{'trades':>8s}{'$/mo':>9s}{'min mo':>9s}{'MaxDD':>9s}{'worst day':>11s}")
for lbl,sk in OPTS.items():
    df=run(R0,sk)
    per=df.groupby('mo')['net'].sum()/pd.Series(CAL).reindex(sorted(df['mo'].unique()))*21
    d=df.groupby('d')['net'].sum(); eq=d.cumsum()
    print(f"  {lbl:26s}{len(df):>8d}{df['net'].sum()/N_SESS*21:>+9,.0f}{per.min():>+9,.0f}"
          f"{float((eq-eq.cummax()).min()):>+9,.0f}{d.min():>+11,.0f}")
print()
print("  day by day, every FOMC day and the day after:")
for d in sorted(set(fomc)|set(after)):
    s=b[b['d']==d]
    if len(s): print(f"    {d} {'FOMC ' if d in fomc else 'after'}  {len(s):3d} trades  ${s['net'].sum():+8,.0f}")
print()
print("="*112); print("C. THE FULL STACK — breaker + R1d + FOMC handling"); print("="*112)
print(f"  {'config':44s}{'trades':>8s}{'$/mo':>9s}{'min mo':>9s}{'MaxDD':>9s}{'worst day':>11s}")
FULL={
 'today (R0)':(R0,frozenset(),0),
 'R0 + breaker2':(R0,frozenset(),2),
 'R1d':(R1D,frozenset(),0),
 'R1d + breaker2':(R1D,frozenset(),2),
 'R1d + breaker2 + skip FOMC day':(R1D,frozenset(fomc),2),
 'R1d + breaker2 + skip FOMC & after':(R1D,frozenset(fomc)|frozenset(after),2),
}
for lbl,(fn,sk,br) in FULL.items():
    df=run(fn,sk,br)
    per=df.groupby('mo')['net'].sum()/pd.Series(CAL).reindex(sorted(df['mo'].unique()))*21
    d=df.groupby('d')['net'].sum(); eq=d.cumsum()
    print(f"  {lbl:44s}{len(df):>8d}{df['net'].sum()/N_SESS*21:>+9,.0f}{per.min():>+9,.0f}"
          f"{float((eq-eq.cummax()).min()):>+9,.0f}{d.min():>+11,.0f}")
