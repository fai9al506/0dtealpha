# -*- coding: utf-8 -*-
"""S291 - the July losers all had a STRONGLY contradicting tech basket (+1.1 to +2.3
while short; deadband is 0.15). Does a strength threshold help, and does it hold in
every month? Replay includes the S203 guard so the baseline is honest."""
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
c.close()
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')
def bp(r):
    v=r.get('basket_pct'); return None if v is None else float(v)
def contradiction(r):
    """how strongly the basket disagrees with this trade, in %, 0 if it agrees."""
    v=bp(r)
    if v is None: return 0.0
    if abs(v) < DEAD: return 0.0
    return abs(v) if ((v>0) != r['is_long']) else 0.0
def confirms(r):
    v=bp(r)
    return v is not None and abs(v)>=DEAD and ((v>0)==r['is_long'])
def is_sc_short(r): return r['setup_name']=='Skew Charm' and not r['is_long']
basket=lambda r: 2 if confirms(r) else 1
def run(qtyfn, skip_thr=None):
    openp,last,out=[],{},[]
    for r in rows:
        if not lf.passes_v20(r,gaps): continue
        if skip_thr is not None and contradiction(r) >= skip_thr: continue
        t=r['et']; openp=[p for p in openp if p[0]>t]
        n=sum(1 for p in openp if p[1]==r['is_long'])
        if n >= (2 if r['is_long'] else 3): continue
        k=(r['setup_name'],r['is_long'])
        if k in last and (t-last[k]).total_seconds()<90: continue
        sib=[p for p in openp if p[1]==r['is_long'] and p[3]==r['setup_name']]
        if len(sib)>=2 and r.get('spot'):
            sgn=1.0 if r['is_long'] else -1.0
            if sum((float(r['spot'])-p[2])*sgn for p in sib) < 0: continue
        last[k]=t; q=qtyfn(r,n); pts=float(r['outcome_pnl'])
        openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),
                      r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        out.append({'d':t.date(),'setup':r['setup_name'],'long':r['is_long'],'q':q,'pts':pts,
                    'net':(pts-HAIR)*q*DPP-FEE*q})
    df=pd.DataFrame(out); df['mo']=pd.to_datetime(df['d']).dt.strftime('%Y-%m'); return df
R0 = lambda r,n: basket(r)
R1D= lambda r,n: max(basket(r), 3 if (is_sc_short(r) and n>=2) else (2 if (is_sc_short(r) and n>=1) else 1))
print("="*112)
print("A. SKIP A TRADE WHOSE BASKET CONTRADICTS BY MORE THAN X%   (baseline includes S203)")
print("="*112)
print(f"  {'threshold':14s}{'trades':>8s}{'$/mo':>9s}{'min mo':>9s}{'JULY':>9s}{'MaxDD':>9s}{'worst day':>11s}{'blocked':>9s}")
base=run(R0)
for thr in [None,2.0,1.5,1.2,1.0,0.8,0.6,0.4]:
    df=run(R0,thr)
    per=df.groupby('mo')['net'].sum()/pd.Series(CAL).reindex(sorted(df['mo'].unique()))*21
    d=df.groupby('d')['net'].sum(); eq=d.cumsum()
    lbl='none (today)' if thr is None else f'>= {thr:.1f}%'
    print(f"  {lbl:14s}{len(df):>8d}{df['net'].sum()/N_SESS*21:>+9,.0f}{per.min():>+9,.0f}"
          f"{per.get('2026-07',0):>+9,.0f}{float((eq-eq.cummax()).min()):>+9,.0f}{d.min():>+11,.0f}"
          f"{len(base)-len(df):>9d}")
print()
print("="*112); print("B. HOW MANY TRADES DO WE EVEN HAVE AT EACH CONTRADICTION LEVEL, AND ARE THEY BAD?"); print("="*112)
allt=[]
for r in rows:
    if not lf.passes_v20(r,gaps): continue
    allt.append({'c':contradiction(r),'pts':float(r['outcome_pnl']),'long':r['is_long'],
                 'mo':r['et'].strftime('%Y-%m')})
A=pd.DataFrame(allt)
print(f"  {'contradiction':18s}{'n':>6s}{'avg pt':>9s}{'WR':>7s}")
for lo,hi,lbl in [(0,0.001,'agrees/neutral'),(0.001,0.5,'0-0.5%'),(0.5,1.0,'0.5-1%'),
                  (1.0,1.5,'1-1.5%'),(1.5,2.0,'1.5-2%'),(2.0,99,'over 2%')]:
    s=A[(A['c']>=lo)&(A['c']<hi)]
    if len(s): print(f"  {lbl:18s}{len(s):>6d}{s['pts'].mean():>+9.2f}{(s['pts']>0).mean()*100:>6.0f}%")
print()
print("="*112); print("C. LEAVE-ONE-MONTH-OUT for the best threshold, with and without R1d sizing"); print("="*112)
for thr in [1.5,1.2,1.0]:
    a=run(R0); b=run(R0,thr); e=run(R1D,thr)
    A2=a.groupby('mo')['net'].sum(); B2=b.groupby('mo')['net'].sum(); E2=e.groupby('mo')['net'].sum()
    wins=sum(1 for m in CAL if B2.get(m,0) >= A2.get(m,0))
    print(f"\n  threshold {thr:.1f}%  ->  R0 ${a['net'].sum()/N_SESS*21:+,.0f}/mo   "
          f"skip ${b['net'].sum()/N_SESS*21:+,.0f}/mo   skip+R1d ${e['net'].sum()/N_SESS*21:+,.0f}/mo   "
          f"months not worse {wins}/6")
    for m in sorted(CAL):
        print(f"    {m}  today ${A2.get(m,0):+8,.0f}   skip ${B2.get(m,0):+8,.0f}   "
              f"skip+R1d ${E2.get(m,0):+8,.0f}")
