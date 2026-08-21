# -*- coding: utf-8 -*-
"""S290 - the replay was missing the S203 underwater-stack guard that the live system
runs. Add it, then test a day-level circuit breaker for the trend-day re-fire problem
that cost July (07-30: SIX full stops in a row as SPX ground up)."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
N_SESS=117; MARGIN=265.0
CAL={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
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

def run(qtyfn, s203=False, breaker=0):
    """s203: block a 3rd same-setup same-dir entry when the open ones are net losing.
       breaker=N: after N consecutive STOPPED trades of the same setup+direction on a
       day, take no more of them that day."""
    openp,last,out=[],{},[]; peak_s=0
    streak={}; day=None
    for r in rows:
        if not lf.passes_v20(r,gaps): continue
        t=r['et']
        if t.date()!=day: day=t.date(); streak={}
        openp=[p for p in openp if p[0]>t]
        n=sum(1 for p in openp if p[1]==r['is_long'])
        if n >= (2 if r['is_long'] else 3): continue
        k=(r['setup_name'],r['is_long'])
        if k in last and (t-last[k]).total_seconds()<90: continue
        if breaker and streak.get(k,0) >= breaker: continue
        if s203:
            sib=[p for p in openp if p[1]==r['is_long'] and p[3]==r['setup_name']]
            if len(sib) >= 2:
                sgn = 1.0 if r['is_long'] else -1.0
                spot=float(r['spot']) if r.get('spot') else None
                if spot is not None:
                    unreal=sum((spot-fp)*sgn for _,_,fp,_ in [(a,b,cc,d) for a,b,cc,d in
                               [(p[0],p[1],p[2],p[3]) for p in sib]])
                    if unreal < 0: continue
        last[k]=t; q=qtyfn(r,n); pts=float(r['outcome_pnl'])
        openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),
                      r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        peak_s=max(peak_s, sum(p[4] for p in openp if not p[1]))
        streak[k] = streak.get(k,0)+1 if pts <= -13.9 else 0
        out.append({'d':t.date(),'setup':r['setup_name'],'long':r['is_long'],'slot':n,'q':q,
                    'pts':pts,'net':(pts-HAIR)*q*DPP-FEE*q})
    df=pd.DataFrame(out); df['mo']=pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    return df, peak_s

R0   = lambda r,n: basket(r)
R1D  = lambda r,n: max(basket(r), 3 if (is_sc_short(r) and n>=2) else (2 if (is_sc_short(r) and n>=1) else 1))
CASES = {
 'R0  no guard modelled':        (R0,  False, 0),
 'R0  + S203 guard (REAL)':      (R0,  True,  0),
 'R0  + S203 + breaker 2':       (R0,  True,  2),
 'R0  + S203 + breaker 3':       (R0,  True,  3),
 'R1d no guard modelled':        (R1D, False, 0),
 'R1d + S203 guard (REAL)':      (R1D, True,  0),
 'R1d + S203 + breaker 2':       (R1D, True,  2),
 'R1d + S203 + breaker 3':       (R1D, True,  3),
}
print("="*118)
print("A. WHAT THE GUARDS DO  (117 sessions, V20, costs charged)")
print("="*118)
print(f"  {'case':30s}{'trades':>8s}{'$/mo':>9s}{'min mo':>9s}{'JULY':>9s}{'MaxDD':>9s}{'worst day':>11s}{'pk short':>9s}")
res={}
for k,(fn,s203,br) in CASES.items():
    df,ps=run(fn,s203,br); res[k]=df
    per=df.groupby('mo')['net'].sum()/pd.Series(CAL).reindex(sorted(df['mo'].unique()))*21
    d=df.groupby('d')['net'].sum(); eq=d.cumsum()
    print(f"  {k:30s}{len(df):>8d}{df['net'].sum()/N_SESS*21:>+9,.0f}{per.min():>+9,.0f}"
          f"{per.get('2026-07',0):>+9,.0f}{float((eq-eq.cummax()).min()):>+9,.0f}{d.min():>+11,.0f}{ps:>9.0f}")
print()
print("="*118); print("B. MONTH BY MONTH — the two best candidates vs today"); print("="*118)
print(f"  {'month':9s}" + "".join(f"{k[:22]:>24s}" for k in ['R0  + S203 guard (REAL)','R1d + S203 guard (REAL)','R1d + S203 + breaker 2']))
for m in sorted(CAL):
    line=f"  {m:9s}"
    for k in ['R0  + S203 guard (REAL)','R1d + S203 guard (REAL)','R1d + S203 + breaker 2']:
        line += f"{res[k].groupby('mo')['net'].sum().get(m,0):>+24,.0f}"
    print(line)
print()
print("="*118); print("C. THE BREAKER — what does it actually block?"); print("="*118)
a=res['R1d + S203 guard (REAL)']; b=res['R1d + S203 + breaker 2']
print(f"  trades removed by breaker-2: {len(a)-len(b)}")
for m in sorted(CAL):
    av=a.groupby('mo')['net'].sum().get(m,0); bv=b.groupby('mo')['net'].sum().get(m,0)
    print(f"    {m}  ${av:+8,.0f} -> ${bv:+8,.0f}   {bv-av:+8,.0f}  "
          f"{'HELPS' if bv>av else ('SAME' if bv==av else 'HURTS')}")
