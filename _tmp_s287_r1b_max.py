# -*- coding: utf-8 -*-
"""S287 - the real code sizes with max(qty,2), NOT a multiplier.
`real_trader._effective_qty` says so explicitly. My R1b sim multiplied, which
invented 4-MES trades the live system would never place. Re-measure both ways."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
MARGIN, SAFE, N_SESS = 265.0, 0.70, 117
SHORT_EQ = 3271.61
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')
def confirms(r):
    v=r.get('basket_pct')
    if v is None: return False
    v=float(v)
    return abs(v)>=DEAD and ((v>0)==r['is_long'])
def is_sc_short(r): return r['setup_name']=='Skew Charm' and not r['is_long']
def run(qtyfn):
    openp,last,out=[],{},[]; peak_s=0
    for r in rows:
        if not lf.passes_v20(r,gaps): continue
        t=r['et']; openp=[p for p in openp if p[0]>t]
        n=sum(1 for p in openp if p[1]==r['is_long'])
        if n >= (2 if r['is_long'] else 3): continue
        k=(r['setup_name'],r['is_long'])
        if k in last and (t-last[k]).total_seconds()<90: continue
        last[k]=t; q=qtyfn(r,n)
        openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),r['is_long'],q))
        peak_s=max(peak_s, sum(p[2] for p in openp if not p[1]))
        pts=float(r['outcome_pnl'])
        out.append({'d':t.date(),'setup':r['setup_name'],'long':r['is_long'],'slot':n,'q':q,
                    'pts':pts,'net':(pts-HAIR)*q*DPP-FEE*q})
    return pd.DataFrame(out), peak_s
Q0   = lambda r,n: 2 if confirms(r) else 1                      # today
QMAX = lambda r,n: max(2 if confirms(r) else 1, 2 if (is_sc_short(r) and n>=1) else 1)   # max() = the real code
QMUL = lambda r,n: (2 if confirms(r) else 1) * (2 if (is_sc_short(r) and n>=1) else 1)   # what I simulated
Q3   = lambda r,n: max(2 if confirms(r) else 1, 3 if (is_sc_short(r) and n>=1) else 1)   # stacked -> 3, max()
cal={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}
print("="*112)
print("R1b UNDER THE TWO SIZING RULES  (117 sessions, V20, costs charged)")
print("="*112)
print(f"  {'variant':36s}{'$/mo':>9s}{'worst mo':>10s}{'MaxDD':>9s}{'pk SHORT':>10s}{'margin$':>9s}{'need eq':>9s}{'sizes used':>22s}")
for nm,fn in [('R0  today',Q0),('R1b-max  (what the code does)',QMAX),
              ('R1b-mult (what I simulated)',QMUL),('R1c-max  stacked -> 3',Q3)]:
    df,ps=run(fn); df['mo']=pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    per=df.groupby('mo')['net'].sum()/pd.Series(cal).reindex(sorted(df['mo'].unique()))*21
    d=df.groupby('d')['net'].sum(); eq=d.cumsum()
    sc=df[(df['setup']=='Skew Charm')&(~df['long'])]
    sizes=" ".join(f"{int(q)}x:{k}" for q,k in sc['q'].value_counts().sort_index().items())
    print(f"  {nm:36s}{df['net'].sum()/N_SESS*21:>+9,.0f}{per.min():>+10,.0f}"
          f"{float((eq-eq.cummax()).min()):>+9,.0f}{ps:>10.0f}{ps*MARGIN:>9,.0f}{ps*MARGIN/SAFE:>9,.0f}{sizes:>22s}")
print()
print("  Month by month, R1b-max (the implementable one) vs today:")
a,_=run(Q0); b,_=run(QMAX)
for x in (a,b): x['mo']=pd.to_datetime(x['d']).dt.strftime('%Y-%m')
A=a.groupby('mo')['net'].sum(); B=b.groupby('mo')['net'].sum()
for m in sorted(cal):
    print(f"    {m}   today ${A.get(m,0):+8,.0f}   R1b-max ${B.get(m,0):+8,.0f}   {B.get(m,0)-A.get(m,0):+8,.0f}  "
          f"{'HELPS' if B.get(m,0)>A.get(m,0) else ('SAME' if B.get(m,0)==A.get(m,0) else 'HURTS')}")
print()
print("  MARGIN HEADROOM — how often would the SHORT account be too small?")
for nm,fn in [('R0 today',Q0),('R1b-max',QMAX)]:
    df,ps=run(fn)
    print(f"    {nm:10s} peak {ps:>3.0f} MES = ${ps*MARGIN:,.0f} margin.  "
          f"short acct ${SHORT_EQ:,.0f} -> peak uses {ps*MARGIN/SHORT_EQ*100:.0f}% of it"
          f"  {'(over the 70% comfort line)' if ps*MARGIN/SHORT_EQ>SAFE else '(inside 70%)'}")
    print(f"               with +$1,000 -> {ps*MARGIN/(SHORT_EQ+1000)*100:.0f}% of the account")
