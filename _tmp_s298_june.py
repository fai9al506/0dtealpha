# -*- coding: utf-8 -*-
"""S298 - June was -$1,088 real. V20 simulates -$225. Where does each dollar of that
difference come from? Decompose it honestly - not all of it is V20."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
DAILY=-300.0
JUN0,JUN1=pd.Timestamp('2026-06-01').date(), pd.Timestamp('2026-07-01').date()
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-06-01'
    AND (ts AT TIME ZONE 'America/New_York')<'2026-07-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
brk=pd.read_sql(text("select day, net, n_trades from tsrt_daily_stmt where day >= '2026-06-01' and day < '2026-07-01' order by day"),c)
c.close()
brk['net']=brk['net'].astype(float)
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')

def is_fri(r): return r['et'].weekday()==4
def esabs_lowvix(r):
    return r['setup_name']=='ES Absorption' and (r.get('vix') is None or float(r['vix'])<20)

def run(pred):
    openp,last,out=[],{},[]; realized=0.0; day=None; closed=[]
    for r in rows:
        t=r['et']
        if t.date()!=day: day=t.date(); realized=0.0; closed=[]
        for ct,nv in [x for x in closed if x[0]<=t]: realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not pred(r): continue
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
        pts=float(r['outcome_pnl']); net=(pts-HAIR)*q*DPP-FEE*q
        ct=t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct,r['is_long'],float(r['spot']) if r.get('spot') else 0.0,r['setup_name'],q))
        closed.append((ct,net))
        out.append({'d':t.date(),'setup':r['setup_name'],'long':r['is_long'],'fri':is_fri(r),
                    'q':q,'pts':pts,'net':net,'vix':pd.to_numeric(r.get('vix'),errors='coerce')})
    return pd.DataFrame(out)

V16 = lambda r: lf.passes_v16(r,gaps)
V16_NOFRI = lambda r: V16(r) and not is_fri(r)
V16_ESABS = lambda r: V16(r) and not esabs_lowvix(r)
V20 = lambda r: lf.passes_v20(r,gaps)

print("="*100)
print("JUNE 2026 — STEP BY STEP FROM V16 TO V20")
print("="*100)
a=run(V16); b=run(V16_ESABS); cq=run(V16_NOFRI); d=run(V20)
print(f"  {'config':34s}{'trades':>8s}{'net $':>10s}{'change':>10s}")
print(f"  {'V16 (what June ran)':34s}{len(a):>8d}{a['net'].sum():>+10,.0f}{'':>10s}")
print(f"  {'V16 + ES Abs VIX>=20 only':34s}{len(b):>8d}{b['net'].sum():>+10,.0f}{b['net'].sum()-a['net'].sum():>+10,.0f}")
print(f"  {'V16 + no Friday only':34s}{len(cq):>8d}{cq['net'].sum():>+10,.0f}{cq['net'].sum()-a['net'].sum():>+10,.0f}")
print(f"  {'V20 = both':34s}{len(d):>8d}{d['net'].sum():>+10,.0f}{d['net'].sum()-a['net'].sum():>+10,.0f}")
print()
print("="*100)
print("WHAT EXACTLY DID V20 REMOVE FROM JUNE?")
print("="*100)
rm_es=a[(a['setup']=='ES Absorption')&((a['vix'].isna())|(a['vix']<20))]
rm_fr=a[a['fri']]
print(f"  ES Absorption below VIX 20 : {len(rm_es):3d} trades  ${rm_es['net'].sum():+8,.0f}  "
      f"WR {(rm_es['pts']>0).mean()*100 if len(rm_es) else 0:3.0f}%")
print(f"  everything on a Friday     : {len(rm_fr):3d} trades  ${rm_fr['net'].sum():+8,.0f}  "
      f"WR {(rm_fr['pts']>0).mean()*100 if len(rm_fr) else 0:3.0f}%")
both=a[((a['setup']=='ES Absorption')&((a['vix'].isna())|(a['vix']<20)))|(a['fri'])]
print(f"  removed in total           : {len(both):3d} trades  ${both['net'].sum():+8,.0f}")
print()
print("  the Fridays V20 deletes:")
for dd,s in rm_fr.groupby('d'):
    print(f"    {dd}  {len(s):2d} trades  ${s['net'].sum():+8,.0f}")
print()
print("="*100)
print("BUT V20 IS NOT THE WHOLE STORY — the honest reconciliation")
print("="*100)
print(f"  what the BROKER actually paid in June       ${brk['net'].sum():+8,.0f}   ({len(brk)} days, {int(brk['n_trades'].sum())} trades)")
print(f"  the SAME month simulated on V16             ${a['net'].sum():+8,.0f}   ({len(a)} trades)")
print(f"    -> gap between real and V16-sim           ${brk['net'].sum()-a['net'].sum():+8,.0f}")
print(f"  V20 improvement on top of V16               ${d['net'].sum()-a['net'].sum():+8,.0f}")
print(f"  V20 simulated June                          ${d['net'].sum():+8,.0f}")
print()
print("  so of the -$1,088 -> -$225 difference:")
print(f"    ${d['net'].sum()-a['net'].sum():+,.0f} is V20 (removing ES Abs low-VIX + Fridays)")
print(f"    ${brk['net'].sum()-a['net'].sum():+,.0f} is everything else: old filter versions live that month,")
print(f"            basket GATE vs sizing, execution, and days we traded that V16 would not")
