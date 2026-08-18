# -*- coding: utf-8 -*-
"""S307 - re-measure V21 using the prev-day move the LIVE filter can actually see
(chain_snapshots), not the spx_ohlc_1m series the study used."""
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
gaps=lf.load_gaps(c); moves=lf.load_prev_moves(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')

def run(v21):
    openp,last,out=[],{},[]; realized=0.0; d0=None; closed=[]
    for r in rows:
        t=r['et']
        if t.date()!=d0: d0=t.date(); realized=0.0; closed=[]
        for ct,nv in [x for x in closed if x[0]<=t]: realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not lf.passes_v20(r,gaps): continue
        if v21 and lf.v21_blocks(r,moves): continue
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
        closed.append((ct,net)); out.append({'d':t.date(),'net':net})
    o=pd.DataFrame(out); o['mo']=pd.to_datetime(o['d']).dt.strftime('%Y-%m'); return o

a=run(False); b=run(True)
def st(o):
    per=o.groupby('mo')['net'].sum()/pd.Series(CAL).reindex(sorted(o['mo'].unique()))*21
    dd=o.groupby('d')['net'].sum(); eq=dd.cumsum()
    bw=o[(o['d']>=pd.Timestamp('2026-06-05').date())&(o['d']<=pd.Timestamp('2026-06-12').date())]['net'].sum()
    return (len(o), o['net'].sum()/N_SESS*21, per.min(), float((eq-eq.cummax()).min()),
            dd.rolling(5).sum().min(), bw)
print("="*104)
print("V21 MEASURED ON THE DATA THE LIVE FILTER ACTUALLY USES (chain_snapshots)")
print("="*104)
print(f"  {'':16s}{'trades':>8s}{'$/mo':>9s}{'min mo':>9s}{'MaxDD':>9s}{'worst wk':>10s}{'bad week':>10s}")
for nm,o in (('V20',a),('V21 (chain)',b)):
    t,m,mn,dd,wk,bw=st(o)
    print(f"  {nm:16s}{t:>8d}{m:>+9,.0f}{mn:>+9,.0f}{dd:>+9,.0f}{wk:>+10,.0f}{bw:>+10,.0f}")
print()
print("  month by month:")
am=a.groupby('mo')['net'].sum(); bm=b.groupby('mo')['net'].sum()
wins=0
for m in sorted(CAL):
    x,y=am.get(m,0),bm.get(m,0)
    if y>=x-1: wins+=1
    print(f"    {m}   V20 ${x:+8,.0f}   V21 ${y:+8,.0f}   {y-x:+8,.0f}  "
          f"{'HELPS' if y>x+1 else ('SAME' if abs(y-x)<=1 else 'HURTS')}")
print(f"    LOMO {wins}/6")
print()
print("="*104); print("THE BLOCKED COHORT ON THIS BASIS"); print("="*104)
blk=[r for r in rows if lf.passes_v20(r,gaps) and lf.v21_blocks(r,moves)]
pts=np.array([float(r['outcome_pnl']) for r in blk])
se=pts.std(ddof=1)/np.sqrt(len(pts))
print(f"  n={len(pts)} on {len({r['et'].date() for r in blk})} days   mean {pts.mean():+.2f} pt   "
      f"WR {(pts>0).mean()*100:.0f}%   t={pts.mean()/se:+.2f}   total {pts.sum():+.1f} pts")
import collections
for d,g in sorted(collections.Counter(str(r['et'].date()) for r in blk).items()):
    gg=[float(r['outcome_pnl']) for r in blk if str(r['et'].date())==d]
    print(f"    {d}  {g:2d} shorts  {sum(gg):+7.1f} pts   prev {moves.get(d):+.2f}%")
