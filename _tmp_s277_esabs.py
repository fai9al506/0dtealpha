# -*- coding: utf-8 -*-
"""Is ES Absorption a bad setup? V16 book, chain sim, costs charged."""
import os, sys, pandas as pd, numpy as np
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York")
HAIRCUT, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
b=[]
for r in rows:
    if not lf.passes_v16(r,gaps): continue
    d=dict(r); d['et']=d['ts'].astimezone(ET).replace(tzinfo=None)
    d['is_long']=str(d.get('direction','')).lower() in ('long','bullish'); b.append(d)
b.sort(key=lambda x:x['et'])
def qty(r):
    v=r.get('basket_pct')
    if v is None: return 1
    v=float(v)
    if abs(v)<DEAD: return 1
    return 2 if ((v>0)==r['is_long']) else 1
# cap 2 long / 3 short + 90s dedup, same replay as S276
openp=[]; last={}; taken=[]
for r in b:
    t=r['et']; openp=[p for p in openp if p[0]>t]
    n=sum(1 for p in openp if p[1]==r['is_long'])
    if n >= (2 if r['is_long'] else 3): continue
    k=(r['setup_name'],r['is_long'])
    if k in last and (t-last[k]).total_seconds()<90: continue
    last[k]=t
    openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)), r['is_long']))
    q=qty(r); pts=float(r['outcome_pnl'])
    taken.append({'d':t.date(),'et':t,'setup':r['setup_name'],'long':r['is_long'],'grade':r.get('grade'),
                  'q':q,'pts':pts,'net':(pts-HAIRCUT)*q*DPP-FEE*q})
tk=pd.DataFrame(taken); tk['mo']=pd.to_datetime(tk['d']).dt.strftime('%Y-%m')
print("="*96); print("A. EVERY SETUP IN THE V16 BOOK (2026-03-01 -> today, cap 2/3, costs charged)"); print("="*96)
g=tk.groupby('setup').agg(n=('net','size'),total=('net','sum'),avg=('net','mean'),
                          wr=('pts',lambda s:(s>0).mean()*100)).sort_values('total',ascending=False)
for s,r in g.iterrows():
    print(f"  {s:20s} n={r['n']:4.0f}  total ${r['total']:+8,.0f}  avg ${r['avg']:+6.0f}/trade  WR {r['wr']:3.0f}%")
print(f"\n  BOOK TOTAL ${tk['net'].sum():+,.0f}")
ea=tk[tk['setup']=='ES Absorption']
print(f"  BOOK WITHOUT ES ABSORPTION ${tk[tk['setup']!='ES Absorption']['net'].sum():+,.0f}  "
      f"(ES Abs contributes ${ea['net'].sum():+,.0f})")
print(); print("="*96); print("B. ES ABSORPTION BY MONTH  (shorts were CUT from the filter on 2026-07-27, S229)"); print("="*96)
for mo,s in ea.groupby('mo'):
    L=s[s['long']]; S=s[~s['long']]
    print(f"  {mo}  n={len(s):3d}  total ${s['net'].sum():+7,.0f}  WR {(s['pts']>0).mean()*100:3.0f}%   "
          f"| longs n={len(L):3d} ${L['net'].sum():+7,.0f}  shorts n={len(S):3d} ${S['net'].sum():+7,.0f}")
print(); print("="*96); print("C. ES ABSORPTION LONGS ONLY, BY GRADE (this is what we trade today)"); print("="*96)
L=ea[ea['long']]
for gr,s in L.groupby('grade'):
    print(f"  grade {str(gr):4s}  n={len(s):3d}  total ${s['net'].sum():+7,.0f}  avg ${s['net'].mean():+6.0f}  WR {(s['pts']>0).mean()*100:3.0f}%")
print(f"  ALL LONGS      n={len(L):3d}  total ${L['net'].sum():+7,.0f}  avg ${L['net'].mean():+6.0f}  WR {(L['pts']>0).mean()*100:3.0f}%")
print(); print("  ES Abs LONGS since the shorts were cut (2026-07-27 ->):")
Lr=L[pd.to_datetime(L['d'])>=pd.Timestamp('2026-07-27')]
print(f"    n={len(Lr)}  total ${Lr['net'].sum():+,.0f}  WR {(Lr['pts']>0).mean()*100 if len(Lr) else 0:.0f}%")
print(); print("="*96); print("D. LEAVE-ONE-MONTH-OUT: would dropping ES Abs longs have helped in EVERY month?"); print("="*96)
for mo,s in L.groupby('mo'):
    print(f"    {mo}: n={len(s):3d}  ${s['net'].sum():+7,.0f}  -> dropping {'HELPS' if s['net'].sum()<0 else 'HURTS'}")

print(); print("="*96); print("E. IS THE RECENT FADE REAL, OR NOISE?"); print("="*96)
import numpy as np
rec = L[pd.to_datetime(L['d'])>=pd.Timestamp('2026-05-01')]
old = L[pd.to_datetime(L['d'])< pd.Timestamp('2026-05-01')]
for nm,s in [('Mar-Apr', old), ('May-Aug', rec)]:
    se = s['net'].std(ddof=1)/np.sqrt(len(s))
    print(f"  {nm:8s} n={len(s):3d}  total ${s['net'].sum():+7,.0f}  avg ${s['net'].mean():+6.1f}/trade  "
          f"SE ${se:5.1f}  t={s['net'].mean()/se:+5.2f}  WR {(s['pts']>0).mean()*100:3.0f}%")
print("  -> a |t| under ~2 means we cannot tell it apart from zero.")
print()
print("  Today's arithmetic the user asked about:")
td = tk[tk['d']==pd.Timestamp('2026-08-17').date()]
print(f"    day with ES Abs    ${td['net'].sum():+.2f}")
print(f"    day without ES Abs ${td[td['setup']!='ES Absorption']['net'].sum():+.2f}")
