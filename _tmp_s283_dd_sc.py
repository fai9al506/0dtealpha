# -*- coding: utf-8 -*-
"""S283 - DD Exhaustion fade + Skew Charm long vs short, on the V20 book."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')
def qty(r):
    v=r.get('basket_pct')
    if v is None: return 1
    v=float(v); return 1 if abs(v)<DEAD else (2 if ((v>0)==r['is_long']) else 1)
openp=[];last={};tk=[]
for r in rows:
    if not lf.passes_v20(r,gaps): continue
    t=r['et']; openp=[p for p in openp if p[0]>t]
    if sum(1 for p in openp if p[1]==r['is_long']) >= (2 if r['is_long'] else 3): continue
    k=(r['setup_name'],r['is_long'])
    if k in last and (t-last[k]).total_seconds()<90: continue
    last[k]=t; openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),r['is_long']))
    q=qty(r); pts=float(r['outcome_pnl'])
    tk.append({'d':t.date(),'et':t,'setup':r['setup_name'],'long':r['is_long'],'grade':r.get('grade'),
               'para':r.get('paradigm'),'vix':pd.to_numeric(r.get('vix'),errors='coerce'),
               'align':r.get('greek_alignment'),'pts':pts,'q':q,'net':(pts-HAIR)*q*DPP-FEE*q})
df=pd.DataFrame(tk); df['mo']=pd.to_datetime(df['d']).dt.strftime('%Y-%m')
def blk(t,s):
    if not len(s): print(f"  {t:26s} (none)"); return
    se=s['net'].std(ddof=1)/np.sqrt(len(s)) if len(s)>1 else float('nan')
    print(f"  {t:26s} n={len(s):4d}  ${s['net'].sum():+8,.0f}  ${s['net'].mean():+6.1f}/t  "
          f"WR {(s['pts']>0).mean()*100:3.0f}%  t={s['net'].mean()/se if se==se and se>0 else 0:+5.2f}")
print("="*98); print("3. DD EXHAUSTION — where did it go?  (V20 book, longs only; shorts already blocked)"); print("="*98)
dd=df[df['setup']=='DD Exhaustion']
print("  by month:")
for mo,s in dd.groupby('mo'): blk(f'   {mo}', s)
print("\n  by grade:")
for g,s in dd.groupby('grade'): blk(f'   grade {g}', s)
print("\n  by VIX band:")
for lo,hi in [(0,16),(16,18),(18,20),(20,22),(22,99)]:
    blk(f'   VIX {lo}-{hi}', dd[(dd['vix']>=lo)&(dd['vix']<hi)])
print("\n  by paradigm (top 6 by count):")
for p,s in sorted(dd.groupby('para'), key=lambda x:-len(x[1]))[:6]: blk(f'   {str(p)[:20]}', s)
print("\n  early vs late window:")
blk('   Mar-Apr', dd[pd.to_datetime(dd['d'])<pd.Timestamp('2026-05-01')])
blk('   May-Aug', dd[pd.to_datetime(dd['d'])>=pd.Timestamp('2026-05-01')])
print()
print("="*98); print("4. SKEW CHARM — LONG vs SHORT, separately (top-ranked setup, do not lose it)"); print("="*98)
sc=df[df['setup']=='Skew Charm']
for side,s in [('LONG',sc[sc['long']]),('SHORT',sc[~sc['long']])]:
    print(f"\n  --- Skew Charm {side} ---")
    blk('   ALL', s)
    for mo,ss in s.groupby('mo'): blk(f'   {mo}', ss)
    print("   by VIX:")
    for lo,hi in [(0,16),(16,18),(18,20),(20,22),(22,99)]:
        blk(f'    VIX {lo}-{hi}', s[(s['vix']>=lo)&(s['vix']<hi)])
    print("   by grade:")
    for g,ss in s.groupby('grade'): blk(f'    grade {g}', ss)
    print("   recent 60d vs before:")
    cut=pd.Timestamp('2026-06-18')
    blk('    before', s[pd.to_datetime(s['d'])<cut]); blk('    last 60d', s[pd.to_datetime(s['d'])>=cut])
