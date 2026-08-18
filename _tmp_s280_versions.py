# -*- coding: utf-8 -*-
"""S280 - how are V17 / V18 / V19 doing against the live filter (now V20)?
Same basis as PROJECTION.md: chain outcome_pnl, -0.6 pt/contract, $1.92/RT,
basket sizing, cap 2 long / 3 short, 90s dedup. $ at 1 MES base."""
import os, sys, pandas as pd, numpy as np
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

VERS={'V16 (old base)':lf.passes_v16,'V20 (LIVE)':lf.passes_v20,'V17':lf.passes_v17,
      'V18':lf.passes_v18,'V19':lf.passes_v19,'V16-fri':lf.passes_v16_fri}
def qty(r):
    v=r.get('basket_pct')
    if v is None: return 1
    v=float(v); return 1 if abs(v)<DEAD else (2 if ((v>0)==r['is_long']) else 1)
def replay(fn):
    openp=[];last={};out=[]
    for r in rows:
        if not fn(r,gaps): continue
        t=r['et']; openp=[p for p in openp if p[0]>t]
        if sum(1 for p in openp if p[1]==r['is_long']) >= (2 if r['is_long'] else 3): continue
        k=(r['setup_name'],r['is_long'])
        if k in last and (t-last[k]).total_seconds()<90: continue
        last[k]=t; openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),r['is_long']))
        q=qty(r); pts=float(r['outcome_pnl'])
        out.append({'d':t.date(),'setup':r['setup_name'],'pts':pts,'net':(pts-HAIR)*q*DPP-FEE*q,'q':q})
    return pd.DataFrame(out)
res={k:replay(v) for k,v in VERS.items()}
def maxdd(daily):
    eq=daily.cumsum(); return float((eq-eq.cummax()).min())
print("="*106); print("ALL VERSIONS, 2026-03-01 -> today (119 calendar sessions, cap 2/3, costs charged, 1 MES)"); print("="*106)
print(f"  {'version':16s} {'trades':>7s} {'total $':>10s} {'$/month':>9s} {'per trade':>10s} {'WR':>5s} {'MaxDD':>9s} {'red days':>9s}")
for k,df in res.items():
    daily=df.groupby('d')['net'].sum()
    mo=df['net'].sum()/119*21
    print(f"  {k:16s} {len(df):7d} {df['net'].sum():+10,.0f} {mo:+9,.0f} {df['net'].mean():+10.1f} "
          f"{(df['pts']>0).mean()*100:4.0f}% {maxdd(daily):+9,.0f} {int((daily<0).sum()):9d}")
print(); print("="*106); print("SAME, BUT ONLY THE RECENT REGIME (2026-06-01 -> today) — the low-vol months"); print("="*106)
print(f"  {'version':16s} {'trades':>7s} {'total $':>10s} {'$/month':>9s} {'per trade':>10s} {'WR':>5s} {'MaxDD':>9s}")
for k,df in res.items():
    s=df[pd.to_datetime(df['d'])>=pd.Timestamp('2026-06-01')]
    daily=s.groupby('d')['net'].sum()
    print(f"  {k:16s} {len(s):7d} {s['net'].sum():+10,.0f} {s['net'].sum()/55*21:+9,.0f} {s['net'].mean():+10.1f} "
          f"{(s['pts']>0).mean()*100:4.0f}% {maxdd(daily):+9,.0f}")
print(); print("="*106); print("MONTH BY MONTH ($)"); print("="*106)
print(f"  {'month':9s}" + "".join(f"{k:>16s}" for k in res))
mos=sorted(set(pd.to_datetime(res['V16 (old base)']['d']).dt.strftime('%Y-%m')))
for m in mos:
    line=f"  {m:9s}"
    for k,df in res.items():
        s=df[pd.to_datetime(df['d']).dt.strftime('%Y-%m')==m]
        line+=f"{s['net'].sum():>+16,.0f}"
    print(line)
