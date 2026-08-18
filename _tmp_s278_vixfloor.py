# -*- coding: utf-8 -*-
"""S278 - a VIX FLOOR on ES Absorption instead of switching it off.
Floor (>=20) vs band (20-26): fewer parameters is safer. Test both, LOMO both."""
import os, sys, pandas as pd, numpy as np
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
os.environ['ES_ABS_REAL_TRADE_ENABLED']='true'   # measure the setup as it WAS
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
    v=float(v); return 1 if abs(v)<DEAD else (2 if ((v>0)==r['is_long']) else 1)
def replay(drop):
    openp=[];last={};out=[]
    for r in b:
        if drop(r): continue
        t=r['et']; openp=[p for p in openp if p[0]>t]
        if sum(1 for p in openp if p[1]==r['is_long']) >= (2 if r['is_long'] else 3): continue
        k=(r['setup_name'],r['is_long'])
        if k in last and (t-last[k]).total_seconds()<90: continue
        last[k]=t; openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),r['is_long']))
        q=qty(r); pts=float(r['outcome_pnl'])
        out.append({'d':t.date(),'setup':r['setup_name'],'pts':pts,'net':(pts-HAIR)*q*DPP-FEE*q})
    return pd.DataFrame(out)
def vixof(r):
    v=pd.to_numeric(r.get('vix'),errors='coerce')
    return v if v==v else 0.0
RULES={
 'A  keep ES Abs as-is (V16 today)': lambda r: False,
 'B  ES Abs OFF completely':         lambda r: r['setup_name']=='ES Absorption',
 'C  ES Abs only VIX >= 20':         lambda r: r['setup_name']=='ES Absorption' and vixof(r)<20,
 'D  ES Abs only VIX 20-26':         lambda r: r['setup_name']=='ES Absorption' and not (20<=vixof(r)<26),
 'E  ES Abs only VIX >= 22':         lambda r: r['setup_name']=='ES Absorption' and vixof(r)<22,
}
res={}
print("="*104); print("WHOLE BOOK under each rule (2026-03-01 -> today, cap 2/3, costs charged)"); print("="*104)
for nm,f in RULES.items():
    df=replay(f); res[nm]=df
    ea=df[df['setup']=='ES Absorption']
    print(f"  {nm:34s} book ${df['net'].sum():+8,.0f}   trades {len(df):4d}   "
          f"ES Abs kept {len(ea):3d} worth ${ea['net'].sum():+7,.0f}")
base=res['A  keep ES Abs as-is (V16 today)']['net'].sum()
print(f"\n  vs today's book: " + "  ".join(
    f"{nm.split()[0]}={res[nm]['net'].sum()-base:+,.0f}" for nm in RULES))
print(); print("="*104); print("MONTH BY MONTH (book total under each rule)"); print("="*104)
print(f"  {'month':9s}" + "".join(f"{nm.split()[0]:>12s}" for nm in RULES))
for mo in sorted(set(pd.to_datetime(res['A  keep ES Abs as-is (V16 today)']['d']).dt.strftime('%Y-%m'))):
    line=f"  {mo:9s}"
    for nm in RULES:
        df=res[nm]; s=df[pd.to_datetime(df['d']).dt.strftime('%Y-%m')==mo]
        line+=f"{s['net'].sum():>+12,.0f}"
    print(line)
print(); print("="*104); print("LEAVE-ONE-MONTH-OUT: does the VIX>=20 floor help (or at least not hurt) EVERY month?"); print("="*104)
A=res['A  keep ES Abs as-is (V16 today)']; C=res['C  ES Abs only VIX >= 20']
for mo in sorted(set(pd.to_datetime(A['d']).dt.strftime('%Y-%m'))):
    a=A[pd.to_datetime(A['d']).dt.strftime('%Y-%m')==mo]['net'].sum()
    c2=C[pd.to_datetime(C['d']).dt.strftime('%Y-%m')==mo]['net'].sum()
    print(f"    {mo}:  as-is ${a:+8,.0f}   with floor ${c2:+8,.0f}   delta ${c2-a:+7,.0f}  "
          f"{'HELPS' if c2>a else ('SAME' if c2==a else 'HURTS')}")
print(); print("="*104); print("HOW OFTEN WOULD THE FLOOR EVEN LET IT TRADE? (sessions with VIX >= 20)"); print("="*104)
vx=pd.DataFrame([{'d':r['et'].date(),'vix':vixof(r)} for r in b])
day=vx.groupby('d')['vix'].max().reset_index(); day['mo']=pd.to_datetime(day['d']).dt.strftime('%Y-%m')
for mo,s in day.groupby('mo'):
    print(f"    {mo}: {int((s['vix']>=20).sum()):2d} of {len(s):2d} sessions had VIX >= 20  "
          f"({(s['vix']>=20).mean()*100:3.0f}%)")
