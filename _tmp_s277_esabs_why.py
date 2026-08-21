# -*- coding: utf-8 -*-
"""Why did ES Absorption stop working after April?
Two candidates that changed at the same time:
  (a) VOLATILITY collapsed (VIX 27 in Mar -> 15-18 May-Aug)
  (b) the ES FEED switched Rithmic -> Sierra on 2026-04-30
Separate them: if low-VIX ES Abs was ALREADY bad in Mar-Apr (on the OLD feed),
the cause is volatility. If high-VIX ES Abs still WINS after the switch, same.
"""
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
    return 1 if abs(v)<DEAD else (2 if ((v>0)==r['is_long']) else 1)
openp=[];last={};tk=[]
for r in b:
    t=r['et']; openp=[p for p in openp if p[0]>t]
    if sum(1 for p in openp if p[1]==r['is_long']) >= (2 if r['is_long'] else 3): continue
    k=(r['setup_name'],r['is_long'])
    if k in last and (t-last[k]).total_seconds()<90: continue
    last[k]=t; openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),r['is_long']))
    q=qty(r); pts=float(r['outcome_pnl'])
    tk.append({'d':t.date(),'et':t,'setup':r['setup_name'],'long':r['is_long'],'grade':r.get('grade'),
               'vix':pd.to_numeric(r.get('vix'),errors='coerce'),'align':r.get('greek_alignment'),
               'para':r.get('paradigm'),'pts':pts,'net':(pts-HAIR)*q*DPP-FEE*q})
tk=pd.DataFrame(tk)
ea=tk[(tk['setup']=='ES Absorption') & (tk['long'])].copy()
ea['era']=np.where(pd.to_datetime(ea['d'])<pd.Timestamp('2026-04-30'),'Rithmic (Mar-Apr)','Sierra (May-Aug)')

print("="*100); print("1. THE CONFOUND — what each era actually looked like"); print("="*100)
for era,s in ea.groupby('era'):
    print(f"  {era:20s} n={len(s):3d}  total ${s['net'].sum():+7,.0f}  avg VIX {s['vix'].mean():4.1f}  "
          f"WR {(s['pts']>0).mean()*100:3.0f}%")

print(); print("="*100); print("2. THE TEST — ES Abs longs by VIX, INSIDE each era"); print("="*100)
print(f"  {'VIX band':12s} {'Rithmic Mar-Apr':>28s}   {'Sierra May-Aug':>28s}")
for lo,hi in [(0,18),(18,20),(20,22),(22,99)]:
    line=f"  VIX {lo:2d}-{hi:2d}    "
    for era in ['Rithmic (Mar-Apr)','Sierra (May-Aug)']:
        s=ea[(ea['era']==era)&(ea['vix']>=lo)&(ea['vix']<hi)]
        line += (f"n={len(s):3d} ${s['net'].sum():+6,.0f} WR{(s['pts']>0).mean()*100:3.0f}%".rjust(30)
                 if len(s) else "—".rjust(30))
    print(line)

print(); print("="*100); print("3. VERDICT TEST"); print("="*100)
lo_old=ea[(ea['era']=='Rithmic (Mar-Apr)')&(ea['vix']<20)]
hi_new=ea[(ea['era']=='Sierra (May-Aug)')&(ea['vix']>=20)]
print(f"  LOW vol on the OLD feed   n={len(lo_old):3d}  ${lo_old['net'].sum():+7,.0f}  "
      f"avg ${lo_old['net'].mean() if len(lo_old) else 0:+6.1f}  WR {(lo_old['pts']>0).mean()*100 if len(lo_old) else 0:3.0f}%")
print(f"  HIGH vol on the NEW feed  n={len(hi_new):3d}  ${hi_new['net'].sum():+7,.0f}  "
      f"avg ${hi_new['net'].mean() if len(hi_new) else 0:+6.1f}  WR {(hi_new['pts']>0).mean()*100 if len(hi_new) else 0:3.0f}%")
print("  If LOW-vol-old is bad AND HIGH-vol-new is good -> it is VOLATILITY, not the feed.")

print(); print("="*100); print("4. WHOLE WINDOW: ES Abs longs by VIX (both eras pooled)"); print("="*100)
for lo,hi in [(0,16),(16,18),(18,20),(20,22),(22,26),(26,99)]:
    s=ea[(ea['vix']>=lo)&(ea['vix']<hi)]
    if not len(s): continue
    se=s['net'].std(ddof=1)/np.sqrt(len(s)) if len(s)>1 else float('nan')
    print(f"  VIX {lo:2d}-{hi:2d}  n={len(s):3d}  total ${s['net'].sum():+7,.0f}  avg ${s['net'].mean():+6.1f}  "
          f"WR {(s['pts']>0).mean()*100:3.0f}%  t={s['net'].mean()/se if se==se and se>0 else 0:+5.2f}")

print(); print("="*100); print("5. IF WE ADD A VIX FLOOR — what would ES Abs have made?"); print("="*100)
for thr in [0,16,18,19,20,21,22]:
    s=ea[ea['vix']>=thr] if thr else ea
    kept=len(s); print(f"  VIX >= {thr:2d}:  keeps {kept:3d}/{len(ea)} trades  total ${s['net'].sum():+7,.0f}  "
          f"WR {(s['pts']>0).mean()*100 if kept else 0:3.0f}%   "
          f"May-Aug part ${s[pd.to_datetime(s['d'])>=pd.Timestamp('2026-05-01')]['net'].sum():+7,.0f}")
print()
print("  leave-one-month-out for a VIX>=20 floor (must help or be flat in EVERY month):")
for mo,s in ea.groupby(pd.to_datetime(ea['d']).dt.strftime('%Y-%m')):
    cut=s[s['vix']<20]
    print(f"    {mo}: would drop {len(cut):3d} trades worth ${cut['net'].sum():+7,.0f}  "
          f"-> {'HELPS' if cut['net'].sum()<0 else 'HURTS'}")

print(); print("="*100)
print("6. CONTROL — is this ES-Abs-specific, or does the WHOLE book behave this way?")
print("="*100)
for su in ['ES Absorption','Skew Charm','DD Exhaustion','AG Short']:
    s0=tk[tk['setup']==su]
    line=f"  {su:15s}"
    for lo,hi in [(0,18),(18,20),(20,22),(22,26),(26,99)]:
        s=s0[(s0['vix']>=lo)&(s0['vix']<hi)]
        line += (f"{lo}-{hi}: ${s['net'].mean():+5.1f}(n{len(s)})".rjust(20) if len(s) else "—".rjust(20))
    print(line)
print("  (avg $ per trade in each VIX band. If only ES Abs flips sign below 20, it is specific.)")
