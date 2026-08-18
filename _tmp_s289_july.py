# -*- coding: utf-8 -*-
"""S289 - why does scaling the stacked SC shorts make JULY worse?
One bad day, or a real regime where stacking stops working?"""
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
def confirms(r):
    v=r.get('basket_pct')
    if v is None: return False
    v=float(v); return abs(v)>=DEAD and ((v>0)==r['is_long'])
def is_sc_short(r): return r['setup_name']=='Skew Charm' and not r['is_long']
def run(qtyfn):
    openp,last,out=[],{},[]
    for r in rows:
        if not lf.passes_v20(r,gaps): continue
        t=r['et']; openp=[p for p in openp if p[0]>t]
        n=sum(1 for p in openp if p[1]==r['is_long'])
        if n >= (2 if r['is_long'] else 3): continue
        k=(r['setup_name'],r['is_long'])
        if k in last and (t-last[k]).total_seconds()<90: continue
        last[k]=t; q=qtyfn(r,n)
        openp.append((t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),r['is_long'],q))
        pts=float(r['outcome_pnl'])
        out.append({'d':t.date(),'et':t,'setup':r['setup_name'],'long':r['is_long'],'slot':n,'q':q,
                    'grade':r.get('grade'),'vix':pd.to_numeric(r.get('vix'),errors='coerce'),
                    'pts':pts,'net':(pts-HAIR)*q*DPP-FEE*q})
    df=pd.DataFrame(out); df['mo']=pd.to_datetime(df['d']).dt.strftime('%Y-%m'); return df
basket=lambda r: 2 if confirms(r) else 1
R0 =run(lambda r,n: basket(r))
R1d=run(lambda r,n: max(basket(r), 3 if (is_sc_short(r) and n>=2) else (2 if (is_sc_short(r) and n>=1) else 1)))
print("="*104); print("1. THE STACKED SC SHORTS, MONTH BY MONTH (1 MES each, so size is not the story)"); print("="*104)
st=R0[(R0['setup']=='Skew Charm')&(~R0['long'])&(R0['slot']>=1)]
print(f"  {'month':9s}{'n':>5s}{'$ total':>10s}{'$/t':>8s}{'WR':>6s}{'pts avg':>9s}")
for m,s in st.groupby('mo'):
    print(f"  {m:9s}{len(s):>5d}{s['net'].sum():>+10,.0f}{s['net'].mean():>+8.1f}"
          f"{(s['pts']>0).mean()*100:>5.0f}%{s['pts'].mean():>+9.2f}")
print()
print("="*104); print("2. JULY, EVERY STACKED SC SHORT, ONE LINE EACH"); print("="*104)
jl=st[st['mo']=='2026-07'].sort_values('et')
for _,r in jl.iterrows():
    print(f"  {r['et']:%m-%d %H:%M}  slot{r['slot']}  grade {str(r['grade']):3s}  vix {r['vix']:.1f}  "
          f"{r['pts']:+7.2f} pt   ${r['net']:+7.0f}")
print(f"\n  July stacked total: {len(jl)} trades  ${jl['net'].sum():+,.0f}  "
      f"WR {(jl['pts']>0).mean()*100:.0f}%")
print()
print("="*104); print("3. IS IT ONE DAY?  July stacked-short P&L by DAY"); print("="*104)
byday=jl.groupby('d').agg(n=('net','size'),net=('net','sum'),pts=('pts','sum')).sort_values('net')
for d,r in byday.iterrows():
    print(f"  {d}  {int(r['n'])} trades  ${r['net']:+7,.0f}  ({r['pts']:+.1f} pt)")
print(f"\n  worst day ${byday['net'].min():+,.0f}   without it July stacked = "
      f"${jl['net'].sum()-byday['net'].min():+,.0f}")
print()
print("="*104); print("4. THE WHOLE-BOOK JULY DIFFERENCE — where do the extra contracts land?"); print("="*104)
for nm,df in (('R0',R0),('R1d',R1d)):
    j=df[df['mo']=='2026-07']
    print(f"  {nm}: July book ${j['net'].sum():+,.0f}   "
          f"SC-short stacked ${j[(j['setup']=='Skew Charm')&(~j['long'])&(j['slot']>=1)]['net'].sum():+,.0f}")
print()
print("="*104); print("5. CONTROL — is July special, or does stacking just fail when SC-short is weak?"); print("="*104)
allsc=R0[(R0['setup']=='Skew Charm')&(~R0['long'])]
print(f"  {'month':9s}{'first (slot0)':>18s}{'stacked (slot>=1)':>20s}{'stacked-first':>15s}")
for m,s in allsc.groupby('mo'):
    f0=s[s['slot']==0]['net'].mean(); f1=s[s['slot']>=1]['net'].mean()
    f1 = f1 if f1==f1 else 0
    print(f"  {m:9s}{f0:>+18.1f}{f1:>+20.1f}{f1-f0:>+15.1f}")
