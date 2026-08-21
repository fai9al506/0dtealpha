# -*- coding: utf-8 -*-
"""S302 - AUDIT the relief-rally rule before anyone trades it.
The user's worry: "when the market goes down sometimes it goes MORE down, and we would
be skipping good shorts." That is the right question. Eight checks."""
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
gaps=lf.load_gaps(c)
px=pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d, bar_open, bar_close
    from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-02-19' order by ts"""),c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
px['d']=pd.to_datetime(px['d']).dt.date
g=px.groupby('d')
day=pd.DataFrame({'open':g['bar_open'].first(),'close':g['bar_close'].last()}).reset_index()
day['ret']=(day['close']-day['open'])/day['open']*100
day['prev']=day['ret'].shift(1)
PR=dict(zip(day['d'],day['prev']))
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')


print("="*104)
print("A. PER-TRADE, INSIDE EACH VIX BAND (the evidence that matters)")
print("="*104)
book=[r for r in rows if lf.passes_v20(r,gaps)]
TD=dict(zip(day['d'],day['ret']))
df=pd.DataFrame([{'d':r['et'].date(),'long':r['is_long'],'pts':float(r['outcome_pnl']),
                  'prev':PR.get(r['et'].date()),'mo':r['et'].strftime('%Y-%m'),
                  'vix':pd.to_numeric(r.get('vix'),errors='coerce')} for r in book]).dropna(subset=['prev','vix'])
sh=df[~df['long']]
print(f"  {'VIX band':12s}{'n after down':>14s}{'pt':>9s}{'WR':>6s}{'n other':>10s}{'pt':>9s}{'WR':>6s}{'gap':>9s}")
for lo,hi,t in [(0,18,'VIX <18'),(18,20,'VIX 18-20'),(20,22,'VIX 20-22'),(22,26,'VIX 22-26'),(26,99,'VIX 26+')]:
    s2=sh[(sh['vix']>=lo)&(sh['vix']<hi)]
    a=s2[s2['prev']<-0.5]; b=s2[s2['prev']>=-0.5]
    if len(a)==0 or len(b)==0:
        print(f"  {t:12s}{len(a):>14d}{'--':>9s}{'--':>6s}{len(b):>10d}{'--':>9s}{'--':>6s}{'--':>9s}")
        continue
    print(f"  {t:12s}{len(a):>14d}{a['pts'].mean():>+9.2f}{(a['pts']>0).mean()*100:>5.0f}%"
          f"{len(b):>10d}{b['pts'].mean():>+9.2f}{(b['pts']>0).mean()*100:>5.0f}%{a['pts'].mean()-b['pts'].mean():>+9.2f}")

def run(skip):
    openp,last,out=[],{},[]; realized=0.0; d0=None; closed=[]
    for r in rows:
        t=r['et']
        if t.date()!=d0: d0=t.date(); realized=0.0; closed=[]
        for ct,nv in [x for x in closed if x[0]<=t]: realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not lf.passes_v20(r,gaps): continue
        if realized<=DAILY: continue
        if skip(r, PR.get(t.date())): continue
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

def vixrule(thr, vmax):
    def f(r,p):
        if r['is_long'] or p is None: return False
        v=r.get('vix')
        if v is None: return False
        return p < thr and float(v) < vmax
    return f

CAL2={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}
RULES={'V20 as-is':(lambda r,p: False)}
for vmax in [20,22,24]:
    for thr in [-0.5,-0.8]:
        RULES[f'skip shorts prev<{thr} & VIX<{vmax}']=vixrule(thr,vmax)
print()
print("="*104); print("B. AS A RULE - full replay"); print("="*104)
print(f"  {'rule':34s}{'trades':>8s}{'$/mo':>9s}{'min mo':>9s}{'MaxDD':>9s}{'worst wk':>10s}{'bad wk':>9s}{'LOMO':>7s}")
res={}; base=run(RULES['V20 as-is']); bm=base.groupby('mo')['net'].sum()
for k,f in RULES.items():
    d=run(f); res[k]=d
    per=d.groupby('mo')['net'].sum()/pd.Series(CAL2).reindex(sorted(d['mo'].unique()))*21
    dd=d.groupby('d')['net'].sum(); eq=dd.cumsum()
    bw=d[(d['d']>=pd.Timestamp('2026-06-05').date())&(d['d']<=pd.Timestamp('2026-06-12').date())]['net'].sum()
    wins=sum(1 for m in CAL2 if d.groupby('mo')['net'].sum().get(m,0) >= bm.get(m,0)-1)
    print(f"  {k:34s}{len(d):>8d}{d['net'].sum()/117*21:>+9,.0f}{per.min():>+9,.0f}"
          f"{float((eq-eq.cummax()).min()):>+9,.0f}{dd.rolling(5).sum().min():>+10,.0f}{bw:>+9,.0f}{wins:>5d}/6")
print()
print("="*104); print("C. MARCH - what the VIX-conditioned rule actually gives up"); print("="*104)
mar=sh[(sh['mo']=='2026-03')&(sh['prev']<-0.5)]
print(f"  all March shorts after a down day : n={len(mar):3d}  {mar['pts'].sum():+7.1f} pts")
for v in [20,22,24]:
    lo=mar[mar['vix']<v]; hi=mar[mar['vix']>=v]
    print(f"    VIX<{v}: skipped n={len(lo):3d} {lo['pts'].sum():+7.1f} pts  |  "
          f"VIX>={v}: KEPT n={len(hi):3d} {hi['pts'].sum():+7.1f} pts")
best='skip shorts prev<-0.5 & VIX<22'
print()
print("  month by month for " + best + ":")
for m in sorted(CAL2):
    a=bm.get(m,0); b=res[best].groupby('mo')['net'].sum().get(m,0)
    print(f"    {m}   V20 ${a:+8,.0f}   rule ${b:+8,.0f}   {b-a:+8,.0f}  "
          f"{'HELPS' if b>a+1 else ('SAME' if abs(b-a)<=1 else 'HURTS')}")
