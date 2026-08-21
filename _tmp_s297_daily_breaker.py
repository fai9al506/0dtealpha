# -*- coding: utf-8 -*-
"""S297 - the user's correction: the $300 DAILY breaker has existed since TSRT began.
None of my replays modelled it. Redo the comparison with it in, so the only difference
between 'June' and 'today' is the S293 per-setup breaker shipped 2026-08-17."""
import os, sys, itertools
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
DAILY_BREAKER=-300.0
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')

def run(daily_breaker=True, setup_breaker=0, guard=True, sizing='R0'):
    """daily_breaker: once the day's REALIZED P&L <= -$300, take no new entries that day
    (it blocks entries, it does NOT flatten - so open trades still run)."""
    openp,last,out=[],{},[]
    streak, day, realized = {}, None, 0.0
    closed=[]   # (close_time, net) still to be realized
    for r in rows:
        t=r['et']
        if t.date()!=day:
            day=t.date(); streak={}; realized=0.0; closed=[]
        # realize anything that closed before now
        for ct,nv in [x for x in closed if x[0]<=t]:
            realized+=nv
        closed=[x for x in closed if x[0]>t]
        if not lf.passes_v20(r,gaps): continue
        if daily_breaker and realized <= DAILY_BREAKER: continue
        openp=[p for p in openp if p[0]>t]
        n=sum(1 for p in openp if p[1]==r['is_long'])
        if n >= (2 if r['is_long'] else 3): continue
        k=(r['setup_name'],r['is_long'])
        if k in last and (t-last[k]).total_seconds()<90: continue
        if setup_breaker and streak.get(k,0)>=setup_breaker: continue
        if guard:
            sib=[p for p in openp if p[1]==r['is_long'] and p[3]==r['setup_name']]
            if len(sib)>=2 and r.get('spot'):
                sgn=1.0 if r['is_long'] else -1.0
                if sum((float(r['spot'])-p[2])*sgn for p in sib) < 0: continue
        last[k]=t
        v=r.get('basket_pct')
        q=1 if v is None or abs(float(v))<DEAD else (2 if ((float(v)>0)==r['is_long']) else 1)
        if sizing=='R1d' and r['setup_name']=='Skew Charm' and not r['is_long'] and n>=1:
            q=max(q, 3 if n>=2 else 2)
        pts=float(r['outcome_pnl']); net=(pts-HAIR)*q*DPP-FEE*q
        ct=t+timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct,r['is_long'],float(r['spot']) if r.get('spot') else 0.0,r['setup_name'],q))
        closed.append((ct,net))
        streak[k]=streak.get(k,0)+1 if pts<=-13.9 else 0
        out.append({'d':t.date(),'net':net})
    return pd.DataFrame(out).groupby('d')['net'].sum().sort_index()

def prof(name, d):
    up,dn=d[d>0],d[d<0]
    jun=d[(d.index>=pd.Timestamp('2026-06-01').date())&(d.index<pd.Timestamp('2026-07-01').date())]
    eq=d.cumsum(); wk=d.rolling(5).sum().dropna()
    streak=max((len(list(g)) for k,g in itertools.groupby(d<0) if k), default=0)
    print(f"  {name:38s}{d.sum():>+9,.0f}{d.min():>+10,.0f}{int((d<-300).sum()):>7d}"
          f"{int((d>300).sum()):>7d}{abs(up.mean()/dn.mean()):>7.2f}{wk.min():>+10,.0f}"
          f"{float((eq-eq.cummax()).min()):>+10,.0f}{jun.sum():>+9,.0f}{streak:>7d}")

print("="*118)
print("WITH THE $300 DAILY BREAKER MODELLED (it has been live since TSRT began)")
print("="*118)
print(f"  {'config':38s}{'total':>9s}{'worst day':>10s}{'<-300':>7s}{'>+300':>7s}{'G:R':>7s}"
      f"{'worst wk':>10s}{'MaxDD':>10s}{'JUNE':>9s}{'streak':>7s}")
prof("A  no daily breaker (my old wrong sim)", run(daily_breaker=False, setup_breaker=0))
prof("B  JUNE'S REAL CONFIG ($300 + guard)",  run(daily_breaker=True,  setup_breaker=0))
prof("C  TODAY ($300 + guard + S293)",        run(daily_breaker=True,  setup_breaker=2))
prof("D  SEPT PLAN (C + R1d sizing)",         run(daily_breaker=True,  setup_breaker=2, sizing='R1d'))
print()
b=run(daily_breaker=True, setup_breaker=0); cfg=run(daily_breaker=True, setup_breaker=2)
print("="*118)
print("SO WHAT DOES THE NEW SETUP-BREAKER ACTUALLY ADD ON TOP OF THE $300 ONE?")
print("="*118)
print(f"  June:        ${b[(b.index>=pd.Timestamp('2026-06-01').date())&(b.index<pd.Timestamp('2026-07-01').date())].sum():+,.0f}"
      f"  ->  ${cfg[(cfg.index>=pd.Timestamp('2026-06-01').date())&(cfg.index<pd.Timestamp('2026-07-01').date())].sum():+,.0f}")
print(f"  worst day:   ${b.min():+,.0f}  ->  ${cfg.min():+,.0f}")
print(f"  worst week:  ${b.rolling(5).sum().min():+,.0f}  ->  ${cfg.rolling(5).sum().min():+,.0f}")
print(f"  days <-$300: {int((b<-300).sum())}  ->  {int((cfg<-300).sum())}")
print(f"  total:       ${b.sum():+,.0f}  ->  ${cfg.sum():+,.0f}")
print()
print("  the -$300 breaker days that REMAIN under today's config:")
for d,v in cfg[cfg<-290].items(): print(f"    {d}  ${v:+,.0f}")

print()
print("="*118)
print("IS THE S293 SETUP BREAKER NEEDED FOR THE R1d SIZING RUNG?")
print("="*118)
print(f"  {'config':38s}{'total':>9s}{'worst day':>10s}{'<-300':>7s}{'>+300':>7s}{'G:R':>7s}"
      f"{'worst wk':>10s}{'MaxDD':>10s}{'JUNE':>9s}{'streak':>7s}")
prof("R1d WITHOUT the setup breaker", run(daily_breaker=True, setup_breaker=0, sizing='R1d'))
prof("R1d WITH the setup breaker",    run(daily_breaker=True, setup_breaker=2, sizing='R1d'))
print()
a=run(daily_breaker=True, setup_breaker=0, sizing='R1d')
b2=run(daily_breaker=True, setup_breaker=2, sizing='R1d')
cal={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}
print("  month by month, R1d without vs with the setup breaker:")
for m in sorted(cal):
    am=a[[d for d in a.index if d.strftime('%Y-%m')==m]].sum()
    bm=b2[[d for d in b2.index if d.strftime('%Y-%m')==m]].sum()
    print(f"    {m}  without ${am:+8,.0f}   with ${bm:+8,.0f}   {bm-am:+8,.0f}")
print(f"\n  TOTAL  without ${a.sum():+,.0f}   with ${b2.sum():+,.0f}   {b2.sum()-a.sum():+,.0f}")
