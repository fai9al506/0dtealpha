# -*- coding: utf-8 -*-
"""S294 — why is the capital flat after 6 months? Broker truth only, no story.

Three candidate explanations, tested against each other:
  A. THE EDGE ISN'T REAL — the filter earns in simulation and not in life.
  B. WE BARELY TRADED — the money is small because the system was rarely armed.
  C. ONE BAD STRETCH ATE IT — June, and the rest was fine.
"""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

sys.path.insert(0, 'app')
import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15

E = create_engine(os.environ["DATABASE_URL"])
c = E.connect().execution_options(isolation_level="AUTOCOMMIT")
brk = pd.read_sql(text("select day, net, n_trades from tsrt_daily_stmt order by day"), c)
brk['day'] = pd.to_datetime(brk['day']).dt.date
brk['net'] = brk['net'].astype(float)
sessions = [r[0] for r in c.execute(text(
    """select distinct (ts at time zone 'America/New_York')::date d from spx_ohlc_1m
       where (ts at time zone 'America/New_York') >= '2026-02-19' order by d""")).all()]
gaps = lf.load_gaps(c)
rows = c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-02-19'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')

live_days = set(brk['day'])
FIRST, LAST = min(sessions), max(sessions)
all_sess = [d for d in sessions if d >= min(live_days)]

print("=" * 104)
print("1. HOW MUCH OF THE TIME WAS THE SYSTEM ACTUALLY TRADING?")
print("=" * 104)
print(f"  first market session in our data : {FIRST}")
print(f"  first REAL trading day           : {min(live_days)}")
print(f"  last REAL trading day            : {max(live_days)}")
print(f"  calendar sessions since first real trade : {len(all_sess)}")
print(f"  sessions we ACTUALLY traded              : {len(live_days)}"
      f"   = {len(live_days)/len(all_sess)*100:.0f}%")
print(f"  sessions the system was OFF              : {len(all_sess)-len(live_days)}"
      f"   = {(1-len(live_days)/len(all_sess))*100:.0f}%")
brk['mo'] = pd.to_datetime(brk['day']).dt.strftime('%Y-%m')
cal = pd.Series(all_sess).groupby(pd.to_datetime(pd.Series(all_sess)).dt.strftime('%Y-%m')).size()
print()
print(f"  {'month':9s}{'sessions':>10s}{'traded':>8s}{'% on':>7s}{'net $':>10s}{'$/traded day':>14s}")
for m in sorted(cal.index):
    s = brk[brk['mo'] == m]
    print(f"  {m:9s}{cal[m]:>10d}{len(s):>8d}{len(s)/cal[m]*100:>6.0f}%{s['net'].sum():>+10,.0f}"
          f"{(s['net'].mean() if len(s) else 0):>+14,.0f}")
print(f"  {'TOTAL':9s}{len(all_sess):>10d}{len(live_days):>8d}"
      f"{len(live_days)/len(all_sess)*100:>6.0f}%{brk['net'].sum():>+10,.0f}"
      f"{brk['net'].mean():>+14,.0f}")

print()
print("=" * 104)
print("2. ON THE DAYS WE DID TRADE, IS THE EDGE REAL?  (broker truth, no simulation)")
print("=" * 104)
print(f"  traded days {len(brk)}   net ${brk['net'].sum():+,.2f}   "
      f"${brk['net'].mean():+.2f}/day   green {int((brk['net']>0).sum())} / red {int((brk['net']<0).sum())}")
for lbl, s in [('May (11 days)', brk[brk['mo'] == '2026-05']),
               ('June (20 days)', brk[brk['mo'] == '2026-06']),
               ('July (1 day)', brk[brk['mo'] == '2026-07']),
               ('August (5 days)', brk[brk['mo'] == '2026-08'])]:
    if len(s):
        print(f"    {lbl:16s} ${s['net'].sum():+8,.0f}   ${s['net'].mean():+7,.0f}/day   "
              f"green {int((s['net']>0).sum())}/{len(s)}")
ex_jun = brk[brk['mo'] != '2026-06']
print(f"\n  EXCLUDING JUNE: {len(ex_jun)} days, ${ex_jun['net'].sum():+,.0f}, "
      f"${ex_jun['net'].mean():+.0f}/day, green {int((ex_jun['net']>0).sum())}/{len(ex_jun)}")
print(f"  JUNE ALONE    : 20 days, ${brk[brk['mo']=='2026-06']['net'].sum():+,.0f}")

print()
print("=" * 104)
print("3. WHAT DID WE MISS ON THE DAYS THE SYSTEM WAS OFF?  (V20 sim, costs charged)")
print("=" * 104)


def replay(days=None):
    openp, last, out = [], {}, []
    for r in rows:
        if not lf.passes_v20(r, gaps):
            continue
        t = r['et']
        if days is not None and t.date() not in days:
            continue
        openp = [p for p in openp if p[0] > t]
        n = sum(1 for p in openp if p[1] == r['is_long'])
        if n >= (2 if r['is_long'] else 3):
            continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90:
            continue
        sib = [p for p in openp if p[1] == r['is_long'] and p[3] == r['setup_name']]
        if len(sib) >= 2 and r.get('spot'):
            sgn = 1.0 if r['is_long'] else -1.0
            if sum((float(r['spot']) - p[2]) * sgn for p in sib) < 0:
                continue
        last[k] = t
        v = r.get('basket_pct')
        q = 1 if v is None or abs(float(v)) < DEAD else (2 if ((float(v) > 0) == r['is_long']) else 1)
        openp.append((t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),
                      r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        out.append({'d': t.date(), 'net': (float(r['outcome_pnl']) - HAIR) * q * DPP - FEE * q})
    return pd.DataFrame(out)


sim_all = replay()
sim_all = sim_all[sim_all['d'].isin(all_sess)]
on = sim_all[sim_all['d'].isin(live_days)].groupby('d')['net'].sum()
off_days = [d for d in all_sess if d not in live_days]
off = sim_all[sim_all['d'].isin(off_days)].groupby('d')['net'].sum()
print(f"  sim says the ON  days were worth ${on.sum():+,.0f}   (broker actually made "
      f"${brk['net'].sum():+,.0f})")
print(f"  sim says the OFF days were worth ${off.sum():+,.0f}   over {len(off)} sessions "
      f"= ${off.mean() if len(off) else 0:+.0f}/day")
print(f"\n  ==> the money left on the table by NOT TRADING: ${off.sum():+,.0f}")

print()
print("=" * 104)
print("4. THE DECISIVE TEST — does the simulation match the broker on the SAME days?")
print("=" * 104)
j = pd.DataFrame({'sim': on}).join(brk.set_index('day')['net'].rename('broker'), how='inner')
j['diff'] = j['broker'] - j['sim']
print(f"  days compared {len(j)}")
print(f"  sim total    ${j['sim'].sum():+,.0f}")
print(f"  broker total ${j['broker'].sum():+,.0f}")
print(f"  difference   ${j['diff'].sum():+,.0f}   ({j['broker'].sum()/j['sim'].sum()*100 if j['sim'].sum() else 0:.0f}% captured)")
print(f"  per day      sim ${j['sim'].mean():+.0f}   broker ${j['broker'].mean():+.0f}   "
      f"diff ${j['diff'].mean():+.0f}")
print(f"  correlation  {j['sim'].corr(j['broker']):.2f}")
print(f"  broker beat sim on {int((j['diff']>0).sum())} of {len(j)} days")
print()
print("  by month:")
j['mo'] = pd.to_datetime(j.index).strftime('%Y-%m')
for m, s in j.groupby('mo'):
    print(f"    {m}  {len(s):3d} days   sim ${s['sim'].sum():+8,.0f}   broker ${s['broker'].sum():+8,.0f}   "
          f"capture {s['broker'].sum()/s['sim'].sum()*100 if s['sim'].sum() else 0:6.0f}%")
