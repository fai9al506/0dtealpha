# -*- coding: utf-8 -*-
"""S296 — "we make small profits, then one bad week kills it all". Is that true,
and is it still true under the CURRENT config (V20 + S203 guard + S293 breaker)?

Measures the thing that actually matters: are our BIG days big enough and frequent
enough to outrun our bad ones, or is the shape small-wins / huge-losses?
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
BREAKER_DAY = -300.0

E = create_engine(os.environ["DATABASE_URL"])
c = E.connect().execution_options(isolation_level="AUTOCOMMIT")
gaps = lf.load_gaps(c)
rows = c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
brk = pd.read_sql(text("select day, net from tsrt_daily_stmt order by day"), c)
c.close()
brk['day'] = pd.to_datetime(brk['day']).dt.date
brk['net'] = brk['net'].astype(float)
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def run(sizing='R0', breaker=2, guard=True):
    openp, last, out = [], {}, []
    streak, day = {}, None
    for r in rows:
        if not lf.passes_v20(r, gaps):
            continue
        t = r['et']
        if t.date() != day:
            day = t.date(); streak = {}
        openp = [p for p in openp if p[0] > t]
        n = sum(1 for p in openp if p[1] == r['is_long'])
        if n >= (2 if r['is_long'] else 3):
            continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90:
            continue
        if breaker and streak.get(k, 0) >= breaker:
            continue
        if guard:
            sib = [p for p in openp if p[1] == r['is_long'] and p[3] == r['setup_name']]
            if len(sib) >= 2 and r.get('spot'):
                sgn = 1.0 if r['is_long'] else -1.0
                if sum((float(r['spot']) - p[2]) * sgn for p in sib) < 0:
                    continue
        last[k] = t
        v = r.get('basket_pct')
        q = 1 if v is None or abs(float(v)) < DEAD else (2 if ((float(v) > 0) == r['is_long']) else 1)
        if sizing == 'R1d' and r['setup_name'] == 'Skew Charm' and not r['is_long'] and n >= 1:
            q = max(q, 3 if n >= 2 else 2)
        pts = float(r['outcome_pnl'])
        openp.append((t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),
                      r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        streak[k] = streak.get(k, 0) + 1 if pts <= -13.9 else 0
        out.append({'d': t.date(), 'net': (pts - HAIR) * q * DPP - FEE * q})
    return pd.DataFrame(out).groupby('d')['net'].sum().sort_index()


def profile(name, daily):
    up = daily[daily > 0]
    dn = daily[daily < 0]
    print(f"\n  {name}")
    print(f"    days {len(daily)}   total ${daily.sum():+,.0f}   green {len(up)} / red {len(dn)} "
          f"({len(up)/len(daily)*100:.0f}% green)")
    print(f"    average GREEN day ${up.mean():+7.0f}      average RED day ${dn.mean():+7.0f}"
          f"      ratio {abs(up.mean()/dn.mean()):.2f}x")
    print(f"    biggest GREEN     ${up.max():+7.0f}      biggest RED     ${dn.min():+7.0f}")
    print(f"    days over +$300   {int((daily>300).sum()):3d}          days under -$300  {int((daily<-300).sum()):3d}")
    print(f"    top 5 green ${up.nlargest(5).sum():+,.0f}   worst 5 red ${dn.nsmallest(5).sum():+,.0f}"
          f"   net of those 10: ${up.nlargest(5).sum()+dn.nsmallest(5).sum():+,.0f}")
    wk = daily.rolling(5).sum().dropna()
    print(f"    WORST 5-day window ${wk.min():+,.0f}    BEST 5-day window ${wk.max():+,.0f}")
    print(f"    5-day windows negative: {int((wk<0).sum())} of {len(wk)} ({(wk<0).mean()*100:.0f}%)")
    eq = daily.cumsum()
    print(f"    max drawdown ${float((eq-eq.cummax()).min()):+,.0f}   "
          f"longest red streak {max((len(list(g)) for k,g in __import__('itertools').groupby(daily<0) if k), default=0)} days")


print("=" * 100)
print("1. WHAT ACTUALLY HAPPENED — broker truth, 37 live days")
print("=" * 100)
profile("BROKER (as traded, all old configs)", brk.set_index('day')['net'])
print(f"\n    the -$300 breaker days: "
      + ", ".join(f"{d} ${v:+.0f}" for d, v in
                  brk[brk['net'] <= -290][['day', 'net']].itertuples(index=False)))

print()
print("=" * 100)
print("2. THE SAME QUESTION UNDER THE CURRENT CONFIG (V20 + S203 + S293 breaker)")
print("=" * 100)
d_now = run('R0', breaker=2)
d_r1d = run('R1d', breaker=2)
d_old = run('R0', breaker=0, guard=False)
profile("V20 with NO breaker / no guard (what June ran)", d_old)
profile("V20 + guard + breaker (what we run NOW)", d_now)
profile("V20 + guard + breaker + R1d sizing (Sept plan)", d_r1d)

print()
print("=" * 100)
print("3. THE DIRECT ANSWER — can the good days outrun the bad ones?")
print("=" * 100)
for nm, dd in [('June 2026 alone', d_now[(d_now.index >= pd.Timestamp('2026-06-01').date()) &
                                          (d_now.index < pd.Timestamp('2026-07-01').date())]),
               ('everything else', d_now[(d_now.index < pd.Timestamp('2026-06-01').date()) |
                                          (d_now.index >= pd.Timestamp('2026-07-01').date())])]:
    print(f"  {nm:18s} {len(dd):3d} days  ${dd.sum():+8,.0f}  worst day ${dd.min():+7,.0f}  "
          f"green {(dd>0).mean()*100:.0f}%")
print()
wk = d_now.rolling(5).sum().dropna()
rec = []
for i in range(len(wk)):
    if wk.iloc[i] < 0:
        fwd = d_now.iloc[d_now.index.get_loc(wk.index[i]) + 1:]
        if len(fwd) >= 10:
            rec.append(fwd.iloc[:10].sum())
if rec:
    print(f"  After a LOSING 5-day window, the next 10 days averaged ${np.mean(rec):+,.0f} "
          f"({sum(1 for x in rec if x>0)}/{len(rec)} recovered)")
print()
print("  How long does it take to earn back the worst week?")
w = float(wk.min()); avg = float(d_now.mean())
print(f"    worst 5-day window ${w:+,.0f}   average day ${avg:+.0f}   "
      f"-> {abs(w)/avg:.0f} trading days to recover")
