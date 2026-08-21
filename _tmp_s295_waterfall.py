# -*- coding: utf-8 -*-
"""S295 — the portal shows ~2,000 points (~$10,000). The account is flat.
Where does every dollar go? One waterfall, each step named and measured.
"""
import os, sys, json
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
gaps = lf.load_gaps(c)
rows = c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-02-19'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
brk = pd.read_sql(text("select day, gross, comm, net, n_trades from tsrt_daily_stmt order by day"), c)
# every real trade ever placed, including before the statement era
lids = c.execute(text("""select o.setup_log_id, o.state, l.ts at time zone 'America/New_York' et,
    l.setup_name from real_trade_orders o join setup_log l on l.id=o.setup_log_id order by l.ts""")).all()
c.close()

brk['day'] = pd.to_datetime(brk['day']).dt.date
for col in ('gross', 'comm', 'net'):
    brk[col] = brk[col].astype(float)
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')

live_days = set(brk['day'])
ERA0 = min(live_days)

print("=" * 100)
print("0. WHEN DID REAL MONEY ACTUALLY TRADE?")
print("=" * 100)
by_month = {}
for lid, st, et, sn in lids:
    st = st if isinstance(st, dict) else json.loads(st)
    m = et.strftime('%Y-%m')
    by_month.setdefault(m, {'n': 0, 'q': 0})
    by_month[m]['n'] += 1
    by_month[m]['q'] += int(st.get('quantity') or 1)
print(f"  {'month':9s}{'real trades placed':>20s}{'contracts':>12s}")
for m in sorted(by_month):
    print(f"  {m:9s}{by_month[m]['n']:>20d}{by_month[m]['q']:>12d}")
print(f"\n  broker statement covers {min(live_days)} -> {max(live_days)} ({len(live_days)} days)")
print(f"  real trades exist from {min(e for _,_,e,_ in lids).date()} "
      f"-> {max(e for _,_,e,_ in lids).date()}")


def replay(v20=True, cap=True, sizing=True, costs=True, days=None, guard=True):
    openp, last, out = [], {}, []
    for r in rows:
        if v20 and not lf.passes_v20(r, gaps):
            continue
        t = r['et']
        if days is not None and t.date() not in days:
            continue
        if cap:
            openp = [p for p in openp if p[0] > t]
            n = sum(1 for p in openp if p[1] == r['is_long'])
            if n >= (2 if r['is_long'] else 3):
                continue
            k = (r['setup_name'], r['is_long'])
            if k in last and (t - last[k]).total_seconds() < 90:
                continue
            if guard:
                sib = [p for p in openp if p[1] == r['is_long'] and p[3] == r['setup_name']]
                if len(sib) >= 2 and r.get('spot'):
                    sgn = 1.0 if r['is_long'] else -1.0
                    if sum((float(r['spot']) - p[2]) * sgn for p in sib) < 0:
                        continue
            last[k] = t
        q = 1
        if sizing:
            v = r.get('basket_pct')
            if v is not None and abs(float(v)) >= DEAD and ((float(v) > 0) == r['is_long']):
                q = 2
        if cap:
            openp.append((t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),
                          r['is_long'], float(r['spot']) if r.get('spot') else 0.0,
                          r['setup_name'], q))
        pts = float(r['outcome_pnl'])
        net = (pts - HAIR) * q * DPP - FEE * q if costs else pts * q * DPP
        out.append({'d': t.date(), 'pts': pts * q, 'net': net})
    return pd.DataFrame(out)


ERA = {d for d in {r['et'].date() for r in rows} if d >= ERA0}

print()
print("=" * 100)
print("THE WATERFALL — from what the portal shows to what is in the account")
print("=" * 100)
raw = replay(v20=False, cap=False, sizing=False, costs=False)
print(f"  1. EVERY signal, 1 MES, no filter, no cap, no costs")
print(f"     {len(raw):5d} trades   {raw['pts'].sum():+8,.0f} pts   ${raw['net'].sum():+10,.0f}"
      f"   <-- the portal number")
f1 = replay(v20=True, cap=False, sizing=False, costs=False)
print(f"  2. keep only what the V20 FILTER admits")
print(f"     {len(f1):5d} trades   {f1['pts'].sum():+8,.0f} pts   ${f1['net'].sum():+10,.0f}"
      f"   ({f1['net'].sum()-raw['net'].sum():+,.0f})")
f2 = replay(v20=True, cap=True, sizing=False, costs=False)
print(f"  3. apply the CAP (2 long / 3 short), 90s dedup and the S203 guard")
print(f"     {len(f2):5d} trades   {f2['pts'].sum():+8,.0f} pts   ${f2['net'].sum():+10,.0f}"
      f"   ({f2['net'].sum()-f1['net'].sum():+,.0f})")
f3 = replay(v20=True, cap=True, sizing=True, costs=False)
print(f"  4. apply BASKET SIZING (2x when the basket confirms)")
print(f"     {len(f3):5d} trades   {f3['pts'].sum():+8,.0f} pts   ${f3['net'].sum():+10,.0f}"
      f"   ({f3['net'].sum()-f2['net'].sum():+,.0f})")
f4 = replay(v20=True, cap=True, sizing=True, costs=True)
print(f"  5. charge REAL COSTS (-0.6 pt/contract slippage + $1.92/contract fees)")
print(f"     {len(f4):5d} trades   {f4['pts'].sum():+8,.0f} pts   ${f4['net'].sum():+10,.0f}"
      f"   ({f4['net'].sum()-f3['net'].sum():+,.0f})")
f5 = f4[f4['d'] >= ERA0]
print(f"  6. only the ERA the real account has existed (from {ERA0})")
print(f"     {len(f5):5d} trades                 ${f5['net'].sum():+10,.0f}"
      f"   ({f5['net'].sum()-f4['net'].sum():+,.0f})")
f6 = f5[f5['d'].isin(live_days)]
print(f"  7. only the DAYS THE SYSTEM WAS ACTUALLY ARMED ({len(live_days)} of "
      f"{len({d for d in f5['d']})} sessions)")
print(f"     {len(f6):5d} trades                 ${f6['net'].sum():+10,.0f}"
      f"   ({f6['net'].sum()-f5['net'].sum():+,.0f})  <-- THE BIG ONE")
print(f"  8. what the BROKER actually paid")
print(f"     {int(brk['n_trades'].sum()):5d} trades                 ${brk['net'].sum():+10,.0f}"
      f"   ({brk['net'].sum()-f6['net'].sum():+,.0f})  = execution")
print()
print(f"  ALL-TIME account movement (user's deposit $6,000 -> equity $6,015.92): ~+$16")
print(f"  the statement era alone is ${brk['net'].sum():+,.0f}, so everything BEFORE "
      f"{ERA0} cost about ${16 - brk['net'].sum():+,.0f}")

print()
print("=" * 100)
print("9. THE ERAS — did each change actually improve LIVE money?")
print("=" * 100)
ERAS = [
 ("E1  first live run",        None,               pd.Timestamp('2026-05-14').date()),
 ("E2  V16 era, pre-S217",     pd.Timestamp('2026-05-14').date(), pd.Timestamp('2026-06-13').date()),
 ("E3  post-S217 (basket gate)",pd.Timestamp('2026-06-13').date(), pd.Timestamp('2026-07-01').date()),
 ("E4  Jul (mostly OFF)",      pd.Timestamp('2026-07-01').date(), pd.Timestamp('2026-08-10').date()),
 ("E5  restart 08-10 (V16-SB)",pd.Timestamp('2026-08-10').date(), pd.Timestamp('2026-12-31').date()),
]
print(f"  {'era':30s}{'days':>6s}{'net $':>10s}{'$/day':>9s}{'green':>8s}")
for lbl, a, b in ERAS:
    s = brk if a is None else brk[(brk['day'] >= a) & (brk['day'] < b)]
    if a is None:
        print(f"  {lbl:30s}{'--':>6s}{'~-694':>10s}{'':>9s}{'':>8s}   (before the statement era)")
        continue
    if not len(s): continue
    print(f"  {lbl:30s}{len(s):>6d}{s['net'].sum():>+10,.0f}{s['net'].mean():>+9,.0f}"
          f"{int((s['net']>0).sum()):>5d}/{len(s):<3d}")

print()
print("=" * 100)
print("10. THE DAYS WE TRADED THINGS V20 WOULD NEVER TAKE")
print("=" * 100)
simdays = set(f5['d'])
matched = sorted(live_days & simdays)
unmatched = sorted(live_days - simdays)
mb = brk[brk['day'].isin(matched)]['net'].sum()
ub = brk[brk['day'].isin(unmatched)]['net'].sum()
sm = f6.groupby('d')['net'].sum()
print(f"  days where V20 also had trades   : {len(matched):3d}   broker ${mb:+8,.0f}   "
      f"sim ${sm.sum():+8,.0f}   capture {mb/sm.sum()*100 if sm.sum() else 0:.0f}%")
print(f"  days where V20 had NOTHING       : {len(unmatched):3d}   broker ${ub:+8,.0f}"
      f"   <-- traded under an OLDER filter")
for d in unmatched:
    v = brk[brk['day'] == d]
    print(f"      {d}  ${v['net'].iloc[0]:+8,.0f}  ({int(v['n_trades'].iloc[0])} trades)")
print()
print("  ==> of the -$1,049 'execution' line above, "
      f"${ub:+,.0f} is actually OLD-FILTER days, not execution.")
print(f"      true execution gap on comparable days: ${mb - sm.sum():+,.0f} "
      f"({mb/sm.sum()*100 if sm.sum() else 0:.0f}% capture)")
