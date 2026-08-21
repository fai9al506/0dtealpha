# -*- coding: utf-8 -*-
"""S313 stage 9 - WHAT IS THE +$50 INTERACTION, really?

I said "a blocked short frees a slot a bigger long takes". That is WRONG: long and
short have separate accounts and separate caps (2 long / 3 short), so a short slot
can never be taken by a long.

Find the real mechanism by tracing the reason each trade was skipped, with the rule
off vs on, and isolate which gate changed its mind."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
DAILY, VIXMAX = -300.0, 24.0

E = create_engine(os.environ['DATABASE_URL'])
c = E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps = lf.load_gaps(c)
px = pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d,
    bar_open, bar_close from spx_ohlc_1m
    where (ts at time zone 'America/New_York')>='2026-02-19' order by ts"""), c)
rows = c.execute(text("SELECT " + lf.COLS + """, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
px['d'] = pd.to_datetime(px['d']).dt.date
g = px.groupby('d')
day = pd.DataFrame({'open': g['bar_open'].first(), 'close': g['bar_close'].last()}).reset_index()
day['oc'] = (day['close'] - day['open']) / day['open'] * 100
prev = {}
ds = list(day['d'])
for i in range(1, len(ds)):
    prev[ds[i]] = day.iloc[i - 1]['oc']
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def vixok(r):
    v = r.get('vix'); return v is not None and float(v) < VIXMAX


def fire(thr):
    def f(r, p):
        if p is None or pd.isna(p): return False
        return p < thr and vixok(r)
    return f


BLK, LNG = fire(-0.8), fire(-0.5)


def run(block, size):
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []
    for r in rows:
        t = r['et']
        if t.date() != d0:
            d0 = t.date(); realized = 0.0; closed = []
        for ct, nv in [x for x in closed if x[0] <= t]: realized += nv
        closed = [x for x in closed if x[0] > t]
        p = prev.get(t.date())
        rec = {'d': t.date(), 'et': t, 'setup': r['setup_name'], 'long': r['is_long'],
               'pnl': float(r['outcome_pnl']), 'q': 0, 'net': 0.0, 'why': None,
               'realized_at_signal': realized}
        if not lf.passes_v20(r, gaps):
            continue
        if realized <= DAILY:
            rec['why'] = 'DAILY_BREAKER'; out.append(rec); continue
        if block and (not r['is_long']) and BLK(r, p):
            rec['why'] = 'v22_short_block'; out.append(rec); continue
        openp = [o for o in openp if o[0] > t]
        if sum(1 for o in openp if o[1] == r['is_long']) >= (2 if r['is_long'] else 3):
            rec['why'] = 'CAP'; out.append(rec); continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90:
            rec['why'] = 'dedup'; out.append(rec); continue
        sib = [o for o in openp if o[1] == r['is_long'] and o[3] == r['setup_name']]
        if len(sib) >= 2 and r.get('spot'):
            sgn = 1.0 if r['is_long'] else -1.0
            if sum((float(r['spot']) - o[2]) * sgn for o in sib) < 0:
                rec['why'] = 'underwater'; out.append(rec); continue
        last[k] = t
        v = r.get('basket_pct')
        q = 1 if v is None or abs(float(v)) < DEAD else (2 if ((float(v) > 0) == r['is_long']) else 1)
        if size and r['is_long'] and LNG(r, p):
            q = min(q * 2, 3)
        net = (float(r['outcome_pnl']) - HAIR) * q * DPP - FEE * q
        ct = t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct, r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        closed.append((ct, net))
        rec.update(q=q, net=net, why='TAKEN'); out.append(rec)
    return pd.DataFrame(out)


A = run(False, False)          # baseline
B = run(True, False)           # block only
C = run(False, True)           # size only
D = run(True, True)            # both

print("=" * 112)
print("THE FOUR RUNS")
print("=" * 112)
for lbl, df in [('baseline', A), ('block only', B), ('size only', C), ('BOTH', D)]:
    t = df[df['why'] == 'TAKEN']
    print("  %-12s taken %4d  contracts %4d  net $%+8.0f" % (lbl, len(t), t['q'].sum(), t['net'].sum()))
ab = B[B['why'] == 'TAKEN']['net'].sum() - A[A['why'] == 'TAKEN']['net'].sum()
ac = C[C['why'] == 'TAKEN']['net'].sum() - A[A['why'] == 'TAKEN']['net'].sum()
ad = D[D['why'] == 'TAKEN']['net'].sum() - A[A['why'] == 'TAKEN']['net'].sum()
print()
print("  block only      %+8.0f" % ab)
print("  size only       %+8.0f" % ac)
print("  sum             %+8.0f" % (ab + ac))
print("  both            %+8.0f" % ad)
print("  INTERACTION     %+8.0f" % (ad - ab - ac))

print()
print("=" * 112)
print("WHERE DOES IT COME FROM?  trades TAKEN in BOTH but not in SIZE-ONLY")
print("  (if the cap were the mechanism these would be LONGS blocked by the LONG cap;")
print("   if the breaker is the mechanism they were blocked by DAILY_BREAKER)")
print("=" * 112)
kd = set(zip(D[D['why'] == 'TAKEN']['d'], D[D['why'] == 'TAKEN']['et']))
kc = set(zip(C[C['why'] == 'TAKEN']['d'], C[C['why'] == 'TAKEN']['et']))
extra = sorted(kd - kc)
print("  trades that exist in BOTH but not in size-only: %d" % len(extra))
whys = {}
for d, t in extra:
    r = C[(C['d'] == d) & (C['et'] == t)]
    w = r.iloc[0]['why'] if len(r) else 'not-in-C'
    whys[w] = whys.get(w, 0) + 1
    rr = D[(D['d'] == d) & (D['et'] == t)].iloc[0]
    print("     %s %s %-18s %-5s q=%d  $%+7.0f   | in size-only it was: %s"
          % (d, pd.Timestamp(t).strftime('%H:%M'), rr['setup'],
             'LONG' if rr['long'] else 'SHORT', int(rr['q']), rr['net'], w))
print()
print("  reason they were skipped in the size-only run:")
for w, n in sorted(whys.items(), key=lambda x: -x[1]):
    print("     %-18s %d" % (w, n))
print()
print("  DAILY_BREAKER count per run:")
for lbl, df in [('baseline', A), ('block only', B), ('size only', C), ('BOTH', D)]:
    print("     %-12s %d signals hit the $300 breaker" % (lbl, (df['why'] == 'DAILY_BREAKER').sum()))
