# -*- coding: utf-8 -*-
"""S313 stage 12 - SPLIT THE EFFECT BY ACCOUNT.

Long and short are separately funded accounts (210VYX65 / 210VYX91) with separate
caps, so the day-by-day table has to say what each ACCOUNT does, not just the book.

Expected shape, to be confirmed or refuted by the numbers:
  - the LONG account should carry the whole gain (it is the one being sized up)
  - the SHORT account should be UNCHANGED, because V22 does not touch the short
    rule at all - except on days where the $300 breaker fires earlier and kills
    shorts that V21 would have taken."""
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


def run(size):
    openp, last = [], {}
    realized = 0.0; d0 = None; closed = []
    per = {}
    for r in rows:
        t = r['et']; dd = t.date()
        p = prev.get(dd)
        if dd != d0:
            d0 = dd; realized = 0.0; closed = []
        per.setdefault(dd, {'L$': 0.0, 'S$': 0.0, 'nL': 0, 'nS': 0,
                            'cL': 0, 'cS': 0, 'brk': None})
        for ct, nv in [x for x in closed if x[0] <= t]: realized += nv
        closed = [x for x in closed if x[0] > t]
        if not lf.passes_v20(r, gaps): continue
        if realized <= DAILY:
            if per[dd]['brk'] is None: per[dd]['brk'] = t.strftime('%H:%M')
            continue
        if (not r['is_long']) and BLK(r, p): continue        # same in V21 and V22
        openp = [o for o in openp if o[0] > t]
        if sum(1 for o in openp if o[1] == r['is_long']) >= (2 if r['is_long'] else 3): continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90: continue
        sib = [o for o in openp if o[1] == r['is_long'] and o[3] == r['setup_name']]
        if len(sib) >= 2 and r.get('spot'):
            sgn = 1.0 if r['is_long'] else -1.0
            if sum((float(r['spot']) - o[2]) * sgn for o in sib) < 0: continue
        last[k] = t
        v = r.get('basket_pct')
        q = 1 if v is None or abs(float(v)) < DEAD else (2 if ((float(v) > 0) == r['is_long']) else 1)
        if size and r['is_long'] and LNG(r, p):
            q = min(q * 2, 3)
        net = (float(r['outcome_pnl']) - HAIR) * q * DPP - FEE * q
        ct = t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct, r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        closed.append((ct, net))
        if r['is_long']:
            per[dd]['L$'] += net; per[dd]['nL'] += 1; per[dd]['cL'] += q
        else:
            per[dd]['S$'] += net; per[dd]['nS'] += 1; per[dd]['cS'] += q
    return per


A, B = run(False), run(True)
alld = sorted(set(A) | set(B))
trig = [d for d in alld if prev.get(d) is not None and not pd.isna(prev.get(d)) and prev[d] < -0.5]

print("=" * 132)
print("V22 vs V21 BY ACCOUNT - every day the rule can act on")
print("  LONG account 210VYX65 (being sized up)      SHORT account 210VYX91 (rule unchanged)")
print("=" * 132)
print("  %-11s%7s | %-27s | %-27s | %8s" % (
    'day', 'prev%', 'LONG   V21 $ -> V22 $   ctr', 'SHORT  V21 $ -> V22 $   ctr', 'breaker'))
print("  " + "-" * 128)
tl = ts = 0.0
for d in trig:
    a, b = A.get(d, {}), B.get(d, {})
    dl = b.get('L$', 0) - a.get('L$', 0)
    dsh = b.get('S$', 0) - a.get('S$', 0)
    if abs(dl) < 0.5 and abs(dsh) < 0.5 and not b.get('brk'):
        continue
    tl += dl; ts += dsh
    print("  %-11s%+7.2f | %+8.0f -> %+8.0f  %2d->%-2d | %+8.0f -> %+8.0f  %2d->%-2d | %s" % (
        d, prev[d],
        a.get('L$', 0), b.get('L$', 0), a.get('cL', 0), b.get('cL', 0),
        a.get('S$', 0), b.get('S$', 0), a.get('cS', 0), b.get('cS', 0),
        (b.get('brk') or '-')))
print("  " + "-" * 128)
TL = sum(B.get(d, {}).get('L$', 0) - A.get(d, {}).get('L$', 0) for d in alld)
TS = sum(B.get(d, {}).get('S$', 0) - A.get(d, {}).get('S$', 0) for d in alld)
print("  %-11s%7s | %-27s | %-27s |" % ('', '', 'net change  %+8.0f' % TL, 'net change  %+8.0f' % TS))

print()
print("=" * 132)
print("WHO CARRIES IT")
print("=" * 132)
print("  LONG account  (210VYX65) : %+8.0f   over 119 sessions  =  %+6.0f per 21-session month" % (TL, TL / 119 * 21))
print("  SHORT account (210VYX91) : %+8.0f   over 119 sessions  =  %+6.0f per 21-session month" % (TS, TS / 119 * 21))
print("  TOTAL                    : %+8.0f                        %+6.0f per month" % (TL + TS, (TL + TS) / 119 * 21))
print()
share = TL / (TL + TS) * 100 if (TL + TS) else 0
print("  the LONG account carries %.0f%% of the change" % share)

print()
print("=" * 132)
print("ABSOLUTE LEVELS - what each account earns in total, V21 vs V22")
print("=" * 132)
al = sum(v['L$'] for v in A.values()); as_ = sum(v['S$'] for v in A.values())
bl = sum(v['L$'] for v in B.values()); bs_ = sum(v['S$'] for v in B.values())
print("  %-24s%12s%12s%12s" % ('account', 'V21 $', 'V22 $', 'change'))
print("  %-24s%+12.0f%+12.0f%+12.0f" % ('LONG  210VYX65', al, bl, bl - al))
print("  %-24s%+12.0f%+12.0f%+12.0f" % ('SHORT 210VYX91', as_, bs_, bs_ - as_))
print("  %-24s%+12.0f%+12.0f%+12.0f" % ('BOOK', al + as_, bl + bs_, bl + bs_ - al - as_))
print()
print("  per 21-session month:")
print("  %-24s%+12.0f%+12.0f%+12.0f" % ('LONG  210VYX65', al / 119 * 21, bl / 119 * 21, (bl - al) / 119 * 21))
print("  %-24s%+12.0f%+12.0f%+12.0f" % ('SHORT 210VYX91', as_ / 119 * 21, bs_ / 119 * 21, (bs_ - as_) / 119 * 21))
