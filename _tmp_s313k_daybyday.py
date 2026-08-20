# -*- coding: utf-8 -*-
"""S313 stage 11 - DAY BY DAY. Every day the V22 rule can act on, showing what
changes on the SHORT side, what changes on the LONG side, whether the $300 breaker
was hit, and the dollar difference V22 minus V21.

Only days where the previous session fell more than 0.5% can differ at all, so the
whole difference must be visible in this table. That is checked at the end."""
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
    openp, last = [], {}
    realized = 0.0; d0 = None; closed = []
    per = {}
    for r in rows:
        t = r['et']; dd = t.date()
        p = prev.get(dd)
        if dd != d0:
            d0 = dd; realized = 0.0; closed = []
        per.setdefault(dd, {'nS': 0, 'nL': 0, 'cS': 0, 'cL': 0, 'net': 0.0,
                            'killed': 0, 'brk_time': None, 'blocked': 0})
        for ct, nv in [x for x in closed if x[0] <= t]: realized += nv
        closed = [x for x in closed if x[0] > t]
        if not lf.passes_v20(r, gaps): continue
        if realized <= DAILY:
            per[dd]['killed'] += 1
            if per[dd]['brk_time'] is None:
                per[dd]['brk_time'] = t.strftime('%H:%M')
            continue
        if block and (not r['is_long']) and BLK(r, p):
            per[dd]['blocked'] += 1
            continue
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
            per[dd]['nL'] += 1; per[dd]['cL'] += q
        else:
            per[dd]['nS'] += 1; per[dd]['cS'] += q
        per[dd]['net'] += net
    return per


A = run(True, False)    # V21
B = run(True, True)     # V22
alld = sorted(set(A) | set(B))
trig = [d for d in alld if prev.get(d) is not None and not pd.isna(prev.get(d)) and prev[d] < -0.5]

print("=" * 134)
print("EVERY DAY V22 CAN ACT ON  (previous session fell more than 0.5%%) - %d days" % len(trig))
print("  SHORT BLK = shorts skipped (only when yesterday fell >0.8%% - identical in V21 and V22)")
print("  the ONLY thing V22 changes is the long CONTRACTS column")
print("=" * 134)
hdr = ("  %-11s%8s%6s | %-22s | %-22s | %9s" %
       ('day', 'prev%', 'blk', 'V21   S/L trades  ctr  brk', 'V22   S/L trades  ctr  brk', 'delta $'))
print(hdr)
print("  " + "-" * 130)
tot = 0.0
brk21 = brk22 = 0
for d in trig:
    a, b = A.get(d, {}), B.get(d, {})
    if not a and not b: continue
    da = b.get('net', 0) - a.get('net', 0)
    tot += da
    ba = a.get('brk_time') or '-'
    bb = b.get('brk_time') or '-'
    brk21 += (a.get('brk_time') is not None)
    brk22 += (b.get('brk_time') is not None)
    mark = '  <<<' if bb != ba else ''
    print("  %-11s%+8.2f%6s | %2d/%-2d %8d ctr %5s | %2d/%-2d %8d ctr %5s | %+9.0f%s" % (
        d, prev[d], 'YES' if prev[d] < -0.8 else 'no',
        a.get('nS', 0), a.get('nL', 0), a.get('cS', 0) + a.get('cL', 0), ba,
        b.get('nS', 0), b.get('nL', 0), b.get('cS', 0) + b.get('cL', 0), bb, da, mark))
print("  " + "-" * 130)
print("  %-11s%8s%6s | %19s %5d | %19s %5d | %+9.0f" % (
    'TOTAL', '', '', 'days breaker hit:', brk21, 'days breaker hit:', brk22, tot))

# non-trigger days must be identical
other = [d for d in alld if d not in trig]
diff = [d for d in other if abs(B.get(d, {}).get('net', 0) - A.get(d, {}).get('net', 0)) > 0.01]
print()
print("  CHECK - non-trigger days that differ: %d  (must be 0)" % len(diff))
print("  CHECK - total delta from this table %+.0f  vs whole-book delta %+.0f" % (
    tot, sum(B.get(d, {}).get('net', 0) - A.get(d, {}).get('net', 0) for d in alld)))

print()
print("=" * 134)
print("SIGNALS KILLED BY THE $300 BREAKER, per day")
print("=" * 134)
print("  %-11s%10s%10s%9s" % ('day', 'V21 killed', 'V22 killed', 'extra'))
k21 = k22 = 0
for d in trig:
    a, b = A.get(d, {}), B.get(d, {})
    ka, kb = a.get('killed', 0), b.get('killed', 0)
    k21 += ka; k22 += kb
    if ka or kb:
        print("  %-11s%10d%10d%+9d" % (d, ka, kb, kb - ka))
print("  %-11s%10d%10d%+9d   (on trigger days)" % ('TOTAL', k21, k22, k22 - k21))
ok21 = sum(v.get('killed', 0) for v in A.values())
ok22 = sum(v.get('killed', 0) for v in B.values())
print("  %-11s%10d%10d%+9d   (whole book)" % ('ALL DAYS', ok21, ok22, ok22 - ok21))
