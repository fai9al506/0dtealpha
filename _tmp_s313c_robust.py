# -*- coding: utf-8 -*-
"""S313 stage 3 - ROBUSTNESS. 35 variants were swept, so the best one is optimistic
by construction. Four tests before believing anything:
  1. month by month, every month's delta vs baseline
  2. TRAIN Mar-May / TEST Jun-Aug - pick on train only, score on test
  3. RANDOM CONTROL - same action on the same NUMBER of randomly chosen days
  4. how many DAYS and TRADES the rule actually touches"""
import os, sys, pickle
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
DAILY, VIXMAX = -300.0, 24.0
rng = np.random.default_rng(313)

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
day['pclose'] = day['close'].shift(1)
day['oc'] = (day['close'] - day['open']) / day['open'] * 100
day['cc'] = (day['close'] - day['pclose']) / day['pclose'] * 100
prev = {}
ds = list(day['d'])
for i in range(1, len(ds)):
    p = day.iloc[i - 1]
    prev[ds[i]] = {'oc': p['oc'], 'cc': p['cc']}
day['mo'] = pd.to_datetime(day['d']).dt.strftime('%Y-%m')
CAL = day[day['d'] >= pd.to_datetime('2026-03-01').date()].groupby('mo')['d'].count().to_dict()
N_SESS = sum(CAL.values())
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def run(skip, size, only=None):
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []; peak = 0
    for r in rows:
        t = r['et']
        if only and t.strftime('%Y-%m') not in only: continue
        if t.date() != d0:
            d0 = t.date(); realized = 0.0; closed = []
        for ct, nv in [x for x in closed if x[0] <= t]: realized += nv
        closed = [x for x in closed if x[0] > t]
        p = prev.get(t.date())
        if not lf.passes_v20(r, gaps): continue
        if realized <= DAILY: continue
        if skip(r, p): continue
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
        q = size(r, p, q)
        net = (float(r['outcome_pnl']) - HAIR) * q * DPP - FEE * q
        ct = t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct, r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        closed.append((ct, net))
        peak = max(peak, sum(o[4] for o in openp))
        out.append({'d': t.date(), 'net': net, 'long': r['is_long'], 'q': q})
    df = pd.DataFrame(out); df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    df.attrs['peak'] = peak
    return df


def vixok(r):
    v = r.get('vix'); return v is not None and float(v) < VIXMAX


def ocT(thr):
    def f(r, p):
        if p is None or p['oc'] is None or pd.isna(p['oc']): return False
        return p['oc'] < thr and vixok(r)
    return f


NOSKIP = lambda r, p: False
NOSIZE = lambda r, p, q: q
V = {}
for nm, thr in [('oc<-0.8', -0.8), ('oc<-0.5', -0.5)]:
    tg = ocT(thr)
    V[nm + ' block only (V21 style)'] = ((lambda tg: lambda r, p: (not r['is_long']) and tg(r, p))(tg), NOSIZE)
    V[nm + ' block + long floor 2'] = ((lambda tg: lambda r, p: (not r['is_long']) and tg(r, p))(tg),
                                       (lambda tg: lambda r, p, q: max(q, 2) if (r['is_long'] and tg(r, p)) else q)(tg))
    V[nm + ' block + long x2'] = ((lambda tg: lambda r, p: (not r['is_long']) and tg(r, p))(tg),
                                  (lambda tg: lambda r, p, q: q * 2 if (r['is_long'] and tg(r, p)) else q)(tg))

months = sorted(CAL)
base = run(NOSKIP, NOSIZE)
bs = base.groupby('mo')['net'].sum()
print("=" * 122)
print("(1) MONTH BY MONTH - dollar delta vs V20 baseline. A rule must not lose in any month.")
print("=" * 122)
print("  %-32s" % 'variant' + "".join("%10s" % m for m in months) + "%10s%9s" % ('total', 'months+'))
print("  %-32s" % 'V20 baseline $' + "".join("%10.0f" % bs.get(m, 0) for m in months) + "%10.0f" % bs.sum())
for k, (sk, sz) in V.items():
    s = run(sk, sz).groupby('mo')['net'].sum()
    d = [float(s.get(m, 0) - bs.get(m, 0)) for m in months]
    print("  %-32s" % k + "".join("%+10.0f" % x for x in d) + "%+10.0f%7d/%d" % (sum(d), sum(1 for x in d if x >= -1), len(months)))

print()
print("=" * 122)
print("(2) TRAIN Mar-May  ->  TEST Jun-Aug   (choose on train, score on test; test is 3 unseen months)")
print("=" * 122)
TR = ['2026-03', '2026-04', '2026-05']; TE = ['2026-06', '2026-07', '2026-08']
btr = run(NOSKIP, NOSIZE, only=TR); bte = run(NOSKIP, NOSIZE, only=TE)
ntr = sum(CAL[m] for m in TR); nte = sum(CAL[m] for m in TE)
print("  %-32s%14s%14s%14s%14s" % ('variant', 'TRAIN $/mo', 'vs base', 'TEST $/mo', 'vs base'))
print("  %-32s%+14.0f%14s%+14.0f%14s" % ('V20 baseline',
      btr['net'].sum() / ntr * 21, '-', bte['net'].sum() / nte * 21, '-'))
for k, (sk, sz) in V.items():
    a = run(sk, sz, only=TR)['net'].sum() / ntr * 21
    b = run(sk, sz, only=TE)['net'].sum() / nte * 21
    print("  %-32s%+14.0f%+14.0f%+14.0f%+14.0f" % (k, a, a - btr['net'].sum() / ntr * 21,
                                                   b, b - bte['net'].sum() / nte * 21))

print()
print("=" * 122)
print("(3) HOW MANY DAYS DOES IT TOUCH?  (this is the real sample size)")
print("=" * 122)
for nm, thr in [('oc<-0.8', -0.8), ('oc<-0.5', -0.5)]:
    hit = [d for d in ds if prev.get(d) and prev[d]['oc'] is not None
           and not pd.isna(prev[d]['oc']) and prev[d]['oc'] < thr
           and d >= pd.to_datetime('2026-03-01').date()]
    hit = [d for d in hit if d in set(base['d'])]
    print("  %-10s fires on %2d trading days of %d   (%s)" % (
        nm, len(hit), N_SESS, ", ".join(str(x) for x in hit)))

print()
print("=" * 122)
print("(4) RANDOM CONTROL - same action, same number of days, but days chosen AT RANDOM")
print("=" * 122)
alld = sorted(set(base['d']))
for nm, thr in [('oc<-0.8', -0.8), ('oc<-0.5', -0.5)]:
    hit = set(d for d in ds if prev.get(d) and prev[d]['oc'] is not None
              and not pd.isna(prev[d]['oc']) and prev[d]['oc'] < thr
              and d >= pd.to_datetime('2026-03-01').date())
    k = len([d for d in hit if d in set(alld)])
    tg = ocT(thr)
    real = run((lambda r, p: (not r['is_long']) and tg(r, p)),
               (lambda r, p, q: q * 2 if (r['is_long'] and tg(r, p)) else q))
    realv = real['net'].sum() / N_SESS * 21
    vals = []
    TRIALS = 300
    for _ in range(TRIALS):
        pick = set(rng.choice(len(alld), size=k, replace=False).tolist())
        S = set(alld[i] for i in pick)
        df = run((lambda r, p: (not r['is_long']) and r['et'].date() in S),
                 (lambda r, p, q: q * 2 if (r['is_long'] and r['et'].date() in S) else q))
        vals.append(df['net'].sum() / N_SESS * 21)
    vals = np.array(vals)
    print("  %-10s block+x2 on the REAL %2d days -> %+.0f $/mo" % (nm, k, realv))
    print("  %-10s same action on %2d RANDOM days -> %+.0f $/mo (sd %.0f)  |  random >= real: %d/%d  p=%.3f"
          % ('', k, vals.mean(), vals.std(), int((vals >= realv).sum()), TRIALS, (vals >= realv).mean()))
