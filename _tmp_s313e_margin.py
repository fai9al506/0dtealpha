# -*- coding: utf-8 -*-
"""S313 stage 5 - THE DECIDING NUMBER. I recommended the smaller variant on margin
grounds without measuring it. Peak MES in stage 2 was TOTAL across both directions,
but the two accounts are funded separately and there is NO cross-margin:
    longs  210VYX65  $2,609.80
    shorts 210VYX91  $3,461.43
So what matters is peak LONG contracts on its own account.

Also: the full per-month dollar table for every candidate, and how OFTEN the peak
is actually reached - a ceiling touched once in 119 sessions is a different risk
from one touched weekly."""
import os, sys
import numpy as np, pandas as pd
from collections import Counter
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
DAILY, VIXMAX = -300.0, 24.0
MES_MARGIN = 265.0
ACCT_LONG, ACCT_SHORT = 2609.80, 3461.43

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
day['mo'] = pd.to_datetime(day['d']).dt.strftime('%Y-%m')
CAL = day[day['d'] >= pd.to_datetime('2026-03-01').date()].groupby('mo')['d'].count().to_dict()
N_SESS = sum(CAL.values())
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def run(skip, size):
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []
    pl = ps = 0
    hist_l, hist_s = Counter(), Counter()
    for r in rows:
        t = r['et']
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
        cl = sum(o[4] for o in openp if o[1])
        cs = sum(o[4] for o in openp if not o[1])
        pl = max(pl, cl); ps = max(ps, cs)
        hist_l[cl] += 1; hist_s[cs] += 1
        out.append({'d': t.date(), 'net': net, 'long': r['is_long'], 'q': q})
    df = pd.DataFrame(out); df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    df.attrs.update(peak_long=pl, peak_short=ps, hist_l=hist_l, hist_s=hist_s)
    return df


def vixok(r):
    v = r.get('vix'); return v is not None and float(v) < VIXMAX


def T(thr):
    def f(r, p):
        if p is None or pd.isna(p): return False
        return p < thr and vixok(r)
    return f


NOSKIP = lambda r, p: False
NOSIZE = lambda r, p, q: q


def mk(thr, mode):
    tg = T(thr)
    sk = lambda r, p: (not r['is_long']) and tg(r, p)
    if mode == 'block':
        return sk, NOSIZE
    if mode == 'floor2':
        return sk, (lambda r, p, q: max(q, 2) if (r['is_long'] and tg(r, p)) else q)
    if mode == 'x2':
        return sk, (lambda r, p, q: q * 2 if (r['is_long'] and tg(r, p)) else q)


def cap3(thr):
    tg = T(thr)
    sk = lambda r, p: (not r['is_long']) and tg(r, p)
    return sk, (lambda r, p, q: min(q * 2, 3) if (r['is_long'] and tg(r, p)) else q)


CAND = {
    'V20 baseline': (NOSKIP, NOSIZE),
    'V21 oc<-0.8 block': mk(-0.8, 'block'),
    'oc<-0.8 block+floor2': mk(-0.8, 'floor2'),
    'oc<-0.8 block+x2': mk(-0.8, 'x2'),
    'oc<-0.5 block+floor2': mk(-0.5, 'floor2'),
    'oc<-0.5 block+x2 CAP3': cap3(-0.5),
    'oc<-0.5 block+x2': mk(-0.5, 'x2'),
}
months = sorted(CAL)
print("=" * 126)
print("(1) FULL PER-MONTH DOLLARS (normalised to 21 sessions) - the whole distribution, not just the minimum")
print("=" * 126)
print("  %-24s" % 'variant' + "".join("%11s" % m for m in months) + "%11s%11s" % ('MIN', 'avg'))
R = {}
for k, (sk, sz) in CAND.items():
    df = run(sk, sz); R[k] = df
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(months) * 21
    print("  %-24s" % k + "".join("%+11.0f" % per.get(m, 0) for m in months)
          + "%+11.0f%+11.0f" % (per.min(), df['net'].sum() / N_SESS * 21))

print()
print("=" * 126)
print("(2) PEAK CONTRACTS PER ACCOUNT - there is NO cross-margin, each account funds its own side")
print("    long acct 210VYX65 $%.2f   short acct 210VYX91 $%.2f   margin $%.0f/MES" % (ACCT_LONG, ACCT_SHORT, MES_MARGIN))
print("=" * 126)
print("  %-24s%10s%12s%9s   %10s%12s%9s" % (
    'variant', 'peakLONG', 'margin $', 'use %', 'peakSHORT', 'margin $', 'use %'))
for k in CAND:
    df = R[k]
    pl, ps = df.attrs['peak_long'], df.attrs['peak_short']
    print("  %-24s%10d%12.0f%8.0f%%   %10d%12.0f%8.0f%%" % (
        k, pl, pl * MES_MARGIN, pl * MES_MARGIN / ACCT_LONG * 100,
        ps, ps * MES_MARGIN, ps * MES_MARGIN / ACCT_SHORT * 100))

print()
print("=" * 126)
print("(3) HOW OFTEN IS THE LONG BOOK THAT BIG?  count of moments at each concurrent long size")
print("=" * 126)
for k in ('V20 baseline', 'oc<-0.8 block+x2', 'oc<-0.5 block+x2', 'oc<-0.5 block+x2 CAP3'):
    h = R[k].attrs['hist_l']
    tot = sum(h.values())
    sizes = sorted(h)
    print("  %-24s" % k + "  ".join("%dMES:%d(%.0f%%)" % (s, h[s], h[s] / tot * 100) for s in sizes if s > 0))
