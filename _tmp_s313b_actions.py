# -*- coding: utf-8 -*-
"""S313 stage 2 - ACTIONS. The map says: after a down session our SHORTS are bad
and our LONGS are very good. Test what to DO about it, in the full replay.

Actions tested per trigger:
  block_s   skip shorts            (what V21 does today)
  force2_l  long size = max(q,2)   (floor of 2 contracts)
  x2_l      long size = q * 2      (true doubling, up to 4)
  both      block shorts AND force2 longs
  x2_both   block shorts AND x2 longs

Full replay: V20 + cap 2 long / 3 short + 90s dedup + S203 + $300 breaker +
basket sizing, haircut 0.6pt + $1.92 per contract charged inside.
Also reports PEAK CONCURRENT CONTRACTS - sizing costs margin, slots do not."""
import os, sys, pickle
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
DAILY = -300.0
VIXMAX = 24.0

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
day['gap'] = (day['open'] - day['pclose']) / day['pclose'] * 100
day['cum2'] = day['cc'] + day['cc'].shift(1)
prev = {}
ds = list(day['d'])
for i in range(1, len(ds)):
    p = day.iloc[i - 1]
    prev[ds[i]] = {'oc': p['oc'], 'cc': p['cc'], 'gap': p['gap'], 'cum2': p['cum2']}
day['mo'] = pd.to_datetime(day['d']).dt.strftime('%Y-%m')
CAL = day[day['d'] >= pd.to_datetime('2026-03-01').date()].groupby('mo')['d'].count().to_dict()
N_SESS = sum(CAL.values())

rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def run(skip, size):
    """skip(r,p)->bool ; size(r,p,q)->int"""
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []
    peak = 0
    for r in rows:
        t = r['et']
        if t.date() != d0:
            d0 = t.date(); realized = 0.0; closed = []
        for ct, nv in [x for x in closed if x[0] <= t]:
            realized += nv
        closed = [x for x in closed if x[0] > t]
        p = prev.get(t.date())
        if not lf.passes_v20(r, gaps): continue
        if realized <= DAILY: continue
        if skip(r, p): continue
        openp = [o for o in openp if o[0] > t]
        n = sum(1 for o in openp if o[1] == r['is_long'])
        if n >= (2 if r['is_long'] else 3): continue
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
    df = pd.DataFrame(out)
    df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    df.attrs['peak'] = peak
    return df


def vixok(r):
    v = r.get('vix')
    return v is not None and float(v) < VIXMAX


def trig(name, thr):
    def f(r, p):
        if p is None: return False
        x = p.get(name)
        if x is None or (isinstance(x, float) and np.isnan(x)): return False
        return x < thr and vixok(r)
    return f


NOSKIP = lambda r, p: False
NOSIZE = lambda r, p, q: q

TRIGGERS = {
    'oc < -0.8 (V21)': trig('oc', -0.8),
    'oc < -0.5': trig('oc', -0.5),
    'oc < -0.3': trig('oc', -0.3),
    'cc < -0.6': trig('cc', -0.6),
    'cc < -0.3': trig('cc', -0.3),
    'cum2 < -0.8': trig('cum2', -0.8),
    'gap < -0.2': trig('gap', -0.2),
}


def build(tg, mode):
    if mode == 'block_s':
        return (lambda r, p: (not r['is_long']) and tg(r, p)), NOSIZE
    if mode == 'force2_l':
        return NOSKIP, (lambda r, p, q: max(q, 2) if (r['is_long'] and tg(r, p)) else q)
    if mode == 'x2_l':
        return NOSKIP, (lambda r, p, q: q * 2 if (r['is_long'] and tg(r, p)) else q)
    if mode == 'both':
        return (lambda r, p: (not r['is_long']) and tg(r, p)), \
               (lambda r, p, q: max(q, 2) if (r['is_long'] and tg(r, p)) else q)
    if mode == 'x2_both':
        return (lambda r, p: (not r['is_long']) and tg(r, p)), \
               (lambda r, p, q: q * 2 if (r['is_long'] and tg(r, p)) else q)


base = run(NOSKIP, NOSIZE)
bs = base.groupby('mo')['net'].sum()
months = sorted(CAL)


def line(lbl, df):
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(sorted(df['mo'].unique())) * 21
    dd = df.groupby('d')['net'].sum(); eq = dd.cumsum()
    s = df.groupby('mo')['net'].sum()
    deltas = [float(s.get(m, 0) - bs.get(m, 0)) for m in months]
    lomo = "%d/%d" % (sum(1 for x in deltas if x >= -1), len(months))
    print("  %-30s%7d%8d%+9.0f%+9.0f%+9.0f%+10.0f%8s%7d" % (
        lbl, len(df), df.attrs['peak'], df['net'].sum() / N_SESS * 21, per.min(), per.max(),
        float((eq - eq.cummax()).min()), lomo, df['q'].sum()))


print("=" * 128)
print("S313 - WHAT TO DO ABOUT A DOWN PREVIOUS SESSION    full replay, %d sessions, V20 + cap + dedup + S203 + $300 breaker" % N_SESS)
print("=" * 128)
print("  %-30s%7s%8s%9s%9s%9s%10s%8s%7s" % (
    'variant', 'trades', 'peakMES', '$/mo', 'min mo', 'max mo', 'MaxDD', 'LOMO', 'ctrts'))
line('BASELINE V20 (nothing)', base)
print()
for tname, tg in TRIGGERS.items():
    for mode in ('block_s', 'force2_l', 'x2_l', 'both', 'x2_both'):
        sk, sz = build(tg, mode)
        line("%-16s %s" % (tname, mode), run(sk, sz))
    print()
pickle.dump({'CAL': CAL, 'N_SESS': N_SESS}, open('_tmp_s313b.pkl', 'wb'))
