# -*- coding: utf-8 -*-
"""S312c - the two variants left: a CUMULATIVE multi-day decline (the user's
'slow grind' case - three small down days that no single-day threshold sees), and
distance below a short moving average. Plus a RANDOM CONTROL for V21, because it
blocks only 7 trades and a 7-trade edge must beat chance."""
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
END = '2026-08-19'
rng = np.random.default_rng(20260819)

E = create_engine(os.environ['DATABASE_URL'])
c = E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps = lf.load_gaps(c)
px = pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d,
    bar_open, bar_close from spx_ohlc_1m
    where (ts at time zone 'America/New_York')>='2026-02-19' order by ts"""), c)
rows = c.execute(text("SELECT " + lf.COLS + """, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01'
    AND (ts AT TIME ZONE 'America/New_York')<'""" + END + """'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()

px['d'] = pd.to_datetime(px['d']).dt.date
g = px.groupby('d')
day = pd.DataFrame({'open': g['bar_open'].first(), 'close': g['bar_close'].last()}).reset_index()
day['pclose'] = day['close'].shift(1)
day['oc'] = (day['close'] - day['open']) / day['open'] * 100
day['cc'] = (day['close'] - day['pclose']) / day['pclose'] * 100
day['cum2'] = day['cc'] + day['cc'].shift(1)
day['cum3'] = day['cc'] + day['cc'].shift(1) + day['cc'].shift(2)
day['ma5'] = day['close'].rolling(5).mean()
day['vs_ma5'] = (day['close'] - day['ma5']) / day['ma5'] * 100
prev = {}
ds = list(day['d'])
for i in range(1, len(ds)):
    p = day.iloc[i - 1]
    prev[ds[i]] = {'oc': p['oc'], 'cc': p['cc'], 'cum2': p['cum2'],
                   'cum3': p['cum3'], 'vs_ma5': p['vs_ma5']}
day['mo'] = pd.to_datetime(day['d']).dt.strftime('%Y-%m')
CAL = day[day['d'] >= pd.to_datetime('2026-03-01').date()].groupby('mo')['d'].count().to_dict()
N_SESS = sum(CAL.values())

rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def run(skip):
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []
    for r in rows:
        t = r['et']
        if t.date() != d0:
            d0 = t.date(); realized = 0.0; closed = []
        for ct, nv in [x for x in closed if x[0] <= t]:
            realized += nv
        closed = [x for x in closed if x[0] > t]
        if not lf.passes_v20(r, gaps): continue
        if realized <= DAILY: continue
        if skip(r, prev.get(t.date())): continue
        openp = [p for p in openp if p[0] > t]
        n = sum(1 for p in openp if p[1] == r['is_long'])
        if n >= (2 if r['is_long'] else 3): continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90: continue
        sib = [p for p in openp if p[1] == r['is_long'] and p[3] == r['setup_name']]
        if len(sib) >= 2 and r.get('spot'):
            sgn = 1.0 if r['is_long'] else -1.0
            if sum((float(r['spot']) - p[2]) * sgn for p in sib) < 0: continue
        last[k] = t
        v = r.get('basket_pct')
        q = 1 if v is None or abs(float(v)) < DEAD else (2 if ((float(v) > 0) == r['is_long']) else 1)
        net = (float(r['outcome_pnl']) - HAIR) * q * DPP - FEE * q
        ct = t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct, r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        closed.append((ct, net))
        out.append({'d': t.date(), 'net': net})
    df = pd.DataFrame(out)
    df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    return df


def vixok(r):
    v = r.get('vix')
    return v is not None and float(v) < VIXMAX


def fld(name, thr):
    def f(r, p):
        if r['is_long'] or p is None: return False
        v = p.get(name)
        if v is None or (isinstance(v, float) and np.isnan(v)): return False
        return v < thr and vixok(r)
    return f


CAND = {'V20 (none)': lambda r, p: False, 'V21 oc<-0.8 (SHIPPED)': fld('oc', -0.8)}
for t in (-0.8, -1.0, -1.5, -2.0):
    CAND['2-day cum cc < %.1f%%' % t] = fld('cum2', t)
for t in (-1.0, -1.5, -2.0, -2.5):
    CAND['3-day cum cc < %.1f%%' % t] = fld('cum3', t)
for t in (-0.5, -1.0, -1.5):
    CAND['close vs 5d MA < %.1f%%' % t] = fld('vs_ma5', t)

print("=" * 112)
print("S312c - THE SLOW-GRIND VARIANTS (cumulative decline, distance below a 5-day average)")
print("        full replay: V20 + cap 2/3 + dedup + S203 + $300 breaker.  sessions=%d" % N_SESS)
print("=" * 112)
print("  %-24s%7s%9s%9s%9s%9s%9s" % ('rule', 'trades', '$/mo', 'min mo', 'max mo', 'MaxDD', 'LOMO'))
R = {}
base = None
for k, f in CAND.items():
    df = run(f); R[k] = df
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(sorted(df['mo'].unique())) * 21
    dd = df.groupby('d')['net'].sum(); eq = dd.cumsum()
    s = df.groupby('mo')['net'].sum()
    if base is None:
        base = s; lomo = ''
    else:
        d = [float(s.get(m, 0) - base.get(m, 0)) for m in sorted(CAL)]
        lomo = '%d/%d' % (sum(1 for x in d if x >= -1), len(d))
    print("  %-24s%7d%+9.0f%+9.0f%+9.0f%+9.0f%9s" % (
        k, len(df), df['net'].sum() / N_SESS * 21, per.min(), per.max(),
        float((eq - eq.cummax()).min()), lomo))

# ---------- random control for V21 ----------
print()
print("=" * 112)
print("RANDOM CONTROL - V21 blocks only 7 trades. Does blocking 7 RANDOM shorts do as well?")
print("=" * 112)
real = R['V21 oc<-0.8 (SHIPPED)']['net'].sum() / N_SESS * 21
b = R['V20 (none)']['net'].sum() / N_SESS * 21
short_idx = [i for i, r in enumerate(rows)
             if (not r['is_long']) and lf.passes_v20(r, gaps) and vixok(r)]
n_block = 7
wins = 0; TRIALS = 400; vals = []
for _ in range(TRIALS):
    kill = set(rng.choice(short_idx, size=min(n_block, len(short_idx)), replace=False))
    ids = {id(rows[i]) for i in kill}
    df = run(lambda r, p: id(r) in ids)
    v = df['net'].sum() / N_SESS * 21
    vals.append(v)
    if v >= real: wins += 1
vals = np.array(vals)
print("  V20 baseline            %+8.0f $/mo" % b)
print("  V21 (the real 7)        %+8.0f $/mo" % real)
print("  random 7 blocked shorts %+8.0f $/mo  (mean of %d trials, sd %.0f)" % (vals.mean(), TRIALS, vals.std()))
print("  random trials that matched or beat V21: %d / %d   ->  p = %.3f" % (wins, TRIALS, wins / TRIALS))
pickle.dump({'R': R, 'rand': vals, 'real': real}, open('_tmp_s312c_res.pkl', 'wb'))
