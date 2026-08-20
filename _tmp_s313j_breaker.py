# -*- coding: utf-8 -*-
"""S313 stage 10 - IS THE $300 BREAKER TOO TIGHT ON V22's BIGGER DAYS?

The user's point: on a trigger day the long book runs up to 3 contracts instead of
up to 2, so a FIXED $300 daily loss limit is reached in fewer points. The breaker is
effectively TIGHTER exactly on the days we chose to be bigger.

Three questions:
  1. What do the extra breaker hits actually COST? Score the signals it kills.
  2. Does a bigger limit ON TRIGGER DAYS ONLY help, and what does it cost in drawdown?
  3. Is a bigger limit on EVERY day better or worse than targeting it?

Drawdown and worst-day are reported for every variant - a looser breaker cannot be
judged on P&L alone."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
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
day['oc'] = (day['close'] - day['open']) / day['open'] * 100
prev = {}
ds = list(day['d'])
for i in range(1, len(ds)):
    prev[ds[i]] = day.iloc[i - 1]['oc']
day['mo'] = pd.to_datetime(day['d']).dt.strftime('%Y-%m')
CAL = day[day['d'] >= pd.to_datetime('2026-03-01').date()].groupby('mo')['d'].count().to_dict()
N_SESS = sum(CAL.values())
months = sorted(CAL)
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


def run(block, size, lim_normal=-300.0, lim_trigger=None):
    """lim_trigger: daily loss limit on a LONG-trigger day; None = same as normal."""
    openp, last, out, killed = [], {}, [], []
    realized = 0.0; d0 = None; closed = []; lim = lim_normal
    for r in rows:
        t = r['et']
        p = prev.get(t.date())
        if t.date() != d0:
            d0 = t.date(); realized = 0.0; closed = []
            trig = (p is not None and not pd.isna(p) and p < -0.5)
            lim = (lim_trigger if (lim_trigger is not None and trig) else lim_normal)
        for ct, nv in [x for x in closed if x[0] <= t]: realized += nv
        closed = [x for x in closed if x[0] > t]
        if not lf.passes_v20(r, gaps): continue
        if realized <= lim:
            killed.append({'d': t.date(), 'pnl': float(r['outcome_pnl']), 'long': r['is_long']})
            continue
        if block and (not r['is_long']) and BLK(r, p): continue
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
        out.append({'d': t.date(), 'net': net, 'q': q})
    df = pd.DataFrame(out); df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    df.attrs['killed'] = pd.DataFrame(killed)
    return df


base = run(False, False)
bs = base.groupby('mo')['net'].sum()
bmo = base['net'].sum() / N_SESS * 21


def line(lbl, df):
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(months) * 21
    dd = df.groupby('d')['net'].sum(); eq = dd.cumsum()
    s = df.groupby('mo')['net'].sum()
    dl = [float(s.get(m, 0) - bs.get(m, 0)) for m in months]
    k = df.attrs['killed']
    print("  %-32s%+9.0f%+9.0f%+9.0f%+10.0f%8s%9d" % (
        lbl, df['net'].sum() / N_SESS * 21, per.min(),
        float((eq - eq.cummax()).min()), dd.min(),
        "%d/%d" % (sum(1 for x in dl if x >= -1), len(months)), len(k)))
    return df


print("=" * 110)
print("(1) WHAT DO THE BREAKER-KILLED SIGNALS ACTUALLY SCORE?")
print("    If they are net LOSERS the breaker is doing its job and hitting it more is FINE.")
print("=" * 110)
v21 = run(True, False)
v22 = run(True, True)
for lbl, df in [('V21 (block only)', v21), ('V22 (block + size)', v22)]:
    k = df.attrs['killed']
    if len(k) == 0:
        print("  %-22s none" % lbl); continue
    print("  %-22s killed %3d signals   avg %+6.2f pt   total %+8.1f pt   winners %.0f%%"
          % (lbl, len(k), k['pnl'].mean(), k['pnl'].sum(), (k['pnl'] > 0).mean() * 100))
extra = len(v22.attrs['killed']) - len(v21.attrs['killed'])
print("  -> V22 kills %d MORE signals than V21" % extra)

print()
print("=" * 110)
print("(2) DOES A BIGGER LIMIT ON TRIGGER DAYS HELP?   ($300 stays on every other day)")
print("=" * 110)
print("  %-32s%9s%9s%9s%10s%8s%9s" % ('variant', '$/mo', 'min mo', 'MaxDD', 'worst day', 'LOMO', 'killed'))
line('V21 today ($300)', v21)
line('V22, $300 everywhere', v22)
for L in (-375.0, -450.0, -525.0, -600.0):
    line('V22, trigger days $%d' % abs(L), run(True, True, -300.0, L))

print()
print("=" * 110)
print("(3) FOR COMPARISON - raising the limit on EVERY day (not targeted)")
print("=" * 110)
print("  %-32s%9s%9s%9s%10s%8s%9s" % ('variant', '$/mo', 'min mo', 'MaxDD', 'worst day', 'LOMO', 'killed'))
for L in (-375.0, -450.0, -600.0):
    line('V22, $%d EVERY day' % abs(L), run(True, True, L, None))
print()
print("  A looser breaker cannot be judged on $/mo alone - read MaxDD and worst day.")
