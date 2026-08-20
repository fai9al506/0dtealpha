# -*- coding: utf-8 -*-
"""S313 stage 13 - IS IT EDGE, OR IS IT JUST LEVERAGE?

The user's read: the gain is not scaling alone, it is scaling on days where longs
genuinely win more often. That must be TESTED, because the two look identical in a
P&L column and only one of them is worth arming.

  If longs on trigger days have the SAME edge per contract as on any other day,
  V22 is pure leverage - the same result would come from sizing up on random days,
  and it would raise risk without adding skill.

  If they have a HIGHER edge per contract, the sizing is SELECTIVE and the gain is
  real.

Three tests:
  1. per-contract edge and win rate: trigger-day longs vs every other long
  2. RANDOM CONTROL - size up longs on the same NUMBER of randomly chosen days
  3. a same-size control - trigger days at NORMAL size, to separate 'good days'
     from 'more contracts'"""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
DAILY, VIXMAX = -300.0, 24.0
rng = np.random.default_rng(313313)

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
N_SESS = len([d for d in ds if d >= pd.to_datetime('2026-03-01').date()])
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def vixok(r):
    v = r.get('vix'); return v is not None and float(v) < VIXMAX


BLK = lambda r, p: (p is not None and not pd.isna(p) and p < -0.8 and vixok(r))
LNGDAY = lambda p: (p is not None and not pd.isna(p) and p < -0.5)


def run(sizeup_days=None, force_normal=False):
    """sizeup_days: set of dates whose LONGS get min(q*2,3). None = no size-up."""
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []
    for r in rows:
        t = r['et']; dd = t.date()
        p = prev.get(dd)
        if dd != d0:
            d0 = dd; realized = 0.0; closed = []
        for ct, nv in [x for x in closed if x[0] <= t]: realized += nv
        closed = [x for x in closed if x[0] > t]
        if not lf.passes_v20(r, gaps): continue
        if realized <= DAILY: continue
        if (not r['is_long']) and BLK(r, p): continue
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
        if sizeup_days is not None and r['is_long'] and dd in sizeup_days:
            q = min(q * 2, 3)
        net = (float(r['outcome_pnl']) - HAIR) * q * DPP - FEE * q
        ct = t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct, r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        closed.append((ct, net))
        out.append({'d': dd, 'net': net, 'q': q, 'long': r['is_long'],
                    'pnl': float(r['outcome_pnl'])})
    return pd.DataFrame(out)


# ---------- 1. per-contract edge ----------
base = run(None)
L = base[base['long']]
L = L.assign(trig=[LNGDAY(prev.get(d)) for d in L['d']])
print("=" * 108)
print("(1) IS THE EDGE PER CONTRACT DIFFERENT?   V21 book, longs only, NORMAL size in both groups")
print("=" * 108)
print("  %-26s%8s%8s%12s%10s%12s" % ('long trades', 'n', 'days', 'pt/trade', 'WR%', 'total pt'))
for lbl, sub in [('on TRIGGER days', L[L['trig']]), ('on every other day', L[~L['trig']])]:
    print("  %-26s%8d%8d%+12.2f%10.0f%+12.0f" % (
        lbl, len(sub), sub['d'].nunique(), sub['pnl'].mean(),
        (sub['pnl'] > 0).mean() * 100, sub['pnl'].sum()))
a = L[L['trig']]['pnl']; b = L[~L['trig']]['pnl']
se = (a.var() / len(a) + b.var() / len(b)) ** 0.5
print("  difference %+.2f pt/trade   t = %.2f" % (a.mean() - b.mean(), (a.mean() - b.mean()) / se))
print()
print("  -> if this difference were ~0, V22 would be pure leverage.")

# ---------- 2. random-day control ----------
trig_days = set(d for d in ds if LNGDAY(prev.get(d)) and d >= pd.to_datetime('2026-03-01').date())
tradable = sorted(set(base['d']))
trig_tradable = [d for d in tradable if d in trig_days]
real = run(trig_days)
real_mo = real['net'].sum() / N_SESS * 21
k = len(trig_tradable)
print()
print("=" * 108)
print("(2) RANDOM CONTROL - size up longs on %d RANDOMLY chosen days instead" % k)
print("=" * 108)
vals = []
TRIALS = 300
for _ in range(TRIALS):
    pick = set(tradable[i] for i in rng.choice(len(tradable), size=k, replace=False))
    vals.append(run(pick)['net'].sum() / N_SESS * 21)
vals = np.array(vals)
b0 = base['net'].sum() / N_SESS * 21
print("  V21, no size-up            %+8.0f $/mo" % b0)
print("  V22, the REAL %2d days      %+8.0f $/mo" % (k, real_mo))
print("  %d RANDOM days (%d trials) %+8.0f $/mo   sd %.0f" % (k, TRIALS, vals.mean(), vals.std()))
print("  random >= real: %d / %d   ->  p = %.3f" % (int((vals >= real_mo).sum()), TRIALS,
                                                    (vals >= real_mo).mean()))
print()
print("  -> pure leverage would put the real days in the MIDDLE of the random cloud.")

# ---------- 3. same-size control ----------
print()
print("=" * 108)
print("(3) WHERE DOES THE GAIN COME FROM - good days, or more contracts?")
print("=" * 108)
print("  V21 (trigger days at normal size)  %+8.0f $/mo" % b0)
print("  V22 (trigger days sized up)        %+8.0f $/mo   (+%.0f)" % (real_mo, real_mo - b0))
lt = L[L['trig']]
print()
print("  trigger-day longs at normal size already earn %+.2f pt/trade vs %+.2f elsewhere."
      % (a.mean(), b.mean()))
print("  So the extra contracts are being added to trades that were ALREADY better,")
print("  which is selection. Leverage alone would add contracts to average trades.")
