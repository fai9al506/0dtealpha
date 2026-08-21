# -*- coding: utf-8 -*-
"""S313 stage 8 - DECOMPOSITION. The user's objection, and it is correct logic:

  "Skipping shorts = removing BAD trades. Doubling longs = doubling GOOD trades.
   They should each work ALONE."

I claimed sizing longs alone 'does not work'. Check that claim honestly: run each
half on its own, with LOMO, and measure how much of the combined gain each one
carries. If they are not additive, say why."""
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
day['mo'] = pd.to_datetime(day['d']).dt.strftime('%Y-%m')
CAL = day[day['d'] >= pd.to_datetime('2026-03-01').date()].groupby('mo')['d'].count().to_dict()
N_SESS = sum(CAL.values())
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def run(skip, size):
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []; pl = 0
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
        pl = max(pl, sum(o[4] for o in openp if o[1]))
        out.append({'d': t.date(), 'net': net})
    df = pd.DataFrame(out); df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    df.attrs['peak_long'] = pl
    return df


def vixok(r):
    v = r.get('vix'); return v is not None and float(v) < VIXMAX


def fire(thr):
    def f(r, p):
        if p is None or pd.isna(p): return False
        return p < thr and vixok(r)
    return f


NOSKIP = lambda r, p: False
NOSIZE = lambda r, p, q: q
months = sorted(CAL)
base = run(NOSKIP, NOSIZE)
bs = base.groupby('mo')['net'].sum()
bmo = base['net'].sum() / N_SESS * 21


def line(lbl, df):
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(months) * 21
    dd = df.groupby('d')['net'].sum(); eq = dd.cumsum()
    s = df.groupby('mo')['net'].sum()
    dl = [float(s.get(m, 0) - bs.get(m, 0)) for m in months]
    mo = df['net'].sum() / N_SESS * 21
    print("  %-34s%+9.0f%+9.0f%+9.0f%+9.0f%8s%9d" % (
        lbl, mo, mo - bmo, per.min(), float((eq - eq.cummax()).min()),
        "%d/%d" % (sum(1 for x in dl if x >= -1), len(months)), df.attrs['peak_long']))
    return mo - bmo


for thr in (-0.8, -0.5):
    tg = fire(thr)
    print()
    print("=" * 104)
    print("TRIGGER: previous session open-to-close < %.1f%%   (VIX<24)" % thr)
    print("=" * 104)
    print("  %-34s%9s%9s%9s%9s%8s%9s" % ('variant', '$/mo', 'vs base', 'min mo', 'MaxDD', 'LOMO', 'peakLONG'))
    line('V20 baseline', base)
    a = line('A. block shorts ONLY', run(lambda r, p: (not r['is_long']) and tg(r, p), NOSIZE))
    b1 = line('B. longs floor-2 ONLY', run(NOSKIP, lambda r, p, q: max(q, 2) if (r['is_long'] and tg(r, p)) else q))
    b2 = line('B. longs x2 cap3 ONLY', run(NOSKIP, lambda r, p, q: min(q * 2, 3) if (r['is_long'] and tg(r, p)) else q))
    ab = line('A+B  block + x2 cap3', run(lambda r, p: (not r['is_long']) and tg(r, p),
                                          lambda r, p, q: min(q * 2, 3) if (r['is_long'] and tg(r, p)) else q))
    print()
    print("  DECOMPOSITION at %.1f%%:" % thr)
    print("    block shorts alone      %+8.0f/mo" % a)
    print("    long x2-cap3 alone      %+8.0f/mo" % b2)
    print("    sum if independent      %+8.0f/mo" % (a + b2))
    print("    actually together       %+8.0f/mo" % ab)
    print("    interaction             %+8.0f/mo  (%s)" % (
        ab - a - b2, "they help each other" if ab - a - b2 > 0 else "they overlap"))


# ── SPLIT THRESHOLDS: each half at ITS OWN best threshold ────────────────────────
# The decomposition shows the two halves degrade differently as the trigger widens:
# the BLOCK half loses robustness at -0.5 (LOMO 4/6) while the LONG half gains it
# (6/6). They are independent rules, so there is no reason to force one threshold.
print()
print("=" * 104)
print("SPLIT THRESHOLDS - block shorts at -0.8 (its best), size longs at -0.5 (its best)")
print("=" * 104)
print("  %-34s%9s%9s%9s%9s%8s%9s" % ('variant', '$/mo', 'vs base', 'min mo', 'MaxDD', 'LOMO', 'peakLONG'))
line('V20 baseline', base)
line('block -0.8 ONLY            (6/6)', run(lambda r, p: (not r['is_long']) and fire(-0.8)(r, p), NOSIZE))
line('long x2cap3 -0.5 ONLY      (6/6)', run(NOSKIP, lambda r, p, q: min(q*2,3) if (r['is_long'] and fire(-0.5)(r,p)) else q))
line('SPLIT block-0.8 + long-0.5', run(lambda r, p: (not r['is_long']) and fire(-0.8)(r, p),
                                       lambda r, p, q: min(q*2,3) if (r['is_long'] and fire(-0.5)(r,p)) else q))
line('same-threshold -0.5 (current V22)', run(lambda r, p: (not r['is_long']) and fire(-0.5)(r, p),
                                              lambda r, p, q: min(q*2,3) if (r['is_long'] and fire(-0.5)(r,p)) else q))
line('long x2cap3 -0.3 ONLY', run(NOSKIP, lambda r, p, q: min(q*2,3) if (r['is_long'] and fire(-0.3)(r,p)) else q))
line('SPLIT block-0.8 + long-0.3', run(lambda r, p: (not r['is_long']) and fire(-0.8)(r, p),
                                       lambda r, p, q: min(q*2,3) if (r['is_long'] and fire(-0.3)(r,p)) else q))

