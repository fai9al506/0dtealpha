# -*- coding: utf-8 -*-
"""S286 — Skew Charm scaling plan, optimised for MARGIN not just money.

The user's observation, which the data supports everywhere we look: Skew Charm is
better stacked, better sized, better with more slots. So the question is not "should
we scale it" but "where do we put each extra contract so it buys the most edge per
dollar of margin".

Key idea tested here: size ONLY the stacked trades. Slot 1 and slot 2 SC shorts earn
+$24.9 and +$45.3 per trade against +$11.5 for the first one. Sizing only those puts
contracts exactly where the edge is, and costs far less peak margin than sizing
everything.

Margin: $265 per MES (measured from S275c: 12 MES = $3,178). Broker needs peak margin
to stay under ~70% of account equity.
"""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

sys.path.insert(0, 'app')
import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
MARGIN_PER_MES = 265.0
SAFE_USE = 0.70          # keep peak margin under 70% of equity
SHORT_EQUITY = 3271.61   # 210VYX91 today
LONG_EQUITY = 2744.31    # 210VYX65 today
N_SESS = 117
SAR = 3.75

E = create_engine(os.environ["DATABASE_URL"])
c = E.connect().execution_options(isolation_level="AUTOCOMMIT")
gaps = lf.load_gaps(c)
rows = c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min
    FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def base_qty(r):
    v = r.get('basket_pct')
    if v is None:
        return 1
    v = float(v)
    return 1 if abs(v) < DEAD else (2 if ((v > 0) == r['is_long']) else 1)


def run(sizer, max_long=2, max_short=3):
    """sizer(row, slot) -> extra multiplier on top of basket sizing.
    Returns trades + peak concurrent contracts per side (that IS the margin)."""
    openp, last, out = [], {}, []
    peak_l = peak_s = 0
    for r in rows:
        if not lf.passes_v20(r, gaps):
            continue
        t = r['et']
        openp = [p for p in openp if p[0] > t]
        n_same = sum(1 for p in openp if p[1] == r['is_long'])
        if n_same >= (max_long if r['is_long'] else max_short):
            continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90:
            continue
        last[k] = t
        q = base_qty(r) * sizer(r, n_same)
        openp.append((t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)),
                      r['is_long'], q))
        cl = sum(p[2] for p in openp if p[1]);  cs = sum(p[2] for p in openp if not p[1])
        peak_l, peak_s = max(peak_l, cl), max(peak_s, cs)
        pts = float(r['outcome_pnl'])
        out.append({'d': t.date(), 'setup': r['setup_name'], 'long': r['is_long'],
                    'slot': n_same, 'q': q, 'pts': pts,
                    'net': (pts - HAIR) * q * DPP - FEE * q})
    return pd.DataFrame(out), peak_l, peak_s


def is_sc_short(r):
    return r['setup_name'] == 'Skew Charm' and not r['is_long']


PLANS = {
    'R0  now — everything 1x':            lambda r, s: 1,
    'R1a SC-short 2x, ALL':               lambda r, s: 2 if is_sc_short(r) else 1,
    'R1b SC-short 2x, STACKED only':      lambda r, s: 2 if (is_sc_short(r) and s >= 1) else 1,
    'R1c SC-short 2x slot1, 3x slot2':    lambda r, s: (3 if s >= 2 else 2) if (is_sc_short(r) and s >= 1) else 1,
    'R2a SC-short 3x, ALL':               lambda r, s: 3 if is_sc_short(r) else 1,
    'R2b SC-short 2x base, 4x stacked':   lambda r, s: (4 if s >= 1 else 2) if is_sc_short(r) else 1,
    'R3  whole book 2x (for contrast)':   lambda r, s: 2,
}

print("=" * 118)
print("SKEW CHARM SCALING — money, risk AND margin. 117 sessions, V20, costs charged.")
print("=" * 118)
print(f"  {'plan':34s}{'trades':>7s}{'$/mo':>9s}{'worst mo':>10s}{'best mo':>9s}"
      f"{'MaxDD':>9s}{'pk SHORT':>9s}{'margin$':>9s}{'need eq':>9s}")
res = {}
for name, sz in PLANS.items():
    df, pl, ps = run(sz)
    res[name] = (df, pl, ps)
    df = df.copy(); df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    sess = df.groupby('mo')['d'].nunique()
    per = df.groupby('mo')['net'].sum() / pd.Series(
        {m: max(len(set(x for x in df[df['mo'] == m]['d'])), 1) for m in sess.index}) * 21
    # normalise by calendar sessions per month instead
    cal = {'2026-03': 22, '2026-04': 21, '2026-05': 20, '2026-06': 21, '2026-07': 22, '2026-08': 11}
    per = df.groupby('mo')['net'].sum() / pd.Series(cal).reindex(sess.index) * 21
    d = df.groupby('d')['net'].sum(); eq = d.cumsum()
    marg = ps * MARGIN_PER_MES
    print(f"  {name:34s}{len(df):>7d}{df['net'].sum()/N_SESS*21:>+9,.0f}{per.min():>+10,.0f}"
          f"{per.max():>+9,.0f}{float((eq-eq.cummax()).min()):>+9,.0f}{ps:>9.0f}"
          f"{marg:>9,.0f}{marg/SAFE_USE:>9,.0f}")

print()
print("=" * 118)
print("MONEY PER DOLLAR OF MARGIN — the number that decides where a contract goes")
print("=" * 118)
r0 = res['R0  now — everything 1x']
base_mo = r0[0]['net'].sum() / N_SESS * 21
base_marg = r0[2] * MARGIN_PER_MES
print(f"  {'plan':34s}{'$/mo':>9s}{'extra $/mo':>12s}{'extra margin':>14s}{'$ per $100 margin':>19s}")
for name, (df, pl, ps) in res.items():
    mo = df['net'].sum() / N_SESS * 21
    marg = ps * MARGIN_PER_MES
    dm, dg = mo - base_mo, marg - base_marg
    eff = (dm / dg * 100) if dg > 0 else float('nan')
    print(f"  {name:34s}{mo:>+9,.0f}{dm:>+12,.0f}{dg:>+14,.0f}"
          f"{('%+.1f' % eff) if eff == eff else '        —':>19s}")

print()
print("=" * 118)
print("GAP RISK — a 40-pt gap through the stops, as a share of the SHORT account")
print("=" * 118)
for name, (df, pl, ps) in res.items():
    gap = ps * 40 * DPP
    print(f"  {name:34s} peak {ps:>4.0f} MES  40-pt gap = ${gap:>7,.0f}  "
          f"= {gap/SHORT_EQUITY*100:>5.0f}% of the short account today")

print()
print("=" * 118)
print("FUNDING — what each rung needs before it can be armed")
print("=" * 118)
print(f"  short account today: ${SHORT_EQUITY:,.2f}   long account: ${LONG_EQUITY:,.2f}   "
      f"(NO cross-margin)")
for name, (df, pl, ps) in res.items():
    need = ps * MARGIN_PER_MES / SAFE_USE
    gapg = SHORT_EQUITY - need
    print(f"  {name:34s} needs ${need:>8,.0f}   {'OK today' if gapg >= 0 else 'short by $%,.0f' % -gapg}")
