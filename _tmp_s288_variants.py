# -*- coding: utf-8 -*-
"""S288 — full, audited comparison of every Skew Charm sizing variant.

THE TWO SIZING RULES AND HOW THEY COMBINE
-----------------------------------------
Rule already live : tech basket AGREES with the trade  -> 2 contracts instead of 1.
Rule proposed     : stacked SC short (2nd/3rd same-side) -> more contracts.

When BOTH apply to the same trade:
  max()      = take the larger of the two.  2 and 2 -> 2.   <- what real_trader does today
  multiply() = multiply them.               2 x 2   -> 4.   <- what my first sim wrongly did

Everything below is measured on: 117 calendar sessions 2026-03-02 -> 08-17, filter V20,
chain `outcome_pnl`, -0.6 pt/contract + $1.92/contract round-turn charged inside, cap
2 long / 3 short, 90s dedup. Margin $265/MES. Short account held $3,271.61 on 08-17.
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
MARGIN, SAFE, N_SESS = 265.0, 0.70, 117
SHORT_EQ, MAIN_CAP = 3271.61, 6015.92
CAL = {'2026-03': 22, '2026-04': 21, '2026-05': 20, '2026-06': 21, '2026-07': 22, '2026-08': 11}

E = create_engine(os.environ["DATABASE_URL"])
c = E.connect().execution_options(isolation_level="AUTOCOMMIT")
gaps = lf.load_gaps(c)
rows = c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def confirms(r):
    v = r.get('basket_pct')
    if v is None:
        return False
    v = float(v)
    return abs(v) >= DEAD and ((v > 0) == r['is_long'])


def is_sc_short(r):
    return r['setup_name'] == 'Skew Charm' and not r['is_long']


def run(qtyfn):
    openp, last, out = [], {}, []
    peak_s = 0
    for r in rows:
        if not lf.passes_v20(r, gaps):
            continue
        t = r['et']
        openp = [p for p in openp if p[0] > t]
        n = sum(1 for p in openp if p[1] == r['is_long'])
        if n >= (2 if r['is_long'] else 3):
            continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90:
            continue
        last[k] = t
        q = qtyfn(r, n)
        openp.append((t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)), r['is_long'], q))
        peak_s = max(peak_s, sum(p[2] for p in openp if not p[1]))
        pts = float(r['outcome_pnl'])
        out.append({'d': t.date(), 'setup': r['setup_name'], 'long': r['is_long'], 'slot': n,
                    'q': q, 'pts': pts, 'net': (pts - HAIR) * q * DPP - FEE * q})
    df = pd.DataFrame(out)
    df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    return df, peak_s


def basket(r):
    return 2 if confirms(r) else 1


VARIANTS = {
    'R0   today (nothing new)':        lambda r, n: basket(r),
    'R1b  stacked -> at least 2':      lambda r, n: max(basket(r), 2 if (is_sc_short(r) and n >= 1) else 1),
    'R1c  stacked -> at least 3':      lambda r, n: max(basket(r), 3 if (is_sc_short(r) and n >= 1) else 1),
    'R1d  slot1 -> 2, slot2 -> 3':     lambda r, n: max(basket(r), 3 if (is_sc_short(r) and n >= 2) else (2 if (is_sc_short(r) and n >= 1) else 1)),
    'R1e  stacked -> at least 4':      lambda r, n: max(basket(r), 4 if (is_sc_short(r) and n >= 1) else 1),
    'R1b-MULT (multiply, not max)':    lambda r, n: basket(r) * (2 if (is_sc_short(r) and n >= 1) else 1),
}

res = {k: run(f) for k, f in VARIANTS.items()}

print("=" * 122)
print("A. EVERY VARIANT — money, risk, margin")
print("=" * 122)
print(f"  {'variant':30s}{'$/mo':>9s}{'min mo':>9s}{'max mo':>9s}{'MaxDD':>9s}{'worst day':>11s}"
      f"{'>breaker':>9s}{'pk short':>9s}{'margin$':>9s}{'need eq':>9s}{'gap%':>7s}")
for k, (df, ps) in res.items():
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(sorted(df['mo'].unique())) * 21
    d = df.groupby('d')['net'].sum()
    eq = d.cumsum()
    print(f"  {k:30s}{df['net'].sum()/N_SESS*21:>+9,.0f}{per.min():>+9,.0f}{per.max():>+9,.0f}"
          f"{float((eq-eq.cummax()).min()):>+9,.0f}{d.min():>+11,.0f}{int((d<-300).sum()):>9d}"
          f"{ps:>9.0f}{ps*MARGIN:>9,.0f}{ps*MARGIN/SAFE:>9,.0f}{ps*40*DPP/MAIN_CAP*100:>6.0f}%")

print()
print("=" * 122)
print("B. MONTH BY MONTH — every variant against today (the leave-one-month-out test)")
print("=" * 122)
base = res['R0   today (nothing new)'][0].groupby('mo')['net'].sum()
print(f"  {'month':9s}" + "".join(f"{k.split()[0]:>12s}" for k in VARIANTS))
for m in sorted(CAL):
    line = f"  {m:9s}"
    for k, (df, _) in res.items():
        line += f"{df.groupby('mo')['net'].sum().get(m, 0):>+12,.0f}"
    print(line)
print(f"  {'wins':9s}" + "".join(
    f"{sum(1 for m in CAL if res[k][0].groupby('mo')['net'].sum().get(m,0) > base.get(m,0)):>9d}/6"
    for k in VARIANTS))

print()
print("=" * 122)
print("C. CONTRACT SIZES ACTUALLY USED on Skew Charm SHORT (322 trades)")
print("=" * 122)
for k, (df, ps) in res.items():
    sc = df[(df['setup'] == 'Skew Charm') & (~df['long'])]
    sizes = "  ".join(f"{int(q)}x: {n:3d}" for q, n in sc['q'].value_counts().sort_index().items())
    print(f"  {k:30s} {sizes}   total contracts {int(sc['q'].sum()):4d}")

print()
print("=" * 122)
print("D. WHERE THE EXTRA MONEY COMES FROM — only the stacked SC shorts change")
print("=" * 122)
r0 = res['R0   today (nothing new)'][0]
for k, (df, ps) in res.items():
    if k.startswith('R0'):
        continue
    sc0 = r0[(r0['setup'] == 'Skew Charm') & (~r0['long']) & (r0['slot'] >= 1)]
    sc1 = df[(df['setup'] == 'Skew Charm') & (~df['long']) & (df['slot'] >= 1)]
    print(f"  {k:30s} stacked SC shorts: {len(sc1):3d} trades   "
          f"${sc0['net'].sum():+7,.0f} -> ${sc1['net'].sum():+7,.0f}   "
          f"(+${sc1['net'].sum()-sc0['net'].sum():,.0f} over the whole window)")

print()
print("=" * 122)
print("E. FUNDING — with today's $3,271.61 and with +$1,000")
print("=" * 122)
for k, (df, ps) in res.items():
    need = ps * MARGIN / SAFE
    print(f"  {k:30s} peak {ps:>3.0f} MES  ${ps*MARGIN:>6,.0f} margin  needs ${need:>7,.0f}   "
          f"today: {'OK' if SHORT_EQ >= need else 'SHORT $%.0f' % (need-SHORT_EQ)}   "
          f"+$1k: {'OK' if SHORT_EQ+1000 >= need else 'SHORT $%.0f' % (need-SHORT_EQ-1000)}")
