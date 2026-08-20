# -*- coding: utf-8 -*-
"""S316d - does blocking the ACCELERATION regime actually help the BOOK?

Raw per-trade numbers are not the test. Under the 2/3 cap a blocked trade frees a slot
that a weaker trade fills, so the only honest measure is the full replay:
V21 + cap 2/3 + 90s dedup + S203 underwater + $300 breaker + basket sizing, costs inside.

Variants: block ACCELERATION for Skew Charm only / for all setups / shorts only / longs
only. Then LOMO and a random control on whatever survives."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
DAILY = -300.0
rng = np.random.default_rng(316)

E = create_engine(os.environ['DATABASE_URL'])
c = E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps = lf.load_gaps(c)
moves = lf.load_prev_moves(c)
px = pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d
    from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-03-01' group by 1"""), c)
rows = c.execute(text("SELECT " + lf.COLS + """, outcome_pnl, outcome_elapsed_min, spot,
  gex_state FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
px['d'] = pd.to_datetime(px['d']).dt.date
CAL = pd.Series(pd.to_datetime(px['d']).dt.strftime('%Y-%m')).value_counts().to_dict()
N_SESS = len(px)
months = sorted(CAL)
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def run(skip, size=None):
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []
    for r in rows:
        t = r['et']
        if t.date() != d0:
            d0 = t.date(); realized = 0.0; closed = []
        for ct, nv in [x for x in closed if x[0] <= t]: realized += nv
        closed = [x for x in closed if x[0] > t]
        if not lf.passes_v20(r, gaps): continue
        if lf.v21_blocks(r, moves): continue
        if realized <= DAILY: continue
        if skip(r): continue
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
        if size is not None:
            q = size(r, q)
        net = (float(r['outcome_pnl']) - HAIR) * q * DPP - FEE * q
        ct = t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct, r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        closed.append((ct, net))
        out.append({'d': t.date(), 'net': net, 'q': q})
    df = pd.DataFrame(out); df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    return df


NONE = lambda r: False
ACC = lambda r: (r.get('gex_state') == 'ACCELERATION')
base = run(NONE)
bs = base.groupby('mo')['net'].sum()
bmo = base['net'].sum() / N_SESS * 21


def line(lbl, df):
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(months) * 21
    dd = df.groupby('d')['net'].sum(); eq = dd.cumsum()
    s = df.groupby('mo')['net'].sum()
    dl = [float(s.get(m, 0) - bs.get(m, 0)) for m in months]
    mo = df['net'].sum() / N_SESS * 21
    print("  %-38s%7d%+9.0f%+9.0f%+9.0f%+9.0f%8s" % (
        lbl, len(df), mo, mo - bmo, per.min(), float((eq - eq.cummax()).min()),
        "%d/%d" % (sum(1 for x in dl if x >= -1), len(months))))
    return mo - bmo


print("=" * 112)
print("BLOCK THE ACCELERATION REGIME - full replay on top of V21, %d sessions" % N_SESS)
print("=" * 112)
print("  %-38s%7s%9s%9s%9s%9s%8s" % ('variant', 'trades', '$/mo', 'vs V21', 'min mo', 'MaxDD', 'LOMO'))
line('V21 (live today)', base)
d1 = line('block ACCEL - Skew Charm only', run(lambda r: ACC(r) and r['setup_name'] == 'Skew Charm'))
d2 = line('block ACCEL - ALL setups', run(ACC))
d3 = line('block ACCEL - SHORTS only', run(lambda r: ACC(r) and not r['is_long']))
d4 = line('block ACCEL - LONGS only', run(lambda r: ACC(r) and r['is_long']))
line('block ACCEL - SC + AG Short', run(lambda r: ACC(r) and r['setup_name'] in ('Skew Charm', 'AG Short')))

print()
print("=" * 112)
print("SIZE DOWN instead of blocking - ACCELERATION trades take 1 MES, never the basket 2x")
print("=" * 112)
print("  %-38s%7s%9s%9s%9s%9s%8s" % ('variant', 'trades', '$/mo', 'vs V21', 'min mo', 'MaxDD', 'LOMO'))
line('V21 (live today)', base)
line('ACCEL -> 1 MES, Skew Charm only', run(NONE, lambda r, q: 1 if (ACC(r) and r['setup_name']=='Skew Charm') else q))
line('ACCEL -> 1 MES, ALL setups', run(NONE, lambda r, q: 1 if ACC(r) else q))
line('ACCEL -> 1 MES, SHORTS only', run(NONE, lambda r, q: 1 if (ACC(r) and not r['is_long']) else q))
line('MEAN_REVERSION -> keep 2x, rest 1', run(NONE, lambda r, q: q if r.get('gex_state')=='MEAN_REVERSION' else 1))

print()
print("=" * 112)
print("IS ACCELERATION VOL-DEPENDENT?  raw per-trade, Skew Charm, by VIX band")
print("=" * 112)
import collections
band = lambda v: '<16' if v<16 else ('16-18' if v<18 else ('18-20' if v<20 else ('20-24' if v<24 else '24+')))
agg = collections.defaultdict(lambda: [0,0.0,0])
for r in rows:
    if r['setup_name']!='Skew Charm' or not lf.passes_v20(r,gaps): continue
    if r.get('vix') is None: continue
    k=(band(float(r['vix'])), 'ACCEL' if ACC(r) else 'other')
    a=agg[k]; a[0]+=1; a[1]+=float(r['outcome_pnl']); a[2]+= (1 if float(r['outcome_pnl'])>0 else 0)
print("  %-8s%22s%22s" % ('VIX', 'ACCELERATION', 'every other state'))
print("  %-8s%9s%7s%6s%9s%7s%6s" % ('','n','pt/t','WR%','n','pt/t','WR%'))
for b in ('<16','16-18','18-20','20-24','24+'):
    a=agg.get((b,'ACCEL'),[0,0,0]); o=agg.get((b,'other'),[0,0,0])
    if a[0]+o[0]==0: continue
    print("  %-8s%9d%7.2f%6.0f%9d%7.2f%6.0f" % (b, a[0], a[1]/a[0] if a[0] else 0, a[2]/a[0]*100 if a[0] else 0,
                                                 o[0], o[1]/o[0] if o[0] else 0, o[2]/o[0]*100 if o[0] else 0))

print()
print("=" * 112)
print("BLOCK ACCELERATION ONLY BELOW A VIX FLOOR (the ES-Absorption pattern)")
print("=" * 112)
print("  %-38s%7s%9s%9s%9s%9s%8s" % ('variant', 'trades', '$/mo', 'vs V21', 'min mo', 'MaxDD', 'LOMO'))
line('V21 (live today)', base)
for FL in (16.0, 18.0, 20.0, 22.0):
    line('block ACCEL SC when VIX < %.0f' % FL,
         run(lambda r, FL=FL: ACC(r) and r['setup_name']=='Skew Charm'
             and r.get('vix') is not None and float(r['vix']) < FL))
    line('  ACCEL SC -> 1 MES when VIX < %.0f' % FL, run(NONE,
         lambda r, q, FL=FL: 1 if (ACC(r) and r['setup_name']=='Skew Charm'
             and r.get('vix') is not None and float(r['vix']) < FL) else q))

print()
print("=" * 112)
print("MONTH BY MONTH - dollar delta vs V21 (a rule must not lose in any month)")
print("=" * 112)
print("  %-38s" % 'variant' + "".join("%10s" % m[-2:] for m in months))
for lbl, f in [('block ACCEL - Skew Charm only', lambda r: ACC(r) and r['setup_name'] == 'Skew Charm'),
               ('block ACCEL - ALL setups', ACC)]:
    s = run(f).groupby('mo')['net'].sum()
    print("  %-38s" % lbl + "".join("%+10.0f" % float(s.get(m, 0) - bs.get(m, 0)) for m in months))

print()
print("=" * 112)
print("RANDOM CONTROL - block the same NUMBER of Skew Charm trades, chosen at random")
print("=" * 112)
n_acc = sum(1 for r in rows if ACC(r) and r['setup_name'] == 'Skew Charm'
            and lf.passes_v20(r, gaps) and not lf.v21_blocks(r, moves))
idx = [i for i, r in enumerate(rows) if r['setup_name'] == 'Skew Charm'
       and lf.passes_v20(r, gaps) and not lf.v21_blocks(r, moves)]
real = bmo + d1
vals = []
TRIALS = 300
for _ in range(TRIALS):
    kill = set(rng.choice(idx, size=min(n_acc, len(idx)), replace=False).tolist())
    ids = {id(rows[i]) for i in kill}
    vals.append(run(lambda r: id(r) in ids)['net'].sum() / N_SESS * 21)
vals = np.array(vals)
print("  V21 baseline                       %+8.0f $/mo" % bmo)
print("  block the REAL %3d ACCELERATION SC  %+8.0f $/mo" % (n_acc, real))
print("  block %3d RANDOM SC (%d trials)     %+8.0f $/mo   sd %.0f" % (n_acc, TRIALS, vals.mean(), vals.std()))
print("  random >= real: %d / %d   ->  p = %.3f" % (int((vals >= real).sum()), TRIALS, (vals >= real).mean()))
