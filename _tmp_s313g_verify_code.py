# -*- coding: utf-8 -*-
"""S313 stage 7 - prove the SHIPPED CODE reproduces the study.

The study used its own inline lambdas. This replays the same window driving the real
functions that will run in production - live_filter.v22_blocks and
live_filter.v22_long_qty - and must land on the same numbers:
    V22 = $2,549/mo, worst month +$1,266, MaxDD -$906
Plus fail-safe unit checks on the sizing helper."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
DAILY = -300.0

print("=" * 100)
print("(A) SHIPPED CONSTANTS")
print("=" * 100)
print("  LIVE_VER        = %s" % lf.LIVE_VER)
print("  V22_PREV_DROP   = %s" % lf.V22_PREV_DROP)
print("  V22_VIX_MAX     = %s" % lf.V22_VIX_MAX)
print("  V22_LONG_CAP    = %s" % lf.V22_LONG_CAP)

print()
print("=" * 100)
print("(B) FAIL-SAFE UNIT CHECKS on live_filter.v22_long_qty")
print("=" * 100)
import datetime as _dt
D = _dt.datetime(2026, 8, 19, 10, 0, tzinfo=_dt.timezone.utc)
MOV = {'2026-08-19': -0.90}
cases = [
    ("long, trigger day, q=1", {'direction': 'long', 'vix': 15.0, 'ts': D}, MOV, 1, 2),
    ("long, trigger day, q=2", {'direction': 'long', 'vix': 15.0, 'ts': D}, MOV, 2, 3),
    ("long, trigger day, q=3 (cap holds)", {'direction': 'long', 'vix': 15.0, 'ts': D}, MOV, 3, 3),
    ("SHORT never sized", {'direction': 'short', 'vix': 15.0, 'ts': D}, MOV, 2, 2),
    ("VIX >= 24 -> no size-up", {'direction': 'long', 'vix': 25.0, 'ts': D}, MOV, 2, 2),
    ("move above threshold", {'direction': 'long', 'vix': 15.0, 'ts': D}, {'2026-08-19': -0.10}, 2, 2),
    ("move UNKNOWN -> unchanged", {'direction': 'long', 'vix': 15.0, 'ts': D}, {}, 2, 2),
    ("vix None -> unchanged", {'direction': 'long', 'vix': None, 'ts': D}, MOV, 2, 2),
    ("no ts -> unchanged", {'direction': 'long', 'vix': 15.0}, MOV, 2, 2),
    ("garbage row -> unchanged", {}, MOV, 2, 2),
]
bad = 0
for lbl, row, mv, q, want in cases:
    got = lf.v22_long_qty(row, mv, q)
    ok = (got == want)
    bad += (not ok)
    print("  %-38s q=%d -> %d  (want %d)  %s" % (lbl, q, got, want, "OK" if ok else "*** FAIL ***"))
print("  failures: %d" % bad)

print()
print("=" * 100)
print("(C) BLOCK-HALF unit checks on live_filter.v22_blocks")
print("=" * 100)
bcases = [
    ("short, trigger day -> BLOCK", {'setup_name': 'Skew Charm', 'direction': 'short', 'vix': 15.0, 'ts': D}, MOV, True),
    ("long, trigger day -> allow", {'setup_name': 'Skew Charm', 'direction': 'long', 'vix': 15.0, 'ts': D}, MOV, False),
    ("short, VIX 25 -> allow", {'setup_name': 'Skew Charm', 'direction': 'short', 'vix': 25.0, 'ts': D}, MOV, False),
    ("short, move unknown -> allow (fail OPEN)", {'setup_name': 'Skew Charm', 'direction': 'short', 'vix': 15.0, 'ts': D}, {}, False),
    ("short, move -0.4 -> allow", {'setup_name': 'Skew Charm', 'direction': 'short', 'vix': 15.0, 'ts': D}, {'2026-08-19': -0.4}, False),
]
for lbl, row, mv, want in bcases:
    got = lf.v22_blocks(row, mv)
    ok = (got == want)
    bad += (not ok)
    print("  %-42s -> %-5s (want %-5s)  %s" % (lbl, got, want, "OK" if ok else "*** FAIL ***"))

print()
print("=" * 100)
print("(D) FULL REPLAY DRIVEN BY THE SHIPPED FUNCTIONS")
print("=" * 100)
E = create_engine(os.environ['DATABASE_URL'])
c = E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps = lf.load_gaps(c)
moves = lf.load_prev_moves(c)
px = pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d from spx_ohlc_1m
    where (ts at time zone 'America/New_York')>='2026-03-01' group by 1"""), c)
rows = c.execute(text("SELECT " + lf.COLS + """, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
px['d'] = pd.to_datetime(px['d']).dt.date
CAL = pd.Series(pd.to_datetime(px['d']).dt.strftime('%Y-%m')).value_counts().to_dict()
N_SESS = len(px)
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def run(use_v22):
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []; pl = 0
    for r in rows:
        t = r['et']
        if t.date() != d0:
            d0 = t.date(); realized = 0.0; closed = []
        for ct, nv in [x for x in closed if x[0] <= t]: realized += nv
        closed = [x for x in closed if x[0] > t]
        if not lf.passes_v20(r, gaps): continue
        if realized <= DAILY: continue
        if use_v22 and lf.v22_blocks(r, moves): continue          # <-- shipped function
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
        if use_v22:
            q = lf.v22_long_qty(r, moves, q)                       # <-- shipped function
        net = (float(r['outcome_pnl']) - HAIR) * q * DPP - FEE * q
        ct = t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct, r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        closed.append((ct, net))
        pl = max(pl, sum(o[4] for o in openp if o[1]))
        out.append({'d': t.date(), 'net': net, 'q': q, 'long': r['is_long']})
    df = pd.DataFrame(out); df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    df.attrs['peak_long'] = pl
    return df


for lbl, f in [('V20 baseline', False), ('V22 (shipped code)', True)]:
    df = run(f)
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(sorted(df['mo'].unique())) * 21
    dd = df.groupby('d')['net'].sum(); eq = dd.cumsum()
    print("  %-22s trades %4d  contracts %4d  $/mo %+6.0f  worst month %+7.0f  MaxDD %+7.0f  peakLONG %d MES"
          % (lbl, len(df), df['q'].sum(), df['net'].sum() / N_SESS * 21, per.min(),
             float((eq - eq.cummax()).min()), df.attrs['peak_long']))
print()
print("  EXPECTED from the study: V22 $/mo +2549, worst month +1266, MaxDD -906, peakLONG 6")
print("  unit-check failures: %d" % bad)
