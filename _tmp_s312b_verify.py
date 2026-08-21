# -*- coding: utf-8 -*-
"""S312b - verify the S312 headline. Three things:
 (1) DIRECT evidence: how do SHORTS actually perform, bucketed by the previous
     session's close-close move, its overnight gap, and gap-then-flat? No replay,
     no cap - just the raw conditional performance, so the mechanism is visible.
 (2) LOMO on every candidate rule (checklist item 7).
 (3) The OR-combination: V21's open-close test OR a close-close test.
Plus a random control for the winner."""
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
day['gap'] = (day['open'] - day['pclose']) / day['pclose'] * 100
prev = {}
ds = list(day['d'])
for i in range(1, len(ds)):
    p = day.iloc[i - 1]
    prev[ds[i]] = {'oc': p['oc'], 'cc': p['cc'], 'gap': p['gap']}
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
        out.append({'d': t.date(), 'net': net, 'long': r['is_long']})
    df = pd.DataFrame(out)
    df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    return df


def vixok(r):
    v = r.get('vix')
    return v is not None and float(v) < VIXMAX


# ---------- (1) DIRECT conditional performance of SHORTS ----------
print("=" * 116)
print("(1) DIRECT EVIDENCE - how V20 SHORTS actually performed, by what the PREVIOUS session did")
print("    raw signal points (no cap), VIX<24 only, so the mechanism is visible without replay noise")
print("=" * 116)
sh = []
for r in rows:
    if r['is_long']: continue
    if not lf.passes_v20(r, gaps): continue
    if not vixok(r): continue
    p = prev.get(r['et'].date())
    if p is None or p['cc'] is None or p['gap'] is None: continue
    sh.append({'pnl': float(r['outcome_pnl']), 'cc': p['cc'], 'oc': p['oc'], 'gap': p['gap']})
sh = pd.DataFrame(sh)
print("  shorts in sample: %d" % len(sh))


def bucket(col, edges, label):
    print("\n  --- by previous session %s ---" % label)
    print("  %-22s%7s%11s%9s%9s" % ('bucket', 'n', 'pt/trade', 'WR%', 'total pt'))
    for lo, hi in edges:
        m = sh[(sh[col] >= lo) & (sh[col] < hi)]
        if len(m) == 0: continue
        print("  %-22s%7d%+11.2f%9.0f%+9.0f" % (
            "%+.2f%% .. %+.2f%%" % (lo, hi), len(m), m['pnl'].mean(),
            (m['pnl'] > 0).mean() * 100, m['pnl'].sum()))


bucket('cc', [(-9, -1.0), (-1.0, -0.8), (-0.8, -0.6), (-0.6, -0.3), (-0.3, 0), (0, 0.5), (0.5, 9)], 'CLOSE->CLOSE')
bucket('oc', [(-9, -1.0), (-1.0, -0.8), (-0.8, -0.5), (-0.5, 0), (0, 0.5), (0.5, 9)], 'OPEN->CLOSE (what V21 uses)')
bucket('gap', [(-9, -0.5), (-0.5, -0.2), (-0.2, 0.2), (0.2, 9)], 'OVERNIGHT GAP')

print("\n  --- the user's idea: previous session GAPPED DOWN then went FLAT ---")
gf = sh[(sh['gap'] < -0.5) & (sh['oc'].abs() < 0.3)]
rest = sh[~((sh['gap'] < -0.5) & (sh['oc'].abs() < 0.3))]
print("  gap<-0.5%% and |open-close|<0.3%% :  n=%3d  %+.2f pt/trade  WR %.0f%%  total %+.0f"
      % (len(gf), gf['pnl'].mean() if len(gf) else 0, (gf['pnl'] > 0).mean() * 100 if len(gf) else 0, gf['pnl'].sum()))
print("  every other short              :  n=%3d  %+.2f pt/trade  WR %.0f%%  total %+.0f"
      % (len(rest), rest['pnl'].mean(), (rest['pnl'] > 0).mean() * 100, rest['pnl'].sum()))

# ---------- (2) candidate rules, LOMO ----------
def oc_rule(thr):
    return lambda r, p: (not r['is_long']) and p is not None and p['oc'] < thr and vixok(r)
def cc_rule(thr):
    return lambda r, p: (not r['is_long']) and p is not None and p['cc'] < thr and vixok(r)
def either(ot, ct):
    return lambda r, p: (not r['is_long']) and p is not None and (p['oc'] < ot or p['cc'] < ct) and vixok(r)


CAND = {
    'V20 (none)': lambda r, p: False,
    'V21 oc<-0.8 (SHIPPED)': oc_rule(-0.8),
    'cc<-0.60': cc_rule(-0.60),
    'cc<-1.20': cc_rule(-1.20),
    'oc<-0.8 OR cc<-0.60': either(-0.8, -0.60),
    'oc<-0.8 OR cc<-0.80': either(-0.8, -0.80),
    'oc<-0.8 OR cc<-1.00': either(-0.8, -1.00),
    'oc<-0.8 OR cc<-1.20': either(-0.8, -1.20),
}
print()
print("=" * 116)
print("(2) CANDIDATES - full replay")
print("=" * 116)
print("  %-24s%7s%9s%9s%9s%9s%10s" % ('rule', 'trades', '$/mo', 'min mo', 'max mo', 'MaxDD', 'worst day'))
R = {}
for k, f in CAND.items():
    df = run(f); R[k] = df
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(sorted(df['mo'].unique())) * 21
    dd = df.groupby('d')['net'].sum(); eq = dd.cumsum()
    print("  %-24s%7d%+9.0f%+9.0f%+9.0f%+9.0f%+10.0f" % (
        k, len(df), df['net'].sum() / N_SESS * 21, per.min(), per.max(),
        float((eq - eq.cummax()).min()), dd.min()))

print()
print("=" * 116)
print("(3) LEAVE-ONE-MONTH-OUT  -  delta vs V20 baseline, in $ for that month (must not be negative)")
print("=" * 116)
base = R['V20 (none)'].groupby('mo')['net'].sum()
months = sorted(CAL)
print("  %-24s" % 'rule' + "".join("%10s" % m[-2:] for m in months) + "%10s" % 'months+')
for k in CAND:
    if k == 'V20 (none)': continue
    s = R[k].groupby('mo')['net'].sum()
    deltas = [float(s.get(m, 0) - base.get(m, 0)) for m in months]
    npos = sum(1 for d in deltas if d >= -1)
    print("  %-24s" % k + "".join("%+10.0f" % d for d in deltas) + "%8d/%d" % (npos, len(months)))

pickle.dump({'R': R, 'CAL': CAL, 'N_SESS': N_SESS, 'sh': sh}, open('_tmp_s312b_res.pkl', 'wb'))
