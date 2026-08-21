# -*- coding: utf-8 -*-
"""S312 - the user's finding: V21 measures the previous session OPEN->CLOSE, but a
market can gap down 5% and sit flat, reading ~0%. The move everyone else quotes
(TradingView et al) is CLOSE->CLOSE. Test that measure, a threshold sweep, and the
user's second idea: a deep DOWN GAP followed by a FLAT session is itself bullish.

Same replay as S301 (which shipped V21): V20 + cap 2/3 + 90s dedup + S203 underwater
+ $300 breaker + basket sizing max(qty,2) + haircut/fees inside the sim.
Excludes today (partial session)."""
import os, sys, pickle
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
DAILY = -300.0
VIXMAX = 24.0          # V21 ceiling - the effect inverts above it, keep those shorts
END = '2026-08-19'     # exclusive: today is still running

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
day['oc'] = (day['close'] - day['open']) / day['open'] * 100          # what V21 uses today
day['cc'] = (day['close'] - day['pclose']) / day['pclose'] * 100      # TradingView daily change
day['gap'] = (day['open'] - day['pclose']) / day['pclose'] * 100      # overnight gap

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
    realized = 0.0
    d0 = None
    closed = []
    for r in rows:
        t = r['et']
        if t.date() != d0:
            d0 = t.date(); realized = 0.0; closed = []
        for ct, nv in [x for x in closed if x[0] <= t]:
            realized += nv
        closed = [x for x in closed if x[0] > t]
        if not lf.passes_v20(r, gaps):
            continue
        if realized <= DAILY:
            continue
        if skip(r, prev.get(t.date())):
            continue
        openp = [p for p in openp if p[0] > t]
        n = sum(1 for p in openp if p[1] == r['is_long'])
        if n >= (2 if r['is_long'] else 3):
            continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90:
            continue
        sib = [p for p in openp if p[1] == r['is_long'] and p[3] == r['setup_name']]
        if len(sib) >= 2 and r.get('spot'):
            sgn = 1.0 if r['is_long'] else -1.0
            if sum((float(r['spot']) - p[2]) * sgn for p in sib) < 0:
                continue
        last[k] = t
        v = r.get('basket_pct')
        q = 1 if v is None or abs(float(v)) < DEAD else (2 if ((float(v) > 0) == r['is_long']) else 1)
        pts = float(r['outcome_pnl'])
        net = (pts - HAIR) * q * DPP - FEE * q
        ct = t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct, r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        closed.append((ct, net))
        out.append({'d': t.date(), 'net': net, 'long': r['is_long'], 'setup': r['setup_name']})
    df = pd.DataFrame(out)
    df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    return df


def vixok(r):
    v = r.get('vix')
    return v is not None and float(v) < VIXMAX


NONE = lambda r, p: False


def oc_rule(thr):
    return lambda r, p: (not r['is_long']) and p is not None and p['oc'] is not None \
        and p['oc'] < thr and vixok(r)


def cc_rule(thr):
    return lambda r, p: (not r['is_long']) and p is not None and p['cc'] is not None \
        and p['cc'] < thr and vixok(r)


def gapflat(gthr, fthr):
    return lambda r, p: (not r['is_long']) and p is not None and p['gap'] is not None \
        and p['oc'] is not None and p['gap'] < gthr and abs(p['oc']) < fthr and vixok(r)


def cc_or_gapflat(cthr, gthr, fthr):
    a = cc_rule(cthr); b = gapflat(gthr, fthr)
    return lambda r, p: a(r, p) or b(r, p)


RULES = {}
RULES['V20 (no down-day rule)'] = NONE
RULES['V21 SHIPPED: oc < -0.8'] = oc_rule(-0.8)
for t in (-0.50, -0.60, -0.65, -0.70, -0.80, -0.90, -1.00, -1.20):
    RULES['CLOSE-CLOSE < %.2f%%' % t] = cc_rule(t)
RULES['GAP<-0.5 & |oc|<0.3 flat'] = gapflat(-0.5, 0.3)
RULES['GAP<-0.4 & |oc|<0.4 flat'] = gapflat(-0.4, 0.4)
RULES['CC<-0.70 OR gapflat'] = cc_or_gapflat(-0.70, -0.5, 0.3)
RULES['CC<-0.65 OR gapflat'] = cc_or_gapflat(-0.65, -0.5, 0.3)

print("=" * 124)
print("S312 - HOW SHOULD THE PREVIOUS DAY DROP BE MEASURED?   full replay: V20 + cap 2/3 + dedup + S203 + $300 breaker")
print("        window 2026-03-01 .. %s (excl)   sessions=%d   VIX ceiling %.0f   basket sizing max(qty,2)" % (END, N_SESS, VIXMAX))
print("=" * 124)
print("  %-30s%7s%8s%9s%9s%9s%9s%10s%10s%9s" % ('rule', 'trades', 'blocked', '$/mo', 'min mo', 'max mo', 'MaxDD', 'worstday', 'worst wk', '$/trade'))
res = {}
base_n = None
for k, f in RULES.items():
    df = run(f)
    res[k] = df
    if base_n is None:
        base_n = len(df)
    per = df.groupby('mo')['net'].sum() / pd.Series(CAL).reindex(sorted(df['mo'].unique())) * 21
    dd = df.groupby('d')['net'].sum()
    eq = dd.cumsum()
    print("  %-30s%7d%8d%+9,.0f%+9,.0f%+9,.0f%+9,.0f%+10,.0f%+10,.0f%+9.1f".replace(',', '') % (
        k, len(df), base_n - len(df), df['net'].sum() / N_SESS * 21, per.min(), per.max(),
        float((eq - eq.cummax()).min()), dd.min(), dd.rolling(5).sum().min(),
        df['net'].sum() / len(df)))

pickle.dump({'res': res, 'CAL': CAL, 'N_SESS': N_SESS}, open('_tmp_s312_res.pkl', 'wb'))
print()
print("saved _tmp_s312_res.pkl")
