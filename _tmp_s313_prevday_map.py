# -*- coding: utf-8 -*-
"""S313 stage 1 - THE MAP. For every way of describing the PREVIOUS session,
show what our LONGS and our SHORTS did, side by side, at DAY level.

Day level is the point. A day's trades all face the same tape, so N DAYS is the
sample size, not N trades. Every table below carries both.

Previous-session features (n-1 relative to n-2):
  cc   close-to-close  (the user's measure, what a chart shows)
  oc   open-to-close   (what V21 uses)
  gap  overnight gap
  gapflat  gapped down then went flat
  cum2 two-session cumulative close-to-close
"""
import os, sys, pickle
import numpy as np, pandas as pd
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
VIXMAX = 24.0
E = create_engine(os.environ['DATABASE_URL'])
c = E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps = lf.load_gaps(c)
px = pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d,
    bar_open, bar_close from spx_ohlc_1m
    where (ts at time zone 'America/New_York')>='2026-02-19' order by ts"""), c)
rows = c.execute(text("SELECT " + lf.COLS + """, outcome_pnl, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()

px['d'] = pd.to_datetime(px['d']).dt.date
g = px.groupby('d')
day = pd.DataFrame({'open': g['bar_open'].first(), 'close': g['bar_close'].last()}).reset_index()
day['pclose'] = day['close'].shift(1)
day['oc'] = (day['close'] - day['open']) / day['open'] * 100
day['cc'] = (day['close'] - day['pclose']) / day['pclose'] * 100
day['gap'] = (day['open'] - day['pclose']) / day['pclose'] * 100
day['cum2'] = day['cc'] + day['cc'].shift(1)
prev = {}
ds = list(day['d'])
for i in range(1, len(ds)):
    p = day.iloc[i - 1]
    prev[ds[i]] = {'oc': p['oc'], 'cc': p['cc'], 'gap': p['gap'], 'cum2': p['cum2']}

T = []
for r in rows:
    rr = dict(r)
    if not lf.passes_v20(rr, gaps): continue
    v = rr.get('vix')
    if v is None or float(v) >= VIXMAX: continue
    d = rr['ts'].astimezone(ET).date()
    p = prev.get(d)
    if p is None or p['cc'] is None or pd.isna(p['cc']): continue
    T.append({'d': d, 'pnl': float(rr['outcome_pnl']),
              'long': str(rr.get('direction', '')).lower() in ('long', 'bullish'),
              'cc': p['cc'], 'oc': p['oc'], 'gap': p['gap'], 'cum2': p['cum2']})
T = pd.DataFrame(T)
print("V20 signals in sample (VIX<24): %d   longs %d   shorts %d   over %d days"
      % (len(T), T['long'].sum(), (~T['long']).sum(), T['d'].nunique()))


def side(sub):
    """returns (n_trades, n_days, pt_per_trade, pt_per_day, days_green, total)"""
    if len(sub) == 0:
        return (0, 0, 0.0, 0.0, '0/0', 0.0)
    per = sub.groupby('d')['pnl'].sum()
    return (len(sub), len(per), sub['pnl'].mean(), per.mean(),
            "%d/%d" % (int((per > 0).sum()), len(per)), sub['pnl'].sum())


def table(title, col, edges):
    print()
    print("=" * 122)
    print(title)
    print("=" * 122)
    print("  %-20s | %-38s | %-38s" % ('previous session', 'S H O R T S', 'L O N G S'))
    print("  %-20s | %6s%6s%9s%9s%8s | %6s%6s%9s%9s%8s" % (
        '', 'trd', 'DAYS', 'pt/trd', 'pt/day', 'days+', 'trd', 'DAYS', 'pt/trd', 'pt/day', 'days+'))
    for lo, hi, lbl in edges:
        m = (T[col] >= lo) & (T[col] < hi)
        s = side(T[m & (~T['long'])]); l = side(T[m & (T['long'])])
        if s[0] == 0 and l[0] == 0: continue
        print("  %-20s | %6d%6d%+9.2f%+9.1f%8s | %6d%6d%+9.2f%+9.1f%8s" % (
            lbl, s[0], s[1], s[2], s[3], s[4], l[0], l[1], l[2], l[3], l[4]))


table("(1) by previous CLOSE-TO-CLOSE   (the user's measure)", 'cc', [
    (-9, -1.0, 'worse than -1.0%'), (-1.0, -0.8, '-1.0 .. -0.8%'),
    (-0.8, -0.6, '-0.8 .. -0.6% TODAY'), (-0.6, -0.3, '-0.6 .. -0.3%'),
    (-0.3, 0.0, '-0.3 .. 0%'), (0.0, 0.5, '0 .. +0.5%'), (0.5, 9, 'above +0.5%')])

table("(2) by previous OPEN-TO-CLOSE   (what V21 uses)", 'oc', [
    (-9, -0.8, 'worse than -0.8%'), (-0.8, -0.5, '-0.8 .. -0.5%'),
    (-0.5, 0.0, '-0.5 .. 0%'), (0.0, 0.5, '0 .. +0.5%'), (0.5, 9, 'above +0.5%')])

table("(3) by previous OVERNIGHT GAP", 'gap', [
    (-9, -0.5, 'gapped down >0.5%'), (-0.5, -0.2, '-0.5 .. -0.2%'),
    (-0.2, 0.2, 'flat gap'), (0.2, 9, 'gapped up >0.2%')])

table("(4) by previous TWO-SESSION cumulative", 'cum2', [
    (-9, -1.5, 'worse than -1.5%'), (-1.5, -0.8, '-1.5 .. -0.8%'),
    (-0.8, 0.0, '-0.8 .. 0%'), (0.0, 9, 'above 0%')])

print()
print("=" * 122)
print("(5) the SHAPE ideas - previous session gapped down then went flat, and its opposites")
print("=" * 122)
print("  %-34s | %6s%6s%9s%9s%8s | %6s%6s%9s%9s%8s" % (
    'previous session shape', 'trd', 'DAYS', 'pt/trd', 'pt/day', 'days+',
    'trd', 'DAYS', 'pt/trd', 'pt/day', 'days+'))
SHAPES = {
    'gap<-0.5 then FLAT (|oc|<0.3)': (T['gap'] < -0.5) & (T['oc'].abs() < 0.3),
    'gap<-0.5 then kept FALLING':    (T['gap'] < -0.5) & (T['oc'] < -0.3),
    'gap<-0.5 then RECOVERED':       (T['gap'] < -0.5) & (T['oc'] > 0.3),
    'no gap, ground DOWN all day':   (T['gap'].abs() < 0.2) & (T['oc'] < -0.5),
    'no gap, ground UP all day':     (T['gap'].abs() < 0.2) & (T['oc'] > 0.5),
    'gap UP then faded':             (T['gap'] > 0.3) & (T['oc'] < -0.3),
}
for lbl, m in SHAPES.items():
    s = side(T[m & (~T['long'])]); l = side(T[m & (T['long'])])
    print("  %-34s | %6d%6d%+9.2f%+9.1f%8s | %6d%6d%+9.2f%+9.1f%8s" % (
        lbl, s[0], s[1], s[2], s[3], s[4], l[0], l[1], l[2], l[3], l[4]))

pickle.dump({'T': T, 'prev': prev}, open('_tmp_s313_map.pkl', 'wb'))
print()
print("DAYS is the sample size. Anything under ~10 days is a curiosity, not a rule.")
