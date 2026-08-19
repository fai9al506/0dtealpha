# -*- coding: utf-8 -*-
"""S312e - the correction. My +5.75 pt / 77% WR figure counted 31 TRADES as 31
observations. They are not independent: they cluster into FIVE DAYS, and inside a
day every short faces the same tape. Effective sample = days, not trades.

Redo every bucket at DAY level, and answer the user's exact rule directly:
what did our shorts do on days where the previous close-to-close was in the
-0.6% .. -1.0% zone (today's -0.69% sits there)?"""
import os, sys
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
prev = {}
ds = list(day['d'])
for i in range(1, len(ds)):
    p = day.iloc[i - 1]
    prev[ds[i]] = {'oc': p['oc'], 'cc': p['cc'], 'gap': p['gap']}

sh = []
for r in rows:
    if str(r.get('direction', '')).lower() in ('long', 'bullish'): continue
    if not lf.passes_v20(dict(r), gaps): continue
    v = r.get('vix')
    if v is None or float(v) >= VIXMAX: continue
    d = r['ts'].astimezone(ET).date()
    p = prev.get(d)
    if p is None or pd.isna(p['cc']): continue
    sh.append({'d': d, 'pnl': float(r['outcome_pnl']), 'cc': p['cc'], 'oc': p['oc'], 'gap': p['gap']})
sh = pd.DataFrame(sh)

print("=" * 112)
print("TRADE-LEVEL vs DAY-LEVEL - the same buckets, counted both ways")
print("  a day's shorts all face the same tape, so DAYS is the honest sample size")
print("=" * 112)
print("  %-26s%8s%8s%11s%11s%9s" % ('bucket (previous session)', 'trades', 'DAYS', 'pt/trade', 'pt/day', 'days +'))


def show(label, mask):
    s = sh[mask]
    if len(s) == 0:
        print("  %-26s%8d%8d" % (label, 0, 0)); return
    per = s.groupby('d')['pnl'].sum()
    print("  %-26s%8d%8d%+11.2f%+11.1f%7d/%d" % (
        label, len(s), len(per), s['pnl'].mean(), per.mean(),
        int((per > 0).sum()), len(per)))


show('cc worse than -1.0%', sh['cc'] < -1.0)
show('cc -1.0 .. -0.8%', (sh['cc'] >= -1.0) & (sh['cc'] < -0.8))
show('cc -0.8 .. -0.6%  <-TODAY', (sh['cc'] >= -0.8) & (sh['cc'] < -0.6))
show('cc -0.6 .. -0.3%', (sh['cc'] >= -0.6) & (sh['cc'] < -0.3))
show('cc -0.3 .. 0%', (sh['cc'] >= -0.3) & (sh['cc'] < 0))
show('cc above 0%', sh['cc'] >= 0)
print()
show('oc worse than -0.8% (V21)', sh['oc'] < -0.8)
show('oc above -0.8%', sh['oc'] >= -0.8)
print()
show('gap<-0.5 & flat (idea)', (sh['gap'] < -0.5) & (sh['oc'].abs() < 0.3))
show('all other shorts', ~((sh['gap'] < -0.5) & (sh['oc'].abs() < 0.3)))

print()
print("=" * 112)
print("THE USER'S EXACT RULE - block shorts when previous CLOSE-TO-CLOSE < threshold")
print("  day-level, so one bad day cannot hide behind many trades")
print("=" * 112)
print("  %-16s%8s%8s%11s%11s%9s%12s" % ('threshold', 'trades', 'DAYS', 'pt/trade', 'pt/day', 'days +', 'total pt'))
for t in (-0.5, -0.6, -0.65, -0.7, -0.8, -1.0):
    s = sh[sh['cc'] < t]
    if len(s) == 0: continue
    per = s.groupby('d')['pnl'].sum()
    print("  cc < %-11.2f%8d%8d%+11.2f%+11.1f%7d/%d%+12.1f" % (
        t, len(s), len(per), s['pnl'].mean(), per.mean(),
        int((per > 0).sum()), len(per), s['pnl'].sum()))
print()
print("  for comparison, V21's own trigger:")
s = sh[sh['oc'] < -0.8]
per = s.groupby('d')['pnl'].sum()
print("  oc < %-11.2f%8d%8d%+11.2f%+11.1f%7d/%d%+12.1f" % (
    -0.8, len(s), len(per), s['pnl'].mean(), per.mean(),
    int((per > 0).sum()), len(per), s['pnl'].sum()))
print()
print("  DAYS in each: that is the real sample size. Under ~10 days nothing is decidable.")
