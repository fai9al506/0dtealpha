# -*- coding: utf-8 -*-
"""S312d - AUDIT the S312 result against the user's challenge.
 (A) prove the day indexing: for a signal on day n, print n, n-1, n-2 and every
     measure, for the last few sessions - so 2026-08-19 must show prev cc = -0.69%.
 (B) list EVERY day in the 'gapped down then flat' bucket with its own short record,
     so the +5.75 pt / 77% number can be checked day by day.
 (C) test the user's ACTUAL hypothesis on PRICE, not on our trades: after a
     gap-down-then-flat session, is the NEXT day bullish?
     Our shorts are intraday fades - a bullish day can still hold winning fades.
     These are two different questions and must be answered separately."""
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
day['next_cc'] = day['cc'].shift(-1)          # what the NEXT session did
day['next_oc'] = day['oc'].shift(-1)
D = day.set_index('d')

print("=" * 118)
print("(A) DAY INDEXING AUDIT - for a signal on day n we read day n-1. Last 8 sessions.")
print("    2026-08-19 must show prev close-close = -0.69% (the user's number).")
print("=" * 118)
print("  %-12s%10s%10s%12s   |  %-12s%10s%10s%10s" % (
    'day n', 'open', 'close', 'n cc%', 'day n-1', 'n-1 cc%', 'n-1 oc%', 'n-1 gap%'))
ds = list(day['d'])
for i in range(len(ds) - 8, len(ds)):
    n = day.iloc[i]; p = day.iloc[i - 1]
    print("  %-12s%10.2f%10.2f%+12.2f   |  %-12s%+10.2f%+10.2f%+10.2f" % (
        n['d'], n['open'], n['close'], n['cc'], p['d'], p['cc'], p['oc'], p['gap']))

# ---- shorts by day ----
sh = []
for r in rows:
    if str(r.get('direction', '')).lower() in ('long', 'bullish'): continue
    if not lf.passes_v20(dict(r), gaps): continue
    v = r.get('vix')
    if v is None or float(v) >= VIXMAX: continue
    d = r['ts'].astimezone(ET).date()
    sh.append({'d': d, 'pnl': float(r['outcome_pnl'])})
sh = pd.DataFrame(sh)
byday = sh.groupby('d').agg(n=('pnl', 'size'), pt=('pnl', 'sum'), wr=('pnl', lambda s: (s > 0).mean() * 100))

print()
print("=" * 118)
print("(B) EVERY 'PREVIOUS SESSION GAPPED DOWN >0.5%% THEN WENT FLAT' DAY, one line each")
print("    (row = the trading day n on which we would have blocked shorts)")
print("=" * 118)
print("  %-12s%10s%10s   |  %6s%10s%9s   |  %10s" % (
    'day n', 'n-1 gap%', 'n-1 oc%', 'shorts', 'pt total', 'WR%', 'n cc%'))
hits = []
for i in range(1, len(ds)):
    p = day.iloc[i - 1]; n = day.iloc[i]
    if pd.isna(p['gap']) or pd.isna(p['oc']): continue
    if p['gap'] < -0.5 and abs(p['oc']) < 0.3:
        b = byday.loc[n['d']] if n['d'] in byday.index else None
        hits.append((n['d'], p['gap'], p['oc'], b, n['cc']))
        print("  %-12s%+10.2f%+10.2f   |  %6s%+10.1f%9s   |  %+10.2f" % (
            n['d'], p['gap'], p['oc'],
            int(b['n']) if b is not None else 0,
            b['pt'] if b is not None else 0.0,
            ("%.0f" % b['wr']) if b is not None else '-',
            n['cc']))
tot_n = sum(int(h[3]['n']) for h in hits if h[3] is not None)
tot_pt = sum(h[3]['pt'] for h in hits if h[3] is not None)
print("  %-12s%10s%10s   |  %6d%+10.1f" % ('TOTAL', '', '', tot_n, tot_pt))
print("  -> %.2f pt per short across those days" % (tot_pt / tot_n if tot_n else 0))

print()
print("=" * 118)
print("(C) THE USER'S ACTUAL HYPOTHESIS, tested on PRICE not on our trades:")
print("    after a session that GAPPED DOWN then went FLAT, is the NEXT day bullish?")
print("=" * 118)
m = (day['gap'] < -0.5) & (day['oc'].abs() < 0.3) & day['next_cc'].notna()
o = (~((day['gap'] < -0.5) & (day['oc'].abs() < 0.3))) & day['next_cc'].notna()
for lbl, sel in [('gap down >0.5% then FLAT', m), ('every other session', o)]:
    s = day[sel]
    print("  %-28s n=%3d   next-day close-close %+6.2f%%   up %3.0f%% of the time   next-day open-close %+6.2f%%"
          % (lbl, len(s), s['next_cc'].mean(), (s['next_cc'] > 0).mean() * 100, s['next_oc'].mean()))

print()
print("  same question by how deep the previous close-close was:")
for lo, hi in [(-9, -1.0), (-1.0, -0.8), (-0.8, -0.6), (-0.6, -0.3), (-0.3, 0), (0, 9)]:
    s = day[(day['cc'] >= lo) & (day['cc'] < hi) & day['next_cc'].notna()]
    if len(s) == 0: continue
    print("    prev cc %+5.2f..%+5.2f%%  n=%3d   next day %+6.2f%%   up %3.0f%%"
          % (lo, hi, len(s), s['next_cc'].mean(), (s['next_cc'] > 0).mean() * 100))
