# -*- coding: utf-8 -*-
"""S315 - "we were right but early". Is 2026-08-19 normal or unusual?

For every FULL stop-out in the V21 book, ask three things using the 1-minute SPX path:
  1. would the setup's direction have PAID by the 15:55 close? (were we early?)
  2. how far was the stop from the day's extreme in that direction?
     (2026-08-19 stopped 2 pt from the high - is that rare?)
  3. does either answer depend on the ENTRY HOUR?

NOTE: entry refits are a CLOSED line (research_v18_v19_entry_exit_closed - per-setup
entry refits failed out of sample). This is a diagnostic to see whether the user's
unease points at something the earlier work did not cover, NOT a refit."""
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
bars = pd.read_sql(text("""select (ts at time zone 'America/New_York') et,
    (ts at time zone 'America/New_York')::date d, bar_high, bar_low, bar_close
    from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-03-01' order by ts"""), c)
rows = c.execute(text("SELECT " + lf.COLS + """, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01'
    AND outcome_pnl IS NOT NULL AND spot IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
bars['d'] = pd.to_datetime(bars['d']).dt.date
bars['et'] = pd.to_datetime(bars['et'])
BY = {d: g for d, g in bars.groupby('d')}

STOP = 14.0     # the SC/AG stop in points
out = []
for r in rows:
    rr = dict(r)
    if not lf.passes_v20(rr, gaps):
        continue
    v = rr.get('vix')
    if v is None or float(v) >= VIXMAX:
        continue
    t = rr['ts'].astimezone(ET).replace(tzinfo=None)
    d = t.date()
    g = BY.get(d)
    if g is None:
        continue
    pnl = float(rr['outcome_pnl'])
    # full stop-out only
    if pnl > -(STOP - 0.5):
        continue
    is_long = str(rr.get('direction', '')).lower() in ('long', 'bullish')
    entry = float(rr['spot'])
    after = g[g['et'] >= pd.Timestamp(t)]
    if len(after) < 5:
        continue
    close = float(after.iloc[-1]['bar_close'])
    hi = float(after['bar_high'].max())
    lo = float(after['bar_low'].min())
    # the stop level in SPX terms, and how close it was to the extreme after entry
    if is_long:
        stop_lvl = entry - STOP
        vindic = close < entry - 0  # a long is vindicated if price ends ABOVE entry
        vindic = close > entry
        dist_extreme = stop_lvl - lo          # how far the stop was above the low
        pay = close - entry
    else:
        stop_lvl = entry + STOP
        vindic = close < entry
        dist_extreme = hi - stop_lvl          # how far the DAY HIGH was above our stop
        pay = entry - close
    out.append({'d': d, 'et': t, 'hour': t.hour, 'setup': rr['setup_name'],
                'long': is_long, 'entry': entry, 'close': close,
                'vindicated': bool(vindic), 'pay_if_held': pay,
                'stop_to_extreme': dist_extreme})
D = pd.DataFrame(out)

print("=" * 104)
print("(1) FULL STOP-OUTS IN THE V21 BOOK - would the direction have PAID by the close?")
print("=" * 104)
print("  total full stop-outs analysed: %d  over %d days" % (len(D), D['d'].nunique()))
for lbl, sub in [('ALL', D), ('SHORTS', D[~D['long']]), ('LONGS', D[D['long']])]:
    if len(sub) == 0: continue
    print("  %-8s n=%3d   direction paid by close: %3.0f%%   avg if held to close %+6.2f pt"
          % (lbl, len(sub), sub['vindicated'].mean() * 100, sub['pay_if_held'].mean()))
print()
print("  a coin flip would be 50%. Well above 50% = we are systematically EARLY.")
print("  Well below = the stop-outs are simply wrong, and holding would be worse.")

print()
print("=" * 104)
print("(2) HOW CLOSE WAS THE STOP TO THE DAY'S EXTREME?   (2026-08-19 was ~2 pt)")
print("=" * 104)
s = D[~D['long']]['stop_to_extreme'].dropna()
print("  SHORT stop-outs: distance from our stop to the highest price after entry")
for q in (10, 25, 50, 75, 90):
    print("     %2dth percentile  %6.1f pt" % (q, np.percentile(s, q)))
print("     within 3 pt of the extreme: %.0f%% of short stop-outs" % ((s <= 3).mean() * 100))
print("     within 5 pt of the extreme: %.0f%% of short stop-outs" % ((s <= 5).mean() * 100))

print()
print("=" * 104)
print("(3) BY ENTRY HOUR - is the morning worse?")
print("=" * 104)
print("  %-8s%8s%14s%16s%16s" % ('hour', 'stops', 'paid by close', 'avg if held', 'stop-to-extreme'))
for h in sorted(D['hour'].unique()):
    sub = D[D['hour'] == h]
    ss = sub[~sub['long']]['stop_to_extreme']
    print("  %-8s%8d%13.0f%%%+16.2f%16s" % (
        "%02d:00" % h, len(sub), sub['vindicated'].mean() * 100, sub['pay_if_held'].mean(),
        ("%.1f pt" % ss.median()) if len(ss) else "-"))

print()
print("=" * 104)
print("(4) WHERE DOES 2026-08-19 SIT?")
print("=" * 104)
a19 = D[D['d'] == pd.to_datetime('2026-08-19').date()]
if len(a19):
    for _, r in a19.iterrows():
        print("  %s %-12s %-5s entry %.2f  close %.2f  paid-if-held %+6.2f  stop was %.1f pt from the high"
              % (r['et'].strftime('%H:%M'), r['setup'], 'LONG' if r['long'] else 'SHORT',
                 r['entry'], r['close'], r['pay_if_held'], r['stop_to_extreme']))
    print("  day: %d of %d stop-outs would have paid by the close" % (a19['vindicated'].sum(), len(a19)))
else:
    print("  (no qualifying stop-outs found for that date)")
