# -*- coding: utf-8 -*-
"""S316 - the user's observation: "when Skew Charm goes against us it goes against us
very fast and hard". Two questions, in order:

  A. IS IT TRUE? Do SC losers move against us faster than SC winners move for us?
  B. IF SO, is there anything visible BEFORE ENTRY that separates them?

Every candidate feature must be computable AT SIGNAL TIME from the 1-minute SPX path,
otherwise it is lookahead and useless as a filter.

Candidates (all pre-entry):
  ret5 / ret15 / ret30   signed so POSITIVE = the tape is already running AGAINST the
                         trade direction (for a short, price rising into our entry)
  vol30                  realised volatility, std of 1-min returns over 30 min
  rng30                  high-low range of the last 30 min
  pos_in_day             where spot sits inside the day's range so far (0=low, 1=high)
  ext_from_open          distance from the session open, signed against the trade
"""
import os, sys
import numpy as np, pandas as pd
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
VIXMAX = 24.0
STOP = 14.0

E = create_engine(os.environ['DATABASE_URL'])
c = E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps = lf.load_gaps(c)
bars = pd.read_sql(text("""select (ts at time zone 'America/New_York') et,
    (ts at time zone 'America/New_York')::date d, bar_open, bar_high, bar_low, bar_close
    from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-03-01' order by ts"""), c)
rows = c.execute(text("SELECT " + lf.COLS + """, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND setup_name='Skew Charm'
    AND outcome_pnl IS NOT NULL AND spot IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
bars['d'] = pd.to_datetime(bars['d']).dt.date
bars['et'] = pd.to_datetime(bars['et'])
BY = {d: g.reset_index(drop=True) for d, g in bars.groupby('d')}

recs = []
for r in rows:
    rr = dict(r)
    if not lf.passes_v20(rr, gaps):
        continue
    v = rr.get('vix')
    if v is None or float(v) >= VIXMAX:
        continue
    t = rr['ts'].astimezone(ET).replace(tzinfo=None)
    g = BY.get(t.date())
    if g is None:
        continue
    pre = g[g['et'] <= pd.Timestamp(t)]
    if len(pre) < 31:
        continue
    is_long = str(rr.get('direction', '')).lower() in ('long', 'bullish')
    sgn = 1.0 if is_long else -1.0      # +1 long, -1 short
    now = float(pre.iloc[-1]['bar_close'])

    def back(n):
        return float(pre.iloc[-min(n, len(pre))]['bar_close'])

    # signed so POSITIVE = tape running AGAINST us before entry
    ret5 = (now - back(6)) * -sgn
    ret15 = (now - back(16)) * -sgn
    ret30 = (now - back(31)) * -sgn
    rr30 = pre.tail(30)
    vol30 = float(pd.Series(rr30['bar_close'].astype(float)).diff().std())
    rng30 = float(rr30['bar_high'].max() - rr30['bar_low'].min())
    dhi, dlo = float(pre['bar_high'].max()), float(pre['bar_low'].min())
    pos = (now - dlo) / (dhi - dlo) if dhi > dlo else 0.5
    pos_adv = pos if not is_long else (1 - pos)   # high = at the extreme we are fading
    ext = (now - float(pre.iloc[0]['bar_open'])) * -sgn
    pnl = float(rr['outcome_pnl'])
    recs.append({'d': t.date(), 'et': t, 'long': is_long, 'pnl': pnl,
                 'stopped': pnl <= -(STOP - 0.5), 'win': pnl > 0,
                 'mins': float(rr.get('outcome_elapsed_min') or 0),
                 'vix': float(v), 'grade': rr.get('grade'), 'para': rr.get('paradigm'),
                 'ret5': ret5, 'ret15': ret15, 'ret30': ret30,
                 'vol30': vol30, 'rng30': rng30, 'pos_adv': pos_adv, 'ext': ext})
D = pd.DataFrame(recs)
print("Skew Charm trades in the V21 book (VIX<24): %d   over %d days" % (len(D), D['d'].nunique()))
print("  full stop-outs %d   winners %d" % (D['stopped'].sum(), D['win'].sum()))

print()
print("=" * 104)
print("(A) IS IT TRUE?  how fast does a loser go against us vs how fast a winner works")
print("=" * 104)
L = D[D['stopped']]; W = D[D['win']]
print("  %-22s%8s%14s%16s" % ('', 'n', 'median min', 'pts per minute'))
print("  %-22s%8d%14.0f%16.3f" % ('LOSERS (full stop)', len(L), L['mins'].median(),
                                  STOP / max(L['mins'].median(), 1)))
print("  %-22s%8d%14.0f%16.3f" % ('WINNERS', len(W), W['mins'].median(),
                                  W['pnl'].median() / max(W['mins'].median(), 1)))
print()
print("  losers reach a FULL 14-pt stop in a median %.0f min;" % L['mins'].median())
print("  winners take a median %.0f min to make %.1f pt." % (W['mins'].median(), W['pnl'].median()))

print()
print("=" * 104)
print("(B) IS THERE ANYTHING VISIBLE BEFORE ENTRY?  losers vs winners, pre-entry only")
print("=" * 104)
print("  %-14s%12s%12s%10s%9s   %s" % ('feature', 'LOSERS', 'WINNERS', 'diff', 't', 'reading'))
feats = [('ret5', 'move last 5m'), ('ret15', 'move last 15m'), ('ret30', 'move last 30m'),
         ('vol30', 'volatility 30m'), ('rng30', 'range 30m'),
         ('pos_adv', 'at the extreme'), ('ext', 'far from open'), ('vix', 'VIX')]
sig = []
for k, lbl in feats:
    a, b = L[k].astype(float), W[k].astype(float)
    se = (a.var() / len(a) + b.var() / len(b)) ** 0.5
    tt = (a.mean() - b.mean()) / se if se else 0
    tag = "*** SEPARATES ***" if abs(tt) >= 2 else ("weak" if abs(tt) >= 1.3 else "")
    if abs(tt) >= 2: sig.append(k)
    print("  %-14s%12.2f%12.2f%+10.2f%9.2f   %s" % (lbl, a.mean(), b.mean(), a.mean() - b.mean(), tt, tag))
print()
print("  (all features are signed so a BIGGER number = tape running harder AGAINST the trade)")
print("  |t| >= 2 means the difference is unlikely to be chance.")

print()
print("=" * 104)
print("(C) THE MOST PROMISING FEATURE, BUCKETED - does it order the outcome?")
print("=" * 104)
for k, lbl in feats:
    a, b = L[k].astype(float), W[k].astype(float)
    se = (a.var() / len(a) + b.var() / len(b)) ** 0.5
    if abs((a.mean() - b.mean()) / se if se else 0) < 1.3:
        continue
    print()
    print("  --- %s ---" % lbl)
    qs = D[k].quantile([0, .25, .5, .75, 1.0]).values
    print("  %-22s%7s%11s%9s%11s" % ('bucket', 'n', 'pt/trade', 'WR%', 'stop-out%'))
    for i in range(4):
        lo, hi = qs[i], qs[i + 1]
        sub = D[(D[k] >= lo) & (D[k] <= hi)] if i == 3 else D[(D[k] >= lo) & (D[k] < hi)]
        if len(sub) == 0: continue
        print("  %-22s%7d%+11.2f%9.0f%11.0f" % (
            "%+.1f .. %+.1f" % (lo, hi), len(sub), sub['pnl'].mean(),
            sub['win'].mean() * 100, sub['stopped'].mean() * 100))
if not sig:
    print()
    print("  NOTHING reached |t| >= 2. On this sample there is no pre-entry feature that")
    print("  separates the fast losers from the winners.")
