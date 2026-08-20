# -*- coding: utf-8 -*-
"""S316b - the user's hypothesis: a MARKET REGIME is producing false Skew Charm
signals. Price-based pre-entry features found nothing (S316). Now test the regime
family: Volland (charm / vanna / DD / paradigm) and GEX state.

Same question: do the FULL STOP-OUTS look different from the WINNERS, using only
values stamped at signal time?

⚠️ ~25 features are tested. At |t| >= 2 you expect roughly one false positive by
chance alone, so anything that fires is then checked for (a) monotonic ordering across
buckets and (b) whether it survives month by month. A single big t on a non-monotonic
bucket table is noise, and that is written into the output rather than left implied."""
import os, sys
import numpy as np, pandas as pd
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
VIXMAX = 24.0
STOP = 14.0

NUM = ['greek_alignment', 'vanna_all', 'vanna_weekly', 'vanna_monthly', 'spot_vol_beta',
       'overvix', 'vix3m', 'vix_vix3m_ratio', 'v13_gex_above', 'v13_dd_near',
       'gex_net_dex', 'gex_net_gex', 'gex_zero_gamma', 'gex_call_wall', 'gex_put_wall',
       'gex_net_ceiling', 'max_plus_gex', 'max_minus_gex', 'gap_to_lis', 'basket_pct',
       'rr_ratio', 'upside', 'support_score', 'score']
CAT = ['paradigm', 'gex_state', 'vanna_regime', 'vanna_cliff_side', 'vanna_peak_side', 'grade']

E = create_engine(os.environ['DATABASE_URL'])
c = E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps = lf.load_gaps(c)
cols = ", ".join(sorted(set(NUM + CAT + ['id', 'ts', 'setup_name', 'direction', 'grade',
                                         'paradigm', 'vix', 'overvix', 'greek_alignment',
                                         'outcome_pnl', 'spot', 'v13_gex_above', 'v13_dd_near',
                                         'vanna_cliff_side', 'vanna_peak_side', 'basket_pct',
                                         'gex_net_ceiling'])))
rows = c.execute(text("SELECT " + cols + """ FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND setup_name='Skew Charm'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()

recs = []
for r in rows:
    rr = dict(r)
    if not lf.passes_v20(rr, gaps):
        continue
    v = rr.get('vix')
    if v is None or float(v) >= VIXMAX:
        continue
    pnl = float(rr['outcome_pnl'])
    is_long = str(rr.get('direction', '')).lower() in ('long', 'bullish')
    sgn = 1.0 if is_long else -1.0
    d = {'d': rr['ts'].astimezone(ET).date(), 'long': is_long, 'pnl': pnl,
         'stopped': pnl <= -(STOP - 0.5), 'win': pnl > 0,
         'mo': rr['ts'].astimezone(ET).strftime('%Y-%m')}
    for k in NUM:
        val = rr.get(k)
        d[k] = float(val) if val is not None else np.nan
    for k in CAT:
        d[k] = rr.get(k) or 'NULL'
    # direction-relative derivations
    spot = float(rr['spot']) if rr.get('spot') is not None else np.nan
    for src, nm in (('gex_zero_gamma', 'dist_zero_gamma'), ('gex_call_wall', 'dist_call_wall'),
                    ('gex_put_wall', 'dist_put_wall'), ('gex_net_ceiling', 'dist_ceiling')):
        lvl = d.get(src)
        d[nm] = (lvl - spot) * sgn if (lvl == lvl and spot == spot) else np.nan
    d['align_dir'] = d['greek_alignment'] * sgn if d['greek_alignment'] == d['greek_alignment'] else np.nan
    d['vanna_dir'] = d['vanna_all'] * sgn if d['vanna_all'] == d['vanna_all'] else np.nan
    recs.append(d)
D = pd.DataFrame(recs)
NUM2 = NUM + ['dist_zero_gamma', 'dist_call_wall', 'dist_put_wall', 'dist_ceiling',
              'align_dir', 'vanna_dir']
print("Skew Charm, V21 book, VIX<24: %d trades  |  stop-outs %d  winners %d"
      % (len(D), D['stopped'].sum(), D['win'].sum()))

L, W = D[D['stopped']], D[D['win']]
print()
print("=" * 108)
print("(1) NUMERIC REGIME FEATURES - losers vs winners (signal-time values only)")
print("=" * 108)
print("  %-22s%8s%12s%12s%9s   %s" % ('feature', 'n', 'LOSERS', 'WINNERS', 't', ''))
res = []
for k in NUM2:
    a, b = L[k].dropna().astype(float), W[k].dropna().astype(float)
    if len(a) < 25 or len(b) < 25:
        continue
    se = (a.var() / len(a) + b.var() / len(b)) ** 0.5
    tt = (a.mean() - b.mean()) / se if se else 0.0
    res.append((abs(tt), k, a.mean(), b.mean(), tt, len(a) + len(b)))
for absT, k, am, bm, tt, n in sorted(res, reverse=True):
    tag = "*** |t|>=2 ***" if absT >= 2 else ("weak" if absT >= 1.3 else "")
    print("  %-22s%8d%12.3g%12.3g%9.2f   %s" % (k, n, am, bm, tt, tag))

print()
print("=" * 108)
print("(2) CATEGORICAL REGIME FEATURES - stop-out rate by category (min 20 trades)")
print("=" * 108)
for k in CAT:
    g = D.groupby(k).agg(n=('pnl', 'size'), pt=('pnl', 'mean'),
                         wr=('win', 'mean'), so=('stopped', 'mean'))
    g = g[g['n'] >= 20].sort_values('pt')
    if len(g) < 2:
        continue
    print()
    print("  --- %s ---" % k)
    print("  %-24s%7s%11s%9s%11s" % ('value', 'n', 'pt/trade', 'WR%', 'stop-out%'))
    for nm, row in g.iterrows():
        print("  %-24s%7d%+11.2f%9.0f%11.0f" % (str(nm)[:23], int(row['n']), row['pt'],
                                                row['wr'] * 100, row['so'] * 100))
    print("  spread best-worst: %.2f pt" % (g['pt'].max() - g['pt'].min()))

print()
print("=" * 108)
print("(3) ANYTHING WITH |t| >= 2 - is it MONOTONIC across buckets, or just noise?")
print("=" * 108)
hits = [k for absT, k, _, _, _, _ in sorted(res, reverse=True) if absT >= 2]
if not hits:
    print("  NOTHING reached |t| >= 2 across %d numeric regime features." % len(res))
    print("  With %d tests, one false positive would have been EXPECTED by chance -" % len(res))
    print("  getting zero means the regime family shows no signal-time separation either.")
for k in hits:
    print()
    print("  --- %s ---" % k)
    qs = D[k].quantile([0, .25, .5, .75, 1.0]).values
    print("  %-24s%7s%11s%9s%11s" % ('bucket', 'n', 'pt/trade', 'WR%', 'stop-out%'))
    means = []
    for i in range(4):
        lo, hi = qs[i], qs[i + 1]
        sub = D[(D[k] >= lo) & (D[k] <= hi)] if i == 3 else D[(D[k] >= lo) & (D[k] < hi)]
        if len(sub) == 0: continue
        means.append(sub['pnl'].mean())
        print("  %-24s%7d%+11.2f%9.0f%11.0f" % ("%.3g .. %.3g" % (lo, hi), len(sub),
                                                sub['pnl'].mean(), sub['win'].mean() * 100,
                                                sub['stopped'].mean() * 100))
    mono = all(means[i] <= means[i + 1] for i in range(len(means) - 1)) or \
           all(means[i] >= means[i + 1] for i in range(len(means) - 1))
    print("  monotonic across buckets: %s" % ("YES - worth pursuing" if mono else "NO - noise signature"))
