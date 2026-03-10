"""Analyze: does SVB/charm add value beyond alignment filter?"""
import os, sys
from sqlalchemy import create_engine, text
from collections import defaultdict
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# What extra data do we have per signal?
sys.stdout.write('Setup_log fields (sample Mar 9):\n')
extras = c.execute(text("""
    SELECT setup_name, direction, greek_alignment,
           paradigm, spot_vol_beta, vanna_all, vanna_weekly,
           outcome_result, outcome_pnl
    FROM setup_log
    WHERE ts::date = '2026-03-09'
    ORDER BY ts LIMIT 10
""")).fetchall()
for r in extras:
    sys.stdout.write('  %-20s %-7s align=%s paradigm=%-15s svb=%s vanna=%s result=%s pnl=%s\n' % (
        r[0], r[1], r[2], r[3], r[4], r[5], r[7], r[8]))

# All trades with outcomes
all_trades = c.execute(text("""
    SELECT ts::date, setup_name, direction, greek_alignment,
           spot_vol_beta, paradigm,
           outcome_result, outcome_pnl
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    ORDER BY ts
""")).fetchall()

a2_trades = [t for t in all_trades if (t[3] or 0) >= 2]

sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('ALIGN >= +2: DOES SVB ADD VALUE?\n')
sys.stdout.write('='*70 + '\n')
sys.stdout.write('Total align>=2: %d trades\n' % len(a2_trades))

for label, filter_fn in [
    ('SVB > 0 (overvixing = bullish)', lambda t: t[4] is not None and float(t[4]) > 0),
    ('SVB < 0 (undervixing = bearish)', lambda t: t[4] is not None and float(t[4]) < 0),
    ('SVB unknown/zero', lambda t: t[4] is None or float(t[4] or 0) == 0),
]:
    subset = [t for t in a2_trades if filter_fn(t)]
    if not subset:
        sys.stdout.write('\n  %s: 0 trades\n' % label)
        continue
    w = sum(1 for t in subset if t[6] and 'WIN' in t[6])
    l = sum(1 for t in subset if t[6] and 'LOSS' in t[6])
    pnl = sum(float(t[7] or 0) for t in subset)
    wr = w/(w+l)*100 if (w+l) else 0
    sys.stdout.write('\n  %s:\n    %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (label, len(subset), w, l, wr, pnl))

# SVB direction match at align >= +2
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('ALIGN >= +2 + SVB DIRECTIONAL MATCH\n')
sys.stdout.write('='*70 + '\n')

for label, filter_fn in [
    ('Long + SVB>0 (both bullish)',
     lambda t: t[2] in ('long','bullish') and t[4] is not None and float(t[4]) > 0),
    ('Short + SVB<0 (both bearish)',
     lambda t: t[2] in ('short','bearish') and t[4] is not None and float(t[4]) < 0),
    ('SVB matches direction',
     lambda t: (t[2] in ('long','bullish') and t[4] is not None and float(t[4]) > 0) or
               (t[2] in ('short','bearish') and t[4] is not None and float(t[4]) < 0)),
    ('SVB opposes direction',
     lambda t: (t[2] in ('long','bullish') and t[4] is not None and float(t[4]) < 0) or
               (t[2] in ('short','bearish') and t[4] is not None and float(t[4]) > 0)),
]:
    subset = [t for t in a2_trades if filter_fn(t)]
    if not subset:
        sys.stdout.write('\n  %s: 0 trades\n' % label)
        continue
    w = sum(1 for t in subset if t[6] and 'WIN' in t[6])
    l = sum(1 for t in subset if t[6] and 'LOSS' in t[6])
    pnl = sum(float(t[7] or 0) for t in subset)
    wr = w/(w+l)*100 if (w+l) else 0
    sys.stdout.write('\n  %s:\n    %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (label, len(subset), w, l, wr, pnl))

# Align exactly +2 breakdown
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('ALIGN EXACTLY +2 vs +3 (the gap)\n')
sys.stdout.write('='*70 + '\n')

a2_only = [t for t in a2_trades if t[3] == 2]
a3_only = [t for t in a2_trades if t[3] == 3]

for label, subset in [('Align exactly +2', a2_only), ('Align exactly +3', a3_only)]:
    w = sum(1 for t in subset if t[6] and 'WIN' in t[6])
    l = sum(1 for t in subset if t[6] and 'LOSS' in t[6])
    pnl = sum(float(t[7] or 0) for t in subset)
    wr = w/(w+l)*100 if (w+l) else 0
    sys.stdout.write('\n  %s: %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (label, len(subset), w, l, wr, pnl))

# At align +2, can SVB filter out the losers?
sys.stdout.write('\n  Align +2 with SVB matching direction:\n')
a2_svb_match = [t for t in a2_only if
    (t[2] in ('long','bullish') and t[4] is not None and float(t[4]) > 0) or
    (t[2] in ('short','bearish') and t[4] is not None and float(t[4]) < 0)]
if a2_svb_match:
    w = sum(1 for t in a2_svb_match if t[6] and 'WIN' in t[6])
    l = sum(1 for t in a2_svb_match if t[6] and 'LOSS' in t[6])
    pnl = sum(float(t[7] or 0) for t in a2_svb_match)
    wr = w/(w+l)*100 if (w+l) else 0
    sys.stdout.write('    %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(a2_svb_match), w, l, wr, pnl))
else:
    sys.stdout.write('    0 trades\n')

sys.stdout.write('\n  Align +2 with SVB opposing direction:\n')
a2_svb_opp = [t for t in a2_only if
    (t[2] in ('long','bullish') and t[4] is not None and float(t[4]) < 0) or
    (t[2] in ('short','bearish') and t[4] is not None and float(t[4]) > 0)]
if a2_svb_opp:
    w = sum(1 for t in a2_svb_opp if t[6] and 'WIN' in t[6])
    l = sum(1 for t in a2_svb_opp if t[6] and 'LOSS' in t[6])
    pnl = sum(float(t[7] or 0) for t in a2_svb_opp)
    wr = w/(w+l)*100 if (w+l) else 0
    sys.stdout.write('    %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(a2_svb_opp), w, l, wr, pnl))
else:
    sys.stdout.write('    0 trades\n')

sys.stdout.write('\n  Align +2 with SVB unknown:\n')
a2_svb_unk = [t for t in a2_only if t[4] is None]
if a2_svb_unk:
    w = sum(1 for t in a2_svb_unk if t[6] and 'WIN' in t[6])
    l = sum(1 for t in a2_svb_unk if t[6] and 'LOSS' in t[6])
    pnl = sum(float(t[7] or 0) for t in a2_svb_unk)
    wr = w/(w+l)*100 if (w+l) else 0
    sys.stdout.write('    %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(a2_svb_unk), w, l, wr, pnl))
else:
    sys.stdout.write('    0 trades\n')

# Combined: align +3 OR (align +2 + SVB match)
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('COMBINED: align +3 OR (align +2 + SVB matches direction)\n')
sys.stdout.write('='*70 + '\n')

def combo_pass(t):
    a = t[3] or 0
    if a >= 3: return True
    if a == 2:
        svb = t[4]
        if svb is not None:
            svb_f = float(svb)
            if t[2] in ('long','bullish') and svb_f > 0: return True
            if t[2] in ('short','bearish') and svb_f < 0: return True
    return False

combo = [t for t in all_trades if combo_pass(t)]
w = sum(1 for t in combo if t[6] and 'WIN' in t[6])
l = sum(1 for t in combo if t[6] and 'LOSS' in t[6])
pnl = sum(float(t[7] or 0) for t in combo)
wr = w/(w+l)*100 if (w+l) else 0
dates = set(t[0] for t in combo)

# DD for combo
equity = 0
peak = 0
max_dd = 0
worst_day_pnl = 0
day_pnl = defaultdict(float)
for t in combo:
    p = float(t[7] or 0)
    equity += p
    day_pnl[str(t[0])] += p
    if equity > peak: peak = equity
    dd = peak - equity
    if dd > max_dd: max_dd = dd
for d, p in day_pnl.items():
    if p < worst_day_pnl: worst_day_pnl = p

sys.stdout.write('  %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(combo), w, l, wr, pnl))
sys.stdout.write('  Active on %d of 22 days\n' % len(dates))
sys.stdout.write('  MaxDD: %.1f pts, Worst day: %+.1f pts\n' % (max_dd, worst_day_pnl))
sys.stdout.write('  Avg: %.1f pts/day (all 22 days)\n' % (pnl/22))

# FINAL COMPARISON TABLE
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('FINAL COMPARISON\n')
sys.stdout.write('='*70 + '\n')

total_pnl = sum(float(t[7] or 0) for t in all_trades)
total_w = sum(1 for t in all_trades if t[6] and 'WIN' in t[6])
total_l = sum(1 for t in all_trades if t[6] and 'LOSS' in t[6])
total_wr = total_w/(total_w+total_l)*100

a2_all = [t for t in all_trades if (t[3] or 0) >= 2]
a2_w = sum(1 for t in a2_all if t[6] and 'WIN' in t[6])
a2_l = sum(1 for t in a2_all if t[6] and 'LOSS' in t[6])
a2_pnl = sum(float(t[7] or 0) for t in a2_all)
a2_wr = a2_w/(a2_w+a2_l)*100

a3_all = [t for t in all_trades if (t[3] or 0) >= 3]
a3_w = sum(1 for t in a3_all if t[6] and 'WIN' in t[6])
a3_l = sum(1 for t in a3_all if t[6] and 'LOSS' in t[6])
a3_pnl = sum(float(t[7] or 0) for t in a3_all)
a3_wr = a3_w/(a3_w+a3_l)*100

sys.stdout.write('\n  %-45s %3d trades  %3.0f%% WR  PnL=%+7.1f  %+5.1f/day\n' %
    ('No filter', len(all_trades), total_wr, total_pnl, total_pnl/22))
sys.stdout.write('  %-45s %3d trades  %3.0f%% WR  PnL=%+7.1f  %+5.1f/day\n' %
    ('Align >= +2', len(a2_all), a2_wr, a2_pnl, a2_pnl/22))
sys.stdout.write('  %-45s %3d trades  %3.0f%% WR  PnL=%+7.1f  %+5.1f/day\n' %
    ('Align >= +3', len(a3_all), a3_wr, a3_pnl, a3_pnl/22))
sys.stdout.write('  %-45s %3d trades  %3.0f%% WR  PnL=%+7.1f  %+5.1f/day\n' %
    ('Align +3 OR (align +2 + SVB match)', len(combo), wr, pnl, pnl/22))

sys.stdout.flush()
c.close()
