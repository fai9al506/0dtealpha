import os, sys
from sqlalchemy import create_engine, text
from collections import defaultdict
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()
all_trades = c.execute(text('SELECT ts::date as d, setup_name, direction, paradigm, outcome_result, outcome_pnl, greek_alignment FROM setup_log WHERE outcome_result IS NOT NULL ORDER BY ts')).fetchall()
total_pnl = sum(float(t[5] or 0) for t in all_trades)
total_n = len(all_trades)
total_w = sum(1 for t in all_trades if t[4] and 'WIN' in t[4])
total_l = sum(1 for t in all_trades if t[4] and 'LOSS' in t[4])
total_wr = total_w/(total_w+total_l)*100 if (total_w+total_l) else 0
sys.stdout.write('Baseline: %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (total_n, total_w, total_l, total_wr, total_pnl))

tests = [
  ('Block shorts at align >= 0', lambda t: t[2] in ('short','bearish') and (t[6] or 0) >= 0),
  ('Block longs at align <= 0', lambda t: t[2] in ('long','bullish') and (t[6] or 0) <= 0),
  ('BOTH alignment gate', lambda t: (t[2] in ('short','bearish') and (t[6] or 0) >= 0) or (t[2] in ('long','bullish') and (t[6] or 0) <= 0)),
  ('Strict: shorts need align<=-2, longs need align>=+2', lambda t: (t[2] in ('short','bearish') and (t[6] or 0) > -2) or (t[2] in ('long','bullish') and (t[6] or 0) < 2)),
]
for name, block_fn in tests:
    blocked = [t for t in all_trades if block_fn(t)]
    passed = [t for t in all_trades if not block_fn(t)]
    b_pnl = sum(float(t[5] or 0) for t in blocked)
    p_pnl = sum(float(t[5] or 0) for t in passed)
    p_w = sum(1 for t in passed if t[4] and 'WIN' in t[4])
    p_l = sum(1 for t in passed if t[4] and 'LOSS' in t[4])
    p_wr = p_w/(p_w+p_l)*100 if (p_w+p_l) else 0
    b_w = sum(1 for t in blocked if t[4] and 'WIN' in t[4])
    b_l = sum(1 for t in blocked if t[4] and 'LOSS' in t[4])
    sys.stdout.write('\n  %s:\n' % name)
    sys.stdout.write('    Passed:  %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(passed), p_w, p_l, p_wr, p_pnl))
    sys.stdout.write('    Blocked: %d trades, %dW/%dL, PnL=%+.1f\n' % (len(blocked), b_w, b_l, b_pnl))
    sys.stdout.write('    Improvement: %+.1f pts\n' % (p_pnl - total_pnl))

sys.stdout.write('\n\nWIN RATE BY ALIGNMENT AND DIRECTION\n')
sys.stdout.write('='*70 + '\n')
stats = defaultdict(lambda: {'w':0,'l':0,'pnl':0})
for t in all_trades:
    a = t[6] if t[6] is not None else 0
    d = t[2]
    key = (d, a)
    s = stats[key]
    s['pnl'] += float(t[5] or 0)
    if t[4] and 'WIN' in t[4]: s['w'] += 1
    if t[4] and 'LOSS' in t[4]: s['l'] += 1
for key in sorted(stats.keys(), key=lambda x: (x[0], x[1])):
    s = stats[key]
    n = s['w'] + s['l']
    wr = s['w']/n*100 if n else 0
    sys.stdout.write('  %-7s align=%+d: %dW/%dL (%.0f%% WR) n=%d, PnL=%+.1f\n' % (key[0], key[1], s['w'], s['l'], wr, n, s['pnl']))
sys.stdout.flush()
c.close()
