import os, sys
from sqlalchemy import create_engine, text
from collections import defaultdict
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()
all_trades = c.execute(text('SELECT ts::date as d, setup_name, direction, paradigm, outcome_result, outcome_pnl, greek_alignment FROM setup_log WHERE outcome_result IS NOT NULL ORDER BY ts')).fetchall()

total_pnl = sum(float(t[5] or 0) for t in all_trades)
total_w = sum(1 for t in all_trades if t[4] and 'WIN' in t[4])
total_l = sum(1 for t in all_trades if t[4] and 'LOSS' in t[4])
total_wr = total_w/(total_w+total_l)*100 if (total_w+total_l) else 0
sys.stdout.write('BASELINE: %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n\n' % (len(all_trades), total_w, total_l, total_wr, total_pnl))

# Filter: abs(alignment) >= 2
filtered = [t for t in all_trades if abs(t[6] or 0) >= 2]
f_pnl = sum(float(t[5] or 0) for t in filtered)
f_w = sum(1 for t in filtered if t[4] and 'WIN' in t[4])
f_l = sum(1 for t in filtered if t[4] and 'LOSS' in t[4])
f_wr = f_w/(f_w+f_l)*100 if (f_w+f_l) else 0
sys.stdout.write('ALIGNMENT >= +2 OR <= -2 (directional strength >= 2):\n')
sys.stdout.write('  %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(filtered), f_w, f_l, f_wr, f_pnl))
sys.stdout.write('  Blocked: %d trades, PnL=%+.1f\n\n' % (len(all_trades)-len(filtered), total_pnl - f_pnl))

# Filter: alignment >= +2 for longs, alignment <= -2 for shorts (with-trend only)
filtered2 = [t for t in all_trades if 
    (t[2] in ('long','bullish') and (t[6] or 0) >= 2) or
    (t[2] in ('short','bearish') and (t[6] or 0) <= -2)]
f2_pnl = sum(float(t[5] or 0) for t in filtered2)
f2_w = sum(1 for t in filtered2 if t[4] and 'WIN' in t[4])
f2_l = sum(1 for t in filtered2 if t[4] and 'LOSS' in t[4])
f2_wr = f2_w/(f2_w+f2_l)*100 if (f2_w+f2_l) else 0
sys.stdout.write('WITH-TREND ONLY (longs align>=+2, shorts align<=-2):\n')
sys.stdout.write('  %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(filtered2), f2_w, f2_l, f2_wr, f2_pnl))
sys.stdout.write('  Blocked: %d trades, PnL=%+.1f\n\n' % (len(all_trades)-len(filtered2), total_pnl - f2_pnl))

# Filter: alignment >= +2 regardless of direction
filtered3 = [t for t in all_trades if (t[6] or 0) >= 2]
f3_pnl = sum(float(t[5] or 0) for t in filtered3)
f3_w = sum(1 for t in filtered3 if t[4] and 'WIN' in t[4])
f3_l = sum(1 for t in filtered3 if t[4] and 'LOSS' in t[4])
f3_wr = f3_w/(f3_w+f3_l)*100 if (f3_w+f3_l) else 0
sys.stdout.write('ALIGNMENT >= +2 (any direction):\n')
sys.stdout.write('  %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(filtered3), f3_w, f3_l, f3_wr, f3_pnl))
sys.stdout.write('  Blocked: %d trades, PnL=%+.1f\n\n' % (len(all_trades)-len(filtered3), total_pnl - f3_pnl))

# Per-setup breakdown for align >= +2 (any direction)
sys.stdout.write('--- Per-setup breakdown (align >= +2, any direction) ---\n')
setup_stats = defaultdict(lambda: {'w':0,'l':0,'pnl':0,'n':0})
for t in filtered3:
    s = setup_stats[t[1]]
    s['n'] += 1
    s['pnl'] += float(t[5] or 0)
    if t[4] and 'WIN' in t[4]: s['w'] += 1
    if t[4] and 'LOSS' in t[4]: s['l'] += 1
for setup, s in sorted(setup_stats.items(), key=lambda x: -x[1]['pnl']):
    wr = s['w']/(s['w']+s['l'])*100 if (s['w']+s['l']) else 0
    sys.stdout.write('  %-20s: %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (setup, s['n'], s['w'], s['l'], wr, s['pnl']))

# Also show align >= +3
sys.stdout.write('\n')
filtered4 = [t for t in all_trades if (t[6] or 0) >= 3]
f4_pnl = sum(float(t[5] or 0) for t in filtered4)
f4_w = sum(1 for t in filtered4 if t[4] and 'WIN' in t[4])
f4_l = sum(1 for t in filtered4 if t[4] and 'LOSS' in t[4])
f4_wr = f4_w/(f4_w+f4_l)*100 if (f4_w+f4_l) else 0
sys.stdout.write('ALIGNMENT >= +3 (any direction):\n')
sys.stdout.write('  %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(filtered4), f4_w, f4_l, f4_wr, f4_pnl))

# Daily avg
dates3 = set(t[0] for t in filtered3)
dates4 = set(t[0] for t in filtered4)
dates_all = set(t[0] for t in all_trades)
sys.stdout.write('\n--- Daily averages ---\n')
sys.stdout.write('  Baseline:   %.1f pts/day (%d days)\n' % (total_pnl/len(dates_all), len(dates_all)))
sys.stdout.write('  Align >= 2: %.1f pts/day (%d days)\n' % (f3_pnl/len(dates3) if dates3 else 0, len(dates3)))
sys.stdout.write('  Align >= 3: %.1f pts/day (%d days)\n' % (f4_pnl/len(dates4) if dates4 else 0, len(dates4)))

sys.stdout.flush()
c.close()
