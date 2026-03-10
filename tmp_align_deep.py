import os, sys
from sqlalchemy import create_engine, text
from collections import defaultdict
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

all_trades = c.execute(text("""
    SELECT ts::date as d, setup_name, direction, paradigm, 
           outcome_result, outcome_pnl, greek_alignment,
           to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t_et
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    ORDER BY ts
""")).fetchall()

# Today = Mar 9 (most recent trading day)
today_trades = [t for t in all_trades if str(t[0]) == '2026-03-09']

sys.stdout.write('='*70 + '\n')
sys.stdout.write('ALIGNMENT +3 ONLY (ALL HISTORY)\n')
sys.stdout.write('='*70 + '\n')

a3 = [t for t in all_trades if (t[6] or 0) >= 3]
a3_w = sum(1 for t in a3 if t[4] and 'WIN' in t[4])
a3_l = sum(1 for t in a3 if t[4] and 'LOSS' in t[4])
a3_pnl = sum(float(t[5] or 0) for t in a3)
a3_wr = a3_w/(a3_w+a3_l)*100 if (a3_w+a3_l) else 0
dates3 = set(t[0] for t in a3)
sys.stdout.write('  %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (len(a3), a3_w, a3_l, a3_wr, a3_pnl))
sys.stdout.write('  Active on %d of 22 trading days (%.0f%%)\n' % (len(dates3), len(dates3)/22*100))
sys.stdout.write('  Avg: %.1f trades/day, %.1f pts/day (on active days)\n' % (len(a3)/len(dates3) if dates3 else 0, a3_pnl/len(dates3) if dates3 else 0))
sys.stdout.write('  Avg across ALL 22 days: %.1f pts/day\n\n' % (a3_pnl/22))

# Per setup
sys.stdout.write('  Per setup:\n')
ss = defaultdict(lambda: {'w':0,'l':0,'pnl':0,'n':0})
for t in a3:
    s = ss[t[1]]
    s['n'] += 1
    s['pnl'] += float(t[5] or 0)
    if t[4] and 'WIN' in t[4]: s['w'] += 1
    if t[4] and 'LOSS' in t[4]: s['l'] += 1
for setup, s in sorted(ss.items(), key=lambda x: -x[1]['pnl']):
    wr = s['w']/(s['w']+s['l'])*100 if (s['w']+s['l']) else 0
    sys.stdout.write('    %-20s: %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (setup, s['n'], s['w'], s['l'], wr, s['pnl']))

# Per day
sys.stdout.write('\n  Per day:\n')
day_stats = defaultdict(lambda: {'w':0,'l':0,'pnl':0,'n':0})
for t in a3:
    s = day_stats[str(t[0])]
    s['n'] += 1
    s['pnl'] += float(t[5] or 0)
    if t[4] and 'WIN' in t[4]: s['w'] += 1
    if t[4] and 'LOSS' in t[4]: s['l'] += 1
for day in sorted(day_stats.keys()):
    s = day_stats[day]
    wr = s['w']/(s['w']+s['l'])*100 if (s['w']+s['l']) else 0
    sys.stdout.write('    %s: %d trades, %dW/%dL (%.0f%% WR), PnL=%+.1f\n' % (day, s['n'], s['w'], s['l'], wr, s['pnl']))

# ============================================
# TODAY (MAR 9) with both filters
# ============================================
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('MAR 9 COMPARISON\n')
sys.stdout.write('='*70 + '\n')

# All trades
t_all_pnl = sum(float(t[5] or 0) for t in today_trades)
t_all_w = sum(1 for t in today_trades if t[4] and 'WIN' in t[4])
t_all_l = sum(1 for t in today_trades if t[4] and 'LOSS' in t[4])
sys.stdout.write('\nNO FILTER (all 32 trades):\n')
sys.stdout.write('  %dW/%dL, PnL=%+.1f pts\n' % (t_all_w, t_all_l, t_all_pnl))

# Align >= +2
t_a2 = [t for t in today_trades if abs(t[6] or 0) >= 2]
t_a2_pnl = sum(float(t[5] or 0) for t in t_a2)
t_a2_w = sum(1 for t in t_a2 if t[4] and 'WIN' in t[4])
t_a2_l = sum(1 for t in t_a2 if t[4] and 'LOSS' in t[4])
sys.stdout.write('\nALIGN >= +2 (any direction):\n')
t_a2b = [t for t in today_trades if (t[6] or 0) >= 2]
t_a2b_pnl = sum(float(t[5] or 0) for t in t_a2b)
t_a2b_w = sum(1 for t in t_a2b if t[4] and 'WIN' in t[4])
t_a2b_l = sum(1 for t in t_a2b if t[4] and 'LOSS' in t[4])
sys.stdout.write('  %d trades, %dW/%dL, PnL=%+.1f pts\n' % (len(t_a2b), t_a2b_w, t_a2b_l, t_a2b_pnl))
for t in t_a2b:
    pnl = float(t[5] or 0)
    sys.stdout.write('    %s ET  %-20s %-7s align=%+d  %-8s %+.1f\n' % (t[7], t[1], t[2], t[6] or 0, t[4] or 'OPEN', pnl))

# Align >= +3
sys.stdout.write('\nALIGN >= +3 (any direction):\n')
t_a3 = [t for t in today_trades if (t[6] or 0) >= 3]
t_a3_pnl = sum(float(t[5] or 0) for t in t_a3)
t_a3_w = sum(1 for t in t_a3 if t[4] and 'WIN' in t[4])
t_a3_l = sum(1 for t in t_a3 if t[4] and 'LOSS' in t[4])
sys.stdout.write('  %d trades, %dW/%dL, PnL=%+.1f pts\n' % (len(t_a3), t_a3_w, t_a3_l, t_a3_pnl))
for t in t_a3:
    pnl = float(t[5] or 0)
    sys.stdout.write('    %s ET  %-20s %-7s align=%+d  %-8s %+.1f\n' % (t[7], t[1], t[2], t[6] or 0, t[4] or 'OPEN', pnl))

# Summary comparison
sys.stdout.write('\n' + '='*70 + '\n')
sys.stdout.write('SUMMARY\n')
sys.stdout.write('='*70 + '\n')
sys.stdout.write('                    Trades   WR      PnL     Pts/day\n')
sys.stdout.write('  No filter:        %3d    %3.0f%%   %+7.1f   %+5.1f\n' % (len(all_trades), total_wr, total_pnl, total_pnl/22))
a2 = [t for t in all_trades if (t[6] or 0) >= 2]
a2_pnl = sum(float(t[5] or 0) for t in a2)
a2_w = sum(1 for t in a2 if t[4] and 'WIN' in t[4])
a2_l = sum(1 for t in a2 if t[4] and 'LOSS' in t[4])
a2_wr = a2_w/(a2_w+a2_l)*100 if (a2_w+a2_l) else 0
sys.stdout.write('  Align >= +2:      %3d    %3.0f%%   %+7.1f   %+5.1f\n' % (len(a2), a2_wr, a2_pnl, a2_pnl/22))
sys.stdout.write('  Align >= +3:       %3d    %3.0f%%   %+7.1f   %+5.1f\n' % (len(a3), a3_wr, a3_pnl, a3_pnl/22))

total_w = sum(1 for t in all_trades if t[4] and 'WIN' in t[4])
total_l = sum(1 for t in all_trades if t[4] and 'LOSS' in t[4])
total_pnl = sum(float(t[5] or 0) for t in all_trades)
total_wr = total_w/(total_w+total_l)*100 if (total_w+total_l) else 0

sys.stdout.write('\n  Mar 9 only:\n')
sys.stdout.write('  No filter:   32 trades, PnL=%+.1f\n' % t_all_pnl)
sys.stdout.write('  Align >= +2: %d trades, PnL=%+.1f\n' % (len(t_a2b), t_a2b_pnl))
sys.stdout.write('  Align >= +3: %d trades, PnL=%+.1f\n' % (len(t_a3), t_a3_pnl))

sys.stdout.flush()
c.close()
