"""Check all today's signals"""
import os, sys
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()
trades = c.execute(text("""
    SELECT to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t_et,
           setup_name, direction, grade, greek_alignment, paradigm,
           outcome_result, outcome_pnl, spot
    FROM setup_log
    WHERE ts::date = '2026-03-10'
    ORDER BY ts
""")).fetchall()
for t in trades:
    pnl = float(t[7] or 0)
    result = t[6] or 'OPEN'
    a3 = '<-- +3' if abs(t[4] or 0) >= 3 else ''
    sys.stdout.write('%s ET  %-20s %-7s [%-7s] align=%+d  %-15s %-8s %+6.1f  %s\n' % (
        t[0], t[1], t[2], t[3] or '?', t[4] or 0, t[5] or '', result, pnl, a3))

sys.stdout.write('\n')

# Summary
a3_trades = [t for t in trades if t[6] and abs(t[4] or 0) >= 3]
other = [t for t in trades if t[6] and abs(t[4] or 0) < 3]
a3_pnl = sum(float(t[7] or 0) for t in a3_trades)
a3_w = sum(1 for t in a3_trades if 'WIN' in t[6])
a3_l = sum(1 for t in a3_trades if 'LOSS' in t[6])
other_pnl = sum(float(t[7] or 0) for t in other)
other_w = sum(1 for t in other if 'WIN' in t[6])
other_l = sum(1 for t in other if 'LOSS' in t[6])
total_pnl = sum(float(t[7] or 0) for t in trades if t[6])

sys.stdout.write('ALIGN +3 trades:  %dW/%dL  PnL=%+.1f\n' % (a3_w, a3_l, a3_pnl))
sys.stdout.write('Other trades:     %dW/%dL  PnL=%+.1f\n' % (other_w, other_l, other_pnl))
sys.stdout.write('Total today:      PnL=%+.1f\n' % total_pnl)

# Longs vs shorts
longs = [t for t in trades if t[6] and t[2] in ('long', 'bullish')]
shorts = [t for t in trades if t[6] and t[2] in ('short', 'bearish')]
l_pnl = sum(float(t[7] or 0) for t in longs)
l_w = sum(1 for t in longs if 'WIN' in t[6])
l_l = sum(1 for t in longs if 'LOSS' in t[6])
s_pnl = sum(float(t[7] or 0) for t in shorts)
s_w = sum(1 for t in shorts if 'WIN' in t[6])
s_l = sum(1 for t in shorts if 'LOSS' in t[6])
sys.stdout.write('\nLongs:   %dW/%dL  PnL=%+.1f\n' % (l_w, l_l, l_pnl))
sys.stdout.write('Shorts:  %dW/%dL  PnL=%+.1f\n' % (s_w, s_l, s_pnl))

sys.stdout.flush()
c.close()
