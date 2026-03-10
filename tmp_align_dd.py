import os, sys
from sqlalchemy import create_engine, text
from collections import defaultdict
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

all_trades = c.execute(text("""
    SELECT ts::date as d, setup_name, direction, paradigm, 
           outcome_result, outcome_pnl, greek_alignment, ts
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    ORDER BY ts
""")).fetchall()

def analyze(trades, label):
    sys.stdout.write('\n%s\n' % ('='*70))
    sys.stdout.write('%s\n' % label)
    sys.stdout.write('%s\n' % ('='*70))
    
    if not trades:
        sys.stdout.write('  No trades\n')
        return
    
    w = sum(1 for t in trades if t[4] and 'WIN' in t[4])
    l = sum(1 for t in trades if t[4] and 'LOSS' in t[4])
    pnl = sum(float(t[5] or 0) for t in trades)
    wr = w/(w+l)*100 if (w+l) else 0
    
    # Equity curve + drawdown
    equity = 0
    peak = 0
    max_dd = 0
    max_dd_pts = 0
    losing_streaks = []
    current_streak = 0
    max_streak = 0
    daily_pnl = defaultdict(float)
    daily_count = defaultdict(int)
    worst_trade = 0
    
    for t in trades:
        p = float(t[5] or 0)
        equity += p
        daily_pnl[str(t[0])] += p
        daily_count[str(t[0])] += 1
        if p < worst_trade:
            worst_trade = p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd
        if t[4] and 'LOSS' in t[4]:
            current_streak += 1
            if current_streak > max_streak:
                max_streak = current_streak
        else:
            if current_streak > 0:
                losing_streaks.append(current_streak)
            current_streak = 0
    if current_streak > 0:
        losing_streaks.append(current_streak)
    
    # Daily stats
    daily_vals = sorted(daily_pnl.items())
    worst_day = min(daily_vals, key=lambda x: x[1])
    best_day = max(daily_vals, key=lambda x: x[1])
    neg_days = sum(1 for d, p in daily_vals if p < 0)
    pos_days = sum(1 for d, p in daily_vals if p > 0)
    zero_days = 22 - len(daily_vals)  # days with no trades
    
    # Daily DD (running)
    daily_equity = 0
    daily_peak = 0
    daily_max_dd = 0
    for d, p in daily_vals:
        daily_equity += p
        if daily_equity > daily_peak:
            daily_peak = daily_equity
        dd = daily_peak - daily_equity
        if dd > daily_max_dd:
            daily_max_dd = dd
    
    sys.stdout.write('  Trades: %d (%dW/%dL, %.0f%% WR)\n' % (len(trades), w, l, wr))
    sys.stdout.write('  Total PnL: %+.1f pts\n' % pnl)
    sys.stdout.write('  Avg PnL/trade: %+.1f pts\n' % (pnl/len(trades)))
    sys.stdout.write('  Worst trade: %+.1f pts\n' % worst_trade)
    sys.stdout.write('  Max losing streak: %d trades\n' % max_streak)
    sys.stdout.write('  Max trade-level DD: %.1f pts\n' % max_dd)
    sys.stdout.write('  Max daily DD: %.1f pts\n' % daily_max_dd)
    sys.stdout.write('\n  Daily breakdown:\n')
    sys.stdout.write('    Active days: %d of 22 (%.0f%%)\n' % (len(daily_vals), len(daily_vals)/22*100))
    sys.stdout.write('    Positive days: %d | Negative days: %d | No-trade days: %d\n' % (pos_days, neg_days, zero_days))
    sys.stdout.write('    Best day:  %s %+.1f pts (%d trades)\n' % (best_day[0], best_day[1], daily_count[best_day[0]]))
    sys.stdout.write('    Worst day: %s %+.1f pts (%d trades)\n' % (worst_day[0], worst_day[1], daily_count[worst_day[0]]))
    sys.stdout.write('    Avg pts/active day: %+.1f\n' % (pnl/len(daily_vals)))
    sys.stdout.write('    Avg pts/all days: %+.1f\n' % (pnl/22))
    
    # Money projections
    sys.stdout.write('\n  MONEY PROJECTIONS (20 trading days/month):\n')
    pts_day = pnl / 22
    for label_m, mult in [('1 ES ($50/pt)', 50), ('2 ES ($100/pt)', 100), ('4 ES ($200/pt)', 200), ('10 MES ($50/pt)', 50)]:
        daily_dollar = pts_day * mult
        monthly = daily_dollar * 20
        max_dd_dollar = max_dd * mult
        sys.stdout.write('    %s: $%+.0f/day, $%+.0f/month, MaxDD=$%.0f\n' % (label_m, daily_dollar, monthly, max_dd_dollar))
    
    sys.stdout.write('\n  E2T 50K COMPATIBILITY:\n')
    # 8 MES = $40/pt
    daily_e2t = pts_day * 40
    max_dd_e2t = max_dd * 40
    worst_day_e2t = worst_day[1] * 40
    sys.stdout.write('    8 MES ($40/pt): $%+.0f/day, MaxDD=$%.0f\n' % (daily_e2t, max_dd_e2t))
    sys.stdout.write('    Worst day ($): $%+.0f (E2T daily limit: $1,100)\n' % worst_day_e2t)
    sys.stdout.write('    MaxDD vs E2T trailing DD ($2,000): $%.0f (%s)\n' % (max_dd_e2t, 'SAFE' if max_dd_e2t < 2000 else 'RISKY'))
    
    # Per-day equity curve
    sys.stdout.write('\n  EQUITY CURVE (daily):\n')
    running = 0
    rpeak = 0
    for d, p in daily_vals:
        running += p
        if running > rpeak: rpeak = running
        dd = rpeak - running
        sys.stdout.write('    %s: %+6.1f pts (cum=%+7.1f, peak=%+7.1f, DD=%.1f)\n' % (d, p, running, rpeak, dd))

# Run for all 3 filters
a2 = [t for t in all_trades if (t[6] or 0) >= 2]
a3 = [t for t in all_trades if (t[6] or 0) >= 3]

analyze(all_trades, 'NO FILTER (BASELINE)')
analyze(a2, 'ALIGNMENT >= +2')
analyze(a3, 'ALIGNMENT >= +3')

sys.stdout.flush()
c.close()
