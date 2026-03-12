import os, json
from sqlalchemy import create_engine, text
from collections import defaultdict

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

rows = c.execute(text("""
    SELECT setup_log_id,
      state->>'setup_name' as setup,
      state->>'direction' as dir,
      (state->>'entry_price')::float as entry,
      (state->>'close_price')::float as close_price,
      state->>'status' as status,
      state->>'ts_placed' as placed
    FROM options_trade_orders
    ORDER BY setup_log_id
""")).fetchall()

print(f"TOTAL OPTIONS TRADES: {len(rows)}", flush=True)

# Group by date
daily = defaultdict(list)
for r in rows:
    if r[6]:  # ts_placed
        date = r[6][:10]
    else:
        date = "unknown"
    entry = r[3] or 0
    close = r[4] or 0
    pnl = (close - entry) * 100
    daily[date].append({
        'id': r[0], 'setup': r[1], 'dir': r[2],
        'entry': entry, 'close': close, 'pnl': pnl,
        'status': r[5]
    })

print(f"\n{'Date':<12} {'Trades':>6} {'Wins':>5} {'Loss':>5} {'WR':>5} {'P&L':>8} {'Avg':>7}", flush=True)
print("-" * 60, flush=True)

grand_total = 0
grand_trades = 0
grand_wins = 0
all_pnls = []
for date in sorted(daily.keys()):
    trades = daily[date]
    wins = sum(1 for t in trades if t['pnl'] > 0)
    losses = len(trades) - wins
    total_pnl = sum(t['pnl'] for t in trades)
    wr = wins / len(trades) * 100 if trades else 0
    avg = total_pnl / len(trades) if trades else 0
    grand_total += total_pnl
    grand_trades += len(trades)
    grand_wins += wins
    all_pnls.append(total_pnl)
    print(f"{date:<12} {len(trades):>6} {wins:>5} {losses:>5} {wr:>4.0f}% ${total_pnl:>+7.0f} ${avg:>+6.0f}", flush=True)

print("-" * 60, flush=True)
print(f"{'TOTAL':<12} {grand_trades:>6} {grand_wins:>5} {grand_trades-grand_wins:>5} {grand_wins/grand_trades*100:>4.0f}% ${grand_total:>+7.0f} ${grand_total/grand_trades:>+6.0f}", flush=True)

# Max drawdown
running = 0
peak = 0
max_dd = 0
for p in all_pnls:
    running += p
    if running > peak:
        peak = running
    dd = peak - running
    if dd > max_dd:
        max_dd = dd
print(f"\nCumulative: ${grand_total:+.0f}", flush=True)
print(f"Max daily DD: ${max_dd:.0f}", flush=True)
print(f"Avg/day: ${grand_total/len(daily):+.0f}", flush=True)
print(f"Win days: {sum(1 for p in all_pnls if p > 0)}/{len(all_pnls)}", flush=True)

# By setup
print(f"\n{'Setup':<20} {'Trades':>6} {'WR':>5} {'P&L':>8} {'Avg':>7}", flush=True)
print("-" * 50, flush=True)
by_setup = defaultdict(list)
for date in daily:
    for t in daily[date]:
        by_setup[t['setup']].append(t)
for setup in sorted(by_setup.keys(), key=lambda s: -sum(t['pnl'] for t in by_setup[s])):
    trades = by_setup[setup]
    wins = sum(1 for t in trades if t['pnl'] > 0)
    total = sum(t['pnl'] for t in trades)
    print(f"{setup:<20} {len(trades):>6} {wins/len(trades)*100:>4.0f}% ${total:>+7.0f} ${total/len(trades):>+6.0f}", flush=True)

# Winning vs losing trade sizes
winners = [t['pnl'] for d in daily.values() for t in d if t['pnl'] > 0]
losers = [t['pnl'] for d in daily.values() for t in d if t['pnl'] <= 0]
if winners:
    print(f"\nAvg winner: ${sum(winners)/len(winners):+.0f}", flush=True)
if losers:
    print(f"Avg loser: ${sum(losers)/len(losers):+.0f}", flush=True)
if losers and winners:
    print(f"Win/Loss ratio: {abs(sum(winners)/len(winners)) / abs(sum(losers)/len(losers)):.2f}", flush=True)
    pf = sum(winners) / abs(sum(losers)) if sum(losers) != 0 else float('inf')
    print(f"Profit factor: {pf:.2f}", flush=True)

c.close()
