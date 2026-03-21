"""Two-account simulation: Account A (longs) + Account B (shorts), 2 ES each."""
import psycopg2
from collections import defaultdict
from datetime import timedelta

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

cur.execute('''
    SELECT id, ts, setup_name, direction, grade,
           outcome_result, outcome_pnl, greek_alignment, vix, overvix,
           outcome_elapsed_min
    FROM setup_log
    WHERE grade != 'LOG'
      AND outcome_result IS NOT NULL
    ORDER BY ts ASC
''')
rows = cur.fetchall()
conn.close()


def passes_v9sc(sn, direction, align, vix, overvix):
    is_long = direction in ('long', 'bullish')
    if is_long:
        if align < 2:
            return False
        if sn == 'Skew Charm':
            return True
        if vix is not None and vix > 22:
            ov = overvix if overvix is not None else -99
            if ov < 2:
                return False
        return True
    else:
        if sn in ('Skew Charm', 'AG Short'):
            return True
        if sn == 'DD Exhaustion' and align != 0:
            return True
        return False


PER_PT = 100  # 2 ES × $50/pt

daily_long = defaultdict(lambda: {'pnl_pts': 0, 'trades': 0, 'wins': 0, 'losses': 0})
daily_short = defaultdict(lambda: {'pnl_pts': 0, 'trades': 0, 'wins': 0, 'losses': 0})
daily_combined = defaultdict(lambda: {'pnl_pts': 0, 'trades': 0, 'wins': 0, 'losses': 0})

trade_windows_long = []
trade_windows_short = []

for r in rows:
    sid, ts, sn, d, grade, result, pnl, align, vix, ov, elapsed = r
    align = int(align) if align is not None else 0
    vix_f = float(vix) if vix is not None else None
    ov_f = float(ov) if ov is not None else None
    pnl_f = float(pnl) if pnl is not None else 0.0

    if not passes_v9sc(sn, d, align, vix_f, ov_f):
        continue

    trade_date = ts.date()
    is_long = d in ('long', 'bullish')
    is_win = result == 'WIN'
    is_loss = result == 'LOSS'

    daily_combined[trade_date]['pnl_pts'] += pnl_f
    daily_combined[trade_date]['trades'] += 1
    if is_win:
        daily_combined[trade_date]['wins'] += 1
    if is_loss:
        daily_combined[trade_date]['losses'] += 1

    elapsed_min = int(elapsed) if elapsed else 30

    if is_long:
        daily_long[trade_date]['pnl_pts'] += pnl_f
        daily_long[trade_date]['trades'] += 1
        if is_win:
            daily_long[trade_date]['wins'] += 1
        if is_loss:
            daily_long[trade_date]['losses'] += 1
        trade_windows_long.append((ts, ts + timedelta(minutes=elapsed_min)))
    else:
        daily_short[trade_date]['pnl_pts'] += pnl_f
        daily_short[trade_date]['trades'] += 1
        if is_win:
            daily_short[trade_date]['wins'] += 1
        if is_loss:
            daily_short[trade_date]['losses'] += 1
        trade_windows_short.append((ts, ts + timedelta(minutes=elapsed_min)))

dates = sorted(daily_combined.keys())

print(f'  2 ES/trade = $100/pt. V9-SC filter applied.')
print()
print(f'{"Date":>12s}  {"Acct A (Longs)":>18s}  {"Acct B (Shorts)":>18s}  {"Combined":>18s}  {"Cumul $":>10s}')
print('-' * 90)

cumul_long = 0
cumul_short = 0
cumul_combined = 0
peak_combined = 0
max_dd_combined = 0
peak_long = 0
max_dd_long = 0
peak_short = 0
max_dd_short = 0

for d in dates:
    l = daily_long[d]
    s = daily_short[d]
    c = daily_combined[d]

    l_pnl = round(l['pnl_pts'], 1)
    s_pnl = round(s['pnl_pts'], 1)
    c_pnl = round(c['pnl_pts'], 1)

    l_dollar = round(l_pnl * PER_PT)
    s_dollar = round(s_pnl * PER_PT)
    c_dollar = round(c_pnl * PER_PT)

    cumul_long += l_pnl
    cumul_short += s_pnl
    cumul_combined += c_pnl

    if cumul_combined > peak_combined:
        peak_combined = cumul_combined
    dd = cumul_combined - peak_combined
    if dd < max_dd_combined:
        max_dd_combined = dd

    if cumul_long > peak_long:
        peak_long = cumul_long
    dd_l = cumul_long - peak_long
    if dd_l < max_dd_long:
        max_dd_long = dd_l

    if cumul_short > peak_short:
        peak_short = cumul_short
    dd_s = cumul_short - peak_short
    if dd_s < max_dd_short:
        max_dd_short = dd_s

    marker = ''
    if c_pnl < -50:
        marker = ' !!!'

    print(f'{str(d):>12s}  {l_pnl:+6.1f}pts {l["trades"]:2d}t ${l_dollar:>+6d}  {s_pnl:+6.1f}pts {s["trades"]:2d}t ${s_dollar:>+6d}  {c_pnl:+6.1f}pts {c["trades"]:2d}t ${c_dollar:>+6d}  ${round(cumul_combined * PER_PT):>+8d}{marker}')

print('-' * 90)
print()

total_l = round(cumul_long, 1)
total_s = round(cumul_short, 1)
total_c = round(cumul_combined, 1)
total_trades_l = sum(daily_long[d]['trades'] for d in dates)
total_trades_s = sum(daily_short[d]['trades'] for d in dates)
total_trades_c = sum(daily_combined[d]['trades'] for d in dates)
wins_l = sum(daily_long[d]['wins'] for d in dates)
losses_l = sum(daily_long[d]['losses'] for d in dates)
wins_s = sum(daily_short[d]['wins'] for d in dates)
losses_s = sum(daily_short[d]['losses'] for d in dates)
wins_c = sum(daily_combined[d]['wins'] for d in dates)
losses_c = sum(daily_combined[d]['losses'] for d in dates)

print('=== SUMMARY (30 trading days) ===')
print()
print(f'{"":>20s}  {"Acct A (Longs)":>16s}  {"Acct B (Shorts)":>16s}  {"Combined":>16s}')
print(f'{"Total PnL (pts)":>20s}  {total_l:>+14.1f}  {total_s:>+14.1f}  {total_c:>+14.1f}')
print(f'{"Total PnL ($)":>20s}  {"$" + str(round(total_l * PER_PT)):>14s}  {"$" + str(round(total_s * PER_PT)):>14s}  {"$" + str(round(total_c * PER_PT)):>14s}')
print(f'{"Trades":>20s}  {total_trades_l:>14d}  {total_trades_s:>14d}  {total_trades_c:>14d}')
wr_l = round(wins_l / (wins_l + losses_l) * 100, 1) if (wins_l + losses_l) > 0 else 0
wr_s = round(wins_s / (wins_s + losses_s) * 100, 1) if (wins_s + losses_s) > 0 else 0
wr_c = round(wins_c / (wins_c + losses_c) * 100, 1) if (wins_c + losses_c) > 0 else 0
print(f'{"Win Rate":>20s}  {str(wr_l) + "%":>14s}  {str(wr_s) + "%":>14s}  {str(wr_c) + "%":>14s}')
print(f'{"Wins/Losses":>20s}  {str(wins_l) + "W/" + str(losses_l) + "L":>14s}  {str(wins_s) + "W/" + str(losses_s) + "L":>14s}  {str(wins_c) + "W/" + str(losses_c) + "L":>14s}')
print(f'{"Pts/Day":>20s}  {total_l / len(dates):>+14.1f}  {total_s / len(dates):>+14.1f}  {total_c / len(dates):>+14.1f}')
print(f'{"$/Day":>20s}  {"$" + str(round(total_l / len(dates) * PER_PT)):>14s}  {"$" + str(round(total_s / len(dates) * PER_PT)):>14s}  {"$" + str(round(total_c / len(dates) * PER_PT)):>14s}')
print(f'{"Max Drawdown (pts)":>20s}  {max_dd_long:>+14.1f}  {max_dd_short:>+14.1f}  {max_dd_combined:>+14.1f}')
print(f'{"Max Drawdown ($)":>20s}  {"$" + str(round(max_dd_long * PER_PT)):>14s}  {"$" + str(round(max_dd_short * PER_PT)):>14s}  {"$" + str(round(max_dd_combined * PER_PT)):>14s}')
print(f'{"$/Month (21 days)":>20s}  {"$" + str(round(total_l / len(dates) * 21 * PER_PT)):>14s}  {"$" + str(round(total_s / len(dates) * 21 * PER_PT)):>14s}  {"$" + str(round(total_c / len(dates) * 21 * PER_PT)):>14s}')


def max_concurrent(windows):
    if not windows:
        return 0
    events = []
    for start, end in windows:
        events.append((start, 1))
        events.append((end, -1))
    events.sort(key=lambda x: (x[0], x[1]))
    current = 0
    max_c = 0
    for _, delta in events:
        current += delta
        if current > max_c:
            max_c = current
    return max_c


max_conc_l = max_concurrent(trade_windows_long)
max_conc_s = max_concurrent(trade_windows_short)

print()
print(f'Max concurrent longs:  {max_conc_l} (margin: {max_conc_l} x 2 ES = {max_conc_l * 2} ES)')
print(f'Max concurrent shorts: {max_conc_s} (margin: {max_conc_s} x 2 ES = {max_conc_s * 2} ES)')
print(f'ES margin ~$13,000/contract')
print(f'  Acct A needs: ${max_conc_l * 2 * 13000:,} (peak)')
print(f'  Acct B needs: ${max_conc_s * 2 * 13000:,} (peak)')

worst_day = min(dates, key=lambda d: daily_combined[d]['pnl_pts'])
best_day = max(dates, key=lambda d: daily_combined[d]['pnl_pts'])
print()
print(f'Best day:  {best_day} {daily_combined[best_day]["pnl_pts"]:+.1f} pts (${round(daily_combined[best_day]["pnl_pts"] * PER_PT):+,})')
print(f'Worst day: {worst_day} {daily_combined[worst_day]["pnl_pts"]:+.1f} pts (${round(daily_combined[worst_day]["pnl_pts"] * PER_PT):+,})')

prof_days = sum(1 for d in dates if daily_combined[d]['pnl_pts'] > 0)
print(f'Profitable days: {prof_days}/{len(dates)} ({round(prof_days / len(dates) * 100)}%)')
