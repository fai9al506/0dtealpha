import csv
from datetime import datetime, timedelta

# Load SPX prices
prices = []
with open('exports/spx_prices.csv', 'r', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        if not r['spot'] or r['spot'] == '':
            continue
        ts = datetime.strptime(r['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
        prices.append((ts, float(r['spot'])))
prices.sort()
print(f"Loaded {len(prices)} price points")

# Load trades with charm_limit
with open('exports/setup_log_shorts_spx_mar12_enriched.csv', 'r', encoding='utf-8') as f:
    all_trades = list(csv.DictReader(f))
trades = [t for t in all_trades if t['charm_limit_entry'] and t['charm_limit_entry'] != '']
print(f"Trades with charm_limit: {len(trades)}")

# SL distances by setup
SL = {'Skew Charm': 14, 'DD Exhaustion': 12, 'AG Short': 8, 'Paradigm Reversal': 15}
# Trail params: (mode, be_trigger, activation, gap)
TRAIL = {
    'Skew Charm': ('hybrid', 10, 10, 8),
    'DD Exhaustion': ('continuous', None, 20, 5),
    'AG Short': ('hybrid', 10, 15, 5),
}
FIXED_TARGET = {'Paradigm Reversal': 10}


def market_close(dt):
    return dt.replace(hour=16, minute=0, second=0, microsecond=0)


results = []
for t in trades:
    ts_fire = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
    spot = float(t['spot'])
    charm_limit = float(t['charm_limit_entry'])
    setup = t['setup_name']
    offset = round(charm_limit - spot, 2)
    high_30 = float(t['high_30min'])
    mkt_pnl = float(t['outcome_pnl']) if t['outcome_pnl'] else 0.0

    # --- FILL CHECK: did price reach charm_limit in 30 min? ---
    filled = high_30 >= charm_limit

    if not filled:
        results.append({
            'id': t['id'], 'trade_date': t['trade_date'], 'setup_name': setup,
            'grade': t['grade'], 'ts_et': t['ts_et'][:16],
            'spot': spot, 'charm_limit': charm_limit, 'offset': offset,
            'filled': 'NO', 'fill_time': '',
            'market_sl': round(spot + SL.get(setup, 14), 1),
            'limit_sl': '',
            'market_outcome': t['outcome_result'], 'market_pnl': mkt_pnl,
            'limit_outcome': 'TIMEOUT', 'limit_pnl': 0.0,
            'change': f"saved {abs(mkt_pnl):.1f}" if mkt_pnl < 0 else f"lost opp {mkt_pnl:.1f}",
        })
        continue

    # --- FIND FILL TIME (first price >= charm_limit after fire) ---
    fill_time = None
    for p_ts, p_spot in prices:
        if p_ts >= ts_fire and p_spot >= charm_limit:
            fill_time = p_ts
            break
    if not fill_time:
        fill_time = ts_fire

    # --- SETUP PARAMS ---
    sl_dist = SL.get(setup)
    if not sl_dist:
        bofa_sl = t.get('bofa_stop_level')
        if bofa_sl and bofa_sl != '':
            sl_dist = abs(float(bofa_sl) - spot)
        else:
            sl_dist = 14

    entry = charm_limit
    initial_stop = entry + sl_dist
    trail_stop = initial_stop

    fixed_t = FIXED_TARGET.get(setup)
    target_lvl = entry - fixed_t if fixed_t else None

    trail = TRAIL.get(setup)

    # --- SIMULATE FROM FILL TIME ---
    close_time = market_close(fill_time)
    max_fav = 0.0
    outcome = None
    pnl = None
    t1_hit = False

    for p_ts, p_spot in prices:
        if p_ts < fill_time:
            continue
        if p_ts > close_time:
            break

        # Short: favorable = price going DOWN
        fav = entry - p_spot
        if fav > max_fav:
            max_fav = fav

        # T1 check (split target at +10)
        if max_fav >= 10 and not t1_hit:
            t1_hit = True

        # Trail logic
        if trail:
            mode, be_trig, activation, gap = trail
            new_stop = None
            if mode == 'continuous':
                if max_fav >= activation:
                    new_stop = entry - (max_fav - gap)
            elif mode == 'hybrid':
                if max_fav >= activation:
                    new_stop = entry - (max_fav - gap)
                elif be_trig and max_fav >= be_trig:
                    new_stop = entry  # breakeven
            if new_stop is not None and new_stop < trail_stop:
                trail_stop = new_stop

        # Check stop hit (short: price rises to stop)
        if p_spot >= trail_stop:
            outcome = 'WIN' if trail_stop <= entry else 'LOSS'
            pnl = entry - trail_stop
            if t1_hit:
                pnl = round((10.0 + pnl) / 2, 1)
                outcome = 'WIN' if pnl > 0 else 'LOSS'
            break

        # Check fixed target hit
        if target_lvl and p_spot <= target_lvl:
            outcome = 'WIN'
            pnl = entry - target_lvl
            break

    if outcome is None:
        # EXPIRED at market close
        last_price = spot
        for p_ts, p_spot in prices:
            if p_ts >= fill_time and p_ts <= close_time:
                last_price = p_spot
        pnl = entry - last_price
        if t1_hit:
            pnl = round((10.0 + pnl) / 2, 1)
        outcome = 'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'EXPIRED')

    pnl = round(pnl, 1)

    # Determine change description
    old_out = t['outcome_result']
    if old_out == outcome:
        if outcome == 'WIN':
            change = f"+{pnl - mkt_pnl:.1f} more" if pnl > mkt_pnl else f"{pnl - mkt_pnl:.1f} less"
        elif outcome == 'LOSS':
            change = f"reduced {abs(pnl) - abs(mkt_pnl):.1f}" if abs(pnl) < abs(mkt_pnl) else "same"
        else:
            change = f"{pnl - mkt_pnl:+.1f}"
    else:
        change = f"FLIP {old_out}->{outcome}"

    results.append({
        'id': t['id'], 'trade_date': t['trade_date'], 'setup_name': setup,
        'grade': t['grade'], 'ts_et': t['ts_et'][:16],
        'spot': spot, 'charm_limit': charm_limit, 'offset': offset,
        'filled': 'YES', 'fill_time': fill_time.strftime('%H:%M'),
        'market_sl': round(spot + SL.get(setup, 14), 1),
        'limit_sl': round(initial_stop, 1),
        'market_outcome': t['outcome_result'], 'market_pnl': mkt_pnl,
        'limit_outcome': outcome, 'limit_pnl': pnl,
        'change': change,
    })

# Write CSV
fieldnames = list(results[0].keys())
with open('exports/charm_limit_pnl_comparison.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(results)

# --- PRINT SUMMARY ---
filled_r = [r for r in results if r['filled'] == 'YES']
timeout_r = [r for r in results if r['filled'] == 'NO']

mkt_total = sum(r['market_pnl'] for r in results)
lim_total = sum(r['limit_pnl'] for r in results)
mkt_wins = sum(1 for r in results if r['market_outcome'] == 'WIN')
lim_wins = sum(1 for r in filled_r if r['limit_outcome'] == 'WIN')

print(f"\n{'='*70}")
print(f"CHARM-LIMIT PnL COMPARISON: 96 Short Trades (Mar 12-23)")
print(f"{'='*70}")
print(f"Filled: {len(filled_r)} | Timeout (no fill): {len(timeout_r)}")
print(f"")
print(f"  MARKET entry (spot):   PnL = {mkt_total:+.1f} pts   WR = {mkt_wins}/96 ({mkt_wins/96*100:.0f}%)")
lim_wr = lim_wins / len(filled_r) * 100 if filled_r else 0
print(f"  LIMIT entry (charm):   PnL = {lim_total:+.1f} pts   WR = {lim_wins}/{len(filled_r)} ({lim_wr:.0f}%)")
print(f"  DELTA:                       {lim_total - mkt_total:+.1f} pts")

# Flips
print(f"\n--- OUTCOME FLIPS ---")
flips = [r for r in filled_r if r['market_outcome'] != r['limit_outcome']]
if flips:
    for r in flips:
        print(f"  id={r['id']} {r['trade_date']} {r['setup_name']:<18} "
              f"{r['market_outcome']}({r['market_pnl']:+.1f}) -> {r['limit_outcome']}({r['limit_pnl']:+.1f})  "
              f"offset={r['offset']:+.1f}")
else:
    print("  None")

# Timeouts
print(f"\n--- TIMEOUTS (limit never hit) ---")
if timeout_r:
    to_was_win = [r for r in timeout_r if r['market_outcome'] == 'WIN']
    to_was_loss = [r for r in timeout_r if r['market_outcome'] == 'LOSS']
    to_was_exp = [r for r in timeout_r if r['market_outcome'] == 'EXPIRED']
    to_win_pnl = sum(r['market_pnl'] for r in to_was_win)
    to_loss_pnl = sum(r['market_pnl'] for r in to_was_loss)
    print(f"  Was WIN at market: {len(to_was_win)} trades, lost opportunity of {to_win_pnl:+.1f} pts")
    print(f"  Was LOSS at market: {len(to_was_loss)} trades, saved {abs(to_loss_pnl):.1f} pts of losses")
    print(f"  Was EXPIRED: {len(to_was_exp)} trades")
else:
    print("  None — all limits filled")

# By setup
print(f"\n--- BY SETUP ---")
print(f"{'Setup':<20} {'Total':>5} {'Fill':>5} {'T/O':>5} {'Mkt PnL':>10} {'Lim PnL':>10} {'Delta':>10}")
setups = {}
for r in results:
    s = r['setup_name']
    if s not in setups:
        setups[s] = {'n': 0, 'filled': 0, 'timeout': 0, 'mkt': 0.0, 'lim': 0.0}
    setups[s]['n'] += 1
    setups[s]['mkt'] += r['market_pnl']
    setups[s]['lim'] += r['limit_pnl']
    if r['filled'] == 'YES':
        setups[s]['filled'] += 1
    else:
        setups[s]['timeout'] += 1

for s, d in sorted(setups.items()):
    print(f"{s:<20} {d['n']:>5} {d['filled']:>5} {d['timeout']:>5} {d['mkt']:>+10.1f} {d['lim']:>+10.1f} {d['lim']-d['mkt']:>+10.1f}")
print(f"{'TOTAL':<20} {len(results):>5} {len(filled_r):>5} {len(timeout_r):>5} {mkt_total:>+10.1f} {lim_total:>+10.1f} {lim_total-mkt_total:>+10.1f}")

# By date
print(f"\n--- BY DATE ---")
print(f"{'Date':<12} {'Total':>5} {'Fill':>5} {'T/O':>5} {'Mkt PnL':>10} {'Lim PnL':>10} {'Delta':>10}")
dates = {}
for r in results:
    d = r['trade_date']
    if d not in dates:
        dates[d] = {'n': 0, 'filled': 0, 'timeout': 0, 'mkt': 0.0, 'lim': 0.0}
    dates[d]['n'] += 1
    dates[d]['mkt'] += r['market_pnl']
    dates[d]['lim'] += r['limit_pnl']
    if r['filled'] == 'YES':
        dates[d]['filled'] += 1
    else:
        dates[d]['timeout'] += 1

for d in sorted(dates.keys()):
    v = dates[d]
    print(f"{d:<12} {v['n']:>5} {v['filled']:>5} {v['timeout']:>5} {v['mkt']:>+10.1f} {v['lim']:>+10.1f} {v['lim']-v['mkt']:>+10.1f}")

print(f"\nResults saved to exports/charm_limit_pnl_comparison.csv")
