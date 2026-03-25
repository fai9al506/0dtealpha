import csv
from datetime import datetime, timedelta

# Load 5-min OHLC bars — TradingView timestamps are UTC, convert to ET (UTC-4)
bars = []
with open('exports/spx_5min_ohlc.csv', 'r') as f:
    for r in csv.DictReader(f):
        ts_utc = datetime.utcfromtimestamp(int(r['time']))
        ts_et = ts_utc - timedelta(hours=4)  # UTC to ET (EDT in March)
        bars.append({
            'ts': ts_et,
            'open': float(r['open']),
            'high': float(r['high']),
            'low': float(r['low']),
            'close': float(r['close']),
        })
bars.sort(key=lambda x: x['ts'])
print(f"Loaded {len(bars)} 5-min OHLC bars")

# Load trades with charm_limit
with open('exports/setup_log_shorts_spx_mar12_enriched.csv', 'r', encoding='utf-8') as f:
    all_trades = list(csv.DictReader(f))
trades = [t for t in all_trades if t['charm_limit_entry'] and t['charm_limit_entry'] != '']
print(f"Trades with charm_limit: {len(trades)}")

# SL distances
SL = {'Skew Charm': 14, 'DD Exhaustion': 12, 'AG Short': 8, 'Paradigm Reversal': 15, 'Vanna Pivot Bounce': 8}
TRAIL = {
    'Skew Charm': ('hybrid', 10, 10, 8),
    'DD Exhaustion': ('continuous', None, 20, 5),
    'AG Short': ('hybrid', 10, 15, 5),
}
FIXED_TARGET = {'Paradigm Reversal': 10, 'Vanna Pivot Bounce': 10}


def market_close(dt):
    return dt.replace(hour=16, minute=0, second=0, microsecond=0)


results = []
for t in trades:
    ts_fire = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
    spot = float(t['spot'])
    charm_limit = float(t['charm_limit_entry'])
    setup = t['setup_name']
    offset = round(charm_limit - spot, 2)
    mkt_pnl = float(t['outcome_pnl']) if t['outcome_pnl'] else 0.0

    # Get 5-min bars in the 30-min fill window
    fill_window_end = ts_fire + timedelta(minutes=30)
    fill_bars = [b for b in bars if b['ts'] >= ts_fire and b['ts'] <= fill_window_end]

    # --- FILL CHECK using bar HIGH (captures intra-bar spikes) ---
    fill_high = max((b['high'] for b in fill_bars), default=spot)
    filled = fill_high >= charm_limit

    if not filled:
        results.append({
            'id': t['id'], 'trade_date': t['ts_et'][:10], 'setup_name': setup,
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

    # --- FIND FILL TIME (first bar whose high >= charm_limit) ---
    fill_time = ts_fire
    for b in fill_bars:
        if b['high'] >= charm_limit:
            fill_time = b['ts']
            break

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

    # --- SIMULATE using 5-min OHLC from fill time to close ---
    close_time = market_close(fill_time)
    max_fav = 0.0
    outcome = None
    pnl = None
    t1_hit = False

    sim_bars = [b for b in bars if b['ts'] >= fill_time and b['ts'] <= close_time]

    for b in sim_bars:
        bar_high = b['high']
        bar_low = b['low']

        # Short: favorable = price going DOWN (bar_low is best case)
        fav = entry - bar_low
        if fav > max_fav:
            max_fav = fav

        # T1 check
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

        # Check stop hit (short: bar_high rises to stop)
        if bar_high >= trail_stop:
            outcome = 'WIN' if trail_stop <= entry else 'LOSS'
            pnl = entry - trail_stop
            if t1_hit:
                pnl = round((10.0 + pnl) / 2, 1)
                outcome = 'WIN' if pnl > 0 else 'LOSS'
            break

        # Check fixed target hit (short: bar_low drops to target)
        if target_lvl and bar_low <= target_lvl:
            outcome = 'WIN'
            pnl = entry - target_lvl
            break

    if outcome is None:
        # EXPIRED — use last bar's close
        last_close = sim_bars[-1]['close'] if sim_bars else spot
        pnl = entry - last_close
        if t1_hit:
            pnl = round((10.0 + pnl) / 2, 1)
        outcome = 'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'EXPIRED')

    pnl = round(pnl, 1)

    old_out = t['outcome_result']
    if old_out == outcome:
        if outcome == 'WIN':
            diff = pnl - mkt_pnl
            change = f"+{diff:.1f} more" if diff > 0 else f"{diff:.1f}"
        elif outcome == 'LOSS':
            change = f"reduced {abs(mkt_pnl) - abs(pnl):.1f}" if abs(pnl) < abs(mkt_pnl) else f"{pnl - mkt_pnl:+.1f}"
        else:
            change = f"{pnl - mkt_pnl:+.1f}"
    else:
        change = f"FLIP {old_out}->{outcome}"

    results.append({
        'id': t['id'], 'trade_date': t['ts_et'][:10], 'setup_name': setup,
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
with open('exports/charm_limit_pnl_comparison_v2.csv', 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(results)

# --- SUMMARY ---
filled_r = [r for r in results if r['filled'] == 'YES']
timeout_r = [r for r in results if r['filled'] == 'NO']

mkt_total = sum(r['market_pnl'] for r in results)
lim_total = sum(r['limit_pnl'] for r in results)
mkt_wins = sum(1 for r in results if r['market_outcome'] == 'WIN')
lim_wins = sum(1 for r in filled_r if r['limit_outcome'] == 'WIN')
lim_losses = sum(1 for r in filled_r if r['limit_outcome'] == 'LOSS')

print(f"\n{'='*70}")
print(f"CHARM-LIMIT PnL COMPARISON v2 (5-min OHLC): 96 Short Trades")
print(f"{'='*70}")
print(f"Filled: {len(filled_r)} | Timeout: {len(timeout_r)} | Fill rate: {len(filled_r)/len(results)*100:.0f}%")
print()
print(f"  MARKET entry (spot):   PnL = {mkt_total:+.1f} pts   WR = {mkt_wins}/96 ({mkt_wins/96*100:.0f}%)")
if filled_r:
    print(f"  LIMIT entry (charm):   PnL = {lim_total:+.1f} pts   WR = {lim_wins}/{len(filled_r)} ({lim_wins/len(filled_r)*100:.0f}%)")
    print(f"                         (timeout trades: PnL = 0)")
print(f"  DELTA:                       {lim_total - mkt_total:+.1f} pts")

# Flips
print(f"\n--- OUTCOME FLIPS ---")
flips = [r for r in filled_r if r['market_outcome'] != r['limit_outcome']]
for r in flips:
    print(f"  id={r['id']} {r['trade_date']} {r['setup_name']:<18} "
          f"{r['market_outcome']}({r['market_pnl']:+.1f}) -> {r['limit_outcome']}({r['limit_pnl']:+.1f})  "
          f"offset={r['offset']:+.1f}")
if not flips:
    print("  None")

# Timeouts
print(f"\n--- TIMEOUTS ({len(timeout_r)} trades) ---")
if timeout_r:
    to_win = [r for r in timeout_r if r['market_outcome'] == 'WIN']
    to_loss = [r for r in timeout_r if r['market_outcome'] == 'LOSS']
    to_exp = [r for r in timeout_r if r['market_outcome'] == 'EXPIRED']
    print(f"  Was WIN at market:  {len(to_win)} trades, lost opportunity = {sum(r['market_pnl'] for r in to_win):+.1f} pts")
    print(f"  Was LOSS at market: {len(to_loss)} trades, saved losses = {sum(abs(r['market_pnl']) for r in to_loss):.1f} pts")
    print(f"  Was EXPIRED:        {len(to_exp)} trades, pnl = {sum(r['market_pnl'] for r in to_exp):+.1f} pts")

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

print(f"\nSaved to exports/charm_limit_pnl_comparison_v2.csv")
