import csv
from datetime import datetime, timedelta

# Load 5-min OHLC bars (UTC -> ET)
bars = []
with open('exports/spx_5min_ohlc.csv', 'r') as f:
    for r in csv.DictReader(f):
        ts_utc = datetime.utcfromtimestamp(int(r['time']))
        ts_et = ts_utc - timedelta(hours=4)
        bars.append({'ts': ts_et, 'high': float(r['high']), 'low': float(r['low']), 'close': float(r['close'])})
bars.sort(key=lambda x: x['ts'])

# Load ALL 127 short SPX trades (Mar 12+), not just those with charm_limit
with open('exports/setup_log_shorts_spx_mar12_enriched.csv', 'r', encoding='utf-8') as f:
    trades = list(csv.DictReader(f))
print(f"Total trades: {len(trades)}")

SL = {'Skew Charm': 14, 'DD Exhaustion': 12, 'AG Short': 8, 'Paradigm Reversal': 15, 'Vanna Pivot Bounce': 8, 'BofA Scalp': 14}
TRAIL = {
    'Skew Charm': ('hybrid', 10, 10, 8),
    'DD Exhaustion': ('continuous', None, 20, 5),
    'AG Short': ('hybrid', 10, 15, 5),
}
FIXED_TARGET = {'Paradigm Reversal': 10, 'Vanna Pivot Bounce': 10}


def market_close(dt):
    return dt.replace(hour=16, minute=0, second=0, microsecond=0)


def simulate_trade(entry, ts_start, setup, sl_dist):
    initial_stop = entry + sl_dist
    trail_stop = initial_stop
    trail = TRAIL.get(setup)
    fixed_t = FIXED_TARGET.get(setup)
    target_lvl = entry - fixed_t if fixed_t else None
    close_time = market_close(ts_start)
    max_fav = 0.0
    t1_hit = False

    sim_bars = [b for b in bars if b['ts'] >= ts_start and b['ts'] <= close_time]
    for b in sim_bars:
        fav = entry - b['low']
        if fav > max_fav:
            max_fav = fav
        if max_fav >= 10 and not t1_hit:
            t1_hit = True
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
                    new_stop = entry
            if new_stop is not None and new_stop < trail_stop:
                trail_stop = new_stop
        if b['high'] >= trail_stop:
            outcome = 'WIN' if trail_stop <= entry else 'LOSS'
            pnl = entry - trail_stop
            if t1_hit:
                pnl = round((10.0 + pnl) / 2, 1)
                outcome = 'WIN' if pnl > 0 else 'LOSS'
            return outcome, round(pnl, 1)
        if target_lvl and b['low'] <= target_lvl:
            return 'WIN', round(entry - target_lvl, 1)

    last_close = sim_bars[-1]['close'] if sim_bars else entry
    pnl = entry - last_close
    if t1_hit:
        pnl = round((10.0 + pnl) / 2, 1)
    outcome = 'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'EXPIRED')
    return outcome, round(pnl, 1)


# Test: MARKET vs fixed offsets (1, 2, 3, 4, 5, 7, 10 pts above spot)
offsets_to_test = [0, 1, 2, 3, 4, 5, 7, 10]

print(f"\n{'='*100}")
print(f"FIXED OFFSET STUDY: {len(trades)} Short SPX Trades (Mar 12-23)")
print(f"{'='*100}")
print(f"0 = MARKET order. N = limit at spot + N pts (30-min timeout).\n")

results_all = {}
for offset in offsets_to_test:
    filled = 0
    timeout = 0
    total_pnl = 0.0
    wins = 0
    losses = 0
    details = []

    for t in trades:
        ts_fire = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
        spot = float(t['spot'])
        setup = t['setup_name']
        sl_dist = SL.get(setup, 14)
        if setup == 'BofA Scalp':
            bofa_sl = t.get('bofa_stop_level')
            if bofa_sl and bofa_sl != '':
                sl_dist = abs(float(bofa_sl) - spot)

        if offset == 0:
            # Market order
            outcome, pnl = simulate_trade(spot, ts_fire, setup, sl_dist)
            filled += 1
            total_pnl += pnl
            if outcome == 'WIN': wins += 1
            elif outcome == 'LOSS': losses += 1
            details.append({'pnl': pnl, 'filled': True, 'outcome': outcome})
            continue

        limit_price = spot + offset
        # Check fill in 30-min window
        fill_end = ts_fire + timedelta(minutes=30)
        fill_bars = [b for b in bars if b['ts'] >= ts_fire and b['ts'] <= fill_end]
        fill_high = max((b['high'] for b in fill_bars), default=spot)

        if fill_high >= limit_price:
            # Filled
            fill_time = ts_fire
            for b in fill_bars:
                if b['high'] >= limit_price:
                    fill_time = b['ts']
                    break
            outcome, pnl = simulate_trade(limit_price, fill_time, setup, sl_dist)
            filled += 1
            total_pnl += pnl
            if outcome == 'WIN': wins += 1
            elif outcome == 'LOSS': losses += 1
            details.append({'pnl': pnl, 'filled': True, 'outcome': outcome})
        else:
            timeout += 1
            details.append({'pnl': 0, 'filled': False, 'outcome': 'TIMEOUT'})

    # MaxDD
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for d in details:
        if d['filled']:
            equity += d['pnl']
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    total = filled + timeout
    wr = wins / filled * 100 if filled > 0 else 0
    fill_rate = filled / total * 100
    ratio = total_pnl / max_dd if max_dd > 0 else float('inf')
    avg = total_pnl / filled if filled > 0 else 0

    results_all[offset] = {
        'filled': filled, 'timeout': timeout, 'pnl': total_pnl,
        'wins': wins, 'losses': losses, 'wr': wr, 'fill_rate': fill_rate,
        'max_dd': max_dd, 'ratio': ratio, 'avg': avg, 'details': details,
    }

# Print table
mkt_pnl = results_all[0]['pnl']
print(f"{'Offset':>6} | {'Fill%':>6} | {'Fill':>4} | {'T/O':>4} | {'WR':>5} | {'W':>3} | {'L':>3} | {'PnL':>10} | {'MaxDD':>8} | {'PnL/DD':>7} | {'Avg/fill':>9} | {'vs Mkt':>10}")
print("-" * 110)
for offset in offsets_to_test:
    r = results_all[offset]
    label = "MKT" if offset == 0 else f"+{offset}pt"
    vs = r['pnl'] - mkt_pnl
    print(f"{label:>6} | {r['fill_rate']:>5.0f}% | {r['filled']:>4} | {r['timeout']:>4} | {r['wr']:>4.0f}% | {r['wins']:>3} | {r['losses']:>3} | {r['pnl']:>+10.1f} | {r['max_dd']:>8.1f} | {r['ratio']:>7.2f} | {r['avg']:>+9.1f} | {vs:>+10.1f}")

# Per-setup for market vs best
print(f"\n--- Per-Setup: MARKET vs +3pt vs +5pt ---")
for setup_name in sorted(set(t['setup_name'] for t in trades)):
    setup_trades_idx = [i for i, t in enumerate(trades) if t['setup_name'] == setup_name]
    print(f"\n  {setup_name} ({len(setup_trades_idx)} trades):")
    for offset in [0, 3, 5]:
        r = results_all[offset]
        s_details = [r['details'][i] for i in setup_trades_idx]
        s_filled = [d for d in s_details if d['filled']]
        s_pnl = sum(d['pnl'] for d in s_details)
        s_wins = sum(1 for d in s_filled if d['outcome'] == 'WIN')
        s_wr = s_wins / len(s_filled) * 100 if s_filled else 0
        label = "MKT" if offset == 0 else f"+{offset}pt"
        print(f"    {label:>5}: PnL={s_pnl:>+8.1f}  Fill={len(s_filled):>3}/{len(s_details)}  WR={s_wr:.0f}%")
