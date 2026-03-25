import csv
from datetime import datetime, timedelta

# Load 5-min OHLC bars (UTC -> ET)
bars = []
with open('exports/spx_5min_ohlc.csv', 'r') as f:
    for r in csv.DictReader(f):
        ts_utc = datetime.utcfromtimestamp(int(r['time']))
        ts_et = ts_utc - timedelta(hours=4)
        bars.append({
            'ts': ts_et,
            'high': float(r['high']),
            'low': float(r['low']),
            'close': float(r['close']),
        })
bars.sort(key=lambda x: x['ts'])
print(f"Loaded {len(bars)} 5-min OHLC bars")

# Load charm per-strike data to recompute limits at different thresholds
charm_data = []
with open('exports/volland_charm_strikes.csv', 'r', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        if not r['charm_value'] or not r['strike']:
            continue
        ts = datetime.strptime(r['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
        charm_data.append({
            'ts': ts,
            'strike': float(r['strike']),
            'value': float(r['charm_value']),
            'price': float(r['current_price']) if r['current_price'] else None,
        })
charm_data.sort(key=lambda x: x['ts'])
print(f"Loaded {len(charm_data)} charm strike points")

# Load 96 trades
with open('exports/setup_log_shorts_spx_mar12_enriched.csv', 'r', encoding='utf-8') as f:
    all_trades = list(csv.DictReader(f))
trades = [t for t in all_trades if t['charm_limit_entry'] and t['charm_limit_entry'] != '']
print(f"Trades with charm_limit: {len(trades)}")

# SL/trail params
SL = {'Skew Charm': 14, 'DD Exhaustion': 12, 'AG Short': 8, 'Paradigm Reversal': 15, 'Vanna Pivot Bounce': 8, 'BofA Scalp': 14}
TRAIL = {
    'Skew Charm': ('hybrid', 10, 10, 8),
    'DD Exhaustion': ('continuous', None, 20, 5),
    'AG Short': ('hybrid', 10, 15, 5),
}
FIXED_TARGET = {'Paradigm Reversal': 10, 'Vanna Pivot Bounce': 10}


def market_close(dt):
    return dt.replace(hour=16, minute=0, second=0, microsecond=0)


def get_charm_sr(spot, ts_fire):
    """Find strongest positive charm strike above spot (resistance)
    and strongest negative below (support) from nearest snapshot."""
    # Find charm snapshot closest to (but before) ts_fire
    best_ts = None
    for cd in charm_data:
        if cd['ts'] <= ts_fire:
            best_ts = cd['ts']
        else:
            break
    if not best_ts:
        return None, None

    strikes = [cd for cd in charm_data if cd['ts'] == best_ts]
    resistance = None  # strongest positive charm above spot
    support = None  # strongest negative charm below spot

    for s in strikes:
        if s['strike'] > spot and s['value'] > 0:
            if resistance is None or s['value'] > resistance['value']:
                resistance = s
        if s['strike'] < spot and s['value'] < 0:
            if support is None or s['value'] < support['value']:  # more negative = stronger
                support = s

    r_strike = resistance['strike'] if resistance else None
    s_strike = support['strike'] if support else None
    return r_strike, s_strike


def compute_limit_price(spot, resistance, support, threshold_pct):
    """Compute limit entry for short at given threshold % of S/R range."""
    if resistance is None or support is None:
        return None
    sr_range = resistance - support
    if sr_range <= 0:
        return None
    pos_pct = (spot - support) / sr_range * 100
    # Only use limit if NOT already in top X% of range
    if pos_pct >= (100 - threshold_pct):
        return None  # already near resistance, use market
    limit_price = resistance - sr_range * (threshold_pct / 100)
    return round(limit_price, 1)


def simulate_trade(entry, ts_fill, setup, sl_dist):
    """Simulate a short trade from entry price, return (outcome, pnl)."""
    initial_stop = entry + sl_dist
    trail_stop = initial_stop
    trail = TRAIL.get(setup)
    fixed_t = FIXED_TARGET.get(setup)
    target_lvl = entry - fixed_t if fixed_t else None

    close_time = market_close(ts_fill)
    max_fav = 0.0
    t1_hit = False

    sim_bars = [b for b in bars if b['ts'] >= ts_fill and b['ts'] <= close_time]

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

    # Expired
    last_close = sim_bars[-1]['close'] if sim_bars else entry
    pnl = entry - last_close
    if t1_hit:
        pnl = round((10.0 + pnl) / 2, 1)
    outcome = 'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'EXPIRED')
    return outcome, round(pnl, 1)


# Test thresholds: 0% (market), 5%, 10%, 15%, 20%, 25%, 30% (current)
thresholds = [0, 5, 10, 15, 20, 25, 30]

print(f"\n{'='*90}")
print(f"CHARM S/R THRESHOLD STUDY: 96 Short Trades (Mar 12-23)")
print(f"{'='*90}")
print(f"0% = market order (no limit). 30% = current production setting.")
print()

all_results = {}

for thresh in thresholds:
    filled_count = 0
    timeout_count = 0
    total_pnl = 0.0
    wins = 0
    losses = 0
    expired = 0
    flips_good = 0  # LOSS->WIN
    flips_bad = 0   # WIN->LOSS
    trade_details = []

    for t in trades:
        ts_fire = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
        spot = float(t['spot'])
        setup = t['setup_name']
        mkt_pnl = float(t['outcome_pnl']) if t['outcome_pnl'] else 0.0
        mkt_outcome = t['outcome_result']

        sl_dist = SL.get(setup, 14)
        if setup == 'BofA Scalp':
            bofa_sl = t.get('bofa_stop_level')
            if bofa_sl and bofa_sl != '':
                sl_dist = abs(float(bofa_sl) - spot)

        if thresh == 0:
            # Market order — use spot as entry, simulate fresh
            outcome, pnl = simulate_trade(spot, ts_fire, setup, sl_dist)
            filled_count += 1
            total_pnl += pnl
            if outcome == 'WIN':
                wins += 1
            elif outcome == 'LOSS':
                losses += 1
            else:
                expired += 1
            trade_details.append({
                'id': t['id'], 'setup': setup, 'entry': spot,
                'outcome': outcome, 'pnl': pnl, 'filled': True
            })
            continue

        # Compute limit price at this threshold
        resistance, support = get_charm_sr(spot, ts_fire)
        if resistance and support:
            limit_price = compute_limit_price(spot, resistance, support, thresh)
        else:
            limit_price = None

        if limit_price is None or limit_price <= spot:
            # No valid limit or already near resistance — use market
            outcome, pnl = simulate_trade(spot, ts_fire, setup, sl_dist)
            filled_count += 1
            total_pnl += pnl
            if outcome == 'WIN':
                wins += 1
            elif outcome == 'LOSS':
                losses += 1
            else:
                expired += 1
            trade_details.append({
                'id': t['id'], 'setup': setup, 'entry': spot,
                'outcome': outcome, 'pnl': pnl, 'filled': True
            })
            continue

        # Check fill in 30-min window using 5-min bar highs
        fill_window_end = ts_fire + timedelta(minutes=30)
        fill_bars = [b for b in bars if b['ts'] >= ts_fire and b['ts'] <= fill_window_end]
        fill_high = max((b['high'] for b in fill_bars), default=spot)

        if fill_high >= limit_price:
            # Filled — find fill time
            fill_time = ts_fire
            for b in fill_bars:
                if b['high'] >= limit_price:
                    fill_time = b['ts']
                    break
            outcome, pnl = simulate_trade(limit_price, fill_time, setup, sl_dist)
            filled_count += 1
            total_pnl += pnl
            if outcome == 'WIN':
                wins += 1
            elif outcome == 'LOSS':
                losses += 1
            else:
                expired += 1

            # Track flips
            if mkt_outcome == 'LOSS' and outcome == 'WIN':
                flips_good += 1
            elif mkt_outcome == 'WIN' and outcome == 'LOSS':
                flips_bad += 1

            trade_details.append({
                'id': t['id'], 'setup': setup, 'entry': limit_price,
                'outcome': outcome, 'pnl': pnl, 'filled': True
            })
        else:
            # Timeout
            timeout_count += 1
            total_pnl += 0  # no trade
            trade_details.append({
                'id': t['id'], 'setup': setup, 'entry': None,
                'outcome': 'TIMEOUT', 'pnl': 0, 'filled': False
            })

    total_trades = filled_count + timeout_count
    wr = wins / filled_count * 100 if filled_count > 0 else 0
    fill_rate = filled_count / total_trades * 100

    all_results[thresh] = {
        'filled': filled_count, 'timeout': timeout_count,
        'pnl': total_pnl, 'wins': wins, 'losses': losses,
        'expired': expired, 'wr': wr, 'fill_rate': fill_rate,
        'flips_good': flips_good, 'flips_bad': flips_bad,
        'details': trade_details,
    }

# Print comparison table
print(f"{'Thresh':>6} | {'Fill Rate':>9} | {'Filled':>6} | {'T/O':>5} | {'WR':>6} | {'W':>3} | {'L':>3} | {'E':>3} | {'PnL':>10} | {'L>W':>4} | {'W>L':>4} | {'vs Mkt':>10}")
print("-" * 100)

mkt_pnl_ref = all_results[0]['pnl']
for thresh in thresholds:
    r = all_results[thresh]
    label = "MARKET" if thresh == 0 else f"{thresh}%"
    vs_mkt = r['pnl'] - mkt_pnl_ref
    current = " <- CURRENT" if thresh == 30 else ""
    print(f"{label:>6} | {r['fill_rate']:>8.0f}% | {r['filled']:>6} | {r['timeout']:>5} | {r['wr']:>5.0f}% | {r['wins']:>3} | {r['losses']:>3} | {r['expired']:>3} | {r['pnl']:>+10.1f} | {r['flips_good']:>4} | {r['flips_bad']:>4} | {vs_mkt:>+10.1f}{current}")

# MaxDD calculation for each threshold
print(f"\n{'Thresh':>6} | {'PnL':>10} | {'MaxDD':>8} | {'PnL/DD':>8} | {'Avg/trade':>10} | {'Recommendation':>20}")
print("-" * 80)
for thresh in thresholds:
    r = all_results[thresh]
    # Compute MaxDD from equity curve of filled trades
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for td in r['details']:
        if td['filled']:
            equity += td['pnl']
        # timeout = 0 pnl, equity unchanged
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    ratio = r['pnl'] / max_dd if max_dd > 0 else float('inf')
    avg = r['pnl'] / r['filled'] if r['filled'] > 0 else 0

    label = "MARKET" if thresh == 0 else f"{thresh}%"
    rec = ""
    if thresh == 0:
        rec = "baseline"
    elif thresh == 30:
        rec = "CURRENT"

    print(f"{label:>6} | {r['pnl']:>+10.1f} | {max_dd:>8.1f} | {ratio:>8.2f} | {avg:>+10.1f} | {rec:>20}")

# Per-setup breakdown for top 3 thresholds
best_thresh = max(thresholds, key=lambda t: all_results[t]['pnl'])
print(f"\nBest PnL threshold: {best_thresh}%")

print(f"\n--- Per-Setup at MARKET vs {best_thresh}% vs 30% (current) ---")
for setup_name in sorted(set(t['setup_name'] for t in trades)):
    setup_trades = [t for t in trades if t['setup_name'] == setup_name]
    print(f"\n  {setup_name} ({len(setup_trades)} trades):")
    for thresh in [0, best_thresh, 30]:
        r = all_results[thresh]
        s_trades = [d for d in r['details'] if d['setup'] == setup_name]
        s_filled = [d for d in s_trades if d['filled']]
        s_pnl = sum(d['pnl'] for d in s_trades)
        s_wins = sum(1 for d in s_filled if d['outcome'] == 'WIN')
        s_wr = s_wins / len(s_filled) * 100 if s_filled else 0
        label = "MARKET" if thresh == 0 else f"{thresh}%"
        print(f"    {label:>6}: PnL={s_pnl:>+8.1f}  Fill={len(s_filled):>2}/{len(s_trades)}  WR={s_wr:.0f}%")
