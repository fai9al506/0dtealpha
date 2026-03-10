"""Vanna Pivot Bounce Backtest - uses historical vanna levels + CVD divergence."""
import psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"


def get_vanna_levels(cur):
    """Pull all vanna snapshots, find dominant levels per timestamp."""
    cur.execute("""
        SELECT ts_utc, expiration_option, strike, value, current_price
        FROM volland_exposure_points
        WHERE greek = 'vanna'
          AND expiration_option IN ('THIS_WEEK', 'THIRTY_NEXT_DAYS')
        ORDER BY ts_utc ASC
    """)
    rows = cur.fetchall()
    print(f"  Vanna rows: {len(rows)}")

    # Group by (ts_utc, expiration_option)
    snapshots = defaultdict(lambda: defaultdict(dict))
    prices = {}
    for r in rows:
        ts = r['ts_utc']
        snapshots[ts][r['expiration_option']][r['strike']] = r['value']
        prices[ts] = r['current_price']

    # For each snapshot, find dominant levels
    levels_timeline = []
    for ts in sorted(snapshots.keys()):
        es_price = prices.get(ts)
        if not es_price:
            continue

        all_strikes = {}
        for tf in ['THIS_WEEK', 'THIRTY_NEXT_DAYS']:
            data = snapshots[ts].get(tf, {})
            if not data:
                continue
            total = sum(abs(v) for v in data.values())
            if total == 0:
                continue
            for strike, val in data.items():
                pct = abs(val) / total * 100
                if pct >= 12:  # dominant threshold
                    if strike not in all_strikes:
                        all_strikes[strike] = {}
                    all_strikes[strike][tf] = (val, pct)

        dominant = []
        for strike, tfs in all_strikes.items():
            confluence = len(tfs) > 1
            best_tf = max(tfs.keys(), key=lambda k: tfs[k][1])
            val, pct = tfs[best_tf]
            dominant.append({
                'strike': float(strike), 'value': float(val), 'timeframe': best_tf,
                'pct': round(float(pct), 1), 'confluence': confluence
            })

        if dominant:
            levels_timeline.append((ts, es_price, dominant))

    return levels_timeline


def get_range_bars(cur):
    """Pull ES range bars for CVD swing analysis."""
    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
               cvd_open, cvd_high, cvd_low, cvd_close, ts_start, ts_end, source
        FROM es_range_bars
        WHERE source = 'live'
        ORDER BY ts_start ASC
    """)
    return cur.fetchall()


def find_swings(bars, pivot_n=2):
    """Find swing highs and lows from range bars."""
    swings = []
    for i in range(pivot_n, len(bars) - pivot_n):
        # Swing low
        is_low = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_low'] > bars[i-j]['bar_low'] or bars[i]['bar_low'] > bars[i+j]['bar_low']:
                is_low = False
                break
        if is_low:
            swings.append({
                'type': 'low', 'price': bars[i]['bar_low'], 'cvd': bars[i]['cvd'],
                'ts': bars[i]['ts_start'], 'bar_idx': i
            })

        # Swing high
        is_high = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_high'] < bars[i-j]['bar_high'] or bars[i]['bar_high'] < bars[i+j]['bar_high']:
                is_high = False
                break
        if is_high:
            swings.append({
                'type': 'high', 'price': bars[i]['bar_high'], 'cvd': bars[i]['cvd'],
                'ts': bars[i]['ts_start'], 'bar_idx': i
            })

    swings.sort(key=lambda s: s['ts'])
    return swings


def detect_cvd_divergences(bars, swings):
    """Find CVD divergence points (exhaustion + absorption patterns)."""
    divs = []
    lows = [s for s in swings if s['type'] == 'low']
    highs = [s for s in swings if s['type'] == 'high']

    # Sell exhaustion: lower low + higher CVD -> LONG
    for i in range(1, len(lows)):
        prev, curr = lows[i-1], lows[i]
        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({
                'type': 'sell_exhaustion', 'direction': 'long',
                'price': curr['price'], 'ts': curr['ts'], 'bar_idx': curr['bar_idx'],
                'price_diff': curr['price'] - prev['price'],
                'cvd_diff': curr['cvd'] - prev['cvd']
            })

    # Sell absorption: higher low + lower CVD -> LONG
    for i in range(1, len(lows)):
        prev, curr = lows[i-1], lows[i]
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({
                'type': 'sell_absorption', 'direction': 'long',
                'price': curr['price'], 'ts': curr['ts'], 'bar_idx': curr['bar_idx'],
                'price_diff': curr['price'] - prev['price'],
                'cvd_diff': curr['cvd'] - prev['cvd']
            })

    # Buy exhaustion: higher high + lower CVD -> SHORT
    for i in range(1, len(highs)):
        prev, curr = highs[i-1], highs[i]
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({
                'type': 'buy_exhaustion', 'direction': 'short',
                'price': curr['price'], 'ts': curr['ts'], 'bar_idx': curr['bar_idx'],
                'price_diff': curr['price'] - prev['price'],
                'cvd_diff': curr['cvd'] - prev['cvd']
            })

    # Buy absorption: lower high + higher CVD -> SHORT
    for i in range(1, len(highs)):
        prev, curr = highs[i-1], highs[i]
        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({
                'type': 'buy_absorption', 'direction': 'short',
                'price': curr['price'], 'ts': curr['ts'], 'bar_idx': curr['bar_idx'],
                'price_diff': curr['price'] - prev['price'],
                'cvd_diff': curr['cvd'] - prev['cvd']
            })

    divs.sort(key=lambda d: d['ts'])
    return divs


def run_backtest(levels_timeline, divs, bars, proximity_pts=15, stop_pts=8, target_pts=10):
    """Match vanna levels + CVD divergence -> simulate trades."""
    trades = []
    position = None
    cooldown_until = None
    bars_sorted = sorted(bars, key=lambda b: b['ts_start'])

    for div in divs:
        div_ts = div['ts']
        div_price = div['price']

        if cooldown_until and div_ts < cooldown_until:
            continue

        # Resolve open position first
        if position and not position.get('result'):
            entry = position['entry_price']
            direction = position['direction']
            for b in bars_sorted:
                if b['ts_start'] <= position['entry_ts']:
                    continue
                if b['ts_start'] > div_ts:
                    break
                if direction == 'long':
                    if b['bar_high'] >= entry + target_pts:
                        position['result'] = 'WIN'
                        position['pnl'] = target_pts
                        break
                    if b['bar_low'] <= entry - stop_pts:
                        position['result'] = 'LOSS'
                        position['pnl'] = -stop_pts
                        break
                else:
                    if b['bar_low'] <= entry - target_pts:
                        position['result'] = 'WIN'
                        position['pnl'] = target_pts
                        break
                    if b['bar_high'] >= entry + stop_pts:
                        position['result'] = 'LOSS'
                        position['pnl'] = -stop_pts
                        break
                bar_et = b['ts_start'] + timedelta(hours=-5)
                if bar_et.time() >= dtime(15, 50):
                    position['pnl'] = (b['bar_close'] - entry) if direction == 'long' else (entry - b['bar_close'])
                    position['result'] = 'WIN' if position['pnl'] > 0 else 'LOSS'
                    break

            if position.get('result'):
                trades.append(position)
                cooldown_until = div_ts + timedelta(minutes=15)
                position = None
            else:
                continue  # still in position, skip

        # Find nearest vanna snapshot before this divergence
        nearest_levels = None
        for ts, es_price, levels in reversed(levels_timeline):
            if ts <= div_ts:
                nearest_levels = levels
                break

        if not nearest_levels:
            continue

        # Check if divergence price is near a dominant vanna level with direction agreement
        for lvl in nearest_levels:
            strike = lvl['strike']
            dist = abs(div_price - strike)
            if dist > proximity_pts:
                continue

            vanna_positive = lvl['value'] > 0
            if div['direction'] == 'long' and not vanna_positive:
                continue
            if div['direction'] == 'short' and vanna_positive:
                continue

            trade_date = (div_ts + timedelta(hours=-5)).date()
            position = {
                'entry_ts': div_ts, 'entry_price': div_price,
                'direction': div['direction'], 'pattern': div['type'],
                'vanna_strike': strike, 'vanna_value': lvl['value'],
                'vanna_pct': lvl['pct'], 'vanna_tf': lvl['timeframe'],
                'confluence': lvl['confluence'], 'proximity': round(dist, 1),
                'trade_date': trade_date
            }
            break

    # Resolve final position
    if position and not position.get('result'):
        entry = position['entry_price']
        direction = position['direction']
        for b in bars_sorted:
            if b['ts_start'] <= position['entry_ts']:
                continue
            if direction == 'long':
                if b['bar_high'] >= entry + target_pts:
                    position['result'] = 'WIN'
                    position['pnl'] = target_pts
                    break
                if b['bar_low'] <= entry - stop_pts:
                    position['result'] = 'LOSS'
                    position['pnl'] = -stop_pts
                    break
            else:
                if b['bar_low'] <= entry - target_pts:
                    position['result'] = 'WIN'
                    position['pnl'] = target_pts
                    break
                if b['bar_high'] >= entry + stop_pts:
                    position['result'] = 'LOSS'
                    position['pnl'] = -stop_pts
                    break
            bar_et = b['ts_start'] + timedelta(hours=-5)
            if bar_et.time() >= dtime(15, 50):
                position['pnl'] = (b['bar_close'] - entry) if direction == 'long' else (entry - b['bar_close'])
                position['result'] = 'WIN' if position['pnl'] > 0 else 'LOSS'
                break
        if not position.get('result'):
            position['result'] = 'EXPIRED'
            position['pnl'] = 0
        trades.append(position)

    return trades


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("Loading data...")
    levels_timeline = get_vanna_levels(cur)
    print(f"  Vanna snapshots with dominant levels: {len(levels_timeline)}")

    bars = get_range_bars(cur)
    print(f"  Range bars: {len(bars)}")

    conn.close()

    if not bars:
        print("No range bars found!")
        return

    print("Finding swings...")
    swings = find_swings(bars)
    print(f"  Swings found: {len(swings)}")

    print("Detecting CVD divergences...")
    divs = detect_cvd_divergences(bars, swings)
    print(f"  Divergences found: {len(divs)}")

    div_types = defaultdict(int)
    for d in divs:
        div_types[d['type']] += 1
    for t, n in sorted(div_types.items()):
        print(f"    {t}: {n}")

    print(f"\n{'='*75}")
    print(f"  VANNA PIVOT BOUNCE BACKTEST")
    print(f"{'='*75}")

    configs = [
        (15, 8, 10, "15pt prox, 8 SL, 10 TP"),
        (10, 8, 10, "10pt prox, 8 SL, 10 TP"),
        (20, 8, 10, "20pt prox, 8 SL, 10 TP"),
        (15, 5, 8,  "15pt prox, 5 SL, 8 TP"),
        (15, 10, 12, "15pt prox, 10 SL, 12 TP"),
        (15, 12, 10, "15pt prox, 12 SL, 10 TP (wide stop)"),
    ]

    best_config = None
    best_pts = -999

    for prox, sl, tp, label in configs:
        trades = run_backtest(levels_timeline, divs, bars, prox, sl, tp)
        if not trades:
            print(f"\n  {label}: NO TRADES")
            continue

        wins = sum(1 for t in trades if t['result'] == 'WIN')
        losses = sum(1 for t in trades if t['result'] == 'LOSS')
        total = len(trades)
        wr = wins / total * 100 if total else 0
        total_pts = sum(t['pnl'] for t in trades)
        avg_pnl = total_pts / total if total else 0

        if total_pts > best_pts:
            best_pts = total_pts
            best_config = label

        by_pattern = defaultdict(lambda: {'n': 0, 'w': 0, 'pts': 0})
        for t in trades:
            p = by_pattern[t['pattern']]
            p['n'] += 1
            if t['result'] == 'WIN':
                p['w'] += 1
            p['pts'] += t['pnl']

        conf_trades = [t for t in trades if t.get('confluence')]
        conf_wins = sum(1 for t in conf_trades if t['result'] == 'WIN')

        daily = defaultdict(float)
        for t in trades:
            daily[t['trade_date']] += t['pnl']

        print(f"\n  {label}")
        print(f"  Trades: {total}  W: {wins}  L: {losses}  WR: {wr:.1f}%")
        print(f"  Total: {total_pts:+.1f} pts  Avg: {avg_pnl:+.1f} pts/trade")
        if conf_trades:
            cwr = conf_wins / len(conf_trades) * 100
            print(f"  Confluence trades: {len(conf_trades)}  WR: {cwr:.1f}%")

        print(f"  By pattern:")
        for name in ['sell_exhaustion', 'sell_absorption', 'buy_exhaustion', 'buy_absorption']:
            if name in by_pattern:
                p = by_pattern[name]
                pwr = p['w'] / p['n'] * 100 if p['n'] else 0
                print(f"    {name:<20} N={p['n']:>3}  WR={pwr:>5.1f}%  {p['pts']:>+7.1f} pts")

        print(f"  Daily P&L:")
        cum = 0
        for d in sorted(daily):
            cum += daily[d]
            print(f"    {d}: {daily[d]:>+7.1f} pts  cum: {cum:>+7.1f}")

    # Show sample trades from best/default config
    print(f"\n{'='*75}")
    print(f"  SAMPLE TRADES (15pt prox, 8 SL, 10 TP)")
    print(f"{'='*75}")
    trades = run_backtest(levels_timeline, divs, bars, 15, 8, 10)
    for t in trades[:25]:
        conf = " [CONF]" if t.get('confluence') else ""
        ts_str = t['entry_ts'].strftime('%H:%M') if t['entry_ts'] else '??:??'
        print(f"  {t['trade_date']} {ts_str} UTC "
              f"{t['direction']:>5} @ {t['entry_price']:.0f} "
              f"vanna={t['vanna_strike']:.0f} ({t['vanna_tf'][:4]}, {t['vanna_pct']:.0f}%) "
              f"dist={t['proximity']:.0f}pt "
              f"{t['pattern']:<18} -> {t['result']} {t['pnl']:+.1f}pt{conf}")

    print(f"\n  Best config: {best_config} ({best_pts:+.1f} pts)")


if __name__ == '__main__':
    main()
