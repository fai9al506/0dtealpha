"""Understand the gap between VP backtest (94.4% WR) and live simulation (14% WR).

Systematically compares:
1. Data source: live vs rithmic bars
2. Cross-day vs intra-day swing detection
3. Serial position vs cooldown-based signal count
4. Time window filtering
"""
import psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
import os

DB_URL = os.environ.get('DATABASE_URL')

# ── Shared functions (identical to original backtest) ──

def find_swings(bars, pivot_n=2):
    swings = []
    for i in range(pivot_n, len(bars) - pivot_n):
        is_low = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_low'] > bars[i-j]['bar_low'] or bars[i]['bar_low'] > bars[i+j]['bar_low']:
                is_low = False
                break
        if is_low:
            swings.append({'type': 'low', 'price': bars[i]['bar_low'], 'cvd': bars[i]['cvd'],
                          'ts': bars[i]['ts_start'], 'bar_idx': i})
        is_high = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_high'] < bars[i-j]['bar_high'] or bars[i]['bar_high'] < bars[i+j]['bar_high']:
                is_high = False
                break
        if is_high:
            swings.append({'type': 'high', 'price': bars[i]['bar_high'], 'cvd': bars[i]['cvd'],
                          'ts': bars[i]['ts_start'], 'bar_idx': i})
    swings.sort(key=lambda s: s['ts'])
    return swings


def detect_divs(bars, swings):
    divs = []
    lows = [s for s in swings if s['type'] == 'low']
    highs = [s for s in swings if s['type'] == 'high']
    for i in range(1, len(lows)):
        prev, curr = lows[i-1], lows[i]
        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({'type': 'sell_exhaustion', 'direction': 'long', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx']})
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'type': 'sell_absorption', 'direction': 'long', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx']})
    for i in range(1, len(highs)):
        prev, curr = highs[i-1], highs[i]
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'type': 'buy_exhaustion', 'direction': 'short', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx']})
        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({'type': 'buy_absorption', 'direction': 'short', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx']})
    divs.sort(key=lambda d: d['ts'])
    return divs


def get_vanna_levels(cur, date_filter=None):
    """Get vanna level timeline."""
    where = ""
    if date_filter:
        where = f"AND ts_utc::date >= '{date_filter[0]}' AND ts_utc::date <= '{date_filter[1]}'"
    cur.execute(f"""
        SELECT ts_utc, expiration_option, strike, value, current_price
        FROM volland_exposure_points
        WHERE greek = 'vanna'
          AND expiration_option IN ('THIS_WEEK', 'THIRTY_NEXT_DAYS')
          {where}
        ORDER BY ts_utc ASC
    """)
    rows = cur.fetchall()

    snapshots = defaultdict(lambda: defaultdict(dict))
    prices = {}
    for r in rows:
        ts = r['ts_utc']
        snapshots[ts][r['expiration_option']][r['strike']] = r['value']
        prices[ts] = r['current_price']

    levels_timeline = []
    for ts in sorted(snapshots.keys()):
        es_price = prices.get(ts)
        if not es_price:
            continue
        all_strikes = {}
        for tf in ['THIS_WEEK', 'THIRTY_NEXT_DAYS']:
            data = snapshots[ts].get(tf, {})
            if not data: continue
            total = sum(abs(v) for v in data.values())
            if total == 0: continue
            for strike, val in data.items():
                pct = abs(val) / total * 100
                if pct >= 12:
                    if strike not in all_strikes:
                        all_strikes[strike] = {}
                    all_strikes[strike][tf] = (val, pct)
        dominant = []
        for strike, tfs in all_strikes.items():
            confluence = len(tfs) > 1
            best_tf = max(tfs.keys(), key=lambda k: tfs[k][1])
            val, pct = tfs[best_tf]
            dominant.append({'strike': float(strike), 'value': float(val),
                           'timeframe': best_tf, 'pct': round(float(pct), 1), 'confluence': confluence})
        if dominant:
            levels_timeline.append((ts, es_price, dominant))

    return levels_timeline


def run_backtest_original(levels_timeline, divs, bars, stop_pts=8, target_pts=10, proximity_pts=15):
    """Original backtest: serial position, cross-day swings, no time filter."""
    trades = []
    position = None
    cooldown_until = None
    bars_sorted = sorted(bars, key=lambda b: b['ts_start'])

    for div in divs:
        div_ts = div['ts']
        div_price = div['price']

        if cooldown_until and div_ts < cooldown_until:
            continue

        # Resolve open position
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
                        position['result'] = 'WIN'; position['pnl'] = target_pts; break
                    if b['bar_low'] <= entry - stop_pts:
                        position['result'] = 'LOSS'; position['pnl'] = -stop_pts; break
                else:
                    if b['bar_low'] <= entry - target_pts:
                        position['result'] = 'WIN'; position['pnl'] = target_pts; break
                    if b['bar_high'] >= entry + stop_pts:
                        position['result'] = 'LOSS'; position['pnl'] = -stop_pts; break
            if position.get('result'):
                trades.append(position)
                cooldown_until = div_ts + timedelta(minutes=15)
                position = None
            else:
                continue  # still in position

        nearest_levels = None
        for ts, es_price, levels in reversed(levels_timeline):
            if ts <= div_ts:
                nearest_levels = levels; break
        if not nearest_levels:
            continue

        for lvl in nearest_levels:
            strike = lvl['strike']
            dist = abs(div_price - strike)
            if dist > proximity_pts: continue
            if lvl['value'] > 0 and div['direction'] != 'long': continue
            if lvl['value'] < 0 and div['direction'] != 'short': continue

            trade_date = (div_ts + timedelta(hours=-5)).date()
            position = {
                'entry_ts': div_ts, 'entry_price': div_price,
                'direction': div['direction'], 'pattern': div['type'],
                'vanna_strike': strike, 'trade_date': trade_date,
            }
            break

    # Resolve final
    if position and not position.get('result'):
        entry = position['entry_price']
        direction = position['direction']
        for b in bars_sorted:
            if b['ts_start'] <= position['entry_ts']: continue
            if direction == 'long':
                if b['bar_high'] >= entry + target_pts:
                    position['result'] = 'WIN'; position['pnl'] = target_pts; break
                if b['bar_low'] <= entry - stop_pts:
                    position['result'] = 'LOSS'; position['pnl'] = -stop_pts; break
            else:
                if b['bar_low'] <= entry - target_pts:
                    position['result'] = 'WIN'; position['pnl'] = target_pts; break
                if b['bar_high'] >= entry + stop_pts:
                    position['result'] = 'LOSS'; position['pnl'] = -stop_pts; break
        if not position.get('result'):
            position['result'] = 'EXPIRED'; position['pnl'] = 0
        trades.append(position)

    return trades


def run_backtest_daily(levels_timeline, all_bars_by_date, stop_pts=8, target_pts=10, proximity_pts=15):
    """Intra-day only: swings/divs reset each day, cooldown-based (like live)."""
    trades = []

    for trade_date, bars in sorted(all_bars_by_date.items()):
        if len(bars) < 10:
            continue

        swings = find_swings(bars)
        divs = detect_divs(bars, swings)

        cooldown = {'long': None, 'short': None}

        for div in divs:
            div_ts = div['ts']
            div_price = div['price']

            # Time filter (10:00-15:30 ET)
            et_hour = (div_ts - timedelta(hours=5)).hour
            et_min = (div_ts - timedelta(hours=5)).minute
            et_time = dtime(et_hour, et_min)
            if et_time < dtime(10, 0) or et_time > dtime(15, 30):
                continue

            cd_key = div['direction']
            if cooldown[cd_key] and div_ts < cooldown[cd_key]:
                continue

            # Match vanna
            nearest_levels = None
            for ts, es_price, levels in reversed(levels_timeline):
                if ts <= div_ts:
                    nearest_levels = levels; break
            if not nearest_levels:
                continue

            matched = False
            for lvl in nearest_levels:
                strike = lvl['strike']
                dist = abs(div_price - strike)
                if dist > proximity_pts: continue
                if lvl['value'] > 0 and div['direction'] != 'long': continue
                if lvl['value'] < 0 and div['direction'] != 'short': continue
                matched = True

                entry = div_price
                is_long = div['direction'] == 'long'
                target = entry + target_pts if is_long else entry - target_pts
                stop = entry - stop_pts if is_long else entry + stop_pts

                result = 'EXPIRED'
                pnl = 0
                for b in bars:
                    if b['ts_start'] <= div_ts: continue
                    if is_long:
                        if b['bar_low'] <= stop: result = 'LOSS'; pnl = -stop_pts; break
                        if b['bar_high'] >= target: result = 'WIN'; pnl = target_pts; break
                    else:
                        if b['bar_high'] >= stop: result = 'LOSS'; pnl = -stop_pts; break
                        if b['bar_low'] <= target: result = 'WIN'; pnl = target_pts; break

                if result == 'EXPIRED':
                    exit_p = bars[-1]['bar_close']
                    pnl = (exit_p - entry) if is_long else (entry - exit_p)

                trades.append({
                    'trade_date': trade_date, 'entry_ts': div_ts,
                    'direction': div['direction'], 'entry_price': entry,
                    'result': result, 'pnl': pnl, 'pattern': div['type'],
                    'vanna_strike': strike,
                })
                cooldown[cd_key] = div_ts + timedelta(minutes=15)
                break

    return trades


def print_results(trades, label):
    if not trades:
        print(f"\n  {label}: NO TRADES")
        return

    wins = sum(1 for t in trades if t['result'] == 'WIN')
    losses = sum(1 for t in trades if t['result'] == 'LOSS')
    total = len(trades)
    wr = wins / total * 100 if total else 0
    total_pts = sum(t['pnl'] for t in trades)
    avg = total_pts / total if total else 0

    daily = defaultdict(lambda: {'n': 0, 'w': 0, 'pts': 0})
    for t in trades:
        d = daily[t['trade_date']]
        d['n'] += 1
        if t['result'] == 'WIN': d['w'] += 1
        d['pts'] += t['pnl']

    print(f"\n  {label}")
    print(f"  Trades: {total}  W: {wins}  L: {losses}  WR: {wr:.1f}%  Total: {total_pts:+.1f} pts  Avg: {avg:+.1f}", flush=True)
    print(f"  Daily breakdown:", flush=True)
    cum = 0
    for d in sorted(daily):
        dd = daily[d]
        cum += dd['pts']
        dwr = dd['w'] / dd['n'] * 100 if dd['n'] else 0
        print(f"    {d}: {dd['n']} trades, {dd['w']}W, WR={dwr:.0f}%, {dd['pts']:+.1f} pts, cum={cum:+.1f}", flush=True)


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ── 1. Check data source difference ──
    print("="*70, flush=True)
    print("  GAP ANALYSIS: VP Backtest vs Live", flush=True)
    print("="*70, flush=True)

    # Check bar counts per date per source
    cur.execute("""
        SELECT trade_date, source, count(*) as cnt,
               min(bar_low) as lo, max(bar_high) as hi
        FROM es_range_bars
        WHERE trade_date >= '2026-02-19' AND trade_date <= '2026-03-06'
        GROUP BY trade_date, source
        ORDER BY trade_date, source
    """)
    print("\n--- Bar counts by date and source ---", flush=True)
    for r in cur.fetchall():
        print(f"  {r['trade_date']} {r['source']:8s} {r['cnt']:>4} bars  "
              f"range: {r['lo']:.0f}-{r['hi']:.0f}", flush=True)

    # ── 2. Reproduce original backtest (Feb 19 - Mar 2, live bars) ──
    print("\n" + "="*70, flush=True)
    print("  TEST A: Original backtest (live bars, cross-day swings)", flush=True)
    print("="*70, flush=True)

    levels_timeline = get_vanna_levels(cur, ('2026-02-19', '2026-03-06'))
    print(f"  Vanna snapshots: {len(levels_timeline)}", flush=True)

    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
               ts_start, ts_end, source, trade_date
        FROM es_range_bars
        WHERE source = 'live' AND trade_date >= '2026-02-19' AND trade_date <= '2026-03-02'
        ORDER BY ts_start ASC
    """)
    live_bars = cur.fetchall()
    print(f"  Live bars (Feb 19 - Mar 2): {len(live_bars)}", flush=True)

    if live_bars:
        swings_live = find_swings(live_bars)
        divs_live = detect_divs(live_bars, swings_live)
        print(f"  Swings: {len(swings_live)}, Divergences: {len(divs_live)}", flush=True)
        trades_a = run_backtest_original(levels_timeline, divs_live, live_bars)
        print_results(trades_a, "A: Original (live, cross-day, serial position)")

    # ── 3. Same period, rithmic bars, cross-day swings ──
    print("\n" + "="*70, flush=True)
    print("  TEST B: Rithmic bars, cross-day swings (same method as A)", flush=True)
    print("="*70, flush=True)

    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
               ts_start, ts_end, source, trade_date
        FROM es_range_bars
        WHERE source = 'rithmic' AND trade_date >= '2026-02-19' AND trade_date <= '2026-03-02'
        ORDER BY ts_start ASC
    """)
    rithmic_bars = cur.fetchall()
    print(f"  Rithmic bars (Feb 19 - Mar 2): {len(rithmic_bars)}", flush=True)

    if rithmic_bars:
        swings_rith = find_swings(rithmic_bars)
        divs_rith = detect_divs(rithmic_bars, swings_rith)
        print(f"  Swings: {len(swings_rith)}, Divergences: {len(divs_rith)}", flush=True)
        trades_b = run_backtest_original(levels_timeline, divs_rith, rithmic_bars)
        print_results(trades_b, "B: Rithmic, cross-day, serial position")

    # ── 4. Rithmic bars, intra-day swings, cooldown-based (like live production) ──
    print("\n" + "="*70, flush=True)
    print("  TEST C: Rithmic bars, intra-day swings, cooldown (like live)", flush=True)
    print("="*70, flush=True)

    # Group rithmic bars by trade_date
    rith_by_date = defaultdict(list)
    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
               ts_start, ts_end, source, trade_date
        FROM es_range_bars
        WHERE source = 'rithmic' AND trade_date >= '2026-02-19' AND trade_date <= '2026-03-02'
        ORDER BY trade_date, bar_idx ASC
    """)
    for r in cur.fetchall():
        rith_by_date[r['trade_date']].append(r)

    trades_c = run_backtest_daily(levels_timeline, rith_by_date)
    print_results(trades_c, "C: Rithmic, intra-day, cooldown (like live)")

    # ── 5. Extended: same as C but for Mar 5-6 ──
    print("\n" + "="*70, flush=True)
    print("  TEST D: Rithmic bars, intra-day, cooldown — Mar 5-6 only", flush=True)
    print("="*70, flush=True)

    rith_by_date2 = defaultdict(list)
    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
               ts_start, ts_end, source, trade_date
        FROM es_range_bars
        WHERE source = 'rithmic' AND trade_date IN ('2026-03-05', '2026-03-06')
        ORDER BY trade_date, bar_idx ASC
    """)
    for r in cur.fetchall():
        rith_by_date2[r['trade_date']].append(r)

    trades_d = run_backtest_daily(levels_timeline, rith_by_date2)
    print_results(trades_d, "D: Rithmic, intra-day, cooldown — Mar 5-6")

    # ── 6. Live bars, intra-day swings (isolate data source effect) ──
    print("\n" + "="*70, flush=True)
    print("  TEST E: Live bars, intra-day swings, cooldown", flush=True)
    print("="*70, flush=True)

    live_by_date = defaultdict(list)
    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
               ts_start, ts_end, source, trade_date
        FROM es_range_bars
        WHERE source = 'live' AND trade_date >= '2026-02-19' AND trade_date <= '2026-03-02'
        ORDER BY trade_date, bar_idx ASC
    """)
    for r in cur.fetchall():
        live_by_date[r['trade_date']].append(r)

    trades_e = run_backtest_daily(levels_timeline, live_by_date)
    print_results(trades_e, "E: Live, intra-day, cooldown")

    # ── 7. Divergence quality analysis ──
    print("\n" + "="*70, flush=True)
    print("  DIVERGENCE QUALITY COMPARISON", flush=True)
    print("="*70, flush=True)

    for source_name, bar_groups in [("live", live_by_date), ("rithmic", rith_by_date)]:
        total_divs = 0
        total_swings = 0
        for d, bars in sorted(bar_groups.items()):
            if len(bars) < 10: continue
            sw = find_swings(bars)
            dv = detect_divs(bars, sw)
            total_swings += len(sw)
            total_divs += len(dv)
            print(f"  {source_name} {d}: {len(bars)} bars, {len(sw)} swings, {len(dv)} divs", flush=True)
        print(f"  {source_name} TOTAL: {total_swings} swings, {total_divs} divs", flush=True)
        print(flush=True)

    conn.close()
    print("\nDone.", flush=True)


if __name__ == '__main__':
    main()
