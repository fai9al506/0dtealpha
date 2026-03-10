"""Is the TS VP edge real? Test 3 hypotheses:
1. TS CVD divergence at vanna levels = the VP signal (18 trades, 100% WR claimed)
2. TS CVD divergence at ANY level (no vanna) = is vanna even needed?
3. TS CVD divergence at random levels = is it just the CVD signal?

If #2 and #3 match #1, vanna is irrelevant — the edge is purely TS CVD artifacts.
"""
import psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
import os, random

DB_URL = os.environ.get('DATABASE_URL')
random.seed(42)


def find_swings(bars, pivot_n=2):
    swings = []
    for i in range(pivot_n, len(bars) - pivot_n):
        is_low = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_low'] > bars[i-j]['bar_low'] or bars[i]['bar_low'] > bars[i+j]['bar_low']:
                is_low = False; break
        if is_low:
            swings.append({'type': 'low', 'price': bars[i]['bar_low'], 'cvd': bars[i]['cvd'],
                          'ts': bars[i]['ts_start'], 'bar_idx': i})
        is_high = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_high'] < bars[i-j]['bar_high'] or bars[i]['bar_high'] < bars[i+j]['bar_high']:
                is_high = False; break
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
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx'],
                        'cvd_gap': abs(curr['cvd'] - prev['cvd']),
                        'price_gap': abs(curr['price'] - prev['price'])})
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'type': 'sell_absorption', 'direction': 'long', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx'],
                        'cvd_gap': abs(curr['cvd'] - prev['cvd']),
                        'price_gap': abs(curr['price'] - prev['price'])})
    for i in range(1, len(highs)):
        prev, curr = highs[i-1], highs[i]
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'type': 'buy_exhaustion', 'direction': 'short', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx'],
                        'cvd_gap': abs(curr['cvd'] - prev['cvd']),
                        'price_gap': abs(curr['price'] - prev['price'])})
        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({'type': 'buy_absorption', 'direction': 'short', 'price': curr['price'],
                        'ts': curr['ts'], 'bar_idx': curr['bar_idx'],
                        'cvd_gap': abs(curr['cvd'] - prev['cvd']),
                        'price_gap': abs(curr['price'] - prev['price'])})
    divs.sort(key=lambda d: d['ts'])
    return divs


def get_vanna_timeline(cur, start_date, end_date):
    cur.execute("""
        SELECT ts_utc, expiration_option, strike, value
        FROM volland_exposure_points
        WHERE greek = 'vanna'
          AND expiration_option IN ('THIS_WEEK', 'THIRTY_NEXT_DAYS')
          AND ts_utc::date >= %s AND ts_utc::date <= %s
        ORDER BY ts_utc ASC
    """, (start_date, end_date))
    rows = cur.fetchall()
    snapshots = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        snapshots[r['ts_utc']][r['expiration_option']][r['strike']] = r['value']
    timeline = []
    for ts in sorted(snapshots.keys()):
        all_strikes = {}
        for tf in ['THIS_WEEK', 'THIRTY_NEXT_DAYS']:
            data = snapshots[ts].get(tf, {})
            if not data: continue
            total = sum(abs(v) for v in data.values())
            if total == 0: continue
            for strike, val in data.items():
                pct = abs(val) / total * 100
                if pct >= 12:
                    s = float(strike)
                    if s not in all_strikes or pct > all_strikes[s]['pct']:
                        all_strikes[s] = {'strike': s, 'value': float(val), 'pct': round(pct, 1)}
        if all_strikes:
            timeline.append((ts, list(all_strikes.values())))
    return timeline


def sim_trades(divs, bars, stop=8, target=10):
    """Simulate trades from divergences. Returns list of results."""
    trades = []
    for d in divs:
        entry = d['price']
        is_long = d['direction'] == 'long'
        tgt = entry + target if is_long else entry - target
        stp = entry - stop if is_long else entry + stop
        result = 'EXPIRED'
        pnl = 0
        for j in range(d['bar_idx'] + 1, len(bars)):
            fb = bars[j]
            hi = float(fb['bar_high'])
            lo = float(fb['bar_low'])
            if is_long:
                if lo <= stp: result = 'LOSS'; pnl = -stop; break
                if hi >= tgt: result = 'WIN'; pnl = target; break
            else:
                if hi >= stp: result = 'LOSS'; pnl = -stop; break
                if lo <= tgt: result = 'WIN'; pnl = target; break
        if result == 'EXPIRED':
            exit_p = float(bars[-1]['bar_close'])
            pnl = (exit_p - entry) if is_long else (entry - exit_p)
        trades.append({'result': result, 'pnl': pnl, 'direction': d['direction'],
                      'entry': entry, 'bar_idx': d['bar_idx'],
                      'cvd_gap': d.get('cvd_gap', 0), 'price_gap': d.get('price_gap', 0)})
    return trades


def print_summary(trades, label):
    if not trades:
        print(f"  {label}: NO TRADES", flush=True)
        return
    wins = sum(1 for t in trades if t['result'] == 'WIN')
    losses = sum(1 for t in trades if t['result'] == 'LOSS')
    n = len(trades)
    wr = wins / n * 100
    pts = sum(t['pnl'] for t in trades)
    print(f"  {label}: {n} trades, {wins}W/{losses}L, WR={wr:.1f}%, {pts:+.1f} pts, avg={pts/n:+.1f}", flush=True)


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get dates with live bars
    cur.execute("""
        SELECT DISTINCT trade_date FROM es_range_bars
        WHERE source = 'live'
        ORDER BY trade_date
    """)
    live_dates = [r['trade_date'] for r in cur.fetchall()]
    print(f"Live bar dates: {live_dates}", flush=True)

    vanna_tl = get_vanna_timeline(cur, str(live_dates[0]), str(live_dates[-1]))
    print(f"Vanna snapshots: {len(vanna_tl)}\n", flush=True)

    # ══════════════════════════════════════════════════
    #  PART 1: Compare TS vs Rithmic CVD divergence at ALL levels (no vanna)
    # ══════════════════════════════════════════════════
    print("="*65, flush=True)
    print("  PART 1: ALL CVD divergences (no vanna filter), cooldown 15min", flush=True)
    print("="*65, flush=True)

    for source in ['live', 'rithmic']:
        all_trades = []
        for trade_date in live_dates:
            cur.execute("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
                       ts_start, ts_end
                FROM es_range_bars
                WHERE source = %s AND trade_date = %s AND status = 'closed'
                ORDER BY bar_idx ASC
            """, (source, str(trade_date)))
            bars = cur.fetchall()
            if len(bars) < 15:
                continue

            swings = find_swings(bars)
            divs = detect_divs(bars, swings)

            # Time filter + cooldown
            cooldown = {'long': None, 'short': None}
            filtered = []
            for d in divs:
                ts = d['ts']
                if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                    ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
                else:
                    ts_utc = ts.replace(tzinfo=None)
                et = ts_utc - timedelta(hours=5)
                et_time = dtime(et.hour, et.minute)
                if et_time < dtime(10, 0) or et_time > dtime(15, 30):
                    continue
                cd = d['direction']
                if cooldown[cd] and ts < cooldown[cd]:
                    continue
                filtered.append(d)
                cooldown[cd] = ts + timedelta(minutes=15)

            trades = sim_trades(filtered, bars)
            all_trades.extend(trades)

        print_summary(all_trades, f"{source} ALL divs")

        # By direction
        longs = [t for t in all_trades if t['direction'] == 'long']
        shorts = [t for t in all_trades if t['direction'] == 'short']
        print_summary(longs, f"  {source} LONG")
        print_summary(shorts, f"  {source} SHORT")

    # ══════════════════════════════════════════════════
    #  PART 2: CVD divergence at vanna levels only (the VP signal)
    # ══════════════════════════════════════════════════
    print(f"\n{'='*65}", flush=True)
    print("  PART 2: CVD divergence AT VANNA LEVELS only (VP signal)", flush=True)
    print("="*65, flush=True)

    for source in ['live', 'rithmic']:
        all_trades = []
        for trade_date in live_dates:
            cur.execute("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
                       ts_start, ts_end
                FROM es_range_bars
                WHERE source = %s AND trade_date = %s AND status = 'closed'
                ORDER BY bar_idx ASC
            """, (source, str(trade_date)))
            bars = cur.fetchall()
            if len(bars) < 15:
                continue

            swings = find_swings(bars)
            divs = detect_divs(bars, swings)

            cooldown = {'long': None, 'short': None}
            for d in divs:
                ts = d['ts']
                if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                    ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
                else:
                    ts_utc = ts.replace(tzinfo=None)
                et = ts_utc - timedelta(hours=5)
                et_time = dtime(et.hour, et.minute)
                if et_time < dtime(10, 0) or et_time > dtime(15, 30):
                    continue
                cd = d['direction']
                if cooldown[cd] and ts < cooldown[cd]:
                    continue

                # Match vanna level
                nearest = None
                for t, levels in reversed(vanna_tl):
                    if t <= ts:
                        nearest = levels; break
                if not nearest:
                    continue

                matched = False
                for lv in nearest:
                    dist = abs(d['price'] - lv['strike'])
                    if dist > 15: continue
                    if lv['value'] > 0 and d['direction'] != 'long': continue
                    if lv['value'] < 0 and d['direction'] != 'short': continue
                    matched = True; break

                if not matched:
                    continue

                cooldown[cd] = ts + timedelta(minutes=15)
                trades = sim_trades([d], bars)
                all_trades.extend(trades)

        print_summary(all_trades, f"{source} VP (vanna+CVD)")

    # ══════════════════════════════════════════════════
    #  PART 3: CVD divergence quality — does bigger CVD gap = better WR?
    # ══════════════════════════════════════════════════
    print(f"\n{'='*65}", flush=True)
    print("  PART 3: CVD divergence quality — bigger gap = better?", flush=True)
    print("="*65, flush=True)

    for source in ['live', 'rithmic']:
        all_trades_with_gap = []
        for trade_date in live_dates:
            cur.execute("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
                       ts_start, ts_end
                FROM es_range_bars
                WHERE source = %s AND trade_date = %s AND status = 'closed'
                ORDER BY bar_idx ASC
            """, (source, str(trade_date)))
            bars = cur.fetchall()
            if len(bars) < 15:
                continue

            swings = find_swings(bars)
            divs = detect_divs(bars, swings)

            for d in divs:
                ts = d['ts']
                if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                    ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
                else:
                    ts_utc = ts.replace(tzinfo=None)
                et = ts_utc - timedelta(hours=5)
                et_time = dtime(et.hour, et.minute)
                if et_time < dtime(10, 0) or et_time > dtime(15, 30):
                    continue
                trades = sim_trades([d], bars)
                for t in trades:
                    t['cvd_gap'] = d['cvd_gap']
                all_trades_with_gap.extend(trades)

        if not all_trades_with_gap:
            print(f"  {source}: no data", flush=True)
            continue

        # Sort by CVD gap and split into quartiles
        all_trades_with_gap.sort(key=lambda t: t['cvd_gap'])
        n = len(all_trades_with_gap)
        q_size = n // 4
        if q_size < 3:
            print(f"  {source}: too few trades ({n}) for quartile analysis", flush=True)
            # Just show overall + median split
            median_gap = all_trades_with_gap[n//2]['cvd_gap']
            lo = [t for t in all_trades_with_gap if t['cvd_gap'] <= median_gap]
            hi = [t for t in all_trades_with_gap if t['cvd_gap'] > median_gap]
            print_summary(lo, f"  {source} below-median CVD gap (<={median_gap:.0f})")
            print_summary(hi, f"  {source} above-median CVD gap (>{median_gap:.0f})")
        else:
            for qi in range(4):
                start = qi * q_size
                end = (qi + 1) * q_size if qi < 3 else n
                q_trades = all_trades_with_gap[start:end]
                gap_lo = q_trades[0]['cvd_gap']
                gap_hi = q_trades[-1]['cvd_gap']
                print_summary(q_trades, f"  {source} Q{qi+1} (cvd_gap {gap_lo:.0f}-{gap_hi:.0f})")

    # ══════════════════════════════════════════════════
    #  PART 4: Per-date comparison — same day, live vs rithmic
    # ══════════════════════════════════════════════════
    print(f"\n{'='*65}", flush=True)
    print("  PART 4: Per-date ALL-divs comparison (same dates, both sources)", flush=True)
    print("="*65, flush=True)

    # Only dates where both sources exist
    cur.execute("""
        SELECT trade_date, source, count(*) as cnt
        FROM es_range_bars
        WHERE source IN ('live', 'rithmic')
        GROUP BY trade_date, source
        ORDER BY trade_date, source
    """)
    date_source = defaultdict(dict)
    for r in cur.fetchall():
        date_source[r['trade_date']][r['source']] = r['cnt']

    both_dates = [d for d, s in date_source.items() if 'live' in s and 'rithmic' in s]
    print(f"  Dates with both sources: {both_dates}", flush=True)

    for trade_date in both_dates:
        print(f"\n  --- {trade_date} ---", flush=True)
        for source in ['live', 'rithmic']:
            cur.execute("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
                       ts_start, ts_end
                FROM es_range_bars
                WHERE source = %s AND trade_date = %s AND status = 'closed'
                ORDER BY bar_idx ASC
            """, (source, str(trade_date)))
            bars = cur.fetchall()
            if len(bars) < 15:
                print(f"  {source}: too few bars ({len(bars)})", flush=True)
                continue

            swings = find_swings(bars)
            divs = detect_divs(bars, swings)

            cooldown = {'long': None, 'short': None}
            filtered = []
            for d in divs:
                ts = d['ts']
                if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                    ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
                else:
                    ts_utc = ts.replace(tzinfo=None)
                et = ts_utc - timedelta(hours=5)
                et_time = dtime(et.hour, et.minute)
                if et_time < dtime(10, 0) or et_time > dtime(15, 30):
                    continue
                cd = d['direction']
                if cooldown[cd] and ts < cooldown[cd]:
                    continue
                filtered.append(d)
                cooldown[cd] = ts + timedelta(minutes=15)

            trades = sim_trades(filtered, bars)

            cvd_range = max(b['cvd'] for b in bars) - min(b['cvd'] for b in bars)
            avg_cvd_gap = sum(d['cvd_gap'] for d in filtered) / len(filtered) if filtered else 0
            print_summary(trades, f"  {source} ({len(bars)} bars, cvd_range={cvd_range:.0f}, avg_cvd_gap={avg_cvd_gap:.0f})")

    conn.close()
    print("\nDone.", flush=True)


if __name__ == '__main__':
    main()
