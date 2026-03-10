"""Test: Do dominant vanna levels have predictive power on their own?

Approach: When price touches a dominant vanna level (within X pts), does it bounce?
No CVD divergence required — pure price + vanna level test.

Tests multiple entry strategies:
1. TOUCH: price enters proximity zone → enter immediately
2. BOUNCE: price enters zone then reverses (bar closes moving away) → enter on bounce
3. HOLD: price stays near level for 2+ bars → enter on stability

Uses rithmic bars only. Tests all available dates.
"""
import psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
import os

DB_URL = os.environ.get('DATABASE_URL')


def get_vanna_timeline(cur, start_date, end_date):
    """Get dominant vanna levels per volland snapshot, keyed by timestamp."""
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
            if not data:
                continue
            total = sum(abs(v) for v in data.values())
            if total == 0:
                continue
            for strike, val in data.items():
                pct = abs(val) / total * 100
                if pct >= 12:
                    s = float(strike)
                    if s not in all_strikes or pct > all_strikes[s]['pct']:
                        all_strikes[s] = {'strike': s, 'value': float(val), 'pct': round(pct, 1),
                                         'tf': tf}
        if all_strikes:
            timeline.append((ts, list(all_strikes.values())))

    return timeline


def get_nearest_levels(timeline, ts):
    """Get the most recent vanna levels before timestamp ts."""
    for t, levels in reversed(timeline):
        if t <= ts:
            return levels
    return None


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get all dates with rithmic bars
    cur.execute("""
        SELECT DISTINCT trade_date FROM es_range_bars
        WHERE source = 'rithmic'
        ORDER BY trade_date
    """)
    dates = [r['trade_date'] for r in cur.fetchall()]
    print(f"Rithmic dates: {len(dates)} ({dates[0]} to {dates[-1]})", flush=True)

    # Load vanna timeline
    vanna_tl = get_vanna_timeline(cur, str(dates[0]), str(dates[-1]))
    print(f"Vanna snapshots with dominant levels: {len(vanna_tl)}", flush=True)

    # ── Test configs ──
    configs = [
        # (proximity, stop, target, label)
        (5, 8, 10, "5pt prox, SL8/T10"),
        (8, 8, 10, "8pt prox, SL8/T10"),
        (10, 8, 10, "10pt prox, SL8/T10"),
        (15, 8, 10, "15pt prox, SL8/T10"),
        (5, 5, 8, "5pt prox, SL5/T8"),
        (8, 5, 8, "8pt prox, SL5/T8"),
        (5, 10, 15, "5pt prox, SL10/T15"),
        (8, 10, 15, "8pt prox, SL10/T15"),
    ]

    for proximity, sl, tp, label in configs:
        all_trades = []

        for trade_date in dates:
            cur.execute("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume, bar_delta, cumulative_delta AS cvd,
                       ts_start, ts_end, status
                FROM es_range_bars
                WHERE source = 'rithmic' AND trade_date = %s AND status = 'closed'
                ORDER BY bar_idx ASC
            """, (str(trade_date),))
            bars = cur.fetchall()
            if len(bars) < 20:
                continue

            cooldown = {'long': None, 'short': None}

            for i in range(5, len(bars)):
                bar = bars[i]
                ts = bar['ts_end']
                if ts is None:
                    continue

                # Time filter: 10:00-15:30 ET
                # ts is UTC (timestamptz). Convert to ET.
                if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                    ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
                else:
                    ts_utc = ts.replace(tzinfo=None) if hasattr(ts, 'replace') else ts

                # EST = UTC-5, EDT = UTC-4. Use -5 for simplicity (March = EST still)
                et_hour = (ts_utc - timedelta(hours=5)).hour
                et_min = (ts_utc - timedelta(hours=5)).minute
                et_time = dtime(et_hour, et_min)
                if et_time < dtime(10, 0) or et_time > dtime(15, 30):
                    continue

                bar_close = float(bar['bar_close'])
                bar_low = float(bar['bar_low'])
                bar_high = float(bar['bar_high'])

                # Get vanna levels
                levels = get_nearest_levels(vanna_tl, ts)
                if not levels:
                    continue

                for lv in levels:
                    strike = lv['strike']
                    vanna_val = lv['value']
                    dist = abs(bar_close - strike)

                    if dist > proximity:
                        continue

                    # Direction from vanna sign
                    if vanna_val > 0:
                        direction = 'long'
                    else:
                        direction = 'short'

                    # Bounce confirmation: bar should show price moving AWAY from level
                    # For long: bar came down to level, closed above it (bar_low near/below strike)
                    # For short: bar went up to level, closed below it (bar_high near/above strike)
                    is_bounce = False
                    if direction == 'long':
                        # Price touched from above and bounced: low is near level, close > low
                        if bar_low <= strike + proximity and bar_close > bar_low + 1.0:
                            is_bounce = True
                    else:
                        # Price touched from below and bounced: high is near level, close < high
                        if bar_high >= strike - proximity and bar_close < bar_high - 1.0:
                            is_bounce = True

                    if not is_bounce:
                        continue

                    # Cooldown
                    if cooldown[direction] and ts < cooldown[direction]:
                        continue

                    # Entry
                    entry = bar_close
                    is_long = direction == 'long'
                    target = entry + tp if is_long else entry - tp
                    stop = entry - sl if is_long else entry + sl

                    result = 'EXPIRED'
                    pnl = 0
                    for j in range(i + 1, len(bars)):
                        fb = bars[j]
                        hi = float(fb['bar_high'])
                        lo = float(fb['bar_low'])
                        if is_long:
                            if lo <= stop:
                                result = 'LOSS'; pnl = -sl; break
                            if hi >= target:
                                result = 'WIN'; pnl = tp; break
                        else:
                            if hi >= stop:
                                result = 'LOSS'; pnl = -sl; break
                            if lo <= target:
                                result = 'WIN'; pnl = tp; break

                    if result == 'EXPIRED':
                        exit_p = float(bars[-1]['bar_close'])
                        pnl = (exit_p - entry) if is_long else (entry - exit_p)

                    all_trades.append({
                        'date': trade_date, 'direction': direction,
                        'entry': entry, 'result': result, 'pnl': pnl,
                        'vanna_strike': strike, 'vanna_pct': lv['pct'],
                        'dist': dist,
                    })
                    cooldown[direction] = ts + timedelta(minutes=20)
                    break  # one signal per bar

        # ── Results ──
        if not all_trades:
            print(f"\n  {label}: NO TRADES", flush=True)
            continue

        wins = sum(1 for t in all_trades if t['result'] == 'WIN')
        losses = sum(1 for t in all_trades if t['result'] == 'LOSS')
        total = len(all_trades)
        wr = wins / total * 100
        total_pts = sum(t['pnl'] for t in all_trades)
        avg = total_pts / total

        daily = defaultdict(lambda: {'n': 0, 'w': 0, 'pts': 0})
        for t in all_trades:
            d = daily[t['date']]
            d['n'] += 1
            if t['result'] == 'WIN': d['w'] += 1
            d['pts'] += t['pnl']

        longs = [t for t in all_trades if t['direction'] == 'long']
        shorts = [t for t in all_trades if t['direction'] == 'short']
        long_wr = sum(1 for t in longs if t['result'] == 'WIN') / len(longs) * 100 if longs else 0
        short_wr = sum(1 for t in shorts if t['result'] == 'WIN') / len(shorts) * 100 if shorts else 0

        print(f"\n{'='*60}", flush=True)
        print(f"  {label}", flush=True)
        print(f"  Trades: {total}  W: {wins}  L: {losses}  WR: {wr:.1f}%", flush=True)
        print(f"  Total: {total_pts:+.1f} pts  Avg: {avg:+.1f} pts/trade", flush=True)
        print(f"  Longs: {len(longs)} ({long_wr:.0f}% WR)  Shorts: {len(shorts)} ({short_wr:.0f}% WR)", flush=True)

        # Daily
        print(f"  Daily:", flush=True)
        cum = 0
        for d in sorted(daily):
            dd = daily[d]
            cum += dd['pts']
            dwr = dd['w'] / dd['n'] * 100 if dd['n'] else 0
            print(f"    {d}: {dd['n']}t {dd['w']}W WR={dwr:.0f}% {dd['pts']:+.1f} cum={cum:+.1f}", flush=True)

    # ── RAW LEVEL TEST: just check if price bounces at vanna levels ──
    print(f"\n{'='*60}", flush=True)
    print(f"  RAW BOUNCE RATE AT VANNA LEVELS (no trade, just statistics)", flush=True)
    print(f"{'='*60}", flush=True)

    for proximity in [3, 5, 8, 10]:
        touches = 0
        bounces_5 = 0  # price moved 5+ pts away within 20 bars
        bounces_10 = 0  # price moved 10+ pts away within 20 bars

        for trade_date in dates:
            cur.execute("""
                SELECT bar_idx, bar_high, bar_low, bar_close, ts_start, ts_end
                FROM es_range_bars
                WHERE source = 'rithmic' AND trade_date = %s AND status = 'closed'
                ORDER BY bar_idx ASC
            """, (str(trade_date),))
            bars = cur.fetchall()
            if len(bars) < 30:
                continue

            last_touch_idx = -20  # avoid double counting

            for i in range(5, len(bars) - 20):
                bar = bars[i]
                ts = bar['ts_end']
                if ts is None:
                    continue

                # Time filter
                if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                    ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
                else:
                    ts_utc = ts.replace(tzinfo=None)
                et_time = dtime((ts_utc - timedelta(hours=5)).hour, (ts_utc - timedelta(hours=5)).minute)
                if et_time < dtime(10, 0) or et_time > dtime(15, 30):
                    continue

                if i - last_touch_idx < 20:
                    continue

                bar_close = float(bar['bar_close'])
                levels = get_nearest_levels(vanna_tl, ts)
                if not levels:
                    continue

                for lv in levels:
                    strike = lv['strike']
                    dist = abs(bar_close - strike)
                    if dist > proximity:
                        continue

                    direction = 'long' if lv['value'] > 0 else 'short'
                    touches += 1
                    last_touch_idx = i

                    # Check next 20 bars for bounce
                    entry = bar_close
                    max_favorable = 0
                    for j in range(i + 1, min(i + 21, len(bars))):
                        fb = bars[j]
                        if direction == 'long':
                            fav = float(fb['bar_high']) - entry
                        else:
                            fav = entry - float(fb['bar_low'])
                        max_favorable = max(max_favorable, fav)

                    if max_favorable >= 5:
                        bounces_5 += 1
                    if max_favorable >= 10:
                        bounces_10 += 1
                    break

        b5_rate = bounces_5 / touches * 100 if touches else 0
        b10_rate = bounces_10 / touches * 100 if touches else 0
        print(f"  Proximity {proximity}pt: {touches} touches, "
              f"5pt bounce={bounces_5} ({b5_rate:.1f}%), "
              f"10pt bounce={bounces_10} ({b10_rate:.1f}%)", flush=True)

    # ── CONTROL: random levels (same proximity, same dates) ──
    print(f"\n  CONTROL: random price levels (NOT vanna)", flush=True)
    import random
    random.seed(42)

    for proximity in [5, 8]:
        touches = 0
        bounces_5 = 0
        bounces_10 = 0

        for trade_date in dates:
            cur.execute("""
                SELECT bar_idx, bar_high, bar_low, bar_close, ts_start, ts_end
                FROM es_range_bars
                WHERE source = 'rithmic' AND trade_date = %s AND status = 'closed'
                ORDER BY bar_idx ASC
            """, (str(trade_date),))
            bars = cur.fetchall()
            if len(bars) < 30:
                continue

            # Generate random "levels" in the price range
            lo = min(float(b['bar_low']) for b in bars)
            hi = max(float(b['bar_high']) for b in bars)
            random_levels = [{'strike': random.uniform(lo, hi), 'value': random.choice([1, -1])} for _ in range(3)]

            last_touch_idx = -20

            for i in range(5, len(bars) - 20):
                bar = bars[i]
                ts = bar['ts_end']
                if ts is None:
                    continue

                if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                    ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
                else:
                    ts_utc = ts.replace(tzinfo=None)
                et_time = dtime((ts_utc - timedelta(hours=5)).hour, (ts_utc - timedelta(hours=5)).minute)
                if et_time < dtime(10, 0) or et_time > dtime(15, 30):
                    continue

                if i - last_touch_idx < 20:
                    continue

                bar_close = float(bar['bar_close'])
                for lv in random_levels:
                    dist = abs(bar_close - lv['strike'])
                    if dist > proximity:
                        continue

                    direction = 'long' if lv['value'] > 0 else 'short'
                    touches += 1
                    last_touch_idx = i

                    entry = bar_close
                    max_favorable = 0
                    for j in range(i + 1, min(i + 21, len(bars))):
                        fb = bars[j]
                        if direction == 'long':
                            fav = float(fb['bar_high']) - entry
                        else:
                            fav = entry - float(fb['bar_low'])
                        max_favorable = max(max_favorable, fav)

                    if max_favorable >= 5:
                        bounces_5 += 1
                    if max_favorable >= 10:
                        bounces_10 += 1
                    break

        b5_rate = bounces_5 / touches * 100 if touches else 0
        b10_rate = bounces_10 / touches * 100 if touches else 0
        print(f"  Random {proximity}pt: {touches} touches, "
              f"5pt bounce={bounces_5} ({b5_rate:.1f}%), "
              f"10pt bounce={bounces_10} ({b10_rate:.1f}%)", flush=True)

    conn.close()
    print("\nDone.", flush=True)


if __name__ == '__main__':
    main()
