"""VP simple divergence vs ES Absorption — are they duplicates?

Compare:
1. VP-style: any CVD swing divergence, 15-min cooldown, no quality gate
2. ABS-style: CVD swing divergence + volume gate (1.4x) + z-score (0.5)
3. Overlap: how many signals fire on both?
4. Actual ES Absorption signals from setup_log (what production fires)
"""
import psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
import os

DB_URL = os.environ.get('DATABASE_URL')


def find_swings(bars, pivot_n=2):
    swings = []
    for i in range(pivot_n, len(bars) - pivot_n):
        is_low = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_low'] > bars[i-j]['bar_low'] or bars[i]['bar_low'] > bars[i+j]['bar_low']:
                is_low = False; break
        if is_low:
            swings.append({'type': 'low', 'price': bars[i]['bar_low'], 'cvd': bars[i]['cvd'],
                          'ts': bars[i]['ts_start'], 'bar_idx': i, 'volume': bars[i]['volume']})
        is_high = True
        for j in range(1, pivot_n + 1):
            if bars[i]['bar_high'] < bars[i-j]['bar_high'] or bars[i]['bar_high'] < bars[i+j]['bar_high']:
                is_high = False; break
        if is_high:
            swings.append({'type': 'high', 'price': bars[i]['bar_high'], 'cvd': bars[i]['cvd'],
                          'ts': bars[i]['ts_start'], 'bar_idx': i, 'volume': bars[i]['volume']})
    swings.sort(key=lambda s: s['ts'])
    return swings


def detect_divs_simple(bars, swings):
    """VP-style: any divergence, no quality gate."""
    divs = []
    lows = [s for s in swings if s['type'] == 'low']
    highs = [s for s in swings if s['type'] == 'high']
    for i in range(1, len(lows)):
        prev, curr = lows[i-1], lows[i]
        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({'direction': 'long', 'price': curr['price'], 'ts': curr['ts'],
                        'bar_idx': curr['bar_idx'], 'type': 'sell_exhaustion'})
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'direction': 'long', 'price': curr['price'], 'ts': curr['ts'],
                        'bar_idx': curr['bar_idx'], 'type': 'sell_absorption'})
    for i in range(1, len(highs)):
        prev, curr = highs[i-1], highs[i]
        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'direction': 'short', 'price': curr['price'], 'ts': curr['ts'],
                        'bar_idx': curr['bar_idx'], 'type': 'buy_exhaustion'})
        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({'direction': 'short', 'price': curr['price'], 'ts': curr['ts'],
                        'bar_idx': curr['bar_idx'], 'type': 'buy_absorption'})
    divs.sort(key=lambda d: d['ts'])
    return divs


def detect_divs_abs(bars, swings):
    """ABS-style: divergence + volume gate + CVD z-score."""
    # Compute rolling stats for z-score
    cvd_changes = []
    for i in range(1, len(bars)):
        cvd_changes.append(bars[i]['cvd'] - bars[i-1]['cvd'])

    def get_cvd_std(bar_idx, window=20):
        start = max(0, bar_idx - window)
        segment = cvd_changes[start:bar_idx]
        if len(segment) < 5:
            return None
        import math
        mean = sum(segment) / len(segment)
        var = sum((x - mean)**2 for x in segment) / len(segment)
        return math.sqrt(var) if var > 0 else None

    def get_vol_avg(bar_idx, window=10):
        start = max(0, bar_idx - window)
        vols = [bars[j]['volume'] for j in range(start, bar_idx)]
        return sum(vols) / len(vols) if vols else 0

    divs = []
    lows = [s for s in swings if s['type'] == 'low']
    highs = [s for s in swings if s['type'] == 'high']

    for i in range(1, len(lows)):
        prev, curr = lows[i-1], lows[i]
        cvd_gap = abs(curr['cvd'] - prev['cvd'])
        std = get_cvd_std(curr['bar_idx'])
        if std is None or std == 0:
            continue
        z = cvd_gap / std
        if z < 0.5:
            continue

        # Volume gate on trigger bar
        vol_avg = get_vol_avg(curr['bar_idx'])
        if vol_avg > 0 and curr['volume'] < 1.4 * vol_avg:
            continue

        if curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({'direction': 'long', 'price': curr['price'], 'ts': curr['ts'],
                        'bar_idx': curr['bar_idx'], 'type': 'sell_exhaustion', 'z': z})
        elif curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'direction': 'long', 'price': curr['price'], 'ts': curr['ts'],
                        'bar_idx': curr['bar_idx'], 'type': 'sell_absorption', 'z': z})

    for i in range(1, len(highs)):
        prev, curr = highs[i-1], highs[i]
        cvd_gap = abs(curr['cvd'] - prev['cvd'])
        std = get_cvd_std(curr['bar_idx'])
        if std is None or std == 0:
            continue
        z = cvd_gap / std
        if z < 0.5:
            continue

        vol_avg = get_vol_avg(curr['bar_idx'])
        if vol_avg > 0 and curr['volume'] < 1.4 * vol_avg:
            continue

        if curr['price'] > prev['price'] and curr['cvd'] < prev['cvd']:
            divs.append({'direction': 'short', 'price': curr['price'], 'ts': curr['ts'],
                        'bar_idx': curr['bar_idx'], 'type': 'buy_exhaustion', 'z': z})
        elif curr['price'] < prev['price'] and curr['cvd'] > prev['cvd']:
            divs.append({'direction': 'short', 'price': curr['price'], 'ts': curr['ts'],
                        'bar_idx': curr['bar_idx'], 'type': 'buy_absorption', 'z': z})

    divs.sort(key=lambda d: d['ts'])
    return divs


def sim_trades(divs, bars, stop=8, target=10):
    trades = []
    for d in divs:
        entry = d['price']
        is_long = d['direction'] == 'long'
        tgt = entry + target if is_long else entry - target
        stp = entry - stop if is_long else entry + stop
        result = 'EXPIRED'; pnl = 0
        for j in range(d['bar_idx'] + 1, len(bars)):
            hi = float(bars[j]['bar_high']); lo = float(bars[j]['bar_low'])
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
                      'bar_idx': d['bar_idx'], 'type': d.get('type', ''),
                      'ts': d['ts'], 'price': entry})
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
    print(f"  {label}: {n} trades, {wins}W/{losses}L, WR={wr:.1f}%, {pts:+.1f} pts", flush=True)


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Use rithmic bars for all tests (production source)
    cur.execute("""
        SELECT DISTINCT trade_date FROM es_range_bars
        WHERE source = 'rithmic' ORDER BY trade_date
    """)
    dates = [r['trade_date'] for r in cur.fetchall()]
    print(f"Dates: {dates}", flush=True)

    # ══════════════════════════════════════════════════
    print("\n" + "="*65, flush=True)
    print("  VP-SIMPLE vs ABS-FILTERED on RITHMIC data", flush=True)
    print("="*65, flush=True)

    all_vp = []
    all_abs = []
    overlap_count = 0
    vp_only_count = 0
    abs_only_count = 0

    for trade_date in dates:
        cur.execute("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
                   ts_start, ts_end
            FROM es_range_bars
            WHERE source = 'rithmic' AND trade_date = %s AND status = 'closed'
            ORDER BY bar_idx ASC
        """, (str(trade_date),))
        bars = cur.fetchall()
        if len(bars) < 20:
            continue

        swings = find_swings(bars)

        # VP simple
        vp_divs = detect_divs_simple(bars, swings)
        vp_cd = {'long': None, 'short': None}
        vp_filtered = []
        for d in vp_divs:
            ts = d['ts']
            if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
            else:
                ts_utc = ts.replace(tzinfo=None)
            et = ts_utc - timedelta(hours=5)
            if dtime(et.hour, et.minute) < dtime(10, 0) or dtime(et.hour, et.minute) > dtime(15, 30):
                continue
            cd = d['direction']
            if vp_cd[cd] and ts < vp_cd[cd]:
                continue
            vp_filtered.append(d)
            vp_cd[cd] = ts + timedelta(minutes=15)

        # ABS filtered
        abs_divs = detect_divs_abs(bars, swings)
        abs_cd = {'long': None, 'short': None}
        abs_filtered = []
        for d in abs_divs:
            ts = d['ts']
            if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
            else:
                ts_utc = ts.replace(tzinfo=None)
            et = ts_utc - timedelta(hours=5)
            if dtime(et.hour, et.minute) < dtime(10, 0) or dtime(et.hour, et.minute) > dtime(15, 30):
                continue
            cd = d['direction']
            if abs_cd[cd] and ts < abs_cd[cd]:
                continue
            abs_filtered.append(d)
            abs_cd[cd] = ts + timedelta(minutes=15)

        vp_trades = sim_trades(vp_filtered, bars)
        abs_trades = sim_trades(abs_filtered, bars)
        all_vp.extend(vp_trades)
        all_abs.extend(abs_trades)

        # Check overlap (same bar_idx within 5 bars)
        vp_idxs = set(d['bar_idx'] for d in vp_filtered)
        abs_idxs = set(d['bar_idx'] for d in abs_filtered)

        for vi in vp_idxs:
            matched = any(abs(vi - ai) <= 5 for ai in abs_idxs)
            if matched:
                overlap_count += 1
            else:
                vp_only_count += 1
        for ai in abs_idxs:
            matched = any(abs(ai - vi) <= 5 for vi in vp_idxs)
            if not matched:
                abs_only_count += 1

    print_summary(all_vp, "VP-SIMPLE (any divergence, cooldown only)")
    print_summary(all_abs, "ABS-FILTERED (vol gate 1.4x + z-score 0.5)")
    print(f"\n  Signal overlap (within 5 bars):", flush=True)
    print(f"    Both fire: {overlap_count}", flush=True)
    print(f"    VP only:   {vp_only_count}", flush=True)
    print(f"    ABS only:  {abs_only_count}", flush=True)

    # ══════════════════════════════════════════════════
    # What does ABS add by filtering? Quality of VP-only vs overlap vs ABS-only
    print(f"\n{'='*65}", flush=True)
    print("  QUALITY: VP-only vs ABS-only vs Overlap signals", flush=True)
    print("="*65, flush=True)

    vp_only_trades = []
    abs_only_trades = []
    overlap_trades = []

    for trade_date in dates:
        cur.execute("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
                   ts_start, ts_end
            FROM es_range_bars
            WHERE source = 'rithmic' AND trade_date = %s AND status = 'closed'
            ORDER BY bar_idx ASC
        """, (str(trade_date),))
        bars = cur.fetchall()
        if len(bars) < 20: continue

        swings = find_swings(bars)
        vp_divs = detect_divs_simple(bars, swings)
        abs_divs = detect_divs_abs(bars, swings)

        # Apply time + cooldown
        def filter_divs(divs):
            cd = {'long': None, 'short': None}
            out = []
            for d in divs:
                ts = d['ts']
                if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                    ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
                else:
                    ts_utc = ts.replace(tzinfo=None)
                et = ts_utc - timedelta(hours=5)
                if dtime(et.hour, et.minute) < dtime(10, 0) or dtime(et.hour, et.minute) > dtime(15, 30):
                    continue
                c = d['direction']
                if cd[c] and ts < cd[c]: continue
                out.append(d)
                cd[c] = ts + timedelta(minutes=15)
            return out

        vp_f = filter_divs(vp_divs)
        abs_f = filter_divs(abs_divs)

        vp_idx_set = {d['bar_idx'] for d in vp_f}
        abs_idx_set = {d['bar_idx'] for d in abs_f}

        for d in vp_f:
            is_overlap = any(abs(d['bar_idx'] - ai) <= 5 for ai in abs_idx_set)
            trades = sim_trades([d], bars)
            if is_overlap:
                overlap_trades.extend(trades)
            else:
                vp_only_trades.extend(trades)

        for d in abs_f:
            is_overlap = any(abs(d['bar_idx'] - vi) <= 5 for vi in vp_idx_set)
            if not is_overlap:
                trades = sim_trades([d], bars)
                abs_only_trades.extend(trades)

    print_summary(overlap_trades, "OVERLAP (both VP and ABS fire)")
    print_summary(vp_only_trades, "VP-ONLY (VP fires, ABS doesn't)")
    print_summary(abs_only_trades, "ABS-ONLY (ABS fires, VP doesn't)")

    # ══════════════════════════════════════════════════
    # Compare with actual production ES Absorption signals
    print(f"\n{'='*65}", flush=True)
    print("  ACTUAL PRODUCTION ES ABSORPTION from setup_log", flush=True)
    print("="*65, flush=True)

    cur.execute("""
        SELECT id, direction, grade, score, outcome_result, outcome_pnl,
               created_at AT TIME ZONE 'US/Eastern' as ts
        FROM setup_log
        WHERE setup_name = 'ES Absorption'
        ORDER BY id DESC LIMIT 50
    """)
    abs_signals = cur.fetchall()
    if abs_signals:
        wins = sum(1 for s in abs_signals if s['outcome_result'] == 'WIN')
        losses = sum(1 for s in abs_signals if s['outcome_result'] == 'LOSS')
        resolved = [s for s in abs_signals if s['outcome_result'] in ('WIN', 'LOSS')]
        total_pnl = sum(s['outcome_pnl'] or 0 for s in resolved)
        wr = wins / len(resolved) * 100 if resolved else 0
        print(f"  Last {len(abs_signals)} signals: {len(resolved)} resolved, "
              f"{wins}W/{losses}L, WR={wr:.1f}%, {total_pnl:+.1f} pts", flush=True)

        # By grade
        by_grade = defaultdict(lambda: {'n': 0, 'w': 0, 'pts': 0})
        for s in resolved:
            g = by_grade[s['grade']]
            g['n'] += 1
            if s['outcome_result'] == 'WIN': g['w'] += 1
            g['pts'] += s['outcome_pnl'] or 0
        for grade in sorted(by_grade):
            g = by_grade[grade]
            gwr = g['w'] / g['n'] * 100 if g['n'] else 0
            print(f"    {grade}: {g['n']} trades, WR={gwr:.0f}%, {g['pts']:+.1f} pts", flush=True)

        # Per day (last 10 days)
        by_day = defaultdict(lambda: {'n': 0, 'w': 0, 'l': 0, 'pts': 0})
        for s in resolved:
            d = s['ts'].date() if s['ts'] else None
            if not d: continue
            dd = by_day[d]
            dd['n'] += 1
            if s['outcome_result'] == 'WIN': dd['w'] += 1
            else: dd['l'] += 1
            dd['pts'] += s['outcome_pnl'] or 0
        print(f"\n  Daily (resolved):", flush=True)
        cum = 0
        for d in sorted(by_day):
            dd = by_day[d]
            cum += dd['pts']
            dwr = dd['w'] / dd['n'] * 100 if dd['n'] else 0
            print(f"    {d}: {dd['n']}t {dd['w']}W/{dd['l']}L WR={dwr:.0f}% {dd['pts']:+.1f} cum={cum:+.1f}", flush=True)

    # ══════════════════════════════════════════════════
    # Different SL/TP for both methods
    print(f"\n{'='*65}", flush=True)
    print("  SL/TP OPTIMIZATION on rithmic data", flush=True)
    print("="*65, flush=True)

    for sl, tp in [(5, 5), (5, 8), (5, 10), (8, 8), (8, 10), (8, 12), (10, 10), (10, 12), (10, 15), (12, 10), (12, 15)]:
        vp_all = []
        abs_all = []
        for trade_date in dates:
            cur.execute("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume AS volume, bar_delta AS delta, cumulative_delta AS cvd,
                       ts_start, ts_end
                FROM es_range_bars
                WHERE source = 'rithmic' AND trade_date = %s AND status = 'closed'
                ORDER BY bar_idx ASC
            """, (str(trade_date),))
            bars = cur.fetchall()
            if len(bars) < 20: continue
            swings = find_swings(bars)

            def filt(divs):
                cd = {'long': None, 'short': None}
                out = []
                for d in divs:
                    ts = d['ts']
                    if hasattr(ts, 'utcoffset') and ts.utcoffset() is not None:
                        ts_utc = ts.replace(tzinfo=None) - ts.utcoffset()
                    else:
                        ts_utc = ts.replace(tzinfo=None)
                    et = ts_utc - timedelta(hours=5)
                    if dtime(et.hour, et.minute) < dtime(10, 0) or dtime(et.hour, et.minute) > dtime(15, 30):
                        continue
                    c = d['direction']
                    if cd[c] and ts < cd[c]: continue
                    out.append(d)
                    cd[c] = ts + timedelta(minutes=15)
                return out

            vp_all.extend(sim_trades(filt(detect_divs_simple(bars, swings)), bars, sl, tp))
            abs_all.extend(sim_trades(filt(detect_divs_abs(bars, swings)), bars, sl, tp))

        vp_w = sum(1 for t in vp_all if t['result'] == 'WIN')
        vp_n = len(vp_all)
        vp_pts = sum(t['pnl'] for t in vp_all)
        abs_w = sum(1 for t in abs_all if t['result'] == 'WIN')
        abs_n = len(abs_all)
        abs_pts = sum(t['pnl'] for t in abs_all)
        vp_wr = vp_w / vp_n * 100 if vp_n else 0
        abs_wr = abs_w / abs_n * 100 if abs_n else 0
        print(f"  SL={sl}/T={tp}: VP={vp_n}t WR={vp_wr:.0f}% {vp_pts:+.0f}pts | "
              f"ABS={abs_n}t WR={abs_wr:.0f}% {abs_pts:+.0f}pts", flush=True)

    conn.close()
    print("\nDone.", flush=True)


if __name__ == '__main__':
    main()
