"""
Backtest T1 Absorption criteria variants against historical range bar data.
Replays swing detection + divergence logic, then tests each signal for WIN/LOSS
using a simple forward SL/target window.
"""
import psycopg2, os, json, math
from collections import defaultdict
from datetime import datetime, timedelta

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# -- Settings --
PIVOT_LEFT = 2
PIVOT_RIGHT = 2
VOL_WINDOW = 10
MIN_VOL_RATIO = 1.4
CVD_STD_WINDOW = 20
MAX_TRIGGER_DIST = 40
TARGET_PTS = 10.0
STOP_PTS = 12.0
MAX_BARS_FORWARD = 50

dates_to_test = ['2026-02-24', '2026-02-25', '2026-02-26', '2026-02-27', '2026-03-02']
all_results = []

for date in dates_to_test:
    # Prefer rithmic source
    cur.execute("""
        SELECT DISTINCT source FROM es_range_bars WHERE trade_date = %s
    """, (date,))
    sources = [r[0] for r in cur.fetchall()]
    src = 'rithmic' if 'rithmic' in sources else 'live'

    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume,
               bar_buy_volume, bar_sell_volume, bar_delta, cvd_close,
               ts_start - interval '5 hours' as ts_et
        FROM es_range_bars
        WHERE trade_date = %s AND source = %s
        ORDER BY bar_idx
    """, (date, src))
    rows = cur.fetchall()
    if not rows:
        continue

    bars = []
    for r in rows:
        bars.append({
            'idx': r[0], 'open': r[1], 'high': r[2], 'low': r[3], 'close': r[4],
            'volume': r[5], 'buy_vol': r[6], 'sell_vol': r[7], 'delta': r[8],
            'cvd': r[9], 'time': r[10]
        })

    # Filter to RTH (9:30-16:00 ET)
    bars = [b for b in bars if b['time'] and b['time'].hour >= 9 and
            (b['time'].hour > 9 or b['time'].minute >= 30) and b['time'].hour < 16]

    if len(bars) < 20:
        continue

    # -- Find swing points --
    swing_lows = []
    swing_highs = []
    for i in range(PIVOT_LEFT, len(bars) - PIVOT_RIGHT):
        b = bars[i]
        is_low = True
        for j in range(1, PIVOT_LEFT + 1):
            if b['low'] > bars[i - j]['low']:
                is_low = False
                break
        if is_low:
            for j in range(1, PIVOT_RIGHT + 1):
                if b['low'] > bars[i + j]['low']:
                    is_low = False
                    break
        if is_low:
            swing_lows.append({'bar_i': i, 'price': b['low'], 'cvd': b['cvd'],
                               'time': b['time'], 'idx': b['idx']})

        is_high = True
        for j in range(1, PIVOT_LEFT + 1):
            if b['high'] < bars[i - j]['high']:
                is_high = False
                break
        if is_high:
            for j in range(1, PIVOT_RIGHT + 1):
                if b['high'] < bars[i + j]['high']:
                    is_high = False
                    break
        if is_high:
            swing_highs.append({'bar_i': i, 'price': b['high'], 'cvd': b['cvd'],
                                'time': b['time'], 'idx': b['idx']})

    # -- Rolling CVD std dev --
    def get_cvd_std(bar_i):
        start = max(0, bar_i - CVD_STD_WINDOW)
        if bar_i - start < 5:
            return None
        changes = [abs(bars[k]['cvd'] - bars[k - 1]['cvd']) for k in range(start + 1, bar_i + 1)]
        if not changes:
            return None
        mean = sum(changes) / len(changes)
        var = sum((c - mean) ** 2 for c in changes) / len(changes)
        return math.sqrt(var) if var > 0 else None

    # -- Rolling ATR --
    def get_atr(bar_i):
        start = max(0, bar_i - CVD_STD_WINDOW)
        if bar_i - start < 5:
            return None
        diffs = [abs(bars[k]['close'] - bars[k - 1]['close']) for k in range(start + 1, bar_i + 1)]
        return sum(diffs) / len(diffs) if diffs else None

    # -- Volume check --
    def vol_check(bar_i):
        start = max(0, bar_i - VOL_WINDOW)
        if bar_i - start < 3:
            return 0, False
        vols = [bars[k]['volume'] for k in range(start, bar_i)]
        avg_vol = sum(vols) / len(vols) if vols else 1
        ratio = bars[bar_i]['volume'] / avg_vol if avg_vol > 0 else 0
        return ratio, ratio >= MIN_VOL_RATIO

    # -- Scan trigger bars --
    for trigger_i in range(PIVOT_LEFT + PIVOT_RIGHT + 5, len(bars)):
        trigger_bar = bars[trigger_i]
        vol_ratio, vol_ok = vol_check(trigger_i)
        if not vol_ok:
            continue

        cvd_std = get_cvd_std(trigger_i)
        atr = get_atr(trigger_i)
        if not cvd_std or not atr or atr < 0.5:
            continue

        confirmed_lows = [s for s in swing_lows if s['bar_i'] + PIVOT_RIGHT <= trigger_i]
        confirmed_highs = [s for s in swing_highs if s['bar_i'] + PIVOT_RIGHT <= trigger_i]

        # -- Consecutive swing low pairs --
        bull_divs = []
        for pi in range(1, len(confirmed_lows)):
            s1 = confirmed_lows[pi - 1]
            s2 = confirmed_lows[pi]
            if trigger_i - s2['bar_i'] > MAX_TRIGGER_DIST:
                continue

            cvd_gap = s2['cvd'] - s1['cvd']
            price_dist = s2['price'] - s1['price']
            z = abs(cvd_gap) / cvd_std if cvd_std > 0 else 0
            price_atr = abs(price_dist) / atr if atr > 0 else 0

            if z < 0.5:
                continue

            if price_dist > 0 and cvd_gap < 0:
                pattern = 'sell_absorption'
            elif price_dist < 0 and cvd_gap > 0:
                pattern = 'sell_exhaustion'
            else:
                continue

            bull_divs.append({
                'pattern': pattern,
                'tier': 2 if 'exhaustion' in pattern else 1,
                'z': z, 'price_dist': price_dist, 'price_atr': price_atr,
                'bar_dist': s2['bar_i'] - s1['bar_i'],
                's1_price': s1['price'], 's2_price': s2['price'],
                's1_cvd': s1['cvd'], 's2_cvd': s2['cvd'],
                'trigger_dist': trigger_i - s2['bar_i'],
            })

        # -- Consecutive swing high pairs --
        bear_divs = []
        for pi in range(1, len(confirmed_highs)):
            s1 = confirmed_highs[pi - 1]
            s2 = confirmed_highs[pi]
            if trigger_i - s2['bar_i'] > MAX_TRIGGER_DIST:
                continue

            cvd_gap = s2['cvd'] - s1['cvd']
            price_dist = s2['price'] - s1['price']
            z = abs(cvd_gap) / cvd_std if cvd_std > 0 else 0
            price_atr = abs(price_dist) / atr if atr > 0 else 0

            if z < 0.5:
                continue

            if price_dist < 0 and cvd_gap > 0:
                pattern = 'buy_absorption'
            elif price_dist > 0 and cvd_gap < 0:
                pattern = 'buy_exhaustion'
            else:
                continue

            bear_divs.append({
                'pattern': pattern,
                'tier': 2 if 'exhaustion' in pattern else 1,
                'z': z, 'price_dist': price_dist, 'price_atr': price_atr,
                'bar_dist': s2['bar_i'] - s1['bar_i'],
                's1_price': s1['price'], 's2_price': s2['price'],
                's1_cvd': s1['cvd'], 's2_cvd': s2['cvd'],
                'trigger_dist': trigger_i - s2['bar_i'],
            })

        if not bull_divs and not bear_divs:
            continue

        # -- Compute outcome for each divergence --
        es_price = trigger_bar['close']
        for div in bull_divs + bear_divs:
            direction = 'long' if div in bull_divs else 'short'

            target_price = es_price + TARGET_PTS if direction == 'long' else es_price - TARGET_PTS
            stop_price = es_price - STOP_PTS if direction == 'long' else es_price + STOP_PTS

            outcome = 'EXPIRED'
            pnl = 0
            max_profit = 0
            max_loss = 0

            for fi in range(trigger_i + 1, min(trigger_i + MAX_BARS_FORWARD + 1, len(bars))):
                fb = bars[fi]
                if direction == 'long':
                    profit = fb['high'] - es_price
                    loss = -(es_price - fb['low'])
                    max_profit = max(max_profit, profit)
                    max_loss = min(max_loss, loss)
                    if fb['low'] <= stop_price:
                        outcome = 'LOSS'
                        pnl = -STOP_PTS
                        break
                    if fb['high'] >= target_price:
                        outcome = 'WIN'
                        pnl = TARGET_PTS
                        break
                else:
                    profit = es_price - fb['low']
                    loss = -(fb['high'] - es_price)
                    max_profit = max(max_profit, profit)
                    max_loss = min(max_loss, loss)
                    if fb['high'] >= stop_price:
                        outcome = 'LOSS'
                        pnl = -STOP_PTS
                        break
                    if fb['low'] <= target_price:
                        outcome = 'WIN'
                        pnl = TARGET_PTS
                        break

            div['direction'] = direction
            div['outcome'] = outcome
            div['pnl'] = pnl
            div['max_profit'] = max_profit
            div['max_loss'] = max_loss
            div['es_price'] = es_price
            div['date'] = date
            div['time'] = trigger_bar['time'].strftime('%H:%M') if trigger_bar['time'] else '?'
            div['trigger_i'] = trigger_i
            div['vol_ratio'] = vol_ratio
            div['both_dirs'] = len(bull_divs) > 0 and len(bear_divs) > 0
            all_results.append(div)

conn.close()

# -- Deduplicate: same trigger bar + same direction = keep best z --
seen = {}
for r in all_results:
    key = (r['date'], r['trigger_i'], r['direction'])
    if key not in seen or r['z'] > seen[key]['z']:
        seen[key] = r
deduped = list(seen.values())

print(f"Total raw signals: {len(all_results)}")
print(f"After dedup (best per bar+direction): {len(deduped)}")

# -- Define filter criteria --
criteria = {
    'BASELINE (current z>=0.5)': lambda r: True,
    'F1: block_both_dirs': lambda r: not r['both_dirs'],
    'F2a: price_dist >= 2pt': lambda r: abs(r['price_dist']) >= 2.0,
    'F2b: price_dist >= 3pt': lambda r: abs(r['price_dist']) >= 3.0,
    'F2c: price_dist >= 5pt': lambda r: abs(r['price_dist']) >= 5.0,
    'F3a: bar_dist >= 5': lambda r: r['bar_dist'] >= 5,
    'F3b: bar_dist >= 10': lambda r: r['bar_dist'] >= 10,
    'F3c: bar_dist >= 15': lambda r: r['bar_dist'] >= 15,
    'F4a: z >= 1.0': lambda r: r['z'] >= 1.0,
    'F4b: z >= 1.5': lambda r: r['z'] >= 1.5,
    'F4c: z >= 2.0': lambda r: r['z'] >= 2.0,
    'F5a: price_atr >= 0.5': lambda r: r['price_atr'] >= 0.5,
    'F5b: price_atr >= 1.0': lambda r: r['price_atr'] >= 1.0,
    'F5c: price_atr >= 1.5': lambda r: r['price_atr'] >= 1.5,
    'F5d: price_atr >= 2.0': lambda r: r['price_atr'] >= 2.0,
    'F6a: price>=3 + z>=1.0': lambda r: abs(r['price_dist']) >= 3.0 and r['z'] >= 1.0,
    'F6b: price>=3 + bar>=10': lambda r: abs(r['price_dist']) >= 3.0 and r['bar_dist'] >= 10,
    'F6c: price>=5 + z>=1.0': lambda r: abs(r['price_dist']) >= 5.0 and r['z'] >= 1.0,
    'F6d: no_both + price>=3': lambda r: not r['both_dirs'] and abs(r['price_dist']) >= 3.0,
    'F6e: no_both+price>=3+z>=1': lambda r: not r['both_dirs'] and abs(r['price_dist']) >= 3.0 and r['z'] >= 1.0,
    'F6f: atr>=1.0 + z>=1.0': lambda r: r['price_atr'] >= 1.0 and r['z'] >= 1.0,
    'F6g: atr>=1.5 + z>=1.0': lambda r: r['price_atr'] >= 1.5 and r['z'] >= 1.0,
    'F6h: no_both+atr>=1+z>=1': lambda r: not r['both_dirs'] and r['price_atr'] >= 1.0 and r['z'] >= 1.0,
}

t1_signals = [r for r in deduped if r['tier'] == 1]
t2_signals = [r for r in deduped if r['tier'] == 2]

print(f"\nT1 (absorption) signals: {len(t1_signals)}")
print(f"T2 (exhaustion) signals: {len(t2_signals)}")

t2w = sum(1 for r in t2_signals if r['outcome'] == 'WIN')
t2l = sum(1 for r in t2_signals if r['outcome'] == 'LOSS')
t2e = sum(1 for r in t2_signals if r['outcome'] == 'EXPIRED')
t2p = sum(r['pnl'] for r in t2_signals)
print(f"\nT2 REFERENCE: {len(t2_signals)} signals, {t2w}W/{t2l}L/{t2e}E, "
      f"WR={100 * t2w / max(1, t2w + t2l):.0f}%, PnL={t2p:+.1f}")

print(f"\n{'='*110}")
print(f"=== T1 ABSORPTION ONLY ===")
print(f"{'Criteria':<40} | {'Sigs':>4} | {'W':>3}/{'L':>3}/{'E':>3} | {'WR%':>5} | {'PnL':>8} | {'PnL/t':>7}")
print("-" * 110)

for name in sorted(criteria.keys()):
    filt = criteria[name]
    filtered = [r for r in t1_signals if filt(r)]
    n = len(filtered)
    w = sum(1 for r in filtered if r['outcome'] == 'WIN')
    l = sum(1 for r in filtered if r['outcome'] == 'LOSS')
    e = sum(1 for r in filtered if r['outcome'] == 'EXPIRED')
    pnl = sum(r['pnl'] for r in filtered)
    wr = 100 * w / max(1, w + l)
    ppt = pnl / max(1, n)
    print(f"{name:<40} | {n:>4} | {w:>3}/{l:>3}/{e:>3} | {wr:>5.1f} | {pnl:>+8.1f} | {ppt:>+7.2f}")

print(f"\n{'='*110}")
print(f"=== COMBINED: T1 (filtered) + T2 (always on) ===")
print(f"{'Criteria':<40} | {'Sigs':>4} | {'W':>3}/{'L':>3}/{'E':>3} | {'WR%':>5} | {'PnL':>8} | {'PnL/t':>7}")
print("-" * 110)

for name in sorted(criteria.keys()):
    filt = criteria[name]
    filtered_t1 = [r for r in t1_signals if filt(r)]
    combined = filtered_t1 + t2_signals
    n = len(combined)
    w = sum(1 for r in combined if r['outcome'] == 'WIN')
    l = sum(1 for r in combined if r['outcome'] == 'LOSS')
    e = sum(1 for r in combined if r['outcome'] == 'EXPIRED')
    pnl = sum(r['pnl'] for r in combined)
    wr = 100 * w / max(1, w + l)
    ppt = pnl / max(1, n)
    print(f"{name:<40} | {n:>4} | {w:>3}/{l:>3}/{e:>3} | {wr:>5.1f} | {pnl:>+8.1f} | {ppt:>+7.2f}")

# -- Show detail for a few key filters --
for fname, filt in [
    ('F6e: no_both+price>=3+z>=1', lambda r: not r['both_dirs'] and abs(r['price_dist']) >= 3.0 and r['z'] >= 1.0),
    ('F5c: price_atr >= 1.5', lambda r: r['price_atr'] >= 1.5),
    ('F6h: no_both+atr>=1+z>=1', lambda r: not r['both_dirs'] and r['price_atr'] >= 1.0 and r['z'] >= 1.0),
]:
    best_t1 = sorted([r for r in t1_signals if filt(r)], key=lambda r: (r['date'], r['time']))
    if not best_t1:
        continue
    print(f"\n=== DETAIL: {fname} ({len(best_t1)} signals) ===")
    for r in best_t1:
        print(f"  {r['date']} {r['time']} | {r['pattern']:20} {r['direction']:5} | z={r['z']:.1f} "
              f"pdist={r['price_dist']:+.1f} atr={r['price_atr']:.1f} bdist={r['bar_dist']:>2} "
              f"| ES={r['es_price']:.1f} | {r['outcome']:7} pnl={r['pnl']:+.0f} mp={r['max_profit']:.1f}")
