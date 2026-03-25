"""
Comprehensive Delta Divergence Backtest
Tests 6 setup concepts with parameter sweeps on Rithmic 5-pt ES range bars.
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import psycopg2
import numpy as np
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict
import time as _time
import warnings
warnings.filterwarnings('ignore')

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

# ── Risk management combos ──────────────────────────────────────────
RM_COMBOS = [(8, 10), (8, 12), (10, 10), (10, 15), (12, 15)]
COOLDOWN_BARS = 10

# ── Time buckets (ET) ───────────────────────────────────────────────
TIME_BUCKETS = [
    ("09:30-10:30", dtime(9, 30), dtime(10, 30)),
    ("10:30-12:00", dtime(10, 30), dtime(12, 0)),
    ("12:00-14:00", dtime(12, 0), dtime(14, 0)),
    ("14:00-15:30", dtime(14, 0), dtime(15, 30)),
]

def get_time_bucket(ts_utc):
    """Convert UTC timestamp to ET and return bucket name."""
    if ts_utc is None:
        return None
    et = ts_utc - timedelta(hours=4)  # EDT
    t = et.time()
    for name, start, end in TIME_BUCKETS:
        if start <= t < end:
            return name
    return None

def is_market_hours_et(ts_utc):
    """Check if bar is during market hours (09:30-16:00 ET)."""
    if ts_utc is None:
        return False
    et = ts_utc - timedelta(hours=4)
    t = et.time()
    return dtime(9, 30) <= t <= dtime(16, 0)


# ── Load data ───────────────────────────────────────────────────────
def load_data():
    print("Loading data from PostgreSQL...")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_date, bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume, bar_delta, bar_buy_volume, bar_sell_volume,
               cumulative_delta, ts_start, ts_end
        FROM es_range_bars
        WHERE source = 'rithmic' AND range_pts = 5 AND status = 'closed'
        ORDER BY trade_date, bar_idx
    """)
    rows = cur.fetchall()
    conn.close()

    # Group by trade_date
    days = defaultdict(list)
    for r in rows:
        bar = {
            'trade_date': r[0],
            'bar_idx': r[1],
            'open': r[2], 'high': r[3], 'low': r[4], 'close': r[5],
            'volume': r[6], 'delta': r[7],
            'buy_volume': r[8], 'sell_volume': r[9],
            'cvd': r[10], 'ts_start': r[11], 'ts_end': r[12],
        }
        days[r[0]].append(bar)

    # Sort each day by bar_idx
    for d in days:
        days[d].sort(key=lambda b: b['bar_idx'])

    total_bars = sum(len(v) for v in days.values())
    print(f"  Loaded {total_bars} bars across {len(days)} dates ({min(days)} to {max(days)})")
    return days


# ── Forward simulation ──────────────────────────────────────────────
def forward_sim(bars, trigger_idx, direction, sl_pts, target_pts):
    """
    Walk forward from trigger_idx+1 checking if target or stop hit first.
    direction: 'long' or 'short'
    Returns: (outcome, pnl, mfe, mae, bars_held)
    """
    entry_bar = bars[trigger_idx]
    entry_price = entry_bar['close']

    if direction == 'long':
        target_price = entry_price + target_pts
        stop_price = entry_price - sl_pts
    else:
        target_price = entry_price - target_pts
        stop_price = entry_price + sl_pts

    mfe = 0.0
    mae = 0.0

    for i in range(trigger_idx + 1, len(bars)):
        bar = bars[i]
        if direction == 'long':
            favorable = bar['high'] - entry_price
            adverse = entry_price - bar['low']
        else:
            favorable = entry_price - bar['low']
            adverse = bar['high'] - entry_price

        mfe = max(mfe, favorable)
        mae = max(mae, adverse)

        # Check stop first (conservative), then target
        if direction == 'long':
            if bar['low'] <= stop_price:
                return ('LOSS', -sl_pts, mfe, mae, i - trigger_idx)
            if bar['high'] >= target_price:
                return ('WIN', target_pts, mfe, mae, i - trigger_idx)
        else:
            if bar['high'] >= stop_price:
                return ('LOSS', -sl_pts, mfe, mae, i - trigger_idx)
            if bar['low'] <= target_price:
                return ('WIN', target_pts, mfe, mae, i - trigger_idx)

    # End of day - use close of last bar
    last = bars[-1]
    if direction == 'long':
        pnl = last['close'] - entry_price
    else:
        pnl = entry_price - last['close']
    outcome = 'WIN' if pnl > 0 else 'LOSS'
    return ('EXPIRED', pnl, mfe, mae, len(bars) - trigger_idx - 1)


# ── Signal generators ───────────────────────────────────────────────

def gen_single_bar_absorption(days, vol_mult, delta_mult):
    """SB: bar closes AGAINST its delta direction + volume/delta gates + 8-bar CVD trend."""
    signals = []
    for date, bars in days.items():
        if len(bars) < 20:
            continue
        for i in range(20, len(bars)):
            bar = bars[i]
            if not is_market_hours_et(bar['ts_start']):
                continue

            # Volume gate: bar volume >= vol_mult * 20-bar avg
            avg_vol = np.mean([b['volume'] for b in bars[max(0, i-20):i]])
            if avg_vol == 0 or bar['volume'] < vol_mult * avg_vol:
                continue

            # Delta gate: |delta| >= delta_mult * 20-bar avg |delta|
            avg_delta = np.mean([abs(b['delta']) for b in bars[max(0, i-20):i]])
            if avg_delta == 0 or abs(bar['delta']) < delta_mult * avg_delta:
                continue

            is_green = bar['close'] > bar['open']
            is_red = bar['close'] < bar['open']
            pos_delta = bar['delta'] > 0
            neg_delta = bar['delta'] < 0

            # 8-bar CVD trend
            if i >= 8:
                cvd_start = bars[i-8]['cvd']
                cvd_end = bars[i]['cvd']
                cvd_rising = cvd_end > cvd_start
                cvd_falling = cvd_end < cvd_start
            else:
                continue

            direction = None
            # Bearish: red bar + positive delta + rising CVD (absorption of buyers)
            if is_red and pos_delta and cvd_rising:
                direction = 'short'
            # Bullish: green bar + negative delta + falling CVD (absorption of sellers)
            elif is_green and neg_delta and cvd_falling:
                direction = 'long'

            if direction:
                signals.append((date, i, direction, bar['ts_start']))
    return signals


def gen_two_bar_absorption(days, vol_mult, delta_mult, recovery_pct):
    """SB2: flush bar + recovery bar pattern."""
    signals = []
    for date, bars in days.items():
        if len(bars) < 20:
            continue
        for i in range(21, len(bars)):
            bar_n = bars[i]      # recovery bar
            bar_n1 = bars[i-1]   # flush bar

            if not is_market_hours_et(bar_n['ts_start']):
                continue

            # Flush bar volume gate
            avg_vol = np.mean([b['volume'] for b in bars[max(0, i-21):i-1]])
            if avg_vol == 0 or bar_n1['volume'] < vol_mult * avg_vol:
                continue

            # Flush bar delta gate
            avg_delta = np.mean([abs(b['delta']) for b in bars[max(0, i-21):i-1]])
            if avg_delta == 0 or abs(bar_n1['delta']) < delta_mult * avg_delta:
                continue

            flush_range = bar_n1['high'] - bar_n1['low']
            if flush_range == 0:
                continue

            direction = None

            # Bearish flush (UP): flush is green + positive delta
            if bar_n1['close'] > bar_n1['open'] and bar_n1['delta'] > 0:
                # Recovery: bar_n closes down, recovering >= X% of flush range
                recovery = bar_n1['high'] - bar_n['close']
                if recovery >= recovery_pct * flush_range:
                    direction = 'short'

            # Bullish flush (DOWN): flush is red + negative delta
            elif bar_n1['close'] < bar_n1['open'] and bar_n1['delta'] < 0:
                recovery = bar_n['close'] - bar_n1['low']
                if recovery >= recovery_pct * flush_range:
                    direction = 'long'

            if direction:
                signals.append((date, i, direction, bar_n['ts_start']))
    return signals


def gen_multi_bar_divergence(days, lookback, div_gap, vol_mult):
    """Multi-bar CVD divergence: normalized CVD slope vs price slope."""
    signals = []
    for date, bars in days.items():
        if len(bars) < lookback + 5:
            continue
        for i in range(lookback, len(bars)):
            bar = bars[i]
            if not is_market_hours_et(bar['ts_start']):
                continue

            # Volume gate on trigger bar
            avg_vol = np.mean([b['volume'] for b in bars[max(0, i-20):i]])
            if avg_vol == 0 or bar['volume'] < vol_mult * avg_vol:
                continue

            window = bars[i-lookback:i+1]
            prices = [b['close'] for b in window]
            cvds = [b['cvd'] for b in window]

            price_range = max(prices) - min(prices)
            cvd_range = max(cvds) - min(cvds)

            if price_range == 0 or cvd_range == 0:
                continue

            # Normalized slopes
            price_slope = (prices[-1] - prices[0]) / price_range
            cvd_slope = (cvds[-1] - cvds[0]) / cvd_range

            gap = cvd_slope - price_slope

            direction = None
            # Bearish: CVD rising while price falling/flat (gap > threshold)
            if cvd_slope > 0.15 and gap > div_gap:
                direction = 'short'
            # Bullish: CVD falling while price rising/flat (gap < -threshold)
            elif cvd_slope < -0.15 and gap < -div_gap:
                direction = 'long'

            if direction:
                signals.append((date, i, direction, bar['ts_start']))
    return signals


def gen_cvd_reversal(days, cvd_lookback, vol_mult):
    """CVD hits N-bar extreme but bar closes against CVD direction."""
    signals = []
    for date, bars in days.items():
        if len(bars) < cvd_lookback + 5:
            continue
        for i in range(cvd_lookback, len(bars)):
            bar = bars[i]
            if not is_market_hours_et(bar['ts_start']):
                continue

            # Volume gate
            avg_vol = np.mean([b['volume'] for b in bars[max(0, i-20):i]])
            if avg_vol > 0 and bar['volume'] < vol_mult * avg_vol:
                continue

            window_cvds = [b['cvd'] for b in bars[i-cvd_lookback:i+1]]
            current_cvd = bar['cvd']
            is_green = bar['close'] > bar['open']
            is_red = bar['close'] < bar['open']

            direction = None
            # Bearish: CVD at N-bar high but bar closes red
            if current_cvd >= max(window_cvds[:-1]) and is_red:
                direction = 'short'
            # Bullish: CVD at N-bar low but bar closes green
            elif current_cvd <= min(window_cvds[:-1]) and is_green:
                direction = 'long'

            if direction:
                signals.append((date, i, direction, bar['ts_start']))
    return signals


def gen_delta_spike(days, delta_mult, mode):
    """Delta spike: extreme delta bar with follow-through or reversal."""
    signals = []
    for date, bars in days.items():
        if len(bars) < 22:
            continue
        for i in range(21, len(bars) - 1):  # need next bar
            bar = bars[i]
            next_bar = bars[i + 1]
            if not is_market_hours_et(bar['ts_start']):
                continue

            # Delta gate: extreme delta
            avg_delta = np.mean([abs(b['delta']) for b in bars[max(0, i-20):i]])
            if avg_delta == 0 or abs(bar['delta']) < delta_mult * avg_delta:
                continue

            bar_up = bar['close'] > bar['open']
            bar_down = bar['close'] < bar['open']
            next_up = next_bar['close'] > next_bar['open']
            next_down = next_bar['close'] < next_bar['open']

            direction = None

            if mode == 'continuation':
                # Extreme delta + next bar continues = momentum
                if bar['delta'] > 0 and bar_up and next_up:
                    direction = 'long'
                elif bar['delta'] < 0 and bar_down and next_down:
                    direction = 'short'
            elif mode == 'reversal':
                # Extreme delta + next bar reverses = exhaustion
                if bar['delta'] > 0 and bar_up and next_down:
                    direction = 'short'
                elif bar['delta'] < 0 and bar_down and next_up:
                    direction = 'long'

            if direction:
                # Signal on next_bar (i+1) since we need it to confirm
                signals.append((date, i + 1, direction, next_bar['ts_start']))
    return signals


def gen_volume_climax(days, lookback, min_vol_ratio):
    """Volume climax: highest volume bar in N-bar window, directional, next bar reverses."""
    signals = []
    for date, bars in days.items():
        if len(bars) < lookback + 5:
            continue
        for i in range(lookback, len(bars) - 1):
            bar = bars[i]
            next_bar = bars[i + 1]
            if not is_market_hours_et(bar['ts_start']):
                continue

            # Is this the highest volume bar in the lookback window?
            window_vols = [b['volume'] for b in bars[i-lookback:i]]
            max_prev_vol = max(window_vols) if window_vols else 0
            avg_vol = np.mean(window_vols) if window_vols else 0

            if avg_vol == 0 or bar['volume'] < min_vol_ratio * avg_vol:
                continue

            if max_prev_vol > 0 and bar['volume'] <= max_prev_vol:
                continue  # Not the highest

            # Bar must be directional (full range, or at least clear direction)
            bar_up = bar['close'] > bar['open']
            bar_down = bar['close'] < bar['open']
            next_up = next_bar['close'] > next_bar['open']
            next_down = next_bar['close'] < next_bar['open']

            direction = None
            # Climax up + next reverses down
            if bar_up and next_down:
                direction = 'short'
            # Climax down + next reverses up
            elif bar_down and next_up:
                direction = 'long'

            if direction:
                signals.append((date, i + 1, direction, next_bar['ts_start']))
    return signals


# ── Apply cooldown ──────────────────────────────────────────────────
def apply_cooldown(signals, cooldown_bars=COOLDOWN_BARS):
    """Remove signals within cooldown_bars of last same-direction signal per day."""
    filtered = []
    last_long = {}  # date -> bar_idx
    last_short = {}
    for date, idx, direction, ts in signals:
        if direction == 'long':
            if date in last_long and idx - last_long[date] < cooldown_bars:
                continue
            last_long[date] = idx
        else:
            if date in last_short and idx - last_short[date] < cooldown_bars:
                continue
            last_short[date] = idx
        filtered.append((date, idx, direction, ts))
    return filtered


# ── Run backtest for a set of signals ───────────────────────────────
def run_backtest(days, signals, sl, target):
    """Run forward sim for all signals, return per-trade results."""
    results = []
    for date, idx, direction, ts in signals:
        bars = days[date]
        if idx >= len(bars) - 1:
            continue
        outcome, pnl, mfe, mae, bars_held = forward_sim(bars, idx, direction, sl, target)
        bucket = get_time_bucket(ts)
        results.append({
            'date': date, 'idx': idx, 'direction': direction,
            'outcome': outcome, 'pnl': pnl, 'mfe': mfe, 'mae': mae,
            'bars_held': bars_held, 'bucket': bucket,
        })
    return results


# ── Compute stats ───────────────────────────────────────────────────
def compute_stats(results):
    if not results:
        return None
    n = len(results)
    wins = sum(1 for r in results if r['outcome'] == 'WIN')
    losses = sum(1 for r in results if r['outcome'] == 'LOSS')
    expired = sum(1 for r in results if r['outcome'] == 'EXPIRED')
    pnls = [r['pnl'] for r in results]
    total_pnl = sum(pnls)
    wr = wins / n * 100 if n > 0 else 0
    avg_mfe = np.mean([r['mfe'] for r in results])
    avg_mae = np.mean([r['mae'] for r in results])
    gross_win = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p < 0))
    pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
    std_pnl = np.std(pnls) if len(pnls) > 1 else 0
    sharpe = total_pnl / std_pnl if std_pnl > 0 else 0

    longs = [r for r in results if r['direction'] == 'long']
    shorts = [r for r in results if r['direction'] == 'short']

    return {
        'n': n, 'wins': wins, 'losses': losses, 'expired': expired,
        'wr': wr, 'pnl': total_pnl, 'avg_mfe': avg_mfe, 'avg_mae': avg_mae,
        'pf': pf, 'sharpe': sharpe, 'std_pnl': std_pnl,
        'n_longs': len(longs), 'long_wr': sum(1 for r in longs if r['outcome']=='WIN')/len(longs)*100 if longs else 0,
        'long_pnl': sum(r['pnl'] for r in longs),
        'n_shorts': len(shorts), 'short_wr': sum(1 for r in shorts if r['outcome']=='WIN')/len(shorts)*100 if shorts else 0,
        'short_pnl': sum(r['pnl'] for r in shorts),
    }


def compute_bucket_stats(results):
    buckets = defaultdict(list)
    for r in results:
        if r['bucket']:
            buckets[r['bucket']].append(r)
    out = {}
    for bname, bres in buckets.items():
        out[bname] = compute_stats(bres)
    return out


# ── Main backtest loop ──────────────────────────────────────────────
def main():
    start = _time.time()
    days = load_data()

    all_results = []  # (setup_name, params_str, sl, target, stats, bucket_stats, results)

    # ────────────────────────────────────────────────────────────────
    # 1. Single-Bar Absorption (SB)
    # ────────────────────────────────────────────────────────────────
    print("\n═══ 1. Single-Bar Absorption (SB) ═══")
    vol_mults = [1.3, 1.5, 1.8, 2.0, 2.5]
    delta_mults = [0.8, 1.0, 1.2, 1.5, 2.0]
    total_sb = len(vol_mults) * len(delta_mults) * len(RM_COMBOS)
    done = 0
    for vm in vol_mults:
        for dm in delta_mults:
            signals = gen_single_bar_absorption(days, vm, dm)
            signals = apply_cooldown(signals)
            if not signals:
                done += len(RM_COMBOS)
                continue
            for sl, tgt in RM_COMBOS:
                results = run_backtest(days, signals, sl, tgt)
                stats = compute_stats(results)
                if stats and stats['n'] >= 3:
                    bstats = compute_bucket_stats(results)
                    params = f"vol={vm},delta={dm}"
                    all_results.append(('SB', params, sl, tgt, stats, bstats, results))
                done += 1
                if done % 25 == 0:
                    print(f"  SB: {done}/{total_sb} combos done ({len(signals)} signals for vol={vm},delta={dm})")
    print(f"  SB complete: {done} combos tested")

    # ────────────────────────────────────────────────────────────────
    # 2. Two-Bar Absorption (SB2)
    # ────────────────────────────────────────────────────────────────
    print("\n═══ 2. Two-Bar Absorption (SB2) ═══")
    vol_mults_2 = [1.2, 1.5, 1.8]
    delta_mults_2 = [1.0, 1.5, 2.0]
    recovery_pcts = [0.50, 0.60, 0.70, 0.80]
    total_sb2 = len(vol_mults_2) * len(delta_mults_2) * len(recovery_pcts) * len(RM_COMBOS)
    done = 0
    for vm in vol_mults_2:
        for dm in delta_mults_2:
            for rp in recovery_pcts:
                signals = gen_two_bar_absorption(days, vm, dm, rp)
                signals = apply_cooldown(signals)
                if not signals:
                    done += len(RM_COMBOS)
                    continue
                for sl, tgt in RM_COMBOS:
                    results = run_backtest(days, signals, sl, tgt)
                    stats = compute_stats(results)
                    if stats and stats['n'] >= 3:
                        bstats = compute_bucket_stats(results)
                        params = f"vol={vm},delta={dm},rec={rp}"
                        all_results.append(('SB2', params, sl, tgt, stats, bstats, results))
                    done += 1
                    if done % 25 == 0:
                        print(f"  SB2: {done}/{total_sb2} combos done")
    print(f"  SB2 complete: {done} combos tested")

    # ────────────────────────────────────────────────────────────────
    # 3. Multi-Bar CVD Divergence
    # ────────────────────────────────────────────────────────────────
    print("\n═══ 3. Multi-Bar CVD Divergence ═══")
    lookbacks = [5, 8, 10, 15]
    div_gaps = [0.2, 0.3, 0.5]
    vol_gates = [1.0, 1.5, 2.0]
    total_div = len(lookbacks) * len(div_gaps) * len(vol_gates) * len(RM_COMBOS)
    done = 0
    for lb in lookbacks:
        for dg in div_gaps:
            for vg in vol_gates:
                signals = gen_multi_bar_divergence(days, lb, dg, vg)
                signals = apply_cooldown(signals)
                if not signals:
                    done += len(RM_COMBOS)
                    continue
                for sl, tgt in RM_COMBOS:
                    results = run_backtest(days, signals, sl, tgt)
                    stats = compute_stats(results)
                    if stats and stats['n'] >= 3:
                        bstats = compute_bucket_stats(results)
                        params = f"lb={lb},gap={dg},vol={vg}"
                        all_results.append(('CVD-Div', params, sl, tgt, stats, bstats, results))
                    done += 1
                    if done % 50 == 0:
                        print(f"  CVD-Div: {done}/{total_div} combos done")
    print(f"  CVD-Div complete: {done} combos tested")

    # ────────────────────────────────────────────────────────────────
    # 4. CVD Reversal
    # ────────────────────────────────────────────────────────────────
    print("\n═══ 4. CVD Reversal ═══")
    cvd_lookbacks = [10, 20, 30]
    vol_mults_4 = [1.0, 1.5, 2.0]
    total_cvdr = len(cvd_lookbacks) * len(vol_mults_4) * len(RM_COMBOS)
    done = 0
    for clb in cvd_lookbacks:
        for vm in vol_mults_4:
            signals = gen_cvd_reversal(days, clb, vm)
            signals = apply_cooldown(signals)
            if not signals:
                done += len(RM_COMBOS)
                continue
            for sl, tgt in RM_COMBOS:
                results = run_backtest(days, signals, sl, tgt)
                stats = compute_stats(results)
                if stats and stats['n'] >= 3:
                    bstats = compute_bucket_stats(results)
                    params = f"lb={clb},vol={vm}"
                    all_results.append(('CVD-Rev', params, sl, tgt, stats, bstats, results))
                done += 1
                if done % 10 == 0:
                    print(f"  CVD-Rev: {done}/{total_cvdr} combos done")
    print(f"  CVD-Rev complete: {done} combos tested")

    # ────────────────────────────────────────────────────────────────
    # 5. Delta Spike + Follow-Through
    # ────────────────────────────────────────────────────────────────
    print("\n═══ 5. Delta Spike + Follow-Through ═══")
    delta_mults_5 = [2.0, 2.5, 3.0]
    modes = ['continuation', 'reversal']
    total_ds = len(delta_mults_5) * len(modes) * len(RM_COMBOS)
    done = 0
    for dm in delta_mults_5:
        for mode in modes:
            signals = gen_delta_spike(days, dm, mode)
            signals = apply_cooldown(signals)
            if not signals:
                done += len(RM_COMBOS)
                continue
            for sl, tgt in RM_COMBOS:
                results = run_backtest(days, signals, sl, tgt)
                stats = compute_stats(results)
                if stats and stats['n'] >= 3:
                    bstats = compute_bucket_stats(results)
                    params = f"delta={dm},mode={mode}"
                    all_results.append(('DeltaSpike', params, sl, tgt, stats, bstats, results))
                done += 1
                if done % 10 == 0:
                    print(f"  DeltaSpike: {done}/{total_ds} combos done")
    print(f"  DeltaSpike complete: {done} combos tested")

    # ────────────────────────────────────────────────────────────────
    # 6. Volume Climax Reversal
    # ────────────────────────────────────────────────────────────────
    print("\n═══ 6. Volume Climax Reversal ═══")
    vc_lookbacks = [10, 20, 30]
    vc_ratios = [2.0, 2.5, 3.0]
    total_vc = len(vc_lookbacks) * len(vc_ratios) * len(RM_COMBOS)
    done = 0
    for vlb in vc_lookbacks:
        for vr in vc_ratios:
            signals = gen_volume_climax(days, vlb, vr)
            signals = apply_cooldown(signals)
            if not signals:
                done += len(RM_COMBOS)
                continue
            for sl, tgt in RM_COMBOS:
                results = run_backtest(days, signals, sl, tgt)
                stats = compute_stats(results)
                if stats and stats['n'] >= 3:
                    bstats = compute_bucket_stats(results)
                    params = f"lb={vlb},ratio={vr}"
                    all_results.append(('VolClimax', params, sl, tgt, stats, bstats, results))
                done += 1
                if done % 10 == 0:
                    print(f"  VolClimax: {done}/{total_vc} combos done")
    print(f"  VolClimax complete: {done} combos tested")

    elapsed = _time.time() - start
    print(f"\n{'='*100}")
    print(f"BACKTEST COMPLETE: {len(all_results)} valid parameter combos tested in {elapsed:.1f}s")
    print(f"{'='*100}")

    # ── OUTPUT RESULTS ──────────────────────────────────────────────

    def fmt_row(setup, params, sl, tgt, s):
        return (f"{setup:12s} {params:35s} SL={sl:2d}/T={tgt:2d}  "
                f"N={s['n']:4d}  W={s['wins']:3d}  L={s['losses']:3d}  E={s['expired']:2d}  "
                f"WR={s['wr']:5.1f}%  PnL={s['pnl']:+8.1f}  "
                f"MFE={s['avg_mfe']:5.1f}  MAE={s['avg_mae']:5.1f}  "
                f"PF={s['pf']:5.2f}  Sharpe={s['sharpe']:+6.2f}  "
                f"L:{s['n_longs']:3d}({s['long_wr']:4.0f}%)={s['long_pnl']:+6.1f}  "
                f"S:{s['n_shorts']:3d}({s['short_wr']:4.0f}%)={s['short_pnl']:+6.1f}")

    # ── TOP 20 by PnL ──────────────────────────────────────────────
    print(f"\n{'='*100}")
    print("TOP 20 PARAMETER COMBOS BY PnL")
    print(f"{'='*100}")
    sorted_pnl = sorted(all_results, key=lambda x: x[4]['pnl'], reverse=True)[:20]
    for i, (setup, params, sl, tgt, stats, _, _) in enumerate(sorted_pnl, 1):
        print(f"{i:2d}. {fmt_row(setup, params, sl, tgt, stats)}")

    # ── TOP 10 by Profit Factor (min 20 signals) ───────────────────
    print(f"\n{'='*100}")
    print("TOP 10 BY PROFIT FACTOR (min 20 signals)")
    print(f"{'='*100}")
    filtered_pf = [x for x in all_results if x[4]['n'] >= 20]
    sorted_pf = sorted(filtered_pf, key=lambda x: x[4]['pf'], reverse=True)[:10]
    for i, (setup, params, sl, tgt, stats, _, _) in enumerate(sorted_pf, 1):
        print(f"{i:2d}. {fmt_row(setup, params, sl, tgt, stats)}")

    # ── TOP 5 by Sharpe (min 20 signals) ───────────────────────────
    print(f"\n{'='*100}")
    print("TOP 5 BY SHARPE-LIKE RATIO (min 20 signals)")
    print(f"{'='*100}")
    sorted_sharpe = sorted(filtered_pf, key=lambda x: x[4]['sharpe'], reverse=True)[:5]
    for i, (setup, params, sl, tgt, stats, _, _) in enumerate(sorted_sharpe, 1):
        print(f"{i:2d}. {fmt_row(setup, params, sl, tgt, stats)}")

    # ── TIME-OF-DAY breakdown for top combos ────────────────────────
    print(f"\n{'='*100}")
    print("TIME-OF-DAY BREAKDOWN FOR TOP 10 BY PnL")
    print(f"{'='*100}")
    for i, (setup, params, sl, tgt, stats, bstats, _) in enumerate(sorted_pnl[:10], 1):
        print(f"\n{i:2d}. {setup} | {params} | SL={sl}/T={tgt} | Total: N={stats['n']} WR={stats['wr']:.1f}% PnL={stats['pnl']:+.1f}")
        for bname in ["09:30-10:30", "10:30-12:00", "12:00-14:00", "14:00-15:30"]:
            if bname in bstats:
                bs = bstats[bname]
                print(f"    {bname}: N={bs['n']:3d}  WR={bs['wr']:5.1f}%  PnL={bs['pnl']:+7.1f}  PF={bs['pf']:5.2f}  "
                      f"L:{bs['n_longs']}({bs['long_wr']:.0f}%)  S:{bs['n_shorts']}({bs['short_wr']:.0f}%)")
            else:
                print(f"    {bname}: no signals")

    # ── RECOMMENDED: Best 2-3 different concepts ────────────────────
    print(f"\n{'='*100}")
    print("RECOMMENDED SETUPS (best from different concepts, min 20 signals)")
    print(f"{'='*100}")

    # Pick best per concept by PnL (min 20 signals)
    concept_best = {}
    for setup, params, sl, tgt, stats, bstats, results in all_results:
        if stats['n'] < 20:
            continue
        key = setup
        if key not in concept_best or stats['pnl'] > concept_best[key][4]['pnl']:
            concept_best[key] = (setup, params, sl, tgt, stats, bstats, results)

    ranked_concepts = sorted(concept_best.values(), key=lambda x: x[4]['pnl'], reverse=True)

    for i, (setup, params, sl, tgt, stats, bstats, results) in enumerate(ranked_concepts[:5], 1):
        print(f"\n{'─'*90}")
        print(f"  #{i} {setup} | {params} | SL={sl}/T={tgt}")
        print(f"  Signals: {stats['n']}  |  Wins: {stats['wins']}  |  Losses: {stats['losses']}  |  Expired: {stats['expired']}")
        print(f"  WR: {stats['wr']:.1f}%  |  PnL: {stats['pnl']:+.1f} pts  |  PF: {stats['pf']:.2f}  |  Sharpe: {stats['sharpe']:+.2f}")
        print(f"  Avg MFE: {stats['avg_mfe']:.1f} pts  |  Avg MAE: {stats['avg_mae']:.1f} pts")
        print(f"  Longs:  {stats['n_longs']} ({stats['long_wr']:.0f}% WR, {stats['long_pnl']:+.1f} pts)")
        print(f"  Shorts: {stats['n_shorts']} ({stats['short_wr']:.0f}% WR, {stats['short_pnl']:+.1f} pts)")
        print(f"  Time breakdown:")
        for bname in ["09:30-10:30", "10:30-12:00", "12:00-14:00", "14:00-15:30"]:
            if bname in bstats:
                bs = bstats[bname]
                print(f"    {bname}: N={bs['n']:3d}  WR={bs['wr']:5.1f}%  PnL={bs['pnl']:+7.1f}  PF={bs['pf']:5.2f}")

    # ── Per-concept summary table ───────────────────────────────────
    print(f"\n{'='*100}")
    print("CONCEPT SUMMARY (best combo per concept, min 10 signals)")
    print(f"{'='*100}")
    concept_best_10 = {}
    for setup, params, sl, tgt, stats, bstats, results in all_results:
        if stats['n'] < 10:
            continue
        key = setup
        if key not in concept_best_10 or stats['pnl'] > concept_best_10[key][4]['pnl']:
            concept_best_10[key] = (setup, params, sl, tgt, stats, bstats, results)

    print(f"{'Setup':12s} {'Params':35s} {'SL/T':8s} {'N':>5s} {'WR%':>6s} {'PnL':>8s} {'PF':>6s} {'Sharpe':>7s} {'MFE':>6s} {'MAE':>6s}")
    print("─" * 100)
    for setup, params, sl, tgt, stats, _, _ in sorted(concept_best_10.values(), key=lambda x: x[4]['pnl'], reverse=True):
        print(f"{setup:12s} {params:35s} {sl:2d}/{tgt:2d}    {stats['n']:5d} {stats['wr']:5.1f}% {stats['pnl']:+7.1f} {stats['pf']:5.2f} {stats['sharpe']:+6.2f} {stats['avg_mfe']:5.1f} {stats['avg_mae']:5.1f}")

    # ── Signals per date for top combo ──────────────────────────────
    if ranked_concepts:
        best = ranked_concepts[0]
        print(f"\n{'='*100}")
        print(f"DAILY BREAKDOWN: {best[0]} | {best[1]} | SL={best[2]}/T={best[3]}")
        print(f"{'='*100}")
        daily = defaultdict(list)
        for r in best[6]:
            daily[r['date']].append(r)
        print(f"{'Date':>12s} {'N':>4s} {'W':>3s} {'L':>3s} {'E':>3s} {'PnL':>8s} {'WR%':>6s}")
        for d in sorted(daily.keys()):
            dr = daily[d]
            n = len(dr)
            w = sum(1 for r in dr if r['outcome'] == 'WIN')
            l = sum(1 for r in dr if r['outcome'] == 'LOSS')
            e = sum(1 for r in dr if r['outcome'] == 'EXPIRED')
            pnl = sum(r['pnl'] for r in dr)
            wr = w/n*100 if n else 0
            print(f"  {d}  {n:4d}  {w:3d}  {l:3d}  {e:3d}  {pnl:+7.1f}  {wr:5.1f}%")

    print(f"\nTotal execution time: {_time.time()-start:.1f}s")


if __name__ == '__main__':
    main()
