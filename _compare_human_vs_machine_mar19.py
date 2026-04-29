#!/usr/bin/env python3
"""
Human vs Machine Absorption Comparison — March 19, 2026

Loads all 5-pt Rithmic range bars for Mar 19, maps to 14 user-identified
absorption signals, computes detailed metrics (including intra-bar max/min
delta), runs Config F trail sim, then runs MEDIUM machine detector and
compares.

Usage: railway run -- python -u _compare_human_vs_machine_mar19.py
       or: python -u _compare_human_vs_machine_mar19.py  (with DATABASE_URL set)
"""

import os, sys, json, math, re, csv
from datetime import datetime, time as dtime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

sys.stdout.reconfigure(line_buffering=True, encoding='utf-8', errors='replace')

NY = ZoneInfo("America/New_York")
DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    print("ERROR: DATABASE_URL not set. Use 'railway run' or set it in env.")
    sys.exit(1)
engine = create_engine(DB_URL)

TRADE_DATE = "2026-03-19"

# Config F trail: SL=8, immediate trail gap=8, no TP, timeout=100 bars
CONFIG_F = {
    'name': 'F: No TP, immediate trail gap=8',
    'sl': 8, 'be_trigger': None, 'activation': 0, 'gap': 8,
    'has_be': False, 'has_activation': False,
}

MEDIUM_CRITERIA = {
    'min_abs_delta_single': 300,
    'min_abs_delta_multi': 600,
    'max_body_ratio': 0.75,
    'min_volume': 3000,
    'two_bar_window': True,
    'cooldown_bars': 10,
    'market_hours_only': True,
}

# ═══════════════════════════════════════════════════════════════════════
# USER-IDENTIFIED SIGNALS (14 total from Mar 19)
# Time is ET, direction as specified by user
# ═══════════════════════════════════════════════════════════════════════

USER_SIGNALS = [
    {
        'id': 1,
        'time_et': '09:35',
        'direction': 'bullish',
        'note': '-delta, green bar',
        'multi_bar': False,
        'confirmation': 2,   # signal 2 is confirmation of 1
    },
    {
        'id': 2,
        'time_et': '09:39',
        'direction': 'bullish',
        'note': 'confirmation of #1',
        'multi_bar': False,
        'is_confirmation': True,
    },
    {
        'id': 3,
        'time_et': '09:46',
        'direction': 'bullish',
        'note': '2 bars at 9:46, combined flat, net -200 delta',
        'multi_bar': True,
        'n_bars': 2,
    },
    {
        'id': 4,
        'time_et': '09:52',  # midpoint of 9:50-9:54
        'direction': 'bearish',
        'note': 'confirm, probably lost',
        'multi_bar': True,
        'n_bars': 2,
        'user_outcome_note': 'probably lost',
    },
    {
        'id': 5,
        'time_et': '10:11',
        'direction': 'bearish',
        'note': 'very strong, high volume, high +delta, max delta even higher, big red close -> market 6650->6622',
        'multi_bar': False,
        'user_outcome_note': 'market 6650->6622 = +28 pts',
    },
    {
        'id': 6,
        'time_et': '11:05',
        'direction': 'bearish',
        'note': '11:05 + bar after, combined +400 delta, closed flat',
        'multi_bar': True,
        'n_bars': 2,
    },
    {
        'id': 7,
        'time_et': '11:36',
        'direction': 'bullish',
        'note': '-477 delta, closed green, "have DD but moved in favor strong"',
        'multi_bar': False,
    },
    {
        'id': 8,
        'time_et': '12:07',
        'direction': 'bearish',
        'note': '+delta, red candle',
        'multi_bar': False,
    },
    {
        'id': 9,
        'time_et': '12:35',
        'direction': 'bullish',
        'note': '-delta, closed green',
        'multi_bar': False,
    },
    {
        'id': 10,
        'time_et': '12:41',  # midpoint of 12:40-12:42
        'direction': 'bearish',
        'note': 'combined +1200 delta, closed flat -> market moved +20 pts in favor',
        'multi_bar': True,
        'n_bars': 2,
        'user_outcome_note': '+20 pts in favor',
    },
    {
        'id': 11,
        'time_et': '12:52',
        'direction': 'bullish',
        'note': '-delta, green close, but lost',
        'multi_bar': False,
        'user_outcome_note': 'lost',
    },
    {
        'id': 12,
        'time_et': '14:13',
        'direction': 'bearish',
        'note': 'slightly +delta, doji bar, "all delta is on the wick"',
        'multi_bar': False,
    },
    {
        'id': 13,
        'time_et': '14:41',
        'direction': 'bullish',
        'note': 'very strong, -900 delta, green close, "all -delta below at wick, min delta -1300!" -> market 6629->6668',
        'multi_bar': False,
        'user_outcome_note': 'market 6629->6668 = +39 pts. Bearish at 14:58 countered.',
    },
    {
        'id': 14,
        'time_et': '15:04',
        'direction': 'bullish',
        'note': 'continuation of #13',
        'multi_bar': False,
    },
]


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════

def load_bars():
    """Load all 5-pt Rithmic range bars for Mar 19."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume, bar_delta, bar_buy_volume, bar_sell_volume,
                   cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
                   ts_start AT TIME ZONE 'America/New_York' as ts_start_et,
                   ts_end AT TIME ZONE 'America/New_York' as ts_end_et,
                   EXTRACT(EPOCH FROM (ts_end - ts_start))::int as duration_sec
            FROM es_range_bars
            WHERE source = 'rithmic' AND trade_date = :td
              AND range_pts = 5.0 AND status = 'closed'
            ORDER BY bar_idx
        """), {"td": TRADE_DATE}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])
    if len(df) == 0:
        print(f"ERROR: No bars found for {TRADE_DATE}")
        sys.exit(1)
    _add_derived_cols(df)
    return df


def _add_derived_cols(df):
    df['is_green'] = df['bar_close'] > df['bar_open']
    df['body'] = (df['bar_close'] - df['bar_open']).abs()
    df['bar_range'] = df['bar_high'] - df['bar_low']
    df['body_ratio'] = df['body'] / df['bar_range'].replace(0, np.nan)
    df['duration_s'] = df['duration_sec'].astype(float)
    df['vol_per_sec'] = df['bar_volume'] / df['duration_s'].replace(0, np.nan)
    df['delta_per_sec'] = df['bar_delta'].abs() / df['duration_s'].replace(0, np.nan)
    # Intra-bar max/min delta (peak pressure within bar)
    df['max_delta_intrabar'] = df['cvd_high'] - df['cvd_open']
    df['min_delta_intrabar'] = df['cvd_low'] - df['cvd_open']
    df['abs_ratio'] = df['bar_delta'].abs() / df['bar_volume'].replace(0, np.nan)

    # Rolling 20-bar averages (shifted to exclude current bar)
    df['vol_avg_20'] = df['bar_volume'].rolling(20, min_periods=1).mean().shift(1)
    df['delta_avg_20'] = df['bar_delta'].abs().rolling(20, min_periods=1).mean().shift(1)
    df['vps_avg_20'] = df['vol_per_sec'].rolling(20, min_periods=1).mean().shift(1)
    df['delta_std_20'] = df['bar_delta'].rolling(20, min_periods=5).std().shift(1)

    df['vol_ratio'] = df['bar_volume'] / df['vol_avg_20'].replace(0, np.nan)
    df['vol_rate_ratio'] = df['vol_per_sec'] / df['vps_avg_20'].replace(0, np.nan)
    df['delta_ratio'] = df['bar_delta'].abs() / df['delta_avg_20'].replace(0, np.nan)
    df['delta_zscore'] = df['bar_delta'] / df['delta_std_20'].replace(0, np.nan)

    # 5-bar price trend
    df['price_trend_5'] = df['bar_close'] - df['bar_close'].shift(5)
    # Trend consistency: how many of last 5 bars moved same direction
    for i in range(len(df)):
        if i < 5:
            df.loc[df.index[i], 'trend_consistency_5'] = np.nan
            continue
        changes = df['bar_close'].iloc[i-4:i+1].diff().dropna()
        if len(changes) == 0:
            df.loc[df.index[i], 'trend_consistency_5'] = 0
        else:
            up = (changes > 0).sum()
            down = (changes < 0).sum()
            df.loc[df.index[i], 'trend_consistency_5'] = max(up, down) / len(changes)


# ═══════════════════════════════════════════════════════════════════════
# SIGNAL BAR MATCHING
# ═══════════════════════════════════════════════════════════════════════

def time_to_seconds(t_str):
    """Convert 'HH:MM' or 'HH:MM:SS' to seconds since midnight."""
    parts = t_str.split(':')
    h, m = int(parts[0]), int(parts[1])
    s = int(parts[2]) if len(parts) > 2 else 0
    return h * 3600 + m * 60 + s


def bar_time_seconds(ts):
    """Get seconds since midnight from a timestamp."""
    if hasattr(ts, 'hour'):
        return ts.hour * 3600 + ts.minute * 60 + ts.second
    return 0


def find_closest_bar(df, time_et, n_bars=1):
    """Find the bar(s) closest to the given ET time.

    For single bars: returns the bar whose ts_end is closest.
    For multi-bar: returns n_bars consecutive bars closest to the time.
    """
    target_s = time_to_seconds(time_et)

    # Compute distance from each bar's end time to target
    df = df.copy()
    df['_dist'] = df['ts_end_et'].apply(lambda x: abs(bar_time_seconds(x) - target_s))

    if n_bars == 1:
        closest_idx = df['_dist'].idxmin()
        return df.loc[[closest_idx]]
    else:
        # Find the closest bar, then include the one before/after
        closest_idx = df['_dist'].idxmin()
        pos = df.index.get_loc(closest_idx)

        # Try to get n_bars centered around the closest
        start = max(0, pos)
        end = min(len(df), start + n_bars)
        if end - start < n_bars:
            start = max(0, end - n_bars)

        return df.iloc[start:end]


# ═══════════════════════════════════════════════════════════════════════
# TRAIL SIMULATION (Config F)
# ═══════════════════════════════════════════════════════════════════════

def simulate_trail(df, signal_idx, direction, entry_price, config=CONFIG_F, max_bars=100):
    """Simulate Config F trail: SL=8, immediate trail gap=8, no TP."""
    mask = df['bar_idx'] > signal_idx
    future = df.loc[mask].head(max_bars)
    if len(future) == 0:
        return {'outcome': 'NO_DATA', 'pnl': 0, 'max_profit': 0, 'bars_held': 0,
                'exit_price': entry_price, 'mfe': 0, 'mae': 0,
                'price_5': None, 'price_10': None, 'price_20': None}

    sl = config['sl']
    gap = config['gap']
    max_profit = 0
    mae_val = 0

    # Track prices at +5, +10, +20 bars
    price_at = {}
    for target_n in [5, 10, 20]:
        if len(future) >= target_n:
            price_at[target_n] = float(future.iloc[target_n - 1]['bar_close'])
        else:
            price_at[target_n] = None

    for i, (_, bar) in enumerate(future.iterrows()):
        if direction == 'bullish':
            bar_profit_high = bar['bar_high'] - entry_price
            bar_profit_low = bar['bar_low'] - entry_price
        else:
            bar_profit_high = entry_price - bar['bar_low']
            bar_profit_low = entry_price - bar['bar_high']

        max_profit = max(max_profit, bar_profit_high)
        mae_val = max(mae_val, -bar_profit_low)

        # Config F: immediate trail — stop starts at -sl, moves up as profit grows
        stop_level = max(max_profit - gap, -sl)

        if bar_profit_low <= stop_level:
            pnl = stop_level
            if direction == 'bullish':
                exit_price = entry_price + stop_level
            else:
                exit_price = entry_price - stop_level
            outcome = 'WIN' if pnl > 0 else ('BE' if pnl == 0 else 'LOSS')
            return {
                'outcome': outcome, 'pnl': round(pnl, 2),
                'max_profit': round(max_profit, 2), 'bars_held': i + 1,
                'exit_price': round(exit_price, 2),
                'mfe': round(max_profit, 2), 'mae': round(mae_val, 2),
                'price_5': price_at.get(5), 'price_10': price_at.get(10),
                'price_20': price_at.get(20),
            }

    # Expired at bar 100
    last_close = float(future.iloc[-1]['bar_close'])
    pnl = (last_close - entry_price) if direction == 'bullish' else (entry_price - last_close)
    outcome = 'WIN' if pnl > 0 else ('BE' if pnl == 0 else 'LOSS')
    return {
        'outcome': outcome, 'pnl': round(pnl, 2),
        'max_profit': round(max_profit, 2), 'bars_held': len(future),
        'exit_price': round(last_close, 2),
        'mfe': round(max_profit, 2), 'mae': round(mae_val, 2),
        'price_5': price_at.get(5), 'price_10': price_at.get(10),
        'price_20': price_at.get(20),
    }


# ═══════════════════════════════════════════════════════════════════════
# MFE/MAE TRACKER (next 30 bars, per bar)
# ═══════════════════════════════════════════════════════════════════════

def compute_mfe_mae_30(df, signal_idx, direction, entry_price):
    """Compute MFE and MAE over next 30 bars from entry."""
    mask = df['bar_idx'] > signal_idx
    future = df.loc[mask].head(30)
    if len(future) == 0:
        return 0, 0

    mfe = 0
    mae = 0
    for _, bar in future.iterrows():
        if direction == 'bullish':
            profit_high = bar['bar_high'] - entry_price
            profit_low = bar['bar_low'] - entry_price
        else:
            profit_high = entry_price - bar['bar_low']
            profit_low = entry_price - bar['bar_high']
        mfe = max(mfe, profit_high)
        mae = max(mae, -profit_low)
    return round(mfe, 2), round(mae, 2)


# ═══════════════════════════════════════════════════════════════════════
# MACHINE DETECTOR (MEDIUM criteria from v2 study)
# ═══════════════════════════════════════════════════════════════════════

def scan_machine_signals(df, criteria=MEDIUM_CRITERIA):
    """Run MEDIUM criteria detector on a single day's bars.

    DIRECTION LOGIC:
    - Positive delta + price flat/red/small body = BEARISH (buyers absorbed)
    - Negative delta + price flat/green/small body = BULLISH (sellers absorbed)
    """
    signals = []
    if len(df) < 25:
        return signals

    last_bull_idx = -100
    last_bear_idx = -100
    cd = criteria['cooldown_bars']

    for i in range(20, len(df)):
        bar = df.iloc[i]
        idx = int(bar['bar_idx'])

        # Market hours
        if criteria.get('market_hours_only', True):
            et = bar['ts_end_et']
            if hasattr(et, 'time'):
                if et.time() < dtime(9, 35) or et.time() > dtime(15, 55):
                    continue

        # ---- Single bar check ----
        single_hit = False
        direction = None
        is_multi = False
        combined_delta = None
        combined_vol = None
        combined_body_ratio = None

        if (abs(bar['bar_delta']) >= criteria['min_abs_delta_single'] and
            bar['bar_volume'] >= criteria['min_volume'] and
            pd.notna(bar['body_ratio']) and bar['body_ratio'] <= criteria['max_body_ratio']):

            if bar['bar_delta'] > 0 and (not bar['is_green'] or bar['body_ratio'] <= 0.40):
                direction = 'bearish'
                single_hit = True
            elif bar['bar_delta'] < 0 and (bar['is_green'] or bar['body_ratio'] <= 0.40):
                direction = 'bullish'
                single_hit = True

        # ---- Two-bar check ----
        two_bar_hit = False
        if not single_hit and criteria.get('two_bar_window', True) and i >= 1:
            prev = df.iloc[i - 1]
            combined_delta = int(bar['bar_delta'] + prev['bar_delta'])
            combined_vol = int(bar['bar_volume'] + prev['bar_volume'])
            net_price = bar['bar_close'] - prev['bar_open']
            combined_range = max(bar['bar_high'], prev['bar_high']) - min(bar['bar_low'], prev['bar_low'])
            combined_body_ratio = abs(net_price) / combined_range if combined_range > 0 else 1

            if (abs(combined_delta) >= criteria['min_abs_delta_multi'] and
                combined_vol >= criteria['min_volume'] and
                combined_body_ratio <= criteria['max_body_ratio']):

                if combined_delta > 0 and (net_price <= 0 or combined_body_ratio <= 0.40):
                    direction = 'bearish'
                    two_bar_hit = True
                    is_multi = True
                elif combined_delta < 0 and (net_price >= 0 or combined_body_ratio <= 0.40):
                    direction = 'bullish'
                    two_bar_hit = True
                    is_multi = True

        if not single_hit and not two_bar_hit:
            continue

        # Cooldown check
        if direction == 'bullish' and idx - last_bull_idx < cd:
            continue
        if direction == 'bearish' and idx - last_bear_idx < cd:
            continue

        entry_price = float(bar['bar_close'])

        sig = {
            'bar_idx': idx,
            'direction': direction,
            'is_multi': is_multi,
            'entry_price': entry_price,
            'et_end': bar['ts_end_et'].strftime('%H:%M:%S') if hasattr(bar['ts_end_et'], 'strftime') else str(bar['ts_end_et']),
            'et_start': bar['ts_start_et'].strftime('%H:%M:%S') if hasattr(bar['ts_start_et'], 'strftime') else str(bar['ts_start_et']),
            'delta': int(bar['bar_delta']) if single_hit else combined_delta,
            'volume': int(bar['bar_volume']) if single_hit else combined_vol,
            'body_ratio': float(bar['body_ratio']) if single_hit else combined_body_ratio,
            'vol_ratio': float(bar['vol_ratio']) if pd.notna(bar['vol_ratio']) else None,
            'max_delta_intrabar': float(bar['max_delta_intrabar']) if pd.notna(bar['max_delta_intrabar']) else None,
            'min_delta_intrabar': float(bar['min_delta_intrabar']) if pd.notna(bar['min_delta_intrabar']) else None,
        }

        signals.append(sig)
        if direction == 'bullish':
            last_bull_idx = idx
        else:
            last_bear_idx = idx

    return signals


# ═══════════════════════════════════════════════════════════════════════
# THRESHOLD DIAGNOSIS: Why did machine miss a signal?
# ═══════════════════════════════════════════════════════════════════════

def diagnose_miss(df, bar_indices, user_direction, criteria=MEDIUM_CRITERIA):
    """Check which MEDIUM thresholds block a user-identified signal.
    Returns list of (threshold_name, required, actual, pass/fail).
    """
    results = []

    # Get primary bar (last of multi)
    bars_in_signal = df[df['bar_idx'].isin(bar_indices)]
    if len(bars_in_signal) == 0:
        return [("NO_BARS", "bars exist", "none found", "FAIL")]

    bar = bars_in_signal.iloc[-1]  # trigger bar
    idx = int(bar['bar_idx'])

    # Check market hours
    et = bar['ts_end_et']
    if hasattr(et, 'time'):
        in_hours = dtime(9, 35) <= et.time() <= dtime(15, 55)
    else:
        in_hours = True
    results.append(("market_hours", "9:35-15:55", f"{et}", "PASS" if in_hours else "FAIL"))

    # Check if bar position >= 20
    pos = df.index.get_loc(df[df['bar_idx'] == idx].index[0]) if idx in df['bar_idx'].values else -1
    results.append(("min_position", ">=20", f"{pos}", "PASS" if pos >= 20 else "FAIL"))

    # Single bar checks
    abs_delta = abs(float(bar['bar_delta']))
    volume = float(bar['bar_volume'])
    body_ratio_val = float(bar['body_ratio']) if pd.notna(bar['body_ratio']) else 1.0

    results.append(("single_abs_delta", f">={criteria['min_abs_delta_single']}", f"{abs_delta:.0f}",
                     "PASS" if abs_delta >= criteria['min_abs_delta_single'] else "FAIL"))
    results.append(("single_volume", f">={criteria['min_volume']}", f"{volume:.0f}",
                     "PASS" if volume >= criteria['min_volume'] else "FAIL"))
    results.append(("single_body_ratio", f"<={criteria['max_body_ratio']}", f"{body_ratio_val:.3f}",
                     "PASS" if body_ratio_val <= criteria['max_body_ratio'] else "FAIL"))

    # Direction check for single bar
    delta_val = float(bar['bar_delta'])
    is_green = bool(bar['is_green'])

    if user_direction == 'bullish':
        dir_pass = delta_val < 0 and (is_green or body_ratio_val <= 0.40)
        results.append(("single_dir_bullish", "delta<0 AND (green OR body<=0.4)",
                         f"delta={delta_val:.0f}, green={is_green}, body={body_ratio_val:.2f}",
                         "PASS" if dir_pass else "FAIL"))
    else:
        dir_pass = delta_val > 0 and (not is_green or body_ratio_val <= 0.40)
        results.append(("single_dir_bearish", "delta>0 AND (red OR body<=0.4)",
                         f"delta={delta_val:.0f}, green={is_green}, body={body_ratio_val:.2f}",
                         "PASS" if dir_pass else "FAIL"))

    single_pass = (abs_delta >= criteria['min_abs_delta_single'] and
                   volume >= criteria['min_volume'] and
                   body_ratio_val <= criteria['max_body_ratio'] and
                   dir_pass and in_hours and pos >= 20)
    results.append(("SINGLE_BAR_TOTAL", "all pass", "", "PASS" if single_pass else "FAIL"))

    # Two-bar checks
    if len(bar_indices) >= 2:
        first_bar = bars_in_signal.iloc[0]
        comb_delta = float(bars_in_signal['bar_delta'].sum())
        comb_vol = float(bars_in_signal['bar_volume'].sum())
        net_price = float(bar['bar_close'] - first_bar['bar_open'])
        comb_range = float(bars_in_signal['bar_high'].max() - bars_in_signal['bar_low'].min())
        comb_body = abs(net_price) / comb_range if comb_range > 0 else 1

        results.append(("multi_abs_delta", f">={criteria['min_abs_delta_multi']}", f"{abs(comb_delta):.0f}",
                         "PASS" if abs(comb_delta) >= criteria['min_abs_delta_multi'] else "FAIL"))
        results.append(("multi_volume", f">={criteria['min_volume']}", f"{comb_vol:.0f}",
                         "PASS" if comb_vol >= criteria['min_volume'] else "FAIL"))
        results.append(("multi_body_ratio", f"<={criteria['max_body_ratio']}", f"{comb_body:.3f}",
                         "PASS" if comb_body <= criteria['max_body_ratio'] else "FAIL"))
    else:
        # Check with previous bar as 2-bar combo
        bar_pos = df.index.get_loc(df[df['bar_idx'] == idx].index[0])
        if bar_pos >= 1:
            prev = df.iloc[bar_pos - 1]
            comb_delta = float(bar['bar_delta'] + prev['bar_delta'])
            comb_vol = float(bar['bar_volume'] + prev['bar_volume'])
            net_price = float(bar['bar_close'] - prev['bar_open'])
            comb_range = float(max(bar['bar_high'], prev['bar_high']) - min(bar['bar_low'], prev['bar_low']))
            comb_body = abs(net_price) / comb_range if comb_range > 0 else 1

            results.append(("multi_abs_delta (w/prev)", f">={criteria['min_abs_delta_multi']}",
                             f"{abs(comb_delta):.0f}",
                             "PASS" if abs(comb_delta) >= criteria['min_abs_delta_multi'] else "FAIL"))
            results.append(("multi_volume (w/prev)", f">={criteria['min_volume']}", f"{comb_vol:.0f}",
                             "PASS" if comb_vol >= criteria['min_volume'] else "FAIL"))
            results.append(("multi_body_ratio (w/prev)", f"<={criteria['max_body_ratio']}", f"{comb_body:.3f}",
                             "PASS" if comb_body <= criteria['max_body_ratio'] else "FAIL"))

    # Cooldown — need full context, just note it
    results.append(("cooldown", f"{criteria['cooldown_bars']} bars", "check vs prior signals", "CHECK"))

    return results


# ═══════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════

print("=" * 120)
print("HUMAN vs MACHINE ABSORPTION — MARCH 19, 2026")
print("=" * 120)

# ─────────────────────────────────────────────────────────────────────
# STEP 1: Load all bars
# ─────────────────────────────────────────────────────────────────────
print("\n[1] LOADING ALL BARS FOR MAR 19...")
df = load_bars()
print(f"    Loaded {len(df)} closed 5-pt Rithmic range bars")
print(f"    Price range: {df['bar_low'].min():.2f} - {df['bar_high'].max():.2f}")
print(f"    Time range: {df['ts_start_et'].iloc[0]} - {df['ts_end_et'].iloc[-1]}")
print(f"    Total volume: {df['bar_volume'].sum():,.0f}")
print(f"    Avg vol/bar: {df['bar_volume'].mean():.0f}, median: {df['bar_volume'].median():.0f}")
print(f"    Avg |delta|/bar: {df['bar_delta'].abs().mean():.0f}")

# Print ALL bars for reference
print(f"\n{'idx':>5} {'start_ET':>10} {'end_ET':>10} {'O':>8} {'H':>8} {'L':>8} {'C':>8} {'clr':>4} "
      f"{'vol':>6} {'delta':>7} {'buy':>6} {'sell':>6} {'dur':>4} "
      f"{'max_d':>7} {'min_d':>7} {'body%':>5} {'v_rat':>5}")
print("-" * 135)
for _, row in df.iterrows():
    clr = 'G' if row['is_green'] else 'R'
    ts_s = row['ts_start_et'].strftime('%H:%M:%S') if hasattr(row['ts_start_et'], 'strftime') else ''
    ts_e = row['ts_end_et'].strftime('%H:%M:%S') if hasattr(row['ts_end_et'], 'strftime') else ''
    vr = f"{row['vol_ratio']:.1f}" if pd.notna(row['vol_ratio']) else "n/a"
    br = f"{row['body_ratio']:.2f}" if pd.notna(row['body_ratio']) else "n/a"
    max_d = f"{row['max_delta_intrabar']:+.0f}" if pd.notna(row['max_delta_intrabar']) else "n/a"
    min_d = f"{row['min_delta_intrabar']:+.0f}" if pd.notna(row['min_delta_intrabar']) else "n/a"
    print(f"{int(row['bar_idx']):>5} {ts_s:>10} {ts_e:>10} "
          f"{row['bar_open']:>8.2f} {row['bar_high']:>8.2f} {row['bar_low']:>8.2f} {row['bar_close']:>8.2f} "
          f"{clr:>4} {int(row['bar_volume']):>6} {int(row['bar_delta']):>+7} "
          f"{int(row['bar_buy_volume']):>6} {int(row['bar_sell_volume']):>6} "
          f"{int(row['duration_sec']):>4} {max_d:>7} {min_d:>7} {br:>5} {vr:>5}")

# ─────────────────────────────────────────────────────────────────────
# STEP 2: Process each user signal
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("[2] USER SIGNALS — DETAILED BAR DATA")
print("=" * 120)

user_processed = []

for sig in USER_SIGNALS:
    sig_id = sig['id']
    time_et = sig['time_et']
    direction = sig['direction']
    n_bars = sig.get('n_bars', 1) if sig.get('multi_bar') else 1

    print(f"\n{'─' * 120}")
    print(f"SIGNAL #{sig_id}  |  Time: {time_et} ET  |  Direction: {direction.upper()}  |  "
          f"{'Multi-bar (' + str(n_bars) + ')' if n_bars > 1 else 'Single bar'}")
    print(f"User note: {sig['note']}")
    if sig.get('user_outcome_note'):
        print(f"User outcome: {sig['user_outcome_note']}")
    print()

    # Find closest bar(s)
    matched_bars = find_closest_bar(df, time_et, n_bars)
    bar_indices = list(matched_bars['bar_idx'].astype(int).values)

    print(f"  Matched bar(s): idx={bar_indices}")
    print(f"  {'idx':>5} {'start_ET':>10} {'end_ET':>10} {'O':>8} {'H':>8} {'L':>8} {'C':>8} {'clr':>4} "
          f"{'vol':>6} {'delta':>7} {'buy':>6} {'sell':>6} {'dur':>4} "
          f"{'max_d':>7} {'min_d':>7} {'body':>5} {'body%':>5} {'v_rat':>5} {'d_zsc':>5}")
    print(f"  {'-' * 118}")

    for _, row in matched_bars.iterrows():
        clr = 'G' if row['is_green'] else 'R'
        ts_s = row['ts_start_et'].strftime('%H:%M:%S')
        ts_e = row['ts_end_et'].strftime('%H:%M:%S')
        vr = f"{row['vol_ratio']:.1f}" if pd.notna(row['vol_ratio']) else "n/a"
        br = f"{row['body_ratio']:.2f}" if pd.notna(row['body_ratio']) else "n/a"
        max_d = f"{row['max_delta_intrabar']:+.0f}" if pd.notna(row['max_delta_intrabar']) else "n/a"
        min_d = f"{row['min_delta_intrabar']:+.0f}" if pd.notna(row['min_delta_intrabar']) else "n/a"
        dz = f"{row['delta_zscore']:.1f}" if pd.notna(row['delta_zscore']) else "n/a"
        print(f"  {int(row['bar_idx']):>5} {ts_s:>10} {ts_e:>10} "
              f"{row['bar_open']:>8.2f} {row['bar_high']:>8.2f} {row['bar_low']:>8.2f} {row['bar_close']:>8.2f} "
              f"{clr:>4} {int(row['bar_volume']):>6} {int(row['bar_delta']):>+7} "
              f"{int(row['bar_buy_volume']):>6} {int(row['bar_sell_volume']):>6} "
              f"{int(row['duration_sec']):>4} {max_d:>7} {min_d:>7} "
              f"{row['body']:.2f} {br:>5} {vr:>5} {dz:>5}")

    # Compute combined metrics for multi-bar
    last_bar = matched_bars.iloc[-1]
    first_bar = matched_bars.iloc[0]

    if n_bars > 1:
        total_vol = int(matched_bars['bar_volume'].sum())
        total_delta = int(matched_bars['bar_delta'].sum())
        total_buy = int(matched_bars['bar_buy_volume'].sum())
        total_sell = int(matched_bars['bar_sell_volume'].sum())
        net_price = float(last_bar['bar_close'] - first_bar['bar_open'])
        combined_high = float(matched_bars['bar_high'].max())
        combined_low = float(matched_bars['bar_low'].min())
        combined_range = combined_high - combined_low
        combined_body = abs(net_price)
        combined_body_ratio = combined_body / combined_range if combined_range > 0 else 0
        combined_duration = int(matched_bars['duration_sec'].sum())
        # Max/min delta across bars (peak pressure in any single bar)
        peak_max_d = float(matched_bars['max_delta_intrabar'].max())
        peak_min_d = float(matched_bars['min_delta_intrabar'].min())

        print(f"\n  COMBINED: delta={total_delta:+}, vol={total_vol}, buy={total_buy}, sell={total_sell}, "
              f"net_price={net_price:+.2f}, body_ratio={combined_body_ratio:.3f}, "
              f"dur={combined_duration}s")
        print(f"  Peak max_delta={peak_max_d:+.0f}, peak min_delta={peak_min_d:+.0f}")
    else:
        total_vol = int(last_bar['bar_volume'])
        total_delta = int(last_bar['bar_delta'])
        total_buy = int(last_bar['bar_buy_volume'])
        total_sell = int(last_bar['bar_sell_volume'])
        net_price = float(last_bar['bar_close'] - last_bar['bar_open'])
        combined_body_ratio = float(last_bar['body_ratio']) if pd.notna(last_bar['body_ratio']) else 0
        combined_duration = int(last_bar['duration_sec'])
        peak_max_d = float(last_bar['max_delta_intrabar']) if pd.notna(last_bar['max_delta_intrabar']) else 0
        peak_min_d = float(last_bar['min_delta_intrabar']) if pd.notna(last_bar['min_delta_intrabar']) else 0

    # Delta wick ratio analysis
    print(f"\n  INTRA-BAR PRESSURE ANALYSIS:")
    for _, row in matched_bars.iterrows():
        max_d = float(row['max_delta_intrabar']) if pd.notna(row['max_delta_intrabar']) else 0
        min_d = float(row['min_delta_intrabar']) if pd.notna(row['min_delta_intrabar']) else 0
        net_d = float(row['bar_delta'])

        # For bullish absorption: negative delta absorbed -> look at min_delta vs net
        # For bearish absorption: positive delta absorbed -> look at max_delta vs net
        if direction == 'bullish' and net_d != 0:
            # Sellers pushed hard (min_d very negative) but net delta recovered
            absorbed = abs(min_d) - abs(net_d)
            print(f"    idx={int(row['bar_idx'])}: net_delta={net_d:+.0f}, min_delta(peak sell)={min_d:+.0f}, "
                  f"max_delta(peak buy)={max_d:+.0f}")
            print(f"      -> Sell pressure absorbed: {abs(min_d):.0f} peak selling, "
                  f"only {abs(net_d):.0f} remained = {absorbed:.0f} absorbed")
            if abs(net_d) > 0:
                wick_ratio = abs(min_d) / abs(net_d)
                print(f"      -> Wick ratio (peak/net): {wick_ratio:.2f}x")
        elif direction == 'bearish' and net_d != 0:
            absorbed = abs(max_d) - abs(net_d)
            print(f"    idx={int(row['bar_idx'])}: net_delta={net_d:+.0f}, max_delta(peak buy)={max_d:+.0f}, "
                  f"min_delta(peak sell)={min_d:+.0f}")
            print(f"      -> Buy pressure absorbed: {abs(max_d):.0f} peak buying, "
                  f"only {abs(net_d):.0f} remained = {absorbed:.0f} absorbed")
            if abs(net_d) > 0:
                wick_ratio = abs(max_d) / abs(net_d)
                print(f"      -> Wick ratio (peak/net): {wick_ratio:.2f}x")
        else:
            print(f"    idx={int(row['bar_idx'])}: net_delta={net_d:+.0f}, max={max_d:+.0f}, min={min_d:+.0f}")

    # Context: 5-bar trend
    pt5 = float(last_bar['price_trend_5']) if pd.notna(last_bar['price_trend_5']) else 0
    tc5 = float(last_bar['trend_consistency_5']) if pd.notna(last_bar['trend_consistency_5']) else 0
    print(f"\n  CONTEXT: 5-bar price trend: {pt5:+.2f}, trend consistency: {tc5:.2f}")

    # Entry and outcome
    entry_price = float(last_bar['bar_close'])
    entry_idx = int(last_bar['bar_idx'])

    # Compute MFE/MAE over 30 bars
    mfe_30, mae_30 = compute_mfe_mae_30(df, entry_idx, direction, entry_price)

    # Trail sim with Config F
    result = simulate_trail(df, entry_idx, direction, entry_price)

    print(f"\n  OUTCOME (Config F: SL=8, trail gap=8, timeout=100 bars):")
    print(f"    Entry: {entry_price:.2f} at idx={entry_idx}")
    print(f"    Result: {result['outcome']} | PnL: {result['pnl']:+.2f} | "
          f"MFE: {result['mfe']:.2f} | MAE: {result['mae']:.2f} | Bars held: {result['bars_held']}")
    print(f"    MFE/MAE (30-bar): MFE={mfe_30:.2f}, MAE={mae_30:.2f}")

    if result['price_5'] is not None:
        p5_pnl = (result['price_5'] - entry_price) if direction == 'bullish' else (entry_price - result['price_5'])
        print(f"    Price at +5 bars: {result['price_5']:.2f} ({p5_pnl:+.2f} pts)")
    if result['price_10'] is not None:
        p10_pnl = (result['price_10'] - entry_price) if direction == 'bullish' else (entry_price - result['price_10'])
        print(f"    Price at +10 bars: {result['price_10']:.2f} ({p10_pnl:+.2f} pts)")
    if result['price_20'] is not None:
        p20_pnl = (result['price_20'] - entry_price) if direction == 'bullish' else (entry_price - result['price_20'])
        print(f"    Price at +20 bars: {result['price_20']:.2f} ({p20_pnl:+.2f} pts)")

    user_processed.append({
        'sig_id': sig_id,
        'time_et': time_et,
        'direction': direction,
        'bar_indices': bar_indices,
        'entry_idx': entry_idx,
        'entry_price': entry_price,
        'delta': total_delta,
        'volume': total_vol,
        'buy_vol': total_buy,
        'sell_vol': total_sell,
        'body_ratio': combined_body_ratio,
        'is_green': bool(last_bar['is_green']),
        'duration_sec': combined_duration,
        'max_delta_intrabar': peak_max_d,
        'min_delta_intrabar': peak_min_d,
        'vol_ratio': float(last_bar['vol_ratio']) if pd.notna(last_bar['vol_ratio']) else None,
        'delta_zscore': float(last_bar['delta_zscore']) if pd.notna(last_bar['delta_zscore']) else None,
        'price_trend_5': pt5,
        'trend_consistency_5': tc5,
        'mfe_30': mfe_30,
        'mae_30': mae_30,
        'outcome': result['outcome'],
        'pnl': result['pnl'],
        'mfe': result['mfe'],
        'mae': result['mae'],
        'bars_held': result['bars_held'],
        'note': sig['note'],
        'is_multi': n_bars > 1,
    })

# ─────────────────────────────────────────────────────────────────────
# STEP 3: Summary table of all user signals
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("[3] USER SIGNALS SUMMARY TABLE")
print("=" * 120)

print(f"\n{'#':>3} {'Time':>6} {'Dir':>7} {'Bars':>5} {'Entry':>8} {'Delta':>7} {'Vol':>6} "
      f"{'MaxD':>7} {'MinD':>7} {'Body%':>5} {'VRat':>5} {'DZsc':>5} "
      f"{'MFE30':>6} {'MAE30':>6} {'Out':>7} {'PnL':>6} {'MFE':>6} {'MAE':>6}")
print("-" * 130)

total_pnl = 0
wins = 0
losses = 0
for s in user_processed:
    vr = f"{s['vol_ratio']:.1f}" if s['vol_ratio'] is not None else "n/a"
    dz = f"{s['delta_zscore']:.1f}" if s['delta_zscore'] is not None else "n/a"
    print(f"{s['sig_id']:>3} {s['time_et']:>6} {s['direction']:>7} "
          f"{'M' if s['is_multi'] else 'S':>5} "
          f"{s['entry_price']:>8.2f} {s['delta']:>+7} {s['volume']:>6} "
          f"{s['max_delta_intrabar']:>+7.0f} {s['min_delta_intrabar']:>+7.0f} "
          f"{s['body_ratio']:>5.2f} {vr:>5} {dz:>5} "
          f"{s['mfe_30']:>6.1f} {s['mae_30']:>6.1f} "
          f"{s['outcome']:>7} {s['pnl']:>+6.1f} {s['mfe']:>6.1f} {s['mae']:>6.1f}")
    total_pnl += s['pnl']
    if s['pnl'] > 0:
        wins += 1
    elif s['pnl'] < 0:
        losses += 1

print(f"\nTotal: {len(user_processed)} signals, {wins}W/{losses}L, net PnL: {total_pnl:+.1f} pts "
      f"(WR: {wins/len(user_processed)*100:.0f}%)")


# ─────────────────────────────────────────────────────────────────────
# STEP 4: Machine detector on same day
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("[4] MACHINE DETECTOR (MEDIUM CRITERIA) — MARCH 19, 2026")
print("=" * 120)

machine_signals = scan_machine_signals(df)
print(f"\nMachine found {len(machine_signals)} signals")

print(f"\n{'#':>3} {'Time':>10} {'Dir':>7} {'Multi':>5} {'Entry':>8} {'Delta':>7} {'Vol':>6} "
      f"{'Body%':>5} {'VRat':>5} {'MaxD':>7} {'MinD':>7} {'Out':>7} {'PnL':>6}")
print("-" * 110)

m_total_pnl = 0
m_wins = 0
m_losses = 0
for i, ms in enumerate(machine_signals):
    result = simulate_trail(df, ms['bar_idx'], ms['direction'], ms['entry_price'])
    ms['outcome'] = result['outcome']
    ms['pnl'] = result['pnl']
    ms['mfe'] = result['mfe']
    ms['mae'] = result['mae']
    ms['bars_held'] = result['bars_held']

    vr = f"{ms['vol_ratio']:.1f}" if ms['vol_ratio'] is not None else "n/a"
    max_d = f"{ms['max_delta_intrabar']:+.0f}" if ms['max_delta_intrabar'] is not None else "n/a"
    min_d = f"{ms['min_delta_intrabar']:+.0f}" if ms['min_delta_intrabar'] is not None else "n/a"
    print(f"{i+1:>3} {ms['et_end']:>10} {ms['direction']:>7} "
          f"{'Y' if ms['is_multi'] else 'N':>5} "
          f"{ms['entry_price']:>8.2f} {ms['delta']:>+7} {ms['volume']:>6} "
          f"{ms['body_ratio']:>5.3f} {vr:>5} {max_d:>7} {min_d:>7} "
          f"{ms['outcome']:>7} {ms['pnl']:>+6.1f}")
    m_total_pnl += ms['pnl']
    if ms['pnl'] > 0:
        m_wins += 1
    elif ms['pnl'] < 0:
        m_losses += 1

print(f"\nTotal: {len(machine_signals)} signals, {m_wins}W/{m_losses}L, net PnL: {m_total_pnl:+.1f} pts "
      f"(WR: {m_wins/len(machine_signals)*100:.0f}%)" if machine_signals else "\nNo machine signals found!")


# ─────────────────────────────────────────────────────────────────────
# STEP 5: COMPARISON — Matched, User-only, Machine-only
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("[5] COMPARISON: HUMAN vs MACHINE")
print("=" * 120)

# Match signals by proximity: user bar_idx within 2 of machine bar_idx, same direction
MATCH_WINDOW = 3  # bars

matched = []
user_only = []
machine_matched_indices = set()

for us in user_processed:
    found_match = False
    for mi, ms in enumerate(machine_signals):
        if mi in machine_matched_indices:
            continue
        # Check if any of the user's bar indices are close to the machine's
        for ub_idx in us['bar_indices']:
            if abs(ub_idx - ms['bar_idx']) <= MATCH_WINDOW and us['direction'] == ms['direction']:
                matched.append((us, ms))
                machine_matched_indices.add(mi)
                found_match = True
                break
        if found_match:
            break
    if not found_match:
        user_only.append(us)

machine_only = [ms for mi, ms in enumerate(machine_signals) if mi not in machine_matched_indices]

# ── MATCHED ──
print(f"\n{'─' * 60}")
print(f"BOTH AGREE ({len(matched)} signals) — HIGH CONFIDENCE")
print(f"{'─' * 60}")
if matched:
    for us, ms in matched:
        print(f"\n  Signal #{us['sig_id']} ({us['time_et']} ET, {us['direction'].upper()}) "
              f"= Machine bar_idx={ms['bar_idx']} ({ms['et_end']})")
        print(f"    User: delta={us['delta']:+}, vol={us['volume']}, PnL={us['pnl']:+.1f}")
        print(f"    Machine: delta={ms['delta']:+}, vol={ms['volume']}, PnL={ms['pnl']:+.1f}")
else:
    print("  (none)")

# ── USER ONLY ──
print(f"\n{'─' * 60}")
print(f"USER CAUGHT, MACHINE MISSED ({len(user_only)} signals)")
print(f"{'─' * 60}")
if user_only:
    for us in user_only:
        print(f"\n  Signal #{us['sig_id']} ({us['time_et']} ET, {us['direction'].upper()}) "
              f"| PnL: {us['pnl']:+.1f} | MFE30: {us['mfe_30']:.1f}")
        print(f"    delta={us['delta']:+}, vol={us['volume']}, body%={us['body_ratio']:.3f}")
        print(f"    max_delta={us['max_delta_intrabar']:+.0f}, min_delta={us['min_delta_intrabar']:+.0f}")
        print(f"    Note: {us['note']}")

        # Diagnose why machine missed
        print(f"\n    THRESHOLD DIAGNOSIS:")
        checks = diagnose_miss(df, us['bar_indices'], us['direction'])
        for check_name, required, actual, status in checks:
            marker = "OK" if status == "PASS" else ("XX" if status == "FAIL" else "??")
            print(f"      [{marker}] {check_name}: required {required}, got {actual}")

        # Find the blocking reason(s)
        blockers = [c for c in checks if c[3] == "FAIL"]
        if blockers:
            print(f"    >> BLOCKED BY: {', '.join(c[0] for c in blockers)}")
        else:
            print(f"    >> No threshold failures — likely cooldown or direction mismatch")
else:
    print("  (none)")

# ── MACHINE ONLY ──
print(f"\n{'─' * 60}")
print(f"MACHINE CAUGHT, USER SKIPPED ({len(machine_only)} signals)")
print(f"{'─' * 60}")
if machine_only:
    for ms in machine_only:
        print(f"\n  Machine bar_idx={ms['bar_idx']} ({ms['et_end']} ET, {ms['direction'].upper()}) "
              f"| PnL: {ms['pnl']:+.1f}")

        # Get full bar details
        bar_row = df[df['bar_idx'] == ms['bar_idx']]
        if len(bar_row) > 0:
            br = bar_row.iloc[0]
            clr = 'GREEN' if br['is_green'] else 'RED'
            max_d = float(br['max_delta_intrabar']) if pd.notna(br['max_delta_intrabar']) else 0
            min_d = float(br['min_delta_intrabar']) if pd.notna(br['min_delta_intrabar']) else 0
            print(f"    OHLC: {br['bar_open']:.2f}/{br['bar_high']:.2f}/{br['bar_low']:.2f}/{br['bar_close']:.2f} ({clr})")
            print(f"    delta={int(br['bar_delta']):+}, vol={int(br['bar_volume'])}, "
                  f"body%={br['body_ratio']:.3f}")
            print(f"    max_delta={max_d:+.0f}, min_delta={min_d:+.0f}")
            vr = f"{br['vol_ratio']:.1f}" if pd.notna(br['vol_ratio']) else "n/a"
            print(f"    vol_ratio={vr}, dur={int(br['duration_sec'])}s")
            print(f"    Outcome: {ms['outcome']} | PnL: {ms['pnl']:+.1f} | MFE: {ms['mfe']:.1f} | MAE: {ms['mae']:.1f}")

        if ms['is_multi']:
            print(f"    (detected as 2-bar combo)")
else:
    print("  (none)")


# ─────────────────────────────────────────────────────────────────────
# STEP 6: Summary Statistics
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("[6] SUMMARY STATISTICS")
print("=" * 120)

user_pnls = [s['pnl'] for s in user_processed]
user_wins = sum(1 for p in user_pnls if p > 0)
user_losses_n = sum(1 for p in user_pnls if p < 0)

machine_pnls = [s['pnl'] for s in machine_signals]
m_wins_n = sum(1 for p in machine_pnls if p > 0) if machine_pnls else 0
m_losses_n = sum(1 for p in machine_pnls if p < 0) if machine_pnls else 0

matched_user_pnls = [us['pnl'] for us, _ in matched]
matched_wins = sum(1 for p in matched_user_pnls if p > 0)

user_only_pnls = [s['pnl'] for s in user_only]
user_only_wins = sum(1 for p in user_only_pnls if p > 0)

machine_only_pnls = [s['pnl'] for s in machine_only]
machine_only_wins = sum(1 for p in machine_only_pnls if p > 0) if machine_only_pnls else 0

print(f"\n  {'Category':>25} {'Count':>6} {'Wins':>5} {'WR':>6} {'PnL':>8} {'AvgPnL':>8}")
print(f"  {'-' * 60}")
print(f"  {'User signals':>25} {len(user_processed):>6} {user_wins:>5} "
      f"{user_wins/len(user_processed)*100:>5.0f}% {sum(user_pnls):>+8.1f} "
      f"{sum(user_pnls)/len(user_processed):>+8.2f}")

if machine_signals:
    print(f"  {'Machine signals':>25} {len(machine_signals):>6} {m_wins_n:>5} "
          f"{m_wins_n/len(machine_signals)*100:>5.0f}% {sum(machine_pnls):>+8.1f} "
          f"{sum(machine_pnls)/len(machine_signals):>+8.2f}")

if matched:
    print(f"  {'BOTH agree':>25} {len(matched):>6} {matched_wins:>5} "
          f"{matched_wins/len(matched)*100 if matched else 0:>5.0f}% {sum(matched_user_pnls):>+8.1f} "
          f"{sum(matched_user_pnls)/len(matched) if matched else 0:>+8.2f}")

if user_only:
    print(f"  {'User-only':>25} {len(user_only):>6} {user_only_wins:>5} "
          f"{user_only_wins/len(user_only)*100 if user_only else 0:>5.0f}% {sum(user_only_pnls):>+8.1f} "
          f"{sum(user_only_pnls)/len(user_only) if user_only else 0:>+8.2f}")

if machine_only:
    print(f"  {'Machine-only':>25} {len(machine_only):>6} {machine_only_wins:>5} "
          f"{machine_only_wins/len(machine_only)*100 if machine_only else 0:>5.0f}% {sum(machine_only_pnls):>+8.1f} "
          f"{sum(machine_only_pnls)/len(machine_only) if machine_only else 0:>+8.2f}")

# Key observations
print(f"\n  KEY OBSERVATIONS:")
user_edge = sum(user_only_pnls) if user_only_pnls else 0
machine_edge = sum(machine_only_pnls) if machine_only_pnls else 0
print(f"    User's unique edge: {user_edge:+.1f} pts from {len(user_only)} signals machine missed")
print(f"    Machine's unique edge: {machine_edge:+.1f} pts from {len(machine_only)} signals user skipped")

# ─────────────────────────────────────────────────────────────────────
# STEP 7: Export CSV
# ─────────────────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("[7] EXPORTING CSV")
print("=" * 120)

csv_rows = []
for s in user_processed:
    # Check if matched
    match_status = 'USER_ONLY'
    for us, ms in matched:
        if us['sig_id'] == s['sig_id']:
            match_status = 'BOTH'
            break

    csv_rows.append({
        'source': 'USER',
        'match_status': match_status,
        'sig_id': s['sig_id'],
        'time_et': s['time_et'],
        'direction': s['direction'],
        'bar_indices': str(s['bar_indices']),
        'entry_idx': s['entry_idx'],
        'entry_price': s['entry_price'],
        'delta': s['delta'],
        'volume': s['volume'],
        'buy_vol': s['buy_vol'],
        'sell_vol': s['sell_vol'],
        'body_ratio': round(s['body_ratio'], 4),
        'is_green': s['is_green'],
        'duration_sec': s['duration_sec'],
        'max_delta_intrabar': s['max_delta_intrabar'],
        'min_delta_intrabar': s['min_delta_intrabar'],
        'vol_ratio': round(s['vol_ratio'], 2) if s['vol_ratio'] else None,
        'delta_zscore': round(s['delta_zscore'], 2) if s['delta_zscore'] else None,
        'price_trend_5': round(s['price_trend_5'], 2),
        'trend_consistency_5': round(s['trend_consistency_5'], 3),
        'mfe_30': s['mfe_30'],
        'mae_30': s['mae_30'],
        'outcome': s['outcome'],
        'pnl': s['pnl'],
        'mfe': s['mfe'],
        'mae': s['mae'],
        'bars_held': s['bars_held'],
        'is_multi': s['is_multi'],
        'note': s['note'],
    })

for i, ms in enumerate(machine_signals):
    if i in machine_matched_indices:
        continue  # already in CSV as BOTH

    bar_row = df[df['bar_idx'] == ms['bar_idx']]
    br = bar_row.iloc[0] if len(bar_row) > 0 else None

    csv_rows.append({
        'source': 'MACHINE',
        'match_status': 'MACHINE_ONLY',
        'sig_id': f'M{i+1}',
        'time_et': ms['et_end'][:5],
        'direction': ms['direction'],
        'bar_indices': str([ms['bar_idx']]),
        'entry_idx': ms['bar_idx'],
        'entry_price': ms['entry_price'],
        'delta': ms['delta'],
        'volume': ms['volume'],
        'buy_vol': int(br['bar_buy_volume']) if br is not None else None,
        'sell_vol': int(br['bar_sell_volume']) if br is not None else None,
        'body_ratio': round(ms['body_ratio'], 4),
        'is_green': bool(br['is_green']) if br is not None else None,
        'duration_sec': int(br['duration_sec']) if br is not None else None,
        'max_delta_intrabar': ms['max_delta_intrabar'],
        'min_delta_intrabar': ms['min_delta_intrabar'],
        'vol_ratio': round(ms['vol_ratio'], 2) if ms['vol_ratio'] else None,
        'delta_zscore': round(float(br['delta_zscore']), 2) if br is not None and pd.notna(br['delta_zscore']) else None,
        'price_trend_5': round(float(br['price_trend_5']), 2) if br is not None and pd.notna(br['price_trend_5']) else None,
        'trend_consistency_5': round(float(br['trend_consistency_5']), 3) if br is not None and pd.notna(br['trend_consistency_5']) else None,
        'mfe_30': None,
        'mae_30': None,
        'outcome': ms.get('outcome', ''),
        'pnl': ms.get('pnl', 0),
        'mfe': ms.get('mfe', 0),
        'mae': ms.get('mae', 0),
        'bars_held': ms.get('bars_held', 0),
        'is_multi': ms['is_multi'],
        'note': '',
    })

csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'exports', 'human_vs_machine_mar19.csv')
os.makedirs(os.path.dirname(csv_path), exist_ok=True)

with open(csv_path, 'w', newline='', encoding='utf-8') as f:
    if csv_rows:
        writer = csv.DictWriter(f, fieldnames=csv_rows[0].keys())
        writer.writeheader()
        writer.writerows(csv_rows)

print(f"  Saved {len(csv_rows)} rows to {csv_path}")

print("\n" + "=" * 120)
print("DONE")
print("=" * 120)
