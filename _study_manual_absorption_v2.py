#!/usr/bin/env python3
"""
ES Absorption Study v2 — Two Critical Fixes:
  1. Signal #9 direction fixed (positive delta + flat/red = BEARISH, not bullish)
  2. Trailing stop instead of fixed TP — 6 trail configs tested

Runs on: 14 manual signals + full March scan (medium criteria)
Also tests Volland filters on the best trail config.

Usage: railway run -- python -u _study_manual_absorption_v2.py
"""

import os, sys, json, math, re
from datetime import datetime, time as dtime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

sys.stdout.reconfigure(line_buffering=True, encoding='utf-8', errors='replace')

NY = ZoneInfo("America/New_York")
engine = create_engine(os.environ['DATABASE_URL'])

# ──────────────────────────────────────────────────────────────────────
# Trail config definitions
# ──────────────────────────────────────────────────────────────────────

TRAIL_CONFIGS = {
    'A': {
        'name': 'A: Tight trail',
        'sl': 8, 'be_trigger': 8, 'activation': 10, 'gap': 5,
        'has_be': True, 'has_activation': True,
    },
    'B': {
        'name': 'B: Medium trail',
        'sl': 8, 'be_trigger': 8, 'activation': 12, 'gap': 8,
        'has_be': True, 'has_activation': True,
    },
    'C': {
        'name': 'C: Wide trail',
        'sl': 8, 'be_trigger': 10, 'activation': 15, 'gap': 10,
        'has_be': True, 'has_activation': True,
    },
    'D': {
        'name': 'D: Continuous (DD style)',
        'sl': 8, 'be_trigger': None, 'activation': 10, 'gap': 5,
        'has_be': False, 'has_activation': True,
    },
    'E': {
        'name': 'E: Split target (SC style)',
        'sl': 8, 'be_trigger': None, 'activation': 10, 'gap': 5,
        'has_be': False, 'has_activation': True,
        'split': True, 't1_pct': 0.5, 't1_pts': 10,
    },
    'F': {
        'name': 'F: No TP, immediate trail gap=8',
        'sl': 8, 'be_trigger': None, 'activation': 0, 'gap': 8,
        'has_be': False, 'has_activation': False,
    },
}

# ──────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────

def load_bars(trade_date: str) -> pd.DataFrame:
    """Load all 5-pt rithmic range bars for a date."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                   cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
                   ts_start AT TIME ZONE 'America/New_York' as et_start,
                   ts_end AT TIME ZONE 'America/New_York' as et_end
            FROM es_range_bars
            WHERE trade_date = :td AND source = 'rithmic'
              AND range_pts = 5.0 AND status = 'closed'
            ORDER BY bar_idx
        """), {"td": trade_date}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])
    if len(df) > 0:
        _add_derived_cols(df)
    return df


def load_all_march_bars() -> pd.DataFrame:
    """Load all 5-pt rithmic range bars for March 2026."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                   cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
                   ts_start AT TIME ZONE 'America/New_York' as et_start,
                   ts_end AT TIME ZONE 'America/New_York' as et_end
            FROM es_range_bars
            WHERE trade_date >= '2026-03-01' AND trade_date <= '2026-03-31'
              AND source = 'rithmic' AND range_pts = 5.0 AND status = 'closed'
            ORDER BY trade_date, bar_idx
        """)).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])
    if len(df) > 0:
        _add_derived_cols(df, per_day=True)
    return df


def _add_derived_cols(df, per_day=False):
    df['is_green'] = df['bar_close'] > df['bar_open']
    df['body'] = (df['bar_close'] - df['bar_open']).abs()
    df['bar_range'] = df['bar_high'] - df['bar_low']
    df['body_ratio'] = df['body'] / df['bar_range'].replace(0, np.nan)
    df['duration_s'] = (df['et_end'] - df['et_start']).dt.total_seconds()
    df['vol_per_sec'] = df['bar_volume'] / df['duration_s'].replace(0, np.nan)
    df['delta_per_sec'] = df['bar_delta'].abs() / df['duration_s'].replace(0, np.nan)
    df['intra_max_delta'] = df['cvd_high'] - df['cvd_open']
    df['intra_min_delta'] = df['cvd_low'] - df['cvd_open']
    df['abs_ratio'] = df['bar_delta'].abs() / df['bar_volume'].replace(0, np.nan)

    if per_day:
        for td in df['trade_date'].unique():
            mask = df['trade_date'] == td
            df.loc[mask, 'vol_avg_20'] = df.loc[mask, 'bar_volume'].rolling(20, min_periods=1).mean().shift(1).values
            df.loc[mask, 'delta_avg_20'] = df.loc[mask, 'bar_delta'].abs().rolling(20, min_periods=1).mean().shift(1).values
            df.loc[mask, 'vps_avg_20'] = df.loc[mask, 'vol_per_sec'].rolling(20, min_periods=1).mean().shift(1).values
    else:
        df['vol_avg_20'] = df['bar_volume'].rolling(20, min_periods=1).mean().shift(1)
        df['delta_avg_20'] = df['bar_delta'].abs().rolling(20, min_periods=1).mean().shift(1)
        df['vps_avg_20'] = df['vol_per_sec'].rolling(20, min_periods=1).mean().shift(1)

    df['vol_ratio'] = df['bar_volume'] / df['vol_avg_20'].replace(0, np.nan)
    df['delta_ratio'] = df['bar_delta'].abs() / df['delta_avg_20'].replace(0, np.nan)
    df['vps_ratio'] = df['vol_per_sec'] / df['vps_avg_20'].replace(0, np.nan)


# ──────────────────────────────────────────────────────────────────────
# Trail simulation engine
# ──────────────────────────────────────────────────────────────────────

def simulate_trail(df: pd.DataFrame, signal_idx: int, direction: str,
                   entry_price: float, config: dict,
                   max_bars: int = 100) -> dict:
    """
    Simulate a trail-based exit bar by bar.

    Returns dict with outcome, pnl, max_profit, bars_held, exit_price.
    For split-target (config E), returns average of T1 and T2.
    """
    mask = df['bar_idx'] > signal_idx
    future = df.loc[mask].head(max_bars)
    if len(future) == 0:
        return {'outcome': 'NO_DATA', 'pnl': 0, 'max_profit': 0, 'bars_held': 0,
                'exit_price': entry_price, 'mfe': 0, 'mae': 0}

    sl = config['sl']
    is_split = config.get('split', False)

    if is_split:
        # Config E: Split target — simulate T1 and T2 independently
        t1_result = _sim_t1(df, signal_idx, direction, entry_price, config, max_bars)
        t2_result = _sim_trail_leg(df, signal_idx, direction, entry_price, config, max_bars)
        t1_pct = config['t1_pct']
        t2_pct = 1 - t1_pct
        avg_pnl = t1_result['pnl'] * t1_pct + t2_result['pnl'] * t2_pct
        return {
            'outcome': f"T1:{t1_result['outcome']}/T2:{t2_result['outcome']}",
            'pnl': round(avg_pnl, 2),
            'max_profit': max(t1_result['max_profit'], t2_result['max_profit']),
            'bars_held': max(t1_result['bars_held'], t2_result['bars_held']),
            'exit_price': t2_result['exit_price'],
            'mfe': t2_result['mfe'],
            'mae': max(t1_result['mae'], t2_result['mae']),
            't1_pnl': t1_result['pnl'],
            't2_pnl': t2_result['pnl'],
        }
    else:
        return _sim_trail_leg(df, signal_idx, direction, entry_price, config, max_bars)


def _sim_t1(df, signal_idx, direction, entry_price, config, max_bars):
    """Simulate T1: fixed take-profit at t1_pts, with same SL."""
    mask = df['bar_idx'] > signal_idx
    future = df.loc[mask].head(max_bars)
    sl = config['sl']
    tp = config['t1_pts']
    max_profit = 0
    mae_val = 0

    for i, (_, bar) in enumerate(future.iterrows()):
        if direction == 'bullish':
            profit_at_high = bar['bar_high'] - entry_price
            profit_at_low = bar['bar_low'] - entry_price
            loss_check = bar['bar_low'] - entry_price
            win_check = bar['bar_high'] - entry_price
        else:
            profit_at_high = entry_price - bar['bar_low']
            profit_at_low = entry_price - bar['bar_high']
            loss_check = -(bar['bar_high'] - entry_price)
            win_check = entry_price - bar['bar_low']

        max_profit = max(max_profit, profit_at_high)
        mae_val = max(mae_val, -profit_at_low)

        # Check SL
        if loss_check <= -sl:
            return {'outcome': 'LOSS', 'pnl': -sl, 'max_profit': max_profit,
                    'bars_held': i + 1, 'exit_price': entry_price + (-sl if direction == 'bullish' else sl),
                    'mfe': max_profit, 'mae': mae_val}
        # Check TP
        if win_check >= tp:
            return {'outcome': 'WIN', 'pnl': tp, 'max_profit': max_profit,
                    'bars_held': i + 1, 'exit_price': entry_price + (tp if direction == 'bullish' else -tp),
                    'mfe': max_profit, 'mae': mae_val}

    # Expired
    last_close = future.iloc[-1]['bar_close']
    pnl = (last_close - entry_price) if direction == 'bullish' else (entry_price - last_close)
    return {'outcome': 'EXPIRED', 'pnl': round(pnl, 2), 'max_profit': max_profit,
            'bars_held': len(future), 'exit_price': last_close,
            'mfe': max_profit, 'mae': mae_val}


def _sim_trail_leg(df, signal_idx, direction, entry_price, config, max_bars):
    """Simulate trail-only leg (no fixed TP)."""
    mask = df['bar_idx'] > signal_idx
    future = df.loc[mask].head(max_bars)
    if len(future) == 0:
        return {'outcome': 'NO_DATA', 'pnl': 0, 'max_profit': 0, 'bars_held': 0,
                'exit_price': entry_price, 'mfe': 0, 'mae': 0}

    sl = config['sl']
    be_trigger = config.get('be_trigger')
    has_be = config.get('has_be', False)
    activation = config.get('activation', 0)
    has_activation = config.get('has_activation', True)
    gap = config['gap']

    stop_distance = sl  # current stop distance from entry
    be_hit = False
    trail_active = False
    max_profit = 0
    mae_val = 0

    for i, (_, bar) in enumerate(future.iterrows()):
        if direction == 'bullish':
            bar_profit_high = bar['bar_high'] - entry_price
            bar_profit_low = bar['bar_low'] - entry_price
        else:
            bar_profit_high = entry_price - bar['bar_low']
            bar_profit_low = entry_price - bar['bar_high']

        # Update max profit
        max_profit = max(max_profit, bar_profit_high)
        mae_val = max(mae_val, -bar_profit_low)

        # Compute current stop level (in profit space, negative = below entry)
        if has_activation and not trail_active:
            # Before activation
            if has_be and not be_hit:
                stop_level = -sl  # initial stop
                if max_profit >= be_trigger:
                    be_hit = True
                    stop_level = 0  # breakeven
            elif has_be and be_hit:
                stop_level = 0  # at breakeven, waiting for activation
            else:
                stop_level = -sl  # no BE step, just initial stop

            # Check activation
            if max_profit >= activation:
                trail_active = True
                stop_level = max_profit - gap
        elif trail_active or not has_activation:
            # Trail is active (or config F: always trailing)
            if not has_activation:
                # Config F: immediate trail — stop starts at -sl, moves up as profit grows
                stop_level = max(max_profit - gap, -sl)
            else:
                stop_level = max_profit - gap
        else:
            stop_level = -sl

        # Check if stop hit this bar
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
            }

    # Expired at bar 100
    last_close = future.iloc[-1]['bar_close']
    pnl = (last_close - entry_price) if direction == 'bullish' else (entry_price - last_close)
    outcome = 'WIN' if pnl > 0 else ('BE' if pnl == 0 else 'LOSS')
    return {
        'outcome': outcome, 'pnl': round(pnl, 2),
        'max_profit': round(max_profit, 2), 'bars_held': len(future),
        'exit_price': round(last_close, 2),
        'mfe': round(max_profit, 2), 'mae': round(mae_val, 2),
    }


def compute_stats(results: list) -> dict:
    """Compute aggregate stats from a list of trail simulation results."""
    if not results:
        return {'count': 0, 'wins': 0, 'losses': 0, 'wr': 0, 'pnl': 0,
                'max_dd': 0, 'pf': 0, 'avg_pnl': 0, 'avg_bars': 0}

    wins = sum(1 for r in results if r['pnl'] > 0)
    losses = sum(1 for r in results if r['pnl'] < 0)
    bes = sum(1 for r in results if r['pnl'] == 0)
    total_pnl = sum(r['pnl'] for r in results)
    gross_wins = sum(r['pnl'] for r in results if r['pnl'] > 0)
    gross_losses = abs(sum(r['pnl'] for r in results if r['pnl'] < 0))
    avg_bars = sum(r['bars_held'] for r in results) / len(results)

    # MaxDD
    running = 0
    peak = 0
    max_dd = 0
    for r in results:
        running += r['pnl']
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    pf = gross_wins / gross_losses if gross_losses > 0 else float('inf')
    wr = wins / len(results) * 100

    return {
        'count': len(results), 'wins': wins, 'losses': losses, 'bes': bes,
        'wr': round(wr, 1), 'pnl': round(total_pnl, 1),
        'max_dd': round(max_dd, 1), 'pf': round(pf, 2),
        'avg_pnl': round(total_pnl / len(results), 2),
        'avg_bars': round(avg_bars, 1),
        'gross_wins': round(gross_wins, 1),
        'gross_losses': round(gross_losses, 1),
    }


# ──────────────────────────────────────────────────────────────────────
# Signal scanning (MEDIUM criteria, direction logic FIXED)
# ──────────────────────────────────────────────────────────────────────

MEDIUM_CRITERIA = {
    'min_abs_delta_single': 300,
    'min_abs_delta_multi': 600,
    'max_body_ratio': 0.75,
    'min_volume': 3000,
    'two_bar_window': True,
    'cooldown_bars': 10,
    'market_hours_only': True,
}


def scan_signals(df: pd.DataFrame, criteria: dict = None) -> list:
    """
    Scan all bars using given criteria. Returns list of signal dicts.

    DIRECTION LOGIC (FIXED in v2):
    - Positive delta + price flat/red/small body = BEARISH (buyers absorbed, couldn't push price up)
    - Negative delta + price flat/green/small body = BULLISH (sellers absorbed, couldn't push price down)
    """
    if criteria is None:
        criteria = MEDIUM_CRITERIA
    signals = []
    dates = sorted(df['trade_date'].unique())

    for td in dates:
        day_df = df[df['trade_date'] == td].reset_index(drop=True)
        if len(day_df) < 25:
            continue

        last_bull_idx = -100
        last_bear_idx = -100
        cd = criteria['cooldown_bars']

        for i in range(20, len(day_df)):
            bar = day_df.iloc[i]
            idx = int(bar['bar_idx'])

            # Market hours
            if criteria.get('market_hours_only', True):
                et = bar['et_end']
                if hasattr(et, 'time'):
                    if et.time() < dtime(9, 35) or et.time() > dtime(15, 55):
                        continue

            # ---- Single bar check ----
            single_hit = False
            direction = None

            if (abs(bar['bar_delta']) >= criteria['min_abs_delta_single'] and
                bar['bar_volume'] >= criteria['min_volume'] and
                pd.notna(bar['body_ratio']) and bar['body_ratio'] <= criteria['max_body_ratio']):

                # FIXED DIRECTION LOGIC:
                # Positive delta (net buying) + price flat/red = buyers ABSORBED = BEARISH
                # Negative delta (net selling) + price flat/green = sellers ABSORBED = BULLISH
                if bar['bar_delta'] > 0 and (not bar['is_green'] or bar['body_ratio'] <= 0.40):
                    direction = 'bearish'
                    single_hit = True
                elif bar['bar_delta'] < 0 and (bar['is_green'] or bar['body_ratio'] <= 0.40):
                    direction = 'bullish'
                    single_hit = True

            # ---- Two-bar check (combine current + previous) ----
            two_bar_hit = False
            if not single_hit and criteria.get('two_bar_window', True) and i >= 1:
                prev = day_df.iloc[i - 1]
                combined_delta = int(bar['bar_delta'] + prev['bar_delta'])
                combined_vol = int(bar['bar_volume'] + prev['bar_volume'])
                net_price = bar['bar_close'] - prev['bar_open']
                combined_range = max(bar['bar_high'], prev['bar_high']) - min(bar['bar_low'], prev['bar_low'])
                combined_body_ratio = abs(net_price) / combined_range if combined_range > 0 else 1

                if (abs(combined_delta) >= criteria['min_abs_delta_multi'] and
                    combined_vol >= criteria['min_volume'] and
                    combined_body_ratio <= criteria['max_body_ratio']):

                    # Same direction logic for 2-bar:
                    if combined_delta > 0 and (net_price <= 0 or combined_body_ratio <= 0.40):
                        direction = 'bearish'
                        two_bar_hit = True
                    elif combined_delta < 0 and (net_price >= 0 or combined_body_ratio <= 0.40):
                        direction = 'bullish'
                        two_bar_hit = True

            if not single_hit and not two_bar_hit:
                continue

            # Cooldown check
            if direction == 'bullish' and idx - last_bull_idx < cd:
                continue
            if direction == 'bearish' and idx - last_bear_idx < cd:
                continue

            entry_price = bar['bar_close']

            sig = {
                'date': td,
                'bar_idx': idx,
                'direction': direction,
                'is_multi': two_bar_hit,
                'entry_price': float(entry_price),
                'et_end': bar['et_end'].strftime('%H:%M:%S') if hasattr(bar['et_end'], 'strftime') else str(bar['et_end']),
                'delta': int(bar['bar_delta']) if single_hit else combined_delta,
                'volume': int(bar['bar_volume']) if single_hit else combined_vol,
                'body_ratio': float(bar['body_ratio']) if single_hit else combined_body_ratio,
                'vol_ratio': float(bar['vol_ratio']) if pd.notna(bar['vol_ratio']) else None,
            }

            signals.append(sig)

            if direction == 'bullish':
                last_bull_idx = idx
            else:
                last_bear_idx = idx

    return signals


# ──────────────────────────────────────────────────────────────────────
# Volland data loader
# ──────────────────────────────────────────────────────────────────────

def load_volland_cache():
    """Load all March Volland snapshots into a cache dict keyed by trade_date."""
    cache = {}
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ts AT TIME ZONE 'America/New_York' as ts_et,
                   (ts AT TIME ZONE 'America/New_York')::date as trade_date,
                   payload->'statistics'->>'paradigm' as paradigm,
                   payload->'statistics'->>'lines_in_sand' as lis,
                   payload->'statistics'->>'delta_decay_hedging' as dd_hedging,
                   payload->'statistics'->>'aggregatedCharm' as agg_charm,
                   payload->'statistics'->'spot_vol_beta' as svb
            FROM volland_snapshots
            WHERE ts >= '2026-03-01' AND ts < '2026-04-01'
              AND payload->'statistics' IS NOT NULL
              AND payload->'statistics'->>'paradigm' IS NOT NULL
            ORDER BY ts
        """)).mappings().all()
    for r in rows:
        d = dict(r)
        td = d['trade_date']
        if td not in cache:
            cache[td] = []
        cache[td].append(d)
    return cache


def get_volland_at_time(cache, trade_date, et_time_str):
    """Get closest Volland snapshot before signal time from cache."""
    if isinstance(trade_date, str):
        trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()

    day_snaps = cache.get(trade_date, [])
    if not day_snaps:
        return None

    parts = et_time_str.split(':')
    target_s = int(parts[0]) * 3600 + int(parts[1]) * 60 + (int(parts[2]) if len(parts) > 2 else 0)

    best = None
    for snap in day_snaps:
        ts_et = snap['ts_et']
        snap_s = ts_et.hour * 3600 + ts_et.minute * 60 + ts_et.second
        if snap_s <= target_s:
            best = snap

    if best:
        d = dict(best)
        # Parse LIS
        lis_str = d.get('lis') or ''
        lis_match = re.search(r'[\d,]+\.?\d*', lis_str.replace(',', ''))
        d['lis_val'] = float(lis_match.group()) if lis_match else None
        # Parse SVB
        svb_raw = d.get('svb')
        if svb_raw:
            try:
                svb_data = json.loads(svb_raw) if isinstance(svb_raw, str) else svb_raw
                if isinstance(svb_data, dict):
                    d['svb_val'] = float(svb_data.get('correlation', 0))
                else:
                    d['svb_val'] = float(svb_data)
            except (ValueError, TypeError, json.JSONDecodeError):
                d['svb_val'] = None
        else:
            d['svb_val'] = None
        # Parse DD
        dd_str = d.get('dd_hedging') or ''
        d['dd_dir'] = 'long' if 'long' in dd_str.lower() else ('short' if 'short' in dd_str.lower() else 'neutral')
        # Parse charm
        charm_raw = d.get('agg_charm')
        try:
            d['charm_val'] = float(charm_raw) if charm_raw else None
        except (ValueError, TypeError):
            d['charm_val'] = None
        return d
    return None


def enrich_with_volland(signals, cache):
    """Add Volland data to each signal."""
    for sig in signals:
        vd = get_volland_at_time(cache, sig['date'], sig['et_end'])
        if vd:
            sig['paradigm'] = vd.get('paradigm', '')
            sig['lis_val'] = vd.get('lis_val')
            sig['dd_dir'] = vd.get('dd_dir', 'neutral')
            sig['svb_val'] = vd.get('svb_val')
            sig['charm_val'] = vd.get('charm_val')

            if sig['lis_val'] is not None:
                sig['lis_dist'] = abs(sig['entry_price'] - sig['lis_val'])
            else:
                sig['lis_dist'] = None

            paradigm = (sig['paradigm'] or '').upper()
            sig['paradigm_aligned'] = (
                (sig['direction'] == 'bullish' and 'GEX' in paradigm) or
                (sig['direction'] == 'bearish' and 'AG' in paradigm)
            )
            sig['dd_aligned'] = (
                (sig['direction'] == 'bullish' and sig['dd_dir'] == 'long') or
                (sig['direction'] == 'bearish' and sig['dd_dir'] == 'short')
            )
            sig['charm_aligned'] = None
            if sig['charm_val'] is not None:
                sig['charm_aligned'] = (
                    (sig['direction'] == 'bullish' and sig['charm_val'] > 0) or
                    (sig['direction'] == 'bearish' and sig['charm_val'] < 0)
                )
        else:
            for k in ['paradigm', 'lis_val', 'dd_dir', 'svb_val', 'charm_val',
                       'lis_dist', 'paradigm_aligned', 'dd_aligned', 'charm_aligned']:
                sig[k] = None


# =====================================================================
# MAIN EXECUTION
# =====================================================================

print("=" * 90)
print("ES ABSORPTION STUDY v2 — Direction Fix + Trailing Stop")
print("=" * 90)

# ──────────────────────────────────────────────────────────────────────
# PART 0: Verify Signal #9 direction fix
# ──────────────────────────────────────────────────────────────────────

print("\n" + "=" * 90)
print("PART 0: VERIFY SIGNAL #9 (Mar 27, 9:47-9:48 ET)")
print("=" * 90)

with engine.connect() as conn:
    sig9_rows = conn.execute(text("""
        SELECT bar_idx, bar_open as open, bar_high as high, bar_low as low, bar_close as close,
               bar_volume as volume, bar_delta as delta, bar_buy_volume as buy_volume,
               bar_sell_volume as sell_volume,
               cvd_open, cvd_high, cvd_low, cvd_close,
               CASE WHEN bar_close >= bar_open THEN 'GREEN' ELSE 'RED' END as color,
               ts_start AT TIME ZONE 'America/New_York' as ts_start_et,
               ts_end AT TIME ZONE 'America/New_York' as ts_end_et
        FROM es_range_bars
        WHERE source = 'rithmic' AND trade_date = '2026-03-27'
          AND range_pts = 5.0 AND status = 'closed'
          AND ts_start AT TIME ZONE 'America/New_York'
              BETWEEN '2026-03-27 09:45:00' AND '2026-03-27 09:52:00'
        ORDER BY bar_idx
    """)).mappings().all()

print(f"\nBars at 9:45-9:52 ET on Mar 27:")
print(f"{'idx':>5} {'start_ET':>12} {'end_ET':>12} {'O':>8} {'H':>8} {'L':>8} {'C':>8} {'color':>5} {'vol':>6} {'delta':>7} {'buy':>6} {'sell':>6}")
print("-" * 110)
total_delta_9 = 0
for r in sig9_rows:
    d = dict(r)
    print(f"{d['bar_idx']:>5} {str(d['ts_start_et'])[11:19]:>12} {str(d['ts_end_et'])[11:19]:>12} "
          f"{d['open']:>8.2f} {d['high']:>8.2f} {d['low']:>8.2f} {d['close']:>8.2f} "
          f"{d['color']:>5} {d['volume']:>6} {d['delta']:>+7} {d['buy_volume']:>6} {d['sell_volume']:>6}")
    total_delta_9 += d['delta']

print(f"\nCombined delta for signal #9 bars: {total_delta_9:+}")
print(f"Direction: Positive delta + flat/red close = BEARISH absorption")
print(f"Previous study had this as BULLISH — WRONG. Fixed to BEARISH in v2.")

# ──────────────────────────────────────────────────────────────────────
# PART 1: Load data + define manual signals
# ──────────────────────────────────────────────────────────────────────

print("\n" + "=" * 90)
print("PART 1: LOADING DATA & DEFINING 14 MANUAL SIGNALS")
print("=" * 90)

bars_27 = load_bars('2026-03-27')
bars_16 = load_bars('2026-03-16')
print(f"Mar 27: {len(bars_27)} closed 5-pt bars")
print(f"Mar 16: {len(bars_16)} closed 5-pt bars")

# Signal #9 now has TWO bars (matching the user's description) and is BEARISH
# We need to find the two bars at 9:47-9:48 ET
sig9_bar_indices = [int(r['bar_idx']) for r in sig9_rows[:2]] if len(sig9_rows) >= 2 else [194]

SIGNALS = [
    (1, '2026-03-27', [391, 392], 'bearish',
     '2 bars: delta +314/+1576 (buyers), green candles, combined flat -> buyers absorbed'),
    (2, '2026-03-27', [372], 'bullish',
     'Single bar: dlt=-744, vol=9378, RED, body=4.75 -> sellers absorbed'),
    (3, '2026-03-27', [331], 'bullish',
     'Single bar: dlt=-982 (very neg), vol=5040, RED body=2.75 -> sellers absorbed'),
    (4, '2026-03-27', [321, 322], 'bullish',
     '2 bars: combined dlt=-1601, vol=25737, both RED -> sellers absorbed'),
    (5, '2026-03-27', [298], 'bearish',
     'Single bar: dlt=+535, RED body=2.50 -> buyers absorbed, price dropped'),
    (6, '2026-03-27', [277, 278], 'bullish',
     '2 bars: combined dlt=-1038, vol=22750, both RED -> climax selling, +32pt reversal'),
    # Signal 7: will be auto-detected
    (7, '2026-03-27', [271], 'unknown',
     'Single bar near 11:06 ET (auto-detect direction)'),
    (8, '2026-03-27', [251, 252], 'unknown',
     '2 bars: idx=251 GREEN dlt=+286, idx=252 RED dlt=-178 -> mixed signals'),
    # Signal 9: FIXED — two bars at 9:47-9:48 with positive delta = BEARISH
    (9, '2026-03-27', sig9_bar_indices, 'bearish',
     'FIX: 2 bars at 9:47-9:48, combined delta ~+1191, price flat -> BEARISH absorption'),
    (10, '2026-03-27', [177, 178], 'bullish',
     '2 bars: combined dlt=-2016, vol=13288, both RED -> sellers absorbed'),
    (11, '2026-03-16', [227], 'bullish',
     'Single bar: dlt=+239, GREEN body=3.75, vol=1455 -> moved in favor after drawdown'),
    (12, '2026-03-16', [268], 'bearish',
     'Single bar: dlt=+98, GREEN body=1.50/5.00 (doji-like) -> bearish'),
    (13, '2026-03-16', [278, 280, 281], 'bearish',
     '3 bars: main at 11:22, confirmed by 11:30 & 11:37'),
    (14, '2026-03-16', [291], 'bullish',
     'Single bar: dlt=-297, RED body=4.00, vol=1093'),
]

# Auto-detect signal 7 direction
mask_7 = bars_27['et_end'].apply(lambda x: abs(x.hour * 3600 + x.minute * 60 + x.second - (11*3600 + 6*60)) < 120)
candidates_7 = bars_27[mask_7]
if len(candidates_7) > 0:
    best = candidates_7.iloc[(candidates_7['et_end'].apply(
        lambda x: abs(x.hour * 3600 + x.minute * 60 + x.second - (11*3600 + 6*60)))).argmin()]
    sig7_idx = int(best['bar_idx'])
    SIGNALS[6] = (7, '2026-03-27', [sig7_idx], 'unknown',
                  f"Single bar: idx={sig7_idx}, dlt={best['bar_delta']}, "
                  f"{'GREEN' if best['is_green'] else 'RED'} body={best['body']:.2f}")
    print(f"  Signal 7 auto-detected: idx={sig7_idx}")

# Process signals
all_signals = []

for sig_num, sig_date, bar_indices, direction, desc in SIGNALS:
    df = bars_27 if sig_date == '2026-03-27' else bars_16
    sig_bars = df[df['bar_idx'].isin(bar_indices)].copy()
    if len(sig_bars) == 0:
        print(f"  WARNING: Signal {sig_num} - no bars found for indices {bar_indices}")
        continue

    is_multi = len(bar_indices) > 1
    last_bar = sig_bars.iloc[-1]
    first_bar = sig_bars.iloc[0]

    combined = {
        'sig_num': sig_num,
        'date': sig_date,
        'bar_indices': bar_indices,
        'is_multi': is_multi,
        'n_bars': len(bar_indices),
        'direction': direction,
        'description': desc,
        'entry_price': float(last_bar['bar_close']),
        'entry_idx': int(last_bar['bar_idx']),
        'et_end': last_bar['et_end'].strftime('%H:%M:%S'),
        'et_start': first_bar['et_start'].strftime('%H:%M:%S'),
    }

    if is_multi:
        combined['volume'] = int(sig_bars['bar_volume'].sum())
        combined['delta'] = int(sig_bars['bar_delta'].sum())
        combined['buy_vol'] = int(sig_bars['bar_buy_volume'].sum())
        combined['sell_vol'] = int(sig_bars['bar_sell_volume'].sum())
        combined['price_change'] = round(float(last_bar['bar_close'] - first_bar['bar_open']), 2)
        combined['high'] = float(sig_bars['bar_high'].max())
        combined['low'] = float(sig_bars['bar_low'].min())
        combined['bar_range'] = round(combined['high'] - combined['low'], 2)
        combined['body'] = abs(combined['price_change'])
        combined['body_ratio'] = round(combined['body'] / combined['bar_range'], 4) if combined['bar_range'] > 0 else 0
        combined['is_green'] = combined['price_change'] > 0
    else:
        bar = sig_bars.iloc[0]
        combined['volume'] = int(bar['bar_volume'])
        combined['delta'] = int(bar['bar_delta'])
        combined['buy_vol'] = int(bar['bar_buy_volume'])
        combined['sell_vol'] = int(bar['bar_sell_volume'])
        combined['price_change'] = round(float(bar['bar_close'] - bar['bar_open']), 2)
        combined['high'] = float(bar['bar_high'])
        combined['low'] = float(bar['bar_low'])
        combined['bar_range'] = round(float(bar['bar_range']), 2)
        combined['body'] = round(float(bar['body']), 2)
        combined['body_ratio'] = round(float(bar['body_ratio']), 4) if pd.notna(bar['body_ratio']) else 0
        combined['is_green'] = bool(bar['is_green'])

    # Auto-detect direction for unknowns
    if direction == 'unknown':
        if combined['delta'] > 0 and (not combined['is_green'] or combined['body_ratio'] < 0.4):
            combined['direction'] = 'bearish'
        elif combined['delta'] < 0 and (combined['is_green'] or combined['body_ratio'] < 0.4):
            combined['direction'] = 'bullish'
        else:
            combined['direction'] = 'bearish' if combined['delta'] > 0 else 'bullish'

    all_signals.append(combined)

print(f"\nProcessed {len(all_signals)} manual signals")

# Print signal summary
print(f"\n{'#':>3} {'Date':>10} {'Dir':>7} {'Bars':>8} {'Time ET':>12} {'Entry':>8} {'Delta':>7} {'Vol':>6} {'Body%':>6}")
print("-" * 80)
for s in all_signals:
    print(f"{s['sig_num']:>3} {s['date']:>10} {s['direction']:>7} {str(s['bar_indices']):>8} "
          f"{s['et_end']:>12} {s['entry_price']:>8.2f} {s['delta']:>+7} {s['volume']:>6} {s['body_ratio']:>6.2f}")


# ──────────────────────────────────────────────────────────────────────
# PART 2: Trail simulation on 14 manual signals
# ──────────────────────────────────────────────────────────────────────

print("\n" + "=" * 90)
print("PART 2: TRAIL CONFIGS ON 14 MANUAL SIGNALS")
print("=" * 90)

# Also compute MFE/MAE for context
for s in all_signals:
    df = bars_27 if s['date'] == '2026-03-27' else bars_16
    mask = df['bar_idx'] > s['entry_idx']
    future = df.loc[mask].head(100)
    if len(future) > 0:
        if s['direction'] == 'bullish':
            s['mfe_100'] = round(float(future['bar_high'].max() - s['entry_price']), 2)
            s['mae_100'] = round(float(s['entry_price'] - future['bar_low'].min()), 2)
        else:
            s['mfe_100'] = round(float(s['entry_price'] - future['bar_low'].min()), 2)
            s['mae_100'] = round(float(future['bar_high'].max() - s['entry_price']), 2)
    else:
        s['mfe_100'] = 0
        s['mae_100'] = 0

manual_results = {}  # config_key -> list of results

for cfg_key, cfg in TRAIL_CONFIGS.items():
    print(f"\n--- {cfg['name']} (SL={cfg['sl']}) ---")
    results = []

    for s in all_signals:
        df = bars_27 if s['date'] == '2026-03-27' else bars_16
        r = simulate_trail(df, s['entry_idx'], s['direction'], s['entry_price'], cfg)
        r['sig_num'] = s['sig_num']
        r['direction'] = s['direction']
        results.append(r)

    manual_results[cfg_key] = results

    # Per-signal detail
    print(f"  {'#':>3} {'Dir':>5} {'MFE':>6} {'MAE':>6} {'Outcome':>15} {'PnL':>7} {'MaxP':>6} {'Bars':>5}")
    print("  " + "-" * 60)
    for s, r in zip(all_signals, results):
        print(f"  {s['sig_num']:>3} {s['direction'][:5]:>5} {s['mfe_100']:>6.1f} {s['mae_100']:>6.1f} "
              f"{r['outcome']:>15} {r['pnl']:>+7.1f} {r['max_profit']:>6.1f} {r['bars_held']:>5}")

    stats = compute_stats(results)
    print(f"\n  TOTAL: {stats['count']} signals, WR={stats['wr']:.1f}%, PnL={stats['pnl']:+.1f}, "
          f"MaxDD={stats['max_dd']:.1f}, PF={stats['pf']:.2f}, Avg PnL={stats['avg_pnl']:+.2f}, "
          f"Avg Bars={stats['avg_bars']:.1f}")

# Comparison table
print("\n" + "=" * 90)
print("COMPARISON TABLE — 14 MANUAL SIGNALS")
print("=" * 90)
print(f"\n{'Config':>30} {'Sigs':>5} {'WR%':>6} {'PnL':>8} {'MaxDD':>7} {'PF':>6} {'Avg PnL':>8} {'AvgBars':>8}")
print("-" * 88)
for cfg_key, cfg in TRAIL_CONFIGS.items():
    stats = compute_stats(manual_results[cfg_key])
    print(f"{cfg['name']:>30} {stats['count']:>5} {stats['wr']:>6.1f} {stats['pnl']:>+8.1f} "
          f"{stats['max_dd']:>7.1f} {stats['pf']:>6.2f} {stats['avg_pnl']:>+8.2f} {stats['avg_bars']:>8.1f}")

# Also add fixed SL8/TP10 for reference
print(f"\n{'Fixed SL8/TP10 (reference)':>30}", end='')
# Compute fixed TP outcomes
fixed_results = []
for s in all_signals:
    df = bars_27 if s['date'] == '2026-03-27' else bars_16
    mask = df['bar_idx'] > s['entry_idx']
    future = df.loc[mask].head(100)
    pnl = 0
    outcome = 'EXPIRED'
    bars_held = len(future)
    for i, (_, bar) in enumerate(future.iterrows()):
        if s['direction'] == 'bullish':
            if bar['bar_low'] <= s['entry_price'] - 8:
                pnl = -8; outcome = 'LOSS'; bars_held = i+1; break
            if bar['bar_high'] >= s['entry_price'] + 10:
                pnl = 10; outcome = 'WIN'; bars_held = i+1; break
        else:
            if bar['bar_high'] >= s['entry_price'] + 8:
                pnl = -8; outcome = 'LOSS'; bars_held = i+1; break
            if bar['bar_low'] <= s['entry_price'] - 10:
                pnl = 10; outcome = 'WIN'; bars_held = i+1; break
    if outcome == 'EXPIRED' and len(future) > 0:
        lc = float(future.iloc[-1]['bar_close'])
        pnl = round((lc - s['entry_price']) if s['direction'] == 'bullish' else (s['entry_price'] - lc), 2)
    fixed_results.append({'pnl': pnl, 'outcome': outcome, 'bars_held': bars_held, 'max_profit': 0, 'exit_price': 0, 'mfe': 0, 'mae': 0})
fs = compute_stats(fixed_results)
print(f" {fs['count']:>5} {fs['wr']:>6.1f} {fs['pnl']:>+8.1f} {fs['max_dd']:>7.1f} {fs['pf']:>6.2f} {fs['avg_pnl']:>+8.2f} {fs['avg_bars']:>8.1f}")


# ──────────────────────────────────────────────────────────────────────
# PART 3: Direction fix impact analysis
# ──────────────────────────────────────────────────────────────────────

print("\n" + "=" * 90)
print("PART 3: DIRECTION FIX IMPACT — HOW MANY SIGNALS CHANGED?")
print("=" * 90)

print("\nLoading all March 5-pt rithmic bars...")
all_march = load_all_march_bars()
print(f"Total March bars: {len(all_march)}")

all_march_mh = all_march[
    all_march['et_end'].apply(lambda x: dtime(9, 30) <= x.time() <= dtime(16, 0))
].copy()
print(f"Market hours bars: {len(all_march_mh)}")

# Scan with OLD direction logic (v1: positive delta = bullish)
def scan_old_direction(df, criteria=None):
    """v1 scan with WRONG direction logic for comparison."""
    if criteria is None:
        criteria = MEDIUM_CRITERIA
    signals = []
    dates = sorted(df['trade_date'].unique())

    for td in dates:
        day_df = df[df['trade_date'] == td].reset_index(drop=True)
        if len(day_df) < 25:
            continue

        last_bull_idx = -100
        last_bear_idx = -100
        cd = criteria['cooldown_bars']

        for i in range(20, len(day_df)):
            bar = day_df.iloc[i]
            idx = int(bar['bar_idx'])

            if criteria.get('market_hours_only', True):
                et = bar['et_end']
                if hasattr(et, 'time'):
                    if et.time() < dtime(9, 35) or et.time() > dtime(15, 55):
                        continue

            single_hit = False
            direction = None

            if (abs(bar['bar_delta']) >= criteria['min_abs_delta_single'] and
                bar['bar_volume'] >= criteria['min_volume'] and
                pd.notna(bar['body_ratio']) and bar['body_ratio'] <= criteria['max_body_ratio']):

                # OLD (WRONG) logic: positive delta = bearish, negative = bullish
                # Wait — actually v1 had the SAME text. Let me re-read:
                # v1: "if bar['bar_delta'] > 0 and (not bar['is_green'] or bar['body_ratio'] <= 0.40): direction = 'bearish'"
                # That IS the correct absorption logic. The bug was in signal #9 MANUAL labeling, not scan direction.
                # But user says: "positive delta treated as bullish instead of bearish"
                # So the v1 scan logic was actually correct for single-bar, but let's check both.
                # Actually the v1 scan had correct direction logic but user's MANUAL signal 9 was wrong.
                # The scan function direction logic is the same in v1 and v2.
                if bar['bar_delta'] > 0 and (not bar['is_green'] or bar['body_ratio'] <= 0.40):
                    direction = 'bearish'
                    single_hit = True
                elif bar['bar_delta'] < 0 and (bar['is_green'] or bar['body_ratio'] <= 0.40):
                    direction = 'bullish'
                    single_hit = True

            two_bar_hit = False
            if not single_hit and criteria.get('two_bar_window', True) and i >= 1:
                prev = day_df.iloc[i - 1]
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
                    elif combined_delta < 0 and (net_price >= 0 or combined_body_ratio <= 0.40):
                        direction = 'bullish'
                        two_bar_hit = True

            if not single_hit and not two_bar_hit:
                continue

            if direction == 'bullish' and idx - last_bull_idx < cd:
                continue
            if direction == 'bearish' and idx - last_bear_idx < cd:
                continue

            signals.append({
                'date': td, 'bar_idx': idx, 'direction': direction,
                'is_multi': two_bar_hit, 'entry_price': float(bar['bar_close']),
                'et_end': bar['et_end'].strftime('%H:%M:%S') if hasattr(bar['et_end'], 'strftime') else str(bar['et_end']),
                'delta': int(bar['bar_delta']) if single_hit else combined_delta,
                'volume': int(bar['bar_volume']) if single_hit else combined_vol,
                'body_ratio': float(bar['body_ratio']) if single_hit else combined_body_ratio,
                'vol_ratio': float(bar['vol_ratio']) if pd.notna(bar['vol_ratio']) else None,
            })

            if direction == 'bullish':
                last_bull_idx = idx
            else:
                last_bear_idx = idx

    return signals

# The scan direction logic in v1 was actually the same as v2 — the bug was only in manual signal #9.
# But let's verify by running both and comparing.
print("\nScanning with v2 (fixed) direction logic...")
v2_sigs = scan_signals(all_march_mh)
print(f"  v2 signals: {len(v2_sigs)}")

print("Scanning with v1 direction logic (for comparison)...")
v1_sigs = scan_old_direction(all_march_mh)
print(f"  v1 signals: {len(v1_sigs)}")

# Compare
v2_keys = set((str(s['date']), s['bar_idx'], s['direction']) for s in v2_sigs)
v1_keys = set((str(s['date']), s['bar_idx'], s['direction']) for s in v1_sigs)

only_v2 = v2_keys - v1_keys
only_v1 = v1_keys - v2_keys
both = v2_keys & v1_keys

print(f"\n  Same in both: {len(both)}")
print(f"  Only in v2: {len(only_v2)}")
print(f"  Only in v1: {len(only_v1)}")

# Direction changes
v2_by_idx = {(str(s['date']), s['bar_idx']): s['direction'] for s in v2_sigs}
v1_by_idx = {(str(s['date']), s['bar_idx']): s['direction'] for s in v1_sigs}
common_idx = set(v2_by_idx.keys()) & set(v1_by_idx.keys())
flipped = [(k, v1_by_idx[k], v2_by_idx[k]) for k in common_idx if v1_by_idx[k] != v2_by_idx[k]]
print(f"  Direction flipped: {len(flipped)}")
if flipped:
    for k, old_d, new_d in flipped[:10]:
        print(f"    {k[0]} idx={k[1]}: {old_d} -> {new_d}")

print(f"\nNote: The scan direction logic was IDENTICAL in v1 and v2.")
print(f"The direction bug was only in the MANUAL labeling of signal #9 (bullish -> bearish).")
print(f"The scan correctly derives direction from delta vs price for all {len(v2_sigs)} signals.")


# ──────────────────────────────────────────────────────────────────────
# PART 4: Full March scan with all 6 trail configs
# ──────────────────────────────────────────────────────────────────────

print("\n" + "=" * 90)
print("PART 4: FULL MARCH SCAN — ALL 6 TRAIL CONFIGS")
print("=" * 90)

march_sigs = v2_sigs  # already scanned

n_days = all_march_mh['trade_date'].nunique()
n_bull = sum(1 for s in march_sigs if s['direction'] == 'bullish')
n_bear = sum(1 for s in march_sigs if s['direction'] == 'bearish')
print(f"\nTotal signals: {len(march_sigs)} ({len(march_sigs)/n_days:.1f} per day)")
print(f"Bullish: {n_bull}, Bearish: {n_bear}")
print(f"Days: {n_days}")

# Simulate all configs
march_results = {}  # config_key -> list of results

for cfg_key, cfg in TRAIL_CONFIGS.items():
    print(f"\nSimulating {cfg['name']}...")
    results = []

    for sig in march_sigs:
        td = sig['date']
        if isinstance(td, str):
            td = datetime.strptime(td, '%Y-%m-%d').date()
        day_df = all_march_mh[all_march_mh['trade_date'] == td].copy()
        if len(day_df) == 0:
            continue

        r = simulate_trail(day_df, sig['bar_idx'], sig['direction'], sig['entry_price'], cfg)
        r['date'] = sig['date']
        r['bar_idx'] = sig['bar_idx']
        r['direction'] = sig['direction']
        r['et_end'] = sig['et_end']
        r['entry_price'] = sig['entry_price']
        r['delta'] = sig['delta']
        r['volume'] = sig['volume']
        results.append(r)

    march_results[cfg_key] = results
    stats = compute_stats(results)
    print(f"  {stats['count']} signals: WR={stats['wr']:.1f}%, PnL={stats['pnl']:+.1f}, "
          f"MaxDD={stats['max_dd']:.1f}, PF={stats['pf']:.2f}")

# Also compute fixed SL8/TP10 for reference
print("\nSimulating Fixed SL8/TP10 (reference)...")
fixed_march_results = []
for sig in march_sigs:
    td = sig['date']
    if isinstance(td, str):
        td = datetime.strptime(td, '%Y-%m-%d').date()
    day_df = all_march_mh[all_march_mh['trade_date'] == td].copy()
    if len(day_df) == 0:
        continue

    mask = day_df['bar_idx'] > sig['bar_idx']
    future = day_df.loc[mask].head(100)
    pnl = 0
    outcome = 'EXPIRED'
    bars_held = len(future)
    max_p = 0
    mae_v = 0
    for i, (_, bar) in enumerate(future.iterrows()):
        if sig['direction'] == 'bullish':
            mp = bar['bar_high'] - sig['entry_price']
            ma = sig['entry_price'] - bar['bar_low']
            if bar['bar_low'] <= sig['entry_price'] - 8:
                pnl = -8; outcome = 'LOSS'; bars_held = i+1; break
            if bar['bar_high'] >= sig['entry_price'] + 10:
                pnl = 10; outcome = 'WIN'; bars_held = i+1; break
        else:
            mp = sig['entry_price'] - bar['bar_low']
            ma = bar['bar_high'] - sig['entry_price']
            if bar['bar_high'] >= sig['entry_price'] + 8:
                pnl = -8; outcome = 'LOSS'; bars_held = i+1; break
            if bar['bar_low'] <= sig['entry_price'] - 10:
                pnl = 10; outcome = 'WIN'; bars_held = i+1; break
        max_p = max(max_p, mp)
        mae_v = max(mae_v, ma)
    if outcome == 'EXPIRED' and len(future) > 0:
        lc = float(future.iloc[-1]['bar_close'])
        pnl = round((lc - sig['entry_price']) if sig['direction'] == 'bullish' else (sig['entry_price'] - lc), 2)
    fixed_march_results.append({
        'pnl': pnl, 'outcome': outcome, 'bars_held': bars_held,
        'max_profit': max_p, 'exit_price': 0, 'mfe': max_p, 'mae': mae_v,
        'date': sig['date'], 'bar_idx': sig['bar_idx'],
        'direction': sig['direction'], 'et_end': sig['et_end'],
    })
fixed_march_stats = compute_stats(fixed_march_results)
print(f"  {fixed_march_stats['count']} signals: WR={fixed_march_stats['wr']:.1f}%, PnL={fixed_march_stats['pnl']:+.1f}, "
      f"MaxDD={fixed_march_stats['max_dd']:.1f}, PF={fixed_march_stats['pf']:.2f}")

# COMPARISON TABLE
print("\n" + "=" * 90)
print("COMPARISON TABLE — FULL MARCH SCAN")
print("=" * 90)
print(f"\n{'Config':>30} {'Sigs':>5} {'WR%':>6} {'PnL':>8} {'MaxDD':>7} {'PF':>6} {'Avg PnL':>8} {'AvgBars':>8} {'GrossW':>8} {'GrossL':>8}")
print("-" * 106)
for cfg_key, cfg in TRAIL_CONFIGS.items():
    stats = compute_stats(march_results[cfg_key])
    print(f"{cfg['name']:>30} {stats['count']:>5} {stats['wr']:>6.1f} {stats['pnl']:>+8.1f} "
          f"{stats['max_dd']:>7.1f} {stats['pf']:>6.2f} {stats['avg_pnl']:>+8.2f} {stats['avg_bars']:>8.1f} "
          f"{stats['gross_wins']:>8.1f} {stats['gross_losses']:>8.1f}")
print(f"{'Fixed SL8/TP10 (reference)':>30} {fixed_march_stats['count']:>5} {fixed_march_stats['wr']:>6.1f} {fixed_march_stats['pnl']:>+8.1f} "
      f"{fixed_march_stats['max_dd']:>7.1f} {fixed_march_stats['pf']:>6.2f} {fixed_march_stats['avg_pnl']:>+8.2f} {fixed_march_stats['avg_bars']:>8.1f} "
      f"{fixed_march_stats['gross_wins']:>8.1f} {fixed_march_stats['gross_losses']:>8.1f}")

# By direction
print("\n--- By Direction ---")
for cfg_key, cfg in TRAIL_CONFIGS.items():
    results = march_results[cfg_key]
    bull_r = [r for r in results if r['direction'] == 'bullish']
    bear_r = [r for r in results if r['direction'] == 'bearish']
    bs = compute_stats(bull_r)
    ss = compute_stats(bear_r)
    print(f"  {cfg['name']:>30}  BULL: {bs['count']:>4}t WR={bs['wr']:>5.1f}% PnL={bs['pnl']:>+7.1f} DD={bs['max_dd']:>5.1f}  "
          f"BEAR: {ss['count']:>4}t WR={ss['wr']:>5.1f}% PnL={ss['pnl']:>+7.1f} DD={ss['max_dd']:>5.1f}")

# By time of day
print("\n--- By Time of Day (Best Trail Config) ---")

# Find best config (highest PnL)
best_cfg_key = max(march_results.keys(), key=lambda k: compute_stats(march_results[k])['pnl'])
best_cfg = TRAIL_CONFIGS[best_cfg_key]
best_results = march_results[best_cfg_key]
best_stats = compute_stats(best_results)

print(f"\nBest config: {best_cfg['name']} (PnL={best_stats['pnl']:+.1f})")
print(f"\n{'Time':>12} {'Sigs':>5} {'WR%':>6} {'PnL':>8} {'MaxDD':>7} {'PF':>6} {'Avg PnL':>8}")
print("-" * 65)

for h_start, h_end, label in [(9, 10, '09:30-10:00'), (10, 11, '10:00-11:00'),
                                (11, 12, '11:00-12:00'), (12, 13, '12:00-13:00'),
                                (13, 14, '13:00-14:00'), (14, 15, '14:00-15:00'),
                                (15, 16, '15:00-16:00')]:
    hour_results = [r for r in best_results
                    if r.get('et_end') and int(r['et_end'][:2]) >= h_start and int(r['et_end'][:2]) < h_end]
    if hour_results:
        hs = compute_stats(hour_results)
        print(f"{label:>12} {hs['count']:>5} {hs['wr']:>6.1f} {hs['pnl']:>+8.1f} {hs['max_dd']:>7.1f} {hs['pf']:>6.2f} {hs['avg_pnl']:>+8.2f}")

# Daily breakdown
print(f"\n--- Daily Breakdown ({best_cfg['name']}) ---")
daily_pnl = defaultdict(lambda: {'pnl': 0, 'count': 0, 'wins': 0})
for r in best_results:
    d = str(r['date'])
    daily_pnl[d]['pnl'] += r['pnl']
    daily_pnl[d]['count'] += 1
    if r['pnl'] > 0:
        daily_pnl[d]['wins'] += 1

print(f"{'Date':>12} {'Sigs':>5} {'Wins':>5} {'PnL':>8}")
print("-" * 35)
for d in sorted(daily_pnl.keys()):
    dp = daily_pnl[d]
    print(f"{d:>12} {dp['count']:>5} {dp['wins']:>5} {dp['pnl']:>+8.1f}")


# ──────────────────────────────────────────────────────────────────────
# PART 5: Volland filters on best trail config
# ──────────────────────────────────────────────────────────────────────

print("\n" + "=" * 90)
print("PART 5: VOLLAND FILTERS (on best trail config)")
print("=" * 90)

print("\nLoading Volland data...")
volland_cache = load_volland_cache()
total_vol_snaps = sum(len(v) for v in volland_cache.values())
print(f"Loaded {total_vol_snaps} Volland snapshots across {len(volland_cache)} days")

# Enrich march signals with Volland data
# We need to pair the signal with its result
sig_with_results = []
for sig, r in zip(march_sigs, best_results):
    combined = {**sig, **r}
    sig_with_results.append(combined)

enrich_with_volland(sig_with_results, volland_cache)

total_with_volland = [s for s in sig_with_results if s.get('paradigm') is not None]
print(f"Signals with Volland data: {len(total_with_volland)}/{len(sig_with_results)}")

def filter_stats_trail(sigs, label):
    """Compute stats for pre-computed trail results."""
    if not sigs:
        print(f"  {label}: 0 signals")
        return {'label': label, 'count': 0}

    stats = compute_stats(sigs)
    print(f"  {label}: {stats['count']} signals, WR={stats['wr']:.1f}%, PnL={stats['pnl']:+.1f}, "
          f"MaxDD={stats['max_dd']:.1f}, PF={stats['pf']:.2f}")
    return {'label': label, **stats}

print(f"\nVolland Filter Impact ({best_cfg['name']}):")
print("-" * 80)

filter_stats_trail(total_with_volland, 'No filter (baseline)')

# LIS proximity
for dist in [5, 10, 15, 20]:
    filtered = [s for s in total_with_volland if s.get('lis_dist') is not None and s['lis_dist'] <= dist]
    filter_stats_trail(filtered, f'LIS within {dist} pts')

# Paradigm
para_aligned = [s for s in total_with_volland if s.get('paradigm_aligned')]
filter_stats_trail(para_aligned, 'Paradigm aligned')
para_not = [s for s in total_with_volland if s.get('paradigm_aligned') == False]
filter_stats_trail(para_not, 'Paradigm NOT aligned')

# DD alignment
dd_aligned = [s for s in total_with_volland if s.get('dd_aligned')]
filter_stats_trail(dd_aligned, 'DD aligned')
dd_not = [s for s in total_with_volland if s.get('dd_aligned') == False]
filter_stats_trail(dd_not, 'DD NOT aligned')

# Charm alignment
charm_aligned = [s for s in total_with_volland if s.get('charm_aligned')]
filter_stats_trail(charm_aligned, 'Charm aligned')
charm_not = [s for s in total_with_volland if s.get('charm_aligned') == False]
filter_stats_trail(charm_not, 'Charm NOT aligned')

# SVB
svb_pos = [s for s in total_with_volland if s.get('svb_val') is not None and s['svb_val'] >= 0]
filter_stats_trail(svb_pos, 'SVB >= 0')
svb_neg = [s for s in total_with_volland if s.get('svb_val') is not None and s['svb_val'] < 0]
filter_stats_trail(svb_neg, 'SVB < 0')

# Bearish only
bears_only = [s for s in total_with_volland if s['direction'] == 'bearish']
filter_stats_trail(bears_only, 'Bearish only')
bulls_only = [s for s in total_with_volland if s['direction'] == 'bullish']
filter_stats_trail(bulls_only, 'Bullish only')

# Combined filters
print("\nCombined Filters:")
combo1 = [s for s in total_with_volland
          if s.get('lis_dist') is not None and s['lis_dist'] <= 15
          and s.get('paradigm_aligned')]
filter_stats_trail(combo1, 'LIS<=15 + Paradigm')

combo2 = [s for s in total_with_volland
          if s.get('paradigm_aligned') and s.get('dd_aligned')]
filter_stats_trail(combo2, 'Paradigm + DD aligned')

combo3 = [s for s in total_with_volland
          if s.get('lis_dist') is not None and s['lis_dist'] <= 15
          and s.get('dd_aligned')]
filter_stats_trail(combo3, 'LIS<=15 + DD aligned')

combo4 = [s for s in total_with_volland
          if s.get('lis_dist') is not None and s['lis_dist'] <= 15
          and s.get('paradigm_aligned') and s.get('dd_aligned')]
filter_stats_trail(combo4, 'LIS<=15 + Paradigm + DD')

# Bearish + paradigm aligned
combo5 = [s for s in total_with_volland
          if s['direction'] == 'bearish' and s.get('paradigm_aligned')]
filter_stats_trail(combo5, 'Bearish + Paradigm aligned')

# SVB >= 0 + paradigm aligned
combo6 = [s for s in total_with_volland
          if s.get('svb_val') is not None and s['svb_val'] >= 0
          and s.get('paradigm_aligned')]
filter_stats_trail(combo6, 'SVB>=0 + Paradigm aligned')

# Bearish + LIS <= 10
combo7 = [s for s in total_with_volland
          if s['direction'] == 'bearish'
          and s.get('lis_dist') is not None and s['lis_dist'] <= 10]
filter_stats_trail(combo7, 'Bearish + LIS<=10')

# Paradigm subtype
print("\nParadigm Subtype Breakdown:")
paradigm_groups = defaultdict(list)
for s in total_with_volland:
    p = s.get('paradigm') or 'NONE'
    paradigm_groups[p.upper()].append(s)
for p_name, p_sigs in sorted(paradigm_groups.items(), key=lambda x: -len(x[1])):
    if len(p_sigs) >= 3:
        filter_stats_trail(p_sigs, f'Paradigm={p_name}')


# ──────────────────────────────────────────────────────────────────────
# PART 6: Export
# ──────────────────────────────────────────────────────────────────────

print("\n" + "=" * 90)
print("PART 6: EXPORT")
print("=" * 90)

# Prepare export data: all march signals with all trail config results
export_rows = []
for i, sig in enumerate(march_sigs):
    row = {
        'date': sig['date'],
        'bar_idx': sig['bar_idx'],
        'direction': sig['direction'],
        'is_multi': sig['is_multi'],
        'entry_price': sig['entry_price'],
        'et_end': sig['et_end'],
        'delta': sig['delta'],
        'volume': sig['volume'],
        'body_ratio': sig['body_ratio'],
        'vol_ratio': sig['vol_ratio'],
    }

    # Add Volland data if available
    if i < len(sig_with_results):
        swr = sig_with_results[i]
        for k in ['paradigm', 'lis_val', 'lis_dist', 'dd_dir', 'svb_val',
                   'charm_val', 'paradigm_aligned', 'dd_aligned', 'charm_aligned']:
            row[k] = swr.get(k)

    # Add trail results for each config
    for cfg_key in TRAIL_CONFIGS:
        if i < len(march_results[cfg_key]):
            r = march_results[cfg_key][i]
            row[f'{cfg_key}_outcome'] = r['outcome']
            row[f'{cfg_key}_pnl'] = r['pnl']
            row[f'{cfg_key}_max_profit'] = r['max_profit']
            row[f'{cfg_key}_bars_held'] = r['bars_held']
            row[f'{cfg_key}_mfe'] = r.get('mfe', 0)
            row[f'{cfg_key}_mae'] = r.get('mae', 0)

    # Add fixed TP reference
    if i < len(fixed_march_results):
        fr = fixed_march_results[i]
        row['fixed_outcome'] = fr['outcome']
        row['fixed_pnl'] = fr['pnl']

    export_rows.append(row)

export_df = pd.DataFrame(export_rows)
export_path = 'exports/manual_absorption_study_v2.csv'
export_df.to_csv(export_path, index=False)
print(f"Exported {len(export_df)} signals to {export_path}")

# Print final summary
print("\n" + "=" * 90)
print("FINAL SUMMARY")
print("=" * 90)

print(f"\n1. Direction Fix: Signal #9 corrected from BULLISH to BEARISH")
print(f"   The scan direction logic was already correct in v1 — only the manual label was wrong.")
print(f"   All {len(march_sigs)} scan signals use correct direction logic.")

print(f"\n2. Trail vs Fixed TP comparison:")
print(f"   Fixed SL8/TP10: PnL={fixed_march_stats['pnl']:+.1f}, WR={fixed_march_stats['wr']:.1f}%, MaxDD={fixed_march_stats['max_dd']:.1f}")
print(f"   Best trail ({best_cfg['name']}): PnL={best_stats['pnl']:+.1f}, WR={best_stats['wr']:.1f}%, MaxDD={best_stats['max_dd']:.1f}")
improvement = best_stats['pnl'] - fixed_march_stats['pnl']
print(f"   Improvement: {improvement:+.1f} pts")

# Find best config by various criteria
best_pnl_key = max(march_results.keys(), key=lambda k: compute_stats(march_results[k])['pnl'])
best_wr_key = max(march_results.keys(), key=lambda k: compute_stats(march_results[k])['wr'])
best_pf_key = max(march_results.keys(), key=lambda k: compute_stats(march_results[k])['pf'])
best_dd_key = min(march_results.keys(), key=lambda k: compute_stats(march_results[k])['max_dd'])

print(f"\n3. Best configs by metric:")
s = compute_stats(march_results[best_pnl_key])
print(f"   Best PnL:  {TRAIL_CONFIGS[best_pnl_key]['name']} — PnL={s['pnl']:+.1f}, WR={s['wr']:.1f}%, MaxDD={s['max_dd']:.1f}")
s = compute_stats(march_results[best_wr_key])
print(f"   Best WR:   {TRAIL_CONFIGS[best_wr_key]['name']} — PnL={s['pnl']:+.1f}, WR={s['wr']:.1f}%, MaxDD={s['max_dd']:.1f}")
s = compute_stats(march_results[best_pf_key])
print(f"   Best PF:   {TRAIL_CONFIGS[best_pf_key]['name']} — PnL={s['pnl']:+.1f}, WR={s['wr']:.1f}%, PF={s['pf']:.2f}")
s = compute_stats(march_results[best_dd_key])
print(f"   Best DD:   {TRAIL_CONFIGS[best_dd_key]['name']} — PnL={s['pnl']:+.1f}, MaxDD={s['max_dd']:.1f}")

print(f"\nScript: _study_manual_absorption_v2.py")
print(f"Export: {export_path}")
print("Done.")
