"""
GEX Long Full Backtest -- Reconstructed from raw historical data.
Uses volland_snapshots (paradigm/LIS/target), chain_snapshots (spot/GEX),
and es_delta_bars + chain_snapshots spot for forward price simulation.

Research only -- does NOT modify any code or database.
"""

import os
import re
import json
import numpy as np
import pandas as pd
from datetime import datetime, time as dtime, timedelta
from sqlalchemy import create_engine, text
from itertools import product
import pytz

NY = pytz.timezone("US/Eastern")
DB_URL = os.environ.get("DATABASE_URL", "")
engine = create_engine(DB_URL)

# ============================================================
# STEP 0: Load all raw data
# ============================================================

def parse_dollar(s):
    """Parse '$6,875' -> 6875.0"""
    if not s or s in ('None', 'null', '', 'Terms Of Service', 'Undefined'):
        return None
    s = s.replace('$', '').replace(',', '').strip()
    try:
        return float(s)
    except:
        return None


def load_volland_gex():
    """Load all volland snapshots where paradigm starts with GEX."""
    print("Loading volland GEX snapshots...")
    q = text("""
        SELECT ts,
               payload->'statistics'->>'paradigm' as paradigm,
               payload->'statistics'->>'lines_in_sand' as lis,
               payload->'statistics'->>'target' as target,
               payload->'statistics'->>'delta_decay_hedging' as dd,
               payload->'statistics'->>'aggregatedCharm' as charm
        FROM volland_snapshots
        WHERE payload->'statistics'->>'paradigm' LIKE 'GEX%'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)

    df['lis_val'] = df['lis'].apply(parse_dollar)
    df['target_val'] = df['target'].apply(parse_dollar)
    df['dd_val'] = df['dd'].apply(parse_dollar)
    df['charm_val'] = df['charm'].apply(lambda x: float(x) if x and x not in ('None','') else None)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['trade_date'] = df['ts'].dt.tz_convert(NY).dt.date

    print(f"  Loaded {len(df)} GEX snapshots across {df['trade_date'].nunique()} days")
    print(f"  Paradigm breakdown: {df['paradigm'].value_counts().to_dict()}")
    return df


def load_chain_spot():
    """Load chain_snapshots spot prices for price path simulation."""
    print("Loading chain spot prices...")
    q = text("""
        SELECT ts, spot
        FROM chain_snapshots
        WHERE spot IS NOT NULL AND spot > 0
        AND ts >= '2026-01-21' AND ts <= '2026-03-07'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['trade_date'] = df['ts'].dt.tz_convert(NY).dt.date
    print(f"  Loaded {len(df)} spot observations across {df['trade_date'].nunique()} days")
    return df


def load_es_bars():
    """Load ES 1-minute bars for higher-res forward sim where available."""
    print("Loading ES 1-minute bars...")
    q = text("""
        SELECT ts, trade_date, bar_open_price, bar_high_price, bar_low_price, bar_close_price
        FROM es_delta_bars
        ORDER BY ts
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    print(f"  Loaded {len(df)} bars across {df['trade_date'].nunique()} days")
    return df


def load_chain_gex(sample_every_n=10):
    """Load chain_snapshots and compute GEX per strike.

    Columns layout (from DB):
    0:Volume, 1:OpenInt, 2:IV, 3:Gamma, 4:Delta, 5:BID, 6:BIDQ, 7:ASK, 8:ASKQ, 9:LAST,
    10:Strike,
    11:LAST, 12:ASK, 13:ASKQ, 14:BID, 15:BIDQ, 16:Delta, 17:Gamma, 18:IV, 19:OpenInt, 20:Volume

    Left side = Calls, Right side = Puts
    """
    print(f"Loading chain snapshots for GEX computation (every {sample_every_n}th snapshot)...")

    # Get all chain snapshot IDs in the GEX date range
    q = text("""
        SELECT id, ts, spot, rows
        FROM chain_snapshots
        WHERE spot IS NOT NULL AND spot > 0
        AND ts >= '2026-01-21' AND ts <= '2026-03-07'
        ORDER BY ts
    """)

    results = []
    with engine.connect() as conn:
        cursor = conn.execute(q)
        count = 0
        processed = 0
        for row in cursor:
            count += 1
            if count % sample_every_n != 0:
                continue
            processed += 1

            snap_id = row[0]
            ts = row[1]
            spot = row[2]
            chain_rows = row[3]

            if not chain_rows:
                continue

            best_plus_gex = None
            best_plus_gex_strike = None
            best_minus_gex = None
            best_minus_gex_strike = None

            for cr in chain_rows:
                strike = cr[10]
                c_gamma = cr[3] if cr[3] else 0
                c_oi = cr[1] if cr[1] else 0
                p_gamma = cr[17] if cr[17] else 0
                p_oi = cr[19] if cr[19] else 0

                # net_gex = C_Gamma * C_OI * 100 + (-P_Gamma * P_OI * 100)
                # Note: P_Gamma from chain is typically negative already? Let's check
                # Actually P_Gamma is stored as positive (it's the absolute value of gamma)
                # The formula: put_gex = -gamma * OI * 100 (dealers short puts -> negative GEX)
                net_gex = c_gamma * c_oi * 100 - p_gamma * p_oi * 100

                if best_plus_gex is None or net_gex > best_plus_gex:
                    best_plus_gex = net_gex
                    best_plus_gex_strike = strike
                if best_minus_gex is None or net_gex < best_minus_gex:
                    best_minus_gex = net_gex
                    best_minus_gex_strike = strike

            results.append({
                'ts': ts,
                'spot': spot,
                'plus_gex_strike': best_plus_gex_strike,
                'plus_gex_value': best_plus_gex,
                'minus_gex_strike': best_minus_gex_strike,
                'minus_gex_value': best_minus_gex,
            })

    df = pd.DataFrame(results)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['trade_date'] = df['ts'].dt.tz_convert(NY).dt.date
    print(f"  Processed {processed} of {count} snapshots, got {len(df)} GEX computations")
    return df


# ============================================================
# STEP 1: Build unified signal dataset
# ============================================================

def build_signal_dataset(vol_df, gex_df, spot_df):
    """
    For each volland GEX snapshot, find the nearest chain GEX computation
    and the nearest spot price. Build a unified dataset with all force fields.
    """
    print("\nBuilding unified signal dataset...")

    # Merge volland with nearest chain GEX
    vol_df = vol_df.sort_values('ts').reset_index(drop=True)
    gex_df = gex_df.sort_values('ts').reset_index(drop=True)

    # Use merge_asof to find nearest chain GEX for each volland snapshot
    merged = pd.merge_asof(
        vol_df[['ts', 'trade_date', 'paradigm', 'lis_val', 'target_val', 'dd_val', 'charm_val']],
        gex_df[['ts', 'spot', 'plus_gex_strike', 'minus_gex_strike']],
        on='ts',
        tolerance=pd.Timedelta('10min'),
        direction='nearest'
    )

    # Drop rows where we couldn't find a nearby chain snapshot
    before = len(merged)
    merged = merged.dropna(subset=['spot', 'lis_val', 'target_val', 'plus_gex_strike', 'minus_gex_strike'])
    print(f"  Merged: {before} -> {len(merged)} rows after dropping NaN")

    # Compute force fields
    merged['lis_dist'] = merged['lis_val'] - merged['spot']  # positive = LIS above spot
    merged['target_dist'] = merged['target_val'] - merged['spot']  # positive = target above
    merged['plus_gex_dist'] = merged['plus_gex_strike'] - merged['spot']  # positive = +GEX above
    merged['minus_gex_dist'] = merged['minus_gex_strike'] - merged['spot']  # positive = -GEX above

    # Market hours filter (9:30-16:00 ET)
    merged['et_time'] = merged['ts'].dt.tz_convert(NY).dt.time
    merged = merged[(merged['et_time'] >= dtime(9, 30)) & (merged['et_time'] <= dtime(16, 0))]
    print(f"  After market hours filter: {len(merged)} rows")

    return merged


# ============================================================
# STEP 2: Forward price simulation
# ============================================================

def build_price_paths(spot_df, es_bars_df):
    """
    Build a unified price path per day from chain spots + ES bars.
    Returns dict: trade_date -> sorted list of (np.datetime64 UTC, price)
    """
    print("\nBuilding forward price paths...")
    paths = {}

    # Group chain spots by date -- convert to np.datetime64 for consistency
    for dt, grp in spot_df.groupby('trade_date'):
        pts = [(np.datetime64(t), p) for t, p in zip(grp['ts'].values, grp['spot'].values)]
        paths[dt] = pts

    # Add ES bars where available (higher resolution)
    # ES bars use SPX-relative prices but are ES futures -- there's a spread.
    # For forward sim we'll use them as-is since the spread is roughly constant intraday.
    if es_bars_df is not None and len(es_bars_df) > 0:
        for dt, grp in es_bars_df.groupby('trade_date'):
            es_pts = []
            for _, row in grp.iterrows():
                ts_val = row['ts']
                if hasattr(ts_val, 'tzinfo') and ts_val.tzinfo is None:
                    ts_val = pd.Timestamp(ts_val, tz='UTC')
                ts_np = ts_val.to_datetime64() if hasattr(ts_val, 'to_datetime64') else np.datetime64(ts_val)
                # For each bar, add high and low to catch stop/target hits
                es_pts.append((ts_np, row['bar_high_price']))
                es_pts.append((ts_np, row['bar_low_price']))

            if dt in paths:
                # Merge with chain spots, prefer ES bars for intrabar resolution
                combined = paths[dt] + es_pts
                combined.sort(key=lambda x: x[0])
                paths[dt] = combined
            else:
                es_pts.sort(key=lambda x: x[0])
                paths[dt] = es_pts

    print(f"  Built price paths for {len(paths)} days")
    return paths


def simulate_trade(entry_ts, entry_price, direction, price_path,
                   stop_pts, target_pts, max_minutes=120):
    """
    Simulate a trade using the price path.
    Returns: (outcome, pnl, mfe, mae, duration_minutes, exit_price)

    direction: 'LONG' or 'SHORT'
    """
    if direction != 'LONG':
        return ('SKIP', 0, 0, 0, 0, entry_price)

    entry_ts_np = np.datetime64(entry_ts) if not isinstance(entry_ts, np.datetime64) else entry_ts
    max_ts = entry_ts_np + np.timedelta64(max_minutes, 'm')

    # Filter price path to after entry
    future_prices = [(ts, p) for ts, p in price_path if ts > entry_ts_np and ts <= max_ts]

    if not future_prices:
        return ('NO_DATA', 0, 0, 0, 0, entry_price)

    stop_price = entry_price - stop_pts
    target_price = entry_price + target_pts

    mfe = 0  # max favorable excursion
    mae = 0  # max adverse excursion

    for ts, price in future_prices:
        pnl_at_price = price - entry_price
        mfe = max(mfe, pnl_at_price)
        mae = min(mae, pnl_at_price)

        # Check stop first (conservative)
        if price <= stop_price:
            duration = (ts - entry_ts_np) / np.timedelta64(1, 'm')
            return ('LOSS', -stop_pts, mfe, mae, duration, stop_price)

        # Check target
        if price >= target_price:
            duration = (ts - entry_ts_np) / np.timedelta64(1, 'm')
            return ('WIN', target_pts, mfe, mae, duration, target_price)

    # Expired -- close at last price
    last_ts, last_price = future_prices[-1]
    pnl = last_price - entry_price
    duration = (last_ts - entry_ts_np) / np.timedelta64(1, 'm')
    return ('EXPIRED', pnl, mfe, mae, duration, last_price)


# ============================================================
# STEP 3: Deduplication with cooldown
# ============================================================

def deduplicate_signals(signals_df, cooldown_minutes=30):
    """
    Apply cooldown: once a signal fires, skip next signals for cooldown_minutes.
    Returns deduped DataFrame.
    """
    signals_df = signals_df.sort_values('ts').reset_index(drop=True)
    kept = []
    last_fire = {}  # trade_date -> last fire timestamp

    for _, row in signals_df.iterrows():
        dt = row['trade_date']
        ts = row['ts']

        if dt in last_fire:
            elapsed = (ts - last_fire[dt]) / np.timedelta64(1, 'm')
            if elapsed < cooldown_minutes:
                continue

        kept.append(row)
        last_fire[dt] = ts

    return pd.DataFrame(kept).reset_index(drop=True)


# ============================================================
# STEP 4: Filter functions
# ============================================================

def filter_lis_magnet(df):
    """LIS Magnet: spot below LIS, within 5 pts (LIS pulls price up)."""
    return df[(df['lis_dist'] > 0) & (df['lis_dist'] <= 5)]

def filter_lis_support(df):
    """LIS Support: spot above LIS, within 5 pts (LIS supports from below)."""
    return df[(df['lis_dist'] < 0) & (df['lis_dist'] >= -5)]

def filter_lis_nearby(df, gap=5):
    """LIS nearby: |lis_dist| <= gap."""
    return df[df['lis_dist'].abs() <= gap]

def filter_full_force(df, require_minus_gex=False):
    """Full force alignment:
    - LIS within 5 pts
    - +GEX above spot with >=10 pts room
    - Target above spot with >=10 pts room
    - Optionally: -GEX within 10 pts
    """
    mask = (
        (df['lis_dist'].abs() <= 5) &
        (df['plus_gex_dist'] >= 10) &
        (df['target_dist'] >= 10)
    )
    if require_minus_gex:
        mask = mask & (df['minus_gex_dist'].abs() <= 10)
    return df[mask]

def compute_force_score(df):
    """Compute force score (0-5) for each signal."""
    score = pd.Series(0, index=df.index)

    # 1. LIS within 5 pts
    score += (df['lis_dist'].abs() <= 5).astype(int)

    # 2. -GEX within 15 pts
    score += (df['minus_gex_dist'].abs() <= 15).astype(int)

    # 3. +GEX above spot with >=10 pts room
    score += (df['plus_gex_dist'] >= 10).astype(int)

    # 4. Target above spot with >=15 pts room
    score += (df['target_dist'] >= 15).astype(int)

    # 5. All agree "up" -- all 4 above are true
    all_up = (
        (df['lis_dist'].abs() <= 5) &
        (df['minus_gex_dist'].abs() <= 15) &
        (df['plus_gex_dist'] >= 10) &
        (df['target_dist'] >= 15)
    )
    score += all_up.astype(int)

    return score


# ============================================================
# STEP 5: Results computation
# ============================================================

def compute_results(trades_df, label=""):
    """Compute and print performance metrics for a set of trades."""
    if len(trades_df) == 0:
        print(f"\n{'='*60}")
        print(f"  {label}: NO TRADES")
        print(f"{'='*60}")
        return None

    n = len(trades_df)
    wins = (trades_df['outcome'] == 'WIN').sum()
    losses = (trades_df['outcome'] == 'LOSS').sum()
    expired = (trades_df['outcome'] == 'EXPIRED').sum()
    no_data = (trades_df['outcome'] == 'NO_DATA').sum()

    total_pnl = trades_df['pnl'].sum()
    avg_pnl = trades_df['pnl'].mean()

    gross_wins = trades_df.loc[trades_df['pnl'] > 0, 'pnl'].sum()
    gross_losses = abs(trades_df.loc[trades_df['pnl'] < 0, 'pnl'].sum())
    pf = gross_wins / gross_losses if gross_losses > 0 else float('inf')

    wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    avg_mfe = trades_df['mfe'].mean()
    avg_mae = trades_df['mae'].mean()
    avg_dur = trades_df['duration'].mean()

    # Max drawdown (cumulative)
    cum_pnl = trades_df['pnl'].cumsum()
    peak = cum_pnl.cummax()
    dd = cum_pnl - peak
    max_dd = dd.min()

    result = {
        'label': label,
        'trades': n,
        'wins': wins,
        'losses': losses,
        'expired': expired,
        'wr': wr,
        'total_pnl': total_pnl,
        'avg_pnl': avg_pnl,
        'pf': pf,
        'avg_mfe': avg_mfe,
        'avg_mae': avg_mae,
        'avg_duration': avg_dur,
        'max_dd': max_dd,
    }

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"  Trades: {n} ({wins}W / {losses}L / {expired}E / {no_data}N)")
    print(f"  Win Rate: {wr:.1f}%")
    print(f"  Total PnL: {total_pnl:+.1f} pts")
    print(f"  Avg PnL: {avg_pnl:+.2f} pts/trade")
    print(f"  Profit Factor: {pf:.2f}")
    print(f"  Avg MFE: {avg_mfe:+.1f} pts  |  Avg MAE: {avg_mae:+.1f} pts")
    print(f"  Avg Duration: {avg_dur:.0f} min")
    print(f"  Max Drawdown: {max_dd:.1f} pts")

    return result


def run_sl_tp_matrix(signals_df, price_paths, label_prefix=""):
    """Run multiple SL/TP combos and print results table."""
    combos = [
        (8, 10), (10, 10), (12, 15), (15, 15), (15, 20), (20, 15)
    ]

    all_results = []
    for sl, tp in combos:
        trades = simulate_all(signals_df, price_paths, sl, tp)
        r = compute_results(trades, f"{label_prefix} SL={sl}/T={tp}")
        if r:
            r['sl'] = sl
            r['tp'] = tp
            all_results.append(r)

    return all_results


def simulate_all(signals_df, price_paths, stop_pts, target_pts, max_minutes=120):
    """Simulate all signals with given SL/TP."""
    results = []
    for _, sig in signals_df.iterrows():
        dt = sig['trade_date']
        if dt not in price_paths:
            results.append({
                'ts': sig['ts'], 'trade_date': dt, 'spot': sig['spot'],
                'lis_val': sig.get('lis_val'), 'target_val': sig.get('target_val'),
                'paradigm': sig.get('paradigm'),
                'outcome': 'NO_DATA', 'pnl': 0, 'mfe': 0, 'mae': 0,
                'duration': 0, 'exit_price': sig['spot'],
                'lis_dist': sig.get('lis_dist', 0), 'target_dist': sig.get('target_dist', 0),
                'plus_gex_dist': sig.get('plus_gex_dist', 0), 'minus_gex_dist': sig.get('minus_gex_dist', 0),
            })
            continue

        outcome, pnl, mfe, mae, dur, exit_p = simulate_trade(
            sig['ts'], sig['spot'], 'LONG', price_paths[dt],
            stop_pts, target_pts, max_minutes
        )
        results.append({
            'ts': sig['ts'], 'trade_date': dt, 'spot': sig['spot'],
            'lis_val': sig.get('lis_val'), 'target_val': sig.get('target_val'),
            'paradigm': sig.get('paradigm'),
            'outcome': outcome, 'pnl': pnl, 'mfe': mfe, 'mae': mae,
            'duration': dur, 'exit_price': exit_p,
            'lis_dist': sig.get('lis_dist', 0), 'target_dist': sig.get('target_dist', 0),
            'plus_gex_dist': sig.get('plus_gex_dist', 0), 'minus_gex_dist': sig.get('minus_gex_dist', 0),
        })

    return pd.DataFrame(results)


# ============================================================
# STEP 6: Optimal filter search
# ============================================================

def optimal_filter_search(all_signals, price_paths, min_trades=20):
    """Test combinations of filters and SL/TP to find optimal setup."""
    lis_gaps = [3, 5, 7, 10]
    pos_gex_mins = [5, 10, 15, 20]
    target_mins = [10, 15, 20]
    sl_tp_combos = [(8, 10), (10, 10), (12, 15), (15, 15), (15, 20), (20, 15)]

    results = []
    total = len(lis_gaps) * len(pos_gex_mins) * len(target_mins) * len(sl_tp_combos)
    print(f"\nOptimal filter search: {total} combinations...")

    count = 0
    for lis_gap, pgex_min, tgt_min, (sl, tp) in product(
        lis_gaps, pos_gex_mins, target_mins, sl_tp_combos
    ):
        count += 1

        # Apply filters
        filtered = all_signals[
            (all_signals['lis_dist'].abs() <= lis_gap) &
            (all_signals['plus_gex_dist'] >= pgex_min) &
            (all_signals['target_dist'] >= tgt_min)
        ]

        if len(filtered) < 5:  # Too few even before cooldown
            continue

        # Deduplicate
        deduped = deduplicate_signals(filtered, cooldown_minutes=30)

        if len(deduped) < min_trades:
            continue

        # Simulate
        trades = simulate_all(deduped, price_paths, sl, tp)
        valid_trades = trades[trades['outcome'] != 'NO_DATA']

        if len(valid_trades) < min_trades:
            continue

        wins = (valid_trades['outcome'] == 'WIN').sum()
        losses = (valid_trades['outcome'] == 'LOSS').sum()
        total_pnl = valid_trades['pnl'].sum()
        gross_w = valid_trades.loc[valid_trades['pnl'] > 0, 'pnl'].sum()
        gross_l = abs(valid_trades.loc[valid_trades['pnl'] < 0, 'pnl'].sum())
        pf = gross_w / gross_l if gross_l > 0 else 999
        wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0

        results.append({
            'lis_gap': lis_gap,
            'pgex_min': pgex_min,
            'tgt_min': tgt_min,
            'sl': sl,
            'tp': tp,
            'trades': len(valid_trades),
            'wins': wins,
            'losses': losses,
            'wr': wr,
            'total_pnl': total_pnl,
            'avg_pnl': total_pnl / len(valid_trades),
            'pf': pf,
        })

    if not results:
        print("  No combinations met minimum trade count!")
        return pd.DataFrame()

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('pf', ascending=False)

    print(f"\n{'='*80}")
    print(f"  TOP 10 OPTIMAL FILTER COMBINATIONS (min {min_trades} trades)")
    print(f"{'='*80}")
    print(f"  {'LIS':>4} {'PG>':>4} {'TG>':>4} {'SL':>3} {'TP':>3} {'#':>4} {'WR%':>6} {'PnL':>8} {'Avg':>7} {'PF':>6}")
    print(f"  {'-'*4} {'-'*4} {'-'*4} {'-'*3} {'-'*3} {'-'*4} {'-'*6} {'-'*8} {'-'*7} {'-'*6}")
    for _, r in results_df.head(10).iterrows():
        print(f"  {r['lis_gap']:4.0f} {r['pgex_min']:4.0f} {r['tgt_min']:4.0f} "
              f"{r['sl']:3.0f} {r['tp']:3.0f} {r['trades']:4.0f} "
              f"{r['wr']:5.1f}% {r['total_pnl']:+7.1f} {r['avg_pnl']:+6.2f} {r['pf']:5.2f}")

    return results_df


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("  GEX LONG FULL BACKTEST -- Reconstructed from Raw Data")
    print("=" * 70)

    # Load data
    vol_df = load_volland_gex()
    spot_df = load_chain_spot()
    es_bars = load_es_bars()
    gex_df = load_chain_gex(sample_every_n=5)  # Every 5th snapshot for better resolution

    # Build price paths for forward simulation
    price_paths = build_price_paths(spot_df, es_bars)

    # Build unified signal dataset
    all_signals = build_signal_dataset(vol_df, gex_df, spot_df)

    if len(all_signals) == 0:
        print("ERROR: No signals found after merging data sources!")
        return

    # Print raw signal stats
    print(f"\n{'='*60}")
    print(f"  RAW SIGNAL DATA SUMMARY")
    print(f"{'='*60}")
    print(f"  Total GEX snapshots (market hours): {len(all_signals)}")
    print(f"  Date range: {all_signals['trade_date'].min()} to {all_signals['trade_date'].max()}")
    print(f"  Days: {all_signals['trade_date'].nunique()}")
    print(f"  Paradigm: {all_signals['paradigm'].value_counts().to_dict()}")
    print(f"\n  Force field distributions:")
    for col in ['lis_dist', 'target_dist', 'plus_gex_dist', 'minus_gex_dist']:
        print(f"    {col:>18}: mean={all_signals[col].mean():+.1f}, "
              f"median={all_signals[col].median():+.1f}, "
              f"min={all_signals[col].min():+.1f}, max={all_signals[col].max():+.1f}")

    # ============================================================
    # STEP 3: Deduplicate ALL signals (any GEX paradigm, 30min cooldown)
    # ============================================================

    all_deduped = deduplicate_signals(all_signals, cooldown_minutes=30)
    print(f"\n  After 30-min cooldown dedup: {len(all_signals)} -> {len(all_deduped)} signals")
    print(f"  Per-day distribution:")
    day_counts = all_deduped.groupby('trade_date').size()
    print(f"    Mean: {day_counts.mean():.1f} signals/day, Max: {day_counts.max()}")

    # Compute force scores for all signals
    all_signals['force_score'] = compute_force_score(all_signals)
    all_deduped['force_score'] = compute_force_score(all_deduped)

    # ============================================================
    # TEST A: LIS Magnet standalone
    # ============================================================
    print("\n" + "#" * 70)
    print("  TEST A: LIS MAGNET (spot below LIS, within 5 pts)")
    print("#" * 70)

    lis_magnet = filter_lis_magnet(all_signals)
    lis_magnet_deduped = deduplicate_signals(lis_magnet, cooldown_minutes=30)
    print(f"  Signals: {len(lis_magnet)} raw -> {len(lis_magnet_deduped)} after cooldown")

    if len(lis_magnet_deduped) > 0:
        run_sl_tp_matrix(lis_magnet_deduped, price_paths, "LIS Magnet")

    # ============================================================
    # TEST B: LIS Support standalone
    # ============================================================
    print("\n" + "#" * 70)
    print("  TEST B: LIS SUPPORT (spot above LIS, within 5 pts)")
    print("#" * 70)

    lis_support = filter_lis_support(all_signals)
    lis_support_deduped = deduplicate_signals(lis_support, cooldown_minutes=30)
    print(f"  Signals: {len(lis_support)} raw -> {len(lis_support_deduped)} after cooldown")

    if len(lis_support_deduped) > 0:
        run_sl_tp_matrix(lis_support_deduped, price_paths, "LIS Support")

    # ============================================================
    # TEST C: Full force alignment
    # ============================================================
    print("\n" + "#" * 70)
    print("  TEST C: FULL FORCE ALIGNMENT")
    print("#" * 70)

    # Without -GEX requirement
    full_force = filter_full_force(all_signals, require_minus_gex=False)
    ff_deduped = deduplicate_signals(full_force, cooldown_minutes=30)
    print(f"  Without -GEX: {len(full_force)} raw -> {len(ff_deduped)} after cooldown")

    if len(ff_deduped) > 0:
        run_sl_tp_matrix(ff_deduped, price_paths, "Full Force (no -GEX)")

    # With -GEX requirement
    full_force_gex = filter_full_force(all_signals, require_minus_gex=True)
    ff_gex_deduped = deduplicate_signals(full_force_gex, cooldown_minutes=30)
    print(f"\n  With -GEX <=10: {len(full_force_gex)} raw -> {len(ff_gex_deduped)} after cooldown")

    if len(ff_gex_deduped) > 0:
        run_sl_tp_matrix(ff_gex_deduped, price_paths, "Full Force (+GEX)")

    # ============================================================
    # TEST D: Force score tiers
    # ============================================================
    print("\n" + "#" * 70)
    print("  TEST D: FORCE SCORE TIERS")
    print("#" * 70)

    print(f"\n  Force score distribution (all signals):")
    print(f"  {all_deduped['force_score'].value_counts().sort_index().to_dict()}")

    for score_min in range(0, 6):
        tier = all_deduped[all_deduped['force_score'] >= score_min]
        if len(tier) == 0:
            continue
        print(f"\n  --- Force Score >= {score_min}: {len(tier)} signals ---")
        trades = simulate_all(tier, price_paths, 10, 10)  # Default SL/TP
        compute_results(trades, f"Force >= {score_min} (SL=10/T=10)")

        # Also test best SL/TP combos for high-scoring tiers
        if score_min >= 3 and len(tier) >= 10:
            trades_wide = simulate_all(tier, price_paths, 15, 15)
            compute_results(trades_wide, f"Force >= {score_min} (SL=15/T=15)")

    # ============================================================
    # TEST E: Optimal filter search
    # ============================================================
    print("\n" + "#" * 70)
    print("  TEST E: OPTIMAL FILTER SEARCH")
    print("#" * 70)

    # Use min_trades=10 since we have limited data
    optimal_results = optimal_filter_search(all_signals, price_paths, min_trades=10)

    # Also try with lower min_trades to see if anything emerges
    if len(optimal_results) < 10:
        print("\n  Trying with min_trades=5...")
        optimal_results = optimal_filter_search(all_signals, price_paths, min_trades=5)

    # ============================================================
    # BASELINE: Any GEX paradigm, no filter
    # ============================================================
    print("\n" + "#" * 70)
    print("  BASELINE: ANY GEX PARADIGM, NO FILTER")
    print("#" * 70)

    run_sl_tp_matrix(all_deduped, price_paths, "Baseline GEX")

    # ============================================================
    # BONUS: Paradigm sub-types
    # ============================================================
    print("\n" + "#" * 70)
    print("  BONUS: PARADIGM SUB-TYPE PERFORMANCE")
    print("#" * 70)

    for paradigm in ['GEX-PURE', 'GEX-LIS', 'GEX-TARGET', 'GEX-MESSY']:
        sub = all_signals[all_signals['paradigm'] == paradigm]
        if len(sub) == 0:
            continue
        sub_deduped = deduplicate_signals(sub, cooldown_minutes=30)
        if len(sub_deduped) == 0:
            continue
        trades = simulate_all(sub_deduped, price_paths, 10, 10)
        compute_results(trades, f"{paradigm} (SL=10/T=10)")

    # ============================================================
    # BONUS: Additional targeted tests
    # ============================================================
    print("\n" + "#" * 70)
    print("  BONUS: TARGETED TESTS")
    print("#" * 70)

    # LIS below + target above (directional agreement)
    agree = all_signals[
        (all_signals['lis_dist'].abs() <= 10) &  # LIS nearby
        (all_signals['target_dist'] > 0)  # Target above
    ]
    agree_deduped = deduplicate_signals(agree, cooldown_minutes=30)
    if len(agree_deduped) > 0:
        trades = simulate_all(agree_deduped, price_paths, 10, 10)
        compute_results(trades, "LIS <=10 + Target above (SL=10/T=10)")

    # Spot below LIS + target above (strongest long signal)
    strong = all_signals[
        (all_signals['lis_dist'] > 0) &          # LIS above (magnet)
        (all_signals['lis_dist'] <= 10) &
        (all_signals['target_dist'] >= 10) &       # Target >=10 above
        (all_signals['plus_gex_dist'] > 0)         # +GEX above
    ]
    strong_deduped = deduplicate_signals(strong, cooldown_minutes=30)
    if len(strong_deduped) > 0:
        for sl, tp in [(8, 10), (10, 10), (15, 15)]:
            trades = simulate_all(strong_deduped, price_paths, sl, tp)
            compute_results(trades, f"Strong Long (LIS magnet + target + +GEX above) SL={sl}/T={tp}")

    # Charm positive + GEX (charm supports longs)
    charm_long = all_signals[
        (all_signals['charm_val'].notna()) &
        (all_signals['charm_val'] > 0) &
        (all_signals['lis_dist'].abs() <= 10)
    ]
    charm_deduped = deduplicate_signals(charm_long, cooldown_minutes=30)
    if len(charm_deduped) > 0:
        trades = simulate_all(charm_deduped, price_paths, 10, 10)
        compute_results(trades, "Charm Positive + LIS <=10 (SL=10/T=10)")

    # ============================================================
    # DETAILED TRADE LOG for best performing filter
    # ============================================================
    print("\n" + "#" * 70)
    print("  DETAILED TRADE LOG -- LIS Magnet (SL=10/T=10)")
    print("#" * 70)

    if len(lis_magnet_deduped) > 0:
        trades = simulate_all(lis_magnet_deduped, price_paths, 10, 10)
        valid = trades[trades['outcome'] != 'NO_DATA']
        print(f"\n  {'Date':>12} {'Time':>8} {'Spot':>8} {'LIS':>8} {'LIS_d':>6} "
              f"{'Tgt':>8} {'Tgt_d':>6} {'+GEX':>6} {'-GEX':>6} {'Out':>7} {'PnL':>7} "
              f"{'MFE':>6} {'MAE':>6} {'Dur':>5}")
        print(f"  {'-'*12} {'-'*8} {'-'*8} {'-'*8} {'-'*6} "
              f"{'-'*8} {'-'*6} {'-'*6} {'-'*6} {'-'*7} {'-'*7} "
              f"{'-'*6} {'-'*6} {'-'*5}")

        cum_pnl = 0
        for _, t in valid.iterrows():
            et = pd.Timestamp(t['ts']).tz_convert(NY)
            cum_pnl += t['pnl']
            print(f"  {str(t['trade_date']):>12} {et.strftime('%H:%M'):>8} "
                  f"{t['spot']:>8.0f} {t['lis_val']:>8.0f} {t['lis_dist']:>+6.1f} "
                  f"{t['target_val']:>8.0f} {t['target_dist']:>+6.1f} "
                  f"{t['plus_gex_dist']:>+6.1f} {t['minus_gex_dist']:>+6.1f} "
                  f"{t['outcome']:>7} {t['pnl']:>+7.1f} "
                  f"{t['mfe']:>+6.1f} {t['mae']:>+6.1f} {t['duration']:>5.0f}")
        print(f"\n  Cumulative PnL: {cum_pnl:+.1f} pts")

    # ============================================================
    # FINAL SUMMARY
    # ============================================================
    print("\n" + "=" * 70)
    print("  FINAL SUMMARY")
    print("=" * 70)

    # Compare all tests at SL=10/T=10
    summary_tests = [
        ("Baseline (all GEX)", all_deduped),
        ("LIS Magnet (<=5 below)", lis_magnet_deduped if len(lis_magnet_deduped) > 0 else pd.DataFrame()),
        ("LIS Support (<=5 above)", lis_support_deduped if len(lis_support_deduped) > 0 else pd.DataFrame()),
        ("Full Force (no -GEX)", ff_deduped if len(ff_deduped) > 0 else pd.DataFrame()),
        ("Full Force (+GEX)", ff_gex_deduped if len(ff_gex_deduped) > 0 else pd.DataFrame()),
    ]

    print(f"\n  {'Filter':>30} {'#':>4} {'WR%':>6} {'PnL':>8} {'Avg':>7} {'PF':>6}")
    print(f"  {'-'*30} {'-'*4} {'-'*6} {'-'*8} {'-'*7} {'-'*6}")

    for label, sigs in summary_tests:
        if len(sigs) == 0:
            print(f"  {label:>30} {'N/A':>4}")
            continue
        trades = simulate_all(sigs, price_paths, 10, 10)
        valid = trades[trades['outcome'] != 'NO_DATA']
        if len(valid) == 0:
            print(f"  {label:>30} {'0':>4}")
            continue

        w = (valid['outcome'] == 'WIN').sum()
        l = (valid['outcome'] == 'LOSS').sum()
        pnl = valid['pnl'].sum()
        gw = valid.loc[valid['pnl'] > 0, 'pnl'].sum()
        gl = abs(valid.loc[valid['pnl'] < 0, 'pnl'].sum())
        pf = gw / gl if gl > 0 else 999
        wr = w / (w + l) * 100 if (w + l) > 0 else 0

        print(f"  {label:>30} {len(valid):4d} {wr:5.1f}% {pnl:+7.1f} "
              f"{pnl/len(valid):+6.2f} {pf:5.2f}")

    # Force score summary
    print(f"\n  {'Force Score':>30} {'#':>4} {'WR%':>6} {'PnL':>8} {'Avg':>7} {'PF':>6}")
    print(f"  {'-'*30} {'-'*4} {'-'*6} {'-'*8} {'-'*7} {'-'*6}")

    for score_min in range(0, 6):
        tier = all_deduped[all_deduped['force_score'] >= score_min]
        if len(tier) == 0:
            continue
        trades = simulate_all(tier, price_paths, 10, 10)
        valid = trades[trades['outcome'] != 'NO_DATA']
        if len(valid) == 0:
            continue

        w = (valid['outcome'] == 'WIN').sum()
        l = (valid['outcome'] == 'LOSS').sum()
        pnl = valid['pnl'].sum()
        gw = valid.loc[valid['pnl'] > 0, 'pnl'].sum()
        gl = abs(valid.loc[valid['pnl'] < 0, 'pnl'].sum())
        pf = gw / gl if gl > 0 else 999
        wr = w / (w + l) * 100 if (w + l) > 0 else 0

        print(f"  {'Force >= ' + str(score_min):>30} {len(valid):4d} {wr:5.1f}% {pnl:+7.1f} "
              f"{pnl/len(valid):+6.2f} {pf:5.2f}")

    print("\n  Done. Research only -- no code or DB modified.")


if __name__ == "__main__":
    main()
