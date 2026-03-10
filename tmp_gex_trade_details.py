"""
GEX Long Force Alignment — Detailed Trade Report
Reruns the Full Force backtest (LIS<=5, +GEX>=10, Target>=10) with SL=12/T=15
and prints per-trade details: date, time, spot, levels, MDD, MFE, outcome, timing.
"""
import os
import numpy as np
import pandas as pd
from datetime import time as dtime
from sqlalchemy import create_engine, text
import pytz

NY = pytz.timezone("US/Eastern")
DB_URL = os.environ.get("DATABASE_URL", "")
engine = create_engine(DB_URL)


def parse_dollar(s):
    if not s or s in ('None', 'null', '', 'Terms Of Service', 'Undefined'):
        return None
    s = s.replace('$', '').replace(',', '').strip()
    try:
        return float(s)
    except:
        return None


# ── Load data ──

def load_volland_gex():
    q = text("""
        SELECT ts,
               payload->'statistics'->>'paradigm' as paradigm,
               payload->'statistics'->>'lines_in_sand' as lis,
               payload->'statistics'->>'target' as target
        FROM volland_snapshots
        WHERE payload->'statistics'->>'paradigm' LIKE 'GEX%'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df['lis_val'] = df['lis'].apply(parse_dollar)
    df['target_val'] = df['target'].apply(parse_dollar)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['trade_date'] = df['ts'].dt.tz_convert(NY).dt.date
    return df


def load_chain_spot():
    q = text("""
        SELECT ts, spot FROM chain_snapshots
        WHERE spot IS NOT NULL AND spot > 0
        AND ts >= '2026-01-21' AND ts <= '2026-03-08'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['trade_date'] = df['ts'].dt.tz_convert(NY).dt.date
    return df


def load_chain_gex(sample_every_n=10):
    q = text("""
        SELECT ts, spot, rows FROM chain_snapshots
        WHERE spot IS NOT NULL AND spot > 0
        AND ts >= '2026-01-21' AND ts <= '2026-03-08'
        ORDER BY ts
    """)
    results = []
    with engine.connect() as conn:
        cursor = conn.execute(q)
        count = 0
        for row in cursor:
            count += 1
            if count % sample_every_n != 0:
                continue
            ts, spot, chain_rows = row[0], row[1], row[2]
            if not chain_rows:
                continue
            best_plus_gex, best_plus_strike = None, None
            best_minus_gex, best_minus_strike = None, None
            for cr in chain_rows:
                strike = cr[10]
                c_gamma = cr[3] if cr[3] else 0
                c_oi = cr[1] if cr[1] else 0
                p_gamma = cr[17] if cr[17] else 0
                p_oi = cr[19] if cr[19] else 0
                net_gex = c_gamma * c_oi * 100 - p_gamma * p_oi * 100
                if best_plus_gex is None or net_gex > best_plus_gex:
                    best_plus_gex = net_gex
                    best_plus_strike = strike
                if best_minus_gex is None or net_gex < best_minus_gex:
                    best_minus_gex = net_gex
                    best_minus_strike = strike
            results.append({
                'ts': ts, 'spot': spot,
                'plus_gex_strike': best_plus_strike,
                'minus_gex_strike': best_minus_strike,
            })
    df = pd.DataFrame(results)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['trade_date'] = df['ts'].dt.tz_convert(NY).dt.date
    return df


def load_es_bars():
    q = text("""
        SELECT ts, trade_date, bar_high_price, bar_low_price
        FROM es_delta_bars ORDER BY ts
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    return df


# ── Build signal dataset ──

def build_signals(vol_df, gex_df):
    vol_df = vol_df.sort_values('ts').reset_index(drop=True)
    gex_df = gex_df.sort_values('ts').reset_index(drop=True)
    merged = pd.merge_asof(
        vol_df[['ts', 'trade_date', 'paradigm', 'lis_val', 'target_val']],
        gex_df[['ts', 'spot', 'plus_gex_strike', 'minus_gex_strike']],
        on='ts', tolerance=pd.Timedelta('10min'), direction='nearest'
    )
    merged = merged.dropna(subset=['spot', 'lis_val', 'target_val', 'plus_gex_strike', 'minus_gex_strike'])
    merged['lis_dist'] = merged['lis_val'] - merged['spot']
    merged['target_dist'] = merged['target_val'] - merged['spot']
    merged['plus_gex_dist'] = merged['plus_gex_strike'] - merged['spot']
    merged['minus_gex_dist'] = merged['minus_gex_strike'] - merged['spot']
    merged['et_time'] = merged['ts'].dt.tz_convert(NY).dt.time
    merged = merged[(merged['et_time'] >= dtime(9, 30)) & (merged['et_time'] <= dtime(16, 0))]
    return merged


# ── Full Force filter ──

def filter_full_force(df):
    return df[
        (df['lis_dist'].abs() <= 5) &
        (df['plus_gex_dist'] >= 10) &
        (df['target_dist'] >= 10)
    ]


# ── Cooldown dedup ──

def deduplicate(df, cooldown_min=30):
    df = df.sort_values('ts').reset_index(drop=True)
    kept = []
    last_fire = {}
    for _, row in df.iterrows():
        dt = row['trade_date']
        ts = row['ts']
        if dt in last_fire:
            elapsed = (ts - last_fire[dt]) / np.timedelta64(1, 'm')
            if elapsed < cooldown_min:
                continue
        kept.append(row)
        last_fire[dt] = ts
    return pd.DataFrame(kept).reset_index(drop=True)


# ── Price paths ──

def build_price_paths(spot_df, es_df):
    paths = {}
    for dt, grp in spot_df.groupby('trade_date'):
        pts = [(np.datetime64(t), p) for t, p in zip(grp['ts'].values, grp['spot'].values)]
        paths[dt] = pts
    if es_df is not None and len(es_df) > 0:
        for dt, grp in es_df.groupby('trade_date'):
            es_pts = []
            for _, row in grp.iterrows():
                ts_val = row['ts']
                if hasattr(ts_val, 'tzinfo') and ts_val.tzinfo is None:
                    ts_val = pd.Timestamp(ts_val, tz='UTC')
                ts_np = ts_val.to_datetime64() if hasattr(ts_val, 'to_datetime64') else np.datetime64(ts_val)
                es_pts.append((ts_np, row['bar_high_price']))
                es_pts.append((ts_np, row['bar_low_price']))
            if dt in paths:
                paths[dt] = sorted(paths[dt] + es_pts, key=lambda x: x[0])
            else:
                paths[dt] = sorted(es_pts, key=lambda x: x[0])
    return paths


# ── Simulate with detailed tracking ──

def simulate_detailed(entry_ts, entry_price, price_path, sl=12, tp=15, max_min=120):
    """Returns dict with outcome, pnl, mfe, mae, mdd_time, mfe_time, exit_time, exit_price."""
    entry_ts_np = np.datetime64(entry_ts) if not isinstance(entry_ts, np.datetime64) else entry_ts
    max_ts = entry_ts_np + np.timedelta64(max_min, 'm')
    future = [(ts, p) for ts, p in price_path if ts > entry_ts_np and ts <= max_ts]
    if not future:
        return {'outcome': 'NO_DATA', 'pnl': 0, 'mfe': 0, 'mae': 0,
                'mfe_time': 0, 'mae_time': 0, 'exit_time': 0, 'exit_price': entry_price}

    stop_price = entry_price - sl
    target_price = entry_price + tp
    mfe = 0
    mae = 0
    mfe_time = 0
    mae_time = 0

    for ts, price in future:
        pnl_now = price - entry_price
        elapsed = (ts - entry_ts_np) / np.timedelta64(1, 'm')
        if pnl_now > mfe:
            mfe = pnl_now
            mfe_time = elapsed
        if pnl_now < mae:
            mae = pnl_now
            mae_time = elapsed
        if price <= stop_price:
            return {'outcome': 'LOSS', 'pnl': -sl, 'mfe': mfe, 'mae': mae,
                    'mfe_time': mfe_time, 'mae_time': mae_time,
                    'exit_time': elapsed, 'exit_price': stop_price}
        if price >= target_price:
            return {'outcome': 'WIN', 'pnl': tp, 'mfe': mfe, 'mae': mae,
                    'mfe_time': mfe_time, 'mae_time': mae_time,
                    'exit_time': elapsed, 'exit_price': target_price}

    last_ts, last_price = future[-1]
    pnl = last_price - entry_price
    elapsed = (last_ts - entry_ts_np) / np.timedelta64(1, 'm')
    return {'outcome': 'EXPIRED', 'pnl': pnl, 'mfe': mfe, 'mae': mae,
            'mfe_time': mfe_time, 'mae_time': mae_time,
            'exit_time': elapsed, 'exit_price': last_price}


# ── Main ──

def main():
    print("Loading data...")
    vol_df = load_volland_gex()
    gex_df = load_chain_gex(sample_every_n=10)
    spot_df = load_chain_spot()
    es_df = load_es_bars()

    signals = build_signals(vol_df, gex_df)
    print(f"Total GEX signals during market hours: {len(signals)}")

    ff = filter_full_force(signals)
    print(f"After Full Force filter: {len(ff)}")

    deduped = deduplicate(ff, cooldown_min=30)
    print(f"After 30-min cooldown dedup: {len(deduped)}")

    paths = build_price_paths(spot_df, es_df)

    # Run simulation with SL=12, T=15 (deployed params)
    SL, TP = 12, 15
    print(f"\n{'='*120}")
    print(f"  GEX LONG FORCE ALIGNMENT — DETAILED TRADE LOG (SL={SL} / T={TP})")
    print(f"{'='*120}")
    print(f"{'#':>3} {'Date':>12} {'Time ET':>8} {'Paradigm':>14} {'Spot':>8} {'LIS':>8} "
          f"{'LIS gap':>7} {'+GEX':>6} {'-GEX':>6} {'Target':>8} "
          f"{'Result':>8} {'PnL':>7} {'MFE':>7} {'MFE@':>6} {'MAE':>7} {'MAE@':>6} "
          f"{'Exit@':>6} {'ExitPx':>8}")
    print("-" * 120)

    trades = []
    cum_pnl = 0
    max_cum = 0
    max_dd = 0

    for i, (_, sig) in enumerate(deduped.iterrows()):
        dt = sig['trade_date']
        if dt not in paths:
            continue

        r = simulate_detailed(sig['ts'], sig['spot'], paths[dt], sl=SL, tp=TP)
        if r['outcome'] == 'NO_DATA':
            continue

        cum_pnl += r['pnl']
        max_cum = max(max_cum, cum_pnl)
        dd = cum_pnl - max_cum
        if dd < max_dd:
            max_dd = dd

        et_time = sig['ts'].tz_convert(NY).strftime('%H:%M')
        result_marker = {'WIN': 'WIN', 'LOSS': 'LOSS', 'EXPIRED': 'EXP'}[r['outcome']]

        print(f"{i+1:3d} {str(dt):>12} {et_time:>8} {sig['paradigm']:>14} "
              f"{sig['spot']:8.1f} {sig['lis_val']:8.1f} "
              f"{sig['lis_dist']:+7.1f} {sig['plus_gex_strike']:6.0f} {sig['minus_gex_strike']:6.0f} "
              f"{sig['target_val']:8.1f} "
              f"{result_marker:>8} {r['pnl']:+7.1f} "
              f"{r['mfe']:+7.1f} {r['mfe_time']:5.0f}m {r['mae']:+7.1f} {r['mae_time']:5.0f}m "
              f"{r['exit_time']:5.0f}m {r['exit_price']:8.2f}")

        trades.append({
            'date': str(dt), 'time_et': et_time, 'paradigm': sig['paradigm'],
            'spot': sig['spot'], 'lis': sig['lis_val'],
            'lis_gap': sig['lis_dist'], 'plus_gex': sig['plus_gex_strike'],
            'minus_gex': sig['minus_gex_strike'], 'target': sig['target_val'],
            **r
        })

    print("-" * 120)

    # Summary
    n = len(trades)
    wins = sum(1 for t in trades if t['outcome'] == 'WIN')
    losses = sum(1 for t in trades if t['outcome'] == 'LOSS')
    expired = sum(1 for t in trades if t['outcome'] == 'EXPIRED')
    total_pnl = sum(t['pnl'] for t in trades)
    gross_wins = sum(t['pnl'] for t in trades if t['pnl'] > 0)
    gross_losses = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
    pf = gross_wins / gross_losses if gross_losses > 0 else float('inf')
    avg_mfe = np.mean([t['mfe'] for t in trades])
    avg_mae = np.mean([t['mae'] for t in trades])
    avg_dur = np.mean([t['exit_time'] for t in trades])

    print(f"\n  SUMMARY")
    print(f"  Trades: {n} ({wins}W / {losses}L / {expired}E)")
    print(f"  Win Rate: {wins/(wins+losses)*100:.1f}%")
    print(f"  Total PnL: {total_pnl:+.1f} pts")
    print(f"  Profit Factor: {pf:.2f}")
    print(f"  Avg MFE: {avg_mfe:+.1f} pts  |  Avg MAE: {avg_mae:+.1f} pts")
    print(f"  Avg Duration: {avg_dur:.0f} min")
    print(f"  Max System DD: {max_dd:.1f} pts")
    print(f"  Cum PnL: {cum_pnl:+.1f} pts")


if __name__ == "__main__":
    main()
