"""
GEX Long Force Alignment — SPX-ONLY price path (no ES bars contamination).
Uses chain_snapshots SPX spot for BOTH entry and forward simulation.
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
    print(f"  Loaded {len(df)} chain spot observations, ~{df['ts'].diff().median().total_seconds():.0f}s interval")
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


def filter_full_force(df):
    return df[
        (df['lis_dist'].abs() <= 5) &
        (df['plus_gex_dist'] >= 10) &
        (df['target_dist'] >= 10)
    ]


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


def build_spx_only_paths(spot_df):
    """SPX-ONLY price paths from chain_snapshots. No ES bars."""
    paths = {}
    for dt, grp in spot_df.groupby('trade_date'):
        pts = [(np.datetime64(t), p) for t, p in zip(grp['ts'].values, grp['spot'].values)]
        paths[dt] = sorted(pts, key=lambda x: x[0])
    return paths


def simulate_detailed(entry_ts, entry_price, price_path, sl=12, tp=15, max_min=120):
    entry_ts_np = np.datetime64(entry_ts) if not isinstance(entry_ts, np.datetime64) else entry_ts
    max_ts = entry_ts_np + np.timedelta64(max_min, 'm')
    future = [(ts, p) for ts, p in price_path if ts > entry_ts_np and ts <= max_ts]
    if not future:
        return {'outcome': 'NO_DATA', 'pnl': 0, 'mfe': 0, 'mae': 0,
                'mfe_time': 0, 'mae_time': 0, 'exit_time': 0, 'exit_price': entry_price,
                'data_points': 0}

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
                    'exit_time': elapsed, 'exit_price': stop_price,
                    'data_points': len(future)}
        if price >= target_price:
            return {'outcome': 'WIN', 'pnl': tp, 'mfe': mfe, 'mae': mae,
                    'mfe_time': mfe_time, 'mae_time': mae_time,
                    'exit_time': elapsed, 'exit_price': target_price,
                    'data_points': len(future)}

    last_ts, last_price = future[-1]
    pnl = last_price - entry_price
    elapsed = (last_ts - entry_ts_np) / np.timedelta64(1, 'm')
    return {'outcome': 'EXPIRED', 'pnl': pnl, 'mfe': mfe, 'mae': mae,
            'mfe_time': mfe_time, 'mae_time': mae_time,
            'exit_time': elapsed, 'exit_price': last_price,
            'data_points': len(future)}


def main():
    print("=" * 120)
    print("  GEX LONG FORCE ALIGNMENT — SPX-ONLY (NO ES BARS)")
    print("  Using chain_snapshots SPX spot (~2min intervals) for price simulation")
    print("  This eliminates ES-SPX spread contamination")
    print("=" * 120)

    print("\nLoading data...")
    vol_df = load_volland_gex()
    gex_df = load_chain_gex(sample_every_n=10)
    spot_df = load_chain_spot()

    signals = build_signals(vol_df, gex_df)
    ff = filter_full_force(signals)
    deduped = deduplicate(ff, cooldown_min=30)
    # Filter out toxic paradigm subtypes
    before_filter = len(deduped)
    deduped = deduped[~deduped['paradigm'].str.upper().str.contains('TARGET|MESSY', na=False)].reset_index(drop=True)
    print(f"Signals: {len(signals)} total -> {len(ff)} full force -> {before_filter} after cooldown -> {len(deduped)} after TARGET/MESSY filter")

    paths = build_spx_only_paths(spot_df)

    SL, TP = 12, 15
    print(f"\n{'#':>3} {'Date':>12} {'Time':>6} {'Paradigm':>14} {'Spot':>8} {'LIS':>8} "
          f"{'Gap':>5} {'Result':>8} {'PnL':>7} {'MFE':>7} {'MFE@':>6} {'MAE':>7} {'MAE@':>6} "
          f"{'Exit@':>6} {'Pts':>4}")
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

        print(f"{i+1:3d} {str(dt):>12} {et_time:>6} {sig['paradigm']:>14} "
              f"{sig['spot']:8.1f} {sig['lis_val']:8.1f} "
              f"{sig['lis_dist']:+5.1f} "
              f"{result_marker:>8} {r['pnl']:+7.1f} "
              f"{r['mfe']:+7.1f} {r['mfe_time']:5.0f}m {r['mae']:+7.1f} {r['mae_time']:5.0f}m "
              f"{r['exit_time']:5.0f}m {r['data_points']:4d}")

        trades.append({**r, 'date': str(dt), 'paradigm': sig['paradigm'], 'spot': sig['spot']})

    print("-" * 120)
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

    print(f"\n  SUMMARY (SPX-ONLY, SL={SL}/T={TP})")
    print(f"  Trades: {n} ({wins}W / {losses}L / {expired}E)")
    print(f"  Win Rate: {wins / (wins + losses) * 100:.1f}%" if (wins + losses) > 0 else "  Win Rate: N/A")
    print(f"  Total PnL: {total_pnl:+.1f} pts")
    print(f"  Profit Factor: {pf:.2f}")
    print(f"  Avg MFE: {avg_mfe:+.1f} pts  |  Avg MAE: {avg_mae:+.1f} pts")
    print(f"  Avg Duration: {avg_dur:.0f} min")
    print(f"  Max System DD: {max_dd:.1f} pts")

    # Also run other SL/TP combos
    print(f"\n\n  SL/TP MATRIX (SPX-ONLY)")
    print(f"  {'SL':>4} {'TP':>4} {'Trades':>6} {'W':>3} {'L':>3} {'E':>3} {'WR%':>6} {'PnL':>8} {'PF':>6} {'AvgMFE':>7} {'AvgMAE':>7} {'MaxDD':>7}")
    print(f"  {'-'*4} {'-'*4} {'-'*6} {'-'*3} {'-'*3} {'-'*3} {'-'*6} {'-'*8} {'-'*6} {'-'*7} {'-'*7} {'-'*7}")

    for sl, tp in [(8, 10), (8, 15), (10, 10), (10, 15), (12, 10), (12, 15), (15, 15), (15, 20)]:
        t_list = []
        cp = 0
        mc = 0
        md = 0
        for _, sig in deduped.iterrows():
            dt = sig['trade_date']
            if dt not in paths:
                continue
            r = simulate_detailed(sig['ts'], sig['spot'], paths[dt], sl=sl, tp=tp)
            if r['outcome'] == 'NO_DATA':
                continue
            t_list.append(r)
            cp += r['pnl']
            mc = max(mc, cp)
            if cp - mc < md:
                md = cp - mc

        w = sum(1 for t in t_list if t['outcome'] == 'WIN')
        l = sum(1 for t in t_list if t['outcome'] == 'LOSS')
        e = sum(1 for t in t_list if t['outcome'] == 'EXPIRED')
        tp_sum = sum(t['pnl'] for t in t_list)
        gw = sum(t['pnl'] for t in t_list if t['pnl'] > 0)
        gl = abs(sum(t['pnl'] for t in t_list if t['pnl'] < 0))
        p = gw / gl if gl > 0 else float('inf')
        wr = w / (w + l) * 100 if (w + l) > 0 else 0
        am = np.mean([t['mfe'] for t in t_list]) if t_list else 0
        aa = np.mean([t['mae'] for t in t_list]) if t_list else 0
        print(f"  {sl:4d} {tp:4d} {len(t_list):6d} {w:3d} {l:3d} {e:3d} {wr:5.1f}% {tp_sum:+8.1f} {p:6.2f} {am:+7.1f} {aa:+7.1f} {md:+7.1f}")


if __name__ == "__main__":
    main()
