"""
GEX Long Force Alignment — SPX-ONLY + Greek alignment filter.
Pulls greek_alignment from setup_log for matching dates,
and also tests alignment from volland_snapshots directly.
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
               payload->'statistics'->>'target' as target,
               payload->'statistics'->>'aggregatedCharm' as charm,
               payload->'statistics'->>'spotVolBeta' as svb
        FROM volland_snapshots
        WHERE payload->'statistics'->>'paradigm' LIKE 'GEX%'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df['lis_val'] = df['lis'].apply(parse_dollar)
    df['target_val'] = df['target'].apply(parse_dollar)
    df['charm_val'] = df['charm'].apply(lambda x: float(x) if x and x not in ('None', '') else None)
    df['svb_val'] = df['svb'].apply(lambda x: float(x) if x and x not in ('None', '') else None)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['trade_date'] = df['ts'].dt.tz_convert(NY).dt.date
    return df


def load_vanna_data():
    """Load vanna exposure data for alignment calculation."""
    q = text("""
        SELECT vs.ts,
               vs.payload->'statistics'->>'spotVolBeta' as svb
        FROM volland_snapshots vs
        WHERE vs.payload->'statistics'->>'paradigm' LIKE 'GEX%'
        ORDER BY vs.ts
    """)
    # Also get vanna points for alignment
    q2 = text("""
        SELECT vs.ts, vep.exposure_type, vep.points
        FROM volland_snapshots vs
        JOIN volland_exposure_points vep ON vep.snapshot_id = vs.id
        WHERE vs.payload->'statistics'->>'paradigm' LIKE 'GEX%'
        AND vep.exposure_type IN ('vannaAllExpiries', 'vannaThisWeek', 'vannaThirtyNextDays')
        ORDER BY vs.ts
    """)
    with engine.connect() as conn:
        svb_df = pd.read_sql(q, conn)
        try:
            vanna_df = pd.read_sql(q2, conn)
        except:
            vanna_df = pd.DataFrame()
    return svb_df, vanna_df


def load_greek_alignment_from_setup_log():
    """Load greek_alignment values from actual GEX Long setup_log entries."""
    q = text("""
        SELECT ts, spot, greek_alignment, vanna_all, spot_vol_beta, grade,
               outcome_result, outcome_pnl
        FROM setup_log
        WHERE setup_name = 'GEX Long'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    df['trade_date'] = df['ts'].dt.tz_convert(NY).dt.date
    print(f"  setup_log GEX Long entries: {len(df)}")
    if len(df) > 0:
        print(f"  Alignment distribution: {df['greek_alignment'].value_counts().sort_index().to_dict()}")
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


def build_signals(vol_df, gex_df):
    vol_df = vol_df.sort_values('ts').reset_index(drop=True)
    gex_df = gex_df.sort_values('ts').reset_index(drop=True)
    merged = pd.merge_asof(
        vol_df[['ts', 'trade_date', 'paradigm', 'lis_val', 'target_val', 'charm_val', 'svb_val']],
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
                'mfe_time': 0, 'mae_time': 0, 'exit_time': 0, 'exit_price': entry_price}

    stop_price = entry_price - sl
    target_price = entry_price + tp
    mfe, mae, mfe_time, mae_time = 0, 0, 0, 0

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


def run_simulation(deduped, paths, sl, tp, label=""):
    trades = []
    cum_pnl = 0
    max_cum = 0
    max_dd = 0
    for _, sig in deduped.iterrows():
        dt = sig['trade_date']
        if dt not in paths:
            continue
        r = simulate_detailed(sig['ts'], sig['spot'], paths[dt], sl=sl, tp=tp)
        if r['outcome'] == 'NO_DATA':
            continue
        cum_pnl += r['pnl']
        max_cum = max(max_cum, cum_pnl)
        dd = cum_pnl - max_cum
        if dd < max_dd:
            max_dd = dd
        trades.append({**r, 'date': str(dt), 'time_et': sig['ts'].tz_convert(NY).strftime('%H:%M'),
                        'paradigm': sig['paradigm'], 'spot': sig['spot'],
                        'charm': sig.get('charm_val'), 'svb': sig.get('svb_val'),
                        'lis_dist': sig['lis_dist']})
    return trades, max_dd


def print_results(trades, max_dd, label):
    n = len(trades)
    if n == 0:
        print(f"\n  {label}: NO TRADES")
        return
    wins = sum(1 for t in trades if t['outcome'] == 'WIN')
    losses = sum(1 for t in trades if t['outcome'] == 'LOSS')
    expired = sum(1 for t in trades if t['outcome'] == 'EXPIRED')
    total_pnl = sum(t['pnl'] for t in trades)
    gross_wins = sum(t['pnl'] for t in trades if t['pnl'] > 0)
    gross_losses = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
    pf = gross_wins / gross_losses if gross_losses > 0 else float('inf')
    wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    avg_mfe = np.mean([t['mfe'] for t in trades])
    avg_mae = np.mean([t['mae'] for t in trades])

    print(f"\n  {label}")
    print(f"  Trades: {n} ({wins}W / {losses}L / {expired}E) | WR: {wr:.1f}% | "
          f"PnL: {total_pnl:+.1f} | PF: {pf:.2f} | MaxDD: {max_dd:.1f} | "
          f"AvgMFE: {avg_mfe:+.1f} | AvgMAE: {avg_mae:+.1f}")


def main():
    print("=" * 120)
    print("  GEX LONG — SPX-ONLY + GREEK FILTER ANALYSIS")
    print("=" * 120)

    print("\n1. Loading data...")
    vol_df = load_volland_gex()
    gex_df = load_chain_gex(sample_every_n=10)
    spot_df = load_chain_spot()
    setup_log = load_greek_alignment_from_setup_log()

    signals = build_signals(vol_df, gex_df)
    ff = filter_full_force(signals)
    deduped = deduplicate(ff, cooldown_min=30)
    # Filter out toxic paradigm subtypes
    before_pf = len(deduped)
    deduped = deduped[~deduped['paradigm'].str.upper().str.contains('TARGET|MESSY', na=False)].reset_index(drop=True)
    paths = build_spx_only_paths(spot_df)
    print(f"  {len(signals)} signals -> {len(ff)} full force -> {before_pf} cooldown -> {len(deduped)} after TARGET/MESSY filter")

    # ── Baseline ──
    print(f"\n{'='*120}")
    print("2. BASELINE (no Greek filter)")
    print(f"{'='*120}")

    for sl, tp in [(8, 10), (12, 15)]:
        trades, dd = run_simulation(deduped, paths, sl, tp)
        print_results(trades, dd, f"SL={sl}/T={tp}")

        # Print per-trade detail
        print(f"\n  {'#':>3} {'Date':>12} {'Time':>6} {'Paradigm':>14} {'Spot':>8} "
              f"{'Charm':>8} {'SVB':>6} {'Result':>6} {'PnL':>7} {'MFE':>7} {'MAE':>7}")
        print(f"  {'-'*100}")
        for i, t in enumerate(trades):
            charm_str = f"{t['charm']:+.0f}" if t['charm'] is not None else "N/A"
            svb_str = f"{t['svb']:.2f}" if t['svb'] is not None else "N/A"
            print(f"  {i+1:3d} {t['date']:>12} {t['time_et']:>6} {t['paradigm']:>14} {t['spot']:8.1f} "
                  f"{charm_str:>8} {svb_str:>6} {t['outcome']:>6} {t['pnl']:+7.1f} "
                  f"{t['mfe']:+7.1f} {t['mae']:+7.1f}")

    # ── Greek alignment from setup_log ──
    print(f"\n\n{'='*120}")
    print("3. SETUP_LOG ALIGNMENT BREAKDOWN (actual logged GEX Long trades)")
    print(f"{'='*120}")

    if len(setup_log) > 0:
        for align in sorted(setup_log['greek_alignment'].dropna().unique()):
            subset = setup_log[setup_log['greek_alignment'] == align]
            n = len(subset)
            resolved = subset[subset['outcome_result'].isin(['WIN', 'LOSS', 'EXPIRED'])]
            if len(resolved) > 0:
                wins = (resolved['outcome_result'] == 'WIN').sum()
                total = len(resolved)
                pnl = resolved['outcome_pnl'].sum()
                print(f"  Alignment {align:+d}: {n} signals, {total} resolved, "
                      f"{wins}W/{total-wins}L = {wins/total*100:.0f}% WR, PnL: {pnl:+.1f}")
            else:
                print(f"  Alignment {align:+d}: {n} signals (none resolved)")

    # ── Charm-based filter on our backtest signals ──
    print(f"\n\n{'='*120}")
    print("4. CHARM FILTER (from volland data at signal time)")
    print(f"{'='*120}")
    print("  GEX Long = LONG direction. Charm > 0 = bullish (supports longs).")
    print("  Filter: block when charm < 0 (opposes long)")

    for sl, tp in [(8, 10), (12, 15)]:
        # No filter
        trades_all, dd_all = run_simulation(deduped, paths, sl, tp)
        print_results(trades_all, dd_all, f"SL={sl}/T={tp} — ALL")

        # Charm > 0 only
        charm_positive = deduped[deduped['charm_val'] > 0]
        if len(charm_positive) > 0:
            trades_charm, dd_charm = run_simulation(charm_positive, paths, sl, tp)
            print_results(trades_charm, dd_charm, f"SL={sl}/T={tp} — CHARM > 0 only")

        # Charm < 0 only (what we'd block)
        charm_negative = deduped[deduped['charm_val'] < 0]
        if len(charm_negative) > 0:
            trades_blocked, dd_blocked = run_simulation(charm_negative, paths, sl, tp)
            print_results(trades_blocked, dd_blocked, f"SL={sl}/T={tp} — CHARM < 0 (blocked)")

        # Charm unknown
        charm_unknown = deduped[deduped['charm_val'].isna()]
        if len(charm_unknown) > 0:
            trades_unk, dd_unk = run_simulation(charm_unknown, paths, sl, tp)
            print_results(trades_unk, dd_unk, f"SL={sl}/T={tp} — CHARM unknown")

    # ── SVB filter ──
    print(f"\n\n{'='*120}")
    print("5. SVB (Spot-Vol-Beta) FILTER")
    print(f"{'='*120}")

    for sl, tp in [(8, 10), (12, 15)]:
        # SVB > 0 (positive correlation: vol rises with price)
        svb_positive = deduped[deduped['svb_val'] > 0]
        if len(svb_positive) > 0:
            trades_sp, dd_sp = run_simulation(svb_positive, paths, sl, tp)
            print_results(trades_sp, dd_sp, f"SL={sl}/T={tp} — SVB > 0")

        # SVB <= 0
        svb_negative = deduped[deduped['svb_val'] <= 0]
        if len(svb_negative) > 0:
            trades_sn, dd_sn = run_simulation(svb_negative, paths, sl, tp)
            print_results(trades_sn, dd_sn, f"SL={sl}/T={tp} — SVB <= 0")

    # ── Combined: charm > 0 AND svb > 0 ──
    print(f"\n\n{'='*120}")
    print("6. COMBINED FILTERS")
    print(f"{'='*120}")

    for sl, tp in [(8, 10), (12, 15)]:
        # Charm > 0 + SVB > 0
        combo1 = deduped[(deduped['charm_val'] > 0) & (deduped['svb_val'] > 0)]
        if len(combo1) > 0:
            trades_c1, dd_c1 = run_simulation(combo1, paths, sl, tp)
            print_results(trades_c1, dd_c1, f"SL={sl}/T={tp} — Charm>0 + SVB>0")

        # Charm > 0 OR unknown (pass-through)
        combo2 = deduped[(deduped['charm_val'] > 0) | (deduped['charm_val'].isna())]
        if len(combo2) > 0:
            trades_c2, dd_c2 = run_simulation(combo2, paths, sl, tp)
            print_results(trades_c2, dd_c2, f"SL={sl}/T={tp} — Charm>0 or unknown")

        # Paradigm subtype
        print(f"\n  Paradigm subtype breakdown (SL={sl}/T={tp}):")
        for ptype in deduped['paradigm'].unique():
            sub = deduped[deduped['paradigm'] == ptype]
            trades_p, dd_p = run_simulation(sub, paths, sl, tp)
            if trades_p:
                w = sum(1 for t in trades_p if t['outcome'] == 'WIN')
                l = sum(1 for t in trades_p if t['outcome'] == 'LOSS')
                e = sum(1 for t in trades_p if t['outcome'] == 'EXPIRED')
                pnl = sum(t['pnl'] for t in trades_p)
                wr = w/(w+l)*100 if (w+l) > 0 else 0
                print(f"    {ptype:>14}: {len(trades_p)} trades, {w}W/{l}L/{e}E = {wr:.0f}% WR, {pnl:+.1f} pts")


if __name__ == "__main__":
    main()
