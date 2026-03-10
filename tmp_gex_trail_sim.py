"""
GEX Long Force Alignment — SPX-ONLY trailing stop simulation.
Compares fixed TP vs various trailing stop configurations.
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
    paths = {}
    for dt, grp in spot_df.groupby('trade_date'):
        pts = [(np.datetime64(t), p) for t, p in zip(grp['ts'].values, grp['spot'].values)]
        paths[dt] = sorted(pts, key=lambda x: x[0])
    return paths


def sim_fixed(entry_ts, entry_price, price_path, sl=8, tp=10, max_min=120):
    """Fixed SL/TP simulation."""
    entry_ts_np = np.datetime64(entry_ts) if not isinstance(entry_ts, np.datetime64) else entry_ts
    max_ts = entry_ts_np + np.timedelta64(max_min, 'm')
    future = [(ts, p) for ts, p in price_path if ts > entry_ts_np and ts <= max_ts]
    if not future:
        return {'outcome': 'NO_DATA', 'pnl': 0, 'mfe': 0, 'mae': 0, 'exit_time': 0}

    mfe, mae = 0, 0
    for ts, price in future:
        pnl_now = price - entry_price
        elapsed = (ts - entry_ts_np) / np.timedelta64(1, 'm')
        if pnl_now > mfe: mfe = pnl_now
        if pnl_now < mae: mae = pnl_now
        if price <= entry_price - sl:
            return {'outcome': 'LOSS', 'pnl': -sl, 'mfe': mfe, 'mae': mae, 'exit_time': elapsed}
        if price >= entry_price + tp:
            return {'outcome': 'WIN', 'pnl': tp, 'mfe': mfe, 'mae': mae, 'exit_time': elapsed}

    last_price = future[-1][1]
    elapsed = (future[-1][0] - entry_ts_np) / np.timedelta64(1, 'm')
    return {'outcome': 'EXPIRED', 'pnl': last_price - entry_price, 'mfe': mfe, 'mae': mae, 'exit_time': elapsed}


def sim_trail(entry_ts, entry_price, price_path, sl=8, be_trigger=None,
              trail_activation=None, trail_gap=5, max_min=120):
    """
    Trailing stop simulation.
    - sl: initial stop loss pts
    - be_trigger: move stop to breakeven when profit >= this (None = no BE)
    - trail_activation: start trailing when profit >= this (None = no trail)
    - trail_gap: trail distance behind max profit
    - max_min: max hold time
    """
    entry_ts_np = np.datetime64(entry_ts) if not isinstance(entry_ts, np.datetime64) else entry_ts
    max_ts = entry_ts_np + np.timedelta64(max_min, 'm')
    future = [(ts, p) for ts, p in price_path if ts > entry_ts_np and ts <= max_ts]
    if not future:
        return {'outcome': 'NO_DATA', 'pnl': 0, 'mfe': 0, 'mae': 0, 'exit_time': 0, 'trail_stop': None}

    stop_price = entry_price - sl
    max_profit = 0
    mfe, mae = 0, 0
    be_triggered = False
    trail_active = False

    for ts, price in future:
        pnl_now = price - entry_price
        elapsed = (ts - entry_ts_np) / np.timedelta64(1, 'm')
        if pnl_now > mfe: mfe = pnl_now
        if pnl_now < mae: mae = pnl_now

        if pnl_now > max_profit:
            max_profit = pnl_now

        # Breakeven trigger
        if be_trigger is not None and not be_triggered and max_profit >= be_trigger:
            stop_price = entry_price + 0.5  # BE + commissions
            be_triggered = True

        # Trail activation
        if trail_activation is not None and max_profit >= trail_activation:
            trail_stop = entry_price + max_profit - trail_gap
            if trail_stop > stop_price:
                stop_price = trail_stop
                trail_active = True

        # Check stop
        if price <= stop_price:
            pnl = stop_price - entry_price
            outcome = 'WIN' if pnl > 0 else 'LOSS' if pnl < 0 else 'BE'
            return {'outcome': outcome, 'pnl': pnl, 'mfe': mfe, 'mae': mae,
                    'exit_time': elapsed, 'trail_stop': stop_price - entry_price if trail_active else None}

    last_price = future[-1][1]
    elapsed = (future[-1][0] - entry_ts_np) / np.timedelta64(1, 'm')
    pnl = last_price - entry_price
    return {'outcome': 'EXPIRED', 'pnl': pnl, 'mfe': mfe, 'mae': mae,
            'exit_time': elapsed, 'trail_stop': stop_price - entry_price if trail_active else None}


def run_sim(deduped, paths, sim_fn, **kwargs):
    trades = []
    cum_pnl = 0
    max_cum = 0
    max_dd = 0
    for _, sig in deduped.iterrows():
        dt = sig['trade_date']
        if dt not in paths:
            continue
        r = sim_fn(sig['ts'], sig['spot'], paths[dt], **kwargs)
        if r['outcome'] == 'NO_DATA':
            continue
        cum_pnl += r['pnl']
        max_cum = max(max_cum, cum_pnl)
        dd = cum_pnl - max_cum
        if dd < max_dd:
            max_dd = dd
        trades.append({**r, 'date': str(dt), 'paradigm': sig['paradigm'],
                        'spot': sig['spot'], 'lis_dist': sig['lis_dist'],
                        'time_et': sig['ts'].tz_convert(NY).strftime('%H:%M'),
                        'cum_pnl': cum_pnl})
    return trades, max_dd


def print_summary(trades, max_dd, label):
    n = len(trades)
    if n == 0:
        print(f"  {label}: NO TRADES")
        return
    wins = sum(1 for t in trades if t['outcome'] == 'WIN')
    losses = sum(1 for t in trades if t['outcome'] == 'LOSS')
    be = sum(1 for t in trades if t['outcome'] == 'BE')
    expired = sum(1 for t in trades if t['outcome'] == 'EXPIRED')
    total_pnl = sum(t['pnl'] for t in trades)
    gross_wins = sum(t['pnl'] for t in trades if t['pnl'] > 0)
    gross_losses = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
    pf = gross_wins / gross_losses if gross_losses > 0 else float('inf')
    wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    avg_mfe = np.mean([t['mfe'] for t in trades])
    avg_pnl = total_pnl / n
    avg_dur = np.mean([t['exit_time'] for t in trades])

    print(f"  {label}")
    print(f"    {n} trades ({wins}W / {losses}L / {be}BE / {expired}E) | "
          f"WR: {wr:.0f}% | PnL: {total_pnl:+.1f} | PF: {pf:.2f} | "
          f"MaxDD: {max_dd:+.1f} | AvgPnL: {avg_pnl:+.1f} | AvgMFE: {avg_mfe:+.1f} | AvgDur: {avg_dur:.0f}m")


def print_trades(trades, label):
    print(f"\n  {label} — Trade-by-Trade:")
    print(f"  {'#':>2} {'Date':>12} {'Time':>5} {'Paradigm':>10} {'Spot':>8} {'Gap':>5} "
          f"{'Result':>6} {'PnL':>7} {'MFE':>7} {'MAE':>7} {'Exit@':>5} {'CumPnL':>7}")
    print(f"  {'-'*95}")
    for i, t in enumerate(trades):
        print(f"  {i+1:2d} {t['date']:>12} {t['time_et']:>5} {t['paradigm']:>10} "
              f"{t['spot']:8.1f} {t['lis_dist']:+5.1f} "
              f"{t['outcome']:>6} {t['pnl']:+7.1f} {t['mfe']:+7.1f} {t['mae']:+7.1f} "
              f"{t['exit_time']:4.0f}m {t['cum_pnl']:+7.1f}")


def main():
    print("=" * 110)
    print("  GEX LONG — TRAILING STOP vs FIXED TP COMPARISON")
    print("  SPX-only data, GEX-TARGET/MESSY filtered out")
    print("=" * 110)

    vol_df = load_volland_gex()
    gex_df = load_chain_gex(sample_every_n=10)
    spot_df = load_chain_spot()

    signals = build_signals(vol_df, gex_df)
    ff = filter_full_force(signals)
    deduped = deduplicate(ff, cooldown_min=30)
    deduped = deduped[~deduped['paradigm'].str.upper().str.contains('TARGET|MESSY', na=False)].reset_index(drop=True)
    paths = build_spx_only_paths(spot_df)
    print(f"  {len(deduped)} trades after all filters\n")

    # ── FIXED TP BASELINES ──
    print("=" * 110)
    print("  1. FIXED TP BASELINES")
    print("=" * 110)
    for sl, tp in [(8, 10), (8, 15), (12, 15)]:
        trades, dd = run_sim(deduped, paths, sim_fixed, sl=sl, tp=tp)
        print_summary(trades, dd, f"Fixed SL={sl} / TP={tp}")

    # ── TRAILING STOP CONFIGS ──
    print(f"\n{'='*110}")
    print("  2. TRAILING STOP CONFIGURATIONS")
    print("=" * 110)

    trail_configs = [
        # (label, sl, be_trigger, trail_activation, trail_gap, max_min)
        ("SL=8, BE@5, Trail@10/gap=5, 120m",       8,  5,  10, 5,  120),
        ("SL=8, BE@5, Trail@10/gap=3, 120m",       8,  5,  10, 3,  120),
        ("SL=8, BE@8, Trail@10/gap=5, 120m",       8,  8,  10, 5,  120),
        ("SL=8, BE@10, Trail@15/gap=5, 120m",      8,  10, 15, 5,  120),
        ("SL=8, no BE, Trail@10/gap=5, 120m",      8,  None, 10, 5, 120),
        ("SL=8, no BE, Trail@8/gap=5, 120m",       8,  None, 8,  5, 120),
        ("SL=8, BE@5, Trail@8/gap=3, 120m",        8,  5,  8,  3,  120),
        ("SL=8, BE@5, Trail@10/gap=5, 60m",        8,  5,  10, 5,  60),
        ("SL=8, BE@5, Trail@10/gap=5, 180m",       8,  5,  10, 5,  180),
        ("SL=12, BE@10, Trail@15/gap=5, 120m",     12, 10, 15, 5,  120),
        ("SL=12, BE@5, Trail@10/gap=5, 120m",      12, 5,  10, 5,  120),
        ("SL=12, BE@10, Trail@12/gap=5, 120m",     12, 10, 12, 5,  120),
        ("SL=12, no BE, Trail@15/gap=5, 120m",     12, None, 15, 5, 120),
        ("SL=12, BE@10, Trail@15/gap=8, 120m",     12, 10, 15, 8,  120),
        ("SL=8, BE@5, Trail@5/gap=3, 120m (tight)", 8, 5,  5,  3,  120),
        ("SL=8, BE@5, Trail@15/gap=5, 120m (wide)", 8, 5,  15, 5,  120),
    ]

    best_pnl = -999
    best_label = ""
    all_results = []

    for label, sl, be, act, gap, maxm in trail_configs:
        trades, dd = run_sim(deduped, paths, sim_trail,
                             sl=sl, be_trigger=be, trail_activation=act,
                             trail_gap=gap, max_min=maxm)
        print_summary(trades, dd, label)
        total_pnl = sum(t['pnl'] for t in trades)
        all_results.append((label, trades, dd, total_pnl))
        if total_pnl > best_pnl:
            best_pnl = total_pnl
            best_label = label

    # ── BEST TRAIL CONFIG DETAIL ──
    print(f"\n{'='*110}")
    print(f"  3. BEST TRAIL CONFIG: {best_label}")
    print("=" * 110)

    for label, trades, dd, pnl in all_results:
        if label == best_label:
            print_summary(trades, dd, label)
            print_trades(trades, label)
            break

    # ── COMPARISON: best fixed vs best trail ──
    print(f"\n{'='*110}")
    print("  4. HEAD-TO-HEAD: BEST FIXED vs BEST TRAIL")
    print("=" * 110)

    trades_fixed, dd_fixed = run_sim(deduped, paths, sim_fixed, sl=8, tp=10)
    print_summary(trades_fixed, dd_fixed, "FIXED: SL=8 / TP=10")
    print_trades(trades_fixed, "FIXED SL=8/TP=10")

    # Find best trail and print detail
    for label, trades, dd, pnl in all_results:
        if label == best_label:
            print()
            print_summary(trades, dd, f"TRAIL: {label}")
            print_trades(trades, f"TRAIL: {label}")
            break

    # ── Also show deployed config (SL=12, BE@10, act=15, gap=5) ──
    print(f"\n{'='*110}")
    print("  5. DEPLOYED CONFIG: SL=12, BE@10, Trail@15/gap=5")
    print("=" * 110)
    trades_dep, dd_dep = run_sim(deduped, paths, sim_trail,
                                  sl=12, be_trigger=10, trail_activation=15,
                                  trail_gap=5, max_min=120)
    print_summary(trades_dep, dd_dep, "DEPLOYED: SL=12, BE@10, Trail@15/gap=5")
    print_trades(trades_dep, "DEPLOYED")

    # ── TOP 5 ranked by PnL ──
    print(f"\n{'='*110}")
    print("  6. TOP 5 CONFIGS BY PnL")
    print("=" * 110)
    all_results.sort(key=lambda x: x[3], reverse=True)
    for i, (label, trades, dd, pnl) in enumerate(all_results[:5]):
        print(f"  #{i+1}: {pnl:+.1f} pts — {label}")
        n = len(trades)
        w = sum(1 for t in trades if t['outcome'] == 'WIN')
        l = sum(1 for t in trades if t['outcome'] == 'LOSS')
        wr = w/(w+l)*100 if (w+l) > 0 else 0
        print(f"       {n} trades, {w}W/{l}L, WR={wr:.0f}%, MaxDD={dd:+.1f}")


if __name__ == "__main__":
    main()
