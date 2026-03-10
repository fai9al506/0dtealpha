"""
GEX Long Force Alignment Analysis
Research only - does NOT modify any code or database
"""
import sqlalchemy as sa, pandas as pd, numpy as np, json, os, sys

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = sa.create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})


def parse_dollar(s):
    if s is None:
        return np.nan
    return float(str(s).replace('$', '').replace(',', ''))


def compute_gex_from_chain(chain_ids):
    """Compute +GEX/-GEX strikes from chain_snapshots rows."""
    results = []
    batch_size = 100
    for i in range(0, len(chain_ids), batch_size):
        batch = chain_ids[i:i + batch_size]
        if not batch:
            break
        id_list = ','.join(str(int(x)) for x in batch)
        df_batch = pd.read_sql(
            f"SELECT id, columns, rows FROM chain_snapshots WHERE id IN ({id_list})", engine
        )
        for _, row in df_batch.iterrows():
            cols = row['columns']
            rows_data = row['rows']
            chain_id = row['id']
            if rows_data is None or cols is None:
                continue
            if isinstance(rows_data, str):
                rows_data = json.loads(rows_data)
            strike_idx = cols.index('Strike')
            max_plus, max_minus, max_gex, min_gex = None, None, -1e20, 1e20
            for r in rows_data:
                try:
                    strike = float(r[strike_idx])
                    cg = float(r[3]) if r[3] else 0
                    co = float(r[1]) if r[1] else 0
                    pg = float(r[strike_idx + 7]) if r[strike_idx + 7] else 0
                    po = float(r[strike_idx + 9]) if r[strike_idx + 9] else 0
                    net_gex = (cg * co * 100) + (-pg * po * 100)
                    if net_gex > max_gex:
                        max_gex, max_plus = net_gex, strike
                    if net_gex < min_gex:
                        min_gex, max_minus = net_gex, strike
                except (ValueError, TypeError, IndexError):
                    continue
            results.append({
                'chain_id': chain_id,
                'max_plus_gex': max_plus,
                'max_minus_gex': max_minus,
            })
        print(f"  Processed {min(i + batch_size, len(chain_ids))}/{len(chain_ids)} chain snapshots")
    return pd.DataFrame(results)


def compute_force_score(df):
    """Add force score columns to dataframe with spot, lis, target, max_plus_gex, max_minus_gex."""
    df = df.copy()
    df['lis_dist'] = df['lis'] - df['spot']
    df['neg_gex_dist'] = df['max_minus_gex'] - df['spot']
    df['pos_gex_dist'] = df['max_plus_gex'] - df['spot']
    df['target_dist'] = df['target'] - df['spot']

    df['f1_lis_nearby'] = (df['lis_dist'].abs() <= 5).astype(int)
    df['f2_neg_gex_above'] = (df['neg_gex_dist'] > 0).astype(int)
    df['f3_pos_gex_above'] = (df['pos_gex_dist'] >= 10).astype(int)
    df['f4_target_above'] = (df['target_dist'] >= 10).astype(int)
    df['f5_sandwich'] = (
        (df['lis_dist'] <= 0) & (df['lis_dist'] >= -5) & (df['neg_gex_dist'] > 0)
    ).astype(int)
    df['f6_double_magnet'] = (
        (df['lis_dist'] > 0) & (df['lis_dist'] <= 5) & (df['pos_gex_dist'] >= 10)
    ).astype(int)
    df['force_score'] = (
        df['f1_lis_nearby'] + df['f2_neg_gex_above'] + df['f3_pos_gex_above'] +
        df['f4_target_above'] + df['f5_sandwich'] + df['f6_double_magnet']
    )
    return df


def part1():
    """Score existing 42 GEX Long trades."""
    print("=" * 80)
    print("PART 1: GEX LONG FORCE ALIGNMENT - 42 LOGGED TRADES")
    print("=" * 80)

    df = pd.read_sql("""
        SELECT id, ts, spot, lis, target, max_plus_gex, max_minus_gex, gap_to_lis,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               greek_alignment, grade, score, first_hour
        FROM setup_log WHERE setup_name = 'GEX Long' ORDER BY ts
    """, engine)

    df = compute_force_score(df)
    df['is_win'] = (df['outcome_result'] == 'WIN').astype(int)

    print(f"\nTotal trades: {len(df)}")

    # Performance by force_score
    print("\n--- Performance by Force Score ---")
    for fs in sorted(df['force_score'].unique()):
        sub = df[df['force_score'] == fs]
        n = len(sub)
        wr = sub['is_win'].mean() * 100
        pnl = sub['outcome_pnl'].sum()
        gw = sub.loc[sub['outcome_pnl'] > 0, 'outcome_pnl'].sum()
        gl = abs(sub.loc[sub['outcome_pnl'] < 0, 'outcome_pnl'].sum())
        pf = gw / gl if gl > 0 else float('inf')
        print(f"  Force={fs}: N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+7.1f}, PF={pf:5.2f}")

    # Cross with greek_alignment
    print("\n--- Force Score x Greek Alignment ---")
    for fs in sorted(df['force_score'].unique()):
        for ga in sorted(df['greek_alignment'].dropna().unique()):
            sub = df[(df['force_score'] == fs) & (df['greek_alignment'] == ga)]
            if len(sub) == 0:
                continue
            n = len(sub)
            wr = sub['is_win'].mean() * 100
            pnl = sub['outcome_pnl'].sum()
            print(f"  Force={fs}, Align={int(ga):+d}: N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+7.1f}")

    # Component analysis
    print("\n--- Force Component Summary ---")
    for col in ['f1_lis_nearby', 'f2_neg_gex_above', 'f3_pos_gex_above',
                'f4_target_above', 'f5_sandwich', 'f6_double_magnet']:
        on = df[df[col] == 1]
        off = df[df[col] == 0]
        n1, n0 = len(on), len(off)
        wr1 = on['is_win'].mean() * 100 if n1 > 0 else 0
        wr0 = off['is_win'].mean() * 100 if n0 > 0 else 0
        pnl1 = on['outcome_pnl'].sum()
        pnl0 = off['outcome_pnl'].sum()
        print(f"  {col:20s}: ON={n1:2d} WR={wr1:5.1f}% PnL={pnl1:+7.1f} | OFF={n0:2d} WR={wr0:5.1f}% PnL={pnl0:+7.1f}")

    # Detailed component buckets
    print("\n--- LIS Distance Buckets ---")
    bins = [(-100, -20), (-20, -10), (-10, -5), (-5, -3), (-3, 0), (0, 5)]
    for lo, hi in bins:
        sub = df[(df['lis_dist'] >= lo) & (df['lis_dist'] < hi)]
        n = len(sub)
        if n == 0:
            continue
        wr = sub['is_win'].mean() * 100
        pnl = sub['outcome_pnl'].sum()
        print(f"  [{lo:+4d} to {hi:+4d}): N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+7.1f}")

    print("\n--- -GEX Distance Buckets ---")
    bins = [(-80, -50), (-50, -20), (-20, -10), (-10, 0), (0, 15)]
    for lo, hi in bins:
        sub = df[(df['neg_gex_dist'] >= lo) & (df['neg_gex_dist'] < hi)]
        n = len(sub)
        if n == 0:
            continue
        wr = sub['is_win'].mean() * 100
        pnl = sub['outcome_pnl'].sum()
        print(f"  [{lo:+4d} to {hi:+4d}): N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+7.1f}")

    print("\n--- Target Distance Buckets ---")
    bins = [(10, 20), (20, 30), (30, 50), (50, 130)]
    for lo, hi in bins:
        sub = df[(df['target_dist'] >= lo) & (df['target_dist'] < hi)]
        n = len(sub)
        if n == 0:
            continue
        wr = sub['is_win'].mean() * 100
        pnl = sub['outcome_pnl'].sum()
        print(f"  [{lo:+4d} to {hi:+4d}): N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+7.1f}")

    # Best combos
    print("\n--- Best 2-Factor Combos ---")
    combos = [
        ("LIS<=5 + Align>=1", (df['lis_dist'].abs() <= 5) & (df['greek_alignment'] >= 1)),
        ("LIS<=3 + Align>=1", (df['lis_dist'].abs() <= 3) & (df['greek_alignment'] >= 1)),
        ("+GEX<50 + Align>=1", (df['pos_gex_dist'] < 50) & (df['greek_alignment'] >= 1)),
        ("Target<30 + Align>=1", (df['target_dist'] < 30) & (df['greek_alignment'] >= 1)),
        ("Target<50 + Align>=1", (df['target_dist'] < 50) & (df['greek_alignment'] >= 1)),
        ("gap<=5 + Align>=1", (df['gap_to_lis'] <= 5) & (df['greek_alignment'] >= 1)),
        ("LIS<=5 + -GEX>-15", (df['lis_dist'].abs() <= 5) & (df['neg_gex_dist'] >= -15)),
    ]
    for name, mask in combos:
        sub = df[mask]
        n = len(sub)
        if n == 0:
            continue
        wr = sub['is_win'].mean() * 100
        pnl = sub['outcome_pnl'].sum()
        gw = sub.loc[sub['outcome_pnl'] > 0, 'outcome_pnl'].sum()
        gl = abs(sub.loc[sub['outcome_pnl'] < 0, 'outcome_pnl'].sum())
        pf = gw / gl if gl > 0 else float('inf')
        print(f"  {name:30s}: N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+7.1f}, PF={pf:.2f}")

    print("\n--- Best 3-Factor Combos ---")
    combos3 = [
        ("LIS<=5 + Align>=1 + Target<30",
         (df['lis_dist'].abs() <= 5) & (df['greek_alignment'] >= 1) & (df['target_dist'] < 30)),
        ("LIS<=5 + Align>=1 + Target<50",
         (df['lis_dist'].abs() <= 5) & (df['greek_alignment'] >= 1) & (df['target_dist'] < 50)),
        ("LIS<=5 + Align>=1 + -GEX>-15",
         (df['lis_dist'].abs() <= 5) & (df['greek_alignment'] >= 1) & (df['neg_gex_dist'] >= -15)),
        ("+GEX<50 + Align>=1 + Target<30",
         (df['pos_gex_dist'] < 50) & (df['greek_alignment'] >= 1) & (df['target_dist'] < 30)),
        ("gap<=5 + Align>=1 + Target<30",
         (df['gap_to_lis'] <= 5) & (df['greek_alignment'] >= 1) & (df['target_dist'] < 30)),
        ("LIS<=5 + Align>=1 + +GEX<50",
         (df['lis_dist'].abs() <= 5) & (df['greek_alignment'] >= 1) & (df['pos_gex_dist'] < 50)),
    ]
    for name, mask in combos3:
        sub = df[mask]
        n = len(sub)
        if n == 0:
            continue
        wr = sub['is_win'].mean() * 100
        pnl = sub['outcome_pnl'].sum()
        gw = sub.loc[sub['outcome_pnl'] > 0, 'outcome_pnl'].sum()
        gl = abs(sub.loc[sub['outcome_pnl'] < 0, 'outcome_pnl'].sum())
        pf = gw / gl if gl > 0 else float('inf')
        print(f"  {name:40s}: N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+7.1f}, PF={pf:.2f}")

    return df


def part2():
    """Reconstruct ALL GEX paradigm moments with forward price action."""
    print("\n" + "=" * 80)
    print("PART 2: ALL GEX PARADIGM MOMENTS - FORWARD PRICE ACTION")
    print("=" * 80)

    # Pull GEX paradigm snapshots
    volland = pd.read_sql("""
        SELECT ts,
               payload->'statistics'->>'paradigm' as paradigm,
               payload->'statistics'->>'lines_in_sand' as lis_raw,
               payload->'statistics'->>'target' as target_raw
        FROM volland_snapshots
        WHERE payload->'statistics'->>'paradigm' ILIKE '%%GEX%%'
          AND payload->>'error_event' IS NULL
        ORDER BY ts
    """, engine)
    volland['ts'] = pd.to_datetime(volland['ts'], utc=True)
    volland['lis'] = volland['lis_raw'].apply(parse_dollar)
    volland['target'] = volland['target_raw'].apply(parse_dollar)
    volland['window'] = volland['ts'].dt.floor('2min')
    volland_dedup = volland.drop_duplicates(subset='window', keep='first').copy()
    print(f"GEX paradigm: {len(volland)} -> {len(volland_dedup)} deduped")

    # Match with chain snapshots for spot + GEX computation
    chain_ts = pd.read_sql(
        "SELECT id, ts, spot FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY ts", engine
    )
    chain_ts['ts'] = pd.to_datetime(chain_ts['ts'], utc=True)
    volland_dedup = volland_dedup.sort_values('ts')
    chain_ts = chain_ts.sort_values('ts')
    merged = pd.merge_asof(
        volland_dedup, chain_ts, on='ts', direction='nearest',
        tolerance=pd.Timedelta('3min'), suffixes=('', '_chain')
    )
    merged = merged.dropna(subset=['spot'])

    # Compute GEX
    chain_ids = merged['id'].unique().tolist()
    print(f"Computing GEX for {len(chain_ids)} chain snapshots...")
    gex_df = compute_gex_from_chain(chain_ids)
    m = merged.merge(gex_df, left_on='id', right_on='chain_id', how='left')
    m = m.dropna(subset=['max_plus_gex', 'max_minus_gex', 'lis', 'target'])
    m = compute_force_score(m)
    print(f"Final: {len(m)} GEX moments")

    # Forward price action using ES 1-min bars
    print("\nPulling ES 1-min bars...")
    es_bars = pd.read_sql("""
        SELECT ts, bar_close_price as price, bar_high_price as high, bar_low_price as low
        FROM es_delta_bars ORDER BY ts
    """, engine)
    es_bars['ts'] = pd.to_datetime(es_bars['ts'], utc=True)
    print(f"  {len(es_bars)} ES 1-min bars")

    es_min_ts = es_bars['ts'].min()
    es_max_ts = es_bars['ts'].max()
    m_fwd = m[(m['ts'] >= es_min_ts) & (m['ts'] <= es_max_ts - pd.Timedelta('30min'))].copy()
    print(f"  {len(m_fwd)} GEX moments within ES bar range")

    if len(m_fwd) == 0:
        print("  No overlap - skipping forward analysis")
        return m

    # Forward MFE/MAE
    es_prices = es_bars[['ts', 'price', 'high', 'low']].values
    es_ts_arr = es_bars['ts'].values

    fwd_data = {w: {'mfe': [], 'mae': [], 'final': [], 'idx': []} for w in [30, 60, 120]}

    for idx, row in m_fwd.iterrows():
        entry_ts = row['ts']
        entry_ts_np = np.datetime64(entry_ts)

        mask_after = es_ts_arr > entry_ts_np
        if not mask_after.any():
            continue

        first_after_idx = np.argmax(mask_after)
        first_es_price = es_prices[first_after_idx, 1]  # close

        for w in [30, 60, 120]:
            end_ts_np = entry_ts_np + np.timedelta64(w, 'm')
            mask_window = mask_after & (es_ts_arr <= end_ts_np)
            window_indices = np.where(mask_window)[0]
            if len(window_indices) == 0:
                continue
            highs = es_prices[window_indices, 2]
            lows = es_prices[window_indices, 3]
            last_close = es_prices[window_indices[-1], 1]

            mfe = highs.max() - first_es_price
            mae = lows.min() - first_es_price
            final = last_close - first_es_price

            fwd_data[w]['mfe'].append(mfe)
            fwd_data[w]['mae'].append(mae)
            fwd_data[w]['final'].append(final)
            fwd_data[w]['idx'].append(idx)

    for w in [30, 60, 120]:
        if not fwd_data[w]['idx']:
            continue
        fwd_df = pd.DataFrame(fwd_data[w])
        mfw = m_fwd.loc[fwd_df['idx']].copy()
        mfw['mfe'] = fwd_df['mfe'].values
        mfw['mae'] = fwd_df['mae'].values
        mfw['final'] = fwd_df['final'].values

        print(f"\n--- Forward {w}min by Force Score ---")
        for fs in sorted(mfw['force_score'].unique()):
            sub = mfw[mfw['force_score'] == fs]
            n = len(sub)
            avg_mfe = sub['mfe'].mean()
            avg_mae = sub['mae'].mean()
            avg_final = sub['final'].mean()

            # Simulate SL/T combos for LONG
            for sl, t in [(8, 10), (10, 15), (15, 20)]:
                wins = losses = flat = 0
                for _, sr in sub.iterrows():
                    if sr['mae'] <= -sl:
                        losses += 1
                    elif sr['mfe'] >= t:
                        wins += 1
                    else:
                        flat += 1
                total = wins + losses + flat
                wr = wins / total * 100 if total > 0 else 0
                pnl = wins * t - losses * sl
                if sl == 8 and t == 10:
                    print(f"  Force={fs}: N={n:4d}, MFE={avg_mfe:+5.1f}, MAE={avg_mae:+5.1f}, "
                          f"Final={avg_final:+5.1f} | SL{sl}/T{t}: W={wins} L={losses} F={flat} "
                          f"WR={wr:.0f}% PnL={pnl:+d}")

    # Force distribution
    print("\n--- Force Score Distribution (all GEX moments) ---")
    for fs in sorted(m['force_score'].unique()):
        n = (m['force_score'] == fs).sum()
        pct = n / len(m) * 100
        print(f"  Force={fs}: {n:5d} ({pct:5.1f}%)")

    print("\n--- Component Frequency ---")
    for col in ['f1_lis_nearby', 'f2_neg_gex_above', 'f3_pos_gex_above',
                'f4_target_above', 'f5_sandwich', 'f6_double_magnet']:
        on = (m[col] == 1).sum()
        print(f"  {col:20s}: {on:5d} / {len(m)} ({on/len(m)*100:5.1f}%)")

    return m


def part3(m_fwd_with_es=None):
    """Test entry criteria combos systematically on logged trades."""
    print("\n" + "=" * 80)
    print("PART 3: SYSTEMATIC COMBO TESTING ON 42 LOGGED TRADES")
    print("=" * 80)

    df = pd.read_sql("""
        SELECT id, ts, spot, lis, target, max_plus_gex, max_minus_gex, gap_to_lis,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               greek_alignment, grade, score, first_hour
        FROM setup_log WHERE setup_name = 'GEX Long' ORDER BY ts
    """, engine)
    df = compute_force_score(df)
    df['is_win'] = (df['outcome_result'] == 'WIN').astype(int)

    # Test all combos
    results = []
    force_thresholds = [0, 2, 3, 4, 5]
    align_options = [None, 1, 3]
    lis_gap_options = [None, 3, 5, 7, 10]

    # Also test specific component filters
    component_filters = {
        'none': pd.Series(True, index=df.index),
        'lis_nearby': df['f1_lis_nearby'] == 1,
        'neg_gex_above': df['f2_neg_gex_above'] == 1,
        'target<30': df['target_dist'] < 30,
        'target<50': df['target_dist'] < 50,
        '+gex<50': df['pos_gex_dist'] < 50,
        '-gex>-15': df['neg_gex_dist'] >= -15,
        '-gex>-10': df['neg_gex_dist'] >= -10,
    }

    for align in align_options:
        for comp_name, comp_mask in component_filters.items():
            mask = comp_mask.copy()
            if align is not None:
                mask = mask & (df['greek_alignment'] >= align)

            sub = df[mask]
            n = len(sub)
            if n < 3:
                continue
            wins = sub['is_win'].sum()
            wr = wins / n * 100
            pnl = sub['outcome_pnl'].sum()
            gw = sub.loc[sub['outcome_pnl'] > 0, 'outcome_pnl'].sum()
            gl = abs(sub.loc[sub['outcome_pnl'] < 0, 'outcome_pnl'].sum())
            pf = gw / gl if gl > 0 else 999
            avg_pnl = sub['outcome_pnl'].mean()

            label = f"align>={align}" if align else "no_align"
            label += f" + {comp_name}"
            results.append({
                'filter': label, 'N': n, 'WR': wr, 'PnL': pnl,
                'PF': pf, 'AvgPnL': avg_pnl
            })

    # Multi-component combos
    multi_combos = [
        ("lis_nearby + target<30", df['f1_lis_nearby'].eq(1) & (df['target_dist'] < 30)),
        ("lis_nearby + +gex<50", df['f1_lis_nearby'].eq(1) & (df['pos_gex_dist'] < 50)),
        ("lis_nearby + -gex>-15", df['f1_lis_nearby'].eq(1) & (df['neg_gex_dist'] >= -15)),
        ("target<30 + +gex<50", (df['target_dist'] < 30) & (df['pos_gex_dist'] < 50)),
        ("lis_nearby + target<30 + +gex<50",
         df['f1_lis_nearby'].eq(1) & (df['target_dist'] < 30) & (df['pos_gex_dist'] < 50)),
    ]
    for combo_name, combo_mask in multi_combos:
        for align in [None, 1]:
            mask = combo_mask.copy()
            if align is not None:
                mask = mask & (df['greek_alignment'] >= align)
            sub = df[mask]
            n = len(sub)
            if n < 3:
                continue
            wins = sub['is_win'].sum()
            wr = wins / n * 100
            pnl = sub['outcome_pnl'].sum()
            gw = sub.loc[sub['outcome_pnl'] > 0, 'outcome_pnl'].sum()
            gl = abs(sub.loc[sub['outcome_pnl'] < 0, 'outcome_pnl'].sum())
            pf = gw / gl if gl > 0 else 999
            avg_pnl = sub['outcome_pnl'].mean()
            label = f"align>={align}" if align else "no_align"
            label += f" + {combo_name}"
            results.append({
                'filter': label, 'N': n, 'WR': wr, 'PnL': pnl,
                'PF': pf, 'AvgPnL': avg_pnl
            })

    rdf = pd.DataFrame(results)
    rdf = rdf.sort_values('PnL', ascending=False)
    print("\n--- All Combos Ranked by PnL ---")
    print(f"{'Filter':<55s} {'N':>3s} {'WR%':>6s} {'PnL':>8s} {'PF':>6s} {'Avg':>6s}")
    print("-" * 90)
    for _, r in rdf.iterrows():
        print(f"  {r['filter']:<53s} {r['N']:3.0f} {r['WR']:5.1f}% {r['PnL']:+7.1f} {r['PF']:5.2f} {r['AvgPnL']:+5.1f}")

    # Highlight top candidates with N >= 5
    print("\n--- TOP CANDIDATES (N>=5, PnL>0) ---")
    top = rdf[(rdf['N'] >= 5) & (rdf['PnL'] > 0)].head(20)
    for _, r in top.iterrows():
        print(f"  {r['filter']:<53s} N={r['N']:3.0f} WR={r['WR']:5.1f}% PnL={r['PnL']:+7.1f} PF={r['PF']:5.2f}")


def part4():
    """Test reverse direction: -GEX below spot distance vs outcome."""
    print("\n" + "=" * 80)
    print("PART 4: -GEX BELOW SPOT DISTANCE vs OUTCOME")
    print("=" * 80)

    df = pd.read_sql("""
        SELECT id, ts, spot, lis, target, max_plus_gex, max_minus_gex,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               greek_alignment
        FROM setup_log WHERE setup_name = 'GEX Long' ORDER BY ts
    """, engine)
    df = compute_force_score(df)
    df['is_win'] = (df['outcome_result'] == 'WIN').astype(int)

    # -GEX below spot: neg_gex_dist < 0
    below = df[df['neg_gex_dist'] < 0].copy()
    below['neg_gex_below'] = -below['neg_gex_dist']  # positive distance

    print(f"\n-GEX below spot: {len(below)} / {len(df)} trades")

    bins = [(0, 10), (10, 20), (20, 40), (40, 80)]
    for lo, hi in bins:
        sub = below[(below['neg_gex_below'] >= lo) & (below['neg_gex_below'] < hi)]
        n = len(sub)
        if n == 0:
            continue
        wr = sub['is_win'].mean() * 100
        pnl = sub['outcome_pnl'].sum()
        avg_mxp = sub['outcome_max_profit'].mean()
        avg_mxl = sub['outcome_max_loss'].mean()
        print(f"  -GEX [{lo:3d}-{hi:3d}) below: N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+7.1f}, "
              f"AvgMFE={avg_mxp:+5.1f}, AvgMAE={avg_mxl:+5.1f}")

    # Cross with alignment
    print("\n--- -GEX Below Spot x Alignment ---")
    for lo, hi in bins:
        for ga in [1, -1]:
            sub = below[(below['neg_gex_below'] >= lo) & (below['neg_gex_below'] < hi) &
                        (below['greek_alignment'] >= ga) & (below['greek_alignment'] <= (ga if ga > 0 else 3))]
            if ga > 0:
                sub = below[(below['neg_gex_below'] >= lo) & (below['neg_gex_below'] < hi) &
                            (below['greek_alignment'] >= 1)]
            else:
                sub = below[(below['neg_gex_below'] >= lo) & (below['neg_gex_below'] < hi) &
                            (below['greek_alignment'] < 1)]
            n = len(sub)
            if n == 0:
                continue
            wr = sub['is_win'].mean() * 100
            pnl = sub['outcome_pnl'].sum()
            label = "align>=1" if ga > 0 else "align<1"
            print(f"  -GEX [{lo:3d}-{hi:3d}) + {label}: N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+7.1f}")


def part5():
    """Find the A+ sweet spot."""
    print("\n" + "=" * 80)
    print("PART 5: A+ SWEET SPOT RECOMMENDATION")
    print("=" * 80)

    df = pd.read_sql("""
        SELECT id, ts, spot, lis, target, max_plus_gex, max_minus_gex, gap_to_lis,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               greek_alignment
        FROM setup_log WHERE setup_name = 'GEX Long' ORDER BY ts
    """, engine)
    df = compute_force_score(df)
    df['is_win'] = (df['outcome_result'] == 'WIN').astype(int)

    # Systematically test ALL meaningful combinations
    candidates = []

    # Build all filter dimensions
    lis_filters = {
        'any': pd.Series(True, index=df.index),
        'lis<=5': df['lis_dist'].abs() <= 5,
        'lis<=3': df['lis_dist'].abs() <= 3,
    }
    align_filters = {
        'any': pd.Series(True, index=df.index),
        'align>=1': df['greek_alignment'] >= 1,
        'align>=3': df['greek_alignment'] >= 3,
    }
    target_filters = {
        'any': pd.Series(True, index=df.index),
        'tgt<30': df['target_dist'] < 30,
        'tgt<50': df['target_dist'] < 50,
    }
    gex_filters = {
        'any': pd.Series(True, index=df.index),
        '+gex<50': df['pos_gex_dist'] < 50,
        '+gex<30': df['pos_gex_dist'] < 30,
        '-gex>-15': df['neg_gex_dist'] >= -15,
        '-gex>-10': df['neg_gex_dist'] >= -10,
    }

    for ln, lm in lis_filters.items():
        for an, am in align_filters.items():
            for tn, tm in target_filters.items():
                for gn, gm in gex_filters.items():
                    mask = lm & am & tm & gm
                    sub = df[mask]
                    n = len(sub)
                    if n < 5:
                        continue
                    wins = sub['is_win'].sum()
                    wr = wins / n * 100
                    pnl = sub['outcome_pnl'].sum()
                    gw = sub.loc[sub['outcome_pnl'] > 0, 'outcome_pnl'].sum()
                    gl = abs(sub.loc[sub['outcome_pnl'] < 0, 'outcome_pnl'].sum())
                    pf = gw / gl if gl > 0 else 999

                    # Skip if all 'any'
                    filters_used = [f for f in [ln, an, tn, gn] if f != 'any']
                    if not filters_used:
                        filters_used = ['baseline']

                    label = ' + '.join(filters_used)
                    candidates.append({
                        'filter': label, 'N': n, 'WR': wr, 'PnL': pnl,
                        'PF': pf, 'Wins': wins, 'Losses': n - wins
                    })

    cdf = pd.DataFrame(candidates)
    # Filter: WR >= 45%, PnL > 0, N >= 5
    good = cdf[(cdf['WR'] >= 45) & (cdf['PnL'] > 0) & (cdf['N'] >= 5)]
    good = good.sort_values('PnL', ascending=False)

    print("\n--- ALL PROFITABLE COMBOS (WR>=45%, N>=5) sorted by PnL ---")
    print(f"{'Filter':<50s} {'N':>3s} {'W':>3s} {'L':>3s} {'WR%':>6s} {'PnL':>8s} {'PF':>6s}")
    print("-" * 85)
    for _, r in good.iterrows():
        print(f"  {r['filter']:<48s} {r['N']:3.0f} {r['Wins']:3.0f} {r['Losses']:3.0f} "
              f"{r['WR']:5.1f}% {r['PnL']:+7.1f} {r['PF']:5.2f}")

    # Also show different SL/T ratios for top combos
    print("\n--- TOP 5 COMBOS with DIFFERENT SL/T RATIOS ---")
    top5 = good.head(5)
    for _, r in top5.iterrows():
        label = r['filter']
        # Reconstruct mask
        mask = pd.Series(True, index=df.index)
        parts = label.split(' + ')
        for p in parts:
            if p == 'lis<=5':
                mask &= df['lis_dist'].abs() <= 5
            elif p == 'lis<=3':
                mask &= df['lis_dist'].abs() <= 3
            elif p == 'align>=1':
                mask &= df['greek_alignment'] >= 1
            elif p == 'align>=3':
                mask &= df['greek_alignment'] >= 3
            elif p == 'tgt<30':
                mask &= df['target_dist'] < 30
            elif p == 'tgt<50':
                mask &= df['target_dist'] < 50
            elif p == '+gex<50':
                mask &= df['pos_gex_dist'] < 50
            elif p == '+gex<30':
                mask &= df['pos_gex_dist'] < 30
            elif p == '-gex>-15':
                mask &= df['neg_gex_dist'] >= -15
            elif p == '-gex>-10':
                mask &= df['neg_gex_dist'] >= -10

        sub = df[mask]
        print(f"\n  {label} (N={len(sub)}):")

        # For each SL/T combo, check if MFE/MAE support it
        for sl, t in [(8, 10), (10, 10), (10, 15), (12, 15), (15, 15), (15, 20), (20, 15)]:
            wins = ((sub['outcome_max_profit'] >= t) | (sub['outcome_pnl'] > 0)).sum()
            losses = (sub['outcome_max_loss'].abs() >= sl).sum()
            # More precise: check outcome
            w = (sub['outcome_pnl'] > 0).sum()
            l = (sub['outcome_pnl'] < 0).sum()
            # Use actual pnl
            sim_pnl = sub['outcome_pnl'].sum()
            print(f"    SL={sl:2d}/T={t:2d}: (actual outcomes) W={w} L={l} PnL={sim_pnl:+.1f}")

    # FINAL RECOMMENDATION
    print("\n" + "=" * 80)
    print("FINAL RECOMMENDATION")
    print("=" * 80)
    if len(good) > 0:
        best = good.iloc[0]
        print(f"\n  Best combo: {best['filter']}")
        print(f"  N={best['N']:.0f}, WR={best['WR']:.1f}%, PnL={best['PnL']:+.1f}, PF={best['PF']:.2f}")
        print(f"  Wins={best['Wins']:.0f}, Losses={best['Losses']:.0f}")
    else:
        print("  No combo meets criteria (WR>=45%, PnL>0, N>=5)")

    # Show individual trades for best combo
    if len(good) > 0:
        label = good.iloc[0]['filter']
        mask = pd.Series(True, index=df.index)
        parts = label.split(' + ')
        for p in parts:
            if p == 'lis<=5':
                mask &= df['lis_dist'].abs() <= 5
            elif p == 'lis<=3':
                mask &= df['lis_dist'].abs() <= 3
            elif p == 'align>=1':
                mask &= df['greek_alignment'] >= 1
            elif p == 'tgt<30':
                mask &= df['target_dist'] < 30
            elif p == 'tgt<50':
                mask &= df['target_dist'] < 50
            elif p == '+gex<50':
                mask &= df['pos_gex_dist'] < 50
            elif p == '+gex<30':
                mask &= df['pos_gex_dist'] < 30
            elif p == '-gex>-15':
                mask &= df['neg_gex_dist'] >= -15
            elif p == '-gex>-10':
                mask &= df['neg_gex_dist'] >= -10

        sub = df[mask].sort_values('ts')
        print(f"\n  Trades in best combo:")
        for _, r in sub.iterrows():
            ts_str = str(r['ts'])[:16]
            print(f"    #{r['id']:3.0f} {ts_str} spot={r['spot']:7.1f} LIS={r['lis']:7.0f} "
                  f"T={r['target']:7.0f} +GEX={r['max_plus_gex']:7.0f} -GEX={r['max_minus_gex']:7.0f} "
                  f"| ld={r['lis_dist']:+5.1f} gd={r['neg_gex_dist']:+5.1f} pd={r['pos_gex_dist']:+5.1f} "
                  f"td={r['target_dist']:+5.1f} | {r['outcome_result']:7s} {r['outcome_pnl']:+5.1f}")


if __name__ == '__main__':
    part1()
    part2()
    part3()
    part4()
    part5()
