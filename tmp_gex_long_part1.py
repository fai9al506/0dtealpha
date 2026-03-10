"""
Part 1: Find MISSED GEX Long signals from raw Volland data.
Paradigm names: GEX-PURE, GEX-LIS, GEX-TARGET, GEX-MESSY
LIS stored as: payload->'statistics'->>'lines_in_sand' (e.g. "$6,721")
"""
import sqlalchemy as sa
import pandas as pd
import numpy as np
from datetime import timedelta, time as dtime
import json

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = sa.create_engine(DB_URL)

def parse_dollar(val):
    """Parse '$6,721' or '6721' to float"""
    if val is None or val == '' or val == 'null':
        return None
    s = str(val).replace('$', '').replace(',', '').strip()
    try:
        return float(s)
    except:
        return None

print("=" * 80)
print("PART 1: FIND MISSED GEX LONG SIGNALS FROM RAW VOLLAND DATA")
print("=" * 80)

# Step 1: Get ALL GEX paradigm snapshots with LIS and spot
print("\nStep 1: Getting all GEX paradigm snapshots with LIS...")
with engine.connect() as conn:
    q = sa.text("""
    SELECT v.ts,
           v.payload->'statistics'->>'paradigm' as paradigm,
           v.payload->'statistics'->>'lines_in_sand' as lis_str,
           v.payload->'statistics'->>'target' as target_str,
           v.payload->'statistics'->>'aggregatedCharm' as agg_charm,
           v.payload->'statistics'->>'delta_decay_hedging' as dd_hedging
    FROM volland_snapshots v
    WHERE v.payload->'statistics'->>'paradigm' LIKE 'GEX%'
      AND v.payload->'statistics'->>'lines_in_sand' IS NOT NULL
    ORDER BY v.ts
    """)
    vdf = pd.read_sql(q, conn)

print(f"  Found {len(vdf)} Volland snapshots with GEX paradigm + LIS")
vdf['lis'] = vdf['lis_str'].apply(parse_dollar)
vdf['target'] = vdf['target_str'].apply(parse_dollar)
vdf['ts'] = pd.to_datetime(vdf['ts'])

# Drop invalid LIS
vdf = vdf[vdf['lis'].notna()].copy()
print(f"  After filtering invalid LIS: {len(vdf)}")
print(f"  Date range: {vdf['ts'].min()} to {vdf['ts'].max()}")
print(f"  LIS range: {vdf['lis'].min():.0f} to {vdf['lis'].max():.0f}")
print(f"  Paradigm breakdown:")
print(vdf['paradigm'].value_counts().to_string())

# Step 2: Get matching SPX spots from chain_snapshots
print("\nStep 2: Matching with SPX spot prices...")
with engine.connect() as conn:
    q = sa.text("""
    SELECT ts, spot FROM chain_snapshots
    WHERE ts >= :start AND ts <= :end
    ORDER BY ts
    """)
    min_ts = vdf['ts'].min() - timedelta(minutes=5)
    max_ts = vdf['ts'].max() + timedelta(minutes=5)
    spots = pd.read_sql(q, conn, params={'start': str(min_ts), 'end': str(max_ts)})

spots['ts'] = pd.to_datetime(spots['ts'])
print(f"  Found {len(spots)} chain snapshots")

# Merge on nearest timestamp
vdf = vdf.sort_values('ts')
spots = spots.sort_values('ts')
merged = pd.merge_asof(vdf, spots, on='ts', tolerance=pd.Timedelta('5min'))
merged = merged[merged['spot'].notna()].copy()
print(f"  Matched {len(merged)} rows with spot prices")

# Filter to market hours ET
merged['ts_et'] = merged['ts'].dt.tz_convert('US/Eastern')
merged['hour'] = merged['ts_et'].dt.hour
merged['minute'] = merged['ts_et'].dt.minute
merged = merged[(merged['hour'] >= 9) & (merged['hour'] < 16)].copy()
merged = merged[~((merged['hour'] == 9) & (merged['minute'] < 30))].copy()
print(f"  Market hours only: {len(merged)}")

# Compute position
merged['gap'] = merged['spot'] - merged['lis']  # negative = below LIS
merged['above_lis'] = merged['spot'] >= merged['lis']
merged['below_lis'] = merged['spot'] < merged['lis']
merged['abs_gap'] = abs(merged['gap'])
merged['trade_date'] = merged['ts_et'].dt.date

# ═══════════════════════════════════════════════════════════════════════
# Overall distribution: how often is spot above vs below LIS when GEX paradigm?
# ═══════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("OVERALL: SPOT POSITION RELATIVE TO LIS DURING GEX PARADIGM")
print("=" * 80)

above = merged[merged['above_lis']]
below = merged[merged['below_lis']]
print(f"\n  Total GEX snapshots (market hours): {len(merged)}")
print(f"  Spot ABOVE LIS: {len(above)} ({100*len(above)/len(merged):.1f}%)")
print(f"  Spot BELOW LIS: {len(below)} ({100*len(below)/len(merged):.1f}%)")

print(f"\n  Gap distribution (spot - LIS):")
for p in [5, 10, 25, 50, 75, 90, 95]:
    print(f"    {p}th percentile: {merged['gap'].quantile(p/100):.1f}")

# Gap buckets
print(f"\n  Gap buckets:")
for lo, hi, label in [(-100, -20, 'Below -20'), (-20, -10, '[-20,-10]'), (-10, -5, '[-10,-5]'),
                       (-5, 0, '[-5,0]'), (0, 5, '[0,5]'), (5, 10, '[5,10]'),
                       (10, 20, '[10,20]'), (20, 100, 'Above +20')]:
    mask = (merged['gap'] >= lo) & (merged['gap'] < hi)
    cnt = mask.sum()
    pct = 100 * cnt / len(merged) if len(merged) > 0 else 0
    print(f"    {label:>12}: {cnt:>5} snapshots ({pct:>5.1f}%)")

# ═══════════════════════════════════════════════════════════════════════
# Below-LIS windows: unique entry points
# ═══════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("BELOW-LIS WINDOWS: Potential Missed GEX Long Signals")
print("=" * 80)

# Get unique windows (entries at least 10 min apart, take first of each cluster)
below_sorted = below.sort_values('ts').copy()
below_sorted['prev_ts'] = below_sorted['ts'].shift(1)
below_sorted['time_gap'] = (below_sorted['ts'] - below_sorted['prev_ts']).dt.total_seconds()
below_sorted['new_window'] = (below_sorted['time_gap'] > 600) | (below_sorted['time_gap'].isna())
# Also reset on date change
below_sorted['prev_date'] = below_sorted['trade_date'].shift(1)
below_sorted.loc[below_sorted['trade_date'] != below_sorted['prev_date'], 'new_window'] = True

windows = below_sorted[below_sorted['new_window']].copy()
print(f"\n  Unique below-LIS windows (10min apart): {len(windows)}")

# For each window, trace forward price
print(f"\n{'Date':>10} {'Time':>5} {'Spot':>7} {'LIS':>7} {'Gap':>6} {'Paradigm':>12} | {'MFE30':>7} {'MFE60':>7} {'MFE120':>7} | {'MAE30':>7} {'MAE60':>7} | {'ReachLIS':>8} {'ReachLIS+10':>11}")

results = []
for _, w in windows.iterrows():
    entry_ts = w['ts']
    entry_spot = w['spot']
    lis_val = w['lis']

    # Get forward spot prices from chain_snapshots
    with engine.connect() as conn:
        fq = sa.text("""
        SELECT ts, spot FROM chain_snapshots
        WHERE ts > :start AND ts <= :end
        ORDER BY ts
        """)
        end_ts = entry_ts + timedelta(hours=2.5)
        fwd = pd.read_sql(fq, conn, params={'start': str(entry_ts), 'end': str(end_ts)})

    if len(fwd) == 0:
        continue

    fwd['ts'] = pd.to_datetime(fwd['ts'])
    result = {
        'date': w['trade_date'], 'time': w['ts_et'].strftime('%H:%M'),
        'spot': entry_spot, 'lis': lis_val, 'gap': w['gap'],
        'paradigm': w['paradigm'],
    }

    for mins in [30, 60, 120]:
        window_fwd = fwd[fwd['ts'] <= entry_ts + timedelta(minutes=mins)]
        if len(window_fwd) == 0:
            continue
        highest = window_fwd['spot'].max()
        lowest = window_fwd['spot'].min()
        mfe = highest - entry_spot
        mae = entry_spot - lowest
        result[f'mfe_{mins}'] = round(mfe, 1)
        result[f'mae_{mins}'] = round(mae, 1)

    all_fwd = fwd['spot']
    result['reached_lis'] = bool(all_fwd.max() >= lis_val) if len(all_fwd) > 0 else False
    result['reached_lis_10'] = bool(all_fwd.max() >= lis_val + 10) if len(all_fwd) > 0 else False

    # Would a simple +10pt target / -8pt stop have worked?
    if len(fwd) > 0:
        fwd_valid = fwd[fwd['spot'].notna()].copy()
        sim_done = False
        for _, fb in fwd_valid.iterrows():
            pnl_at_bar = fb['spot'] - entry_spot
            if pnl_at_bar >= 10:
                result['sim_result'] = 'WIN'
                result['sim_pnl'] = 10
                sim_done = True
                break
            if pnl_at_bar <= -8:
                result['sim_result'] = 'LOSS'
                result['sim_pnl'] = -8
                sim_done = True
                break
        if not sim_done:
            if len(fwd_valid) > 0:
                last_pnl = fwd_valid.iloc[-1]['spot'] - entry_spot
                result['sim_result'] = 'EXPIRED'
                result['sim_pnl'] = round(last_pnl, 1)
            else:
                result['sim_result'] = 'NO_DATA'
                result['sim_pnl'] = 0

    results.append(result)

    def fmt(v):
        return f'{v:>7.1f}' if isinstance(v, (int, float)) else '      -'

    lis_hit = 'YES' if result.get('reached_lis') else 'no'
    lis10_hit = 'YES' if result.get('reached_lis_10') else 'no'
    print(f"{str(w['trade_date']):>10} {w['ts_et'].strftime('%H:%M'):>5} {entry_spot:>7.1f} {lis_val:>7.1f} {w['gap']:>6.1f} {w['paradigm']:>12} | {fmt(result.get('mfe_30'))} {fmt(result.get('mfe_60'))} {fmt(result.get('mfe_120'))} | {fmt(result.get('mae_30'))} {fmt(result.get('mae_60'))} | {lis_hit:>8} {lis10_hit:>11}")

# Summary
rdf = pd.DataFrame(results)
print(f"\n" + "-" * 80)
print(f"BELOW-LIS SUMMARY ({len(rdf)} unique windows)")
print("-" * 80)

if len(rdf) > 0:
    reached_count = rdf['reached_lis'].sum()
    reached_10 = rdf['reached_lis_10'].sum()
    print(f"  Reached LIS within ~2.5hrs: {reached_count}/{len(rdf)} ({100*reached_count/len(rdf):.0f}%)")
    print(f"  Reached LIS+10 within ~2.5hrs: {reached_10}/{len(rdf)} ({100*reached_10/len(rdf):.0f}%)")

    for col in ['mfe_30', 'mfe_60', 'mfe_120', 'mae_30', 'mae_60']:
        valid = rdf[col].dropna()
        if len(valid) > 0:
            print(f"  {col}: mean={valid.mean():.1f}, median={valid.median():.1f}")

    # MFE >= 10 stats
    print(f"\n  MFE >= 10 rates:")
    for mins in [30, 60, 120]:
        col = f'mfe_{mins}'
        valid = rdf[col].dropna()
        if len(valid) > 0:
            r10 = len(valid[valid >= 10])
            print(f"    {mins}min: {r10}/{len(valid)} ({100*r10/len(valid):.0f}%)")

    # Simulated trade results
    if 'sim_result' in rdf.columns:
        print(f"\n  Simulated trades (T=+10, SL=-8):")
        wins = len(rdf[rdf['sim_result'] == 'WIN'])
        losses = len(rdf[rdf['sim_result'] == 'LOSS'])
        expired = len(rdf[rdf['sim_result'] == 'EXPIRED'])
        total_pnl = rdf['sim_pnl'].sum()
        wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        print(f"    {wins}W/{losses}L/{expired}E, WR={wr:.1f}%, PnL={total_pnl:.1f}")

    # By gap bucket
    print(f"\n  By gap (spot - LIS) bucket:")
    for lo, hi, label in [(-50, -10, '[-50,-10]'), (-10, -5, '[-10,-5]'), (-5, 0, '[-5,0]')]:
        mask = (rdf['gap'] >= lo) & (rdf['gap'] < hi)
        sub = rdf[mask]
        if len(sub) == 0:
            continue
        if 'sim_result' in sub.columns:
            w = len(sub[sub['sim_result'] == 'WIN'])
            l = len(sub[sub['sim_result'] == 'LOSS'])
            sp = sub['sim_pnl'].sum()
            print(f"    {label:>12}: {len(sub)} windows, sim={w}W/{l}L, PnL={sp:.1f}")

# ═══════════════════════════════════════════════════════════════════════
# Part 1B: Compare with ABOVE-LIS windows that DID fire
# ═══════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("COMPARISON: ABOVE-LIS (fired) vs BELOW-LIS (missed)")
print("=" * 80)

# Above-LIS windows with gap <= 5 (small gap, near LIS — comparable to below-LIS near LIS)
above_near = above[above['gap'] <= 5].copy()
above_near_sorted = above_near.sort_values('ts')
above_near_sorted['prev_ts'] = above_near_sorted['ts'].shift(1)
above_near_sorted['time_gap'] = (above_near_sorted['ts'] - above_near_sorted['prev_ts']).dt.total_seconds()
above_near_sorted['new_window'] = (above_near_sorted['time_gap'] > 600) | (above_near_sorted['time_gap'].isna())
above_near_sorted['prev_date'] = above_near_sorted['trade_date'].shift(1)
above_near_sorted.loc[above_near_sorted['trade_date'] != above_near_sorted['prev_date'], 'new_window'] = True
above_windows = above_near_sorted[above_near_sorted['new_window']].copy()

print(f"\n  Above-LIS windows (gap <= 5, 10min apart): {len(above_windows)}")

# Trace forward for above-LIS too
above_results = []
for _, w in above_windows.iterrows():
    entry_ts = w['ts']
    entry_spot = w['spot']
    with engine.connect() as conn:
        fq = sa.text("""
        SELECT ts, spot FROM chain_snapshots
        WHERE ts > :start AND ts <= :end
        ORDER BY ts
        """)
        end_ts = entry_ts + timedelta(hours=2.5)
        fwd = pd.read_sql(fq, conn, params={'start': str(entry_ts), 'end': str(end_ts)})
    if len(fwd) == 0:
        continue
    fwd['ts'] = pd.to_datetime(fwd['ts'])
    r = {'gap': w['gap'], 'spot': entry_spot, 'lis': w['lis']}
    for mins in [30, 60, 120]:
        wf = fwd[fwd['ts'] <= entry_ts + timedelta(minutes=mins)]
        if len(wf) > 0:
            r[f'mfe_{mins}'] = round(wf['spot'].max() - entry_spot, 1)
            r[f'mae_{mins}'] = round(entry_spot - wf['spot'].min(), 1)
    # Sim trade
    fwd_v = fwd[fwd['spot'].notna()].copy()
    sim_done2 = False
    for _, fb in fwd_v.iterrows():
        pnl = fb['spot'] - entry_spot
        if pnl >= 10:
            r['sim_result'] = 'WIN'
            r['sim_pnl'] = 10
            sim_done2 = True
            break
        if pnl <= -8:
            r['sim_result'] = 'LOSS'
            r['sim_pnl'] = -8
            sim_done2 = True
            break
    if not sim_done2:
        if len(fwd_v) > 0:
            last_pnl = fwd_v.iloc[-1]['spot'] - entry_spot
            r['sim_result'] = 'EXPIRED'
            r['sim_pnl'] = round(last_pnl, 1)
        else:
            r['sim_result'] = 'NO_DATA'
            r['sim_pnl'] = 0
    above_results.append(r)

adf = pd.DataFrame(above_results)
if len(adf) > 0 and len(rdf) > 0:
    print(f"\n  {'Metric':>20} | {'Below LIS':>15} | {'Above LIS (gap<=5)':>20}")
    for col in ['mfe_30', 'mfe_60', 'mfe_120', 'mae_30', 'mae_60']:
        bv = rdf[col].dropna()
        av = adf[col].dropna()
        b_mean = bv.mean() if len(bv) > 0 else 0
        a_mean = av.mean() if len(av) > 0 else 0
        print(f"  {col:>20} | {b_mean:>15.1f} | {a_mean:>20.1f}")

    # Sim results comparison
    if 'sim_result' in adf.columns and 'sim_result' in rdf.columns:
        bw = len(rdf[rdf['sim_result'] == 'WIN'])
        bl = len(rdf[rdf['sim_result'] == 'LOSS'])
        bpnl = rdf['sim_pnl'].sum()
        bwr = bw / (bw + bl) * 100 if (bw + bl) > 0 else 0

        aw = len(adf[adf['sim_result'] == 'WIN'])
        al = len(adf[adf['sim_result'] == 'LOSS'])
        apnl = adf['sim_pnl'].sum()
        awr = aw / (aw + al) * 100 if (aw + al) > 0 else 0

        print(f"\n  Simulated trades (T=+10, SL=-8):")
        print(f"    Below LIS:         {bw}W/{bl}L, WR={bwr:.0f}%, PnL={bpnl:.1f}")
        print(f"    Above LIS (gap<=5): {aw}W/{al}L, WR={awr:.0f}%, PnL={apnl:.1f}")
