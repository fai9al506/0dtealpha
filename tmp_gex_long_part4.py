"""
Part 4: -GEX computation for below-LIS moments + magnet theory.
Computes max -GEX strike from chain_snapshots for each below-LIS window.
"""
import sqlalchemy as sa
import pandas as pd
import numpy as np
from datetime import timedelta
import json

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = sa.create_engine(DB_URL)

def parse_dollar(val):
    if val is None or val == '' or val == 'null':
        return None
    s = str(val).replace('$', '').replace(',', '').strip()
    try:
        return float(s)
    except:
        return None

def compute_gex_from_chain(conn, ts):
    """Compute max +GEX and max -GEX strikes from chain_snapshots near timestamp ts.
    Rows are lists of values, columns define the structure:
    ['Volume','Open Int','IV','Gamma','Delta','BID','BID QTY','ASK','ASK QTY','LAST',
     'Strike',
     'LAST','ASK','ASK QTY','BID','BID QTY','Delta','Gamma','IV','Open Int','Volume']
    Call side = indices 0-9, Strike = index 10, Put side = indices 11-20
    """
    q = sa.text("""
    SELECT id, ts, spot, columns, rows FROM chain_snapshots
    WHERE ts >= :start AND ts <= :end
    ORDER BY ABS(EXTRACT(EPOCH FROM ts - :target))
    LIMIT 1
    """)
    start_ts = ts - timedelta(minutes=3)
    end_ts = ts + timedelta(minutes=3)
    result = conn.execute(q, {'start': str(start_ts), 'end': str(end_ts), 'target': str(ts)}).fetchone()
    if not result:
        return None, None, None

    spot = result[2]
    columns = result[3]
    rows_data = result[4]

    if not rows_data or not columns:
        return spot, None, None

    if isinstance(rows_data, str):
        rows_data = json.loads(rows_data)
    if isinstance(columns, str):
        columns = json.loads(columns)

    # Build column index map
    # Call side: Gamma at index 3, Open Int at index 1
    # Put side: Gamma at index 17, Open Int at index 19
    # Strike at index 10
    c_gamma_idx = 3   # Call Gamma
    c_oi_idx = 1      # Call Open Int
    p_gamma_idx = 17  # Put Gamma
    p_oi_idx = 19     # Put Open Int
    strike_idx = 10   # Strike

    max_plus_gex = None
    max_minus_gex = None
    max_pos_val = -float('inf')
    max_neg_val = float('inf')

    for row in rows_data:
        try:
            if len(row) < 21:
                continue
            strike = float(row[strike_idx] or 0)
            c_gamma = float(row[c_gamma_idx] or 0)
            p_gamma = float(row[p_gamma_idx] or 0)
            c_oi = float(row[c_oi_idx] or 0)
            p_oi = float(row[p_oi_idx] or 0)

            net_gex = (c_gamma * c_oi * 100.0) + (-p_gamma * p_oi * 100.0)

            if net_gex > max_pos_val:
                max_pos_val = net_gex
                max_plus_gex = strike
            if net_gex < max_neg_val:
                max_neg_val = net_gex
                max_minus_gex = strike
        except (ValueError, TypeError):
            continue

    return spot, max_plus_gex, max_minus_gex

print("=" * 80)
print("PART 4: -GEX COMPUTATION + MAGNET THEORY FOR BELOW-LIS MOMENTS")
print("=" * 80)

# Get below-LIS Volland snapshots
with engine.connect() as conn:
    q = sa.text("""
    SELECT v.ts,
           v.payload->'statistics'->>'paradigm' as paradigm,
           v.payload->'statistics'->>'lines_in_sand' as lis_str,
           v.payload->'statistics'->>'target' as target_str
    FROM volland_snapshots v
    WHERE v.payload->'statistics'->>'paradigm' LIKE 'GEX%'
      AND v.payload->'statistics'->>'lines_in_sand' IS NOT NULL
    ORDER BY v.ts
    """)
    vdf = pd.read_sql(q, conn)

vdf['lis'] = vdf['lis_str'].apply(parse_dollar)
vdf['target'] = vdf['target_str'].apply(parse_dollar)
vdf['ts'] = pd.to_datetime(vdf['ts'])
vdf = vdf[vdf['lis'].notna()].copy()

# Match with spots
with engine.connect() as conn:
    q = sa.text("""
    SELECT ts, spot FROM chain_snapshots
    WHERE ts >= :start AND ts <= :end
    ORDER BY ts
    """)
    spots = pd.read_sql(q, conn, params={'start': str(vdf['ts'].min() - timedelta(minutes=5)),
                                          'end': str(vdf['ts'].max() + timedelta(minutes=5))})

spots['ts'] = pd.to_datetime(spots['ts'])
vdf = vdf.sort_values('ts')
spots = spots.sort_values('ts')
merged = pd.merge_asof(vdf, spots, on='ts', tolerance=pd.Timedelta('5min'))
merged = merged[merged['spot'].notna()].copy()
merged['ts_et'] = merged['ts'].dt.tz_convert('US/Eastern')
merged['hour'] = merged['ts_et'].dt.hour
merged['minute'] = merged['ts_et'].dt.minute
merged = merged[(merged['hour'] >= 9) & (merged['hour'] < 16)].copy()
merged = merged[~((merged['hour'] == 9) & (merged['minute'] < 30))].copy()
merged['gap'] = merged['spot'] - merged['lis']
merged['trade_date'] = merged['ts_et'].dt.date

# Get below-LIS windows
below = merged[merged['spot'] < merged['lis']].copy()
below = below.sort_values('ts')
below['prev_ts'] = below['ts'].shift(1)
below['time_gap'] = (below['ts'] - below['prev_ts']).dt.total_seconds()
below['new_window'] = (below['time_gap'] > 600) | (below['time_gap'].isna())
below['prev_date'] = below['trade_date'].shift(1)
below.loc[below['trade_date'] != below['prev_date'], 'new_window'] = True
windows = below[below['new_window']].copy()

# For each window, compute -GEX from chain_snapshots
print(f"\nComputing -GEX for {len(windows)} below-LIS windows...")
print(f"\n{'Date':>10} {'Time':>5} {'Spot':>7} {'LIS':>7} {'-GEX':>7} {'Gap':>6} {'ClusterLIS-GEX':>14} {'SpotVs-GEX':>11} | {'MFE120':>7} {'MAE120':>7} {'SimResult':>9}")

results = []
with engine.connect() as conn:
    for _, w in windows.iterrows():
        entry_ts = w['ts']
        entry_spot = w['spot']
        lis_val = w['lis']

        spot, plus_gex, minus_gex = compute_gex_from_chain(conn, entry_ts)
        if minus_gex is None:
            continue

        cluster = abs(lis_val - minus_gex)
        spot_vs_mgex = entry_spot - minus_gex  # negative = below -GEX

        # Get forward price
        fq = sa.text("""
        SELECT ts, spot FROM chain_snapshots
        WHERE ts > :start AND ts <= :end
        ORDER BY ts
        """)
        end_ts = entry_ts + timedelta(hours=2.5)
        fwd = pd.read_sql(fq, conn, params={'start': str(entry_ts), 'end': str(end_ts)})
        fwd['ts'] = pd.to_datetime(fwd['ts'])
        fwd = fwd[fwd['spot'].notna()].copy()

        mfe120 = None
        mae120 = None
        sim_result = 'NO_DATA'
        sim_pnl = 0

        if len(fwd) > 0:
            highest = fwd['spot'].max()
            lowest = fwd['spot'].min()
            mfe120 = round(highest - entry_spot, 1)
            mae120 = round(entry_spot - lowest, 1)

            for _, fb in fwd.iterrows():
                pnl = fb['spot'] - entry_spot
                if pnl >= 10:
                    sim_result = 'WIN'
                    sim_pnl = 10
                    break
                if pnl <= -8:
                    sim_result = 'LOSS'
                    sim_pnl = -8
                    break
            else:
                sim_result = 'EXPIRED'
                sim_pnl = round(fwd.iloc[-1]['spot'] - entry_spot, 1)

        result = {
            'date': w['trade_date'],
            'time': w['ts_et'].strftime('%H:%M'),
            'spot': entry_spot,
            'lis': lis_val,
            'minus_gex': minus_gex,
            'plus_gex': plus_gex,
            'gap': w['gap'],
            'cluster': cluster,
            'spot_vs_mgex': spot_vs_mgex,
            'mfe_120': mfe120,
            'mae_120': mae120,
            'sim_result': sim_result,
            'sim_pnl': sim_pnl,
            'paradigm': w['paradigm'],
        }
        results.append(result)

        def fmt(v):
            return f'{v:>7.1f}' if isinstance(v, (int, float)) and v is not None else '      -'

        t = w['ts_et']
        print(f"{str(w['trade_date']):>10} {t.strftime('%H:%M'):>5} {entry_spot:>7.1f} {lis_val:>7.1f} {minus_gex:>7.0f} {w['gap']:>6.1f} {cluster:>14.1f} {spot_vs_mgex:>11.1f} | {fmt(mfe120)} {fmt(mae120)} {sim_result:>9}")

rdf = pd.DataFrame(results)

if len(rdf) > 0:
    print(f"\n" + "=" * 80)
    print(f"MAGNET THEORY ANALYSIS")
    print("=" * 80)

    # LIS as magnet: how often did price reach LIS from below?
    rdf['reached_lis'] = rdf.apply(lambda r: r['mfe_120'] is not None and (r['spot'] + r['mfe_120']) >= r['lis'], axis=1)
    reached = rdf['reached_lis'].sum()
    print(f"\n  Price reached LIS from below within 2hrs: {reached}/{len(rdf)} ({100*reached/len(rdf):.0f}%)")

    # -GEX as magnet: when spot is below -GEX too
    below_mgex = rdf[rdf['spot_vs_mgex'] < 0]
    above_mgex = rdf[rdf['spot_vs_mgex'] >= 0]
    print(f"\n  Spot BELOW -GEX: {len(below_mgex)} windows")
    if len(below_mgex) > 0:
        bg_reached = below_mgex['reached_lis'].sum()
        print(f"    Reached LIS: {bg_reached}/{len(below_mgex)}")
        bw = len(below_mgex[below_mgex['sim_result'] == 'WIN'])
        bl = len(below_mgex[below_mgex['sim_result'] == 'LOSS'])
        bp = below_mgex['sim_pnl'].sum()
        bwr = bw/(bw+bl)*100 if (bw+bl)>0 else 0
        print(f"    Sim trades: {bw}W/{bl}L, WR={bwr:.0f}%, PnL={bp:.1f}")

    print(f"\n  Spot ABOVE -GEX (between -GEX and LIS): {len(above_mgex)} windows")
    if len(above_mgex) > 0:
        ag_reached = above_mgex['reached_lis'].sum()
        print(f"    Reached LIS: {ag_reached}/{len(above_mgex)}")
        aw = len(above_mgex[above_mgex['sim_result'] == 'WIN'])
        al = len(above_mgex[above_mgex['sim_result'] == 'LOSS'])
        ap = above_mgex['sim_pnl'].sum()
        awr = aw/(aw+al)*100 if (aw+al)>0 else 0
        print(f"    Sim trades: {aw}W/{al}L, WR={awr:.0f}%, PnL={ap:.1f}")

    # Cluster analysis for below-LIS
    print(f"\n  Cluster (|LIS - (-GEX)|) analysis:")
    for lo, hi, label in [(0, 5, '[0-5]'), (5, 15, '[5-15]'), (15, 30, '[15-30]'), (30, 100, '[30+]')]:
        mask = (rdf['cluster'] >= lo) & (rdf['cluster'] < hi)
        sub = rdf[mask]
        if len(sub) == 0:
            continue
        w = len(sub[sub['sim_result'] == 'WIN'])
        l = len(sub[sub['sim_result'] == 'LOSS'])
        sp = sub['sim_pnl'].sum()
        wr = w/(w+l)*100 if (w+l)>0 else 0
        reached = sub['reached_lis'].sum()
        print(f"    {label:>8}: {len(sub)} windows, sim={w}W/{l}L WR={wr:.0f}% PnL={sp:.1f}, LIS reached={reached}/{len(sub)}")

    # "A+ Setup" = LIS and -GEX within 5 pts (cluster <= 5) and spot within 5 pts of either
    print(f"\n" + "=" * 80)
    print(f"'A+ SETUP' = Cluster <= 5 (LIS and -GEX near each other)")
    print("=" * 80)
    a_plus = rdf[rdf['cluster'] <= 5]
    print(f"  {len(a_plus)} windows qualify")
    if len(a_plus) > 0:
        for _, r in a_plus.iterrows():
            print(f"    {r['date']} {r['time']} spot={r['spot']:.1f} lis={r['lis']:.1f} -gex={r['minus_gex']:.0f} cluster={r['cluster']:.0f} sim={r['sim_result']} pnl={r['sim_pnl']}")

    # ALL windows: distance-based buckets
    print(f"\n" + "=" * 80)
    print(f"GAP (spot below LIS distance) vs SIMULATED PERFORMANCE")
    print("=" * 80)
    for lo, hi, label in [(-15, -5, 'Gap [-15,-5]'), (-5, -2, 'Gap [-5,-2]'), (-2, 0, 'Gap [-2,0]')]:
        mask = (rdf['gap'] >= lo) & (rdf['gap'] < hi)
        sub = rdf[mask]
        if len(sub) == 0:
            continue
        w = len(sub[sub['sim_result'] == 'WIN'])
        l = len(sub[sub['sim_result'] == 'LOSS'])
        sp = sub['sim_pnl'].sum()
        wr = w/(w+l)*100 if (w+l)>0 else 0
        avg_mfe = sub['mfe_120'].dropna().mean()
        avg_mae = sub['mae_120'].dropna().mean()
        print(f"  {label:>15}: {len(sub)} windows, {w}W/{l}L WR={wr:.0f}% PnL={sp:.1f}, avgMFE120={avg_mfe:.1f}, avgMAE120={avg_mae:.1f}")

# ═══════════════════════════════════════════════════════════════════════
# PART 4B: Compute -GEX for the EXISTING 42 GEX Long trades too
# This gives us the full picture of -GEX cluster for comparison
# ═══════════════════════════════════════════════════════════════════════
print(f"\n" + "=" * 80)
print(f"PART 4B: FULL PICTURE — ALL GEX LONG TRADE DAYS")
print("=" * 80)

with engine.connect() as conn:
    q = sa.text("""
    SELECT id, ts, spot, lis, target, max_minus_gex, gap_to_lis, upside,
           outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
           greek_alignment
    FROM setup_log WHERE setup_name = 'GEX Long'
    ORDER BY ts
    """)
    trades = pd.read_sql(q, conn)

trades['ts_et'] = pd.to_datetime(trades['ts']).dt.tz_convert('US/Eastern')
trades['cluster'] = abs(trades['lis'] - trades['max_minus_gex'])
trades['spot_vs_mgex'] = trades['spot'] - trades['max_minus_gex']

# Now combine: existing trades + hypothetical below-LIS trades
print(f"\nExisting GEX Long trades: {len(trades)}")
if len(rdf) > 0:
    print(f"Hypothetical below-LIS trades: {len(rdf)}")

    # What would the combined portfolio look like with best filters?
    # Existing + hypothetical, all with align >= 1 equivalent, gap <= 5, cluster <= 10
    ex_good = trades[(trades['gap_to_lis'] <= 5) & (trades['cluster'] <= 10) & (trades['greek_alignment'] >= 1)]
    print(f"\n  Existing filtered (gap<=5, cluster<=10, align>=1): {len(ex_good)} trades")
    if len(ex_good) > 0:
        ew = len(ex_good[ex_good['outcome_result'] == 'WIN'])
        el = len(ex_good[ex_good['outcome_result'] == 'LOSS'])
        ep = ex_good['outcome_pnl'].sum()
        ewr = ew/(ew+el)*100 if (ew+el)>0 else 0
        print(f"    {ew}W/{el}L, WR={ewr:.0f}%, PnL={ep:.1f}")

    hyp_good = rdf[(abs(rdf['gap']) <= 5) & (rdf['cluster'] <= 10)]
    print(f"\n  Hypothetical below-LIS filtered (|gap|<=5, cluster<=10): {len(hyp_good)} trades")
    if len(hyp_good) > 0:
        hw = len(hyp_good[hyp_good['sim_result'] == 'WIN'])
        hl = len(hyp_good[hyp_good['sim_result'] == 'LOSS'])
        hp = hyp_good['sim_pnl'].sum()
        hwr = hw/(hw+hl)*100 if (hw+hl)>0 else 0
        print(f"    {hw}W/{hl}L, WR={hwr:.0f}%, PnL={hp:.1f}")

    # Combined
    combined_pnl = (ex_good['outcome_pnl'].sum() if len(ex_good) > 0 else 0) + (hyp_good['sim_pnl'].sum() if len(hyp_good) > 0 else 0)
    combined_n = len(ex_good) + len(hyp_good)
    print(f"\n  COMBINED (existing + below-LIS): {combined_n} trades, PnL={combined_pnl:.1f}")
