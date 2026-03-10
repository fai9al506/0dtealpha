"""
GEX Long Forward Simulation - SL/T Grid Testing on ALL GEX Paradigm Moments
Research only - does NOT modify any code or database
"""
import sqlalchemy as sa, pandas as pd, numpy as np, json

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = sa.create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})


def parse_dollar(s):
    if s is None:
        return np.nan
    return float(str(s).replace('$', '').replace(',', ''))


# Step 1: Pull & deduplicate GEX paradigm snapshots
print("Pulling GEX paradigm snapshots...")
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
print(f"  {len(volland)} -> {len(volland_dedup)} deduped")

# Step 2: Match with chain snapshots
print("Matching chain snapshots...")
chain_ts = pd.read_sql(
    "SELECT id, ts, spot FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY ts",
    engine
)
chain_ts['ts'] = pd.to_datetime(chain_ts['ts'], utc=True)
volland_dedup = volland_dedup.sort_values('ts')
chain_ts = chain_ts.sort_values('ts')
merged = pd.merge_asof(
    volland_dedup, chain_ts, on='ts', direction='nearest',
    tolerance=pd.Timedelta('3min'), suffixes=('', '_chain')
)
merged = merged.dropna(subset=['spot'])
print(f"  {len(merged)} matched")

# Step 3: Compute GEX from chain
chain_ids = merged['id'].unique().tolist()
print(f"Computing GEX from {len(chain_ids)} chains...")
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
    if (i + batch_size) % 200 == 0:
        print(f"  {min(i + batch_size, len(chain_ids))}/{len(chain_ids)}")

gex_df = pd.DataFrame(results)
m = merged.merge(gex_df, left_on='id', right_on='chain_id', how='left')
m = m.dropna(subset=['max_plus_gex', 'max_minus_gex', 'lis', 'target'])

# Step 4: Compute forces
m['lis_dist'] = m['lis'] - m['spot']
m['neg_gex_dist'] = m['max_minus_gex'] - m['spot']
m['pos_gex_dist'] = m['max_plus_gex'] - m['spot']
m['target_dist'] = m['target'] - m['spot']
m['force_score'] = (
    (m['lis_dist'].abs() <= 5).astype(int) +
    (m['neg_gex_dist'] > 0).astype(int) +
    (m['pos_gex_dist'] >= 10).astype(int) +
    (m['target_dist'] >= 10).astype(int) +
    ((m['lis_dist'] <= 0) & (m['lis_dist'] >= -5) & (m['neg_gex_dist'] > 0)).astype(int) +
    ((m['lis_dist'] > 0) & (m['lis_dist'] <= 5) & (m['pos_gex_dist'] >= 10)).astype(int)
)
print(f"Final dataset: {len(m)} GEX moments")

# Step 5: Forward MFE/MAE from ES 1-min bars
print("Pulling ES 1-min bars...")
es_bars = pd.read_sql(
    "SELECT ts, bar_close_price as price, bar_high_price as high, bar_low_price as low "
    "FROM es_delta_bars ORDER BY ts",
    engine,
)
es_bars['ts'] = pd.to_datetime(es_bars['ts'], utc=True)
print(f"  {len(es_bars)} bars")

es_min, es_max = es_bars['ts'].min(), es_bars['ts'].max()
m_fwd = m[(m['ts'] >= es_min) & (m['ts'] <= es_max - pd.Timedelta('30min'))].copy()
print(f"  {len(m_fwd)} moments in ES range")

es_ts_arr = es_bars['ts'].values
es_prices = es_bars[['price', 'high', 'low']].values

mfe_list, mae_list, final_list, valid_idx = [], [], [], []
for idx, row in m_fwd.iterrows():
    entry_ts_np = row['ts'].to_datetime64()
    end_ts_np = entry_ts_np + np.timedelta64(60, 'm')
    mask = (es_ts_arr > entry_ts_np) & (es_ts_arr <= end_ts_np)
    window = np.where(mask)[0]
    if len(window) == 0:
        continue
    first_price = es_prices[window[0], 0]
    mfe_list.append(es_prices[window, 1].max() - first_price)
    mae_list.append(es_prices[window, 2].min() - first_price)
    final_list.append(es_prices[window[-1], 0] - first_price)
    valid_idx.append(idx)

fwd = m_fwd.loc[valid_idx].copy()
fwd['mfe'] = mfe_list
fwd['mae'] = mae_list
fwd['final'] = final_list
print(f"Forward data: {len(fwd)} moments with 60min MFE/MAE\n")

# =====================================================================
# ANALYSIS
# =====================================================================
print("=" * 100)
print("FORWARD-SIMULATED ENTRY CRITERIA (60min window, ES 1-min bars)")
print("=" * 100)

sl_t_combos = [(8, 10), (10, 10), (10, 15), (12, 15), (15, 15), (15, 20), (20, 15)]


def sim_slt(sub, sl, t):
    """Simulate SL/T on forward data. Returns (wins, losses, flat, wr, pnl)."""
    wins = ((sub['mfe'] >= t) & (sub['mae'] > -sl)).sum()
    losses = (sub['mae'] <= -sl).sum()
    flat = len(sub) - wins - losses
    total = len(sub)
    wr = wins / total * 100 if total > 0 else 0
    pnl = wins * t - losses * sl
    return wins, losses, flat, wr, pnl


# Header
hdr = f"{'Filter':<35s}"
for sl, t in sl_t_combos:
    hdr += f" SL{sl}/T{t:<2d}"
hdr += f" {'N':>5s}"
print(hdr)
print("-" * 120)

# Single filters
single_filters = {
    'baseline (all GEX moments)': pd.Series(True, index=fwd.index),
    'force>=3': fwd['force_score'] >= 3,
    'force>=4': fwd['force_score'] >= 4,
    'force>=5': fwd['force_score'] >= 5,
    'lis<=5': fwd['lis_dist'].abs() <= 5,
    'lis<=3': fwd['lis_dist'].abs() <= 3,
    'tgt<30': fwd['target_dist'] < 30,
    'tgt<50': fwd['target_dist'] < 50,
    '+gex<50': fwd['pos_gex_dist'] < 50,
    '+gex<30': fwd['pos_gex_dist'] < 30,
    '-gex>-15': fwd['neg_gex_dist'] >= -15,
    '-gex>-10': fwd['neg_gex_dist'] >= -10,
    '-gex>0 (above spot)': fwd['neg_gex_dist'] > 0,
}

for fname, fmask in single_filters.items():
    sub = fwd[fmask]
    n = len(sub)
    if n < 3:
        continue
    line = f"  {fname:<33s}"
    for sl, t in sl_t_combos:
        _, _, _, wr, pnl = sim_slt(sub, sl, t)
        line += f" {wr:3.0f}%/{pnl:+4.0f}"
    line += f" {n:5d}"
    print(line)

# 2-filter combos
print(f"\n{'2-Filter Combos':<35s}", end="")
for sl, t in sl_t_combos:
    print(f" SL{sl}/T{t:<2d}", end="")
print(f" {'N':>5s}")
print("-" * 120)

combos2 = [
    ('lis<=5 + tgt<50',
     (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 50)),
    ('lis<=5 + tgt<30',
     (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 30)),
    ('lis<=5 + +gex<50',
     (fwd['lis_dist'].abs() <= 5) & (fwd['pos_gex_dist'] < 50)),
    ('lis<=5 + -gex>-15',
     (fwd['lis_dist'].abs() <= 5) & (fwd['neg_gex_dist'] >= -15)),
    ('lis<=5 + -gex>0',
     (fwd['lis_dist'].abs() <= 5) & (fwd['neg_gex_dist'] > 0)),
    ('tgt<50 + +gex<50',
     (fwd['target_dist'] < 50) & (fwd['pos_gex_dist'] < 50)),
    ('tgt<30 + +gex<50',
     (fwd['target_dist'] < 30) & (fwd['pos_gex_dist'] < 50)),
    ('tgt<30 + -gex>-10',
     (fwd['target_dist'] < 30) & (fwd['neg_gex_dist'] >= -10)),
    ('-gex>-10 + lis<=5',
     (fwd['neg_gex_dist'] >= -10) & (fwd['lis_dist'].abs() <= 5)),
    ('-gex>0 + tgt<30',
     (fwd['neg_gex_dist'] > 0) & (fwd['target_dist'] < 30)),
    ('-gex>0 + lis<=5',
     (fwd['neg_gex_dist'] > 0) & (fwd['lis_dist'].abs() <= 5)),
    ('force>=3 + tgt<30',
     (fwd['force_score'] >= 3) & (fwd['target_dist'] < 30)),
    ('force>=3 + lis<=5',
     (fwd['force_score'] >= 3) & (fwd['lis_dist'].abs() <= 5)),
    ('force>=4 + tgt<50',
     (fwd['force_score'] >= 4) & (fwd['target_dist'] < 50)),
]

for name, mask in combos2:
    sub = fwd[mask]
    n = len(sub)
    if n < 3:
        continue
    line = f"  {name:<33s}"
    for sl, t in sl_t_combos:
        _, _, _, wr, pnl = sim_slt(sub, sl, t)
        line += f" {wr:3.0f}%/{pnl:+4.0f}"
    line += f" {n:5d}"
    print(line)

# 3-filter combos
print(f"\n{'3-Filter Combos':<40s}", end="")
for sl, t in sl_t_combos:
    print(f" SL{sl}/T{t:<2d}", end="")
print(f" {'N':>5s}")
print("-" * 120)

combos3 = [
    ('lis<=5 + tgt<50 + +gex<50',
     (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 50) & (fwd['pos_gex_dist'] < 50)),
    ('lis<=5 + tgt<30 + +gex<50',
     (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 30) & (fwd['pos_gex_dist'] < 50)),
    ('lis<=5 + tgt<30 + -gex>-15',
     (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 30) & (fwd['neg_gex_dist'] >= -15)),
    ('lis<=5 + -gex>-10 + tgt<50',
     (fwd['lis_dist'].abs() <= 5) & (fwd['neg_gex_dist'] >= -10) & (fwd['target_dist'] < 50)),
    ('lis<=5 + -gex>0 + tgt<50',
     (fwd['lis_dist'].abs() <= 5) & (fwd['neg_gex_dist'] > 0) & (fwd['target_dist'] < 50)),
    ('lis<=3 + tgt<30 + -gex>-10',
     (fwd['lis_dist'].abs() <= 3) & (fwd['target_dist'] < 30) & (fwd['neg_gex_dist'] >= -10)),
    ('force>=3 + lis<=5 + tgt<30',
     (fwd['force_score'] >= 3) & (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 30)),
    ('force>=4 + lis<=5',
     (fwd['force_score'] >= 4) & (fwd['lis_dist'].abs() <= 5)),
]

for name, mask in combos3:
    sub = fwd[mask]
    n = len(sub)
    if n < 3:
        continue
    line = f"  {name:<38s}"
    for sl, t in sl_t_combos:
        _, _, _, wr, pnl = sim_slt(sub, sl, t)
        line += f" {wr:3.0f}%/{pnl:+4.0f}"
    line += f" {n:5d}"
    print(line)

# =====================================================================
# PART 4: -GEX below distance analysis on forward data
# =====================================================================
print("\n" + "=" * 80)
print("PART 4: -GEX DISTANCE BELOW SPOT vs FORWARD OUTCOME (60min)")
print("=" * 80)

below = fwd[fwd['neg_gex_dist'] < 0].copy()
below['neg_gex_below'] = -below['neg_gex_dist']

bins = [(0, 10), (10, 20), (20, 40), (40, 80)]
print(f"\n{'Bucket':<25s} {'N':>5s} {'AvgMFE':>7s} {'AvgMAE':>7s} {'AvgFin':>7s}  SL8/T10  SL15/T20")
print("-" * 85)
for lo, hi in bins:
    sub = below[(below['neg_gex_below'] >= lo) & (below['neg_gex_below'] < hi)]
    n = len(sub)
    if n == 0:
        continue
    avg_mfe = sub['mfe'].mean()
    avg_mae = sub['mae'].mean()
    avg_final = sub['final'].mean()
    _, _, _, wr1, pnl1 = sim_slt(sub, 8, 10)
    _, _, _, wr2, pnl2 = sim_slt(sub, 15, 20)
    print(f"  -GEX [{lo:3d}-{hi:3d}) below  {n:5d} {avg_mfe:+6.1f} {avg_mae:+7.1f} {avg_final:+6.1f}  "
          f"{wr1:3.0f}%/{pnl1:+4.0f}   {wr2:3.0f}%/{pnl2:+5.0f}")

above = fwd[fwd['neg_gex_dist'] >= 0].copy()
print(f"\n  -GEX ABOVE spot       {len(above):5d} {above['mfe'].mean():+6.1f} {above['mae'].mean():+7.1f} "
      f"{above['final'].mean():+6.1f}")

# =====================================================================
# SUMMARY TABLE
# =====================================================================
print("\n" + "=" * 80)
print("FINAL RANKED TABLE: Best entries for GEX Long")
print("=" * 80)
print("Criteria: N >= 15, PnL > 0, sorted by PnL")
print()

# Collect all combos
all_results = []

all_combos = {
    'baseline': pd.Series(True, index=fwd.index),
    'force>=3': fwd['force_score'] >= 3,
    'force>=4': fwd['force_score'] >= 4,
    'force>=5': fwd['force_score'] >= 5,
    'lis<=5': fwd['lis_dist'].abs() <= 5,
    'lis<=3': fwd['lis_dist'].abs() <= 3,
    'tgt<30': fwd['target_dist'] < 30,
    'tgt<50': fwd['target_dist'] < 50,
    '+gex<50': fwd['pos_gex_dist'] < 50,
    '-gex>-15': fwd['neg_gex_dist'] >= -15,
    '-gex>-10': fwd['neg_gex_dist'] >= -10,
    '-gex>0': fwd['neg_gex_dist'] > 0,
    'lis<=5 + tgt<50': (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 50),
    'lis<=5 + tgt<30': (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 30),
    'lis<=5 + +gex<50': (fwd['lis_dist'].abs() <= 5) & (fwd['pos_gex_dist'] < 50),
    'lis<=5 + -gex>-15': (fwd['lis_dist'].abs() <= 5) & (fwd['neg_gex_dist'] >= -15),
    'lis<=5 + -gex>0': (fwd['lis_dist'].abs() <= 5) & (fwd['neg_gex_dist'] > 0),
    'tgt<50 + +gex<50': (fwd['target_dist'] < 50) & (fwd['pos_gex_dist'] < 50),
    'tgt<30 + +gex<50': (fwd['target_dist'] < 30) & (fwd['pos_gex_dist'] < 50),
    '-gex>0 + lis<=5': (fwd['neg_gex_dist'] > 0) & (fwd['lis_dist'].abs() <= 5),
    'lis<=5 + tgt<50 + +gex<50':
        (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 50) & (fwd['pos_gex_dist'] < 50),
    'lis<=5 + tgt<30 + -gex>-15':
        (fwd['lis_dist'].abs() <= 5) & (fwd['target_dist'] < 30) & (fwd['neg_gex_dist'] >= -15),
    'lis<=5 + -gex>0 + tgt<50':
        (fwd['lis_dist'].abs() <= 5) & (fwd['neg_gex_dist'] > 0) & (fwd['target_dist'] < 50),
}

for sl, t in sl_t_combos:
    for name, mask in all_combos.items():
        sub = fwd[mask]
        n = len(sub)
        if n < 5:
            continue
        w, l, f, wr, pnl = sim_slt(sub, sl, t)
        all_results.append({
            'filter': name, 'SL': sl, 'T': t, 'N': n,
            'W': w, 'L': l, 'F': f, 'WR': wr, 'PnL': pnl,
            'PnL_per_trade': pnl / n
        })

rdf = pd.DataFrame(all_results)
# Show best at each SL/T
for sl, t in [(8, 10), (10, 15), (15, 20)]:
    sub = rdf[(rdf['SL'] == sl) & (rdf['T'] == t) & (rdf['PnL'] > 0) & (rdf['N'] >= 15)]
    if len(sub) == 0:
        sub = rdf[(rdf['SL'] == sl) & (rdf['T'] == t) & (rdf['N'] >= 10)].nlargest(5, 'PnL')
    else:
        sub = sub.nlargest(10, 'PnL')
    print(f"\n--- Best at SL={sl}/T={t} (N>=15, PnL>0) ---")
    print(f"  {'Filter':<35s} {'N':>4s} {'W':>3s} {'L':>3s} {'F':>3s} {'WR%':>5s} {'PnL':>6s} {'$/trade':>7s}")
    for _, r in sub.iterrows():
        print(f"  {r['filter']:<35s} {r['N']:4.0f} {r['W']:3.0f} {r['L']:3.0f} {r['F']:3.0f} "
              f"{r['WR']:4.1f}% {r['PnL']:+5.0f} {r['PnL_per_trade']:+6.2f}")
