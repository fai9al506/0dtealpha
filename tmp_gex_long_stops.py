"""
Supplemental: Test different stop/target combos for GEX Long using forward price data.
Uses ES delta bars for 1-min resolution price data.
"""
import sqlalchemy as sa
import pandas as pd
import numpy as np
from datetime import timedelta

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = sa.create_engine(DB_URL)

# Get all GEX Long trades
with engine.connect() as conn:
    q = sa.text("""
    SELECT id, ts, spot, lis, max_minus_gex, gap_to_lis, upside,
           outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
           greek_alignment, grade
    FROM setup_log WHERE setup_name = 'GEX Long'
    ORDER BY ts
    """)
    df = pd.read_sql(q, conn)

df['ts_et'] = pd.to_datetime(df['ts']).dt.tz_convert('US/Eastern')
df['cluster'] = abs(df['lis'] - df['max_minus_gex'])

print("=" * 80)
print("STOP/TARGET SENSITIVITY ANALYSIS FOR GEX LONG")
print("=" * 80)

# For each trade, simulate with different stop/target combos
# Use ES delta bars (1-min) for forward price
combos = [
    (8, 10, 'Current: SL=8, T=10'),
    (10, 10, 'SL=10, T=10'),
    (12, 10, 'SL=12, T=10'),
    (15, 10, 'SL=15, T=10'),
    (8, 15, 'SL=8, T=15'),
    (10, 15, 'SL=10, T=15'),
    (12, 15, 'SL=12, T=15'),
    (15, 15, 'SL=15, T=15'),
    (10, 20, 'SL=10, T=20'),
    (15, 20, 'SL=15, T=20'),
    (20, 20, 'SL=20, T=20'),
    (8, 8, 'SL=8, T=8'),
    (5, 5, 'SL=5, T=5'),
    (5, 10, 'SL=5, T=10'),
]

# Build forward price series for each trade using es_delta_bars
trade_fwd = {}
with engine.connect() as conn:
    for _, r in df.iterrows():
        trade_ts = pd.Timestamp(r['ts'])
        q = sa.text("""
        SELECT ts, bar_open_price, bar_high_price, bar_low_price, bar_close_price
        FROM es_delta_bars
        WHERE ts > :start AND ts <= :end
        ORDER BY ts
        LIMIT 200
        """)
        bars = pd.read_sql(q, conn, params={'start': str(trade_ts), 'end': str(trade_ts + timedelta(hours=3))})
        if len(bars) == 0:
            continue
        bars['ts'] = pd.to_datetime(bars['ts'])
        es_entry = bars.iloc[0]['bar_open_price']
        trade_fwd[r['id']] = (es_entry, bars)

print(f"\nTrades with ES forward data: {len(trade_fwd)}/{len(df)}")

# Now simulate
results_all = {}
results_aligned = {}  # align >= 1 only

for sl, tgt, label in combos:
    wins = 0
    losses = 0
    expired = 0
    total_pnl = 0
    wins_a = 0
    losses_a = 0
    expired_a = 0
    total_pnl_a = 0

    for _, r in df.iterrows():
        if r['id'] not in trade_fwd:
            continue
        es_entry, bars = trade_fwd[r['id']]
        aligned = r['greek_alignment'] >= 1 if pd.notna(r['greek_alignment']) else False

        sim_result = None
        sim_pnl = 0
        for _, bar in bars.iterrows():
            # Check high for target (long)
            if bar['bar_high_price'] - es_entry >= tgt:
                sim_result = 'WIN'
                sim_pnl = tgt
                break
            # Check low for stop
            if es_entry - bar['bar_low_price'] >= sl:
                sim_result = 'LOSS'
                sim_pnl = -sl
                break

        if sim_result is None:
            sim_result = 'EXPIRED'
            if len(bars) > 0:
                sim_pnl = bars.iloc[-1]['bar_close_price'] - es_entry
            else:
                sim_pnl = 0

        if sim_result == 'WIN':
            wins += 1
        elif sim_result == 'LOSS':
            losses += 1
        else:
            expired += 1
        total_pnl += sim_pnl

        if aligned:
            if sim_result == 'WIN':
                wins_a += 1
            elif sim_result == 'LOSS':
                losses_a += 1
            else:
                expired_a += 1
            total_pnl_a += sim_pnl

    results_all[label] = (wins, losses, expired, total_pnl)
    results_aligned[label] = (wins_a, losses_a, expired_a, total_pnl_a)

# Print results
print(f"\n--- ALL TRADES ---")
print(f"{'Combo':>25} {'W':>4} {'L':>4} {'E':>4} {'WR%':>6} {'PnL':>8} {'AvgPnL':>8} {'PF':>6}")
for label in [c[2] for c in combos]:
    w, l, e, pnl = results_all[label]
    wr = w / (w + l) * 100 if (w + l) > 0 else 0
    avg = pnl / (w + l + e) if (w + l + e) > 0 else 0
    sl_val = int(label.split(',')[0].split('=')[1])
    tgt_val = int(label.split(',')[1].strip().split('=')[1])
    win_pnl = w * tgt_val
    loss_pnl = l * sl_val
    pf = win_pnl / loss_pnl if loss_pnl > 0 else float('inf')
    print(f"{label:>25} {w:>4} {l:>4} {e:>4} {wr:>6.1f} {pnl:>8.1f} {avg:>8.1f} {pf:>6.2f}")

print(f"\n--- ALIGNED ONLY (align >= 1) ---")
print(f"{'Combo':>25} {'W':>4} {'L':>4} {'E':>4} {'WR%':>6} {'PnL':>8} {'AvgPnL':>8} {'PF':>6}")
for label in [c[2] for c in combos]:
    w, l, e, pnl = results_aligned[label]
    wr = w / (w + l) * 100 if (w + l) > 0 else 0
    avg = pnl / (w + l + e) if (w + l + e) > 0 else 0
    sl_val = int(label.split(',')[0].split('=')[1])
    tgt_val = int(label.split(',')[1].strip().split('=')[1])
    win_pnl = w * tgt_val
    loss_pnl = l * sl_val
    pf = win_pnl / loss_pnl if loss_pnl > 0 else float('inf')
    print(f"{label:>25} {w:>4} {l:>4} {e:>4} {wr:>6.1f} {pnl:>8.1f} {avg:>8.1f} {pf:>6.2f}")

# Also test: aligned + gap <= 5 with different stops
print(f"\n--- ALIGNED + GAP <= 5 ---")
print(f"{'Combo':>25} {'W':>4} {'L':>4} {'E':>4} {'WR%':>6} {'PnL':>8} {'PF':>6}")
for sl, tgt, label in combos:
    w = 0
    l = 0
    e = 0
    pnl = 0
    for _, r in df.iterrows():
        if r['id'] not in trade_fwd:
            continue
        if not (r['gap_to_lis'] <= 5 and pd.notna(r['greek_alignment']) and r['greek_alignment'] >= 1):
            continue
        es_entry, bars = trade_fwd[r['id']]
        sim_result = None
        for _, bar in bars.iterrows():
            if bar['bar_high_price'] - es_entry >= tgt:
                sim_result = 'WIN'
                break
            if es_entry - bar['bar_low_price'] >= sl:
                sim_result = 'LOSS'
                break
        if sim_result == 'WIN':
            w += 1
            pnl += tgt
        elif sim_result == 'LOSS':
            l += 1
            pnl -= sl
        else:
            e += 1
            if len(bars) > 0:
                pnl += bars.iloc[-1]['bar_close_price'] - es_entry
    wr = w / (w + l) * 100 if (w + l) > 0 else 0
    win_pnl = w * tgt
    loss_pnl = l * sl
    pf = win_pnl / loss_pnl if loss_pnl > 0 else float('inf')
    print(f"{label:>25} {w:>4} {l:>4} {e:>4} {wr:>6.1f} {pnl:>8.1f} {pf:>6.2f}")

# Final: aligned + gap <= 5 + cluster <= 10
print(f"\n--- ALIGNED + GAP <= 5 + CLUSTER <= 10 ---")
print(f"{'Combo':>25} {'W':>4} {'L':>4} {'E':>4} {'WR%':>6} {'PnL':>8} {'PF':>6}")
for sl, tgt, label in combos:
    w = 0
    l = 0
    e = 0
    pnl = 0
    for _, r in df.iterrows():
        if r['id'] not in trade_fwd:
            continue
        if not (r['gap_to_lis'] <= 5 and pd.notna(r['greek_alignment']) and r['greek_alignment'] >= 1 and r['cluster'] <= 10):
            continue
        es_entry, bars = trade_fwd[r['id']]
        sim_result = None
        for _, bar in bars.iterrows():
            if bar['bar_high_price'] - es_entry >= tgt:
                sim_result = 'WIN'
                break
            if es_entry - bar['bar_low_price'] >= sl:
                sim_result = 'LOSS'
                break
        if sim_result == 'WIN':
            w += 1
            pnl += tgt
        elif sim_result == 'LOSS':
            l += 1
            pnl -= sl
        else:
            e += 1
            if len(bars) > 0:
                pnl += bars.iloc[-1]['bar_close_price'] - es_entry
    wr = w / (w + l) * 100 if (w + l) > 0 else 0
    win_pnl = w * tgt
    loss_pnl = l * sl
    pf = win_pnl / loss_pnl if loss_pnl > 0 else float('inf')
    print(f"{label:>25} {w:>4} {l:>4} {e:>4} {wr:>6.1f} {pnl:>8.1f} {pf:>6.2f}")
