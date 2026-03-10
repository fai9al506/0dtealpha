"""
Deep analysis of GEX Long — historical trades + missed signals from raw Volland data.
RESEARCH ONLY — no code or DB modifications.
"""
import sqlalchemy as sa
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time as dtime

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = sa.create_engine(DB_URL)

# ============================================================================
# PART 2: EXISTING 42 GEX LONG TRADES
# ============================================================================

q = """
SELECT id, ts, setup_name, direction, grade, score, paradigm, spot, lis, target,
       max_plus_gex, max_minus_gex, gap_to_lis, upside, rr_ratio,
       outcome_result, outcome_pnl, outcome_target_level, outcome_stop_level,
       outcome_max_profit, outcome_max_loss, outcome_first_event, outcome_elapsed_min,
       greek_alignment, comments
FROM setup_log WHERE setup_name = 'GEX Long'
ORDER BY ts
"""
df = pd.read_sql(q, engine)

# Computed columns
df['lis_mgex_cluster'] = abs(df['lis'] - df['max_minus_gex'])
df['spot_below_mgex'] = df['spot'] < df['max_minus_gex']
df['spot_above_mgex'] = df['spot'] >= df['max_minus_gex']
df['ts_et'] = pd.to_datetime(df['ts']).dt.tz_convert('US/Eastern')
df['hour'] = df['ts_et'].dt.hour
df['dist_from_mgex'] = df['spot'] - df['max_minus_gex']
df['abs_gap'] = abs(df['spot'] - df['lis'])

def stats_str(sub):
    n = len(sub)
    w = len(sub[sub['outcome_result'] == 'WIN'])
    l = len(sub[sub['outcome_result'] == 'LOSS'])
    wr = w / (w + l) * 100 if (w + l) > 0 else 0
    pnl = sub['outcome_pnl'].sum()
    avg = sub['outcome_pnl'].mean() if n > 0 else 0
    win_pnl = sub[sub['outcome_result'] == 'WIN']['outcome_pnl'].sum()
    loss_pnl = sub[sub['outcome_result'] == 'LOSS']['outcome_pnl'].sum()
    pf = abs(win_pnl / loss_pnl) if loss_pnl != 0 else float('inf')
    return n, w, l, wr, pnl, avg, pf

print("=" * 80)
print("PART 2: DEEP ANALYSIS OF 42 EXISTING GEX LONG TRADES")
print("=" * 80)

n, w, l, wr, pnl, avg, pf = stats_str(df)
print(f"\nOverall: {n} trades, {w}W/{l}L, WR={wr:.1f}%, PnL={pnl:.1f}, Avg={avg:.1f}, PF={pf:.2f}")

# ── 2A: Cluster Analysis ────────────────────────────────────────────────
print("\n" + "=" * 80)
print("2A: A+ CLUSTER ANALYSIS — abs(LIS - (-GEX))")
print("=" * 80)
print(f"\nLIS-to-(-GEX) distance: mean={df['lis_mgex_cluster'].mean():.1f}, median={df['lis_mgex_cluster'].median():.1f}, min={df['lis_mgex_cluster'].min():.1f}, max={df['lis_mgex_cluster'].max():.1f}")

buckets = [(0, 3, '[0-3]'), (3, 5, '[3-5]'), (5, 10, '[5-10]'), (10, 20, '[10-20]'), (20, 200, '[20+]')]
print(f"\n{'Cluster':>10} {'N':>4} {'Wins':>5} {'Losses':>6} {'WR%':>6} {'PnL':>8} {'AvgPnL':>8} {'PF':>6}")
for lo, hi, label in buckets:
    mask = (df['lis_mgex_cluster'] >= lo) & (df['lis_mgex_cluster'] < hi)
    sub = df[mask]
    if len(sub) == 0:
        print(f"{label:>10} {0:>4}")
        continue
    n, w, l, wr, pnl, avg, pf = stats_str(sub)
    print(f"{label:>10} {n:>4} {w:>5} {l:>6} {wr:>6.1f} {pnl:>8.1f} {avg:>8.1f} {pf:>6.2f}")

# Cross-reference: cluster x gap
print(f"\n--- CLUSTER x GAP_TO_LIS WR cross-tab ---")
print(f"{'Cluster':>10} | {'Gap<=5':>20} | {'Gap 5-10':>20} | {'Gap 10+':>20}")
for lo, hi, label in buckets:
    mask = (df['lis_mgex_cluster'] >= lo) & (df['lis_mgex_cluster'] < hi)
    sub = df[mask]
    parts = []
    for glo, ghi, glabel in [(0, 5.01, 'Gap<=5'), (5.01, 10.01, 'Gap 5-10'), (10.01, 100, 'Gap 10+')]:
        g = sub[(sub['gap_to_lis'] >= glo) & (sub['gap_to_lis'] < ghi)]
        if len(g) == 0:
            parts.append("-")
        else:
            gw = len(g[g['outcome_result'] == 'WIN'])
            gl = len(g[g['outcome_result'] == 'LOSS'])
            gp = g['outcome_pnl'].sum()
            parts.append(f"{gw}W/{gl}L={gp:+.0f}")
    print(f"{label:>10} | {parts[0]:>20} | {parts[1]:>20} | {parts[2]:>20}")

# ── 2B: Entry Timing ────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("2B: ENTRY TIMING (Hour of Day)")
print("=" * 80)
print(f"\n{'Hour':>6} {'N':>4} {'Wins':>5} {'Losses':>6} {'WR%':>6} {'PnL':>8} {'AvgPnL':>8}")
for h in sorted(df['hour'].unique()):
    sub = df[df['hour'] == h]
    n, w, l, wr, pnl, avg, pf = stats_str(sub)
    print(f"{h:>6} {n:>4} {w:>5} {l:>6} {wr:>6.1f} {pnl:>8.1f} {avg:>8.1f}")

# ── 2C: Spot vs -GEX ────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("2C: PRICE POSITION RELATIVE TO -GEX")
print("=" * 80)
for label, sub in [("Spot >= -GEX", df[df['spot_above_mgex']]), ("Spot < -GEX", df[df['spot_below_mgex']])]:
    n, w, l, wr, pnl, avg, pf = stats_str(sub)
    print(f"  {label}: {n} trades, {w}W/{l}L, WR={wr:.1f}%, PnL={pnl:.1f}, PF={pf:.2f}")

print(f"\n  Distance from -GEX breakdown:")
for lo, hi, label in [(-100, 0, 'Below -GEX'), (0, 10, '0-10 above'), (10, 30, '10-30 above'), (30, 100, '30+ above')]:
    mask = (df['dist_from_mgex'] >= lo) & (df['dist_from_mgex'] < hi)
    sub = df[mask]
    if len(sub) == 0:
        continue
    n, w, l, wr, pnl, avg, pf = stats_str(sub)
    print(f"    {label:>15}: {n:>3} trades, {w}W/{l}L, WR={wr:.1f}%, PnL={pnl:>7.1f}")

# ── 2D: Upside Analysis ─────────────────────────────────────────────────
print("\n" + "=" * 80)
print("2D: UPSIDE ANALYSIS")
print("=" * 80)
for lo, hi, label in [(0, 15, '[0-15]'), (15, 25, '[15-25]'), (25, 40, '[25-40]'), (40, 100, '[40+]')]:
    mask = (df['upside'] >= lo) & (df['upside'] < hi)
    sub = df[mask]
    if len(sub) == 0:
        continue
    n, w, l, wr, pnl, avg, pf = stats_str(sub)
    print(f"  Upside {label:>8}: {n:>3} trades, {w}W/{l}L, WR={wr:.1f}%, PnL={pnl:>7.1f}, Avg={avg:>6.1f}")

# ── 2E: Greek Alignment ─────────────────────────────────────────────────
print("\n" + "=" * 80)
print("2E: GREEK ALIGNMENT ANALYSIS")
print("=" * 80)
for align in sorted(df['greek_alignment'].dropna().unique()):
    sub = df[df['greek_alignment'] == align]
    n, w, l, wr, pnl, avg, pf = stats_str(sub)
    print(f"  Align={int(align):>3}: {n:>3} trades, {w}W/{l}L, WR={wr:.1f}%, PnL={pnl:>7.1f}")

# ── 2F: Grade Analysis ──────────────────────────────────────────────────
print("\n" + "=" * 80)
print("2F: GRADE ANALYSIS")
print("=" * 80)
for grade in ['A+', 'A', 'A-Entry']:
    sub = df[df['grade'] == grade]
    if len(sub) == 0:
        continue
    n, w, l, wr, pnl, avg, pf = stats_str(sub)
    print(f"  {grade:>8}: {n:>3} trades, {w}W/{l}L, WR={wr:.1f}%, PnL={pnl:>7.1f}, Avg={avg:>6.1f}, PF={pf:.2f}")

# ── 2G: Perfect A+ Filter ───────────────────────────────────────────────
print("\n" + "=" * 80)
print("2G: FILTER SENSITIVITY ANALYSIS")
print("=" * 80)

filters = [
    ("Unfiltered (all 42)", lambda d: d),
    ("Gap<=5 only", lambda d: d[d['gap_to_lis'] <= 5]),
    ("Gap<=5 + align>=1", lambda d: d[(d['gap_to_lis'] <= 5) & (d['greek_alignment'] >= 1)]),
    ("Cluster<=5 only", lambda d: d[d['lis_mgex_cluster'] <= 5]),
    ("Cluster<=5 + align>=1", lambda d: d[(d['lis_mgex_cluster'] <= 5) & (d['greek_alignment'] >= 1)]),
    ("Cluster<=10 only", lambda d: d[d['lis_mgex_cluster'] <= 10]),
    ("Gap<=5 + cluster<=10", lambda d: d[(d['gap_to_lis'] <= 5) & (d['lis_mgex_cluster'] <= 10)]),
    ("Gap<=5 + cluster<=10 + align>=1", lambda d: d[(d['gap_to_lis'] <= 5) & (d['lis_mgex_cluster'] <= 10) & (d['greek_alignment'] >= 1)]),
    ("Align>=1 only", lambda d: d[d['greek_alignment'] >= 1]),
    ("Align>=1 + upside>=20", lambda d: d[(d['greek_alignment'] >= 1) & (d['upside'] >= 20)]),
    ("Align>=1 + gap<=10", lambda d: d[(d['greek_alignment'] >= 1) & (d['gap_to_lis'] <= 10)]),
    ("A+ grade only", lambda d: d[d['grade'] == 'A+']),
    ("A+ + align>=1", lambda d: d[(d['grade'] == 'A+') & (d['greek_alignment'] >= 1)]),
    ("Gap<=5 + cluster<=5 + up>=10 + align>=1", lambda d: d[(d['gap_to_lis'] <= 5) & (d['lis_mgex_cluster'] <= 5) & (d['upside'] >= 10) & (d['greek_alignment'] >= 1)]),
    ("Gap<=5 + cluster<=10 + align>=1 + hour<13", lambda d: d[(d['gap_to_lis'] <= 5) & (d['lis_mgex_cluster'] <= 10) & (d['greek_alignment'] >= 1) & (d['hour'] < 13)]),
    ("Spot <= -GEX + align>=1", lambda d: d[(d['spot'] <= d['max_minus_gex']) & (d['greek_alignment'] >= 1)]),
    ("dist_mgex < 10 + align>=1", lambda d: d[(abs(d['dist_from_mgex']) < 10) & (d['greek_alignment'] >= 1)]),
]

print(f"\n{'Filter':>55} {'N':>4} {'W':>3} {'L':>3} {'WR%':>6} {'PnL':>8} {'AvgPnL':>8} {'PF':>6}")
for name, fn in filters:
    sub = fn(df)
    n, w, l, wr, pnl, avg, pf = stats_str(sub)
    pf_str = f"{pf:.2f}" if pf < 100 else "inf"
    print(f"{name:>55} {n:>4} {w:>3} {l:>3} {wr:>6.1f} {pnl:>8.1f} {avg:>8.1f} {pf_str:>6}")

# Show the trades that pass the best filter
print("\n--- Trades passing 'Gap<=5 + cluster<=10 + align>=1': ---")
best = df[(df['gap_to_lis'] <= 5) & (df['lis_mgex_cluster'] <= 10) & (df['greek_alignment'] >= 1)]
for _, r in best.iterrows():
    t = pd.Timestamp(r['ts']).tz_convert('US/Eastern')
    mg = r['max_minus_gex'] if pd.notna(r['max_minus_gex']) else 0
    mfe = r['outcome_max_profit'] if pd.notna(r['outcome_max_profit']) else 0
    mae = r['outcome_max_loss'] if pd.notna(r['outcome_max_loss']) else 0
    print(f"  #{r['id']:>4} {t.strftime('%m/%d %H:%M')} spot={r['spot']:.1f} lis={r['lis']:.1f} -gex={mg:.1f} gap={r['gap_to_lis']:.1f} cluster={r['lis_mgex_cluster']:.1f} align={int(r['greek_alignment'])} MFE={mfe:.1f} MAE={mae:.1f} -> {r['outcome_result']} {r['outcome_pnl']:.1f}")
