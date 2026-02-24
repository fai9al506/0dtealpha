"""DD Exhaustion trade analysis — 49 trades from dd_exhaustion_analysis.csv"""

import pandas as pd
import numpy as np
import re
from datetime import datetime

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 200)
pd.set_option('display.float_format', '{:.1f}'.format)

# ── Load data ──────────────────────────────────────────────────────────────
df = pd.read_csv(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\dd_exhaustion_analysis.csv")

# Parse timestamps (UTC) and convert to ET
df['ts_utc'] = pd.to_datetime(df['ts'], utc=True)
df['ts_et'] = df['ts_utc'].dt.tz_convert('US/Eastern')
df['hour_et'] = df['ts_et'].dt.hour
df['date_et'] = df['ts_et'].dt.date

# Parse vol_dd_hedging (e.g. "$910,313,076" or "$-1,431,586,368")
def parse_dollar(val):
    if pd.isna(val):
        return np.nan
    s = str(val).replace('$', '').replace(',', '').strip()
    try:
        return float(s)
    except:
        return np.nan

df['dd_hedging_num'] = df['vol_dd_hedging'].apply(parse_dollar)
df['charm_num'] = df['vol_charm'].apply(parse_dollar)

# Outcome helpers
df['is_win'] = df['outcome'] == 'WIN'
df['is_loss'] = df['outcome'] == 'LOSS'
df['is_expired'] = df['outcome'] == 'EXPIRED'

print("=" * 100)
print(f"DD EXHAUSTION ANALYSIS — {len(df)} trades")
print(f"Date range: {df['ts_et'].min().strftime('%Y-%m-%d')} to {df['ts_et'].max().strftime('%Y-%m-%d')}")
print(f"Overall: {df['is_win'].sum()} W / {df['is_loss'].sum()} L / {df['is_expired'].sum()} EXP  |  "
      f"WR {df['is_win'].mean()*100:.1f}%  |  Net P&L: {df['pnl'].sum():+.1f} pts")
print("=" * 100)

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 1: Time of Day (ET) Breakdown
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 1: TIME OF DAY (ET) BREAKDOWN")
print("=" * 100)

# Bucket by hour
bins = [10, 11, 12, 13, 14, 15, 16]
labels = ['10:00-11:00', '11:00-12:00', '12:00-13:00', '13:00-14:00', '14:00-15:00', '15:00-16:00']
df['hour_bucket'] = pd.cut(df['hour_et'], bins=bins, labels=labels, right=False)

time_agg = df.groupby('hour_bucket', observed=False).agg(
    trades=('pnl', 'count'),
    wins=('is_win', 'sum'),
    losses=('is_loss', 'sum'),
    expired=('is_expired', 'sum'),
    total_pnl=('pnl', 'sum'),
).reset_index()
time_agg['wr'] = (time_agg['wins'] / time_agg['trades'] * 100).round(1)
time_agg['avg_pnl'] = (time_agg['total_pnl'] / time_agg['trades']).round(2)
time_agg['total_pnl'] = time_agg['total_pnl'].round(1)

print(time_agg.to_string(index=False))

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 2: DD Shift Magnitude Breakdown
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 2: DD SHIFT MAGNITUDE BREAKDOWN (from dd_shift_score)")
print("=" * 100)
print("Score mapping: 10=200-500M, 15=3B+, 20=500M-1B, 25=2-3B, 30=1-2B, 0=LOG(no score)")

# Map shift score to bucket
shift_map = {
    0: 'LOG (no score)',
    10: '$200M-500M',
    15: '$3B+',
    20: '$500M-1B',
    25: '$2B-3B',
    30: '$1B-2B',
}
df['shift_bucket'] = df['dd_shift_score'].map(shift_map).fillna('Unknown')

# Order for display
shift_order = ['LOG (no score)', '$200M-500M', '$500M-1B', '$1B-2B', '$2B-3B', '$3B+']
df['shift_bucket'] = pd.Categorical(df['shift_bucket'], categories=shift_order, ordered=True)

shift_agg = df.groupby('shift_bucket', observed=False).agg(
    trades=('pnl', 'count'),
    wins=('is_win', 'sum'),
    losses=('is_loss', 'sum'),
    expired=('is_expired', 'sum'),
    total_pnl=('pnl', 'sum'),
).reset_index()
shift_agg['wr'] = (shift_agg['wins'] / shift_agg['trades'] * 100).round(1)
shift_agg['avg_pnl'] = (shift_agg['total_pnl'] / shift_agg['trades']).round(2)
shift_agg['total_pnl'] = shift_agg['total_pnl'].round(1)

print(shift_agg.to_string(index=False))

# Also show raw absolute DD hedging values by shift bucket
print("\n  Raw |DD Hedging| by shift bucket:")
for bucket in shift_order:
    subset = df[df['shift_bucket'] == bucket]
    if len(subset) > 0:
        vals = subset['dd_hedging_num'].abs().dropna()
        if len(vals) > 0:
            print(f"    {bucket:20s}: mean=${vals.mean()/1e6:,.0f}M  median=${vals.median()/1e6:,.0f}M  "
                  f"min=${vals.min()/1e6:,.0f}M  max=${vals.max()/1e6:,.0f}M")

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 3: Charm Magnitude at Signal Time
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 3: CHARM MAGNITUDE AT SIGNAL TIME")
print("=" * 100)

df['abs_charm'] = df['charm_num'].abs()
charm_bins = [0, 20e6, 50e6, 100e6, 250e6, float('inf')]
charm_labels = ['<$20M', '$20-50M', '$50-100M', '$100-250M', '$250M+']
df['charm_bucket'] = pd.cut(df['abs_charm'], bins=charm_bins, labels=charm_labels, right=False)

# Count NaN charm
nan_charm = df['charm_num'].isna().sum()
print(f"  Note: {nan_charm} trades have no charm value (NaN)")
print()

charm_df = df.dropna(subset=['charm_num'])
charm_agg = charm_df.groupby('charm_bucket', observed=False).agg(
    trades=('pnl', 'count'),
    wins=('is_win', 'sum'),
    losses=('is_loss', 'sum'),
    total_pnl=('pnl', 'sum'),
).reset_index()
charm_agg['wr'] = (charm_agg['wins'] / charm_agg['trades'].replace(0, np.nan) * 100).round(1)
charm_agg['avg_pnl'] = (charm_agg['total_pnl'] / charm_agg['trades'].replace(0, np.nan)).round(2)
charm_agg['total_pnl'] = charm_agg['total_pnl'].round(1)

print(charm_agg.to_string(index=False))

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 4: Win vs Loss Pattern Comparison
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 4: WIN vs LOSS PATTERN COMPARISON")
print("=" * 100)

for label, mask in [('WINS', df['is_win']), ('LOSSES', df['is_loss']), ('EXPIRED', df['is_expired'])]:
    sub = df[mask]
    if len(sub) == 0:
        continue
    print(f"\n  {label} ({len(sub)} trades):")
    print(f"    Avg |DD Hedging|:   ${sub['dd_hedging_num'].abs().mean()/1e6:,.0f}M")
    print(f"    Avg |Charm|:        ${sub['charm_num'].abs().mean()/1e6:,.0f}M")
    avg_hour = sub['ts_et'].dt.hour.mean() + sub['ts_et'].dt.minute.mean()/60
    h = int(avg_hour)
    m = int((avg_hour - h) * 60)
    print(f"    Avg time (ET):      {h}:{m:02d}")
    print(f"    Direction split:    {(sub['direction']=='long').sum()}L / {(sub['direction']=='short').sum()}S")
    paradigm_counts = sub['paradigm'].value_counts()
    print(f"    Top paradigm:       {paradigm_counts.index[0]} ({paradigm_counts.iloc[0]})")
    print(f"    Avg max_profit:     {sub['max_profit'].mean():.1f} pts")
    print(f"    Avg max_loss:       {sub['max_loss'].dropna().mean():.1f} pts" if sub['max_loss'].notna().any() else "    Avg max_loss:       N/A")
    print(f"    Avg elapsed_min:    {sub['elapsed_min'].dropna().mean():.0f} min")
    print(f"    Avg P&L:            {sub['pnl'].mean():+.1f} pts")
    print(f"    Avg score:          {sub['score'].mean():.1f}")
    print(f"    Avg dd_shift_score: {sub['dd_shift_score'].mean():.1f}")

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 5: Paradigm Deep Dive
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 5: PARADIGM DEEP DIVE")
print("=" * 100)

# Overall by paradigm
print("\n--- Overall by Paradigm ---")
para_agg = df.groupby('paradigm').agg(
    trades=('pnl', 'count'),
    wins=('is_win', 'sum'),
    losses=('is_loss', 'sum'),
    expired=('is_expired', 'sum'),
    total_pnl=('pnl', 'sum'),
    longs=('direction', lambda x: (x == 'long').sum()),
    shorts=('direction', lambda x: (x == 'short').sum()),
).reset_index()
para_agg['wr'] = (para_agg['wins'] / para_agg['trades'] * 100).round(1)
para_agg['avg_pnl'] = (para_agg['total_pnl'] / para_agg['trades']).round(2)
para_agg['total_pnl'] = para_agg['total_pnl'].round(1)
para_agg = para_agg.sort_values('total_pnl', ascending=False)
print(para_agg.to_string(index=False))

# By paradigm + direction
print("\n--- Paradigm + Direction Combos ---")
combo_agg = df.groupby(['paradigm', 'direction']).agg(
    trades=('pnl', 'count'),
    wins=('is_win', 'sum'),
    losses=('is_loss', 'sum'),
    total_pnl=('pnl', 'sum'),
).reset_index()
combo_agg['wr'] = (combo_agg['wins'] / combo_agg['trades'] * 100).round(1)
combo_agg['avg_pnl'] = (combo_agg['total_pnl'] / combo_agg['trades']).round(2)
combo_agg['total_pnl'] = combo_agg['total_pnl'].round(1)
combo_agg = combo_agg.sort_values('total_pnl', ascending=False)
print(combo_agg.to_string(index=False))

print("\n  Most profitable combo:  ", f"{combo_agg.iloc[0]['paradigm']} {combo_agg.iloc[0]['direction']} "
      f"({combo_agg.iloc[0]['trades']:.0f} trades, {combo_agg.iloc[0]['total_pnl']:+.1f} pts, "
      f"{combo_agg.iloc[0]['wr']:.0f}% WR)")
print("  Least profitable combo: ", f"{combo_agg.iloc[-1]['paradigm']} {combo_agg.iloc[-1]['direction']} "
      f"({combo_agg.iloc[-1]['trades']:.0f} trades, {combo_agg.iloc[-1]['total_pnl']:+.1f} pts, "
      f"{combo_agg.iloc[-1]['wr']:.0f}% WR)")

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 6: Consecutive Trade Analysis (by date)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 6: CONSECUTIVE TRADE ANALYSIS (by date)")
print("=" * 100)

date_agg = df.groupby('date_et').agg(
    trades=('pnl', 'count'),
    first_time=('ts_et', lambda x: x.min().strftime('%H:%M')),
    last_time=('ts_et', lambda x: x.max().strftime('%H:%M')),
    wins=('is_win', 'sum'),
    losses=('is_loss', 'sum'),
    expired=('is_expired', 'sum'),
    net_pnl=('pnl', 'sum'),
).reset_index()
date_agg['net_pnl'] = date_agg['net_pnl'].round(1)
date_agg['wr'] = (date_agg['wins'] / date_agg['trades'] * 100).round(1)
print(date_agg.to_string(index=False))

# Clustering analysis: signals within 30 min of each other
print("\n--- Clustering (signals within 30 min of previous) ---")
df_sorted = df.sort_values('ts_et')
df_sorted['prev_ts'] = df_sorted['ts_et'].shift(1)
df_sorted['prev_date'] = df_sorted['date_et'].shift(1)
df_sorted['gap_min'] = (df_sorted['ts_et'] - df_sorted['prev_ts']).dt.total_seconds() / 60
# Only same-day gaps
df_sorted.loc[df_sorted['date_et'] != df_sorted['prev_date'], 'gap_min'] = np.nan

close_signals = df_sorted[df_sorted['gap_min'] <= 30].copy()
print(f"  {len(close_signals)} signals fired within 30 min of the previous signal (same day)")
print(f"  Average gap: {close_signals['gap_min'].mean():.1f} min")
print(f"  These clustered signals: {close_signals['is_win'].sum()} W / {close_signals['is_loss'].sum()} L / "
      f"{close_signals['is_expired'].sum()} EXP  |  P&L: {close_signals['pnl'].sum():+.1f} pts  |  "
      f"WR: {close_signals['is_win'].mean()*100:.1f}%")

non_clustered = df_sorted[df_sorted['gap_min'].isna() | (df_sorted['gap_min'] > 30)]
print(f"  Non-clustered (>30 min gap or first of day): {len(non_clustered)} trades  |  "
      f"{non_clustered['is_win'].sum()} W / {non_clustered['is_loss'].sum()} L / "
      f"{non_clustered['is_expired'].sum()} EXP  |  P&L: {non_clustered['pnl'].sum():+.1f} pts  |  "
      f"WR: {non_clustered['is_win'].mean()*100:.1f}%")

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 7: Max Profit on Losers
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 7: MAX PROFIT ON LOSERS")
print("=" * 100)

losers = df[df['is_loss']].copy()
print(f"\n  Total LOSS trades: {len(losers)}")
print(f"  Max profit distribution on losers:")
print(f"    max_profit == 0:    {(losers['max_profit'] == 0).sum()} trades (never went green)")
print(f"    max_profit > 0:     {(losers['max_profit'] > 0).sum()} trades")
print(f"    max_profit > 5:     {(losers['max_profit'] > 5).sum()} trades")
print(f"    max_profit > 10:    {(losers['max_profit'] > 10).sum()} trades")
print(f"    max_profit > 15:    {(losers['max_profit'] > 15).sum()} trades")

print(f"\n  Losers with max_profit > 5 (setup was right, exit wrong):")
promising = losers[losers['max_profit'] > 5].sort_values('max_profit', ascending=False)
if len(promising) > 0:
    for _, r in promising.iterrows():
        print(f"    #{r['trade_num']:2d} | {r['ts_et'].strftime('%m/%d %H:%M')} ET | {r['direction']:5s} | "
              f"max_profit={r['max_profit']:5.1f} | pnl={r['pnl']:+6.1f} | {r['paradigm']} | score={r['score']:.0f}")
else:
    print("    None")

print(f"\n  Losers that never went green (max_profit == 0):")
never_green = losers[losers['max_profit'] == 0].sort_values('ts_et')
for _, r in never_green.iterrows():
    print(f"    #{r['trade_num']:2d} | {r['ts_et'].strftime('%m/%d %H:%M')} ET | {r['direction']:5s} | "
          f"pnl={r['pnl']:+6.1f} | {r['paradigm']} | score={r['score']:.0f} | dd_shift_score={r['dd_shift_score']}")

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 8: Feb 19 Afternoon Pattern
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 8: FEB 19 AFTERNOON PATTERN (trades 12-17)")
print("=" * 100)

feb19 = df[df['date_et'] == pd.Timestamp('2026-02-19').date()].sort_values('ts_et')
print(f"\n  All Feb 19 trades ({len(feb19)} total):")
print(f"  {'#':>3s} | {'Time ET':>8s} | {'Dir':>5s} | {'Paradigm':>18s} | {'Score':>5s} | {'Outcome':>7s} | {'P&L':>7s} | {'Max Profit':>10s} | {'Elapsed':>7s}")
print("  " + "-" * 95)
for _, r in feb19.iterrows():
    elapsed = f"{r['elapsed_min']:.0f}m" if pd.notna(r['elapsed_min']) else "N/A"
    print(f"  {r['trade_num']:3d} | {r['ts_et'].strftime('%H:%M'):>8s} | {r['direction']:>5s} | {r['paradigm']:>18s} | "
          f"{r['score']:5.0f} | {r['outcome']:>7s} | {r['pnl']:+7.1f} | {r['max_profit']:10.1f} | {elapsed:>7s}")

feb19_afternoon = feb19[feb19['hour_et'] >= 12]
print(f"\n  Feb 19 afternoon (12:00+ ET): {len(feb19_afternoon)} trades")
print(f"  All BOFA-PURE short: {(feb19_afternoon['paradigm'] == 'BOFA-PURE').all() and (feb19_afternoon['direction'] == 'short').all()}")
print(f"  Net P&L: {feb19_afternoon['pnl'].sum():+.1f} pts")
print(f"  Pattern: Repeated short entries on BOFA-PURE after the morning move already played out")
print(f"  Observation: Only trade 13 (17:41 / 12:41 ET) won — the others expired or stopped out")
print(f"  This suggests the DD signal kept re-firing on the same BOFA-PURE paradigm without price following through")

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 9: Short vs Long by Time Period
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 9: SHORT vs LONG BY TIME PERIOD")
print("=" * 100)

period_bins = [10, 12, 14, 16]
period_labels = ['Morning (10-12)', 'Midday (12-14)', 'Afternoon (14-16)']
df['period'] = pd.cut(df['hour_et'], bins=period_bins, labels=period_labels, right=False)

for period in period_labels:
    sub = df[df['period'] == period]
    if len(sub) == 0:
        print(f"\n  {period}: No trades")
        continue
    print(f"\n  {period} ({len(sub)} trades total):")
    for direction in ['long', 'short']:
        d = sub[sub['direction'] == direction]
        if len(d) == 0:
            print(f"    {direction:5s}: 0 trades")
            continue
        wr = d['is_win'].mean() * 100
        print(f"    {direction:5s}: {len(d):2d} trades | {d['is_win'].sum()}W/{d['is_loss'].sum()}L/{d['is_expired'].sum()}E | "
              f"WR {wr:5.1f}% | P&L {d['pnl'].sum():+7.1f} | Avg {d['pnl'].mean():+5.1f}")

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS 10: Score vs Outcome
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("ANALYSIS 10: SCORE vs OUTCOME")
print("=" * 100)

score_bins = [0, 41, 56, 71, 86, 101]
score_labels = ['0-40', '41-55', '56-70', '71-85', '86-100']
df['score_bucket'] = pd.cut(df['score'], bins=score_bins, labels=score_labels, right=False)

score_agg = df.groupby('score_bucket', observed=False).agg(
    trades=('pnl', 'count'),
    wins=('is_win', 'sum'),
    losses=('is_loss', 'sum'),
    expired=('is_expired', 'sum'),
    total_pnl=('pnl', 'sum'),
    avg_max_profit=('max_profit', 'mean'),
).reset_index()
score_agg['wr'] = (score_agg['wins'] / score_agg['trades'].replace(0, np.nan) * 100).round(1)
score_agg['avg_pnl'] = (score_agg['total_pnl'] / score_agg['trades'].replace(0, np.nan)).round(2)
score_agg['total_pnl'] = score_agg['total_pnl'].round(1)
score_agg['avg_max_profit'] = score_agg['avg_max_profit'].round(1)

print(score_agg.to_string(index=False))

# Check if LOG-only (score=0) trades from early days skew results
log_trades = df[df['score'] == 0]
scored_trades = df[df['score'] > 0]
print(f"\n  LOG-only (score=0): {len(log_trades)} trades, WR {log_trades['is_win'].mean()*100:.1f}%, "
      f"P&L {log_trades['pnl'].sum():+.1f}")
print(f"  Scored (score>0):   {len(scored_trades)} trades, WR {scored_trades['is_win'].mean()*100:.1f}%, "
      f"P&L {scored_trades['pnl'].sum():+.1f}")

# Score correlation
print(f"\n  Score-PnL correlation (scored trades only): {scored_trades['score'].corr(scored_trades['pnl']):.3f}")
print(f"  Score-Win correlation (scored trades only):  {scored_trades['score'].corr(scored_trades['is_win'].astype(float)):.3f}")

# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY & KEY TAKEAWAYS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("KEY TAKEAWAYS")
print("=" * 100)

# Best time
best_time = time_agg.loc[time_agg['avg_pnl'].idxmax()]
worst_time = time_agg.loc[time_agg['trades'] > 0].loc[time_agg['avg_pnl'].idxmin()]
print(f"\n  Best time bucket:    {best_time['hour_bucket']} ({best_time['avg_pnl']:+.1f} avg P&L, {best_time['wr']:.0f}% WR)")
print(f"  Worst time bucket:   {worst_time['hour_bucket']} ({worst_time['avg_pnl']:+.1f} avg P&L, {worst_time['wr']:.0f}% WR)")

# Clustering impact
print(f"\n  Clustered signals (<30min gap) WR: {close_signals['is_win'].mean()*100:.1f}%  "
      f"vs  Non-clustered WR: {non_clustered['is_win'].mean()*100:.1f}%")

# Max profit on losers
print(f"\n  {(losers['max_profit'] > 5).sum()}/{len(losers)} losers had max_profit > 5 (exit timing issue)")
print(f"  {(losers['max_profit'] == 0).sum()}/{len(losers)} losers never went green (signal was wrong)")

print("\n" + "=" * 100)
