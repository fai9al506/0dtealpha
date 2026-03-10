"""
GEX Long Historical Trade Analysis
Compare OLD filter rules vs NEW proposed rules.
Research only — no modifications to code or database.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

# ── Step 1: Explore the table columns ─────────────────────────────────
print("=" * 80)
print("STEP 1: Explore setup_log columns (first 5 GEX Long rows)")
print("=" * 80)

with engine.connect() as conn:
    # First, get all column names
    result = conn.execute(text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'setup_log'
        ORDER BY ordinal_position
    """))
    cols = result.fetchall()
    print("\nAll columns in setup_log:")
    for c in cols:
        print(f"  {c[0]:30s}  {c[1]}")

print("\n" + "=" * 80)
print("STEP 1b: First 5 GEX Long rows (all columns)")
print("=" * 80)

df_sample = pd.read_sql(text("""
    SELECT * FROM setup_log
    WHERE setup_name = 'GEX Long'
    ORDER BY ts
    LIMIT 5
"""), engine)
print(df_sample.to_string(max_colwidth=30))

# ── Step 2: Load ALL GEX Long trades ──────────────────────────────────
print("\n" + "=" * 80)
print("STEP 2: Load ALL GEX Long trades")
print("=" * 80)

df = pd.read_sql(text("""
    SELECT id, ts, setup_name, direction, grade, score,
           paradigm, spot, lis, target,
           max_plus_gex, max_minus_gex,
           gap_to_lis, upside, rr_ratio,
           first_hour, greek_alignment,
           outcome_result, outcome_pnl,
           outcome_target_level, outcome_stop_level,
           outcome_max_profit, outcome_max_loss,
           outcome_first_event, outcome_elapsed_min
    FROM setup_log
    WHERE setup_name = 'GEX Long'
    ORDER BY ts
"""), engine)

print(f"\nTotal GEX Long trades in database: {len(df)}")

# ── Step 2b: Compute derived fields ──────────────────────────────────
df["spot_above_lis"] = df["spot"] >= df["lis"]
df["spot_below_lis"] = df["spot"] < df["lis"]
df["gap_abs"] = (df["spot"] - df["lis"]).abs()
df["spot_above_minus_gex"] = df["spot"] >= df["max_minus_gex"]
df["spot_below_minus_gex"] = df["spot"] < df["max_minus_gex"]
df["dist_spot_to_minus_gex"] = df["spot"] - df["max_minus_gex"]
df["lis_minus_gex_dist"] = (df["lis"] - df["max_minus_gex"]).abs()
df["lis_minus_gex_clustered"] = df["lis_minus_gex_dist"] <= 5
df["upside_to_target"] = df["target"] - df["spot"]
df["is_win"] = df["outcome_result"] == "WIN"

# ── Step 3: Print individual trade table ──────────────────────────────
print("\n" + "=" * 80)
print("STEP 3: Individual GEX Long Trades")
print("=" * 80)

cols_display = [
    "id", "ts", "grade", "score", "spot", "lis", "gap_to_lis",
    "spot_above_lis", "max_minus_gex", "dist_spot_to_minus_gex",
    "lis_minus_gex_dist", "lis_minus_gex_clustered",
    "target", "upside", "rr_ratio",
    "greek_alignment",
    "outcome_result", "outcome_pnl"
]

# Format for readability
df_display = df[cols_display].copy()
df_display["ts"] = df_display["ts"].dt.strftime("%m/%d %H:%M")
print(df_display.to_string(index=False, max_colwidth=12))

# ── Step 4: Filter analysis ──────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 4: OLD vs NEW Filter Rules")
print("=" * 80)

# OLD rules (current code): spot > LIS, gap <= 20, upside >= 10
# These are already ALL the trades in the table since the code rejects before logging
# But let's verify
old_mask = (df["spot"] >= df["lis"]) & (df["gap_to_lis"] <= 20) & (df["upside"] >= 10)

# NEW rules: paradigm=GEX (already filtered), gap_abs <= 5, upside >= 10
# IMPORTANT: spot can be BELOW LIS now, so gap_abs = abs(spot - lis)
new_mask = (df["gap_abs"] <= 5) & (df["upside"] >= 10)

# Trades NEW would REMOVE (gap > 5 that old allowed)
removed_mask = old_mask & ~new_mask  # old=yes, new=no
# Trades NEW would ADD (spot below LIS, gap <= 5 -- but these won't be in DB since code rejects them)
# We can only note this limitation
added_mask = new_mask & ~old_mask  # new=yes, old=no

def print_stats(label, mask):
    subset = df[mask]
    total = len(subset)
    if total == 0:
        print(f"\n  {label}: 0 trades")
        return
    wins = (subset["outcome_result"] == "WIN").sum()
    losses = (subset["outcome_result"] == "LOSS").sum()
    expired = (subset["outcome_result"] == "EXPIRED").sum()
    timeout = (subset["outcome_result"] == "TIMEOUT").sum()
    pending = subset["outcome_result"].isna().sum()
    wr = wins / total * 100 if total > 0 else 0
    pnl = subset["outcome_pnl"].sum()
    avg_pnl = subset["outcome_pnl"].mean()
    avg_win = subset.loc[subset["outcome_result"] == "WIN", "outcome_pnl"].mean()
    avg_loss = subset.loc[subset["outcome_result"] == "LOSS", "outcome_pnl"].mean()

    print(f"\n  {label}:")
    print(f"    Total: {total} trades")
    print(f"    Wins: {wins}, Losses: {losses}, Expired: {expired}, Timeout: {timeout}, Pending: {pending}")
    print(f"    WR: {wr:.1f}%")
    print(f"    Total PnL: {pnl:+.1f} pts")
    print(f"    Avg PnL: {avg_pnl:+.1f} pts")
    if pd.notna(avg_win):
        print(f"    Avg Win: {avg_win:+.1f} pts")
    if pd.notna(avg_loss):
        print(f"    Avg Loss: {avg_loss:+.1f} pts")

print_stats("ALL GEX Long (entire database)", pd.Series(True, index=df.index))
print_stats("OLD rules (spot>=LIS, gap<=20, upside>=10)", old_mask)
print_stats("NEW rules (gap_abs<=5, upside>=10, either side of LIS)", new_mask)
print_stats("REMOVED by NEW (in OLD but gap>5)", removed_mask)
print_stats("ADDED by NEW (in NEW but not OLD - below LIS)", added_mask)

# ── Breakdown by gap ranges ──────────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 4b: Breakdown by Gap-to-LIS Ranges")
print("=" * 80)

for lo, hi in [(0, 2), (2, 5), (5, 10), (10, 15), (15, 20), (20, 999)]:
    mask = (df["gap_abs"] >= lo) & (df["gap_abs"] < hi)
    label = f"Gap [{lo}-{hi})" if hi < 999 else f"Gap [{lo}+)"
    print_stats(label, mask)

# ── Breakdown by LIS/-GEX clustering ─────────────────────────────────
print("\n" + "=" * 80)
print("STEP 4c: LIS/-GEX Clustering (within 5 pts)")
print("=" * 80)

print_stats("LIS and -GEX CLUSTERED (within 5 pts)", df["lis_minus_gex_clustered"])
print_stats("LIS and -GEX SPREAD (>5 pts apart)", ~df["lis_minus_gex_clustered"])

# Clustered + tight gap = A+ candidate
a_plus_mask = df["lis_minus_gex_clustered"] & (df["gap_abs"] <= 5)
print_stats("A+ candidates (clustered + gap<=5)", a_plus_mask)

# ── Breakdown by Greek alignment ──────────────────────────────────────
print("\n" + "=" * 80)
print("STEP 4d: Greek Alignment Breakdown")
print("=" * 80)

for align in sorted(df["greek_alignment"].dropna().unique()):
    mask = df["greek_alignment"] == align
    print_stats(f"Alignment = {int(align)}", mask)

# ── Step 5: Check for below-LIS signals ──────────────────────────────
print("\n" + "=" * 80)
print("STEP 5: Below-LIS Signals Check")
print("=" * 80)

below_lis = df[df["spot"] < df["lis"]]
print(f"\nGEX Long trades where spot < LIS: {len(below_lis)}")
if len(below_lis) > 0:
    print(below_lis[cols_display].to_string(index=False))
else:
    print("(None found — current code rejects spot < LIS before logging)")
    print("We cannot evaluate below-LIS performance from existing data.")
    print("Would need to either:")
    print("  1. Backtest with historical Volland+spot data, or")
    print("  2. Log all evaluations (pass and fail) for future analysis")

# ── Step 6: Combined NEW filter + alignment ───────────────────────────
print("\n" + "=" * 80)
print("STEP 6: NEW Filter + Greek Alignment Combinations")
print("=" * 80)

for min_align in [None, 0, 1, 2]:
    if min_align is None:
        align_mask = pd.Series(True, index=df.index)
        label = "no alignment filter"
    else:
        align_mask = df["greek_alignment"] >= min_align
        label = f"alignment >= {min_align}"

    combined = new_mask & align_mask
    print_stats(f"NEW rules + {label}", combined)

# ── Step 7: Spot position relative to -GEX ───────────────────────────
print("\n" + "=" * 80)
print("STEP 7: Spot Position Relative to -GEX")
print("=" * 80)

above_minus_gex = df["spot"] >= df["max_minus_gex"]
below_minus_gex = df["spot"] < df["max_minus_gex"]

print_stats("Spot ABOVE -GEX", above_minus_gex)
print_stats("Spot BELOW -GEX", below_minus_gex)
print_stats("Spot BELOW -GEX + gap<=5", below_minus_gex & (df["gap_abs"] <= 5))

# ── Step 8: Deep-dive the gap<=5 trades ───────────────────────────────
print("\n" + "=" * 80)
print("STEP 8: Detailed View of Gap<=5 Trades (NEW filter candidates)")
print("=" * 80)

tight = df[new_mask].sort_values("ts")
if len(tight) > 0:
    detail_cols = [
        "id", "ts", "grade", "score", "spot", "lis", "gap_abs",
        "max_minus_gex", "lis_minus_gex_dist", "lis_minus_gex_clustered",
        "target", "upside", "rr_ratio", "greek_alignment",
        "outcome_result", "outcome_pnl", "outcome_max_profit"
    ]
    tight_disp = tight[detail_cols].copy()
    tight_disp["ts"] = tight_disp["ts"].dt.strftime("%m/%d %H:%M")
    print(tight_disp.to_string(index=False))
else:
    print("No trades with gap <= 5")

# ── Step 9: Gap>5 trades that would be removed ───────────────────────
print("\n" + "=" * 80)
print("STEP 9: Detailed View of Gap>5 Trades (would be REMOVED by NEW filter)")
print("=" * 80)

wide = df[removed_mask].sort_values("ts")
if len(wide) > 0:
    detail_cols = [
        "id", "ts", "grade", "score", "spot", "lis", "gap_abs",
        "max_minus_gex", "lis_minus_gex_dist",
        "target", "upside", "rr_ratio", "greek_alignment",
        "outcome_result", "outcome_pnl", "outcome_max_profit"
    ]
    wide_disp = wide[detail_cols].copy()
    wide_disp["ts"] = wide_disp["ts"].dt.strftime("%m/%d %H:%M")
    print(wide_disp.to_string(index=False))
else:
    print("No trades with gap > 5")

# ── Summary ───────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

old_pnl = df.loc[old_mask, "outcome_pnl"].sum()
new_pnl = df.loc[new_mask, "outcome_pnl"].sum()
old_n = old_mask.sum()
new_n = new_mask.sum()
old_wr = (df.loc[old_mask, "outcome_result"] == "WIN").sum() / old_n * 100 if old_n > 0 else 0
new_wr = (df.loc[new_mask, "outcome_result"] == "WIN").sum() / new_n * 100 if new_n > 0 else 0

print(f"""
  OLD filter: {old_n} trades, {old_wr:.1f}% WR, {old_pnl:+.1f} pts total
  NEW filter: {new_n} trades, {new_wr:.1f}% WR, {new_pnl:+.1f} pts total
  Delta:      {new_n - old_n:+d} trades, {new_wr - old_wr:+.1f}% WR, {new_pnl - old_pnl:+.1f} pts

  KEY FINDING: Tighter gap (<=5) {'IMPROVES' if new_pnl > old_pnl else 'WORSENS'} total PnL
  NOTE: Below-LIS signals not in database (rejected by current code) — cannot evaluate here.
""")

print("Done.")
