"""
VIX / OverVIX Analysis vs Setup Outcomes
=========================================
Pulls VIX + SPX daily data from yfinance, setup outcomes from Railway API,
and cross-references to see if VIX level or "overvix" (VIX - realized vol)
correlates with win rate and P&L.

Usage:
    python tmp_vix_backtest.py
"""
import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import requests
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

# ---------------------------------------------------------------------------
# 1. Fetch VIX + SPX daily data from yfinance
# ---------------------------------------------------------------------------
import yfinance as yf

# We need 20 trading days BEFORE Feb 5 for realized vol lookback
# Fetch from Jan 1 to be safe
START_FETCH = "2026-01-01"
END_FETCH   = "2026-03-15"  # day after last date to include Mar 14

# Our analysis range
ANALYSIS_START = "2026-02-05"

print("=" * 90)
print("  VIX / OverVIX Analysis vs 0DTE Setup Outcomes")
print("=" * 90)
print()

print("[1] Fetching VIX daily data from yfinance...")
vix_df = yf.download("^VIX", start=START_FETCH, end=END_FETCH, progress=False)
if vix_df.empty:
    print("ERROR: Could not fetch VIX data from yfinance.")
    sys.exit(1)
# yfinance returns MultiIndex columns when single ticker; flatten
if hasattr(vix_df.columns, 'levels') and len(vix_df.columns.levels) > 1:
    vix_df.columns = vix_df.columns.get_level_values(0)
print(f"    Got {len(vix_df)} VIX trading days ({vix_df.index[0].strftime('%Y-%m-%d')} to {vix_df.index[-1].strftime('%Y-%m-%d')})")

print("[2] Fetching SPX (^GSPC) daily data from yfinance...")
spx_df = yf.download("^GSPC", start=START_FETCH, end=END_FETCH, progress=False)
if spx_df.empty:
    print("ERROR: Could not fetch SPX data from yfinance.")
    sys.exit(1)
if hasattr(spx_df.columns, 'levels') and len(spx_df.columns.levels) > 1:
    spx_df.columns = spx_df.columns.get_level_values(0)
print(f"    Got {len(spx_df)} SPX trading days ({spx_df.index[0].strftime('%Y-%m-%d')} to {spx_df.index[-1].strftime('%Y-%m-%d')})")

# ---------------------------------------------------------------------------
# 2. Calculate realized vol (20-day rolling, annualized)
# ---------------------------------------------------------------------------
print("[3] Calculating 20-day realized volatility...")
spx_df["daily_return"] = spx_df["Close"].pct_change()
spx_df["realized_vol"] = spx_df["daily_return"].rolling(window=20).std() * np.sqrt(252) * 100  # as percentage

# Merge VIX close into SPX dataframe by date
vix_close_map = {}
for dt, row in vix_df.iterrows():
    vix_close_map[dt.strftime("%Y-%m-%d")] = float(row["Close"])

daily_data = {}
for dt, row in spx_df.iterrows():
    date_str = dt.strftime("%Y-%m-%d")
    if date_str < ANALYSIS_START:
        continue
    vix_close = vix_close_map.get(date_str)
    rv = row["realized_vol"]
    if vix_close is None or np.isnan(rv):
        continue
    overvix = vix_close - rv
    daily_data[date_str] = {
        "date": date_str,
        "spx_close": float(row["Close"]),
        "spx_high": float(row["High"]),
        "spx_low": float(row["Low"]),
        "spx_range": float(row["High"] - row["Low"]),
        "vix_close": vix_close,
        "realized_vol": float(rv),
        "overvix": float(overvix),
    }

print(f"    {len(daily_data)} analysis days with complete VIX + realized vol data")

# ---------------------------------------------------------------------------
# 3. Fetch setup outcomes from Railway API
# ---------------------------------------------------------------------------
print("[4] Fetching setup outcomes from Railway API...")
API_URL = "https://0dtealpha.com/api/debug/gex-analysis"

try:
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    api_data = resp.json()
except Exception as e:
    print(f"ERROR: Could not fetch from {API_URL}: {e}")
    sys.exit(1)

outcomes = api_data.get("setup_outcomes", [])
if not outcomes:
    print("ERROR: No setup_outcomes returned from API.")
    sys.exit(1)

print(f"    Got {len(outcomes)} setup outcomes")

# ---------------------------------------------------------------------------
# 4. Match outcomes to daily VIX data
# ---------------------------------------------------------------------------
print("[5] Matching outcomes to daily VIX/OverVIX data...")

# Group outcomes by date
outcomes_by_date = defaultdict(list)
for o in outcomes:
    outcomes_by_date[o["date"]].append(o)

matched_trades = []
unmatched_dates = set()
for date_str, day_outcomes in outcomes_by_date.items():
    if date_str in daily_data:
        dd = daily_data[date_str]
        for o in day_outcomes:
            matched_trades.append({
                **o,
                "vix_close": dd["vix_close"],
                "realized_vol": dd["realized_vol"],
                "overvix": dd["overvix"],
                "spx_close": dd["spx_close"],
                "spx_range": dd["spx_range"],
            })
    else:
        unmatched_dates.add(date_str)

print(f"    Matched {len(matched_trades)} trades across {len(daily_data)} days")
if unmatched_dates:
    print(f"    WARNING: {len(unmatched_dates)} dates had no VIX data: {sorted(unmatched_dates)[:5]}...")

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def calc_stats(trades):
    """Calculate aggregate stats for a list of trades."""
    if not trades:
        return {"count": 0, "wins": 0, "losses": 0, "wr": 0, "pnl": 0, "avg_pnl": 0, "pf": 0}
    wins = sum(1 for t in trades if t["result"] == "WIN")
    losses = sum(1 for t in trades if t["result"] == "LOSS")
    expired = sum(1 for t in trades if t["result"] == "EXPIRED")
    total_pnl = sum(t["pnl"] for t in trades)
    gross_profit = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')
    wr = wins / len(trades) * 100 if trades else 0
    return {
        "count": len(trades), "wins": wins, "losses": losses, "expired": expired,
        "wr": wr, "pnl": total_pnl, "avg_pnl": total_pnl / len(trades),
        "pf": pf,
    }

def print_stats_line(label, stats, indent=2):
    """Print a single stats line."""
    if stats["count"] == 0:
        print(f"{' '*indent}{label:<40s}   0 trades")
        return
    pf_str = f"{stats['pf']:.2f}" if stats["pf"] != float('inf') else "inf"
    print(f"{' '*indent}{label:<40s} {stats['count']:>4d}t  W:{stats['wins']:>3d}  L:{stats['losses']:>3d}  "
          f"WR:{stats['wr']:>5.1f}%  PnL:{stats['pnl']:>+8.1f}  Avg:{stats['avg_pnl']:>+5.1f}  PF:{pf_str:>5s}")

def print_bucket_detail(trades, indent=4):
    """Print per-setup breakdown within a bucket."""
    by_setup = defaultdict(list)
    for t in trades:
        by_setup[t["setup_name"]].append(t)
    for setup in sorted(by_setup.keys()):
        st = calc_stats(by_setup[setup])
        print_stats_line(f"  {setup}", st, indent)

# ---------------------------------------------------------------------------
# 5. V7+AG Filter (match our live filter)
# ---------------------------------------------------------------------------
def passes_v7ag(t):
    """Apply the V7+AG filter — same as production."""
    setup = t["setup_name"]
    dirn = t["direction"]
    align = t.get("alignment")
    if dirn in ("long", "bullish"):
        return align is not None and align >= 2
    else:
        if setup == "Skew Charm":
            return True
        elif setup == "AG Short":
            return True
        elif setup == "DD Exhaustion":
            return align is not None and align != 0
        else:
            return False

filtered_trades = [t for t in matched_trades if passes_v7ag(t)]
unfiltered_trades = matched_trades

print(f"    After V7+AG filter: {len(filtered_trades)} trades (from {len(unfiltered_trades)} total)")
print()

# ===========================================================================
# ANALYSIS SECTIONS
# ===========================================================================

# ---------------------------------------------------------------------------
# A. Per-Day Table
# ---------------------------------------------------------------------------
print("=" * 120)
print("  A. PER-DAY TABLE: Date / VIX / RV / OverVIX / Trades / WR / P&L")
print("=" * 120)
print()

header = (f"  {'Date':<12s} {'VIX':>6s} {'RV20':>6s} {'OvVIX':>6s} {'SPX':>8s} {'Range':>6s} "
          f"{'#All':>4s} {'WR%':>5s} {'PnL':>8s}  "
          f"{'#V7':>4s} {'WR%':>5s} {'PnL':>8s}")
print(header)
print("  " + "-" * 116)

# Also accumulate for summary
all_day_stats = []
for date_str in sorted(daily_data.keys()):
    dd = daily_data[date_str]
    day_all = [t for t in unfiltered_trades if t["date"] == date_str]
    day_v7 = [t for t in filtered_trades if t["date"] == date_str]

    st_all = calc_stats(day_all)
    st_v7 = calc_stats(day_v7)

    wr_all_str = f"{st_all['wr']:>5.1f}" if st_all['count'] > 0 else "  n/a"
    wr_v7_str = f"{st_v7['wr']:>5.1f}" if st_v7['count'] > 0 else "  n/a"

    print(f"  {date_str:<12s} {dd['vix_close']:>6.1f} {dd['realized_vol']:>6.1f} {dd['overvix']:>+6.1f} "
          f"{dd['spx_close']:>8.1f} {dd['spx_range']:>6.1f} "
          f"{st_all['count']:>4d} {wr_all_str} {st_all['pnl']:>+8.1f}  "
          f"{st_v7['count']:>4d} {wr_v7_str} {st_v7['pnl']:>+8.1f}")

    all_day_stats.append({
        **dd, "all_count": st_all["count"], "all_wr": st_all["wr"], "all_pnl": st_all["pnl"],
        "v7_count": st_v7["count"], "v7_wr": st_v7["wr"], "v7_pnl": st_v7["pnl"],
    })

# Totals
st_total_all = calc_stats(unfiltered_trades)
st_total_v7 = calc_stats(filtered_trades)
print("  " + "-" * 116)
print(f"  {'TOTAL':<12s} {'':>6s} {'':>6s} {'':>6s} {'':>8s} {'':>6s} "
      f"{st_total_all['count']:>4d} {st_total_all['wr']:>5.1f} {st_total_all['pnl']:>+8.1f}  "
      f"{st_total_v7['count']:>4d} {st_total_v7['wr']:>5.1f} {st_total_v7['pnl']:>+8.1f}")
print()

# ---------------------------------------------------------------------------
# B. OverVIX Bucket Analysis
# ---------------------------------------------------------------------------
print("=" * 120)
print("  B. OVERVIX BUCKET ANALYSIS (VIX - 20d Realized Vol)")
print("     OverVIX < 0 = 'undervixed' (market calmer than VIX implies)")
print("     OverVIX > 0 = 'overvixed'  (VIX elevated vs recent movement)")
print("=" * 120)
print()

overvix_buckets = [
    ("< 0   (undervixed)", lambda t: t["overvix"] < 0),
    ("0 to +4  (mildly overvixed)", lambda t: 0 <= t["overvix"] < 4),
    ("+4 to +6  (moderately overvixed)", lambda t: 4 <= t["overvix"] < 6),
    ("+6 to +8  (elevated overvix)", lambda t: 6 <= t["overvix"] < 8),
    ("+8 to +10 (high overvix)", lambda t: 8 <= t["overvix"] < 10),
    ("+10 to +13 (very high overvix)", lambda t: 10 <= t["overvix"] < 13),
    ("+13+   (extreme overvix)", lambda t: t["overvix"] >= 13),
]

print("  --- ALL TRADES (unfiltered) ---")
print()
for label, filt in overvix_buckets:
    bucket = [t for t in unfiltered_trades if filt(t)]
    st = calc_stats(bucket)
    print_stats_line(f"OverVIX {label}", st)
    if st["count"] >= 3:
        print_bucket_detail(bucket)
    print()

print("  --- V7+AG FILTERED ---")
print()
for label, filt in overvix_buckets:
    bucket = [t for t in filtered_trades if filt(t)]
    st = calc_stats(bucket)
    print_stats_line(f"OverVIX {label}", st)
    if st["count"] >= 3:
        print_bucket_detail(bucket)
    print()

# ---------------------------------------------------------------------------
# C. VIX Level Bucket Analysis (raw VIX, no realized vol adjustment)
# ---------------------------------------------------------------------------
print("=" * 120)
print("  C. RAW VIX LEVEL BUCKET ANALYSIS")
print("=" * 120)
print()

vix_buckets = [
    ("VIX < 16", lambda t: t["vix_close"] < 16),
    ("VIX 16-18", lambda t: 16 <= t["vix_close"] < 18),
    ("VIX 18-20", lambda t: 18 <= t["vix_close"] < 20),
    ("VIX 20-22", lambda t: 20 <= t["vix_close"] < 22),
    ("VIX 22-24", lambda t: 22 <= t["vix_close"] < 24),
    ("VIX 24-26", lambda t: 24 <= t["vix_close"] < 26),
    ("VIX 26-30", lambda t: 26 <= t["vix_close"] < 30),
    ("VIX 30+",   lambda t: t["vix_close"] >= 30),
]

print("  --- ALL TRADES (unfiltered) ---")
print()
for label, filt in vix_buckets:
    bucket = [t for t in unfiltered_trades if filt(t)]
    st = calc_stats(bucket)
    print_stats_line(label, st)
    if st["count"] >= 3:
        print_bucket_detail(bucket)
    print()

print("  --- V7+AG FILTERED ---")
print()
for label, filt in vix_buckets:
    bucket = [t for t in filtered_trades if filt(t)]
    st = calc_stats(bucket)
    print_stats_line(label, st)
    if st["count"] >= 3:
        print_bucket_detail(bucket)
    print()

# ---------------------------------------------------------------------------
# D. SPX Daily Range Analysis (VIX implied move vs actual)
# ---------------------------------------------------------------------------
print("=" * 120)
print("  D. SPX DAILY RANGE vs VIX (does higher VIX = wider range = different outcomes?)")
print("=" * 120)
print()

range_buckets = [
    ("Range < 30 pts  (tight)", lambda t: t["spx_range"] < 30),
    ("Range 30-50 pts (normal)", lambda t: 30 <= t["spx_range"] < 50),
    ("Range 50-75 pts (wide)", lambda t: 50 <= t["spx_range"] < 75),
    ("Range 75-100 pts (volatile)", lambda t: 75 <= t["spx_range"] < 100),
    ("Range 100+ pts  (extreme)", lambda t: t["spx_range"] >= 100),
]

print("  --- V7+AG FILTERED ---")
print()
for label, filt in range_buckets:
    bucket = [t for t in filtered_trades if filt(t)]
    st = calc_stats(bucket)
    print_stats_line(label, st)
    if st["count"] >= 3:
        print_bucket_detail(bucket)
    print()

# ---------------------------------------------------------------------------
# E. Correlation Analysis
# ---------------------------------------------------------------------------
print("=" * 120)
print("  E. CORRELATION ANALYSIS (per-day)")
print("=" * 120)
print()

# Build per-day arrays for correlation
days_with_trades = [d for d in all_day_stats if d["v7_count"] > 0]

if len(days_with_trades) >= 5:
    vix_arr = np.array([d["vix_close"] for d in days_with_trades])
    overvix_arr = np.array([d["overvix"] for d in days_with_trades])
    rv_arr = np.array([d["realized_vol"] for d in days_with_trades])
    range_arr = np.array([d["spx_range"] for d in days_with_trades])
    wr_arr = np.array([d["v7_wr"] for d in days_with_trades])
    pnl_arr = np.array([d["v7_pnl"] for d in days_with_trades])

    def corr_str(a, b, name_a, name_b):
        if len(a) < 3:
            return f"  {name_a} vs {name_b}: insufficient data"
        r = np.corrcoef(a, b)[0, 1]
        strength = "STRONG" if abs(r) >= 0.5 else "moderate" if abs(r) >= 0.3 else "weak"
        direction = "positive" if r > 0 else "negative"
        return f"  {name_a:<20s} vs {name_b:<15s}: r = {r:+.3f}  ({strength} {direction})"

    print(corr_str(vix_arr, wr_arr, "VIX close", "Win Rate %"))
    print(corr_str(vix_arr, pnl_arr, "VIX close", "Daily P&L"))
    print(corr_str(overvix_arr, wr_arr, "OverVIX", "Win Rate %"))
    print(corr_str(overvix_arr, pnl_arr, "OverVIX", "Daily P&L"))
    print(corr_str(rv_arr, wr_arr, "Realized Vol", "Win Rate %"))
    print(corr_str(rv_arr, pnl_arr, "Realized Vol", "Daily P&L"))
    print(corr_str(range_arr, wr_arr, "SPX Range", "Win Rate %"))
    print(corr_str(range_arr, pnl_arr, "SPX Range", "Daily P&L"))
    print()

    # VIX vs OverVIX correlation (sanity check)
    print(corr_str(vix_arr, overvix_arr, "VIX close", "OverVIX"))
    print(corr_str(vix_arr, rv_arr, "VIX close", "Realized Vol"))
    print()

    # Summary stats
    print(f"  VIX range during period:     {vix_arr.min():.1f} - {vix_arr.max():.1f} (mean {vix_arr.mean():.1f})")
    print(f"  OverVIX range:               {overvix_arr.min():+.1f} - {overvix_arr.max():+.1f} (mean {overvix_arr.mean():+.1f})")
    print(f"  Realized Vol range:          {rv_arr.min():.1f} - {rv_arr.max():.1f} (mean {rv_arr.mean():.1f})")
    print(f"  SPX daily range:             {range_arr.min():.0f} - {range_arr.max():.0f} (mean {range_arr.mean():.0f})")
else:
    print("  Insufficient data for correlation analysis (need >= 5 trading days)")

print()

# ---------------------------------------------------------------------------
# F. Direction-specific analysis (do longs vs shorts behave differently in VIX regimes?)
# ---------------------------------------------------------------------------
print("=" * 120)
print("  F. DIRECTION ANALYSIS BY VIX REGIME (V7+AG filtered)")
print("=" * 120)
print()

for vix_label, vix_filt in [("VIX < 20", lambda t: t["vix_close"] < 20),
                              ("VIX 20-25", lambda t: 20 <= t["vix_close"] < 25),
                              ("VIX 25+", lambda t: t["vix_close"] >= 25)]:
    bucket = [t for t in filtered_trades if vix_filt(t)]
    if not bucket:
        continue
    st_all = calc_stats(bucket)
    longs = [t for t in bucket if t["direction"] in ("long", "bullish")]
    shorts = [t for t in bucket if t["direction"] not in ("long", "bullish")]
    st_long = calc_stats(longs)
    st_short = calc_stats(shorts)
    print(f"  {vix_label}:")
    print_stats_line("ALL", st_all, 4)
    print_stats_line("LONGS", st_long, 4)
    print_stats_line("SHORTS", st_short, 4)
    print()

# ---------------------------------------------------------------------------
# G. OverVIX direction analysis
# ---------------------------------------------------------------------------
print("=" * 120)
print("  G. DIRECTION ANALYSIS BY OVERVIX REGIME (V7+AG filtered)")
print("=" * 120)
print()

for ov_label, ov_filt in [("OverVIX < 6 (low)", lambda t: t["overvix"] < 6),
                            ("OverVIX 6-8 (moderate)", lambda t: 6 <= t["overvix"] < 8),
                            ("OverVIX 8-10 (high)", lambda t: 8 <= t["overvix"] < 10),
                            ("OverVIX 10-13 (very high)", lambda t: 10 <= t["overvix"] < 13),
                            ("OverVIX 13+ (extreme)", lambda t: t["overvix"] >= 13)]:
    bucket = [t for t in filtered_trades if ov_filt(t)]
    if not bucket:
        continue
    st_all = calc_stats(bucket)
    longs = [t for t in bucket if t["direction"] in ("long", "bullish")]
    shorts = [t for t in bucket if t["direction"] not in ("long", "bullish")]
    st_long = calc_stats(longs)
    st_short = calc_stats(shorts)
    print(f"  {ov_label}:")
    print_stats_line("ALL", st_all, 4)
    print_stats_line("LONGS", st_long, 4)
    print_stats_line("SHORTS", st_short, 4)
    print()

# ---------------------------------------------------------------------------
# H. Key Findings Summary
# ---------------------------------------------------------------------------
print("=" * 120)
print("  H. KEY FINDINGS SUMMARY")
print("=" * 120)
print()

# Find best and worst VIX buckets
best_vix_wr = None
worst_vix_wr = None
for label, filt in vix_buckets:
    bucket = [t for t in filtered_trades if filt(t)]
    if len(bucket) < 5:
        continue
    st = calc_stats(bucket)
    if best_vix_wr is None or st["wr"] > best_vix_wr[1]:
        best_vix_wr = (label, st["wr"], st["count"], st["pnl"])
    if worst_vix_wr is None or st["wr"] < worst_vix_wr[1]:
        worst_vix_wr = (label, st["wr"], st["count"], st["pnl"])

best_ov_wr = None
worst_ov_wr = None
for label, filt in overvix_buckets:
    bucket = [t for t in filtered_trades if filt(t)]
    if len(bucket) < 5:
        continue
    st = calc_stats(bucket)
    if best_ov_wr is None or st["wr"] > best_ov_wr[1]:
        best_ov_wr = (label, st["wr"], st["count"], st["pnl"])
    if worst_ov_wr is None or st["wr"] < worst_ov_wr[1]:
        worst_ov_wr = (label, st["wr"], st["count"], st["pnl"])

if best_vix_wr:
    print(f"  Best VIX bucket:    {best_vix_wr[0]:<25s} WR:{best_vix_wr[1]:>5.1f}% ({best_vix_wr[2]}t, PnL:{best_vix_wr[3]:>+.1f})")
if worst_vix_wr:
    print(f"  Worst VIX bucket:   {worst_vix_wr[0]:<25s} WR:{worst_vix_wr[1]:>5.1f}% ({worst_vix_wr[2]}t, PnL:{worst_vix_wr[3]:>+.1f})")
if best_ov_wr:
    print(f"  Best OverVIX:       {best_ov_wr[0]:<35s} WR:{best_ov_wr[1]:>5.1f}% ({best_ov_wr[2]}t, PnL:{best_ov_wr[3]:>+.1f})")
if worst_ov_wr:
    print(f"  Worst OverVIX:      {worst_ov_wr[0]:<35s} WR:{worst_ov_wr[1]:>5.1f}% ({worst_ov_wr[2]}t, PnL:{worst_ov_wr[3]:>+.1f})")

print()
print("  INTERPRETATION GUIDE:")
print("  - OverVIX > 0 means VIX is elevated relative to what SPX actually did recently.")
print("    This often means premium is rich -> good for selling, potentially harder for 0DTE buyers.")
print("  - OverVIX < 0 means VIX is low relative to recent movement.")
print("    Market may be underpricing risk -> potential for larger moves.")
print("  - If WR drops at high VIX/OverVIX, consider reducing position size or skipping trades.")
print("  - If certain setups thrive in specific regimes, consider regime-adaptive filters.")
print()
print("Done.")
