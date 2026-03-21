"""
SVB (Spot Vol Beta) Analysis vs Setup Outcomes
===============================================
Tests whether Volland's Spot-Vol-Beta correlates with setup trading outcomes.

SVB interpretation:
  - Negative = normal (vol rises when spot drops)
  - More negative = vol overreacting to spot drops
  - Near 0 = vol not reacting to spot
  - Positive = unusual (vol rises when spot rises, or vol drops when spot drops)

Hypothesis: SVB may be equivalent to Apollo's "overvix/undervix" concept.
Low SVB (very negative) = high implied vol relative to realized = overvixed.

Data sources:
  - Setup outcomes: Railway API /api/debug/gex-analysis (includes per-trade SVB)
  - Per-day SVB averages: same endpoint (svb_daily section from volland_snapshots)
  - VIX: yfinance for cross-reference

Usage:
    python tmp_svb_backtest.py
"""
import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import requests
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

# ---------------------------------------------------------------------------
# 1. Fetch data from Railway API
# ---------------------------------------------------------------------------
API_URL = "https://0dtealpha.com/api/debug/gex-analysis"

print("=" * 100)
print("  SVB (Spot Vol Beta) Analysis vs 0DTE Setup Outcomes")
print("=" * 100)
print()

print("[1] Fetching data from Railway API...")
try:
    resp = requests.get(API_URL, timeout=30)
    resp.raise_for_status()
    api_data = resp.json()
except Exception as e:
    print(f"ERROR: Could not fetch from {API_URL}: {e}")
    sys.exit(1)

outcomes = api_data.get("setup_outcomes", [])
svb_daily = api_data.get("svb_daily", [])
volland_days = api_data.get("volland_days", [])

if not outcomes:
    print("ERROR: No setup_outcomes returned from API.")
    sys.exit(1)

print(f"    Got {len(outcomes)} setup outcomes")
print(f"    Got {len(svb_daily)} days with SVB data")
print(f"    Got {len(volland_days)} volland days")

# Count trades with per-trade SVB
svb_trade_count = sum(1 for o in outcomes if o.get("svb") is not None)
print(f"    Trades with per-trade SVB: {svb_trade_count} / {len(outcomes)}")

# Build SVB lookup by date (from svb_daily endpoint)
svb_by_date = {}
for s in svb_daily:
    svb_by_date[s["date"]] = s

# Also build SVB from volland_days (first snapshot SVB)
svb_first_by_date = {}
for v in volland_days:
    if v.get("svb") is not None:
        svb_first_by_date[v["date"]] = v["svb"]

# ---------------------------------------------------------------------------
# 2. Fetch VIX data from yfinance for cross-reference
# ---------------------------------------------------------------------------
print("[2] Fetching VIX + SPX daily data from yfinance...")
import yfinance as yf

START_FETCH = "2026-01-01"
END_FETCH = "2026-03-15"
ANALYSIS_START = "2026-02-05"

vix_df = yf.download("^VIX", start=START_FETCH, end=END_FETCH, progress=False)
if vix_df.empty:
    print("WARNING: Could not fetch VIX data from yfinance. VIX cross-reference will be skipped.")
    vix_data = {}
else:
    if hasattr(vix_df.columns, 'levels') and len(vix_df.columns.levels) > 1:
        vix_df.columns = vix_df.columns.get_level_values(0)
    print(f"    Got {len(vix_df)} VIX trading days")

    spx_df = yf.download("^GSPC", start=START_FETCH, end=END_FETCH, progress=False)
    if hasattr(spx_df.columns, 'levels') and len(spx_df.columns.levels) > 1:
        spx_df.columns = spx_df.columns.get_level_values(0)

    # Realized vol
    spx_df["daily_return"] = spx_df["Close"].pct_change()
    spx_df["realized_vol"] = spx_df["daily_return"].rolling(window=20).std() * np.sqrt(252) * 100

    vix_close_map = {}
    for dt, row in vix_df.iterrows():
        vix_close_map[dt.strftime("%Y-%m-%d")] = float(row["Close"])

    vix_data = {}
    for dt, row in spx_df.iterrows():
        date_str = dt.strftime("%Y-%m-%d")
        if date_str < ANALYSIS_START:
            continue
        vix_close = vix_close_map.get(date_str)
        rv = row["realized_vol"]
        if vix_close is None or np.isnan(rv):
            continue
        vix_data[date_str] = {
            "vix_close": vix_close,
            "realized_vol": float(rv),
            "overvix": float(vix_close - rv),
        }
    print(f"    {len(vix_data)} days with VIX data")

# ---------------------------------------------------------------------------
# 3. Enrich trades with SVB (per-trade and per-day)
# ---------------------------------------------------------------------------
print("[3] Enriching trades with SVB data...")

enriched_trades = []
for o in outcomes:
    trade = dict(o)

    # Per-trade SVB (from setup_log.spot_vol_beta at time of signal)
    trade_svb = o.get("svb")

    # Per-day SVB (avg from volland_snapshots)
    day_svb_data = svb_by_date.get(o["date"])
    day_avg_svb = day_svb_data["avg_svb"] if day_svb_data else None

    # Use per-trade SVB if available, else fall back to day average
    trade["svb_trade"] = trade_svb
    trade["svb_day_avg"] = day_avg_svb
    trade["svb"] = trade_svb if trade_svb is not None else day_avg_svb

    # Add VIX data if available
    vd = vix_data.get(o["date"])
    if vd:
        trade["vix_close"] = vd["vix_close"]
        trade["overvix"] = vd["overvix"]
    else:
        trade["vix_close"] = None
        trade["overvix"] = None

    enriched_trades.append(trade)

trades_with_svb = [t for t in enriched_trades if t["svb"] is not None]
trades_with_both = [t for t in enriched_trades if t["svb"] is not None and t["vix_close"] is not None]

print(f"    Trades with SVB: {len(trades_with_svb)} / {len(enriched_trades)}")
print(f"    Trades with SVB + VIX: {len(trades_with_both)} / {len(enriched_trades)}")
print()

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def calc_stats(trades):
    if not trades:
        return {"count": 0, "wins": 0, "losses": 0, "expired": 0, "wr": 0, "pnl": 0, "avg_pnl": 0, "pf": 0}
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
    if stats["count"] == 0:
        print(f"{' '*indent}{label:<45s}   0 trades")
        return
    pf_str = f"{stats['pf']:.2f}" if stats["pf"] != float('inf') else "inf"
    print(f"{' '*indent}{label:<45s} {stats['count']:>4d}t  W:{stats['wins']:>3d}  L:{stats['losses']:>3d}  "
          f"WR:{stats['wr']:>5.1f}%  PnL:{stats['pnl']:>+8.1f}  Avg:{stats['avg_pnl']:>+5.1f}  PF:{pf_str:>5s}")

def print_bucket_detail(trades, indent=4):
    by_setup = defaultdict(list)
    for t in trades:
        by_setup[t["setup_name"]].append(t)
    for setup in sorted(by_setup.keys()):
        st = calc_stats(by_setup[setup])
        print_stats_line(f"  {setup}", st, indent)

def print_direction_detail(trades, indent=4):
    longs = [t for t in trades if t["direction"] in ("long", "bullish")]
    shorts = [t for t in trades if t["direction"] not in ("long", "bullish")]
    st_long = calc_stats(longs)
    st_short = calc_stats(shorts)
    print_stats_line("LONGS", st_long, indent)
    print_stats_line("SHORTS", st_short, indent)

# ---------------------------------------------------------------------------
# V7+AG Filter
# ---------------------------------------------------------------------------
def passes_v7ag(t):
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

all_svb_trades = trades_with_svb
v7_svb_trades = [t for t in trades_with_svb if passes_v7ag(t)]

print(f"  All trades with SVB: {len(all_svb_trades)}")
print(f"  V7+AG filtered with SVB: {len(v7_svb_trades)}")
print()

# ===========================================================================
# ANALYSIS SECTIONS
# ===========================================================================

# ---------------------------------------------------------------------------
# A. Per-Day Table
# ---------------------------------------------------------------------------
print("=" * 130)
print("  A. PER-DAY TABLE: Date / SVB (avg/min/max) / Trades / WR / P&L")
print("=" * 130)
print()

# Group trades by date
dates = sorted(set(t["date"] for t in enriched_trades))

header = (f"  {'Date':<12s} {'AvgSVB':>7s} {'MinSVB':>7s} {'MaxSVB':>7s} {'VIX':>6s} "
          f"{'#All':>4s} {'WR%':>5s} {'PnL':>8s}  "
          f"{'#V7':>4s} {'WR%':>5s} {'PnL':>8s}  SVB-src")
print(header)
print("  " + "-" * 126)

all_day_stats = []
for date_str in dates:
    day_all = [t for t in enriched_trades if t["date"] == date_str]
    day_v7 = [t for t in day_all if passes_v7ag(t)]
    day_svb_info = svb_by_date.get(date_str)

    st_all = calc_stats(day_all)
    st_v7 = calc_stats(day_v7)

    avg_svb_str = f"{day_svb_info['avg_svb']:>+7.3f}" if day_svb_info and day_svb_info.get('avg_svb') is not None else "    n/a"
    min_svb_str = f"{day_svb_info['min_svb']:>+7.3f}" if day_svb_info and day_svb_info.get('min_svb') is not None else "    n/a"
    max_svb_str = f"{day_svb_info['max_svb']:>+7.3f}" if day_svb_info and day_svb_info.get('max_svb') is not None else "    n/a"
    vix_str = f"{vix_data[date_str]['vix_close']:>6.1f}" if date_str in vix_data else "   n/a"

    wr_all_str = f"{st_all['wr']:>5.1f}" if st_all['count'] > 0 else "  n/a"
    wr_v7_str = f"{st_v7['wr']:>5.1f}" if st_v7['count'] > 0 else "  n/a"

    # Count how many trades had per-trade SVB
    trade_svb_count = sum(1 for t in day_all if t.get("svb_trade") is not None)
    svb_src = f"{trade_svb_count}/{len(day_all)} trade-level" if day_svb_info else "no SVB"

    print(f"  {date_str:<12s} {avg_svb_str} {min_svb_str} {max_svb_str} {vix_str} "
          f"{st_all['count']:>4d} {wr_all_str} {st_all['pnl']:>+8.1f}  "
          f"{st_v7['count']:>4d} {wr_v7_str} {st_v7['pnl']:>+8.1f}  {svb_src}")

    all_day_stats.append({
        "date": date_str,
        "avg_svb": day_svb_info["avg_svb"] if day_svb_info else None,
        "vix_close": vix_data[date_str]["vix_close"] if date_str in vix_data else None,
        "overvix": vix_data[date_str]["overvix"] if date_str in vix_data else None,
        "all_count": st_all["count"], "all_wr": st_all["wr"], "all_pnl": st_all["pnl"],
        "v7_count": st_v7["count"], "v7_wr": st_v7["wr"], "v7_pnl": st_v7["pnl"],
    })

# Totals
st_total_all = calc_stats(enriched_trades)
st_total_v7 = calc_stats([t for t in enriched_trades if passes_v7ag(t)])
print("  " + "-" * 126)
print(f"  {'TOTAL':<12s} {'':>7s} {'':>7s} {'':>7s} {'':>6s} "
      f"{st_total_all['count']:>4d} {st_total_all['wr']:>5.1f} {st_total_all['pnl']:>+8.1f}  "
      f"{st_total_v7['count']:>4d} {st_total_v7['wr']:>5.1f} {st_total_v7['pnl']:>+8.1f}")
print()

# ---------------------------------------------------------------------------
# B. SVB Bucket Analysis (per-trade SVB)
# ---------------------------------------------------------------------------
print("=" * 130)
print("  B. SVB BUCKET ANALYSIS (per-trade spot_vol_beta at signal time)")
print("     SVB < -1.5 = vol overreacting (highly negative correlation)")
print("     SVB ~ -0.5 to -1.0 = normal inverse relationship")
print("     SVB > 0 = unusual (vol and spot moving together)")
print("=" * 130)
print()

svb_buckets = [
    ("SVB < -1.50  (extreme neg)",   lambda t: t["svb"] is not None and t["svb"] < -1.50),
    ("SVB -1.50 to -1.00  (strong neg)", lambda t: t["svb"] is not None and -1.50 <= t["svb"] < -1.00),
    ("SVB -1.00 to -0.50  (normal)",     lambda t: t["svb"] is not None and -1.00 <= t["svb"] < -0.50),
    ("SVB -0.50 to  0.00  (weak neg)",   lambda t: t["svb"] is not None and -0.50 <= t["svb"] < 0.00),
    ("SVB  0.00 to +0.50  (unusual)",    lambda t: t["svb"] is not None and 0.00 <= t["svb"] < 0.50),
    ("SVB +0.50+  (extreme pos)",        lambda t: t["svb"] is not None and t["svb"] >= 0.50),
]

print("  --- ALL TRADES (unfiltered, with SVB) ---")
print()
for label, filt in svb_buckets:
    bucket = [t for t in all_svb_trades if filt(t)]
    st = calc_stats(bucket)
    print_stats_line(f"{label}", st)
    if st["count"] >= 3:
        print_bucket_detail(bucket)
    print()

print("  --- V7+AG FILTERED ---")
print()
for label, filt in svb_buckets:
    bucket = [t for t in v7_svb_trades if filt(t)]
    st = calc_stats(bucket)
    print_stats_line(f"{label}", st)
    if st["count"] >= 3:
        print_bucket_detail(bucket)
    print()

# ---------------------------------------------------------------------------
# C. SVB Direction Split (longs vs shorts per SVB bucket)
# ---------------------------------------------------------------------------
print("=" * 130)
print("  C. SVB DIRECTION ANALYSIS (V7+AG filtered)")
print("     Do longs vs shorts behave differently at different SVB levels?")
print("=" * 130)
print()

for label, filt in svb_buckets:
    bucket = [t for t in v7_svb_trades if filt(t)]
    if not bucket:
        continue
    st_all = calc_stats(bucket)
    print(f"  {label}:")
    print_stats_line("ALL", st_all, 4)
    print_direction_detail(bucket, 4)
    if st_all["count"] >= 5:
        print_bucket_detail(bucket, 4)
    print()

# ---------------------------------------------------------------------------
# D. SVB vs OverVIX Comparison (are they the same signal?)
# ---------------------------------------------------------------------------
print("=" * 130)
print("  D. SVB vs OVERVIX COMPARISON (are they correlated?)")
print("     If SVB ~ OverVIX, they measure the same thing.")
print("=" * 130)
print()

# Per-day comparison
days_with_both = [d for d in all_day_stats if d["avg_svb"] is not None and d["overvix"] is not None]
if len(days_with_both) >= 5:
    svb_arr = np.array([d["avg_svb"] for d in days_with_both])
    overvix_arr = np.array([d["overvix"] for d in days_with_both])
    vix_arr = np.array([d["vix_close"] for d in days_with_both])
    wr_arr = np.array([d["v7_wr"] for d in days_with_both if d["v7_count"] > 0])
    pnl_arr = np.array([d["v7_pnl"] for d in days_with_both if d["v7_count"] > 0])
    # Need matching arrays for correlation
    days_w_trades = [d for d in days_with_both if d["v7_count"] > 0]
    svb_wt = np.array([d["avg_svb"] for d in days_w_trades])
    overvix_wt = np.array([d["overvix"] for d in days_w_trades])
    vix_wt = np.array([d["vix_close"] for d in days_w_trades])

    def corr_str(a, b, name_a, name_b):
        if len(a) < 3:
            return f"  {name_a} vs {name_b}: insufficient data"
        r = np.corrcoef(a, b)[0, 1]
        strength = "STRONG" if abs(r) >= 0.5 else "moderate" if abs(r) >= 0.3 else "weak"
        direction = "positive" if r > 0 else "negative"
        return f"  {name_a:<25s} vs {name_b:<15s}: r = {r:+.3f}  ({strength} {direction})"

    print("  Per-day correlations:")
    print(corr_str(svb_arr, overvix_arr, "Day SVB (avg)", "OverVIX"))
    print(corr_str(svb_arr, vix_arr, "Day SVB (avg)", "VIX close"))
    print()

    if len(days_w_trades) >= 5:
        print("  SVB/VIX vs Trading Performance (per-day):")
        print(corr_str(svb_wt, wr_arr, "Day SVB (avg)", "Win Rate %"))
        print(corr_str(svb_wt, pnl_arr, "Day SVB (avg)", "Daily P&L"))
        print(corr_str(overvix_wt, wr_arr, "Day OverVIX", "Win Rate %"))
        print(corr_str(overvix_wt, pnl_arr, "Day OverVIX", "Daily P&L"))
        print(corr_str(vix_wt, wr_arr, "Day VIX close", "Win Rate %"))
        print(corr_str(vix_wt, pnl_arr, "Day VIX close", "Daily P&L"))
        print()

    # Per-trade correlation
    trades_both = [t for t in v7_svb_trades if t.get("svb") is not None and t.get("overvix") is not None]
    if len(trades_both) >= 5:
        t_svb = np.array([t["svb"] for t in trades_both])
        t_ov = np.array([t["overvix"] for t in trades_both])
        t_pnl = np.array([t["pnl"] for t in trades_both])
        t_win = np.array([1 if t["result"] == "WIN" else 0 for t in trades_both])
        print("  Per-trade correlations (V7+AG):")
        print(corr_str(t_svb, t_ov, "Trade SVB", "OverVIX"))
        print(corr_str(t_svb, t_pnl, "Trade SVB", "Trade P&L"))
        print(corr_str(t_svb, t_win, "Trade SVB", "Win (0/1)"))
        print(corr_str(t_ov, t_pnl, "Trade OverVIX", "Trade P&L"))
        print(corr_str(t_ov, t_win, "Trade OverVIX", "Win (0/1)"))
        print()

    # Summary stats
    print(f"  SVB range during period:  {svb_arr.min():+.3f} to {svb_arr.max():+.3f}  (mean {svb_arr.mean():+.3f})")
    print(f"  OverVIX range:            {overvix_arr.min():+.1f} to {overvix_arr.max():+.1f}  (mean {overvix_arr.mean():+.1f})")
else:
    print("  Insufficient data for SVB vs OverVIX comparison (need >= 5 days with both)")

print()

# ---------------------------------------------------------------------------
# E. 2D Grid: VIX Buckets x SVB Buckets
# ---------------------------------------------------------------------------
print("=" * 130)
print("  E. 2D GRID: VIX x SVB BUCKETS (V7+AG filtered)")
print("     Find the optimal VIX + SVB combination")
print("=" * 130)
print()

vix_grid_buckets = [
    ("VIX<20", lambda t: t.get("vix_close") is not None and t["vix_close"] < 20),
    ("VIX 20-24", lambda t: t.get("vix_close") is not None and 20 <= t["vix_close"] < 24),
    ("VIX 24+", lambda t: t.get("vix_close") is not None and t["vix_close"] >= 24),
]

svb_grid_buckets = [
    ("SVB<-1.0", lambda t: t["svb"] is not None and t["svb"] < -1.0),
    ("SVB -1.0 to -0.5", lambda t: t["svb"] is not None and -1.0 <= t["svb"] < -0.5),
    ("SVB -0.5 to 0", lambda t: t["svb"] is not None and -0.5 <= t["svb"] < 0),
    ("SVB>=0", lambda t: t["svb"] is not None and t["svb"] >= 0),
]

# Header
print(f"  {'':>20s}", end="")
for svb_label, _ in svb_grid_buckets:
    print(f" | {svb_label:^28s}", end="")
print()
print(f"  {'':>20s}", end="")
for _ in svb_grid_buckets:
    print(f" | {'#t':>4s} {'WR%':>5s} {'PnL':>8s} {'PF':>5s}", end="")
print()
print("  " + "-" * (20 + len(svb_grid_buckets) * 29))

best_combo = None
best_combo_pnl_per_trade = -999

for vix_label, vix_filt in vix_grid_buckets:
    print(f"  {vix_label:<20s}", end="")
    for svb_label, svb_filt in svb_grid_buckets:
        bucket = [t for t in v7_svb_trades if vix_filt(t) and svb_filt(t)]
        st = calc_stats(bucket)
        if st["count"] > 0:
            pf_str = f"{st['pf']:.1f}" if st["pf"] != float('inf') else "inf"
            print(f" | {st['count']:>4d} {st['wr']:>5.1f} {st['pnl']:>+8.1f} {pf_str:>5s}", end="")
            if st["count"] >= 5 and st["avg_pnl"] > best_combo_pnl_per_trade:
                best_combo = (vix_label, svb_label, st)
                best_combo_pnl_per_trade = st["avg_pnl"]
        else:
            print(f" | {'--':>4s} {'--':>5s} {'--':>8s} {'--':>5s}", end="")
    print()

print()
if best_combo:
    vl, sl, st = best_combo
    pf_str = f"{st['pf']:.2f}" if st["pf"] != float('inf') else "inf"
    print(f"  BEST COMBO (>= 5 trades): {vl} x {sl}")
    print(f"    {st['count']}t, WR={st['wr']:.1f}%, PnL={st['pnl']:+.1f}, Avg={st['avg_pnl']:+.1f}/trade, PF={pf_str}")
print()

# ---------------------------------------------------------------------------
# F. SVB as Trade-Level Filter (what if we block trades with SVB in certain ranges?)
# ---------------------------------------------------------------------------
print("=" * 130)
print("  F. SVB AS TRADE-LEVEL FILTER (V7+AG base)")
print("     Test: what if we block trades where SVB is in a specific range?")
print("=" * 130)
print()

# Baseline
st_base = calc_stats(v7_svb_trades)
pf_base_str = f"{st_base['pf']:.2f}" if st_base["pf"] != float('inf') else "inf"
print(f"  BASELINE (V7+AG, all SVB):  {st_base['count']}t, WR={st_base['wr']:.1f}%, "
      f"PnL={st_base['pnl']:+.1f}, Avg={st_base['avg_pnl']:+.1f}, PF={pf_base_str}")
print()

# Test various SVB blocks
svb_filters = [
    # Block extreme negative
    ("Block SVB < -1.5", lambda t: t["svb"] is not None and t["svb"] >= -1.5),
    ("Block SVB < -1.0", lambda t: t["svb"] is not None and t["svb"] >= -1.0),
    # Block weak/positive
    ("Block SVB >= 0", lambda t: t["svb"] is not None and t["svb"] < 0),
    ("Block SVB >= -0.3", lambda t: t["svb"] is not None and t["svb"] < -0.3),
    ("Block SVB >= -0.5", lambda t: t["svb"] is not None and t["svb"] < -0.5),
    # Specific ranges
    ("Only SVB -1.5 to -0.5", lambda t: t["svb"] is not None and -1.5 <= t["svb"] < -0.5),
    ("Only SVB -1.0 to -0.5", lambda t: t["svb"] is not None and -1.0 <= t["svb"] < -0.5),
    ("Only SVB < -0.5", lambda t: t["svb"] is not None and t["svb"] < -0.5),
    # Block shorts where SVB > -0.5 (current E2T filter)
    ("Current E2T: block shorts SVB >= -0.5", None),  # Special handling
]

for label, filt in svb_filters:
    if label == "Current E2T: block shorts SVB >= -0.5":
        # Special: only blocks shorts, not longs
        filtered = []
        for t in v7_svb_trades:
            is_long = t["direction"] in ("long", "bullish")
            if is_long:
                filtered.append(t)
            else:
                if t["svb"] is not None and t["svb"] < -0.5:
                    filtered.append(t)
        st = calc_stats(filtered)
    else:
        filtered = [t for t in v7_svb_trades if filt(t)]
        st = calc_stats(filtered)

    delta_pnl = st["pnl"] - st_base["pnl"]
    delta_count = st["count"] - st_base["count"]
    pf_str = f"{st['pf']:.2f}" if st["pf"] != float('inf') else "inf"
    marker = " ***" if delta_pnl > 20 else " --" if delta_pnl < -20 else ""
    print(f"  {label:<45s} {st['count']:>4d}t  WR:{st['wr']:>5.1f}%  PnL:{st['pnl']:>+8.1f}  "
          f"Avg:{st['avg_pnl']:>+5.1f}  PF:{pf_str:>5s}  "
          f"({delta_count:>+4d}t, {delta_pnl:>+7.1f} pts){marker}")

print()
print("  *** = improvement > +20 pts,  -- = degradation > -20 pts")
print()

# ---------------------------------------------------------------------------
# G. Per-Setup SVB Analysis (V7+AG filtered)
# ---------------------------------------------------------------------------
print("=" * 130)
print("  G. PER-SETUP SVB ANALYSIS (V7+AG filtered)")
print("     Does SVB affect some setups more than others?")
print("=" * 130)
print()

setups_in_data = sorted(set(t["setup_name"] for t in v7_svb_trades))
for setup in setups_in_data:
    setup_trades = [t for t in v7_svb_trades if t["setup_name"] == setup]
    if len(setup_trades) < 3:
        continue
    st_all = calc_stats(setup_trades)
    print(f"  {setup} ({st_all['count']}t, WR={st_all['wr']:.1f}%, PnL={st_all['pnl']:+.1f}):")

    for label, filt in svb_buckets:
        bucket = [t for t in setup_trades if filt(t)]
        st = calc_stats(bucket)
        if st["count"] > 0:
            print_stats_line(f"  {label}", st, 4)
    print()

# ---------------------------------------------------------------------------
# H. Time-of-Day SVB Analysis (does SVB shift intraday and affect outcomes?)
# ---------------------------------------------------------------------------
print("=" * 130)
print("  H. SVB DATA QUALITY CHECK")
print("     How consistent is SVB within each day?")
print("=" * 130)
print()

for s in svb_daily[:5]:
    if s.get("max_svb") is not None and s.get("min_svb") is not None:
        svb_range = s["max_svb"] - s["min_svb"]
        print(f"  {s['date']}: avg={s['avg_svb']:+.3f}  min={s['min_svb']:+.3f}  max={s['max_svb']:+.3f}  "
              f"range={svb_range:.3f}  snaps={s['snap_count']}")

if svb_daily:
    all_ranges = [s["max_svb"] - s["min_svb"] for s in svb_daily if s.get("max_svb") is not None and s.get("min_svb") is not None]
    print()
    print(f"  Intra-day SVB range: min={min(all_ranges):.3f}, max={max(all_ranges):.3f}, "
          f"avg={np.mean(all_ranges):.3f}, median={np.median(all_ranges):.3f}")
    print(f"  If range is large, per-trade SVB (time-matched) is much more valuable than daily average.")
print()

# ---------------------------------------------------------------------------
# I. Key Findings Summary
# ---------------------------------------------------------------------------
print("=" * 130)
print("  I. KEY FINDINGS SUMMARY")
print("=" * 130)
print()

# Best and worst SVB buckets (V7+AG)
best_svb = None
worst_svb = None
for label, filt in svb_buckets:
    bucket = [t for t in v7_svb_trades if filt(t)]
    if len(bucket) < 5:
        continue
    st = calc_stats(bucket)
    if best_svb is None or st["wr"] > best_svb[1]:
        best_svb = (label, st["wr"], st["count"], st["pnl"], st["avg_pnl"])
    if worst_svb is None or st["wr"] < worst_svb[1]:
        worst_svb = (label, st["wr"], st["count"], st["pnl"], st["avg_pnl"])

if best_svb:
    print(f"  Best SVB bucket (V7+AG):   {best_svb[0]:<35s}  WR:{best_svb[1]:>5.1f}%  "
          f"({best_svb[2]}t, PnL:{best_svb[3]:>+.1f}, Avg:{best_svb[4]:>+.1f})")
if worst_svb:
    print(f"  Worst SVB bucket (V7+AG):  {worst_svb[0]:<35s}  WR:{worst_svb[1]:>5.1f}%  "
          f"({worst_svb[2]}t, PnL:{worst_svb[3]:>+.1f}, Avg:{worst_svb[4]:>+.1f})")

# Check per-trade vs day-avg SVB coverage
trade_svb_pct = svb_trade_count / len(outcomes) * 100 if outcomes else 0
print()
print(f"  Per-trade SVB coverage: {svb_trade_count}/{len(outcomes)} ({trade_svb_pct:.0f}%)")
if trade_svb_pct < 80:
    print(f"  WARNING: Only {trade_svb_pct:.0f}% of trades have per-trade SVB. "
          f"Earlier trades may use day-average fallback.")

print()
print("  INTERPRETATION GUIDE:")
print("  - SVB measures how vol moves relative to spot: negative = normal inverse correlation")
print("  - Very negative SVB (< -1.0) = vol is overreacting to spot moves = 'overvixed'")
print("  - SVB near 0 = vol decoupled from spot = regime uncertainty")
print("  - Positive SVB = unusual (vol rises WITH spot, or drops with spot)")
print("  - If SVB strongly predicts WR/PnL, it could be added as a filter")
print("  - If SVB correlates with OverVIX (r > 0.5), they're measuring the same thing")
print("  - If SVB is orthogonal to OverVIX, combining them may add value")
print()
print("Done.")
