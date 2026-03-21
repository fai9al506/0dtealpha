"""
OverVIX Indicator Analysis — VIX vs VIX3M Term Structure
=========================================================
Apollo's "overvix/undervix" indicator: overvix = VIX - VIX3M
When VIX > VIX3M (overvix > 0): short-term fear exceeds medium-term = BULLISH mean reversion
When VIX < VIX3M (overvix < 0): normal contango = no signal or bearish caution

Data sources:
  - VIX daily:  yfinance ^VIX
  - VIX3M daily: yfinance ^VIX3M
  - SPX daily:  yfinance ^GSPC
  - Setup outcomes: https://0dtealpha.com/api/debug/gex-analysis

Usage:
  pip install requests yfinance pandas numpy
  python tmp_overvix_test.py
"""
import sys, io, math
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import requests
import yfinance as yf
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta

# ============================================================================
# HELPER: safe yfinance column access (handles multi-level columns)
# ============================================================================
def _yf_col(df, col, dt):
    """Safely extract a scalar from yfinance dataframe (handles MultiIndex cols)."""
    val = df.loc[dt, col]
    if hasattr(val, 'iloc'):
        return float(val.iloc[0])
    return float(val)


# ============================================================================
# PART 0: FETCH ALL DATA
# ============================================================================

print("=" * 120)
print("  OVERVIX INDICATOR ANALYSIS (VIX - VIX3M)")
print("  Apollo's term-structure signal vs 0DTE setup outcomes")
print("=" * 120)
print()

# --- VIX + VIX3M + SPX history (Jan 2024 to present for Part 1) ---
HIST_START = "2024-01-01"
HIST_END = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

print("[1] Fetching VIX daily data from yfinance...", flush=True)
vix_df = yf.download("^VIX", start=HIST_START, end=HIST_END, progress=False)
if vix_df.empty:
    print("ERROR: Could not fetch VIX data.")
    sys.exit(1)
if hasattr(vix_df.columns, 'levels') and len(vix_df.columns.levels) > 1:
    vix_df.columns = vix_df.columns.get_level_values(0)
print(f"    Got {len(vix_df)} VIX trading days ({vix_df.index[0].strftime('%Y-%m-%d')} to {vix_df.index[-1].strftime('%Y-%m-%d')})")

print("[2] Fetching VIX3M daily data from yfinance...", flush=True)
vix3m_df = yf.download("^VIX3M", start=HIST_START, end=HIST_END, progress=False)
if vix3m_df.empty:
    print("WARNING: ^VIX3M not available from yfinance. Trying ^VIX3M alternative...")
    # Try CBOE VIX3M ETN alternatives
    vix3m_df = yf.download("VIX3M", start=HIST_START, end=HIST_END, progress=False)
if vix3m_df.empty:
    print("ERROR: Could not fetch VIX3M data. Cannot proceed.")
    sys.exit(1)
if hasattr(vix3m_df.columns, 'levels') and len(vix3m_df.columns.levels) > 1:
    vix3m_df.columns = vix3m_df.columns.get_level_values(0)
print(f"    Got {len(vix3m_df)} VIX3M trading days ({vix3m_df.index[0].strftime('%Y-%m-%d')} to {vix3m_df.index[-1].strftime('%Y-%m-%d')})")

print("[3] Fetching SPX daily data from yfinance...", flush=True)
spx_df = yf.download("^GSPC", start=HIST_START, end=HIST_END, progress=False)
if spx_df.empty:
    print("ERROR: Could not fetch SPX data.")
    sys.exit(1)
if hasattr(spx_df.columns, 'levels') and len(spx_df.columns.levels) > 1:
    spx_df.columns = spx_df.columns.get_level_values(0)
print(f"    Got {len(spx_df)} SPX trading days")

# --- Setup outcomes from Railway API ---
print("[4] Fetching setup outcomes from Railway API...", flush=True)
API_URL = "https://0dtealpha.com/api/debug/gex-analysis"

for attempt in range(5):
    try:
        resp = requests.get(API_URL, timeout=30)
        resp.raise_for_status()
        api_data = resp.json()
        if 'setup_outcomes' in api_data and 'error' not in api_data:
            break
        print(f"    Attempt {attempt+1}: {str(api_data.get('error','no outcomes'))[:80]}")
    except Exception as ex:
        print(f"    Attempt {attempt+1} error: {ex}")
    import time; time.sleep(10)
else:
    print("ERROR: Could not fetch setup outcomes after 5 attempts.")
    sys.exit(1)

outcomes = api_data["setup_outcomes"]
print(f"    Got {len(outcomes)} raw trades")

# Normalize direction
for t in outcomes:
    if t["direction"] == "bullish":
        t["direction"] = "long"
    elif t["direction"] == "bearish":
        t["direction"] = "short"

# --- Build overvix lookup: date_str -> {vix, vix3m, overvix, spx_close} ---
overvix_lookup = {}
common_dates = sorted(set(vix_df.index) & set(vix3m_df.index))
for dt in common_dates:
    ds = dt.strftime("%Y-%m-%d")
    try:
        vix_close = _yf_col(vix_df, "Close", dt)
        vix3m_close = _yf_col(vix3m_df, "Close", dt)
        spx_close = None
        if dt in spx_df.index:
            spx_close = _yf_col(spx_df, "Close", dt)
        overvix = vix_close - vix3m_close
        overvix_lookup[ds] = {
            "vix": round(vix_close, 2),
            "vix3m": round(vix3m_close, 2),
            "overvix": round(overvix, 2),
            "spx_close": round(spx_close, 2) if spx_close else None,
        }
    except Exception:
        pass

print(f"    Built overvix data for {len(overvix_lookup)} trading days")
print()


# ============================================================================
# PART 1: OVERVIX HISTORY & VISUALIZATION
# ============================================================================

print("=" * 120)
print("  PART 1: OVERVIX HISTORY (Jan 2024 to present)")
print("=" * 120)
print()

# Summary statistics
all_overvix = [(ds, d["overvix"]) for ds, d in sorted(overvix_lookup.items())]
ov_values = [v for _, v in all_overvix]

# Categorize days
def classify_overvix(ov):
    if ov > 3:    return "heavily_over"
    elif ov > 1:  return "over"
    elif ov > -1: return "neutral"
    elif ov > -3: return "under"
    else:         return "heavily_under"

n_total = len(ov_values)
n_positive = sum(1 for v in ov_values if v > 0)
n_gt2 = sum(1 for v in ov_values if v > 2)
n_gt3 = sum(1 for v in ov_values if v > 3)
n_negative = sum(1 for v in ov_values if v < 0)
n_lt_m2 = sum(1 for v in ov_values if v < -2)
n_lt_m3 = sum(1 for v in ov_values if v < -3)

print(f"  Total trading days with overvix data: {n_total}")
print(f"  Date range: {all_overvix[0][0]} to {all_overvix[-1][0]}")
print(f"  Overvix range: {min(ov_values):+.2f} to {max(ov_values):+.2f}")
print(f"  Mean: {np.mean(ov_values):+.2f}  Median: {np.median(ov_values):+.2f}  StdDev: {np.std(ov_values):.2f}")
print()
print(f"  OVERVIXED (VIX > VIX3M, overvix > 0):  {n_positive:>4d} days ({n_positive/n_total*100:.1f}%)")
print(f"    Overvix > +2 (signal territory):      {n_gt2:>4d} days ({n_gt2/n_total*100:.1f}%)")
print(f"    Overvix > +3 (heavily overvixed):      {n_gt3:>4d} days ({n_gt3/n_total*100:.1f}%)")
print(f"  UNDERVIXED (VIX < VIX3M, overvix < 0):  {n_negative:>4d} days ({n_negative/n_total*100:.1f}%)")
print(f"    Overvix < -2:                          {n_lt_m2:>4d} days ({n_lt_m2/n_total*100:.1f}%)")
print(f"    Overvix < -3:                          {n_lt_m3:>4d} days ({n_lt_m3/n_total*100:.1f}%)")
print()

# Distribution by bucket
buckets = [
    ("heavily_under", "< -3",   lambda v: v < -3),
    ("under",         "-3 to -1", lambda v: -3 <= v < -1),
    ("neutral",       "-1 to +1", lambda v: -1 <= v <= 1),
    ("over",          "+1 to +2", lambda v: 1 < v <= 2),
    ("signal",        "+2 to +3", lambda v: 2 < v <= 3),
    ("heavily_over",  "> +3",    lambda v: v > 3),
]

print("  DISTRIBUTION BY BUCKET:")
print(f"  {'Bucket':<16s} {'Range':<10s} {'Days':>5s} {'Pct':>6s}  {'AvgOV':>7s}  {'AvgVIX':>7s}")
print(f"  {'-'*16} {'-'*10} {'-'*5} {'-'*6}  {'-'*7}  {'-'*7}")
for bname, brange, bfn in buckets:
    matching = [(ds, d) for ds, d in sorted(overvix_lookup.items()) if bfn(d["overvix"])]
    n = len(matching)
    avg_ov = np.mean([d["overvix"] for _, d in matching]) if matching else 0
    avg_vix = np.mean([d["vix"] for _, d in matching]) if matching else 0
    print(f"  {bname:<16s} {brange:<10s} {n:>5d} {n/n_total*100:>5.1f}%  {avg_ov:>+7.2f}  {avg_vix:>7.2f}")
print()

# Last 60 trading days table
print("  LAST 60 TRADING DAYS:")
print(f"  {'Date':<12s} {'VIX':>6s} {'VIX3M':>6s} {'OverVIX':>8s}  {'State':<16s}")
print(f"  {'-'*12} {'-'*6} {'-'*6} {'-'*8}  {'-'*16}")
last60 = all_overvix[-60:]
for ds, ov in last60:
    d = overvix_lookup[ds]
    state = classify_overvix(ov)
    # Add visual indicator for extreme values
    marker = ""
    if ov > 3: marker = " <<< HEAVY SIGNAL"
    elif ov > 2: marker = " <<<  SIGNAL"
    elif ov < -3: marker = " !!! DANGER"
    print(f"  {ds:<12s} {d['vix']:>6.2f} {d['vix3m']:>6.2f} {ov:>+8.2f}  {state:<16s}{marker}")
print()


# ============================================================================
# PART 2: OVERVIX vs OUR SETUP OUTCOMES
# ============================================================================

print("=" * 120)
print("  PART 2: OVERVIX vs SETUP OUTCOMES (V7+AG filtered)")
print("=" * 120)
print()

# V7+AG filter
def passes_v7ag(t):
    sn = t.get("setup_name", "")
    direction = t.get("direction", "")
    alignment = t.get("alignment")
    if alignment is None:
        return False
    if direction in ("long", "bullish"):
        return alignment >= 2
    if direction in ("short", "bearish"):
        if sn == "Skew Charm": return True
        if sn == "DD Exhaustion": return alignment != 0
        if sn == "AG Short": return True
        return False
    return False

baseline_trades = [t for t in outcomes if passes_v7ag(t)]

# Attach overvix data to each trade
missing_overvix = 0
for t in baseline_trades:
    d = overvix_lookup.get(t["date"])
    if d:
        t["vix"] = d["vix"]
        t["vix3m"] = d["vix3m"]
        t["overvix"] = d["overvix"]
    else:
        t["vix"] = None
        t["vix3m"] = None
        t["overvix"] = None
        missing_overvix += 1

if missing_overvix:
    print(f"  Warning: {missing_overvix} trades had no overvix data for their date")

# Only keep trades with overvix data
trades_with_ov = [t for t in baseline_trades if t["overvix"] is not None]
print(f"  V7+AG baseline: {len(baseline_trades)} trades, {len(trades_with_ov)} with overvix data")
print()

# --- Daily results table ---
trade_dates = sorted(set(t["date"] for t in trades_with_ov))

print("  DAILY RESULTS TABLE (V7+AG filtered):")
print(f"  {'Date':<12s} {'VIX':>5s} {'VIX3M':>6s} {'OV':>6s} {'State':<13s} {'#T':>3s} {'#L':>3s} {'#S':>3s} "
      f"{'W':>3s} {'L':>3s} {'WR':>5s} {'PnL':>8s} {'LPnL':>8s} {'SPnL':>8s}")
print(f"  {'-'*12} {'-'*5} {'-'*6} {'-'*6} {'-'*13} {'-'*3} {'-'*3} {'-'*3} "
      f"{'-'*3} {'-'*3} {'-'*5} {'-'*8} {'-'*8} {'-'*8}")

cum_pnl = 0.0
for d in trade_dates:
    day_trades = [t for t in trades_with_ov if t["date"] == d]
    if not day_trades:
        continue
    sample = day_trades[0]
    vix = sample["vix"]
    vix3m = sample["vix3m"]
    ov = sample["overvix"]
    state = classify_overvix(ov)

    n_trades = len(day_trades)
    n_longs = sum(1 for t in day_trades if t["direction"] == "long")
    n_shorts = n_trades - n_longs
    wins = sum(1 for t in day_trades if t["result"] == "WIN")
    losses = sum(1 for t in day_trades if t["result"] == "LOSS")
    wr = wins / n_trades * 100 if n_trades else 0
    pnl = sum(t["pnl"] for t in day_trades)
    long_pnl = sum(t["pnl"] for t in day_trades if t["direction"] == "long")
    short_pnl = sum(t["pnl"] for t in day_trades if t["direction"] != "long")
    cum_pnl += pnl

    print(f"  {d:<12s} {vix:>5.1f} {vix3m:>6.1f} {ov:>+6.2f} {state:<13s} {n_trades:>3d} {n_longs:>3d} {n_shorts:>3d} "
          f"{wins:>3d} {losses:>3d} {wr:>4.0f}% {pnl:>+7.1f} {long_pnl:>+7.1f} {short_pnl:>+7.1f}")

print(f"  {'-'*12} {'-'*5} {'-'*6} {'-'*6} {'-'*13} {'-'*3} {'-'*3} {'-'*3} "
      f"{'-'*3} {'-'*3} {'-'*5} {'-'*8} {'-'*8} {'-'*8}")
print(f"  {'TOTAL':>85s} {cum_pnl:>+7.1f}")
print()

# --- Overvix bucket analysis ---
print("  OVERVIX BUCKET ANALYSIS:")
print()

analysis_buckets = [
    ("heavily_under", "< -3",     lambda v: v < -3),
    ("under",         "-3 to -1", lambda v: -3 <= v < -1),
    ("neutral",       "-1 to +1", lambda v: -1 <= v <= 1),
    ("over",          "+1 to +2", lambda v: 1 < v <= 2),
    ("signal",        "+2 to +3", lambda v: 2 < v <= 3),
    ("heavily_over",  "> +3",     lambda v: v > 3),
]

def compute_bucket_stats(trades, label=""):
    """Compute stats for a bucket of trades."""
    n = len(trades)
    if n == 0:
        return None
    wins = sum(1 for t in trades if t["result"] == "WIN")
    losses = sum(1 for t in trades if t["result"] == "LOSS")
    wr = wins / n * 100
    pnl = sum(t["pnl"] for t in trades)
    gross_profit = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    pf = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    avg_pnl = pnl / n

    # Long/short split
    longs = [t for t in trades if t["direction"] == "long"]
    shorts = [t for t in trades if t["direction"] != "long"]
    l_wins = sum(1 for t in longs if t["result"] == "WIN")
    s_wins = sum(1 for t in shorts if t["result"] == "WIN")
    l_pnl = sum(t["pnl"] for t in longs)
    s_pnl = sum(t["pnl"] for t in shorts)
    l_wr = l_wins / len(longs) * 100 if longs else 0
    s_wr = s_wins / len(shorts) * 100 if shorts else 0

    return {
        "label": label, "n": n, "wins": wins, "losses": losses, "wr": wr,
        "pnl": pnl, "pf": pf, "avg_pnl": avg_pnl,
        "n_longs": len(longs), "l_wr": l_wr, "l_pnl": l_pnl,
        "n_shorts": len(shorts), "s_wr": s_wr, "s_pnl": s_pnl,
    }

print(f"  {'Bucket':<16s} {'Range':<10s} {'#T':>4s} {'WR':>6s} {'PnL':>8s} {'PF':>6s} {'AvgPnL':>7s}  "
      f"{'#L':>3s} {'L_WR':>5s} {'L_PnL':>7s}  {'#S':>3s} {'S_WR':>5s} {'S_PnL':>7s}")
print(f"  {'-'*16} {'-'*10} {'-'*4} {'-'*6} {'-'*8} {'-'*6} {'-'*7}  "
      f"{'-'*3} {'-'*5} {'-'*7}  {'-'*3} {'-'*5} {'-'*7}")

for bname, brange, bfn in analysis_buckets:
    bt = [t for t in trades_with_ov if bfn(t["overvix"])]
    s = compute_bucket_stats(bt, bname)
    if s is None:
        print(f"  {bname:<16s} {brange:<10s}    0 trades")
        continue
    marker = ""
    if bname == "signal":    marker = "  <<< APOLLO'S SIGNAL"
    if bname == "heavily_over": marker = "  <<< HEAVY OVERVIX"
    print(f"  {bname:<16s} {brange:<10s} {s['n']:>4d} {s['wr']:>5.1f}% {s['pnl']:>+7.1f} {s['pf']:>6.2f} {s['avg_pnl']:>+7.2f}  "
          f"{s['n_longs']:>3d} {s['l_wr']:>4.0f}% {s['l_pnl']:>+7.1f}  "
          f"{s['n_shorts']:>3d} {s['s_wr']:>4.0f}% {s['s_pnl']:>+7.1f}{marker}")
print()

# --- Per-setup breakdown within each bucket ---
print("  PER-SETUP BREAKDOWN BY OVERVIX BUCKET:")
print()

all_setups = sorted(set(t["setup_name"] for t in trades_with_ov))
for bname, brange, bfn in analysis_buckets:
    bt = [t for t in trades_with_ov if bfn(t["overvix"])]
    if not bt:
        continue
    bs = compute_bucket_stats(bt, bname)
    print(f"  --- {bname} ({brange}): {bs['n']} trades, WR {bs['wr']:.1f}%, PnL {bs['pnl']:+.1f} ---")
    for setup in all_setups:
        st = [t for t in bt if t["setup_name"] == setup]
        if not st:
            continue
        sw = sum(1 for t in st if t["result"] == "WIN")
        sp = sum(t["pnl"] for t in st)
        swr = sw / len(st) * 100
        print(f"    {setup:<22s} {len(st):>3d}t  WR:{swr:>5.1f}%  PnL:{sp:>+7.1f}")
    print()


# ============================================================================
# PART 3: SMART VIX GATE (Overvix-Aware)
# ============================================================================

print("=" * 120)
print("  PART 3: SMART VIX GATE TESTS (on top of V7+AG)")
print("=" * 120)
print()

def compute_full_metrics(trades, label=""):
    """Comprehensive metrics with daily aggregation, DD, Sharpe."""
    if not trades:
        return {
            "label": label, "n": 0, "wins": 0, "losses": 0,
            "wr": 0.0, "pnl": 0.0, "pf": 0.0,
            "max_dd": 0.0, "sharpe": 0.0,
            "best_day": 0.0, "worst_day": 0.0,
            "avg_daily_pnl": 0.0,
            "per_setup": {},
        }

    n = len(trades)
    wins = sum(1 for t in trades if t["result"] == "WIN")
    losses = sum(1 for t in trades if t["result"] == "LOSS")
    wr = wins / n * 100
    total_pnl = sum(t["pnl"] for t in trades)
    gross_profit = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    pf = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

    # Daily aggregation
    daily = defaultdict(float)
    for t in trades:
        daily[t["date"]] += t["pnl"]
    daily_pnls = [daily[d] for d in sorted(daily.keys())]

    # Max drawdown
    cum = 0.0; peak = 0.0; max_dd = 0.0
    for dp in daily_pnls:
        cum += dp
        if cum > peak: peak = cum
        dd = peak - cum
        if dd > max_dd: max_dd = dd

    # Sharpe
    avg_daily = np.mean(daily_pnls) if daily_pnls else 0.0
    std_daily = np.std(daily_pnls) if len(daily_pnls) > 1 else 0.0
    sharpe = avg_daily / std_daily if std_daily > 0 else 0.0

    best_day = max(daily_pnls) if daily_pnls else 0.0
    worst_day = min(daily_pnls) if daily_pnls else 0.0

    # Per-setup
    per_setup = {}
    for s in sorted(set(t["setup_name"] for t in trades)):
        st = [t for t in trades if t["setup_name"] == s]
        sw = sum(1 for t in st if t["result"] == "WIN")
        sp = sum(t["pnl"] for t in st)
        per_setup[s] = {"n": len(st), "wins": sw, "wr": sw/len(st)*100, "pnl": sp}

    return {
        "label": label, "n": n, "wins": wins, "losses": losses,
        "wr": wr, "pnl": total_pnl, "pf": pf,
        "max_dd": max_dd, "sharpe": sharpe,
        "best_day": best_day, "worst_day": worst_day,
        "avg_daily_pnl": avg_daily,
        "per_setup": per_setup,
    }

# Baseline
baseline_m = compute_full_metrics(trades_with_ov, "V7+AG (no overvix gate)")

# Define all gate tests
gate_tests = [
    ("G1: Block longs VIX>26 AND overvix<+2",
     "Keep longs when VIX<=26 OR overvix>=+2 (save overvix signal longs)",
     lambda t: t["direction"] != "long" or t["vix"] <= 26 or t["overvix"] >= 2),

    ("G2: Block longs VIX>25 AND overvix<+2",
     "Tighter VIX threshold",
     lambda t: t["direction"] != "long" or t["vix"] <= 25 or t["overvix"] >= 2),

    ("G3: Block longs VIX>26 AND overvix<+1",
     "Lower overvix threshold",
     lambda t: t["direction"] != "long" or t["vix"] <= 26 or t["overvix"] >= 1),

    ("G4: Block ALL when overvix < -2",
     "Heavily undervixed = danger zone",
     lambda t: t["overvix"] >= -2),

    ("G5: Block longs when overvix < 0",
     "Undervixed = no longs",
     lambda t: t["direction"] != "long" or t["overvix"] >= 0),

    ("G6: Only longs when overvix > +2",
     "Apollo full signal: ONLY go long when overvixed",
     lambda t: t["direction"] != "long" or t["overvix"] > 2),

    ("G7: Block longs VIX>26 (old A5)",
     "Original VIX gate for comparison",
     lambda t: t["direction"] != "long" or t["vix"] <= 26),

    ("G8: Apollo mode (normal longs OK, VIX>25 needs overvix>+2)",
     "Allow normal longs at low VIX, require overvix signal at high VIX",
     lambda t: t["direction"] != "long" or t["vix"] <= 25 or t["overvix"] > 2),
]

# Run all tests
gate_results = []
for gname, gdesc, gfn in gate_tests:
    filtered = [t for t in trades_with_ov if gfn(t)]
    m = compute_full_metrics(filtered, gname)
    gate_results.append(m)

# Print comparison table
print(f"  {'Gate':<55s} {'#T':>4s} {'WR':>6s} {'PnL':>8s} {'PF':>6s} {'MaxDD':>7s} {'Sharpe':>7s}  "
      f"{'dT':>4s} {'dWR':>6s} {'dPnL':>8s} {'dDD':>7s} {'dSh':>7s}")
print(f"  {'-'*55} {'-'*4} {'-'*6} {'-'*8} {'-'*6} {'-'*7} {'-'*7}  "
      f"{'-'*4} {'-'*6} {'-'*8} {'-'*7} {'-'*7}")

# Baseline row
bm = baseline_m
print(f"  {'>>> V7+AG BASELINE <<<':<55s} {bm['n']:>4d} {bm['wr']:>5.1f}% {bm['pnl']:>+7.1f} {bm['pf']:>6.2f} "
      f"{bm['max_dd']:>7.1f} {bm['sharpe']:>7.2f}  {'---':>4s} {'---':>6s} {'---':>8s} {'---':>7s} {'---':>7s}")
print(f"  {'-'*55} {'-'*4} {'-'*6} {'-'*8} {'-'*6} {'-'*7} {'-'*7}  "
      f"{'-'*4} {'-'*6} {'-'*8} {'-'*7} {'-'*7}")

for m in gate_results:
    if m["n"] == 0:
        print(f"  {m['label']:<55s}    0 trades (all filtered)")
        continue
    dn = m["n"] - bm["n"]
    dwr = m["wr"] - bm["wr"]
    dpnl = m["pnl"] - bm["pnl"]
    ddd = m["max_dd"] - bm["max_dd"]
    dsh = m["sharpe"] - bm["sharpe"]
    flags = ""
    if dpnl > 10: flags += " $"
    if ddd < -3: flags += " D"
    if dsh > 0.03: flags += " S"
    print(f"  {m['label']:<55s} {m['n']:>4d} {m['wr']:>5.1f}% {m['pnl']:>+7.1f} {m['pf']:>6.2f} "
          f"{m['max_dd']:>7.1f} {m['sharpe']:>7.2f}  "
          f"{dn:>+4d} {dwr:>+5.1f}% {dpnl:>+7.1f} {ddd:>+7.1f} {dsh:>+7.2f}{flags}")

print()
print("  Flags: $ = PnL improved >10pts, D = MaxDD reduced >3pts, S = Sharpe improved >0.03")
print()

# Per-setup detail for each gate
for i, m in enumerate(gate_results):
    gname, gdesc, _ = gate_tests[i]
    if m["n"] == 0:
        continue
    print(f"  {gname}: {gdesc}")
    for sname, sd in sorted(m["per_setup"].items()):
        print(f"    {sname:<22s} {sd['n']:>3d}t  WR:{sd['wr']:>5.1f}%  PnL:{sd['pnl']:>+7.1f}")
    print()


# ============================================================================
# PART 4: OVERVIX AS STANDALONE SWING SIGNAL
# ============================================================================

print("=" * 120)
print("  PART 4: OVERVIX AS STANDALONE SWING SIGNAL")
print("=" * 120)
print()

# Build ordered list of dates with SPX close + overvix
swing_data = []
for ds in sorted(overvix_lookup.keys()):
    d = overvix_lookup[ds]
    if d["spx_close"] is not None:
        swing_data.append({
            "date": ds,
            "overvix": d["overvix"],
            "spx_close": d["spx_close"],
        })

# Swing signal: enter long when overvix crosses above +2
# Exit when overvix drops below 0 OR after 5 trading days
ENTRY_THRESHOLD = 2.0
EXIT_THRESHOLD = 0.0
MAX_HOLD_DAYS = 5

swing_trades = []
in_trade = False
entry_price = 0
entry_date = ""
entry_idx = 0
hold_days = 0

for i, sd in enumerate(swing_data):
    if not in_trade:
        # Check for entry: overvix crosses above +2 (previous day was below)
        if sd["overvix"] > ENTRY_THRESHOLD:
            prev_ov = swing_data[i-1]["overvix"] if i > 0 else 0
            if prev_ov <= ENTRY_THRESHOLD:
                # Enter long at this close
                in_trade = True
                entry_price = sd["spx_close"]
                entry_date = sd["date"]
                entry_idx = i
                hold_days = 0
    else:
        hold_days += 1
        # Check exit conditions
        exit_reason = None
        if sd["overvix"] < EXIT_THRESHOLD:
            exit_reason = "overvix < 0"
        elif hold_days >= MAX_HOLD_DAYS:
            exit_reason = f"{MAX_HOLD_DAYS}-day timeout"

        if exit_reason:
            exit_price = sd["spx_close"]
            pnl = exit_price - entry_price
            pnl_pct = pnl / entry_price * 100
            swing_trades.append({
                "entry_date": entry_date,
                "exit_date": sd["date"],
                "entry_price": entry_price,
                "exit_price": exit_price,
                "hold_days": hold_days,
                "pnl_pts": round(pnl, 2),
                "pnl_pct": round(pnl_pct, 3),
                "exit_reason": exit_reason,
            })
            in_trade = False

# If still in a trade at end of data
if in_trade:
    last = swing_data[-1]
    pnl = last["spx_close"] - entry_price
    swing_trades.append({
        "entry_date": entry_date,
        "exit_date": last["date"] + " (open)",
        "entry_price": entry_price,
        "exit_price": last["spx_close"],
        "hold_days": hold_days,
        "pnl_pts": round(pnl, 2),
        "pnl_pct": round(pnl / entry_price * 100, 3),
        "exit_reason": "still open",
    })

print(f"  Signal rule: LONG when overvix crosses above +{ENTRY_THRESHOLD:.0f}")
print(f"  Exit rule:   overvix drops below {EXIT_THRESHOLD:.0f} OR after {MAX_HOLD_DAYS} trading days")
print(f"  P&L: SPX close-to-close")
print()

if swing_trades:
    print(f"  {'Entry Date':<12s} {'Exit Date':<16s} {'Entry':>8s} {'Exit':>8s} {'Hold':>4s} {'P&L pts':>8s} {'P&L %':>7s} {'Exit Reason':<20s}")
    print(f"  {'-'*12} {'-'*16} {'-'*8} {'-'*8} {'-'*4} {'-'*8} {'-'*7} {'-'*20}")

    total_pnl = 0
    total_pct = 0
    n_wins = 0
    for st in swing_trades:
        total_pnl += st["pnl_pts"]
        total_pct += st["pnl_pct"]
        if st["pnl_pts"] > 0:
            n_wins += 1
        print(f"  {st['entry_date']:<12s} {st['exit_date']:<16s} {st['entry_price']:>8.2f} {st['exit_price']:>8.2f} "
              f"{st['hold_days']:>4d} {st['pnl_pts']:>+8.2f} {st['pnl_pct']:>+6.3f}% {st['exit_reason']:<20s}")

    print(f"  {'-'*12} {'-'*16} {'-'*8} {'-'*8} {'-'*4} {'-'*8} {'-'*7}")
    n_st = len(swing_trades)
    avg_hold = np.mean([st["hold_days"] for st in swing_trades])
    wr = n_wins / n_st * 100 if n_st else 0
    print(f"  TOTAL: {n_st} entries, {n_wins} wins ({wr:.0f}% WR), "
          f"avg hold {avg_hold:.1f} days, total P&L: {total_pnl:+.2f} pts ({total_pct:+.3f}%)")
else:
    print("  No swing signals triggered in the data range.")

print()

# Also test alternative thresholds
alt_thresholds = [
    (1.0, 0.0, 5),
    (1.5, 0.0, 5),
    (2.0, 0.0, 3),
    (2.0, 0.0, 10),
    (2.0, -1.0, 5),
    (3.0, 0.0, 5),
]

print("  ALTERNATIVE THRESHOLD SENSITIVITY:")
print(f"  {'Entry>':>6s} {'Exit<':>5s} {'MaxD':>4s}  {'#Sig':>4s} {'Wins':>4s} {'WR':>5s} {'TotalPnL':>10s} {'AvgHold':>7s}")
print(f"  {'-'*6} {'-'*5} {'-'*4}  {'-'*4} {'-'*4} {'-'*5} {'-'*10} {'-'*7}")

for entry_th, exit_th, max_hold in alt_thresholds:
    # Quick simulation
    alt_trades = []
    in_t = False
    for i, sd in enumerate(swing_data):
        if not in_t:
            if sd["overvix"] > entry_th:
                prev_ov = swing_data[i-1]["overvix"] if i > 0 else 0
                if prev_ov <= entry_th:
                    in_t = True
                    ep = sd["spx_close"]
                    ed = sd["date"]
                    hd = 0
        else:
            hd += 1
            if sd["overvix"] < exit_th or hd >= max_hold:
                pnl = sd["spx_close"] - ep
                alt_trades.append({"pnl": pnl, "hold": hd})
                in_t = False
    if in_t:
        pnl = swing_data[-1]["spx_close"] - ep
        alt_trades.append({"pnl": pnl, "hold": hd})

    if alt_trades:
        nw = sum(1 for t in alt_trades if t["pnl"] > 0)
        tp = sum(t["pnl"] for t in alt_trades)
        ah = np.mean([t["hold"] for t in alt_trades])
        wr = nw / len(alt_trades) * 100
        print(f"  {entry_th:>+5.1f} {exit_th:>+5.1f} {max_hold:>4d}  {len(alt_trades):>4d} {nw:>4d} {wr:>4.0f}% {tp:>+10.2f} {ah:>7.1f}")
    else:
        print(f"  {entry_th:>+5.1f} {exit_th:>+5.1f} {max_hold:>4d}     0 signals")
print()


# ============================================================================
# PART 5: COMBINED ANALYSIS — A5 vs Smart A5
# ============================================================================

print("=" * 120)
print("  PART 5: A5 (block longs VIX>26) vs SMART A5 (block longs VIX>26 AND overvix<+2)")
print("=" * 120)
print()

# Build daily comparison
def apply_gate(trades, gate_fn):
    return [t for t in trades if gate_fn(t)]

gate_a5 = lambda t: t["direction"] != "long" or (t["vix"] is not None and t["vix"] <= 26)
gate_smart = lambda t: t["direction"] != "long" or (t["vix"] is not None and (t["vix"] <= 26 or t["overvix"] >= 2))

a5_trades = apply_gate(trades_with_ov, gate_a5)
smart_trades = apply_gate(trades_with_ov, gate_smart)

# Daily P&L for each
daily_base_pnl = defaultdict(float)
daily_base_count = defaultdict(int)
daily_a5_pnl = defaultdict(float)
daily_a5_count = defaultdict(int)
daily_smart_pnl = defaultdict(float)
daily_smart_count = defaultdict(int)

for t in trades_with_ov:
    daily_base_pnl[t["date"]] += t["pnl"]
    daily_base_count[t["date"]] += 1
for t in a5_trades:
    daily_a5_pnl[t["date"]] += t["pnl"]
    daily_a5_count[t["date"]] += 1
for t in smart_trades:
    daily_smart_pnl[t["date"]] += t["pnl"]
    daily_smart_count[t["date"]] += 1

all_dates = sorted(set(t["date"] for t in trades_with_ov))

print(f"  {'Date':<12s} {'OV':>6s} {'VIX':>5s}  "
      f"{'Base#':>5s} {'BasePnL':>8s}  "
      f"{'A5#':>3s} {'A5_PnL':>8s} {'dA5':>7s}  "
      f"{'Smart#':>6s} {'SmPnL':>8s} {'dSmart':>7s}  {'Saved?':<30s}")
print(f"  {'-'*12} {'-'*6} {'-'*5}  "
      f"{'-'*5} {'-'*8}  "
      f"{'-'*3} {'-'*8} {'-'*7}  "
      f"{'-'*6} {'-'*8} {'-'*7}  {'-'*30}")

cum_base = 0.0
cum_a5 = 0.0
cum_smart = 0.0

for d in all_dates:
    ov_data = overvix_lookup.get(d, {})
    ov = ov_data.get("overvix", 0)
    vix = ov_data.get("vix", 0)

    bp = daily_base_pnl.get(d, 0)
    bc = daily_base_count.get(d, 0)
    ap = daily_a5_pnl.get(d, 0)
    ac = daily_a5_count.get(d, 0)
    sp = daily_smart_pnl.get(d, 0)
    sc = daily_smart_count.get(d, 0)

    cum_base += bp
    cum_a5 += ap
    cum_smart += sp

    da5 = ap - bp
    dsm = sp - bp

    # Identify saved/missed trades
    saved = ""
    if sc > ac:
        extra_pnl = sp - ap
        saved = f"+{sc-ac}t saved ({extra_pnl:+.1f}pt)"
    elif ac > sc:
        saved = f"A5 blocked more"

    print(f"  {d:<12s} {ov:>+6.2f} {vix:>5.1f}  "
          f"{bc:>5d} {bp:>+7.1f}  "
          f"{ac:>3d} {ap:>+7.1f} {da5:>+6.1f}  "
          f"{sc:>6d} {sp:>+7.1f} {dsm:>+6.1f}  {saved:<30s}")

print(f"  {'-'*12} {'-'*6} {'-'*5}  "
      f"{'-'*5} {'-'*8}  "
      f"{'-'*3} {'-'*8} {'-'*7}  "
      f"{'-'*6} {'-'*8} {'-'*7}")
print(f"  {'TOTAL':<12s} {'':>6s} {'':>5s}  "
      f"{'':>5s} {cum_base:>+7.1f}  "
      f"{'':>3s} {cum_a5:>+7.1f} {cum_a5-cum_base:>+6.1f}  "
      f"{'':>6s} {cum_smart:>+7.1f} {cum_smart-cum_base:>+6.1f}")
print()

# Show which specific trades differ
print("  TRADES SAVED BY SMART A5 (that A5 would block):")
a5_blocked = set()
for t in trades_with_ov:
    if not gate_a5(t):
        a5_blocked.add((t["date"], t["setup_name"], t["direction"], t["pnl"], t["result"]))

smart_blocked = set()
for t in trades_with_ov:
    if not gate_smart(t):
        smart_blocked.add((t["date"], t["setup_name"], t["direction"], t["pnl"], t["result"]))

# Trades that A5 blocks but Smart saves
saved_by_smart = a5_blocked - smart_blocked
if saved_by_smart:
    saved_list = sorted(saved_by_smart, key=lambda x: x[0])
    print(f"  {'Date':<12s} {'Setup':<22s} {'Dir':<6s} {'Result':<8s} {'PnL':>7s}")
    print(f"  {'-'*12} {'-'*22} {'-'*6} {'-'*8} {'-'*7}")
    total_saved_pnl = 0
    for d, sn, dr, pnl, res in saved_list:
        total_saved_pnl += pnl
        print(f"  {d:<12s} {sn:<22s} {dr:<6s} {res:<8s} {pnl:>+7.1f}")
    print(f"  {'':>12s} {'':>22s} {'':>6s} {'TOTAL':>8s} {total_saved_pnl:>+7.1f}")
else:
    print("  None — both gates block the same trades.")
print()

# Trades that Smart blocks but A5 allows
extra_blocked = smart_blocked - a5_blocked
if extra_blocked:
    extra_list = sorted(extra_blocked, key=lambda x: x[0])
    print("  TRADES SMART A5 BLOCKS THAT A5 ALLOWS:")
    print(f"  {'Date':<12s} {'Setup':<22s} {'Dir':<6s} {'Result':<8s} {'PnL':>7s}")
    print(f"  {'-'*12} {'-'*22} {'-'*6} {'-'*8} {'-'*7}")
    for d, sn, dr, pnl, res in extra_list:
        print(f"  {d:<12s} {sn:<22s} {dr:<6s} {res:<8s} {pnl:>+7.1f}")
else:
    print("  Smart A5 does not block any trades that A5 allows (strictly less restrictive).")
print()

# Key observation: check if A5 and Smart A5 are identical
if len(a5_trades) == len(smart_trades) and abs(sum(t["pnl"] for t in a5_trades) - sum(t["pnl"] for t in smart_trades)) < 0.01:
    print("  *** KEY FINDING: A5 and Smart A5 produce IDENTICAL results ***")
    print("  This means there were NO days with VIX > 26 AND overvix >= +2 in our trading period.")
    print("  The overvix >= +2 exception never triggered because when VIX was > 26,")
    print("  VIX3M was also high (overvix stayed below +2).")
    # Find the high-VIX days and show their overvix
    high_vix_dates = [d for d in all_dates if overvix_lookup.get(d, {}).get("vix", 0) > 26]
    if high_vix_dates:
        print(f"  High-VIX days (>26) in our data:")
        for d in high_vix_dates:
            od = overvix_lookup.get(d, {})
            print(f"    {d}: VIX={od.get('vix',0):.1f}, VIX3M={od.get('vix3m',0):.1f}, overvix={od.get('overvix',0):+.2f}")
    print()

# Side-by-side metrics
m_base = compute_full_metrics(trades_with_ov, "Baseline V7+AG")
m_a5 = compute_full_metrics(a5_trades, "A5 (block longs VIX>26)")
m_smart = compute_full_metrics(smart_trades, "Smart A5 (VIX>26 AND overvix<+2)")

print("  SIDE-BY-SIDE METRICS:")
print(f"  {'Metric':<25s} {'Baseline':>12s} {'A5':>12s} {'Smart A5':>12s} {'A5 delta':>10s} {'Smart delta':>12s}")
print(f"  {'-'*25} {'-'*12} {'-'*12} {'-'*12} {'-'*10} {'-'*12}")

metrics_to_show = [
    ("Trades", "n", "{:d}", True),
    ("Win Rate %", "wr", "{:.1f}", True),
    ("Total PnL", "pnl", "{:+.1f}", True),
    ("Profit Factor", "pf", "{:.2f}", True),
    ("Max Drawdown", "max_dd", "{:.1f}", False),
    ("Sharpe", "sharpe", "{:.3f}", True),
    ("Best Day", "best_day", "{:+.1f}", True),
    ("Worst Day", "worst_day", "{:+.1f}", False),
    ("Avg Daily PnL", "avg_daily_pnl", "{:+.1f}", True),
]

for mname, mkey, mfmt, higher_better in metrics_to_show:
    bv = m_base[mkey]
    av = m_a5[mkey]
    sv = m_smart[mkey]
    da = av - bv
    ds = sv - bv
    print(f"  {mname:<25s} {mfmt.format(bv):>12s} {mfmt.format(av):>12s} {mfmt.format(sv):>12s} "
          f"{mfmt.format(da):>10s} {mfmt.format(ds):>12s}")
print()


# ============================================================================
# PART 6: KEY FINDINGS & RECOMMENDATION
# ============================================================================

print("=" * 120)
print("  PART 6: KEY FINDINGS & RECOMMENDATION")
print("=" * 120)
print()

# Compute some key datapoints for the conclusion
# How many trades are in overvix > +2?
ov_gt2_trades = [t for t in trades_with_ov if t["overvix"] > 2]
ov_gt2_m = compute_full_metrics(ov_gt2_trades, "overvix > +2")
# How many in overvix < -2?
ov_lt_m2_trades = [t for t in trades_with_ov if t["overvix"] < -2]
ov_lt_m2_m = compute_full_metrics(ov_lt_m2_trades, "overvix < -2")

# Normal days (overvix between -1 and +1)
ov_neutral_trades = [t for t in trades_with_ov if -1 <= t["overvix"] <= 1]
ov_neutral_m = compute_full_metrics(ov_neutral_trades, "neutral")

# How often is overvix > +2 in our trading period?
our_dates = sorted(set(t["date"] for t in trades_with_ov))
n_our_dates = len(our_dates)
n_ov_gt2_dates = len([d for d in our_dates if overvix_lookup.get(d, {}).get("overvix", 0) > 2])

# Best gate
best_gate_idx = max(range(len(gate_results)), key=lambda i: gate_results[i]["pnl"]) if gate_results else 0
best_gate = gate_results[best_gate_idx] if gate_results else None
best_strictly_dominant = None
for m in gate_results:
    if m["n"] == 0:
        continue
    dpnl = m["pnl"] - bm["pnl"]
    ddd = m["max_dd"] - bm["max_dd"]
    dsh = m["sharpe"] - bm["sharpe"]
    if dpnl > 0 and ddd < 0 and dsh > 0:
        if best_strictly_dominant is None or dpnl > (best_strictly_dominant["pnl"] - bm["pnl"]):
            best_strictly_dominant = m

print("  1. OVERVIX AS A MARKET REGIME INDICATOR")
print("  " + "-" * 50)
print(f"     In our trading period ({our_dates[0]} to {our_dates[-1]}), {n_our_dates} trading days:")
print(f"     - Overvix > +2 (signal territory): {n_ov_gt2_dates} days ({n_ov_gt2_dates/n_our_dates*100:.0f}%)")
if ov_gt2_m["n"] > 0:
    print(f"       Trades: {ov_gt2_m['n']}, WR: {ov_gt2_m['wr']:.1f}%, PnL: {ov_gt2_m['pnl']:+.1f}")
if ov_lt_m2_m["n"] > 0:
    print(f"     - Overvix < -2 (danger zone): trades={ov_lt_m2_m['n']}, WR: {ov_lt_m2_m['wr']:.1f}%, PnL: {ov_lt_m2_m['pnl']:+.1f}")
if ov_neutral_m["n"] > 0:
    print(f"     - Neutral (-1 to +1): trades={ov_neutral_m['n']}, WR: {ov_neutral_m['wr']:.1f}%, PnL: {ov_neutral_m['pnl']:+.1f}")
print()

print("  2. OVERVIX vs RAW VIX AS A GATE")
print("  " + "-" * 50)
print(f"     A5 (raw VIX>26 block longs):   {m_a5['n']}t, PnL {m_a5['pnl']:+.1f}, Sharpe {m_a5['sharpe']:.3f}")
print(f"     Smart A5 (VIX>26 + overvix<2): {m_smart['n']}t, PnL {m_smart['pnl']:+.1f}, Sharpe {m_smart['sharpe']:.3f}")
delta_pnl = m_smart["pnl"] - m_a5["pnl"]
if delta_pnl > 0:
    print(f"     Smart A5 is BETTER by {delta_pnl:+.1f} pts (saves profitable longs on overvixed days)")
elif delta_pnl < 0:
    print(f"     A5 is better by {-delta_pnl:+.1f} pts (saved longs on overvixed days were net losers)")
else:
    print(f"     No difference (no high-VIX overvixed days in the data)")
print()

if best_strictly_dominant:
    print(f"  3. BEST STRICTLY DOMINANT GATE (better PnL + lower DD + better Sharpe):")
    print(f"     {best_strictly_dominant['label']}")
    print(f"     Trades: {best_strictly_dominant['n']}, WR: {best_strictly_dominant['wr']:.1f}%, PnL: {best_strictly_dominant['pnl']:+.1f}")
    print(f"     PF: {best_strictly_dominant['pf']:.2f}, MaxDD: {best_strictly_dominant['max_dd']:.1f}, Sharpe: {best_strictly_dominant['sharpe']:.3f}")
else:
    print("  3. NO STRICTLY DOMINANT GATE")
    print("     No gate improved PnL + DD + Sharpe simultaneously.")
    if best_gate and best_gate["n"] > 0:
        print(f"     Best PnL gate: {best_gate['label']}")
        print(f"     Trades: {best_gate['n']}, PnL: {best_gate['pnl']:+.1f} (delta {best_gate['pnl']-bm['pnl']:+.1f})")
print()

print("  4. OVERVIX AS A STANDALONE SWING SIGNAL")
print("  " + "-" * 50)
if swing_trades:
    total_swing_pnl = sum(st["pnl_pts"] for st in swing_trades)
    n_sw = len(swing_trades)
    sw_wins = sum(1 for st in swing_trades if st["pnl_pts"] > 0)
    sw_wr = sw_wins / n_sw * 100 if n_sw else 0
    print(f"     {n_sw} signals since Jan 2024, {sw_wins} wins ({sw_wr:.0f}% WR)")
    print(f"     Total P&L: {total_swing_pnl:+.2f} SPX pts")
    if total_swing_pnl > 0:
        print(f"     POTENTIALLY VIABLE as a swing overlay (needs more data)")
    else:
        print(f"     NOT VIABLE — negative total P&L")
else:
    print("     No signals triggered — overvix rarely crosses +2 in this period")
print()

print("  5. RECOMMENDATION")
print("  " + "-" * 50)

# Determine recommendation based on data
recommendations = []

# Check the real driver: was it overvix or just VIX level?
g1_pnl = gate_results[0]["pnl"] if gate_results[0]["n"] > 0 else 0  # G1: smart VIX gate
g7_pnl = gate_results[6]["pnl"] if gate_results[6]["n"] > 0 else 0  # G7: old A5
if abs(g1_pnl - g7_pnl) < 1.0:
    recommendations.append(
        "KEY INSIGHT: Smart A5 (overvix-aware) and plain A5 (VIX>26) are IDENTICAL in our data. "
        "The improvement comes from blocking high-VIX longs, NOT from overvix signal. "
        "Overvix >= +2 never coincided with VIX > 26 in our period."
    )
    recommendations.append(
        f"IMPLEMENT A5 NOW: Blocking longs at VIX > 26 adds +{g7_pnl - bm['pnl']:.0f} pts, "
        f"cuts MaxDD from {bm['max_dd']:.0f} to {gate_results[6]['max_dd']:.0f}, "
        f"Sharpe {bm['sharpe']:.2f} -> {gate_results[6]['sharpe']:.2f}. Massive improvement."
    )
else:
    best_improvement = max((m["pnl"] - bm["pnl"] for m in gate_results if m["n"] > 0), default=0)
    if best_improvement > 30:
        recommendations.append(f"IMPLEMENT: An overvix gate adds {best_improvement:+.1f} pts to V7+AG.")
    elif best_improvement > 10:
        recommendations.append(f"CONSIDER: Marginal improvement of {best_improvement:+.1f} pts. Monitor more data.")
    else:
        recommendations.append("HOLD: No overvix gate materially improves V7+AG in current data.")

# Check overvix signal quality
if ov_gt2_m["n"] >= 10 and ov_gt2_m["wr"] > 65:
    recommendations.append(f"CONFIRM: Overvix > +2 days show {ov_gt2_m['wr']:.0f}% WR. Signal is valid.")
elif ov_gt2_m["n"] < 10:
    recommendations.append(f"INSUFFICIENT DATA: Only {ov_gt2_m['n']} trades on overvix>+2 days. Need more data to validate Apollo's signal.")

# Check undervix danger
if ov_lt_m2_m["n"] >= 5 and ov_lt_m2_m["wr"] < 50:
    recommendations.append(f"DANGER ZONE: Overvix < -2 shows {ov_lt_m2_m['wr']:.0f}% WR. Consider blocking.")

# Overvix as logging
recommendations.append(
    "LOG OVERVIX: Add overvix value to Telegram trade alerts. "
    "When we eventually get overvix > +2 on a high-VIX day, we'll have real data to validate the signal."
)

for i, rec in enumerate(recommendations):
    print(f"     {i+1}. {rec}")

print()
print("  6. EXACT THRESHOLDS TO USE (if implementing)")
print("  " + "-" * 50)
print("     OPTION A (conservative): Keep V7+AG as-is, add overvix as logging only")
print("       - Log overvix value on each trade in Telegram for pattern recognition")
print("       - Collect 50+ more trading days of data before gating")
print()
print("     OPTION B (moderate): Smart VIX gate")
print("       - Block longs when VIX > 26 AND overvix < +2")
print("       - This keeps the proven A5 VIX gate but unblocks profitable longs")
print("         on overvixed days (when mean reversion favors longs)")
print()
print("     OPTION C (aggressive): Overvix-only gate")
print("       - Block longs when overvix < 0 (undervixed days)")
print("       - Block ALL when overvix < -2 (heavily undervixed)")
print("       - Only recommended with 100+ data points in each bucket")
print()
print("     OPTION D (Apollo mode): VIX-conditional overvix")
print("       - Normal trading when VIX <= 25 (no gate)")
print("       - When VIX > 25: only allow longs if overvix > +2")
print("       - Shorts always allowed")

print()
print("=" * 120)
print("  Done. Run this script again after collecting more overvixed trading days.")
print("=" * 120)
print()
