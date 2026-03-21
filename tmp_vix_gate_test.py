"""VIX Gate Backtest — test VIX-based trading gates on top of V7+AG filter.

Data sources:
  - Setup outcomes: https://0dtealpha.com/api/debug/gex-analysis
  - VIX daily:  yfinance ^VIX
  - SPX daily:  yfinance ^GSPC

Usage:
  pip install requests yfinance pandas
  python tmp_vix_gate_test.py
"""
import sys, io, os, math
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import requests
import yfinance as yf
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta

# ============================================================================
# 1. FETCH DATA
# ============================================================================

print("Fetching setup outcomes from API...", flush=True)
resp = requests.get("https://0dtealpha.com/api/debug/gex-analysis", timeout=30)
resp.raise_for_status()
data = resp.json()
outcomes = data["setup_outcomes"]
print(f"  Got {len(outcomes)} raw trades")

# Normalize direction
for t in outcomes:
    if t["direction"] == "bullish":
        t["direction"] = "long"
    elif t["direction"] == "bearish":
        t["direction"] = "short"

# Parse dates
dates = sorted(set(t["date"] for t in outcomes))
min_date = dates[0]
max_date = dates[-1]
print(f"  Date range: {min_date} to {max_date}")

# Fetch VIX + SPX daily data (with buffer for change calcs)
fetch_start = (datetime.strptime(min_date, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
fetch_end = (datetime.strptime(max_date, "%Y-%m-%d") + timedelta(days=3)).strftime("%Y-%m-%d")

print(f"Fetching VIX daily ({fetch_start} to {fetch_end})...", flush=True)
vix_df = yf.download("^VIX", start=fetch_start, end=fetch_end, progress=False)
print(f"  Got {len(vix_df)} VIX rows")

print(f"Fetching SPX daily ({fetch_start} to {fetch_end})...", flush=True)
spx_df = yf.download("^GSPC", start=fetch_start, end=fetch_end, progress=False)
print(f"  Got {len(spx_df)} SPX rows")

# Build VIX lookup: date_str -> {close, prev_close, pct_change, up_days}
vix_lookup = {}
vix_dates = sorted(vix_df.index)
for i, dt in enumerate(vix_dates):
    ds = dt.strftime("%Y-%m-%d")
    # Handle both single-level and multi-level column index from yfinance
    try:
        close = float(vix_df.loc[dt, "Close"].iloc[0]) if hasattr(vix_df.loc[dt, "Close"], 'iloc') else float(vix_df.loc[dt, "Close"])
    except (IndexError, TypeError):
        close = float(vix_df.loc[dt, "Close"])
    prev_close = None
    pct_change = 0.0
    consecutive_up = 0
    if i > 0:
        prev_dt = vix_dates[i - 1]
        try:
            prev_close = float(vix_df.loc[prev_dt, "Close"].iloc[0]) if hasattr(vix_df.loc[prev_dt, "Close"], 'iloc') else float(vix_df.loc[prev_dt, "Close"])
        except (IndexError, TypeError):
            prev_close = float(vix_df.loc[prev_dt, "Close"])
        if prev_close and prev_close > 0:
            pct_change = (close - prev_close) / prev_close * 100
        # Count consecutive up days
        for j in range(i, -1, -1):
            dt_j = vix_dates[j]
            try:
                c_j = float(vix_df.loc[dt_j, "Close"].iloc[0]) if hasattr(vix_df.loc[dt_j, "Close"], 'iloc') else float(vix_df.loc[dt_j, "Close"])
            except (IndexError, TypeError):
                c_j = float(vix_df.loc[dt_j, "Close"])
            if j == 0:
                break
            dt_prev = vix_dates[j - 1]
            try:
                c_prev = float(vix_df.loc[dt_prev, "Close"].iloc[0]) if hasattr(vix_df.loc[dt_prev, "Close"], 'iloc') else float(vix_df.loc[dt_prev, "Close"])
            except (IndexError, TypeError):
                c_prev = float(vix_df.loc[dt_prev, "Close"])
            if c_j > c_prev:
                consecutive_up += 1
            else:
                break

    vix_lookup[ds] = {
        "close": close,
        "prev_close": prev_close,
        "pct_change": pct_change,
        "consecutive_up": consecutive_up,
    }

# Attach VIX data to each trade
missing_vix = 0
for t in outcomes:
    v = vix_lookup.get(t["date"])
    if v:
        t["vix"] = v["close"]
        t["vix_pct_change"] = v["pct_change"]
        t["vix_consecutive_up"] = v["consecutive_up"]
    else:
        t["vix"] = None
        t["vix_pct_change"] = 0.0
        t["vix_consecutive_up"] = 0
        missing_vix += 1

if missing_vix:
    print(f"  Warning: {missing_vix} trades had no VIX data for their date")

# Show VIX range
vix_values = [t["vix"] for t in outcomes if t["vix"] is not None]
print(f"  VIX range in data: {min(vix_values):.1f} - {max(vix_values):.1f}")
print()

# ============================================================================
# 2. V7+AG FILTER (baseline)
# ============================================================================

def passes_v7ag(t):
    sn = t.get("setup_name", "")
    direction = t.get("direction", "")
    alignment = t.get("alignment")
    if alignment is None:
        return False

    # Longs: alignment >= +2
    if direction in ("long", "bullish"):
        return alignment >= 2

    # Shorts whitelist only:
    if direction in ("short", "bearish"):
        if sn == "Skew Charm":
            return True  # all SC shorts
        if sn == "DD Exhaustion":
            return alignment != 0  # block align=0
        if sn == "AG Short":
            return True  # all AG shorts
        return False  # block ES Abs, BofA, Paradigm Rev shorts

    return False


baseline_trades = [t for t in outcomes if passes_v7ag(t)]
print(f"V7+AG baseline: {len(baseline_trades)} trades (from {len(outcomes)} total)")
print()

# ============================================================================
# 3. METRICS COMPUTATION
# ============================================================================

def compute_metrics(trades, label=""):
    """Compute comprehensive metrics for a trade set."""
    if not trades:
        return {
            "label": label, "n": 0, "wins": 0, "losses": 0, "expired": 0,
            "wr": 0.0, "pnl": 0.0, "avg_pnl": 0.0, "pf": 0.0,
            "max_dd": 0.0, "max_consec_loss_days": 0,
            "sharpe": 0.0, "best_day": 0.0, "worst_day": 0.0,
            "avg_daily_pnl": 0.0, "std_daily_pnl": 0.0,
            "per_setup": {},
        }

    n = len(trades)
    wins = sum(1 for t in trades if t["result"] == "WIN")
    losses = sum(1 for t in trades if t["result"] == "LOSS")
    expired = sum(1 for t in trades if t["result"] == "EXPIRED")
    wr = wins / n * 100 if n > 0 else 0.0
    total_pnl = sum(t["pnl"] for t in trades)
    avg_pnl = total_pnl / n if n > 0 else 0.0

    gross_profit = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    pf = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

    # Daily aggregation
    daily = defaultdict(float)
    for t in trades:
        daily[t["date"]] += t["pnl"]

    all_dates = sorted(daily.keys())
    daily_pnls = [daily[d] for d in all_dates]

    # Max drawdown (cumulative)
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for d_pnl in daily_pnls:
        cum += d_pnl
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd

    # Max consecutive losing days
    max_consec = 0
    consec = 0
    for d_pnl in daily_pnls:
        if d_pnl < 0:
            consec += 1
            if consec > max_consec:
                max_consec = consec
        else:
            consec = 0

    best_day = max(daily_pnls) if daily_pnls else 0.0
    worst_day = min(daily_pnls) if daily_pnls else 0.0
    avg_daily = sum(daily_pnls) / len(daily_pnls) if daily_pnls else 0.0
    std_daily = (sum((x - avg_daily) ** 2 for x in daily_pnls) / len(daily_pnls)) ** 0.5 if len(daily_pnls) > 1 else 0.0
    sharpe = avg_daily / std_daily if std_daily > 0 else 0.0

    # Per-setup breakdown
    per_setup = {}
    setups = sorted(set(t["setup_name"] for t in trades))
    for s in setups:
        st = [t for t in trades if t["setup_name"] == s]
        sw = sum(1 for t in st if t["result"] == "WIN")
        sp = sum(t["pnl"] for t in st)
        per_setup[s] = {"n": len(st), "wins": sw, "wr": sw / len(st) * 100, "pnl": sp}

    return {
        "label": label,
        "n": n, "wins": wins, "losses": losses, "expired": expired,
        "wr": wr, "pnl": total_pnl, "avg_pnl": avg_pnl, "pf": pf,
        "max_dd": max_dd, "max_consec_loss_days": max_consec,
        "sharpe": sharpe, "best_day": best_day, "worst_day": worst_day,
        "avg_daily_pnl": avg_daily, "std_daily_pnl": std_daily,
        "per_setup": per_setup,
    }


def print_metrics(m, baseline_m=None, show_per_setup=True):
    """Print metrics in a clean format, optionally showing delta from baseline."""
    label = m["label"]
    n = m["n"]
    if n == 0:
        print(f"  {label:<50s}   0 trades (all filtered)")
        return

    line = (
        f"  {label:<50s} "
        f"{n:>4d}t  WR:{m['wr']:>5.1f}%  "
        f"PnL:{m['pnl']:>+8.1f}  PF:{m['pf']:>5.2f}  "
        f"DD:{m['max_dd']:>6.1f}  "
        f"Sharpe:{m['sharpe']:>5.2f}  "
        f"Best:{m['best_day']:>+6.1f}  Worst:{m['worst_day']:>+7.1f}"
    )
    print(line)

    if baseline_m and baseline_m["n"] > 0:
        dn = n - baseline_m["n"]
        dwr = m["wr"] - baseline_m["wr"]
        dpnl = m["pnl"] - baseline_m["pnl"]
        dpf = m["pf"] - baseline_m["pf"]
        ddd = m["max_dd"] - baseline_m["max_dd"]
        dsh = m["sharpe"] - baseline_m["sharpe"]
        markers = []
        if dpnl > 0:
            markers.append("PnL+")
        if ddd < 0:
            markers.append("DD-")
        if dsh > 0:
            markers.append("Sharpe+")
        marker_str = "  *** " + ", ".join(markers) + " ***" if markers else ""
        delta_line = (
            f"  {'  delta:':<50s} "
            f"{dn:>+4d}t  WR:{dwr:>+5.1f}%  "
            f"PnL:{dpnl:>+8.1f}  PF:{dpf:>+5.2f}  "
            f"DD:{ddd:>+6.1f}  "
            f"Sharpe:{dsh:>+5.2f}"
            f"{marker_str}"
        )
        print(delta_line)

    if show_per_setup and m["per_setup"]:
        for sname, sd in sorted(m["per_setup"].items()):
            print(
                f"    {sname:<25s} {sd['n']:>3d}t  "
                f"WR:{sd['wr']:>5.1f}%  PnL:{sd['pnl']:>+7.1f}"
            )


# ============================================================================
# 4. DEFINE ALL GATE TESTS
# ============================================================================

def make_gate(name, gate_fn):
    """Apply gate_fn on top of V7+AG baseline. gate_fn(trade) -> True = keep."""
    filtered = [t for t in baseline_trades if gate_fn(t)]
    return compute_metrics(filtered, label=name)


# Pre-compute baseline metrics
baseline_m = compute_metrics(baseline_trades, label="V7+AG (no VIX gate)")

# === Section A: Simple VIX Level Gates ===
gates_a = [
    ("A1: Block ALL when VIX > 25",
     lambda t: t["vix"] is not None and t["vix"] <= 25),
    ("A2: Block ALL when VIX > 26",
     lambda t: t["vix"] is not None and t["vix"] <= 26),
    ("A3: Block ALL when VIX > 28",
     lambda t: t["vix"] is not None and t["vix"] <= 28),
    ("A4: Block LONGS only when VIX > 25",
     lambda t: t["vix"] is not None and (t["direction"] == "short" or t["vix"] <= 25)),
    ("A5: Block LONGS only when VIX > 26",
     lambda t: t["vix"] is not None and (t["direction"] == "short" or t["vix"] <= 26)),
    ("A6: Block LONGS only when VIX > 28",
     lambda t: t["vix"] is not None and (t["direction"] == "short" or t["vix"] <= 28)),
    ("A7: Only trade when VIX 18-25",
     lambda t: t["vix"] is not None and 18 <= t["vix"] <= 25),
    ("A8: Only trade when VIX 20-24",
     lambda t: t["vix"] is not None and 20 <= t["vix"] <= 24),
]

# === Section B: VIX Change Gates ===
gates_b = [
    ("B1: Block longs when VIX rose > 5%",
     lambda t: not (t["direction"] == "long" and t["vix_pct_change"] > 5)),
    ("B2: Block longs when VIX rose > 10%",
     lambda t: not (t["direction"] == "long" and t["vix_pct_change"] > 10)),
    ("B3: Block ALL when VIX rose > 10%",
     lambda t: t["vix_pct_change"] <= 10),
    ("B4: Block longs when VIX up 2+ consec days",
     lambda t: not (t["direction"] == "long" and t["vix_consecutive_up"] >= 2)),
]

# === Section C: VIX + SVB Combined Gates ===
gates_c = [
    ("C1: Block longs when VIX>25 AND SVB<0",
     lambda t: not (t["direction"] == "long" and t["vix"] is not None and t["vix"] > 25 and t.get("svb") is not None and t["svb"] < 0)),
    ("C2: Block ALL when VIX>25 AND SVB<-0.5",
     lambda t: not (t["vix"] is not None and t["vix"] > 25 and t.get("svb") is not None and t["svb"] < -0.5)),
    ("C3: Only trade when VIX<25 OR SVB>0.5",
     lambda t: (t["vix"] is not None and t["vix"] < 25) or (t.get("svb") is not None and t["svb"] > 0.5)),
]

# === Section D: VIX Regime Detection ===
def regime_gate(t):
    """D1-D4 combined regime: Calm=skip, Normal=full, Elevated=shorts only, Crisis=no trade."""
    v = t["vix"]
    if v is None:
        return False
    if v < 20:
        return False  # Calm: skip
    elif v <= 25:
        return True  # Normal: full trading
    elif v <= 30:
        return t["direction"] == "short"  # Elevated: shorts only
    else:
        return False  # Crisis: no trading

gates_d = [
    ("D-Full: Calm=skip, Normal=trade, Elev=shorts, Crisis=skip", regime_gate),
]

# Also test individual regime segments for reference
gates_d_detail = [
    ("D-Calm only (VIX < 20)",
     lambda t: t["vix"] is not None and t["vix"] < 20),
    ("D-Normal only (VIX 20-25)",
     lambda t: t["vix"] is not None and 20 <= t["vix"] <= 25),
    ("D-Elevated only (VIX 25-30)",
     lambda t: t["vix"] is not None and 25 < t["vix"] <= 30),
    ("D-Crisis only (VIX > 30)",
     lambda t: t["vix"] is not None and t["vix"] > 30),
]


# ============================================================================
# 5. RUN ALL TESTS
# ============================================================================

print("=" * 130)
print("VIX GATE BACKTEST RESULTS")
print(f"Data: {min_date} to {max_date}  |  Total outcomes: {len(outcomes)}  |  V7+AG baseline: {len(baseline_trades)} trades")
print("=" * 130)
print()

# -- Baseline --
print("-" * 130)
print("BASELINE (V7+AG, no VIX gate)")
print("-" * 130)
print_metrics(baseline_m, show_per_setup=True)
print()

# VIX distribution for context
print("-" * 130)
print("VIX DISTRIBUTION ON TRADING DAYS")
print("-" * 130)
vix_by_date = {}
for t in baseline_trades:
    if t["vix"] is not None:
        vix_by_date[t["date"]] = t["vix"]
vix_days = sorted(vix_by_date.items())
brackets = [
    ("< 18", 0, 18),
    ("18-20", 18, 20),
    ("20-22", 20, 22),
    ("22-24", 22, 24),
    ("24-26", 24, 26),
    ("26-28", 26, 28),
    ("28-30", 28, 30),
    ("> 30", 30, 999),
]
for bname, lo, hi in brackets:
    days_in = [(d, v) for d, v in vix_days if lo <= v < hi]
    trades_in = [t for t in baseline_trades if t["vix"] is not None and lo <= t["vix"] < hi]
    wins_in = sum(1 for t in trades_in if t["result"] == "WIN")
    pnl_in = sum(t["pnl"] for t in trades_in)
    wr_in = wins_in / len(trades_in) * 100 if trades_in else 0
    print(
        f"  VIX {bname:<6s}: {len(days_in):>2d} days, {len(trades_in):>3d} trades, "
        f"WR:{wr_in:>5.1f}%, PnL:{pnl_in:>+7.1f}"
    )
print()

# -- Section A --
print("=" * 130)
print("SECTION A: SIMPLE VIX LEVEL GATES")
print("=" * 130)
results_a = []
for name, gate_fn in gates_a:
    m = make_gate(name, gate_fn)
    results_a.append(m)
    print_metrics(m, baseline_m=baseline_m, show_per_setup=True)
    print()

# -- Section B --
print("=" * 130)
print("SECTION B: VIX CHANGE GATES (daily VIX % change)")
print("=" * 130)
results_b = []
for name, gate_fn in gates_b:
    m = make_gate(name, gate_fn)
    results_b.append(m)
    print_metrics(m, baseline_m=baseline_m, show_per_setup=True)
    print()

# -- Section C --
print("=" * 130)
print("SECTION C: VIX + SVB COMBINED GATES")
print("=" * 130)
results_c = []
for name, gate_fn in gates_c:
    m = make_gate(name, gate_fn)
    results_c.append(m)
    print_metrics(m, baseline_m=baseline_m, show_per_setup=True)
    print()

# -- Section D --
print("=" * 130)
print("SECTION D: VIX REGIME DETECTION")
print("=" * 130)
print()
print("--- Individual Regime Performance ---")
for name, gate_fn in gates_d_detail:
    m = make_gate(name, gate_fn)
    print_metrics(m, baseline_m=baseline_m, show_per_setup=False)
    print()

print("--- Combined Regime Strategy ---")
results_d = []
for name, gate_fn in gates_d:
    m = make_gate(name, gate_fn)
    results_d.append(m)
    print_metrics(m, baseline_m=baseline_m, show_per_setup=True)
    print()

# ============================================================================
# 6. SUMMARY TABLE
# ============================================================================

all_results = results_a + results_b + results_c + results_d

print()
print("=" * 150)
print("SUMMARY TABLE — ALL GATES vs V7+AG BASELINE")
print("=" * 150)
print(
    f"  {'Gate':<55s} "
    f"{'Trades':>6s} {'WR%':>6s} {'PnL':>8s} {'PF':>6s} "
    f"{'MaxDD':>7s} {'Sharpe':>7s} "
    f"{'dTrades':>7s} {'dWR%':>6s} {'dPnL':>8s} {'dDD':>7s} {'dSharpe':>7s}"
)
print(
    f"  {'-'*55} "
    f"{'-'*6} {'-'*6} {'-'*8} {'-'*6} "
    f"{'-'*7} {'-'*7} "
    f"{'-'*7} {'-'*6} {'-'*8} {'-'*7} {'-'*7}"
)
# Baseline row
print(
    f"  {'>>> V7+AG BASELINE <<<':<55s} "
    f"{baseline_m['n']:>6d} {baseline_m['wr']:>5.1f}% {baseline_m['pnl']:>+7.1f} {baseline_m['pf']:>6.2f} "
    f"{baseline_m['max_dd']:>7.1f} {baseline_m['sharpe']:>7.2f} "
    f"{'---':>7s} {'---':>6s} {'---':>8s} {'---':>7s} {'---':>7s}"
)
print(f"  {'-'*55} {'-'*6} {'-'*6} {'-'*8} {'-'*6} {'-'*7} {'-'*7} {'-'*7} {'-'*6} {'-'*8} {'-'*7} {'-'*7}")

for m in all_results:
    if m["n"] == 0:
        print(f"  {m['label']:<55s}      0 trades (all filtered)")
        continue
    dn = m["n"] - baseline_m["n"]
    dwr = m["wr"] - baseline_m["wr"]
    dpnl = m["pnl"] - baseline_m["pnl"]
    ddd = m["max_dd"] - baseline_m["max_dd"]
    dsh = m["sharpe"] - baseline_m["sharpe"]
    # Flag improvements
    flags = ""
    if dpnl > 20:
        flags += " $"
    if ddd < -5:
        flags += " D"
    if dsh > 0.05:
        flags += " S"
    print(
        f"  {m['label']:<55s} "
        f"{m['n']:>6d} {m['wr']:>5.1f}% {m['pnl']:>+7.1f} {m['pf']:>6.2f} "
        f"{m['max_dd']:>7.1f} {m['sharpe']:>7.2f} "
        f"{dn:>+7d} {dwr:>+5.1f}% {dpnl:>+7.1f} {ddd:>+7.1f} {dsh:>+7.2f}"
        f"{flags}"
    )

print()
print("  Flags: $ = PnL improved >20pts, D = MaxDD reduced >5pts, S = Sharpe improved >0.05")
print()

# ============================================================================
# 7. DAILY BREAKDOWN FOR TOP CANDIDATES
# ============================================================================

# Rank by composite score: +1 for PnL improvement, +1 for DD reduction, +1 for Sharpe improvement
ranked = []
for m in all_results:
    if m["n"] == 0:
        continue
    dpnl = m["pnl"] - baseline_m["pnl"]
    ddd = m["max_dd"] - baseline_m["max_dd"]
    dsh = m["sharpe"] - baseline_m["sharpe"]
    # Normalize scores relative to baseline
    pnl_score = dpnl / max(abs(baseline_m["pnl"]), 1)
    dd_score = -ddd / max(baseline_m["max_dd"], 1)  # negative DD change = good
    sharpe_score = dsh / max(abs(baseline_m["sharpe"]), 0.01)
    composite = pnl_score + dd_score + sharpe_score
    ranked.append((composite, m, dpnl, ddd, dsh))

ranked.sort(key=lambda x: x[0], reverse=True)

print("=" * 130)
print("TOP 5 GATES BY COMPOSITE SCORE (PnL + DD reduction + Sharpe)")
print("=" * 130)
for i, (score, m, dpnl, ddd, dsh) in enumerate(ranked[:5]):
    print(f"\n  #{i+1}: {m['label']}")
    print(f"       Trades: {m['n']}  WR: {m['wr']:.1f}%  PnL: {m['pnl']:+.1f}  PF: {m['pf']:.2f}")
    print(f"       MaxDD: {m['max_dd']:.1f}  Sharpe: {m['sharpe']:.2f}")
    print(f"       vs baseline: PnL {dpnl:+.1f}, DD {ddd:+.1f}, Sharpe {dsh:+.2f}, composite={score:.3f}")
print()

# Daily P&L comparison for top 3
print("=" * 130)
print("DAILY P&L COMPARISON: Baseline vs Top 3 Gates")
print("=" * 130)

top3_gates = ranked[:3]
# Re-apply gate filters for daily breakdown
gate_fns_map = {}
for name, fn in gates_a + gates_b + gates_c + gates_d:
    gate_fns_map[name] = fn

# Collect all trading days
all_trading_dates = sorted(set(t["date"] for t in baseline_trades))

print(
    f"  {'Date':<12s} {'Baseline':>10s} ",
    end=""
)
for _, m, _, _, _ in top3_gates:
    print(f" {m['label'][:25]:>25s}", end="")
print()
print(f"  {'-'*12} {'-'*10} ", end="")
for _ in top3_gates:
    print(f" {'-'*25}", end="")
print()

# Build daily PnL for each gate
daily_base = defaultdict(float)
for t in baseline_trades:
    daily_base[t["date"]] += t["pnl"]

daily_gate = []
for _, m, _, _, _ in top3_gates:
    gate_fn = gate_fns_map.get(m["label"])
    if gate_fn:
        filt = [t for t in baseline_trades if gate_fn(t)]
    else:
        filt = baseline_trades
    dd = defaultdict(float)
    for t in filt:
        dd[t["date"]] += t["pnl"]
    daily_gate.append(dd)

cum_base = 0.0
cum_gates = [0.0] * len(top3_gates)
for d in all_trading_dates:
    bp = daily_base.get(d, 0)
    cum_base += bp
    print(f"  {d:<12s} {bp:>+8.1f}pt ", end="")
    for gi, (_, m, _, _, _) in enumerate(top3_gates):
        gp = daily_gate[gi].get(d, 0)
        cum_gates[gi] += gp
        diff = gp - bp
        diff_str = f"({diff:+.1f})" if abs(diff) > 0.01 else ""
        print(f" {gp:>+8.1f}pt {diff_str:>10s}", end="")
    print()

print(f"  {'-'*12} {'-'*10} ", end="")
for _ in top3_gates:
    print(f" {'-'*25}", end="")
print()
print(f"  {'TOTAL':<12s} {cum_base:>+8.1f}pt ", end="")
for gi in range(len(top3_gates)):
    print(f" {cum_gates[gi]:>+8.1f}pt {'':>10s}", end="")
print()
print()

# ============================================================================
# 8. RECOMMENDATIONS
# ============================================================================

print("=" * 130)
print("RECOMMENDATIONS")
print("=" * 130)
print()

# Best PnL improvement
best_pnl = max(ranked, key=lambda x: x[2]) if ranked else None
# Best DD reduction
best_dd = min(ranked, key=lambda x: x[3]) if ranked else None
# Best Sharpe
best_sharpe = max(ranked, key=lambda x: x[4]) if ranked else None
# Best composite
best_composite = ranked[0] if ranked else None

if best_pnl:
    score, m, dpnl, ddd, dsh = best_pnl
    print(f"  1. BEST PnL IMPROVEMENT: {m['label']}")
    print(f"     PnL: {m['pnl']:+.1f} (delta {dpnl:+.1f}), WR: {m['wr']:.1f}%, PF: {m['pf']:.2f}")
    print(f"     MaxDD: {m['max_dd']:.1f} (delta {ddd:+.1f}), Sharpe: {m['sharpe']:.2f} (delta {dsh:+.2f})")
    print(f"     Trades removed: {baseline_m['n'] - m['n']}")
    print()

if best_dd:
    score, m, dpnl, ddd, dsh = best_dd
    print(f"  2. BEST DRAWDOWN REDUCTION: {m['label']}")
    print(f"     MaxDD: {m['max_dd']:.1f} (delta {ddd:+.1f}), PnL: {m['pnl']:+.1f} (delta {dpnl:+.1f})")
    print(f"     WR: {m['wr']:.1f}%, PF: {m['pf']:.2f}, Sharpe: {m['sharpe']:.2f}")
    print(f"     Trades removed: {baseline_m['n'] - m['n']}")
    print()

if best_sharpe:
    score, m, dpnl, ddd, dsh = best_sharpe
    print(f"  3. BEST RISK-ADJUSTED RETURN (Sharpe): {m['label']}")
    print(f"     Sharpe: {m['sharpe']:.2f} (delta {dsh:+.2f}), PnL: {m['pnl']:+.1f} (delta {dpnl:+.1f})")
    print(f"     MaxDD: {m['max_dd']:.1f} (delta {ddd:+.1f}), WR: {m['wr']:.1f}%, PF: {m['pf']:.2f}")
    print(f"     Trades removed: {baseline_m['n'] - m['n']}")
    print()

if best_composite:
    score, m, dpnl, ddd, dsh = best_composite
    print(f"  4. BEST OVERALL (composite): {m['label']}")
    print(f"     Composite score: {score:.3f}")
    print(f"     PnL: {m['pnl']:+.1f} ({dpnl:+.1f}), MaxDD: {m['max_dd']:.1f} ({ddd:+.1f}), Sharpe: {m['sharpe']:.2f} ({dsh:+.2f})")
    print(f"     Trades: {m['n']} ({baseline_m['n'] - m['n']} removed), WR: {m['wr']:.1f}%, PF: {m['pf']:.2f}")
    print()

# Check if any gate is strictly better (more PnL AND less DD AND better Sharpe)
strictly_better = []
for score, m, dpnl, ddd, dsh in ranked:
    if dpnl > 0 and ddd < 0 and dsh > 0:
        strictly_better.append((m["label"], dpnl, ddd, dsh))

if strictly_better:
    print("  STRICTLY DOMINANT GATES (better PnL + less DD + better Sharpe):")
    for label, dpnl, ddd, dsh in strictly_better:
        print(f"    - {label}:  PnL {dpnl:+.1f}, DD {ddd:+.1f}, Sharpe {dsh:+.2f}")
else:
    print("  No gate is strictly dominant on all 3 metrics.")
    print("  All gates involve a trade-off (e.g. better PnL but worse DD, or vice versa).")

print()
print("=" * 130)
print("NOTES:")
print("  - VIX data is daily close, not intraday. Intraday VIX levels may differ from daily close.")
print("  - SVB (Spot-Vol-Beta) is from Volland; many early trades have SVB=null.")
print("  - Sample size matters: gates that remove many trades may be unreliable.")
print("  - All gates are applied ON TOP of V7+AG baseline (not replacing it).")
print("=" * 130)
print()
print("Done.", flush=True)
