"""V9 vs V8 Backtest using historical VIX3M data.
Reads trade outcomes from API + daily VIX3M from Yahoo Finance.
Compares V8 (VIX>26 gate) vs V9 (VIX>22 gate) with overvix override."""

import json, csv
from collections import defaultdict

# Load trades
with open(r"C:\Users\Faisa\AppData\Local\Temp\all_trades.json") as f:
    trades = json.load(f)

# Load VIX3M daily data
vix3m_by_date = {}
with open(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\tmp_vix_history.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        vix3m_by_date[row["Date"]] = {
            "vix_close": float(row["VIX_Close"]),
            "vix3m_close": float(row["VIX3M_Close"]),
            "overvix": float(row["Overvix"]),
        }

# Enrich trades with overvix
enriched = []
for t in trades:
    date = t.get("date", "")
    vix = t.get("vix")
    result = t.get("result")
    pnl = t.get("pnl", 0)
    if result not in ("WIN", "LOSS"):
        continue  # skip EXPIRED

    # Get VIX3M for this date
    daily = vix3m_by_date.get(date)
    if daily and vix is not None:
        overvix = vix - daily["vix3m_close"]  # intraday VIX - daily VIX3M close
    elif daily:
        vix = daily["vix_close"]  # fallback to daily VIX close
        overvix = daily["overvix"]
    else:
        overvix = None

    t["overvix_calc"] = overvix
    t["vix_used"] = vix
    enriched.append(t)

print(f"Total trades with outcomes (WIN/LOSS): {len(enriched)}")
print(f"Trades with VIX data: {sum(1 for t in enriched if t['vix_used'] is not None)}")
print(f"Trades with overvix: {sum(1 for t in enriched if t['overvix_calc'] is not None)}")
print()

# ── Filter functions ────────────────────────────────────────────────────
def passes_v7ag(t):
    """V7+AG: longs align>=+2, shorts whitelist SC/AG/DD(align!=0)"""
    align = t.get("alignment", 0) or 0
    direction = t.get("direction", "")
    sname = t.get("setup_name", "")
    is_long = direction in ("long", "bullish")

    if is_long:
        return align >= 2
    else:
        if sname == "Skew Charm": return True
        if sname == "AG Short": return True
        if sname == "DD Exhaustion" and align != 0: return True
        return False

def passes_v8(t):
    """V8: V7+AG + VIX gate at 26 with overvix override"""
    if not passes_v7ag(t):
        return False
    direction = t.get("direction", "")
    is_long = direction in ("long", "bullish")
    vix = t.get("vix_used")
    ov = t.get("overvix_calc")

    if is_long and vix is not None and vix > 26:
        if ov is None or ov < 2:
            return False
    return True

def passes_v9(t):
    """V9: V7+AG + VIX gate at 22 with overvix override"""
    if not passes_v7ag(t):
        return False
    direction = t.get("direction", "")
    is_long = direction in ("long", "bullish")
    vix = t.get("vix_used")
    ov = t.get("overvix_calc")

    if is_long and vix is not None and vix > 22:
        if ov is None or ov < 2:
            return False
    return True

def passes_v9_24(t):
    """V9-24: V7+AG + VIX gate at 24 with overvix override"""
    if not passes_v7ag(t):
        return False
    direction = t.get("direction", "")
    is_long = direction in ("long", "bullish")
    vix = t.get("vix_used")
    ov = t.get("overvix_calc")

    if is_long and vix is not None and vix > 24:
        if ov is None or ov < 2:
            return False
    return True

def passes_v9_20(t):
    """V9-20: V7+AG + VIX gate at 20 with overvix override"""
    if not passes_v7ag(t):
        return False
    direction = t.get("direction", "")
    is_long = direction in ("long", "bullish")
    vix = t.get("vix_used")
    ov = t.get("overvix_calc")

    if is_long and vix is not None and vix > 20:
        if ov is None or ov < 2:
            return False
    return True

# ── Analyze ─────────────────────────────────────────────────────────────
def analyze(label, filter_fn):
    passed = [t for t in enriched if filter_fn(t)]
    blocked = [t for t in enriched if not filter_fn(t)]
    wins = sum(1 for t in passed if t["result"] == "WIN")
    losses = sum(1 for t in passed if t["result"] == "LOSS")
    total_pnl = sum(t.get("pnl", 0) for t in passed)
    blocked_pnl = sum(t.get("pnl", 0) for t in blocked)
    wr = wins / len(passed) * 100 if passed else 0

    # Daily P&L for max drawdown and Sharpe
    daily_pnl = defaultdict(float)
    for t in passed:
        daily_pnl[t["date"]] += t.get("pnl", 0)

    days = sorted(daily_pnl.keys())
    cumulative = 0
    peak = 0
    max_dd = 0
    daily_vals = []
    for d in days:
        cumulative += daily_pnl[d]
        daily_vals.append(daily_pnl[d])
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    import statistics
    sharpe = 0
    if daily_vals and len(daily_vals) > 1:
        mean = statistics.mean(daily_vals)
        std = statistics.stdev(daily_vals)
        if std > 0:
            sharpe = mean / std * (252 ** 0.5)  # annualized

    pf = 0
    gross_wins = sum(t.get("pnl", 0) for t in passed if t["result"] == "WIN")
    gross_losses = abs(sum(t.get("pnl", 0) for t in passed if t["result"] == "LOSS"))
    if gross_losses > 0:
        pf = gross_wins / gross_losses

    return {
        "label": label,
        "trades": len(passed),
        "wins": wins,
        "losses": losses,
        "wr": wr,
        "pnl": total_pnl,
        "blocked": len(blocked),
        "blocked_pnl": blocked_pnl,
        "max_dd": max_dd,
        "sharpe": sharpe,
        "pf": pf,
        "daily_pnl": daily_pnl,
    }

# ── Run all filters ─────────────────────────────────────────────────────
filters = [
    ("Unfiltered", lambda t: True),
    ("V7+AG", passes_v7ag),
    ("V8 (VIX>26)", passes_v8),
    ("V9-20 (VIX>20)", passes_v9_20),
    ("V9-22 (VIX>22) [DEPLOYED]", passes_v9),
    ("V9-24 (VIX>24)", passes_v9_24),
]

results = []
for label, fn in filters:
    results.append(analyze(label, fn))

# ── Print comparison table ──────────────────────────────────────────────
print("=" * 120)
print(f"{'Filter':<30} {'Trades':>7} {'W':>5} {'L':>5} {'WR%':>7} {'PnL':>8} {'PF':>6} {'MaxDD':>7} {'Sharpe':>7} {'Blocked':>8} {'Blk PnL':>8}")
print("=" * 120)
for r in results:
    print(f"{r['label']:<30} {r['trades']:>7} {r['wins']:>5} {r['losses']:>5} "
          f"{r['wr']:>6.1f}% {r['pnl']:>+8.1f} {r['pf']:>5.2f}x {r['max_dd']:>7.1f} {r['sharpe']:>7.2f} "
          f"{r['blocked']:>8} {r['blocked_pnl']:>+8.1f}")
print("=" * 120)

# ── V8 vs V9 delta ─────────────────────────────────────────────────────
v8 = next(r for r in results if "V8" in r["label"])
v9 = next(r for r in results if "DEPLOYED" in r["label"])
print(f"\n{'V9 vs V8 DELTA:':<30} {v9['trades']-v8['trades']:>+7} {v9['wins']-v8['wins']:>+5} {v9['losses']-v8['losses']:>+5} "
      f"{v9['wr']-v8['wr']:>+6.1f}% {v9['pnl']-v8['pnl']:>+8.1f} "
      f"{'':>6} {v8['max_dd']-v9['max_dd']:>+7.1f} {v9['sharpe']-v8['sharpe']:>+7.2f}")

# ── What V9 blocks that V8 doesn't ─────────────────────────────────────
print("\n\n=== TRADES BLOCKED BY V9 BUT ALLOWED BY V8 ===")
v9_only_blocked = [t for t in enriched if passes_v8(t) and not passes_v9(t)]
print(f"Total: {len(v9_only_blocked)} trades")
wins_blocked = sum(1 for t in v9_only_blocked if t["result"] == "WIN")
losses_blocked = sum(1 for t in v9_only_blocked if t["result"] == "LOSS")
pnl_blocked = sum(t.get("pnl", 0) for t in v9_only_blocked)
print(f"  Wins blocked: {wins_blocked}, Losses blocked: {losses_blocked}")
print(f"  PnL of blocked trades: {pnl_blocked:+.1f} pts")
if v9_only_blocked:
    wr_blocked = wins_blocked / len(v9_only_blocked) * 100
    print(f"  WR of blocked trades: {wr_blocked:.1f}%")

# By setup
by_setup = defaultdict(list)
for t in v9_only_blocked:
    by_setup[t["setup_name"]].append(t)

print("\n  Per-setup breakdown of V9-blocked trades:")
for sname in sorted(by_setup.keys()):
    trades_s = by_setup[sname]
    w = sum(1 for t in trades_s if t["result"] == "WIN")
    l = sum(1 for t in trades_s if t["result"] == "LOSS")
    p = sum(t.get("pnl", 0) for t in trades_s)
    wr_s = w / len(trades_s) * 100 if trades_s else 0
    print(f"    {sname:<25} {len(trades_s):>3}t  {w}W/{l}L  {wr_s:.0f}% WR  {p:+.1f} pts")

# By date
print("\n  Per-date breakdown of V9-blocked trades:")
by_date = defaultdict(list)
for t in v9_only_blocked:
    by_date[t["date"]].append(t)
for d in sorted(by_date.keys()):
    trades_d = by_date[d]
    w = sum(1 for t in trades_d if t["result"] == "WIN")
    l = sum(1 for t in trades_d if t["result"] == "LOSS")
    p = sum(t.get("pnl", 0) for t in trades_d)
    vix_d = vix3m_by_date.get(d, {})
    vix_str = f"VIX={vix_d.get('vix_close','?'):.1f} OV={vix_d.get('overvix','?'):+.2f}" if vix_d else ""
    print(f"    {d}  {len(trades_d):>3}t  {w}W/{l}L  {p:+.1f} pts  {vix_str}")

# ── What V8 blocks that V9 doesn't (should be none) ────────────────────
v8_only_blocked = [t for t in enriched if passes_v9(t) and not passes_v8(t)]
if v8_only_blocked:
    print(f"\nWARNING: {len(v8_only_blocked)} trades pass V9 but fail V8 — this shouldn't happen!")

# ── Daily P&L comparison ────────────────────────────────────────────────
print("\n\n=== DAILY P&L: V8 vs V9 ===")
all_dates = sorted(set(t["date"] for t in enriched))
print(f"{'Date':<12} {'V8 PnL':>8} {'V9 PnL':>8} {'Delta':>8} {'V8 Cum':>8} {'V9 Cum':>8}")
v8_cum = 0
v9_cum = 0
for d in all_dates:
    v8_day = v8["daily_pnl"].get(d, 0)
    v9_day = v9["daily_pnl"].get(d, 0)
    v8_cum += v8_day
    v9_cum += v9_day
    delta = v9_day - v8_day
    marker = " <<<" if abs(delta) > 10 else ""
    print(f"{d:<12} {v8_day:>+8.1f} {v9_day:>+8.1f} {delta:>+8.1f} {v8_cum:>+8.1f} {v9_cum:>+8.1f}{marker}")

print(f"\n{'TOTAL':<12} {v8['pnl']:>+8.1f} {v9['pnl']:>+8.1f} {v9['pnl']-v8['pnl']:>+8.1f}")
