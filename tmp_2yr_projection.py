"""
2-Year Scaling Projection: 1 MES SC-only → All Setups → ES contracts.

Conservative assumptions:
- Use 65% of March backtest (not every month is high-vol March)
- Bad months: assume 2 per year at 30% of normal
- MaxDD rule: never exceed 30% of capital → scale up only when capital > 3x MaxDD of new config
- Compounding: reinvest profits, scale when thresholds met

March actuals (V10, cap=2):
- SC-only 1 MES:     +$2,905/15d → $4,067/21d → conservative $2,600/mo
- All setups 1 MES:  +$4,754/15d → $6,655/21d → conservative $4,300/mo
- MaxDD SC-only:     $636 (1 MES)
- MaxDD all setups:  $1,014 (1 MES) → ~$550 (cap=2 sim)

Per-contract scaling:
- MES: $5/pt, margin ~$2,737
- ES:  $50/pt, margin ~$15,000 (= 10 MES)
"""

# ---- CONFIG ----
# Conservative monthly PnL per MES contract
SC_ONLY_PER_MES = 2600       # 65% of $4,067
ALL_SETUPS_PER_MES = 4300    # 65% of $6,655

# MaxDD per MES contract
SC_ONLY_DD_PER_MES = 640     # from backtest
ALL_SETUPS_DD_PER_MES = 1020 # from backtest

# Margin per contract
MES_MARGIN = 2737
ES_MARGIN = 15000

# Scale-up rule: capital must be > 3x MaxDD of new config
SAFETY_MULT = 3.0

# Bad months: 2 per year at 30% of normal PnL
BAD_MONTH_RATE = 2/12  # probability of bad month
BAD_MONTH_MULT = 0.30

# Variance: apply random-ish pattern (good/bad months)
# Use a fixed pattern for reproducibility
# 1.0 = normal, 0.3 = bad, 1.3 = great
MONTH_PATTERN = [
    1.0, 1.1, 0.9, 1.2, 0.3, 1.0,   # months 1-6
    1.1, 0.8, 1.3, 0.3, 1.0, 1.2,   # months 7-12
    0.9, 1.1, 1.0, 1.3, 0.3, 0.8,   # months 13-18
    1.2, 1.0, 0.3, 1.1, 1.0, 1.2,   # months 19-24
]

# ---- SCALING LOGIC ----
def get_config(capital):
    """Determine optimal config based on capital."""
    configs = []

    # Try ES contracts (descending)
    for es_qty in range(10, 0, -1):
        margin = es_qty * ES_MARGIN
        dd = es_qty * ALL_SETUPS_DD_PER_MES * 10  # ES = 10x MES
        monthly = es_qty * ALL_SETUPS_PER_MES * 10
        if capital >= margin * 1.5 and capital >= dd * SAFETY_MULT:
            configs.append({
                "instrument": "ES", "qty": es_qty, "setups": "All",
                "monthly": monthly, "max_dd": dd, "margin": margin
            })
            break

    # Try MES contracts (descending)
    for mes_qty in range(20, 0, -1):
        # All setups
        margin = mes_qty * MES_MARGIN
        dd = mes_qty * ALL_SETUPS_DD_PER_MES
        monthly = mes_qty * ALL_SETUPS_PER_MES
        if capital >= margin * 1.5 and capital >= dd * SAFETY_MULT:
            configs.append({
                "instrument": "MES", "qty": mes_qty, "setups": "All",
                "monthly": monthly, "max_dd": dd, "margin": margin
            })
            break

    # Try MES SC-only (descending)
    for mes_qty in range(20, 0, -1):
        margin = mes_qty * MES_MARGIN
        dd = mes_qty * SC_ONLY_DD_PER_MES
        monthly = mes_qty * SC_ONLY_PER_MES
        if capital >= margin * 1.5 and capital >= dd * SAFETY_MULT:
            configs.append({
                "instrument": "MES", "qty": mes_qty, "setups": "SC",
                "monthly": monthly, "max_dd": dd, "margin": margin
            })
            break

    if not configs:
        # Minimum: 1 MES SC-only
        return {
            "instrument": "MES", "qty": 1, "setups": "SC",
            "monthly": SC_ONLY_PER_MES, "max_dd": SC_ONLY_DD_PER_MES,
            "margin": MES_MARGIN
        }

    # Pick the best config (highest monthly PnL that fits risk rules)
    return max(configs, key=lambda c: c["monthly"])


# ---- RUN PROJECTION ----
starting_capital = 5000  # start with $5K
capital = starting_capital

print("="*130)
print(f"2-YEAR SCALING PROJECTION — Starting Capital: ${starting_capital:,}")
print(f"Conservative: 65% of March backtest, 2 bad months/year, scale when capital > 3x MaxDD")
print("="*130)

print(f"\n{'Mo':>3s} | {'Capital':>10s} | {'Config':>20s} | {'Qty':>4s} | {'Base $/mo':>10s} | {'Variance':>8s} | {'Actual $':>10s} | {'MaxDD':>8s} | {'DD%Cap':>6s} | {'New Cap':>10s} | {'ROI%':>6s}")
print("-"*130)

total_pnl = 0
peak_capital = capital
max_dd_capital = 0

for month in range(1, 25):
    cfg = get_config(capital)
    base_monthly = cfg["monthly"]
    variance = MONTH_PATTERN[month - 1]
    actual_pnl = round(base_monthly * variance)
    dd_pct = round(cfg["max_dd"] / capital * 100)
    new_capital = capital + actual_pnl
    total_pnl += actual_pnl

    if new_capital > peak_capital:
        peak_capital = new_capital
    dd_from_peak = peak_capital - new_capital
    if dd_from_peak > max_dd_capital:
        max_dd_capital = dd_from_peak

    config_str = f"{cfg['instrument']} {cfg['setups']}"
    roi_pct = round(actual_pnl / capital * 100)

    var_str = f"x{variance:.1f}"
    if variance <= 0.3:
        var_str += " BAD"
    elif variance >= 1.2:
        var_str += " GOOD"

    print(f" {month:3d} | ${capital:>9,} | {config_str:>20s} | {cfg['qty']:4d} | ${base_monthly:>9,} | {var_str:>8s} | ${actual_pnl:>+9,} | ${cfg['max_dd']:>7,} | {dd_pct:>5d}% | ${new_capital:>9,} | {roi_pct:>+5d}%")

    capital = new_capital

print("-"*130)
print(f"\n  SUMMARY")
print(f"  {'Starting Capital':25s}: ${starting_capital:,}")
print(f"  {'Final Capital':25s}: ${capital:,}")
print(f"  {'Total Profit':25s}: ${total_pnl:,}")
print(f"  {'Total Return':25s}: {total_pnl/starting_capital*100:,.0f}%")
print(f"  {'Max DD from Peak':25s}: ${max_dd_capital:,}")
print(f"  {'Avg Monthly Return':25s}: ${total_pnl//24:,}")

# ---- MILESTONES ----
print(f"\n{'='*130}")
print("KEY MILESTONES")
print("="*130)

capital = starting_capital
milestones = {
    "First $10K": 10000,
    "First $25K": 25000,
    "First $50K": 50000,
    "First $100K": 100000,
    "First $250K": 250000,
    "First $500K": 500000,
    "First $1M": 1000000,
}
hit = set()
for month in range(1, 25):
    cfg = get_config(capital)
    actual_pnl = round(cfg["monthly"] * MONTH_PATTERN[month - 1])
    capital += actual_pnl
    for name, threshold in milestones.items():
        if capital >= threshold and name not in hit:
            hit.add(name)
            print(f"  Month {month:2d}: {name:15s} — capital ${capital:>10,} ({cfg['instrument']} x{cfg['qty']} {cfg['setups']})")

# ---- ALTERNATIVE: More conservative start ----
print(f"\n{'='*130}")
print("ALTERNATIVE: Start $3K (minimum viable)")
print("="*130)

capital = 3000
print(f"\n{'Mo':>3s} | {'Capital':>10s} | {'Config':>20s} | {'Qty':>4s} | {'Actual $':>10s} | {'New Cap':>10s}")
print("-"*80)
for month in range(1, 25):
    cfg = get_config(capital)
    actual_pnl = round(cfg["monthly"] * MONTH_PATTERN[month - 1])
    new_capital = capital + actual_pnl
    config_str = f"{cfg['instrument']} {cfg['setups']}"
    print(f" {month:3d} | ${capital:>9,} | {config_str:>20s} | {cfg['qty']:4d} | ${actual_pnl:>+9,} | ${new_capital:>9,}")
    capital = new_capital
print(f"\n  Final: ${capital:,}")
