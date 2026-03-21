"""V8 Account Growth Projection — auto-scaling when capital allows."""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# From our backtest (V8, Mar 1-13, real option prices, 1 SPY per signal)
CAPITAL_PER_QTY = 3447  # account needed per 1 SPY contract (max day cap + worst day buffer)
PNL_PER_QTY_MONTHLY = 3135  # monthly P&L per 1 SPY contract
WORST_DAY_PER_QTY = 432  # worst single day loss per 1 SPY

# Starting capital scenarios
for start_capital in [3500, 5000, 7000, 10000]:
    print(f"\n{'='*85}")
    print(f" STARTING CAPITAL: ${start_capital:,}")
    print(f"{'='*85}")
    print(f"\n  {'Month':>5s} {'Qty':>4s} {'Start Bal':>11s} {'Monthly PnL':>12s} {'End Bal':>11s} {'Worst Day':>10s} {'Cumul PnL':>11s}")
    print(f"  {'-'*5} {'-'*4} {'-'*11} {'-'*12} {'-'*11} {'-'*10} {'-'*11}")

    balance = float(start_capital)
    cumul_pnl = 0
    for month in range(1, 25):  # 24 months
        # How many SPY contracts can we trade?
        qty = max(1, int(balance // CAPITAL_PER_QTY))
        # Cap at reasonable max
        qty = min(qty, 50)

        monthly_pnl = PNL_PER_QTY_MONTHLY * qty
        worst_day = WORST_DAY_PER_QTY * qty
        end_balance = balance + monthly_pnl
        cumul_pnl += monthly_pnl

        print(f"  {month:>5d} {qty:>4d} ${balance:>10,.0f} ${monthly_pnl:>+11,.0f} ${end_balance:>10,.0f} ${worst_day:>9,.0f} ${cumul_pnl:>+10,.0f}")

        balance = end_balance

        # Milestone markers
        if balance >= 100000 and balance - monthly_pnl < 100000:
            print(f"  {'':>5s} {'':>4s} {'':>11s}   >>> $100K REACHED <<<")
        if balance >= 500000 and balance - monthly_pnl < 500000:
            print(f"  {'':>5s} {'':>4s} {'':>11s}   >>> $500K REACHED <<<")
        if balance >= 1000000 and balance - monthly_pnl < 1000000:
            print(f"  {'':>5s} {'':>4s} {'':>11s}   >>> $1M REACHED <<<")

    print(f"\n  Final balance after 24 months: ${balance:>,.0f}")
    print(f"  Total profit: ${cumul_pnl:>,.0f}")
    print(f"  Initial investment: ${start_capital:>,.0f}")
    print(f"  Return on investment: {cumul_pnl/start_capital*100:,.0f}%")

# ========== CONSERVATIVE SCENARIO (50% of backtest performance) ==========
print(f"\n\n{'#'*85}")
print(f" CONSERVATIVE SCENARIO — 50% of backtest performance (slippage, bad months, etc.)")
print(f"{'#'*85}")

CONSERVATIVE_PNL = PNL_PER_QTY_MONTHLY * 0.5  # $1,568/month per qty

for start_capital in [3500, 5000]:
    print(f"\n{'='*85}")
    print(f" STARTING CAPITAL: ${start_capital:,} (CONSERVATIVE)")
    print(f"{'='*85}")
    print(f"\n  {'Month':>5s} {'Qty':>4s} {'Start Bal':>11s} {'Monthly PnL':>12s} {'End Bal':>11s} {'Cumul PnL':>11s}")
    print(f"  {'-'*5} {'-'*4} {'-'*11} {'-'*12} {'-'*11} {'-'*11}")

    balance = float(start_capital)
    cumul_pnl = 0
    for month in range(1, 25):
        qty = max(1, int(balance // CAPITAL_PER_QTY))
        qty = min(qty, 50)
        monthly_pnl = CONSERVATIVE_PNL * qty
        end_balance = balance + monthly_pnl
        cumul_pnl += monthly_pnl
        print(f"  {month:>5d} {qty:>4d} ${balance:>10,.0f} ${monthly_pnl:>+11,.0f} ${end_balance:>10,.0f} ${cumul_pnl:>+10,.0f}")
        balance = end_balance

    print(f"\n  Final balance after 24 months: ${balance:>,.0f}")
    print(f"  Total profit: ${cumul_pnl:>,.0f}")

# ========== KEY MILESTONES ==========
print(f"\n\n{'='*85}")
print(f" KEY MILESTONES — Starting $5,000")
print(f"{'='*85}")

for label, pnl_mult in [("Full performance", 1.0), ("75% performance", 0.75), ("50% performance", 0.5)]:
    balance = 5000.0
    pnl_per = PNL_PER_QTY_MONTHLY * pnl_mult
    targets = [10000, 25000, 50000, 100000, 250000, 500000, 1000000]
    target_idx = 0
    print(f"\n  {label} (${pnl_per:,.0f}/month per SPY):")
    for month in range(1, 61):
        qty = max(1, int(balance // CAPITAL_PER_QTY))
        qty = min(qty, 50)
        balance += pnl_per * qty
        while target_idx < len(targets) and balance >= targets[target_idx]:
            print(f"    ${targets[target_idx]:>10,} reached in month {month:>2d} ({month/12:.1f} yr) — trading {qty} SPY")
            target_idx += 1
        if target_idx >= len(targets):
            break
    if target_idx < len(targets):
        print(f"    (${targets[target_idx]:,} not reached in 60 months)")

print("\nDone.")
