"""
Realistic 1-year plan: 30% of March performance as baseline.
March V10 all setups: +922.8 pts, MaxDD 202.8 pts

30% baseline:
- Monthly PnL per MES: 922.8 * 0.30 / 15 * 21 = ~388 pts/mo = $1,940/MES
- MaxDD per MES: 202.8 * 0.30 = ~61 pts = $305/MES
  (but keep full MaxDD for risk calc since bad months cluster)

Conservative risk rules:
- Use 30% of March for PnL projection
- Use 100% of March MaxDD for risk (DD doesn't scale down with PnL)
- Scale when capital > 4x MaxDD (extra conservative for real money)
- Include 2 flat months per year (holidays, low vol, system issues)
"""

MES_PT = 5.0
ES_PT = 50.0
MES_MARGIN = 2737
ES_MARGIN = 15000

# March actuals
MARCH_PNL_PTS = 922.8   # 15 trading days
MARCH_DD_PTS = 202.8    # trade-by-trade MaxDD
MARCH_DAYS = 15

# Conservative: 30% of March for PnL
MONTHLY_PTS_PER_MES = MARCH_PNL_PTS * 0.30 / MARCH_DAYS * 21  # ~387 pts
MONTHLY_USD_PER_MES = MONTHLY_PTS_PER_MES * MES_PT

# Keep full MaxDD for risk (worst case doesn't shrink)
DD_PER_MES = MARCH_DD_PTS * MES_PT  # $1,014

SAFETY_MULT = 4.0  # capital must be > 4x MaxDD

# Month pattern: include 2 flat months (0.1x)
PATTERN = [
    0.8, 1.0, 1.2, 0.1, 1.0, 0.9,  # months 1-6
    1.1, 0.8, 0.1, 1.0, 1.2, 1.0,  # months 7-12
]

def get_config(capital):
    """Conservative scaling: only scale when capital > 4x MaxDD."""
    best = None

    # Try ES (1 ES = 10 MES equivalent)
    for es_qty in range(10, 0, -1):
        total_dd = es_qty * DD_PER_MES * 10
        margin = es_qty * ES_MARGIN
        monthly = es_qty * MONTHLY_USD_PER_MES * 10
        if capital >= max(margin * 2, total_dd * SAFETY_MULT):
            cfg = {"inst": "ES", "qty": es_qty, "monthly": monthly,
                   "dd": total_dd, "margin": margin}
            if best is None or cfg["monthly"] > best["monthly"]:
                best = cfg
            break

    # Try MES
    for mes_qty in range(20, 0, -1):
        total_dd = mes_qty * DD_PER_MES
        margin = mes_qty * MES_MARGIN
        monthly = mes_qty * MONTHLY_USD_PER_MES
        if capital >= max(margin * 2, total_dd * SAFETY_MULT):
            cfg = {"inst": "MES", "qty": mes_qty, "monthly": monthly,
                   "dd": total_dd, "margin": margin}
            if best is None or cfg["monthly"] > best["monthly"]:
                best = cfg
            break

    if best is None:
        best = {"inst": "MES", "qty": 1, "monthly": MONTHLY_USD_PER_MES,
                "dd": DD_PER_MES, "margin": MES_MARGIN}
    return best


print("="*120)
print(f"1-YEAR REALISTIC PLAN — 30% of March, Full MaxDD, 4x Safety Buffer")
print(f"="*120)
print(f"\n  Assumptions:")
print(f"    March PnL:         +{MARCH_PNL_PTS:.0f} pts in {MARCH_DAYS} days")
print(f"    March MaxDD:       {MARCH_DD_PTS:.0f} pts")
print(f"    Conservative PnL:  30% of March = {MONTHLY_PTS_PER_MES:.0f} pts/mo per MES (${MONTHLY_USD_PER_MES:,.0f})")
print(f"    MaxDD per MES:     ${DD_PER_MES:,.0f} (using FULL March MaxDD, not 30%)")
print(f"    Scale rule:        Capital > 4x MaxDD of new config")
print(f"    Flat months:       2/year (holidays, low vol)")

capital = 5000
print(f"    Starting capital:  ${capital:,}")

print(f"\n{'Mo':>3s} | {'Capital':>10s} | {'Config':>8s} | {'Qty':>4s} | {'Base $/mo':>10s} | {'Var':>5s} | {'PnL':>10s} | {'MaxDD':>8s} | {'DD%':>5s} | {'End Cap':>10s}")
print("-"*105)

total_pnl = 0
peak = capital
worst_dd = 0

for month in range(1, 13):
    cfg = get_config(capital)
    base = cfg["monthly"]
    var = PATTERN[month - 1]
    pnl = round(base * var)
    new_cap = capital + pnl
    total_pnl += pnl

    if new_cap > peak: peak = new_cap
    dd_from_peak = peak - new_cap
    if dd_from_peak > worst_dd: worst_dd = dd_from_peak

    dd_pct = round(cfg["dd"] / capital * 100)
    var_str = f"x{var:.1f}"
    if var <= 0.1: var_str = "FLAT"
    elif var >= 1.2: var_str = "GOOD"

    print(f" {month:3d} | ${capital:>9,} | {cfg['inst']:>4s} All | {cfg['qty']:4d} | ${base:>9,} | {var_str:>5s} | ${pnl:>+9,} | ${cfg['dd']:>7,} | {dd_pct:>4d}% | ${new_cap:>9,}")
    capital = new_cap

print("-"*105)
print(f"\n  RESULTS")
print(f"  {'Starting Capital':25s}: $5,000")
print(f"  {'Final Capital':25s}: ${capital:,}")
print(f"  {'Total Profit':25s}: ${total_pnl:,}")
print(f"  {'Total Return':25s}: {total_pnl/5000*100:,.0f}%")
print(f"  {'Month 12 Monthly PnL':25s}: ${round(get_config(capital)['monthly']):,}")
print(f"  {'Month 12 MaxDD':25s}: ${get_config(capital)['dd']:,}")

# When does $80K/month happen?
print(f"\n{'='*120}")
print(f"WHEN DOES $80K/MONTH HAPPEN?")
print(f"{'='*120}")

capital = 5000
for month in range(1, 25):
    cfg = get_config(capital)
    var = PATTERN[(month-1) % 12]
    pnl = round(cfg["monthly"] * var)
    capital += pnl
    if cfg["monthly"] >= 80000:
        print(f"\n  Month {month}: Monthly capacity = ${cfg['monthly']:,} — TARGET REACHED")
        print(f"  Capital: ${capital:,}, Config: {cfg['inst']} x{cfg['qty']}, MaxDD: ${cfg['dd']:,} ({round(cfg['dd']/capital*100)}% of capital)")
        break
else:
    print(f"\n  Not reached in 24 months. Month 24 capacity: ${get_config(capital)['monthly']:,}")

# Comparison table at different scales
print(f"\n{'='*120}")
print(f"SCALE REFERENCE — Monthly PnL and Risk at Each Level (30% of March)")
print(f"{'='*120}")
print(f"\n  {'Config':>12s} | {'Monthly PnL':>12s} | {'MaxDD':>10s} | {'Capital Needed':>15s} | {'DD% of Cap':>10s} | {'Monthly ROI':>12s}")
print(f"  {'-'*12}-+-{'-'*12}-+-{'-'*10}-+-{'-'*15}-+-{'-'*10}-+-{'-'*12}")

configs = [
    ("1 MES SC", MONTHLY_PTS_PER_MES * 0.60 * MES_PT, 640, 640*4),  # SC = ~60% of all setups
    ("1 MES All", MONTHLY_USD_PER_MES, DD_PER_MES, DD_PER_MES*4),
    ("2 MES All", MONTHLY_USD_PER_MES*2, DD_PER_MES*2, DD_PER_MES*2*4),
    ("4 MES All", MONTHLY_USD_PER_MES*4, DD_PER_MES*4, DD_PER_MES*4*4),
    ("1 ES All", MONTHLY_USD_PER_MES*10, DD_PER_MES*10, DD_PER_MES*10*4),
    ("2 ES All", MONTHLY_USD_PER_MES*20, DD_PER_MES*20, DD_PER_MES*20*4),
    ("3 ES All", MONTHLY_USD_PER_MES*30, DD_PER_MES*30, DD_PER_MES*30*4),
    ("5 ES All", MONTHLY_USD_PER_MES*50, DD_PER_MES*50, DD_PER_MES*50*4),
    ("10 ES All", MONTHLY_USD_PER_MES*100, DD_PER_MES*100, DD_PER_MES*100*4),
]
for name, monthly, dd, cap_needed in configs:
    dd_pct = round(dd / cap_needed * 100)
    roi = round(monthly / cap_needed * 100)
    print(f"  {name:>12s} | ${monthly:>11,} | ${dd:>9,} | ${cap_needed:>14,} | {dd_pct:>9d}% | {roi:>11d}%")
