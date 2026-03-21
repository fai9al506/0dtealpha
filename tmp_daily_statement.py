"""Daily P&L Statement: V9-SC, SPX points + SPY options (0.50 delta theoretical)."""
import json, csv
from collections import defaultdict

# Load trades
with open(r"C:\Users\Faisa\AppData\Local\Temp\all_trades.json") as f:
    trades = json.load(f)

# Load VIX3M
vix3m_by_date = {}
with open(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\tmp_vix_history.csv") as f:
    for row in csv.DictReader(f):
        vix3m_by_date[row["Date"]] = float(row["VIX3M_Close"])

# Enrich + filter V9-SC
v9_trades = []
for t in trades:
    date = t.get("date", "")
    vix = t.get("vix")
    result = t.get("result")
    if result not in ("WIN", "LOSS"): continue
    if date < "2026-03-01": continue

    # Overvix
    vix3m = vix3m_by_date.get(date)
    overvix = (vix - vix3m) if vix and vix3m else None

    # V9-SC filter
    align = t.get("alignment", 0) or 0
    direction = t.get("direction", "")
    sname = t.get("setup_name", "")
    is_long = direction in ("long", "bullish")

    passed = True
    if is_long:
        if align < 2: passed = False
        elif sname == "Skew Charm": pass  # exempt
        elif vix and vix > 22:
            if overvix is None or overvix < 2: passed = False
    else:
        if sname == "Skew Charm": pass
        elif sname == "AG Short": pass
        elif sname == "DD Exhaustion" and align != 0: pass
        else: passed = False

    if not passed: continue

    t["overvix"] = overvix
    v9_trades.append(t)

# Theoretical 0.50 delta option P&L model
SLIPPAGE = 0.03  # SPY bid/ask slippage per trade
COMMISSION = 1.20  # SPY round trip commission

def option_pnl_spy(spx_pnl, is_win, entry_premium_spx=None, vix_val=22):
    """Estimate SPY option P&L at 0.50 delta.
    SPY premium ~ SPXW/10. SPX move / 10 = SPY move."""
    delta = 0.50
    hold_min = 20 if is_win else 15
    hold_hours = hold_min / 60

    # SPY premium estimate from VIX
    if vix_val and vix_val > 25: spy_premium = 2.50
    elif vix_val and vix_val > 22: spy_premium = 2.00
    elif vix_val and vix_val > 20: spy_premium = 1.60
    else: spy_premium = 1.30

    # Theta: ~12-15% of premium per hour for 0DTE with ~3.5hrs left
    theta_rate = 0.13
    theta_cost = spy_premium * theta_rate * hold_hours

    # Delta P&L: SPY moves 1/10th of SPX
    spy_move = abs(spx_pnl) / 10
    if is_win:
        gamma_boost = 1.12 if spy_move > 0.8 else 1.05
        delta_pnl = delta * spy_move * gamma_boost
    else:
        delta_pnl = -delta * spy_move * 0.93

    option_change = delta_pnl - theta_cost
    pnl = (option_change - SLIPPAGE) * 100 - COMMISSION  # per 1 SPY contract
    return round(pnl, 2), spy_premium

# Build daily statement
daily = defaultdict(lambda: {
    "spx_trades": 0, "spx_wins": 0, "spx_losses": 0, "spx_pnl": 0,
    "opt_trades": 0, "opt_wins": 0, "opt_losses": 0, "opt_pnl": 0,
    "opt_premium_total": 0, "vix": 0
})

for t in v9_trades:
    d = t["date"]
    spx_pnl = t.get("pnl", 0)
    is_win = t["result"] == "WIN"
    vix_val = t.get("vix", 22)

    daily[d]["spx_trades"] += 1
    daily[d]["spx_pnl"] += spx_pnl
    daily[d]["vix"] = vix_val
    if is_win: daily[d]["spx_wins"] += 1
    else: daily[d]["spx_losses"] += 1

    opt_pnl, premium = option_pnl_spy(spx_pnl, is_win, vix_val=vix_val)
    daily[d]["opt_trades"] += 1
    daily[d]["opt_pnl"] += opt_pnl
    daily[d]["opt_premium_total"] += premium * 100  # capital deployed
    if opt_pnl >= 0: daily[d]["opt_wins"] += 1
    else: daily[d]["opt_losses"] += 1

# Print statement
print("=" * 115)
print("DAILY P&L STATEMENT - V9-SC Filter, 0.50 Delta ATM SPY Options (1 contract/trade)")
print("March 2026")
print("=" * 115)
print(f"\n{'Date':<12} {'VIX':>5} | {'SPX':>5} {'W/L':>5} {'SPX PnL':>9} {'SPX Cum':>9} | {'OPT':>4} {'W/L':>5} {'Opt PnL':>9} {'Opt Cum':>9} | {'Capture':>8}")
print("-" * 115)

spx_cum = 0; opt_cum = 0
total_spx_trades = 0; total_opt_trades = 0
total_spx_wins = 0; total_opt_wins = 0

for d in sorted(daily.keys()):
    dd = daily[d]
    spx_cum += dd["spx_pnl"]
    opt_cum += dd["opt_pnl"]
    total_spx_trades += dd["spx_trades"]
    total_opt_trades += dd["opt_trades"]
    total_spx_wins += dd["spx_wins"]
    total_opt_wins += dd["opt_wins"]

    # Capture ratio: how much of SPX P&L was captured in options ($)
    # SPX PnL in dollars at 1 SPY: pnl_pts / 10 * 100 (SPY multiplier)
    spx_dollar = dd["spx_pnl"] / 10 * 100  # what SPX pts are worth in SPY $ terms
    capture = (dd["opt_pnl"] / spx_dollar * 100) if spx_dollar != 0 else 0

    spx_wr = f"{dd['spx_wins']}W/{dd['spx_losses']}L"
    opt_wr = f"{dd['opt_wins']}W/{dd['opt_losses']}L"

    print(f"{d:<12} {dd['vix']:>5.1f} | {dd['spx_trades']:>5} {spx_wr:>5} {dd['spx_pnl']:>+9.1f} {spx_cum:>+9.1f} | "
          f"{dd['opt_trades']:>4} {opt_wr:>5} ${dd['opt_pnl']:>+8.0f} ${opt_cum:>+8.0f} | {capture:>+7.0f}%")

print("-" * 115)
spx_wr_total = total_spx_wins / total_spx_trades * 100 if total_spx_trades else 0
opt_wr_total = total_opt_wins / total_opt_trades * 100 if total_opt_trades else 0
days = len(daily)

print(f"\n{'TOTALS':<12} {'':>5} | {total_spx_trades:>5} {spx_wr_total:>4.0f}% {spx_cum:>+9.1f} {'':>9} | "
      f"{total_opt_trades:>4} {opt_wr_total:>4.0f}% ${opt_cum:>+8.0f} {'':>9} |")

print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")
print(f"  Trading days: {days}")
print(f"  SPX Points:   {spx_cum:>+.1f} pts ({spx_cum/days:>+.1f}/day)")
print(f"  SPY Options:  ${opt_cum:>+,.0f} (${opt_cum/days:>+,.0f}/day)")
print(f"  Monthly (20d): SPX {spx_cum/days*20:>+.0f} pts | SPY ${opt_cum/days*20:>+,.0f}")
print(f"  SPX WR: {spx_wr_total:.0f}% | Options WR: {opt_wr_total:.0f}%")
print(f"  Capital per trade: ~$150-250 (1 SPY ATM 0DTE)")
print(f"  Max concurrent: ~3-5 trades = $450-1,250 capital")
print(f"  Monthly ROI on $4K account: {opt_cum/days*20/4000*100:>+.0f}%")
