"""Simulate debit spreads and credit spreads for 0DTE options.
Uses setup_log outcomes + analytical spread pricing.

Strategy A: DEBIT SPREAD — buy ~0.45 delta, sell 0.35 delta ($10 wide SPXW)
Strategy B: CREDIT SPREAD — sell ATM put (bullish) or ATM call (bearish), buy $10 OTM protection

Both strategies use V9-SC filter."""

import json, csv
from collections import defaultdict

with open("tmp_options_sim_march.json") as f:
    data = json.load(f)

# Load VIX3M for overvix
vix3m_by_date = {}
with open("tmp_vix_history.csv") as f:
    for row in csv.DictReader(f):
        vix3m_by_date[row["Date"]] = float(row["VIX3M_Close"])

# Collect all trades
all_trades = []
for date, d in data.items():
    for t in d.get("trades", []):
        t["date"] = date
        all_trades.append(t)

def passes_v9sc(t):
    align = t.get("align", 0) or 0
    direction = t.get("dir", "")
    sname = t.get("setup", "")
    is_long = direction in ("long", "bullish")
    if is_long:
        if align < 2: return False
    else:
        if sname == "Skew Charm": return True
        if sname == "AG Short": return True
        if sname == "DD Exhaustion" and align != 0: return True
        return False
    if is_long and sname != "Skew Charm":
        vix = t.get("vix")
        vix3m = vix3m_by_date.get(t.get("date"), 25)
        ov = (vix - vix3m) if vix else None
        if vix and vix > 22:
            if ov is None or ov < 2:
                return False
    return True

# ── STRATEGY A: DEBIT SPREAD ─────────────────────────────────
# Buy 0.45 delta, sell 0.35 delta (strike +$10 for calls, -$10 for puts)
# $10 wide SPXW spread
#
# Pricing model for 0DTE at ~0.45 delta:
#   Long leg: 0.45 delta, ~3-5 pts OTM
#   Short leg: 0.35 delta, ~13-15 pts OTM
#   Debit = long_ASK - short_BID
#
# At resolution:
#   SPX moves by pnl_pts in our direction
#   Long strike was ~4 pts OTM → goes (pnl - 4) ITM
#   Short strike was ~14 pts OTM → goes (pnl - 14) ITM
#   Spread intrinsic = max(0, min(10, pnl - 4)) - max(0, pnl - 14)
#   (simplified: max(0, min(10, move_past_long_strike)))

def debit_spread_pnl(spx_pnl, outcome, vix_val, entry_premium):
    """Model debit spread P&L from SPX point outcome.

    entry_premium: the 0.30 delta ASK from sim (we scale to estimate 0.45 delta)
    """
    spread_width = 10.0  # $10 wide SPXW

    # Estimate spread debit from VIX level
    # Higher VIX = wider spreads but also more expensive legs
    # Typical 0DTE $10-wide spread at 0.45 delta:
    if vix_val and vix_val > 25:
        debit = 4.50  # expensive VIX
    elif vix_val and vix_val > 22:
        debit = 3.80
    elif vix_val and vix_val > 20:
        debit = 3.20
    else:
        debit = 2.80

    # At resolution, estimate intrinsic value of spread
    # The 0.45 delta strike is ~4 pts OTM from spot at entry
    otm_distance = 4.0  # approximate for 0.45 delta 0DTE

    favorable_move = abs(spx_pnl) if outcome == "WIN" else -abs(spx_pnl)

    if outcome == "WIN":
        # How far past the long strike did spot move?
        move_past_strike = favorable_move - otm_distance
        if move_past_strike <= 0:
            spread_value = 0.5  # barely moved past, some residual
        elif move_past_strike >= spread_width:
            spread_value = spread_width  # max profit
        else:
            spread_value = move_past_strike + 0.3  # intrinsic + small time value
    else:
        # LOSS: spot moved against us
        # Both legs go further OTM → spread decays but not to zero (short leg decays too)
        loss_magnitude = abs(spx_pnl)
        if loss_magnitude >= 15:
            spread_value = 0.10  # near total loss
        elif loss_magnitude >= 10:
            spread_value = 0.30
        elif loss_magnitude >= 8:
            spread_value = 0.50
        else:
            spread_value = max(0.10, debit * 0.3)  # partial theta decay

    pnl = (spread_value - debit) * 100  # per contract
    return pnl, debit, spread_value


# ── STRATEGY B: CREDIT SPREAD ────────────────────────────────
# Bullish: sell ATM put, buy put $10 below (bull put credit spread)
# Bearish: sell ATM call, buy call $10 above (bear call credit spread)
# Collect premium, profit from theta + direction

def credit_spread_pnl(spx_pnl, outcome, vix_val, direction):
    """Model credit spread P&L.

    For bullish: sell ATM put, buy put $10 below → collect credit
    For bearish: sell ATM call, buy call $10 above → collect credit
    """
    spread_width = 10.0

    # Credit received (ATM short - OTM long)
    if vix_val and vix_val > 25:
        credit = 5.50
    elif vix_val and vix_val > 22:
        credit = 4.80
    elif vix_val and vix_val > 20:
        credit = 4.20
    else:
        credit = 3.50

    max_loss = spread_width - credit  # $4.50-6.50

    is_long = direction in ("long", "bullish")

    if outcome == "WIN":
        # Spot moved in our direction → short option expires OTM → keep most/all credit
        favorable_move = abs(spx_pnl)
        if favorable_move >= 10:
            # Far OTM at expiry → keep full credit
            spread_exit = 0.20  # tiny residual
        elif favorable_move >= 5:
            spread_exit = 1.00
        else:
            spread_exit = 2.00  # still some risk, partial profit
        pnl = (credit - spread_exit) * 100
    else:
        # Spot moved against us → short option goes ITM
        loss_magnitude = abs(spx_pnl)
        if loss_magnitude >= 15:
            # Deep ITM → near max loss
            spread_exit = spread_width - 0.20
        elif loss_magnitude >= 10:
            spread_exit = spread_width - 0.50
        elif loss_magnitude >= 8:
            spread_exit = loss_magnitude - 1.0
        else:
            spread_exit = loss_magnitude * 0.6
        pnl = (credit - spread_exit) * 100

    return pnl, credit, spread_exit


# ── Run simulation ───────────────────────────────────────────
print("=" * 120)
print("0DTE SPREAD STRATEGIES SIMULATION — March 2-17 (V9-SC filter)")
print("SPXW $10-wide spreads. Divide by ~10 for SPY equivalent.")
print("=" * 120)

v9_trades = [t for t in all_trades if passes_v9sc(t)]
print(f"\nV9-SC filtered trades: {len(v9_trades)}")

# Strategy results
naked_total = 0
debit_total = 0
credit_total = 0
naked_wins = 0; naked_losses = 0
debit_wins = 0; debit_losses = 0
credit_wins = 0; credit_losses = 0

daily_naked = defaultdict(float)
daily_debit = defaultdict(float)
daily_credit = defaultdict(float)

# Per-setup tracking
setup_naked = defaultdict(lambda: {"pnl": 0, "count": 0, "w": 0, "l": 0})
setup_debit = defaultdict(lambda: {"pnl": 0, "count": 0, "w": 0, "l": 0})
setup_credit = defaultdict(lambda: {"pnl": 0, "count": 0, "w": 0, "l": 0})

for t in v9_trades:
    vix_val = t.get("vix", 22)
    outcome = t["outcome"]
    spx_pnl = t["spx_pnl"]
    entry_price = t["entry_price"]
    opt_pnl = t["opt_pnl"]
    setup = t["setup"]
    direction = t["dir"]
    date = t["date"]

    # Naked long (current strategy)
    naked_total += opt_pnl
    daily_naked[date] += opt_pnl
    setup_naked[setup]["pnl"] += opt_pnl
    setup_naked[setup]["count"] += 1
    if opt_pnl >= 0:
        naked_wins += 1
        setup_naked[setup]["w"] += 1
    else:
        naked_losses += 1
        setup_naked[setup]["l"] += 1

    # Debit spread
    d_pnl, d_debit, d_exit = debit_spread_pnl(spx_pnl, outcome, vix_val, entry_price)
    debit_total += d_pnl
    daily_debit[date] += d_pnl
    setup_debit[setup]["pnl"] += d_pnl
    setup_debit[setup]["count"] += 1
    if d_pnl >= 0:
        debit_wins += 1
        setup_debit[setup]["w"] += 1
    else:
        debit_losses += 1
        setup_debit[setup]["l"] += 1

    # Credit spread
    c_pnl, c_credit, c_exit = credit_spread_pnl(spx_pnl, outcome, vix_val, direction)
    credit_total += c_pnl
    daily_credit[date] += c_pnl
    setup_credit[setup]["pnl"] += c_pnl
    setup_credit[setup]["count"] += 1
    if c_pnl >= 0:
        credit_wins += 1
        setup_credit[setup]["w"] += 1
    else:
        credit_losses += 1
        setup_credit[setup]["l"] += 1


# ── Summary ──────────────────────────────────────────────────
print(f"\n{'Strategy':<25} {'Trades':>6} {'W':>5} {'L':>5} {'WR':>6} {'SPXW PnL':>12} {'SPY ~':>10} {'$/trade':>8}")
print("-" * 85)
print(f"{'Naked Long (current)':<25} {len(v9_trades):>6} {naked_wins:>5} {naked_losses:>5} "
      f"{naked_wins/len(v9_trades)*100:>5.0f}% ${naked_total:>+11,.0f} ${naked_total/10:>+9,.0f} ${naked_total/len(v9_trades):>+7,.0f}")
print(f"{'Debit Spread ($10w)':<25} {len(v9_trades):>6} {debit_wins:>5} {debit_losses:>5} "
      f"{debit_wins/len(v9_trades)*100:>5.0f}% ${debit_total:>+11,.0f} ${debit_total/10:>+9,.0f} ${debit_total/len(v9_trades):>+7,.0f}")
print(f"{'Credit Spread ($10w)':<25} {len(v9_trades):>6} {credit_wins:>5} {credit_losses:>5} "
      f"{credit_wins/len(v9_trades)*100:>5.0f}% ${credit_total:>+11,.0f} ${credit_total/10:>+9,.0f} ${credit_total/len(v9_trades):>+7,.0f}")

# ── Daily comparison ─────────────────────────────────────────
print(f"\n\n{'Date':<12} {'Naked':>10} {'Debit':>10} {'Credit':>10} | {'Naked Cum':>10} {'Debit Cum':>10} {'Credit Cum':>10}")
print("-" * 80)
n_cum = 0; d_cum = 0; c_cum = 0
for date in sorted(set(t["date"] for t in v9_trades)):
    n_day = daily_naked.get(date, 0)
    d_day = daily_debit.get(date, 0)
    c_day = daily_credit.get(date, 0)
    n_cum += n_day; d_cum += d_day; c_cum += c_day
    best = "N" if n_day >= d_day and n_day >= c_day else ("D" if d_day >= c_day else "C")
    print(f"{date:<12} ${n_day:>+9,.0f} ${d_day:>+9,.0f} ${c_day:>+9,.0f} | ${n_cum:>+9,.0f} ${d_cum:>+9,.0f} ${c_cum:>+9,.0f}  [{best}]")

# ── Per-setup breakdown ──────────────────────────────────────
print(f"\n\n{'=' * 100}")
print("PER-SETUP COMPARISON (V9-SC filtered)")
print(f"{'=' * 100}")
print(f"{'Setup':<22} | {'--- Naked ---':>20} | {'--- Debit ---':>20} | {'--- Credit ---':>20}")
print(f"{'':22} | {'PnL':>10} {'W/L':>9} | {'PnL':>10} {'W/L':>9} | {'PnL':>10} {'W/L':>9}")
print("-" * 100)

all_setups = sorted(set(list(setup_naked.keys()) + list(setup_debit.keys()) + list(setup_credit.keys())))
for s in all_setups:
    n = setup_naked.get(s, {"pnl": 0, "w": 0, "l": 0})
    d = setup_debit.get(s, {"pnl": 0, "w": 0, "l": 0})
    c = setup_credit.get(s, {"pnl": 0, "w": 0, "l": 0})
    print(f"  {s:<20} | ${n['pnl']:>+9,.0f} {n['w']}W/{n['l']}L | ${d['pnl']:>+9,.0f} {d['w']}W/{d['l']}L | ${c['pnl']:>+9,.0f} {c['w']}W/{c['l']}L")


# ── Win/Loss characteristics ─────────────────────────────────
print(f"\n\n{'=' * 80}")
print("WIN/LOSS CHARACTERISTICS")
print(f"{'=' * 80}")

for label, total_pnl, wins, losses, trades in [
    ("Naked Long", naked_total, naked_wins, naked_losses, v9_trades),
    ("Debit Spread", debit_total, debit_wins, debit_losses, v9_trades),
    ("Credit Spread", credit_total, credit_wins, credit_losses, v9_trades),
]:
    w_pnl = 0; l_pnl = 0
    for i, t in enumerate(trades):
        if label == "Naked Long":
            p = t["opt_pnl"]
        elif label == "Debit Spread":
            p, _, _ = debit_spread_pnl(t["spx_pnl"], t["outcome"], t.get("vix", 22), t["entry_price"])
        else:
            p, _, _ = credit_spread_pnl(t["spx_pnl"], t["outcome"], t.get("vix", 22), t["dir"])
        if p >= 0: w_pnl += p
        else: l_pnl += p

    avg_w = w_pnl / max(1, wins)
    avg_l = l_pnl / max(1, losses)
    ratio = abs(avg_w / avg_l) if avg_l != 0 else 999
    be_wr = abs(avg_l) / (abs(avg_w) + abs(avg_l)) * 100 if (avg_w + abs(avg_l)) > 0 else 0

    print(f"\n{label}:")
    print(f"  Avg WIN:  ${avg_w:>+,.0f}   Avg LOSS: ${avg_l:>+,.0f}   Ratio: {ratio:.2f}x")
    print(f"  Break-even WR needed: {be_wr:.0f}%   Actual WR: {wins/(wins+losses)*100:.0f}%")
    gap = wins/(wins+losses)*100 - be_wr
    print(f"  Edge: {gap:+.0f}% {'PROFITABLE' if gap > 0 else 'LOSING'}")
