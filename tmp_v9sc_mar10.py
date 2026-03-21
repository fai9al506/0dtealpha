"""Simulate March 10 options trades under V8 vs V9-SC.
Uses setup_log outcomes + chain_snapshots to estimate option prices."""
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

# March 10 trades only
mar10 = [t for t in trades if t.get("date") == "2026-03-10" and t.get("result") in ("WIN", "LOSS")]

# Enrich with overvix
for t in mar10:
    vix = t.get("vix")
    vix3m = vix3m_by_date.get("2026-03-10", 25.51)
    if vix:
        t["overvix_calc"] = vix - vix3m
    else:
        t["overvix_calc"] = None
    t["vix_used"] = vix

def passes_v9sc(t):
    align = t.get("alignment", 0) or 0
    direction = t.get("direction", "")
    sname = t.get("setup_name", "")
    is_long = direction in ("long", "bullish")
    if is_long:
        if align < 2: return False
    else:
        if sname == "Skew Charm": return True
        if sname == "AG Short": return True
        if sname == "DD Exhaustion" and align != 0: return True
        return False
    # V9-SC: SC exempt, VIX gate at 22
    if is_long and sname != "Skew Charm":
        vix = t.get("vix_used")
        ov = t.get("overvix_calc")
        if vix is not None and vix > 22:
            if ov is None or ov < 2:
                return False
    return True

def passes_v8(t):
    align = t.get("alignment", 0) or 0
    direction = t.get("direction", "")
    sname = t.get("setup_name", "")
    is_long = direction in ("long", "bullish")
    if is_long:
        if align < 2: return False
    else:
        if sname == "Skew Charm": return True
        if sname == "AG Short": return True
        if sname == "DD Exhaustion" and align != 0: return True
        return False
    if is_long:
        vix = t.get("vix_used")
        ov = t.get("overvix_calc")
        if vix is not None and vix > 26:
            if ov is None or ov < 2:
                return False
    return True

# Option P&L estimation from SPX point outcomes
# At ~0.30 delta, rough option behavior:
#   WIN +10 pts SPX → option gains ~$3.00 (delta 0.30 * 10 + gamma acceleration)
#   WIN with trail (>10 pts) → option gains more
#   LOSS -8 pts → option loses ~$2.40 (delta * loss, but accelerates near zero)
#   LOSS -12 pts → option loses ~$3.60
#   LOSS -15 pts → option likely near total loss (~$4-5)
#   LOSS -20 pts → total loss of premium

# More precise: use delta approximation
# Entry premium at ~0.30 delta ≈ VIX-dependent
# VIX 24-25 → SPY ~0.30 delta premium ≈ $0.50-0.80

def estimate_option_pnl(t):
    """Estimate option P&L from SPX point outcome."""
    pnl_pts = t.get("pnl", 0)
    direction = t.get("direction", "")
    vix = t.get("vix_used", 22)

    # Estimate entry premium based on VIX level
    if vix and vix > 25:
        premium = 0.70  # higher VIX = more expensive
    elif vix and vix > 22:
        premium = 0.55
    else:
        premium = 0.45

    delta = 0.30

    if t["result"] == "WIN":
        # Option gain ≈ delta * SPX_move + gamma bonus
        spx_move = abs(pnl_pts)
        # Gamma acceleration: as option goes ITM, delta increases
        if spx_move <= 10:
            opt_gain = delta * spx_move * 0.10  # $0.30 per SPX pt at 0.30 delta, per $1 SPY
            # For SPY (1/10 of SPX): $0.03 per SPX pt... no
            # Actually SPY ≈ SPX/10, so 10 SPX pts ≈ 1 SPY pt
            # At 0.30 delta: option gains $0.30 per $1 SPY move = $0.30 per 10 SPX pts
            # Plus gamma: maybe $0.05-0.10 more
            opt_pnl = 0.30 + 0.05  # ~$0.35 gain on a +10 SPX WIN
        else:
            # Bigger wins: gamma kicks in
            opt_pnl = delta * (spx_move / 10) + 0.10 * ((spx_move - 10) / 10)
        return opt_pnl * 100  # per contract in dollars
    else:
        # LOSS: option decays
        spx_loss = abs(pnl_pts)
        if spx_loss >= 15:
            # Near total loss
            opt_pnl = -premium * 0.90
        elif spx_loss >= 10:
            opt_pnl = -premium * 0.70
        elif spx_loss >= 8:
            opt_pnl = -premium * 0.55
        else:
            opt_pnl = -delta * (spx_loss / 10)
        return opt_pnl * 100

print(f"March 10, 2026 | VIX=24.93 | VIX3M=25.51 | Overvix=-0.58")
print(f"Total WIN/LOSS trades: {len(mar10)}")
print()

# Show all trades
print(f"{'#':<4} {'Setup':<22} {'Dir':<8} {'Align':>5} {'VIX':>5} {'Result':<6} {'PnL':>6} {'V8':>6} {'V9-SC':>6}")
print("-" * 80)

v8_spx = 0; v9_spx = 0
v8_trades = 0; v9_trades = 0
v8_wins = 0; v9_wins = 0
v8_losses = 0; v9_losses = 0

blocked_trades = []

for i, t in enumerate(mar10, 1):
    sname = t["setup_name"]
    direction = t["direction"]
    align = t.get("alignment", 0) or 0
    vix = t.get("vix_used", 0)
    result = t["result"]
    pnl = t.get("pnl", 0)

    v8_pass = passes_v8(t)
    v9_pass = passes_v9sc(t)

    v8_str = f"{pnl:+.1f}" if v8_pass else "SKIP"
    v9_str = f"{pnl:+.1f}" if v9_pass else "BLOCK"

    if v8_pass:
        v8_spx += pnl
        v8_trades += 1
        if result == "WIN": v8_wins += 1
        else: v8_losses += 1

    if v9_pass:
        v9_spx += pnl
        v9_trades += 1
        if result == "WIN": v9_wins += 1
        else: v9_losses += 1

    if v8_pass and not v9_pass:
        blocked_trades.append(t)

    marker = " <-- BLOCKED by V9-SC" if (v8_pass and not v9_pass) else ""
    print(f"{i:<4} {sname:<22} {direction:<8} {align:>+5} {vix:>5.1f} {result:<6} {pnl:>+6.1f} {v8_str:>6} {v9_str:>6}{marker}")

print("-" * 80)
print(f"{'SPX PTS TOTAL':<58} {v8_spx:>+6.1f} {v9_spx:>+6.1f}")
print(f"{'Trades':<58} {v8_trades:>6} {v9_trades:>6}")
print(f"{'Wins/Losses':<58} {v8_wins}W/{v8_losses}L   {v9_wins}W/{v9_losses}L")
if v8_trades: print(f"{'Win Rate':<58} {v8_wins/v8_trades*100:>5.1f}% {v9_wins/v9_trades*100 if v9_trades else 0:>5.1f}%")

print(f"\nDelta (V9-SC vs V8): {v9_spx - v8_spx:+.1f} SPX pts")

if blocked_trades:
    print(f"\n--- V9-SC Blocked {len(blocked_trades)} trades that V8 allowed ---")
    blk_w = sum(1 for t in blocked_trades if t["result"] == "WIN")
    blk_l = sum(1 for t in blocked_trades if t["result"] == "LOSS")
    blk_pnl = sum(t.get("pnl", 0) for t in blocked_trades)
    print(f"  {blk_w}W/{blk_l}L, {blk_pnl:+.1f} SPX pts")
    by_setup = defaultdict(list)
    for t in blocked_trades:
        by_setup[t["setup_name"]].append(t)
    for sn in sorted(by_setup.keys()):
        ts = by_setup[sn]
        w = sum(1 for t in ts if t["result"] == "WIN")
        l = sum(1 for t in ts if t["result"] == "LOSS")
        p = sum(t.get("pnl", 0) for t in ts)
        print(f"    {sn:<22} {len(ts)}t {w}W/{l}L {p:+.1f} pts")

# Option price estimation
print(f"\n\n{'=' * 80}")
print("ESTIMATED OPTIONS P&L (at ~$0.55 avg premium, 0.30 delta)")
print("=" * 80)
print("SPY options: +10 SPX WIN ≈ +$35/contract, -8 LOSS ≈ -$30, -12 LOSS ≈ -$39, -15+ LOSS ≈ -$45-50")
print()

# Rough option conversion: use actual proportions
# WIN +10 → premium roughly +60-80% → ~+$0.35 on $0.55 entry = +$35
# WIN +trail → even more
# LOSS -8 → premium -55% → ~-$0.30 on $0.55 entry = -$30
# LOSS -12 → premium -70% → -$39
# LOSS -15+ → premium ~-90% → -$45-50

def spx_to_option_dollars(pnl_pts, result, premium=0.55):
    """Rough conversion of SPX point PnL to option dollar PnL per contract."""
    if result == "WIN":
        # Proportional: +10 pts ≈ +60% premium, +15 ≈ +100%, +20 ≈ +150%
        pct_gain = abs(pnl_pts) / 10 * 0.60
        return premium * pct_gain * 100
    else:
        # Proportional: -8 ≈ -55%, -12 ≈ -70%, -15 ≈ -85%, -20 ≈ -95%
        pct_loss = min(0.95, abs(pnl_pts) / 20 * 0.95)
        return -premium * pct_loss * 100

v8_opt = 0; v9_opt = 0
for t in mar10:
    opt_pnl = spx_to_option_dollars(t.get("pnl", 0), t["result"])
    if passes_v8(t):
        v8_opt += opt_pnl
    if passes_v9sc(t):
        v9_opt += opt_pnl

print(f"  V8 estimated option P&L:    ${v8_opt:+.0f}")
print(f"  V9-SC estimated option P&L: ${v9_opt:+.0f}")
print(f"  Delta: ${v9_opt - v8_opt:+.0f}")
print(f"\n  (These are estimates. Actual option prices depend on strike, IV, time to expiry)")
