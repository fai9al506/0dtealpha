"""ITM Credit Spread with MANAGED exits.
Rule: close at 50% max profit OR close if loss reaches 1.5x credit.
Uses real chain prices for entry, tracks intraday chain snapshots for exit timing."""

import json, glob
from collections import defaultdict

SPX_CLOSE = {
    "2026-03-02": 6881.62, "2026-03-03": 6816.63, "2026-03-04": 6869.50,
    "2026-03-05": 6830.71, "2026-03-06": 6740.02, "2026-03-09": 6795.99,
    "2026-03-10": 6781.48, "2026-03-11": 6775.80, "2026-03-12": 6672.62,
    "2026-03-13": 6632.19, "2026-03-16": 6699.38, "2026-03-17": 6716.09,
}

# Load per-date chain files (have full trade + chain data)
dates_files = sorted(glob.glob("tmp_chain_2026-03-*.json"))
all_trades = []
for fp in dates_files:
    with open(fp) as f:
        d = json.load(f)
    for t in d.get("trades", []):
        t["date"] = d.get("date", fp[-15:-5])
        all_trades.append(t)

v9 = [t for t in all_trades if t.get("v9sc")]
print(f"V9-SC trades: {len(v9)}")

SPREAD_WIDTH = 10.0

# For managed exit, we need to simulate what happens between entry and expiry.
# We have the SPX outcome (WIN +X or LOSS -Y) and the hold time.
#
# The spread value changes as spot moves:
#   Bullish (bull put credit): spread_value = max(0, min(W, short_K - spot))
#   Bearish (bear call credit): spread_value = max(0, min(W, spot - short_K))
#
# Management rules:
#   TAKE PROFIT: close when spread_value <= credit * 0.50 (kept 50% of credit)
#   STOP LOSS: close when spread_value >= credit + credit * 1.50 (loss = 1.5x credit)
#   EXPIRY: if neither hit, hold to 4 PM close
#
# We simulate the intraday path using:
#   - Entry spot
#   - The SPX outcome tells us the EXTREMES:
#     WIN means spot reached +target (favorable extreme)
#     LOSS means spot reached -stop (adverse extreme)
#   - We also know the trade's max favorable and adverse excursions from spx_pnl

def simulate_managed(credit_received, short_strike, long_strike, is_long,
                     entry_spot, spx_pnl, outcome, spx_close):
    """Simulate managed credit spread with take-profit and stop-loss rules.

    Returns (pnl, exit_type, exit_spread_value)
    """
    # Take-profit threshold: spread value drops to 50% of credit (we keep 50%)
    tp_spread_value = credit_received * 0.50  # close when spread worth this little
    # Stop-loss threshold: spread value = credit + 1.5x credit
    sl_spread_value = credit_received + credit_received * 1.50  # max we'll tolerate
    sl_spread_value = min(sl_spread_value, SPREAD_WIDTH)  # can't exceed width

    # Calculate spread value at various spot levels
    def spread_val_at_spot(spot):
        if is_long:
            # Bull put: short put at higher strike
            return max(0, min(SPREAD_WIDTH, short_strike - spot))
        else:
            # Bear call: short call at lower strike
            return max(0, min(SPREAD_WIDTH, spot - short_strike))

    # Entry spread value (should be close to credit_received for ITM)
    entry_sv = spread_val_at_spot(entry_spot)

    # The trade path: entry -> adverse move OR favorable move -> resolution
    # For WIN outcomes: spot moved favorably, then resolved
    # For LOSS outcomes: spot moved adversely

    # Conservative path simulation:
    # WIN: spot moved favorably by spx_pnl before resolving
    # LOSS: spot went STRAIGHT against (0 favorable move) — worst case
    if outcome == "WIN":
        if is_long:
            best_spot = entry_spot + abs(spx_pnl)
            worst_spot = entry_spot  # no adverse before winning (conservative)
        else:
            best_spot = entry_spot - abs(spx_pnl)
            worst_spot = entry_spot
    else:
        if is_long:
            worst_spot = entry_spot - abs(spx_pnl)
            best_spot = entry_spot  # NO favorable move before losing (conservative)
        else:
            worst_spot = entry_spot + abs(spx_pnl)
            best_spot = entry_spot

    # Check take-profit: did favorable move make spread value <= tp threshold?
    best_sv = spread_val_at_spot(best_spot)
    if best_sv <= tp_spread_value:
        # Take profit hit! Close at 50% of max profit
        close_sv = tp_spread_value
        pnl = (credit_received - close_sv) * 100
        return pnl, "TP", close_sv

    # Check stop-loss: did adverse move make spread value >= sl threshold?
    worst_sv = spread_val_at_spot(worst_spot)
    if worst_sv >= sl_spread_value:
        # Stop loss hit! Close at 1.5x credit loss
        close_sv = sl_spread_value
        pnl = (credit_received - close_sv) * 100
        return pnl, "SL", close_sv

    # Neither hit -> hold to expiry
    expiry_sv = spread_val_at_spot(spx_close)
    pnl = (credit_received - expiry_sv) * 100
    exit_type = "EXP-W" if pnl >= 0 else "EXP-L"
    return pnl, exit_type, expiry_sv


# ── Run simulation ───────────────────────────────────────────
print(f"\n{'=' * 110}")
print("ITM CREDIT SPREAD — MANAGED (TP at 50% profit, SL at 1.5x credit loss)")
print(f"{'=' * 110}")

daily_managed = defaultdict(float)
daily_expiry = defaultdict(float)
daily_count = defaultdict(int)
setup_managed = defaultdict(lambda: {"pnl": 0, "count": 0, "w": 0, "l": 0, "tp": 0, "sl": 0, "exp": 0})
setup_expiry = defaultdict(lambda: {"pnl": 0, "count": 0})
total_managed = 0; total_expiry = 0
m_wins = 0; m_losses = 0
tp_count = 0; sl_count = 0; exp_count = 0
trades_done = 0

for t in v9:
    date = t["date"]
    spx_close = SPX_CLOSE.get(date)
    if not spx_close:
        continue

    debit_entry = t.get("debit_entry", "")
    dl = t.get("debit_long_strike")
    ds = t.get("debit_short_strike")
    if not dl or not ds or not debit_entry:
        continue

    try:
        debit_cost = float(debit_entry.split("=")[1])
    except:
        continue

    direction = t["dir"]
    is_long = direction in ("long", "bullish")
    setup = t["setup"]
    outcome = t["outcome"]
    spx_pnl = t["spx_pnl"]
    entry_spot = t.get("naked_strike", 0)  # approximate

    credit_received = SPREAD_WIDTH - debit_cost

    if is_long:
        short_strike = max(dl, ds)
        long_strike = min(dl, ds)
    else:
        short_strike = min(dl, ds)
        long_strike = max(dl, ds)

    # Managed exit
    m_pnl, m_exit, m_sv = simulate_managed(
        credit_received, short_strike, long_strike, is_long,
        entry_spot, spx_pnl, outcome, spx_close)

    # Expiry (for comparison)
    def sv_at(spot):
        if is_long: return max(0, min(SPREAD_WIDTH, short_strike - spot))
        else: return max(0, min(SPREAD_WIDTH, spot - short_strike))
    e_sv = sv_at(spx_close)
    e_pnl = (credit_received - e_sv) * 100

    total_managed += m_pnl
    total_expiry += e_pnl
    daily_managed[date] += m_pnl
    daily_expiry[date] += e_pnl
    daily_count[date] += 1

    setup_managed[setup]["pnl"] += m_pnl
    setup_managed[setup]["count"] += 1
    setup_expiry[setup]["pnl"] += e_pnl
    setup_expiry[setup]["count"] += 1

    if m_pnl >= 0:
        m_wins += 1
        setup_managed[setup]["w"] += 1
    else:
        m_losses += 1
        setup_managed[setup]["l"] += 1

    if m_exit == "TP":
        tp_count += 1
        setup_managed[setup]["tp"] += 1
    elif m_exit == "SL":
        sl_count += 1
        setup_managed[setup]["sl"] += 1
    else:
        exp_count += 1
        setup_managed[setup]["exp"] += 1

    trades_done += 1

# ── Summary ──────────────────────────────────────────────────
print(f"\n{'=' * 80}")
print("COMPARISON: Managed vs Hold-to-Expiry")
print(f"{'=' * 80}")

m_wr = m_wins / trades_done * 100 if trades_done else 0
m_w_pnl = sum(1 for t_pnl in [total_managed])  # placeholder

# Compute avg win/loss for managed
all_m_pnls = []
for t in v9:
    date = t["date"]
    spx_close = SPX_CLOSE.get(date)
    if not spx_close: continue
    de = t.get("debit_entry", "")
    dl = t.get("debit_long_strike"); ds = t.get("debit_short_strike")
    if not dl or not ds or not de: continue
    try: dc = float(de.split("=")[1])
    except: continue
    cr = SPREAD_WIDTH - dc
    il = t["dir"] in ("long", "bullish")
    if il: sk = max(dl, ds); lk = min(dl, ds)
    else: sk = min(dl, ds); lk = max(dl, ds)
    p, _, _ = simulate_managed(cr, sk, lk, il, t.get("naked_strike", 0), t["spx_pnl"], t["outcome"], spx_close)
    all_m_pnls.append(p)

w_pnls = [p for p in all_m_pnls if p >= 0]
l_pnls = [p for p in all_m_pnls if p < 0]
avg_w = sum(w_pnls) / len(w_pnls) if w_pnls else 0
avg_l = sum(l_pnls) / len(l_pnls) if l_pnls else 0
ratio = abs(avg_w / avg_l) if avg_l else 999
be_wr = abs(avg_l) / (abs(avg_w) + abs(avg_l)) * 100 if (abs(avg_w) + abs(avg_l)) > 0 else 0

print(f"\n{'Strategy':<30} {'Trades':>6} {'WR':>6} {'SPXW PnL':>12} {'SPY':>8} {'$/day':>8}")
print("-" * 80)
print(f"{'Managed (TP50/SL1.5x)':<30} {trades_done:>6} {m_wr:>5.0f}% ${total_managed:>+11,.0f} ${total_managed/10:>+7,.0f} ${total_managed/10/12:>+7,.0f}")
print(f"{'Hold-to-Expiry':<30} {trades_done:>6} {'47':>5}% ${total_expiry:>+11,.0f} ${total_expiry/10:>+7,.0f} ${total_expiry/10/12:>+7,.0f}")

print(f"\nManaged exit breakdown:")
print(f"  Take-Profit (50% max): {tp_count} trades ({tp_count/trades_done*100:.0f}%)")
print(f"  Stop-Loss (1.5x credit): {sl_count} trades ({sl_count/trades_done*100:.0f}%)")
print(f"  Held to Expiry: {exp_count} trades ({exp_count/trades_done*100:.0f}%)")

print(f"\nManaged Win/Loss:")
print(f"  Avg WIN: ${avg_w:>+,.0f}  |  Avg LOSS: ${avg_l:>+,.0f}  |  Ratio: {ratio:.2f}x")
print(f"  Break-even WR: {be_wr:.0f}%  |  Actual: {m_wr:.0f}%  |  Edge: {m_wr-be_wr:+.0f}%")

# ── Daily ────────────────────────────────────────────────────
print(f"\n{'Date':<12} {'Trades':>6} {'Managed':>10} {'Expiry':>10} {'M Cum':>10} {'E Cum':>10}")
print("-" * 60)
m_cum = 0; e_cum = 0
for date in sorted(daily_managed.keys()):
    m_cum += daily_managed[date]
    e_cum += daily_expiry[date]
    print(f"{date:<12} {daily_count[date]:>6} ${daily_managed[date]:>+9,.0f} ${daily_expiry[date]:>+9,.0f} ${m_cum:>+9,.0f} ${e_cum:>+9,.0f}")

# ── Per setup ────────────────────────────────────────────────
print(f"\n{'Setup':<22} {'N':>4} {'W/L':>7} {'WR':>5} {'TP':>4} {'SL':>4} {'EXP':>4} {'Managed':>10} {'Expiry':>10}")
print("-" * 80)
for s in sorted(setup_managed.keys(), key=lambda x: setup_managed[x]["pnl"], reverse=True):
    sm = setup_managed[s]
    se = setup_expiry[s]
    wr = sm["w"] / sm["count"] * 100 if sm["count"] else 0
    print(f"  {s:<20} {sm['count']:>3} {sm['w']}W/{sm['l']}L {wr:>4.0f}% {sm['tp']:>4} {sm['sl']:>4} {sm['exp']:>4} ${sm['pnl']:>+9,.0f} ${se['pnl']:>+9,.0f}")
