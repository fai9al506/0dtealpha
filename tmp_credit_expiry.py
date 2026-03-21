"""ITM Credit Spread — HOLD TO EXPIRY simulation.
Correct methodology: check if SPX closed above/below short strike at 4 PM.
Uses real chain entry prices + expiry intrinsic values."""

import json, glob
from collections import defaultdict

# SPX closing prices (from Yahoo Finance)
SPX_CLOSE = {
    "2026-03-02": 6881.62, "2026-03-03": 6816.63, "2026-03-04": 6869.50,
    "2026-03-05": 6830.71, "2026-03-06": 6740.02, "2026-03-09": 6795.99,
    "2026-03-10": 6781.48, "2026-03-11": 6775.80, "2026-03-12": 6672.62,
    "2026-03-13": 6632.19, "2026-03-16": 6699.38, "2026-03-17": 6716.09,
}

# Load trades
dates = sorted(glob.glob("tmp_chain_2026-03-*.json"))
all_trades = []
for fp in dates:
    with open(fp) as f:
        d = json.load(f)
    for t in d.get("trades", []):
        t["date"] = d.get("date", fp[-15:-5])
        all_trades.append(t)

v9 = [t for t in all_trades if t.get("v9sc")]
print(f"V9-SC trades: {len(v9)}")

SPREAD_WIDTH = 10.0

# For each trade, simulate ITM credit spread held to expiry
# Bullish: sell ITM put (strike = entry_spot + ~5), buy put $10 below
# Bearish: sell ITM call (strike = entry_spot - ~5), buy call $10 above
# Use the debit spread strikes (0.45 delta long = our short put for credit)

print(f"\n{'=' * 120}")
print("ITM CREDIT SPREAD — HOLD TO EXPIRY (real entry prices, expiry intrinsic exit)")
print(f"{'=' * 120}")
print(f"\n{'#':<5} {'Date':<12} {'Setup':<18} {'Dir':<6} {'Spot':>7} {'Close':>7} {'ShortK':>7} {'LongK':>7} {'Credit':>7} {'Expiry':>7} {'PnL':>8} {'Result':<6}")
print("-" * 120)

daily_pnl = defaultdict(float)
daily_count = defaultdict(int)
setup_stats = defaultdict(lambda: {"pnl": 0, "count": 0, "w": 0, "l": 0})
total_pnl = 0
wins = 0; losses = 0
trades_processed = 0

for t in v9:
    date = t["date"]
    spx_close = SPX_CLOSE.get(date)
    if not spx_close:
        continue

    direction = t["dir"]
    is_long = direction in ("long", "bullish")
    setup = t["setup"]

    # Get the spread strikes from debit spread data
    # For credit spread: our short leg = debit's long leg (0.45 delta)
    # Our long leg = debit's short leg
    debit_long = t.get("debit_long_strike")
    debit_short = t.get("debit_short_strike")
    debit_entry = t.get("debit_entry", "")

    if not debit_long or not debit_short or not debit_entry:
        continue

    # Parse the actual entry prices from debit_entry string: "long_ask-short_bid=debit"
    try:
        parts = debit_entry.split("=")
        debit_cost = float(parts[1])
        prices = parts[0].split("-")
        long_ask = float(prices[0])
        short_bid = float(prices[1])
    except:
        continue

    # ITM Credit spread (opposite side of debit):
    # Bullish: sell ITM put at debit_long_strike, buy OTM put at debit_short_strike
    # Wait — need to think about this more carefully.
    #
    # Debit CALL spread for bullish: buy call at K1 (0.45d), sell call at K1+10
    # Equivalent ITM PUT credit spread: sell put at K1+10 (ITM), buy put at K1
    #
    # So: credit_short_strike = debit_short_strike (the higher one for calls)
    #     credit_long_strike = debit_long_strike (the lower one for calls)
    #
    # For bearish (debit PUT spread): buy put at K1 (0.45d), sell put at K1-10
    # Equivalent ITM CALL credit: sell call at K1-10 (ITM), buy call at K1
    #     credit_short_strike = debit_short_strike (lower)
    #     credit_long_strike = debit_long_strike (higher)

    if is_long:
        # Bullish: ITM bull put credit spread
        # Short put at higher strike (ITM), long put at lower strike
        credit_short_strike = max(debit_long, debit_short)  # higher strike
        credit_long_strike = min(debit_long, debit_short)   # lower strike
        # Credit received = short_put_bid - long_put_ask
        # By put-call parity: credit ~ SPREAD_WIDTH - debit_cost
        credit_received = SPREAD_WIDTH - debit_cost

        # At expiry: if SPX > credit_short_strike → both puts OTM → keep full credit
        # If SPX < credit_long_strike → max loss
        # If between → partial loss
        if spx_close >= credit_short_strike:
            # Full WIN — both puts expire OTM
            expiry_value = 0
        elif spx_close <= credit_long_strike:
            # Full LOSS — spread at max width
            expiry_value = SPREAD_WIDTH
        else:
            # Partial — short put ITM, long put OTM
            expiry_value = credit_short_strike - spx_close

    else:
        # Bearish: ITM bear call credit spread
        # Short call at lower strike (ITM), long call at higher strike
        credit_short_strike = min(debit_long, debit_short)  # lower strike
        credit_long_strike = max(debit_long, debit_short)   # higher strike
        credit_received = SPREAD_WIDTH - debit_cost

        # At expiry: if SPX < credit_short_strike → both calls OTM → keep full credit
        if spx_close <= credit_short_strike:
            expiry_value = 0
        elif spx_close >= credit_long_strike:
            expiry_value = SPREAD_WIDTH
        else:
            expiry_value = spx_close - credit_short_strike

    pnl = (credit_received - expiry_value) * 100  # per contract
    is_win = pnl >= 0

    total_pnl += pnl
    daily_pnl[date] += pnl
    daily_count[date] += 1
    setup_stats[setup]["pnl"] += pnl
    setup_stats[setup]["count"] += 1
    if is_win:
        wins += 1
        setup_stats[setup]["w"] += 1
    else:
        losses += 1
        setup_stats[setup]["l"] += 1
    trades_processed += 1

    result = "WIN" if is_win else "LOSS"
    # Get entry spot from naked_strike (approximate)
    spot = t.get("naked_strike", 0)
    print(f"{t['id']:<5} {date:<12} {setup:<18} {direction:<6} {spot:>7.0f} {spx_close:>7.0f} "
          f"{credit_short_strike:>7.0f} {credit_long_strike:>7.0f} ${credit_received:>5.2f} ${expiry_value:>5.2f} ${pnl:>+7.0f} {result}")

# ── Summary ──────────────────────────────────────────────────
print(f"\n{'=' * 80}")
print("SUMMARY — ITM Credit Spread Hold-to-Expiry")
print(f"{'=' * 80}")
wr = wins / trades_processed * 100 if trades_processed else 0
print(f"Trades: {trades_processed}  |  Wins: {wins}  |  Losses: {losses}  |  WR: {wr:.0f}%")
print(f"SPXW PnL: ${total_pnl:>+,.0f}  |  SPY equiv: ${total_pnl/10:>+,.0f}")
print(f"Per trade: ${total_pnl/trades_processed:>+,.0f} SPXW  |  ${total_pnl/10/trades_processed:>+,.0f} SPY")
print(f"Per day: ${total_pnl/12:>+,.0f} SPXW  |  ${total_pnl/10/12:>+,.0f} SPY")

w_pnl = sum(1 for t_idx in range(trades_processed))  # placeholder
# Actually compute avg win/loss
all_pnls = []
for t in v9:
    date = t["date"]
    spx_close = SPX_CLOSE.get(date)
    if not spx_close or not t.get("debit_entry"):
        continue
    try:
        debit_cost = float(t["debit_entry"].split("=")[1])
    except:
        continue
    credit_received = SPREAD_WIDTH - debit_cost
    is_long = t["dir"] in ("long", "bullish")

    dl = t.get("debit_long_strike", 0)
    ds = t.get("debit_short_strike", 0)
    if is_long:
        cs = max(dl, ds); cl = min(dl, ds)
        if spx_close >= cs: ev = 0
        elif spx_close <= cl: ev = SPREAD_WIDTH
        else: ev = cs - spx_close
    else:
        cs = min(dl, ds); cl = max(dl, ds)
        if spx_close <= cs: ev = 0
        elif spx_close >= cl: ev = SPREAD_WIDTH
        else: ev = spx_close - cs
    all_pnls.append((credit_received - ev) * 100)

w_pnls = [p for p in all_pnls if p >= 0]
l_pnls = [p for p in all_pnls if p < 0]
avg_w = sum(w_pnls) / len(w_pnls) if w_pnls else 0
avg_l = sum(l_pnls) / len(l_pnls) if l_pnls else 0
ratio = abs(avg_w / avg_l) if avg_l else 999
be_wr = abs(avg_l) / (abs(avg_w) + abs(avg_l)) * 100 if (abs(avg_w) + abs(avg_l)) > 0 else 0

print(f"\nAvg WIN:  ${avg_w:>+,.0f}  |  Avg LOSS: ${avg_l:>+,.0f}  |  Ratio: {ratio:.2f}x")
print(f"Break-even WR: {be_wr:.0f}%  |  Actual: {wr:.0f}%  |  Edge: {wr-be_wr:+.0f}%")

# ── Daily ────────────────────────────────────────────────────
print(f"\n{'Date':<12} {'Trades':>6} {'PnL':>10} {'Cum':>10} {'SPX Close':>10}")
print("-" * 55)
cum = 0
for date in sorted(daily_pnl.keys()):
    cum += daily_pnl[date]
    print(f"{date:<12} {daily_count[date]:>6} ${daily_pnl[date]:>+9,.0f} ${cum:>+9,.0f} {SPX_CLOSE[date]:>10.0f}")
print(f"{'TOTAL':<12} {trades_processed:>6} ${total_pnl:>+9,.0f}")

# ── Per setup ────────────────────────────────────────────────
print(f"\n{'Setup':<22} {'Trades':>6} {'W/L':>8} {'WR':>5} {'SPXW PnL':>10} {'SPY':>8}")
print("-" * 65)
for s in sorted(setup_stats.keys(), key=lambda x: setup_stats[x]["pnl"], reverse=True):
    st = setup_stats[s]
    wr_s = st["w"] / st["count"] * 100 if st["count"] else 0
    print(f"  {s:<20} {st['count']:>4} {st['w']}W/{st['l']}L {wr_s:>4.0f}% ${st['pnl']:>+9,.0f} ${st['pnl']/10:>+7,.0f}")
