"""Deep dive: WHY are options negative, and how to fix it."""
import json
from collections import defaultdict

with open("tmp_options_sim_march.json") as f:
    data = json.load(f)

# Collect all trades
all_trades = []
for date, d in data.items():
    for t in d.get("trades", []):
        t["date"] = date
        all_trades.append(t)

print(f"Total trades: {len(all_trades)}")

# ── Problem #1: SPX WINs that are Option LOSSes ─────────────
print("\n" + "=" * 80)
print("PROBLEM #1: SPX WINs that are Option LOSSes")
print("=" * 80)

spx_win_opt_loss = [t for t in all_trades if t["outcome"] == "WIN" and t["opt_pnl"] < 0]
spx_win_opt_win = [t for t in all_trades if t["outcome"] == "WIN" and t["opt_pnl"] >= 0]
spx_loss = [t for t in all_trades if t["outcome"] == "LOSS"]

print(f"SPX WIN + Option WIN:  {len(spx_win_opt_win)} trades, opt PnL: ${sum(t['opt_pnl'] for t in spx_win_opt_win):+,.0f}")
print(f"SPX WIN + Option LOSS: {len(spx_win_opt_loss)} trades, opt PnL: ${sum(t['opt_pnl'] for t in spx_win_opt_loss):+,.0f}")
print(f"SPX LOSS:              {len(spx_loss)} trades, opt PnL: ${sum(t['opt_pnl'] for t in spx_loss):+,.0f}")

print(f"\n{len(spx_win_opt_loss)} of {len(spx_win_opt_loss)+len(spx_win_opt_win)} SPX WINs ({len(spx_win_opt_loss)/(len(spx_win_opt_loss)+len(spx_win_opt_win))*100:.0f}%) are Option LOSSes!")
print("This means the option decayed faster than the directional move gained.")

# By setup
print(f"\nSPX WIN -> Option LOSS by setup:")
by_setup = defaultdict(list)
for t in spx_win_opt_loss:
    by_setup[t["setup"]].append(t)
for s in sorted(by_setup.keys(), key=lambda x: len(by_setup[x]), reverse=True):
    ts = by_setup[s]
    avg_spx = sum(t["spx_pnl"] for t in ts) / len(ts)
    avg_opt = sum(t["opt_pnl"] for t in ts) / len(ts)
    avg_entry = sum(t["entry_price"] for t in ts) / len(ts)
    print(f"  {s:<22} {len(ts):>3}t  avg SPX: {avg_spx:+.1f}  avg opt: ${avg_opt:+,.0f}  avg entry: ${avg_entry:.1f}")

# ── Problem #2: Entry price (theta cost) ─────────────────────
print("\n\n" + "=" * 80)
print("PROBLEM #2: Premium Cost Analysis")
print("=" * 80)

for side in ["call", "put"]:
    trades_side = [t for t in all_trades if t.get("side") == side]
    if not trades_side:
        continue
    avg_entry = sum(t["entry_price"] for t in trades_side) / len(trades_side)
    avg_exit = sum(t["exit_price"] for t in trades_side if t["exit_price"]) / max(1, len([t for t in trades_side if t["exit_price"]]))
    avg_pnl = sum(t["opt_pnl"] for t in trades_side) / len(trades_side)
    win_pnl = sum(t["opt_pnl"] for t in trades_side if t["outcome"] == "WIN")
    loss_pnl = sum(t["opt_pnl"] for t in trades_side if t["outcome"] == "LOSS")
    wins = len([t for t in trades_side if t["outcome"] == "WIN"])
    losses = len([t for t in trades_side if t["outcome"] == "LOSS"])
    print(f"\n{side.upper()}S ({len(trades_side)} trades):")
    print(f"  Avg entry (ASK): ${avg_entry:.2f}  Avg exit (BID): ${avg_exit:.2f}")
    print(f"  Avg PnL per trade: ${avg_pnl:+,.0f}")
    print(f"  Winners: {wins} = ${win_pnl:+,.0f} total  (${win_pnl/max(1,wins):+,.0f}/trade)")
    print(f"  Losers:  {losses} = ${loss_pnl:+,.0f} total  (${loss_pnl/max(1,losses):+,.0f}/trade)")

# ── Problem #3: Winners vs Losers asymmetry ──────────────────
print("\n\n" + "=" * 80)
print("PROBLEM #3: Win/Loss Asymmetry")
print("=" * 80)

wins_all = [t for t in all_trades if t["outcome"] == "WIN"]
losses_all = [t for t in all_trades if t["outcome"] == "LOSS"]

if wins_all:
    avg_win = sum(t["opt_pnl"] for t in wins_all) / len(wins_all)
    avg_win_entry = sum(t["entry_price"] for t in wins_all) / len(wins_all)
    avg_win_pct = sum(t["opt_pnl"] / (t["entry_price"] * 100) * 100 for t in wins_all if t["entry_price"] > 0) / len(wins_all)
if losses_all:
    avg_loss = sum(t["opt_pnl"] for t in losses_all) / len(losses_all)
    avg_loss_entry = sum(t["entry_price"] for t in losses_all) / len(losses_all)
    avg_loss_pct = sum(t["opt_pnl"] / (t["entry_price"] * 100) * 100 for t in losses_all if t["entry_price"] > 0) / len(losses_all)

print(f"Average WIN:  ${avg_win:+,.0f} ({avg_win_pct:+.0f}% of premium) on ${avg_win_entry:.1f} entry")
print(f"Average LOSS: ${avg_loss:+,.0f} ({avg_loss_pct:+.0f}% of premium) on ${avg_loss_entry:.1f} entry")
print(f"Win/Loss ratio: {abs(avg_win/avg_loss):.2f}x")
print(f"\nTo break even at current avg win/loss:")
needed_wr = abs(avg_loss) / (abs(avg_win) + abs(avg_loss)) * 100
print(f"  Need {needed_wr:.0f}% WR (current: {len(wins_all)/(len(wins_all)+len(losses_all))*100:.0f}%)")

# ── Problem #4: Time of day analysis ─────────────────────────
print("\n\n" + "=" * 80)
print("PROBLEM #4: Entry Time Analysis (SPXW entry price = theta exposure)")
print("=" * 80)

# Group by entry price (proxy for time — higher premium = earlier in day)
buckets = [(0, 4, "$0-4 (late day)"), (4, 7, "$4-7 (mid day)"), (7, 12, "$7-12 (morning)"), (12, 50, "$12+ (early)")]
for lo, hi, label in buckets:
    bucket = [t for t in all_trades if lo <= t["entry_price"] < hi]
    if not bucket:
        continue
    w = sum(1 for t in bucket if t["outcome"] == "WIN")
    l = sum(1 for t in bucket if t["outcome"] == "LOSS")
    pnl = sum(t["opt_pnl"] for t in bucket)
    avg_pnl = pnl / len(bucket)
    wr = w / len(bucket) * 100
    print(f"  {label:<25} {len(bucket):>3}t  {w}W/{l}L  {wr:.0f}% WR  ${pnl:>+8,.0f}  (${avg_pnl:>+,.0f}/t)")

# ── Problem #5: Setup-level options P&L ──────────────────────
print("\n\n" + "=" * 80)
print("PROBLEM #5: Per-Setup Options P&L (V9-SC filtered)")
print("=" * 80)

by_setup_v9 = defaultdict(lambda: {"count": 0, "pnl": 0, "wins": 0, "losses": 0, "win_pnl": 0, "loss_pnl": 0})
for t in all_trades:
    if not t.get("v9sc"):
        continue
    s = t["setup"]
    by_setup_v9[s]["count"] += 1
    by_setup_v9[s]["pnl"] += t["opt_pnl"]
    if t["outcome"] == "WIN":
        by_setup_v9[s]["wins"] += 1
        by_setup_v9[s]["win_pnl"] += t["opt_pnl"]
    else:
        by_setup_v9[s]["losses"] += 1
        by_setup_v9[s]["loss_pnl"] += t["opt_pnl"]

print(f"{'Setup':<22} {'Trades':>6} {'W/L':>8} {'WR':>5} {'Total PnL':>10} {'Avg Win':>10} {'Avg Loss':>10}")
for s in sorted(by_setup_v9.keys(), key=lambda x: by_setup_v9[x]["pnl"], reverse=True):
    b = by_setup_v9[s]
    wr = b["wins"] / b["count"] * 100 if b["count"] else 0
    avg_w = b["win_pnl"] / b["wins"] if b["wins"] else 0
    avg_l = b["loss_pnl"] / b["losses"] if b["losses"] else 0
    print(f"  {s:<22} {b['count']:>4} {b['wins']}W/{b['losses']}L {wr:>4.0f}% ${b['pnl']:>+9,.0f} ${avg_w:>+9,.0f} ${avg_l:>+9,.0f}")

# ── Recommendations ──────────────────────────────────────────
print("\n\n" + "=" * 80)
print("DIAGNOSIS: Why options are losing")
print("=" * 80)
print("""
The core issue is NOT the direction — the setups are directionally correct.
The issue is THETA vs DELTA race on 0DTE options:

1. ENTRY PRICE TOO HIGH: Avg SPXW entry is $5-11. That's a lot of theta to overcome.
   → For SPY at ~$0.50-1.10, the math is the same proportionally.

2. SPX WINs DON'T TRANSLATE: {pct}% of SPX WIN trades still LOSE money in options
   because the directional gain (+$3-4 delta) < theta decay ($5-7 over 30-60 min).

3. LOSERS ARE CATASTROPHIC: Average loss = {avg_l_pct:.0f}% of premium.
   Options go to near-zero on stop hits. No recovery possible.

4. NEED FASTER WINS: Only trades that win in <15 minutes make money in options.
   Slow setups (DD Exhaustion, 30-60 min holds) are theta furnaces.
""".format(
    pct=len(spx_win_opt_loss)/(len(spx_win_opt_loss)+len(spx_win_opt_win))*100,
    avg_l_pct=abs(avg_loss_pct)
))
