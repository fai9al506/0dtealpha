"""Analyze March 1-17 options simulation results locally."""
import json
from collections import defaultdict

with open("tmp_options_sim_march.json") as f:
    data = json.load(f)

print("=" * 100)
print("MARCH 2026 OPTIONS SIMULATION — V8 vs V9-SC (Real SPXW chain prices)")
print("NOTE: These are SPXW premiums (~$5-11 entry). For SPY equivalent, divide by ~10.")
print("=" * 100)

v8_cum = 0; v9_cum = 0
v8_total_trades = 0; v9_total_trades = 0

print(f"\n{'Date':<12} {'Trades':>6} | {'V8 Tr':>5} {'V8 PnL':>10} {'V8 Cum':>10} | {'V9 Tr':>5} {'V9 PnL':>10} {'V9 Cum':>10} | {'Delta':>10}")
print("-" * 100)

daily_stats = []
for date in sorted(data.keys()):
    d = data[date]
    summary = d.get("summary", {})
    total = summary.get("total_trades", 0)
    v8 = summary.get("v8", {})
    v9 = summary.get("v9sc", {})
    v8_pnl = v8.get("pnl", 0)
    v9_pnl = v9.get("pnl", 0)
    v8_tr = v8.get("trades", 0)
    v9_tr = v9.get("trades", 0)

    if total == 0:
        continue

    v8_cum += v8_pnl
    v9_cum += v9_pnl
    v8_total_trades += v8_tr
    v9_total_trades += v9_tr
    delta = v9_pnl - v8_pnl

    marker = " <<<" if delta > 500 else (" !!!" if delta < -500 else "")
    print(f"{date:<12} {total:>6} | {v8_tr:>5} {v8_pnl:>+10.0f} {v8_cum:>+10.0f} | {v9_tr:>5} {v9_pnl:>+10.0f} {v9_cum:>+10.0f} | {delta:>+10.0f}{marker}")

    daily_stats.append({"date": date, "v8_pnl": v8_pnl, "v9_pnl": v9_pnl, "delta": delta})

print("-" * 100)
print(f"{'TOTAL':<12} {'':>6} | {v8_total_trades:>5} {v8_cum:>+10.0f} {'':>10} | {v9_total_trades:>5} {v9_cum:>+10.0f} {'':>10} | {v9_cum-v8_cum:>+10.0f}")

# Summary stats
print(f"\n\n{'=' * 80}")
print("SUMMARY")
print("=" * 80)
print(f"V8  total: {v8_total_trades} trades, SPXW PnL: ${v8_cum:+,.0f}  (SPY approx: ${v8_cum/10:+,.0f})")
print(f"V9-SC total: {v9_total_trades} trades, SPXW PnL: ${v9_cum:+,.0f}  (SPY approx: ${v9_cum/10:+,.0f})")
print(f"V9-SC improvement: ${v9_cum - v8_cum:+,.0f} SPXW  (${(v9_cum-v8_cum)/10:+,.0f} SPY)")

# Per-trade averages
if v8_total_trades:
    print(f"\nV8  avg per trade: ${v8_cum/v8_total_trades:+,.0f} SPXW")
if v9_total_trades:
    print(f"V9-SC avg per trade: ${v9_cum/v9_total_trades:+,.0f} SPXW")

# Days V9-SC beats V8
v9_better = sum(1 for s in daily_stats if s["delta"] > 0)
v8_better = sum(1 for s in daily_stats if s["delta"] < 0)
same = sum(1 for s in daily_stats if s["delta"] == 0)
print(f"\nDays V9-SC better: {v9_better}/{len(daily_stats)}")
print(f"Days V8 better: {v8_better}/{len(daily_stats)}")
print(f"Days equal: {same}/{len(daily_stats)}")

# Deep dive into blocked trades
print(f"\n\n{'=' * 80}")
print("BLOCKED TRADE ANALYSIS (trades V9-SC blocks that V8 allows)")
print("=" * 80)

total_blocked = 0
blocked_pnl = 0
blocked_by_setup = defaultdict(lambda: {"count": 0, "pnl": 0, "wins": 0, "losses": 0})

for date in sorted(data.keys()):
    trades = data[date].get("trades", [])
    for t in trades:
        if t.get("v8") and not t.get("v9sc"):
            total_blocked += 1
            pnl = t.get("opt_pnl", 0)
            blocked_pnl += pnl
            s = t.get("setup", "?")
            blocked_by_setup[s]["count"] += 1
            blocked_by_setup[s]["pnl"] += pnl
            if t.get("outcome") == "WIN":
                blocked_by_setup[s]["wins"] += 1
            else:
                blocked_by_setup[s]["losses"] += 1

print(f"Total blocked: {total_blocked} trades, ${blocked_pnl:+,.0f} SPXW PnL")
print(f"\nPer setup:")
for s in sorted(blocked_by_setup.keys(), key=lambda x: blocked_by_setup[x]["pnl"]):
    b = blocked_by_setup[s]
    print(f"  {s:<22} {b['count']:>3}t  {b['wins']}W/{b['losses']}L  ${b['pnl']:>+8,.0f}")

# V9-SC exclusive wins (trades V9-SC allows that V8 blocks)
print(f"\n\n{'=' * 80}")
print("V9-SC EXCLUSIVE TRADES (pass V9-SC but fail V8)")
print("=" * 80)
exclusive_count = 0
exclusive_pnl = 0
for date in sorted(data.keys()):
    trades = data[date].get("trades", [])
    for t in trades:
        if t.get("v9sc") and not t.get("v8"):
            exclusive_count += 1
            exclusive_pnl += t.get("opt_pnl", 0)

print(f"Total: {exclusive_count} trades, ${exclusive_pnl:+,.0f}")
if exclusive_count > 0:
    print("(These should be 0 — V9-SC is strictly tighter than V8)")
