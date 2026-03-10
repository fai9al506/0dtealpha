"""
Simulate single-position mode vs portal (all signals).
Quantifies the gap between portal PnL and real execution.
"""
import json
from collections import defaultdict
from datetime import datetime, timedelta

with open("C:/Users/Faisa/AppData/Local/Temp/trade_data.json") as f:
    trades = json.load(f)

# Apply the R1 filter (our new deployed filters)
def r1_filter(t):
    setup = t["setup"]
    ga = t.get("greek_alignment")
    svb = t.get("svb")
    if setup == "GEX Long" and (ga is None or ga < 1):
        return False
    if setup == "AG Short" and ga == -3:
        return False
    if setup == "DD Exhaustion":
        if svb is not None and -0.5 <= svb < 0:
            return False
    if setup == "ES Absorption" and ga is not None and ga < 0:
        return False
    return True

filtered = [t for t in trades if r1_filter(t)]
filtered.sort(key=lambda x: x["ts"])

# Group by date
by_date = defaultdict(list)
for t in filtered:
    by_date[t["trade_date"]].append(t)

def estimate_hold_time(t):
    """Estimate how long a trade holds the position (in minutes)."""
    elapsed = t.get("elapsed_min")
    if elapsed and elapsed > 0:
        return elapsed
    # Default estimates by setup type
    defaults = {
        "Skew Charm": 30,
        "DD Exhaustion": 45,
        "AG Short": 30,
        "GEX Long": 30,
        "BofA Scalp": 20,
        "Paradigm Reversal": 20,
        "ES Absorption": 20,
        "CVD Divergence": 20,
    }
    return defaults.get(t["setup"], 30)


print("=" * 100)
print("SINGLE-POSITION SIMULATION vs PORTAL (ALL SIGNALS)")
print("=" * 100)
print()

# ============================================================
# Simulation 1: Pure single position (eval trader style)
# Take first signal, hold until resolved, then take next
# ============================================================

print("--- SIM 1: PURE SINGLE POSITION (first-come, hold until done) ---")
print()

total_portal_pnl = 0
total_single_pnl = 0
total_portal_trades = 0
total_single_trades = 0
total_skipped = 0

daily_results = []

for date in sorted(by_date.keys()):
    day_trades = by_date[date]
    portal_pnl = sum(t["pnl"] for t in day_trades)
    portal_n = len(day_trades)
    total_portal_pnl += portal_pnl
    total_portal_trades += portal_n

    # Single position sim: take trades sequentially
    taken = []
    position_free_at = None  # datetime when current position closes

    for t in day_trades:
        trade_time = datetime.fromisoformat(t["ts"])
        hold_min = estimate_hold_time(t)
        trade_end = trade_time + timedelta(minutes=hold_min)

        if position_free_at is None or trade_time >= position_free_at:
            # Position is free, take this trade
            taken.append(t)
            position_free_at = trade_end
        # else: skip, position occupied

    single_pnl = sum(t["pnl"] for t in taken)
    single_n = len(taken)
    skipped = portal_n - single_n
    total_single_pnl += single_pnl
    total_single_trades += single_n
    total_skipped += skipped

    daily_results.append({
        "date": date, "portal_n": portal_n, "portal_pnl": portal_pnl,
        "single_n": single_n, "single_pnl": single_pnl, "skipped": skipped,
    })

print(f"{'Date':<12} {'Portal N':>9} {'Portal PnL':>11} {'Single N':>9} {'Single PnL':>11} {'Skipped':>8} {'Gap':>8}")
print("-" * 80)
for d in daily_results:
    gap = d["single_pnl"] - d["portal_pnl"]
    print(f"{d['date']:<12} {d['portal_n']:>9} {d['portal_pnl']:>+11.1f} {d['single_n']:>9} {d['single_pnl']:>+11.1f} {d['skipped']:>8} {gap:>+8.1f}")
print("-" * 80)
n_days = len(daily_results)
print(f"{'TOTAL':<12} {total_portal_trades:>9} {total_portal_pnl:>+11.1f} {total_single_trades:>9} {total_single_pnl:>+11.1f} {total_skipped:>8} {total_single_pnl - total_portal_pnl:>+8.1f}")
print(f"{'DAILY AVG':<12} {total_portal_trades/n_days:>9.1f} {total_portal_pnl/n_days:>+11.1f} {total_single_trades/n_days:>9.1f} {total_single_pnl/n_days:>+11.1f}")
print(f"\nCapture rate: {total_single_pnl/total_portal_pnl*100:.0f}% of portal PnL")
print(f"Trades taken: {total_single_trades}/{total_portal_trades} ({total_single_trades/total_portal_trades*100:.0f}%)")


# ============================================================
# Simulation 2: Priority-based single position
# When multiple signals available, pick the best setup
# ============================================================

print()
print("--- SIM 2: PRIORITY SINGLE POSITION (best setup gets priority) ---")
print()

# Priority order based on PF and WR
PRIORITY = {
    "Skew Charm": 1,        # 91% WR, PF 4.53 — king
    "Paradigm Reversal": 2,  # 89% WR, PF 1.50
    "DD Exhaustion": 3,      # 56% WR, PF 1.54 — workhorse
    "AG Short": 4,           # 54% WR, PF 1.71
    "ES Absorption": 5,      # 58% WR but PF 0.94
    "GEX Long": 6,
    "BofA Scalp": 7,
}

total_priority_pnl = 0
total_priority_trades = 0

daily_priority = []

for date in sorted(by_date.keys()):
    day_trades = by_date[date]
    portal_pnl = sum(t["pnl"] for t in day_trades)

    # Sort by time, but within same ~5min window, pick highest priority
    taken = []
    position_free_at = None

    for t in sorted(day_trades, key=lambda x: (x["ts"], PRIORITY.get(x["setup"], 99))):
        trade_time = datetime.fromisoformat(t["ts"])
        hold_min = estimate_hold_time(t)
        trade_end = trade_time + timedelta(minutes=hold_min)

        if position_free_at is None or trade_time >= position_free_at:
            taken.append(t)
            position_free_at = trade_end

    priority_pnl = sum(t["pnl"] for t in taken)
    priority_n = len(taken)
    total_priority_pnl += priority_pnl
    total_priority_trades += priority_n

    daily_priority.append({
        "date": date, "portal_pnl": portal_pnl,
        "priority_n": priority_n, "priority_pnl": priority_pnl,
    })

print(f"{'Date':<12} {'Portal PnL':>11} {'Priority N':>11} {'Priority PnL':>13}")
print("-" * 55)
for d in daily_priority:
    print(f"{d['date']:<12} {d['portal_pnl']:>+11.1f} {d['priority_n']:>11} {d['priority_pnl']:>+13.1f}")
print("-" * 55)
print(f"{'TOTAL':<12} {total_portal_pnl:>+11.1f} {total_priority_trades:>11} {total_priority_pnl:>+13.1f}")
print(f"{'DAILY AVG':<12} {total_portal_pnl/n_days:>+11.1f} {total_priority_trades/n_days:>11.1f} {total_priority_pnl/n_days:>+13.1f}")
print(f"\nCapture rate: {total_priority_pnl/total_portal_pnl*100:.0f}% of portal PnL")


# ============================================================
# Simulation 3: What if we allow 2 simultaneous positions?
# ============================================================

print()
print("--- SIM 3: DUAL POSITION (max 2 open at once) ---")
print()

total_dual_pnl = 0
total_dual_trades = 0

for date in sorted(by_date.keys()):
    day_trades = by_date[date]
    taken = []
    slots = [None, None]  # 2 position slots, each holds free_at datetime

    for t in day_trades:
        trade_time = datetime.fromisoformat(t["ts"])
        hold_min = estimate_hold_time(t)
        trade_end = trade_time + timedelta(minutes=hold_min)

        # Find a free slot
        placed = False
        for i in range(2):
            if slots[i] is None or trade_time >= slots[i]:
                taken.append(t)
                slots[i] = trade_end
                placed = True
                break

    dual_pnl = sum(t["pnl"] for t in taken)
    total_dual_pnl += dual_pnl
    total_dual_trades += len(taken)

print(f"Dual position: {total_dual_trades} trades, {total_dual_pnl:+.1f} pts ({total_dual_pnl/total_portal_pnl*100:.0f}% capture)")
print(f"Daily avg: {total_dual_pnl/n_days:+.1f} pts/day")


# ============================================================
# Simulation 4: Eval-realistic (proven setups only, single pos)
# ============================================================

print()
print("--- SIM 4: EVAL REALISTIC (proven setups only, single position) ---")
print()

eval_setups = {"DD Exhaustion", "AG Short", "Paradigm Reversal", "Skew Charm"}
eval_trades = [t for t in filtered if t["setup"] in eval_setups]
eval_trades.sort(key=lambda x: x["ts"])

eval_by_date = defaultdict(list)
for t in eval_trades:
    eval_by_date[t["trade_date"]].append(t)

total_eval_pnl = 0
total_eval_trades = 0
eval_daily = []

for date in sorted(eval_by_date.keys()):
    day_trades = eval_by_date[date]
    portal_pnl = sum(t["pnl"] for t in day_trades)

    taken = []
    position_free_at = None

    for t in sorted(day_trades, key=lambda x: (x["ts"], PRIORITY.get(x["setup"], 99))):
        trade_time = datetime.fromisoformat(t["ts"])
        hold_min = estimate_hold_time(t)
        trade_end = trade_time + timedelta(minutes=hold_min)

        if position_free_at is None or trade_time >= position_free_at:
            taken.append(t)
            position_free_at = trade_end

    eval_pnl = sum(t["pnl"] for t in taken)
    eval_n = len(taken)
    total_eval_pnl += eval_pnl
    total_eval_trades += eval_n
    eval_daily.append({"date": date, "portal_pnl": portal_pnl, "eval_n": eval_n, "eval_pnl": eval_pnl})

eval_portal_total = sum(t["pnl"] for t in eval_trades)
print(f"{'Date':<12} {'Portal PnL':>11} {'Eval N':>7} {'Eval PnL':>9}")
print("-" * 45)
for d in eval_daily:
    print(f"{d['date']:<12} {d['portal_pnl']:>+11.1f} {d['eval_n']:>7} {d['eval_pnl']:>+9.1f}")
print("-" * 45)
n_eval_days = len(eval_daily)
print(f"{'TOTAL':<12} {eval_portal_total:>+11.1f} {total_eval_trades:>7} {total_eval_pnl:>+9.1f}")
print(f"{'DAILY AVG':<12} {eval_portal_total/n_eval_days:>+11.1f} {total_eval_trades/n_eval_days:>7.1f} {total_eval_pnl/n_eval_days:>+9.1f}")
print(f"\nCapture rate: {total_eval_pnl/eval_portal_total*100:.0f}%")

# Setup breakdown in eval single-pos
eval_taken_all = []
for date in sorted(eval_by_date.keys()):
    day_trades = eval_by_date[date]
    position_free_at = None
    for t in sorted(day_trades, key=lambda x: (x["ts"], PRIORITY.get(x["setup"], 99))):
        trade_time = datetime.fromisoformat(t["ts"])
        hold_min = estimate_hold_time(t)
        trade_end = trade_time + timedelta(minutes=hold_min)
        if position_free_at is None or trade_time >= position_free_at:
            eval_taken_all.append(t)
            position_free_at = trade_end

print("\nSetup breakdown (eval single-position):")
for setup in sorted(eval_setups):
    sub = [t for t in eval_taken_all if t["setup"] == setup]
    if sub:
        w = sum(1 for t in sub if t["result"] == "WIN")
        l = sum(1 for t in sub if t["result"] == "LOSS")
        wr = w/(w+l)*100 if (w+l) > 0 else 0
        pnl = sum(t["pnl"] for t in sub)
        print(f"  {setup:<22} N={len(sub):>3}  WR={wr:>5.1f}%  PnL={pnl:>+7.1f}")


# ============================================================
# INCOME PROJECTIONS
# ============================================================

print()
print("=" * 100)
print("REALISTIC INCOME PROJECTIONS (single-position constrained)")
print("=" * 100)
print()

scenarios = [
    ("Portal (all signals, R1 filter)", total_portal_pnl / n_days),
    ("SIM single-pos (first come)", total_single_pnl / n_days),
    ("SIM priority single-pos", total_priority_pnl / n_days),
    ("SIM dual position", total_dual_pnl / n_days),
    ("Eval realistic (proven, single-pos)", total_eval_pnl / n_eval_days),
]

print(f"{'Scenario':<45} {'Pts/day':>8} {'10 MES':>10} {'2 ES':>10} {'4 ES':>10}")
print("-" * 95)
for name, daily in scenarios:
    monthly = daily * 21
    print(f"{name:<45} {daily:>+8.1f} ${monthly*50:>9,.0f} ${monthly*100:>9,.0f} ${monthly*200:>9,.0f}")
