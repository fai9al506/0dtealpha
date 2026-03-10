"""
Smart single-position trade selection.
Goal: When you can only take ~5-6 trades/day, pick the BEST ones.
"""
import json
from collections import defaultdict
from datetime import datetime, timedelta

with open("C:/Users/Faisa/AppData/Local/Temp/trade_data.json") as f:
    trades = json.load(f)

# R1 filter (deployed)
def r1_filter(t):
    setup = t["setup"]
    ga = t.get("greek_alignment")
    svb = t.get("svb")
    if setup == "GEX Long" and (ga is None or ga < 1): return False
    if setup == "AG Short" and ga == -3: return False
    if setup == "DD Exhaustion":
        if svb is not None and -0.5 <= svb < 0: return False
    if setup == "ES Absorption" and ga is not None and ga < 0: return False
    return True

filtered = [t for t in trades if r1_filter(t)]
filtered.sort(key=lambda x: x["ts"])

by_date = defaultdict(list)
for t in filtered:
    by_date[t["trade_date"]].append(t)

def estimate_hold(t):
    e = t.get("elapsed_min")
    if e and e > 0: return e
    return {"Skew Charm": 30, "DD Exhaustion": 45, "AG Short": 30,
            "GEX Long": 30, "BofA Scalp": 20, "Paradigm Reversal": 20,
            "ES Absorption": 20, "CVD Divergence": 20}.get(t["setup"], 30)

def sim_single_pos(day_trades, quality_fn=None):
    """Simulate single-position with optional quality filter.
    quality_fn(trade) -> True to take, False to skip."""
    taken = []
    position_free_at = None
    for t in day_trades:
        trade_time = datetime.fromisoformat(t["ts"])
        if position_free_at is not None and trade_time < position_free_at:
            continue  # position occupied
        # Quality gate
        if quality_fn and not quality_fn(t):
            continue
        taken.append(t)
        position_free_at = trade_time + timedelta(minutes=estimate_hold(t))
    return taken

def run_scenario(name, quality_fn=None, setup_filter=None):
    """Run single-pos sim across all days with a quality filter."""
    total_pnl = 0
    total_n = 0
    wins = 0
    losses = 0
    daily_pnls = []
    all_taken = []

    for date in sorted(by_date.keys()):
        day_trades = by_date[date]
        if setup_filter:
            day_trades = [t for t in day_trades if t["setup"] in setup_filter]
        taken = sim_single_pos(day_trades, quality_fn)
        pnl = sum(t["pnl"] for t in taken)
        total_pnl += pnl
        total_n += len(taken)
        wins += sum(1 for t in taken if t["result"] == "WIN")
        losses += sum(1 for t in taken if t["result"] == "LOSS")
        daily_pnls.append(pnl)
        all_taken.extend(taken)

    n_days = len(by_date)
    wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    daily_avg = total_pnl / n_days

    # Max drawdown
    cum = peak = dd = 0
    for p in daily_pnls:
        cum += p; peak = max(peak, cum); dd = max(dd, peak - cum)

    return {
        "name": name, "n": total_n, "w": wins, "l": losses,
        "wr": wr, "pnl": total_pnl, "daily": daily_avg, "dd": dd,
        "taken": all_taken
    }


print("=" * 110)
print("SMART SELECTION: Which quality filters maximize single-position PnL?")
print("=" * 110)
print()

# ============================================================
# Quality filter definitions
# ============================================================

proven = {"DD Exhaustion", "AG Short", "Paradigm Reversal", "Skew Charm"}
all_setups = None  # no filter

scenarios = []

# Baseline: take everything, first come
scenarios.append(run_scenario("Baseline (first come, all setups)"))

# S1: Proven setups only
scenarios.append(run_scenario("Proven setups only (DD/AG/Para/Skew)", setup_filter=proven))

# S2: Alignment >= +1
scenarios.append(run_scenario("Alignment >= +1",
    quality_fn=lambda t: t.get("greek_alignment") is not None and t["greek_alignment"] >= 1))

# S3: Alignment >= +2
scenarios.append(run_scenario("Alignment >= +2",
    quality_fn=lambda t: t.get("greek_alignment") is not None and t["greek_alignment"] >= 2))

# S4: Proven + alignment >= +1
scenarios.append(run_scenario("Proven + alignment >= +1",
    setup_filter=proven,
    quality_fn=lambda t: t.get("greek_alignment") is not None and t["greek_alignment"] >= 1))

# S5: Proven + alignment >= +2
scenarios.append(run_scenario("Proven + alignment >= +2",
    setup_filter=proven,
    quality_fn=lambda t: t.get("greek_alignment") is not None and t["greek_alignment"] >= 2))

# S6: Skip first hour (10:30+) — morning is often choppy
scenarios.append(run_scenario("Skip first hour (after 10:30 ET)",
    quality_fn=lambda t: t.get("hour_et") and t["hour_et"] >= 10.5))

# S7: Proven + skip first hour
scenarios.append(run_scenario("Proven + skip first hour",
    setup_filter=proven,
    quality_fn=lambda t: t.get("hour_et") and t["hour_et"] >= 10.5))

# S8: Only Skew Charm + DD (the two big earners)
scenarios.append(run_scenario("Only Skew + DD",
    setup_filter={"Skew Charm", "DD Exhaustion"}))

# S9: Only Skew Charm + DD, alignment >= +1
scenarios.append(run_scenario("Skew + DD, alignment >= +1",
    setup_filter={"Skew Charm", "DD Exhaustion"},
    quality_fn=lambda t: t.get("greek_alignment") is not None and t["greek_alignment"] >= 1))

# S10: Grade A or better only (skip B, C, A-Entry)
scenarios.append(run_scenario("Grade A+ or A only",
    quality_fn=lambda t: t.get("grade") in ("A+", "A")))

# S11: Proven + Grade A+ or A
scenarios.append(run_scenario("Proven + Grade A+/A",
    setup_filter=proven,
    quality_fn=lambda t: t.get("grade") in ("A+", "A")))

# S12: Skip ES Absorption entirely (it's net negative)
no_abs = {"DD Exhaustion", "AG Short", "Paradigm Reversal", "Skew Charm", "BofA Scalp", "GEX Long"}
scenarios.append(run_scenario("All except ES Absorption",
    setup_filter=no_abs))

# S13: Skew Charm priority — always take Skew Charm, fill gaps with DD/AG/Para
# (need custom sim for this)

# S14: Best 2 time windows only (11-13 for DD, all day for Skew)
def s14_fn(t):
    h = t.get("hour_et")
    if not h: return False
    if t["setup"] == "Skew Charm": return True  # Skew works all day
    if t["setup"] == "Paradigm Reversal": return True  # always take
    return 11 <= h < 13  # DD/AG only in sweet spot

scenarios.append(run_scenario("Skew/Para anytime + DD/AG only 11-13",
    setup_filter=proven, quality_fn=s14_fn))

# S15: Proven + alignment >= +1 + skip first hour
scenarios.append(run_scenario("Proven + align >= +1 + skip 1st hour",
    setup_filter=proven,
    quality_fn=lambda t: (t.get("greek_alignment") is not None and t["greek_alignment"] >= 1
                          and t.get("hour_et") and t["hour_et"] >= 10.5)))

# Print results table
print(f"{'#':<4} {'Scenario':<45} {'N':>4} {'W':>4} {'L':>4} {'WR':>6} {'PnL':>8} {'Pts/d':>6} {'DD':>6} {'$/mo 10M':>10}")
print("-" * 105)
for i, s in enumerate(scenarios):
    monthly = s["daily"] * 21 * 50  # 10 MES * $5/pt
    print(f"{i:<4} {s['name']:<45} {s['n']:>4} {s['w']:>4} {s['l']:>4} {s['wr']:>5.1f}% {s['pnl']:>+8.1f} {s['daily']:>+6.1f} {s['dd']:>6.1f} ${monthly:>9,.0f}")


# ============================================================
# Deep dive on top 3 scenarios
# ============================================================

# Find top 3 by daily PnL
top3 = sorted(scenarios, key=lambda s: s["daily"], reverse=True)[:5]

print()
print("=" * 110)
print("TOP 5 SCENARIOS — Daily breakdown")
print("=" * 110)

for s in top3:
    print(f"\n--- {s['name']} ---")
    # Per-setup breakdown
    setup_stats = defaultdict(lambda: {"n": 0, "w": 0, "l": 0, "pnl": 0})
    for t in s["taken"]:
        ss = setup_stats[t["setup"]]
        ss["n"] += 1
        ss["pnl"] += t["pnl"]
        if t["result"] == "WIN": ss["w"] += 1
        elif t["result"] == "LOSS": ss["l"] += 1

    for setup in sorted(setup_stats.keys()):
        ss = setup_stats[setup]
        wr = ss["w"] / (ss["w"] + ss["l"]) * 100 if (ss["w"] + ss["l"]) > 0 else 0
        print(f"  {setup:<25} N={ss['n']:>3}  WR={wr:>5.1f}%  PnL={ss['pnl']:>+7.1f}")


# ============================================================
# Compare: what if we could SWITCH mid-trade?
# If Skew Charm fires while in a DD position, close DD and take Skew
# ============================================================

print()
print("=" * 110)
print("PREEMPTION: Close lower-priority trade when higher-priority fires")
print("=" * 110)

PRIORITY_RANK = {
    "Skew Charm": 1,
    "Paradigm Reversal": 2,
    "AG Short": 3,
    "DD Exhaustion": 4,
    "ES Absorption": 5,
    "GEX Long": 6,
    "BofA Scalp": 7,
}

total_preempt_pnl = 0
total_preempt_n = 0
preempt_closes = 0

for date in sorted(by_date.keys()):
    day_trades = [t for t in by_date[date] if t["setup"] in proven]
    taken = []
    current_trade = None
    current_free_at = None

    for t in day_trades:
        trade_time = datetime.fromisoformat(t["ts"])
        hold = estimate_hold(t)
        trade_end = trade_time + timedelta(minutes=hold)
        t_rank = PRIORITY_RANK.get(t["setup"], 99)

        if current_trade is None or trade_time >= current_free_at:
            # Position free, take it
            current_trade = t
            current_free_at = trade_end
            taken.append(t)
        elif t_rank < PRIORITY_RANK.get(current_trade["setup"], 99):
            # Higher priority signal — preempt current trade
            # Estimate PnL of closed trade (use partial elapsed as proxy)
            # In reality we'd close at current price — approximate as 0 PnL for preempted trade
            preempt_closes += 1
            current_trade = t
            current_free_at = trade_end
            taken.append(t)

    pnl = sum(t["pnl"] for t in taken)
    total_preempt_pnl += pnl
    total_preempt_n += len(taken)

n_days = len(by_date)
print(f"Preemption (proven setups): {total_preempt_n} trades, {total_preempt_pnl:+.1f} pts, "
      f"{total_preempt_pnl/n_days:+.1f} pts/day, preemptions={preempt_closes}")
print(f"Monthly @ 10 MES: ${total_preempt_pnl/n_days * 21 * 50:,.0f}")
print()
print("NOTE: Preemption PnL is optimistic — assumes preempted trade exits at 0 PnL.")
print("Real impact depends on where the preempted trade is at time of switch.")
