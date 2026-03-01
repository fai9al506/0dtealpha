"""
Reversal & Overlap Analysis
============================
For every trade in setup_log, find trades that fired while it was still open
(opposite direction). Simulate:
  A) Portal mode: both trades run independently (current)
  B) Keep first, skip second (single position / no-reversal)
  C) Reverse: close first at the moment second fires, take second

Estimate trade duration from outcome_elapsed_min.
If not available, estimate from outcome type (WIN~30min, LOSS~15min, EXPIRED~remaining market time).
"""

import os, psycopg2
from datetime import timedelta

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

# Get all trades with outcomes
cur.execute("""
    SELECT id, setup_name, direction,
           ts AT TIME ZONE 'America/New_York' as ts_et,
           outcome_result, outcome_pnl,
           outcome_max_profit, outcome_max_loss,
           outcome_elapsed_min, outcome_first_event,
           spot, outcome_target_level, outcome_stop_level
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    ORDER BY ts
""")

trades = []
for r in cur.fetchall():
    direction_norm = "long" if r[2].lower() in ("long", "bullish") else "short"
    elapsed = r[8] or 30  # default 30 min if missing
    trades.append({
        "id": r[0], "setup": r[1], "direction": direction_norm,
        "ts": r[3], "result": r[4], "pnl": float(r[5] or 0),
        "max_profit": float(r[6] or 0), "max_loss": float(r[7] or 0),
        "elapsed_min": float(elapsed), "first_event": r[9],
        "spot": float(r[10] or 0),
        "target": float(r[11] or 0), "stop": float(r[12] or 0),
    })

print(f"Total trades with outcomes: {len(trades)}")
print()

# For each trade, find overlapping opposite-direction trades
# Trade A is "open" from ts to ts + elapsed_min
# Trade B fires during that window and is opposite direction

conflicts = []
for i, a in enumerate(trades):
    a_start = a["ts"]
    a_end = a_start + timedelta(minutes=a["elapsed_min"])

    for j, b in enumerate(trades):
        if j <= i:
            continue
        if a["direction"] == b["direction"]:
            continue  # same direction, not a conflict

        b_start = b["ts"]

        # B fires while A is still open
        if a_start < b_start < a_end:
            # Calculate how much of A's P&L was captured by the time B fired
            minutes_into_a = (b_start - a_start).total_seconds() / 60
            pct_elapsed = minutes_into_a / a["elapsed_min"] if a["elapsed_min"] > 0 else 1.0

            conflicts.append({
                "a": a, "b": b,
                "minutes_into_a": minutes_into_a,
                "pct_elapsed": pct_elapsed,
                "same_day": a["ts"].date() == b["ts"].date(),
            })

print(f"Total conflicts (opposite trade fires while position open): {len(conflicts)}")
print(f"Same-day conflicts: {sum(1 for c in conflicts if c['same_day'])}")
print()

# ============ ANALYSIS ============

# Group by day to understand the pattern
from collections import defaultdict
day_conflicts = defaultdict(list)
for c in conflicts:
    day_conflicts[c["a"]["ts"].date()].append(c)

print("=" * 80)
print("CONFLICTS BY DAY")
print("=" * 80)
for day in sorted(day_conflicts):
    cs = day_conflicts[day]
    print(f"\n--- {day} ({len(cs)} conflicts) ---")
    for c in cs:
        a, b = c["a"], c["b"]
        print(f"  Trade A: #{a['id']} {a['setup'][:15]:15s} {a['direction']:5s} pnl={a['pnl']:+6.1f} "
              f"(max_p={a['max_profit']:+.1f} max_l={a['max_loss']:+.1f}) dur={a['elapsed_min']:.0f}min")
        print(f"  Trade B: #{b['id']} {b['setup'][:15]:15s} {b['direction']:5s} pnl={b['pnl']:+6.1f} "
              f"(max_p={b['max_profit']:+.1f} max_l={b['max_loss']:+.1f}) dur={b['elapsed_min']:.0f}min")
        print(f"  B fired {c['minutes_into_a']:.1f} min into A ({c['pct_elapsed']*100:.0f}% of A's life)")
        print()

# ============ SCENARIO SIMULATION ============
print("\n" + "=" * 80)
print("SCENARIO SIMULATION")
print("=" * 80)

# For each conflict, simulate 3 scenarios
# But we need to be careful: a trade can be in multiple conflicts
# Let's analyze each UNIQUE conflict pair

# Scenario A: Portal (both run) — just sum both P&Ls
# Scenario B: Keep first, skip second — take A's P&L only
# Scenario C: Reverse — estimate A's P&L at reversal point, then add B's P&L

# For estimating A's P&L at reversal point:
# If A was ultimately a winner and B fired early, A might have been near 0 or slightly positive
# If A was ultimately a loser and B fired early, A was probably already losing
# Rough estimate: linear interpolation of P&L over time (crude but useful)

total_portal = 0  # sum of both
total_keep_first = 0  # only A
total_keep_better = 0  # keep whichever has better P&L
total_reverse = 0  # estimated A P&L at reversal + B's full P&L

# Track unique trade IDs to avoid double-counting
seen_a = set()
seen_b = set()

print(f"\n{'ID_A':>5} {'Setup_A':>15} {'Dir_A':>5} {'PnL_A':>7} | "
      f"{'ID_B':>5} {'Setup_B':>15} {'Dir_B':>5} {'PnL_B':>7} | "
      f"{'Portal':>7} {'KeepA':>7} {'KeepB':>7} {'Better':>7} | {'Min_In':>6}")
print("-" * 120)

for c in conflicts:
    if not c["same_day"]:
        continue  # only same-day conflicts matter for real execution

    a, b = c["a"], c["b"]
    portal = a["pnl"] + b["pnl"]
    keep_a = a["pnl"]
    keep_b = b["pnl"]
    keep_better = max(a["pnl"], b["pnl"])

    total_portal += portal
    total_keep_first += keep_a

    if a["pnl"] >= b["pnl"]:
        total_keep_better += a["pnl"]
    else:
        total_keep_better += b["pnl"]

    print(f"{a['id']:>5} {a['setup'][:15]:>15} {a['direction']:>5} {a['pnl']:>+7.1f} | "
          f"{b['id']:>5} {b['setup'][:15]:>15} {b['direction']:>5} {b['pnl']:>+7.1f} | "
          f"{portal:>+7.1f} {keep_a:>+7.1f} {keep_b:>+7.1f} {keep_better:>+7.1f} | {c['minutes_into_a']:>5.1f}m")

print("-" * 120)
n_conflicts = sum(1 for c in conflicts if c["same_day"])
print(f"\n{'TOTALS':>30} {'':>20} | "
      f"{total_portal:>+7.1f} {total_keep_first:>+7.1f} {'':>7} {total_keep_better:>+7.1f}")

print(f"\n\nSUMMARY OF SAME-DAY CONFLICTS ({n_conflicts} pairs):")
print(f"  Portal (both trades run):  {total_portal:>+8.1f} pts")
print(f"  Keep First (skip second):  {total_keep_first:>+8.1f} pts")
print(f"  Keep Better (oracle/best): {total_keep_better:>+8.1f} pts")

# ============ WHICH TRADE IS USUALLY BETTER? ============
print("\n\n" + "=" * 80)
print("WHICH TRADE WINS IN CONFLICTS?")
print("=" * 80)

a_wins = 0
b_wins = 0
ties = 0
a_win_margin = 0
b_win_margin = 0

for c in conflicts:
    if not c["same_day"]:
        continue
    a, b = c["a"], c["b"]
    if a["pnl"] > b["pnl"]:
        a_wins += 1
        a_win_margin += a["pnl"] - b["pnl"]
    elif b["pnl"] > a["pnl"]:
        b_wins += 1
        b_win_margin += b["pnl"] - a["pnl"]
    else:
        ties += 1

print(f"  First trade (A) wins: {a_wins} times (avg margin: {a_win_margin/max(a_wins,1):+.1f} pts)")
print(f"  Second trade (B) wins: {b_wins} times (avg margin: {b_win_margin/max(b_wins,1):+.1f} pts)")
print(f"  Ties: {ties}")

# ============ BY SETUP TYPE ============
print("\n\n" + "=" * 80)
print("CONFLICTS BY SETUP PAIR")
print("=" * 80)

pair_stats = defaultdict(lambda: {"count": 0, "portal": 0, "keep_a": 0, "keep_b": 0})
for c in conflicts:
    if not c["same_day"]:
        continue
    a, b = c["a"], c["b"]
    pair = f"{a['setup'][:12]} vs {b['setup'][:12]}"
    pair_stats[pair]["count"] += 1
    pair_stats[pair]["portal"] += a["pnl"] + b["pnl"]
    pair_stats[pair]["keep_a"] += a["pnl"]
    pair_stats[pair]["keep_b"] += b["pnl"]

print(f"\n{'Pair':>30} {'N':>4} {'Portal':>8} {'KeepA':>8} {'KeepB':>8} {'Best':>8}")
print("-" * 75)
for pair in sorted(pair_stats, key=lambda p: pair_stats[p]["count"], reverse=True):
    s = pair_stats[pair]
    best = max(s["keep_a"], s["keep_b"])
    print(f"{pair:>30} {s['count']:>4} {s['portal']:>+8.1f} {s['keep_a']:>+8.1f} {s['keep_b']:>+8.1f} {best:>+8.1f}")

# ============ WHAT IF: SINGLE POSITION MODE ============
print("\n\n" + "=" * 80)
print("FULL SIMULATION: SINGLE POSITION MODE vs PORTAL")
print("=" * 80)
print("Walking through all trades chronologically, one position at a time")
print()

# Simulate: walk through all trades in time order
# If position is open (within elapsed_min), skip new trades
# Compare total P&L

portal_total = sum(t["pnl"] for t in trades)
single_pnl = 0
single_trades = 0
skipped_trades = 0
skipped_pnl_lost = 0
position_end = None

for t in trades:
    if position_end and t["ts"] < position_end:
        # Position still open — skip this trade
        skipped_trades += 1
        skipped_pnl_lost += t["pnl"]
        continue

    # Take this trade
    single_pnl += t["pnl"]
    single_trades += 1
    position_end = t["ts"] + timedelta(minutes=t["elapsed_min"])

print(f"Portal mode:          {len(trades)} trades, {portal_total:>+8.1f} pts")
print(f"Single position mode: {single_trades} trades, {single_pnl:>+8.1f} pts")
print(f"Skipped:              {skipped_trades} trades, {skipped_pnl_lost:>+8.1f} pts lost")
print(f"Difference:           {single_pnl - portal_total:>+8.1f} pts")
print()

# Now try: single position but PRIORITIZE higher-WR setups
print("\n--- Single Position with Setup Priority ---")
print("Priority: ES Absorption > AG Short > DD Exhaustion > BofA Scalp > Paradigm > GEX Long")

PRIORITY = {
    "ES Absorption": 1,
    "AG Short": 2,
    "DD Exhaustion": 3,
    "BofA Scalp": 4,
    "Paradigm Reversal": 5,
    "GEX Long": 6,
}

# Group trades into time windows and pick highest priority
# Walk through, but when we have a choice (multiple firing close together), pick highest priority
single2_pnl = 0
single2_trades = 0
position_end2 = None
current_trade = None

for t in trades:
    pri = PRIORITY.get(t["setup"], 99)

    if position_end2 and t["ts"] < position_end2:
        # Would this be better than current?
        # Only swap if fires within 2 min of current (essentially same signal cluster)
        if current_trade:
            cur_pri = PRIORITY.get(current_trade["setup"], 99)
            time_gap = (t["ts"] - current_trade["ts"]).total_seconds() / 60
            if time_gap <= 2 and pri < cur_pri:
                # Swap: undo current, take this one
                single2_pnl -= current_trade["pnl"]
                single2_pnl += t["pnl"]
                current_trade = t
                position_end2 = t["ts"] + timedelta(minutes=t["elapsed_min"])
        continue

    # Take this trade
    single2_pnl += t["pnl"]
    single2_trades += 1
    position_end2 = t["ts"] + timedelta(minutes=t["elapsed_min"])
    current_trade = t

print(f"Priority single mode: {single2_trades} trades, {single2_pnl:>+8.1f} pts")

# ============ WORST CASE: REVERSAL DAMAGE ============
print("\n\n" + "=" * 80)
print("REVERSAL DAMAGE ANALYSIS")
print("=" * 80)
print("Cases where Trade A was profitable when Trade B fired (would lose the profit)")
print()

reversal_damage = 0
reversal_benefit = 0
damage_cases = 0
benefit_cases = 0

for c in conflicts:
    if not c["same_day"]:
        continue
    a, b = c["a"], c["b"]

    # If A was ultimately profitable, reversing would have cost us
    if a["pnl"] > 0:
        # Estimate: A was likely at some profit when B fired
        # Conservative: assume A was at 50% of final pnl when B fired
        estimated_loss = a["pnl"] * 0.5  # profit we'd give up
        net = b["pnl"] - estimated_loss  # what we'd get instead
        if net < 0:
            reversal_damage += abs(net)
            damage_cases += 1
            print(f"  DAMAGE: #{a['id']} {a['setup'][:12]} +{a['pnl']:.1f} -> reversed for "
                  f"#{b['id']} {b['setup'][:12]} {b['pnl']:+.1f}  "
                  f"(lost ~{estimated_loss:.1f} + got {b['pnl']:+.1f} = {net:+.1f})")
        else:
            reversal_benefit += net
            benefit_cases += 1

print(f"\n  Reversal DAMAGE cases: {damage_cases} (total damage: {reversal_damage:+.1f} pts)")
print(f"  Reversal BENEFIT cases: {benefit_cases} (total benefit: {reversal_benefit:+.1f} pts)")
print(f"  Net reversal impact: {reversal_benefit - reversal_damage:+.1f} pts")

conn.close()
