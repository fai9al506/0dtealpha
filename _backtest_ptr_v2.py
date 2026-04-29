"""
Backtest v2: CORRECTED PTR vs Actual AG Short / GEX Long / Skew Charm
Now using ACTUAL risk management parameters from the code.

ACTUAL RM PARAMETERS (from main.py _trail_params):
  AG Short:  hybrid trail, BE@+10, trail activation=15, gap=5, LIS-based stop (max 20)
  GEX Long:  hybrid trail, BE@+8, trail activation=10, gap=5, initial SL=8
  Skew Charm: hybrid trail, BE@+10, trail activation=10, gap=8, initial SL=14
  DD Exhaust: continuous trail, activation=20, gap=5, initial SL=12

PROPOSED PTR RM (what we're testing):
  Variant A: wider trail gap (10 instead of 5) to let winners run further
  Variant B: T1 partial exit at +10 (50%), trail remainder with wider gap
  Variant C: optimized activation/gap from data
"""
import json
from collections import defaultdict

with open("tmp_setups_full.json") as f:
    full_data = json.load(f)

print("=" * 80)
print("  CORRECTED BACKTEST v2 -- Actual RM vs Alternatives")
print("  Data: 1,080 signals | Feb 18 - Mar 25, 2026")
print("=" * 80)


def simulate_hybrid_trail(mfe, mae, initial_sl, be_trigger, activation, gap):
    """
    Simulate hybrid trailing stop.
    Phase 1: Initial stop at initial_sl
    Phase 2: At +be_trigger, stop moves to breakeven (0)
    Phase 3: At +activation, trail activates at MFE - gap

    Conservative path: MAE happens first (worst case).
    Returns (pnl, result)
    """
    if mfe is None: mfe = 0
    if mae is None: mae = 0
    actual_mae = abs(mae) if mae < 0 else mae

    # Phase 1: Did initial stop get hit?
    if actual_mae >= initial_sl:
        return -initial_sl, "STOP"

    # Phase 2+3: MFE determines trail outcome
    if mfe >= activation:
        # Trail activated - captured MFE minus gap
        capture = mfe - gap
        return round(capture, 1), "WIN"
    elif mfe >= be_trigger:
        # BE triggered but trail never activated
        # Price reversed between BE and activation
        # Conservative: exit at breakeven
        return 0.0, "BE"
    else:
        # Never reached BE trigger, but also didn't hit stop
        # Timed out somewhere between entry and BE trigger
        return 0.0, "TIMEOUT"


def simulate_split_trail(mfe, mae, initial_sl, t1_pts, t1_pct, activation, gap):
    """
    Simulate split-target trailing: T1 partial exit, T2 trails.
    Phase 1: Initial stop at initial_sl
    Phase 2: At +t1_pts, exit t1_pct at fixed profit, move stop to BE
    Phase 3: At +activation, trail activates on remaining (1-t1_pct)

    Returns (pnl, result)
    """
    if mfe is None: mfe = 0
    if mae is None: mae = 0
    actual_mae = abs(mae) if mae < 0 else mae

    # Phase 1: Initial stop
    if actual_mae >= initial_sl:
        return -initial_sl, "STOP"

    # Phase 2: T1 exit
    if mfe < t1_pts:
        return 0.0, "TIMEOUT"

    t1_pnl = t1_pts * t1_pct  # e.g., +10 * 0.50 = +5

    # Phase 3: T2 trailing on remaining
    remaining_pct = 1.0 - t1_pct
    if mfe >= activation:
        t2_capture = mfe - gap
    else:
        # Trail never activated, T2 exits at BE
        t2_capture = 0.0

    t2_pnl = t2_capture * remaining_pct
    total = round(t1_pnl + t2_pnl, 1)
    return total, "WIN" if total > 0 else "BE"


def run_analysis(signals, sim_func, sim_params, label):
    """Run simulation on signals and return stats."""
    results = []
    for sig in signals:
        mfe = sig.get("max_profit") or 0
        mae = sig.get("max_loss") or 0
        pnl, result = sim_func(mfe, mae, **sim_params)
        results.append({"pnl": pnl, "result": result, "sig": sig})

    total_pnl = sum(r["pnl"] for r in results)
    wins = sum(1 for r in results if r["pnl"] > 0)
    losses = sum(1 for r in results if r["pnl"] < 0)
    bes = sum(1 for r in results if r["pnl"] == 0)
    wr = wins / len(results) * 100 if results else 0
    avg = total_pnl / len(results) if results else 0
    gross_win = sum(r["pnl"] for r in results if r["pnl"] > 0)
    gross_loss = abs(sum(r["pnl"] for r in results if r["pnl"] < 0))
    pf = gross_win / gross_loss if gross_loss > 0 else float('inf')

    # Max drawdown (running)
    running = 0
    max_dd = 0
    for r in results:
        running += r["pnl"]
        if running < max_dd:
            max_dd = running

    return {
        "label": label, "n": len(results), "wins": wins, "losses": losses, "bes": bes,
        "wr": wr, "total": total_pnl, "avg": avg, "pf": pf, "max_dd": max_dd,
        "results": results
    }


def print_comparison(stats_list):
    """Print side-by-side comparison of multiple strategies."""
    header = f"{'Metric':<22}"
    for s in stats_list:
        header += f" {s['label']:<22}"
    print(header)
    print("-" * (22 + 23 * len(stats_list)))

    for metric, key, fmt in [
        ("Trades", "n", "{}"),
        ("Wins", "wins", "{}"),
        ("Losses", "losses", "{}"),
        ("Breakeven/TO", "bes", "{}"),
        ("Win Rate", "wr", "{:.1f}%"),
        ("Total P&L", "total", "{:+.1f}"),
        ("Avg P&L/trade", "avg", "{:+.2f}"),
        ("Profit Factor", "pf", "{:.2f}x"),
        ("Max Drawdown", "max_dd", "{:.1f}"),
    ]:
        row = f"{metric:<22}"
        for s in stats_list:
            row += f" {fmt.format(s[key]):<22}"
        print(row)


# ── Load actual outcomes for comparison ──
def actual_stats(signals, label):
    """Compute stats from actual recorded outcomes."""
    total = sum((s.get("pnl") or 0) for s in signals)
    wins = sum(1 for s in signals if s.get("outcome") == "WIN")
    losses = sum(1 for s in signals if s.get("outcome") == "LOSS")
    expired = sum(1 for s in signals if s.get("outcome") == "EXPIRED")
    wr = wins / len(signals) * 100 if signals else 0
    avg = total / len(signals) if signals else 0
    gw = sum((s.get("pnl") or 0) for s in signals if (s.get("pnl") or 0) > 0)
    gl = abs(sum((s.get("pnl") or 0) for s in signals if (s.get("pnl") or 0) < 0))
    pf = gw / gl if gl > 0 else float('inf')
    running = 0
    max_dd = 0
    for s in signals:
        running += (s.get("pnl") or 0)
        if running < max_dd:
            max_dd = running
    return {
        "label": label, "n": len(signals), "wins": wins, "losses": losses, "bes": expired,
        "wr": wr, "total": total, "avg": avg, "pf": pf, "max_dd": max_dd,
    }


# ======================================================================
# AG SHORT ANALYSIS
# ======================================================================
print("\n" + "=" * 80)
print("  AG SHORT: Actual vs Simulated Alternatives (73 signals)")
print("=" * 80)

ag = [d for d in full_data if d["setup_name"] == "AG Short"]

# Actual outcomes
ag_actual = actual_stats(ag, "ACTUAL (live)")

# Current RM simulation (verify it matches actual)
ag_current_sim = run_analysis(ag, simulate_hybrid_trail,
    {"initial_sl": 20, "be_trigger": 10, "activation": 15, "gap": 5},
    "Sim: Current RM")

# Alternative 1: Tighter initial stop (15 instead of 20)
ag_alt1 = run_analysis(ag, simulate_hybrid_trail,
    {"initial_sl": 15, "be_trigger": 10, "activation": 15, "gap": 5},
    "Alt1: SL=15")

# Alternative 2: Wider trail gap (8 instead of 5)
ag_alt2 = run_analysis(ag, simulate_hybrid_trail,
    {"initial_sl": 20, "be_trigger": 10, "activation": 15, "gap": 8},
    "Alt2: gap=8")

# Alternative 3: Lower activation (12 instead of 15)
ag_alt3 = run_analysis(ag, simulate_hybrid_trail,
    {"initial_sl": 20, "be_trigger": 8, "activation": 12, "gap": 5},
    "Alt3: act=12")

# Alternative 4: Split target (T1 50% at +10, trail rest with gap=8)
ag_alt4 = run_analysis(ag, simulate_split_trail,
    {"initial_sl": 20, "t1_pts": 10, "t1_pct": 0.5, "activation": 20, "gap": 8},
    "Alt4: Split T1+Trail")

# Alternative 5: Wider gap + lower activation (let it breathe more)
ag_alt5 = run_analysis(ag, simulate_hybrid_trail,
    {"initial_sl": 20, "be_trigger": 10, "activation": 20, "gap": 3},
    "Alt5: act=20,gap=3")

print("\n--- Batch 1: Stop & trail tuning ---")
print_comparison([ag_actual, ag_current_sim, ag_alt1, ag_alt2])

print("\n--- Batch 2: Activation & split ---")
print_comparison([ag_actual, ag_alt3, ag_alt4, ag_alt5])


# ======================================================================
# AG SHORT BY PARADIGM SUBTYPE
# ======================================================================
print("\n" + "=" * 80)
print("  AG SHORT: Performance by Paradigm Subtype")
print("=" * 80)

for para in ["AG-PURE", "AG-LIS", "AG-TARGET"]:
    sigs = [s for s in ag if s.get("paradigm") == para]
    if len(sigs) < 3:
        continue
    stats = actual_stats(sigs, f"{para}")
    avg_mfe = sum((s.get("max_profit") or 0) for s in sigs) / len(sigs)
    avg_mae = sum(abs((s.get("max_loss") or 0)) for s in sigs) / len(sigs)
    print(f"\n{para} ({len(sigs)} trades):")
    print(f"  WR: {stats['wr']:.1f}% | Total: {stats['total']:+.1f} pts | Avg: {stats['avg']:+.1f} | PF: {stats['pf']:.2f}x")
    print(f"  Avg MFE: {avg_mfe:.1f} | Avg MAE: {avg_mae:.1f} | Max DD: {stats['max_dd']:.1f}")

    # Best alternative for this paradigm
    best_label = ""
    best_pnl = stats["total"]
    for label, params in [
        ("gap=8", {"initial_sl": 20, "be_trigger": 10, "activation": 15, "gap": 8}),
        ("gap=3", {"initial_sl": 20, "be_trigger": 10, "activation": 15, "gap": 3}),
        ("act=12", {"initial_sl": 20, "be_trigger": 8, "activation": 12, "gap": 5}),
        ("act=20,g=3", {"initial_sl": 20, "be_trigger": 10, "activation": 20, "gap": 3}),
    ]:
        sim = run_analysis(sigs, simulate_hybrid_trail, params, label)
        if sim["total"] > best_pnl:
            best_pnl = sim["total"]
            best_label = f"{label}: {sim['total']:+.1f} pts ({sim['wr']:.0f}% WR)"
    if best_label:
        print(f"  Better alt: {best_label}")
    else:
        print(f"  Current RM is optimal for this subtype")


# ======================================================================
# AG SHORT BY GRADE
# ======================================================================
print("\n" + "=" * 80)
print("  AG SHORT: Performance by Grade")
print("=" * 80)

for grade in ["A+", "A", "B", "C", "LOG"]:
    sigs = [s for s in ag if s.get("grade") == grade]
    if len(sigs) < 2:
        continue
    stats = actual_stats(sigs, grade)
    avg_mfe = sum((s.get("max_profit") or 0) for s in sigs) / len(sigs)
    print(f"  {grade:<5}: n={len(sigs):<3} WR={stats['wr']:>5.1f}% | PnL={stats['total']:>+7.1f} | Avg={stats['avg']:>+5.1f} | MFE={avg_mfe:>5.1f} | PF={stats['pf']:.2f}x")


# ======================================================================
# GEX LONG ANALYSIS
# ======================================================================
print("\n" + "=" * 80)
print("  GEX LONG: Actual vs Alternatives (50 signals)")
print("=" * 80)

gex = [d for d in full_data if d["setup_name"] == "GEX Long"]
gex_actual = actual_stats(gex, "ACTUAL (live)")

gex_current_sim = run_analysis(gex, simulate_hybrid_trail,
    {"initial_sl": 8, "be_trigger": 8, "activation": 10, "gap": 5},
    "Sim: Current RM")

gex_alt1 = run_analysis(gex, simulate_hybrid_trail,
    {"initial_sl": 12, "be_trigger": 8, "activation": 10, "gap": 5},
    "Alt1: SL=12")

gex_alt2 = run_analysis(gex, simulate_hybrid_trail,
    {"initial_sl": 8, "be_trigger": 8, "activation": 10, "gap": 3},
    "Alt2: gap=3")

print()
print_comparison([gex_actual, gex_current_sim, gex_alt1, gex_alt2])


# ======================================================================
# SKEW CHARM ANALYSIS
# ======================================================================
print("\n" + "=" * 80)
print("  SKEW CHARM: Actual vs Alternatives (240 signals)")
print("=" * 80)

sc = [d for d in full_data if d["setup_name"] == "Skew Charm"]
sc_actual = actual_stats(sc, "ACTUAL (live)")

sc_current_sim = run_analysis(sc, simulate_hybrid_trail,
    {"initial_sl": 14, "be_trigger": 10, "activation": 10, "gap": 8},
    "Sim: Current RM")

sc_alt1 = run_analysis(sc, simulate_hybrid_trail,
    {"initial_sl": 14, "be_trigger": 10, "activation": 15, "gap": 5},
    "Alt1: act=15,gap=5")

sc_alt2 = run_analysis(sc, simulate_hybrid_trail,
    {"initial_sl": 14, "be_trigger": 10, "activation": 12, "gap": 6},
    "Alt2: act=12,gap=6")

sc_alt3 = run_analysis(sc, simulate_split_trail,
    {"initial_sl": 14, "t1_pts": 10, "t1_pct": 0.5, "activation": 15, "gap": 8},
    "Alt3: Split+Trail")

print()
print_comparison([sc_actual, sc_current_sim, sc_alt1, sc_alt2])
print()
print_comparison([sc_actual, sc_alt3])


# ======================================================================
# UNTAPPED POTENTIAL: How much MFE are we actually capturing?
# ======================================================================
print("\n" + "=" * 80)
print("  CAPTURE EFFICIENCY: What % of available MFE do we actually capture?")
print("=" * 80)

for setup_name in ["AG Short", "GEX Long", "Skew Charm", "ES Absorption", "DD Exhaustion"]:
    sigs = [d for d in full_data if d["setup_name"] == setup_name]
    if not sigs:
        continue

    winning = [s for s in sigs if (s.get("pnl") or 0) > 0]
    if not winning:
        continue

    avg_win_pnl = sum((s.get("pnl") or 0) for s in winning) / len(winning)
    avg_win_mfe = sum((s.get("max_profit") or 0) for s in winning) / len(winning)
    capture_rate = (avg_win_pnl / avg_win_mfe * 100) if avg_win_mfe > 0 else 0

    all_mfe = sum((s.get("max_profit") or 0) for s in sigs) / len(sigs)
    all_pnl = sum((s.get("pnl") or 0) for s in sigs) / len(sigs)

    print(f"\n{setup_name} ({len(sigs)} trades, {len(winning)} winners):")
    print(f"  Winners: Avg P&L={avg_win_pnl:+.1f} | Avg MFE={avg_win_mfe:.1f} | Capture={capture_rate:.0f}%")
    print(f"  All:     Avg P&L={all_pnl:+.1f} | Avg MFE={all_mfe:.1f}")

    # Distribution of "left on table" for winners
    left = [(s.get("max_profit") or 0) - (s.get("pnl") or 0) for s in winning]
    left_avg = sum(left) / len(left)
    left_big = sum(1 for l in left if l >= 10)
    print(f"  Left on table (winners): Avg={left_avg:.1f} pts | 10+ pts left: {left_big} ({left_big/len(winning)*100:.0f}%)")


# ======================================================================
# THE REAL QUESTION: Where is the trend opportunity?
# ======================================================================
print("\n" + "=" * 80)
print("  WHERE IS THE REAL TREND OPPORTUNITY?")
print("=" * 80)

print("""
AG Short already trails (BE@+10, activation=15, gap=5).
The question: where is P&L being LOST vs where is it optimal?
""")

# Breakdown: Where do AG Short losses come from?
ag_losses = [s for s in ag if s.get("outcome") == "LOSS"]
ag_wins = [s for s in ag if s.get("outcome") == "WIN"]

print(f"AG Short losses ({len(ag_losses)} trades):")
loss_by_mae = defaultdict(int)
for s in ag_losses:
    mae = abs(s.get("max_loss") or 0)
    mfe = s.get("max_profit") or 0
    if mae >= 20:
        bucket = "Full stop (-20)"
    elif mae >= 15:
        bucket = "Near stop (-15 to -20)"
    else:
        bucket = "Partial loss (< -15)"
    loss_by_mae[bucket] += 1

for bucket, count in sorted(loss_by_mae.items()):
    pct = count / len(ag_losses) * 100
    print(f"  {bucket}: {count} ({pct:.0f}%)")

# What was the MFE before these losses?
print(f"\nLoss trades MFE before stop:")
for s in sorted(ag_losses, key=lambda x: -(x.get("max_profit") or 0)):
    mfe = s.get("max_profit") or 0
    mae = s.get("max_loss") or 0
    pnl = s.get("pnl") or 0
    print(f"  {s['ts'][:16]} {s['grade']:<5} {s['paradigm']:<14} MFE={mfe:>5.1f} MAE={mae:>6.1f} PnL={pnl:>+6.1f}")

# AG wins: how much was the trail actually capturing?
print(f"\nAG Short wins ({len(ag_wins)} trades) - trail efficiency:")
for s in sorted(ag_wins, key=lambda x: -(x.get("max_profit") or 0)):
    mfe = s.get("max_profit") or 0
    pnl = s.get("pnl") or 0
    left = mfe - pnl
    eff = (pnl / mfe * 100) if mfe > 0 else 0
    print(f"  {s['ts'][:16]} {s['grade']:<5} {s['paradigm']:<14} MFE={mfe:>5.1f} PnL={pnl:>+6.1f} Left={left:>5.1f} Eff={eff:.0f}%")


print("\n" + "=" * 80)
print("  SUMMARY")
print("=" * 80)
print(f"""
AG Short ACTUAL: {len(ag)} trades, {actual_stats(ag, '')['wr']:.1f}% WR, {actual_stats(ag, '')['total']:+.1f} pts
  - Already uses trailing stop (BE@10, trail activation=15, gap=5)
  - Avg winner captures {sum((s.get('pnl') or 0) for s in ag_wins)/len(ag_wins):.1f} pts of {sum((s.get('max_profit') or 0) for s in ag_wins)/len(ag_wins):.1f} MFE ({sum((s.get('pnl') or 0) for s in ag_wins)/sum((s.get('max_profit') or 0) for s in ag_wins)*100:.0f}% capture)
  - {len(ag_losses)} losses at avg {sum((s.get('pnl') or 0) for s in ag_losses)/len(ag_losses):.1f} pts

GEX Long ACTUAL: {len(gex)} trades, {actual_stats(gex, '')['wr']:.1f}% WR, {actual_stats(gex, '')['total']:+.1f} pts
  - Avg MFE only {sum((s.get('max_profit') or 0) for s in gex)/len(gex):.1f} pts - NOT a trend candidate

Skew Charm ACTUAL: {len(sc)} trades, {actual_stats(sc, '')['wr']:.1f}% WR, {actual_stats(sc, '')['total']:+.1f} pts
  - Highest volume setup, already trailing
""")
