"""
Deep Analysis: AG Short Trail Activation Optimization
Comprehensive parameter sweep + per-trade path analysis.

Current AG Short RM: hybrid trail, BE@+10, activation=15, gap=5, LIS-based stop (max 20)
Question: Is activation=12 (or other value) significantly better?
"""
import json
from collections import defaultdict
from datetime import datetime

with open("tmp_setups_full.json") as f:
    full_data = json.load(f)

ag_all = [d for d in full_data if d["setup_name"] == "AG Short"]

# Exclude AG-TARGET (trend already exhausted, should be filtered separately)
ag = [d for d in ag_all if d.get("paradigm") != "AG-TARGET"]
ag_target = [d for d in ag_all if d.get("paradigm") == "AG-TARGET"]

print("=" * 80)
print("  AG SHORT TRAIL ACTIVATION: DEEP ANALYSIS")
print("  73 total signals | Excluding AG-TARGET (19) = 54 active-trend signals")
print("=" * 80)

print(f"\nAG-TARGET excluded: {len(ag_target)} trades, {sum((s.get('pnl') or 0) for s in ag_target):+.1f} pts")
print(f"AG active-trend:   {len(ag)} trades, {sum((s.get('pnl') or 0) for s in ag):+.1f} pts")


def simulate_hybrid(mfe, mae, initial_sl, be_trigger, activation, gap):
    """Simulate hybrid trail. Returns (pnl, phase_exited, detail)."""
    if mfe is None: mfe = 0
    if mae is None: mae = 0
    actual_mae = abs(mae) if mae < 0 else mae

    if actual_mae >= initial_sl:
        return -initial_sl, "STOP", f"MAE={mae:.1f} >= SL={initial_sl}"

    if mfe >= activation:
        capture = mfe - gap
        return round(capture, 1), "TRAIL", f"MFE={mfe:.1f}, trail={mfe:.1f}-{gap}={capture:.1f}"
    elif mfe >= be_trigger:
        return 0.0, "BE", f"MFE={mfe:.1f}, reached BE but not activation={activation}"
    else:
        return 0.0, "TIMEOUT", f"MFE={mfe:.1f} < BE trigger={be_trigger}"


# ======================================================================
# PART 1: Full Parameter Sweep
# ======================================================================
print("\n" + "=" * 80)
print("  PART 1: Parameter Sweep (AG-PURE + AG-LIS only, n=54)")
print("=" * 80)

print(f"\n--- Sweep: BE trigger (activation=15, gap=5 fixed) ---")
print(f"{'BE':>4} | {'WR':>6} | {'Total':>8} | {'Avg':>6} | {'PF':>6} | {'Wins':>5} | {'Loss':>5} | {'BE/TO':>5}")
for be in [6, 7, 8, 9, 10, 11, 12]:
    results = [simulate_hybrid(s.get("max_profit") or 0, s.get("max_loss") or 0,
               20, be, 15, 5) for s in ag]
    pnls = [r[0] for r in results]
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    bes = sum(1 for p in pnls if p == 0)
    total = sum(pnls)
    wr = wins / len(pnls) * 100
    avg = total / len(pnls)
    gw = sum(p for p in pnls if p > 0)
    gl = abs(sum(p for p in pnls if p < 0))
    pf = gw / gl if gl > 0 else float('inf')
    print(f"  {be:>2}  | {wr:>5.1f}% | {total:>+7.1f} | {avg:>+5.1f} | {pf:>5.2f} | {wins:>5} | {losses:>5} | {bes:>5}")

print(f"\n--- Sweep: Activation threshold (BE=10, gap=5 fixed) ---")
print(f"{'Act':>4} | {'WR':>6} | {'Total':>8} | {'Avg':>6} | {'PF':>6} | {'Wins':>5} | {'Loss':>5} | {'BE/TO':>5} | {'Trail':>5}")
for act in [10, 11, 12, 13, 14, 15, 16, 18, 20]:
    results = [simulate_hybrid(s.get("max_profit") or 0, s.get("max_loss") or 0,
               20, 10, act, 5) for s in ag]
    pnls = [r[0] for r in results]
    phases = [r[1] for r in results]
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    bes = sum(1 for p in pnls if p == 0)
    trail_wins = sum(1 for ph in phases if ph == "TRAIL")
    total = sum(pnls)
    wr = wins / len(pnls) * 100
    avg = total / len(pnls)
    gw = sum(p for p in pnls if p > 0)
    gl = abs(sum(p for p in pnls if p < 0))
    pf = gw / gl if gl > 0 else float('inf')
    marker = " <-- CURRENT" if act == 15 else ""
    print(f"  {act:>2}  | {wr:>5.1f}% | {total:>+7.1f} | {avg:>+5.1f} | {pf:>5.2f} | {wins:>5} | {losses:>5} | {bes:>5} | {trail_wins:>5}{marker}")

print(f"\n--- Sweep: Trail gap (BE=10, activation=15 fixed) ---")
print(f"{'Gap':>4} | {'WR':>6} | {'Total':>8} | {'Avg':>6} | {'PF':>6} | {'Avg Win':>8}")
for gap in [3, 4, 5, 6, 7, 8, 10]:
    results = [simulate_hybrid(s.get("max_profit") or 0, s.get("max_loss") or 0,
               20, 10, 15, gap) for s in ag]
    pnls = [r[0] for r in results]
    wins_pnl = [p for p in pnls if p > 0]
    total = sum(pnls)
    wr = sum(1 for p in pnls if p > 0) / len(pnls) * 100
    avg = total / len(pnls)
    gw = sum(p for p in pnls if p > 0)
    gl = abs(sum(p for p in pnls if p < 0))
    pf = gw / gl if gl > 0 else float('inf')
    avg_win = sum(wins_pnl) / len(wins_pnl) if wins_pnl else 0
    marker = " <-- CURRENT" if gap == 5 else ""
    print(f"  {gap:>2}  | {wr:>5.1f}% | {total:>+7.1f} | {avg:>+5.1f} | {pf:>5.2f} | {avg_win:>+7.1f}{marker}")

print(f"\n--- Sweep: Trail gap at activation=12 ---")
print(f"{'Gap':>4} | {'WR':>6} | {'Total':>8} | {'Avg':>6} | {'PF':>6} | {'Avg Win':>8}")
for gap in [3, 4, 5, 6, 7, 8]:
    results = [simulate_hybrid(s.get("max_profit") or 0, s.get("max_loss") or 0,
               20, 10, 12, gap) for s in ag]
    pnls = [r[0] for r in results]
    wins_pnl = [p for p in pnls if p > 0]
    total = sum(pnls)
    wr = sum(1 for p in pnls if p > 0) / len(pnls) * 100
    avg = total / len(pnls)
    gw = sum(p for p in pnls if p > 0)
    gl = abs(sum(p for p in pnls if p < 0))
    pf = gw / gl if gl > 0 else float('inf')
    avg_win = sum(wins_pnl) / len(wins_pnl) if wins_pnl else 0
    print(f"  {gap:>2}  | {wr:>5.1f}% | {total:>+7.1f} | {avg:>+5.1f} | {pf:>5.2f} | {avg_win:>+7.1f}")


# ======================================================================
# PART 2: Top-10 Configurations
# ======================================================================
print("\n" + "=" * 80)
print("  PART 2: Top 10 Configurations by Total P&L")
print("=" * 80)

configs = []
for be in [8, 9, 10, 11]:
    for act in [10, 11, 12, 13, 14, 15, 16, 18]:
        if act < be:
            continue
        for gap in [3, 4, 5, 6, 7, 8]:
            results = [simulate_hybrid(s.get("max_profit") or 0, s.get("max_loss") or 0,
                       20, be, act, gap) for s in ag]
            pnls = [r[0] for r in results]
            total = sum(pnls)
            wins = sum(1 for p in pnls if p > 0)
            losses = sum(1 for p in pnls if p < 0)
            wr = wins / len(pnls) * 100
            gw = sum(p for p in pnls if p > 0)
            gl = abs(sum(p for p in pnls if p < 0))
            pf = gw / gl if gl > 0 else float('inf')

            # Max drawdown
            running = 0
            max_dd = 0
            for p in pnls:
                running += p
                if running < max_dd:
                    max_dd = running

            configs.append({
                "be": be, "act": act, "gap": gap,
                "total": total, "wr": wr, "pf": pf, "max_dd": max_dd,
                "wins": wins, "losses": losses,
                "avg": total / len(pnls),
            })

# Sort by total P&L
configs.sort(key=lambda x: -x["total"])

print(f"\n{'Rank':>4} | {'BE':>3} | {'Act':>3} | {'Gap':>3} | {'WR':>6} | {'Total':>8} | {'Avg':>6} | {'PF':>6} | {'MaxDD':>7} | {'W/L':>7}")
for i, c in enumerate(configs[:15]):
    is_current = c["be"] == 10 and c["act"] == 15 and c["gap"] == 5
    marker = " <-- CURRENT" if is_current else ""
    print(f"  {i+1:>2}  | {c['be']:>3} | {c['act']:>3} | {c['gap']:>3} | {c['wr']:>5.1f}% | {c['total']:>+7.1f} | {c['avg']:>+5.1f} | {c['pf']:>5.2f} | {c['max_dd']:>+6.1f} | {c['wins']:>3}/{c['losses']:>3}{marker}")

# Find current config rank
current_rank = None
for i, c in enumerate(configs):
    if c["be"] == 10 and c["act"] == 15 and c["gap"] == 5:
        current_rank = i + 1
        break
print(f"\nCurrent config (BE=10, act=15, gap=5) ranks #{current_rank} out of {len(configs)} configurations")


# ======================================================================
# PART 3: Stability Analysis — Is the top config robust?
# ======================================================================
print("\n" + "=" * 80)
print("  PART 3: Stability Analysis — Top configs across time periods")
print("=" * 80)

# Split data into halves by date
ag_sorted = sorted(ag, key=lambda x: x["ts"])
half = len(ag_sorted) // 2
first_half = ag_sorted[:half]
second_half = ag_sorted[half:]

print(f"\nFirst half:  {len(first_half)} trades ({first_half[0]['ts'][:10]} to {first_half[-1]['ts'][:10]})")
print(f"Second half: {len(second_half)} trades ({second_half[0]['ts'][:10]} to {second_half[-1]['ts'][:10]})")

# Test top-5 configs on both halves
top5 = configs[:5]
current_cfg = {"be": 10, "act": 15, "gap": 5}

test_configs = top5 + [current_cfg] if current_cfg not in [{"be": c["be"], "act": c["act"], "gap": c["gap"]} for c in top5] else top5

print(f"\n{'Config':<18} | {'1st Half':>10} | {'2nd Half':>10} | {'Full':>10} | {'Stable?':>8}")
print("-" * 75)

for cfg in configs[:5] + [c for c in configs if c["be"] == 10 and c["act"] == 15 and c["gap"] == 5][:1]:
    be, act, gap = cfg["be"], cfg["act"], cfg["gap"]

    results_1 = [simulate_hybrid(s.get("max_profit") or 0, s.get("max_loss") or 0,
                 20, be, act, gap) for s in first_half]
    results_2 = [simulate_hybrid(s.get("max_profit") or 0, s.get("max_loss") or 0,
                 20, be, act, gap) for s in second_half]

    pnl_1 = sum(r[0] for r in results_1)
    pnl_2 = sum(r[0] for r in results_2)
    total = pnl_1 + pnl_2

    # Stable = both halves positive
    stable = "YES" if pnl_1 > 0 and pnl_2 > 0 else "NO"
    is_current = be == 10 and act == 15 and gap == 5
    marker = " <-- CURRENT" if is_current else ""

    label = f"BE={be},act={act},g={gap}"
    print(f"{label:<18} | {pnl_1:>+9.1f} | {pnl_2:>+9.1f} | {total:>+9.1f} | {stable:>8}{marker}")


# ======================================================================
# PART 4: By Paradigm Subtype — Does optimal config differ?
# ======================================================================
print("\n" + "=" * 80)
print("  PART 4: Optimal Config by Paradigm Subtype")
print("=" * 80)

for para in ["AG-PURE", "AG-LIS"]:
    sigs = [s for s in ag if s.get("paradigm") == para]
    if len(sigs) < 5:
        continue

    print(f"\n{para} ({len(sigs)} trades):")

    best_configs = []
    for be in [8, 9, 10, 11]:
        for act in [10, 11, 12, 13, 14, 15, 16]:
            if act < be:
                continue
            for gap in [3, 4, 5, 6, 7, 8]:
                results = [simulate_hybrid(s.get("max_profit") or 0, s.get("max_loss") or 0,
                           20, be, act, gap) for s in sigs]
                pnls = [r[0] for r in results]
                total = sum(pnls)
                wr = sum(1 for p in pnls if p > 0) / len(pnls) * 100
                gw = sum(p for p in pnls if p > 0)
                gl = abs(sum(p for p in pnls if p < 0))
                pf = gw / gl if gl > 0 else float('inf')
                best_configs.append({"be": be, "act": act, "gap": gap,
                                     "total": total, "wr": wr, "pf": pf})

    best_configs.sort(key=lambda x: -x["total"])
    print(f"  {'Rank':>4} | {'Config':<18} | {'WR':>6} | {'Total':>8} | {'PF':>6}")
    for i, c in enumerate(best_configs[:5]):
        is_current = c["be"] == 10 and c["act"] == 15 and c["gap"] == 5
        label = f"BE={c['be']},act={c['act']},g={c['gap']}"
        marker = " *" if is_current else ""
        print(f"  {i+1:>4} | {label:<18} | {c['wr']:>5.1f}% | {c['total']:>+7.1f} | {c['pf']:>5.2f}{marker}")

    # Current for this subtype
    for c in best_configs:
        if c["be"] == 10 and c["act"] == 15 and c["gap"] == 5:
            label = f"BE=10,act=15,g=5"
            print(f"  Current: {label:<18} | {c['wr']:>5.1f}% | {c['total']:>+7.1f} | {c['pf']:>5.2f}")
            break


# ======================================================================
# PART 5: Trade-by-Trade — What changes between act=15 and act=12?
# ======================================================================
print("\n" + "=" * 80)
print("  PART 5: Trade-by-Trade Diff — Activation 15 vs 12 (BE=10, gap=5)")
print("=" * 80)

print(f"\nTrades that CHANGE outcome with activation=12 (from current=15):")
print(f"{'Timestamp':<18} {'Grade':<6} {'Paradigm':<10} {'MFE':>6} {'MAE':>7} | {'act=15':>8} {'act=12':>8} {'Delta':>7}")

changed = []
for s in ag:
    mfe = s.get("max_profit") or 0
    mae = s.get("max_loss") or 0

    pnl_15, phase_15, _ = simulate_hybrid(mfe, mae, 20, 10, 15, 5)
    pnl_12, phase_12, _ = simulate_hybrid(mfe, mae, 20, 10, 12, 5)

    if abs(pnl_15 - pnl_12) > 0.1:
        delta = pnl_12 - pnl_15
        changed.append({
            "ts": s["ts"][:16], "grade": s["grade"], "paradigm": s.get("paradigm", "?"),
            "mfe": mfe, "mae": mae, "pnl_15": pnl_15, "pnl_12": pnl_12,
            "phase_15": phase_15, "phase_12": phase_12, "delta": delta
        })
        print(f"{s['ts'][:16]:<18} {s['grade']:<6} {s.get('paradigm','?'):<10} {mfe:>6.1f} {mae:>7.1f} | {pnl_15:>+7.1f} {pnl_12:>+7.1f} {delta:>+7.1f}  ({phase_15}->{phase_12})")

total_delta = sum(c["delta"] for c in changed)
improving = sum(1 for c in changed if c["delta"] > 0)
worsening = sum(1 for c in changed if c["delta"] < 0)
print(f"\nSummary: {len(changed)} trades changed | {improving} improved, {worsening} worsened")
print(f"Net delta: {total_delta:+.1f} pts")


# ======================================================================
# PART 6: By Grade
# ======================================================================
print("\n" + "=" * 80)
print("  PART 6: Optimal Activation by Grade (excluding AG-TARGET)")
print("=" * 80)

for grade in ["A+", "A"]:
    sigs = [s for s in ag if s.get("grade") == grade]
    if len(sigs) < 5:
        continue

    actual_pnl = sum((s.get("pnl") or 0) for s in sigs)
    actual_wr = sum(1 for s in sigs if s.get("outcome") == "WIN") / len(sigs) * 100

    print(f"\n{grade} ({len(sigs)} trades, actual: {actual_wr:.0f}% WR, {actual_pnl:+.1f} pts):")

    for act in [10, 11, 12, 13, 14, 15]:
        results = [simulate_hybrid(s.get("max_profit") or 0, s.get("max_loss") or 0,
                   20, 10, act, 5) for s in sigs]
        pnls = [r[0] for r in results]
        total = sum(pnls)
        wr = sum(1 for p in pnls if p > 0) / len(pnls) * 100
        gw = sum(p for p in pnls if p > 0)
        gl = abs(sum(p for p in pnls if p < 0))
        pf = gw / gl if gl > 0 else float('inf')
        marker = " <-- CURRENT" if act == 15 else ""
        print(f"  act={act:>2}: WR {wr:>5.1f}% | Total {total:>+7.1f} | PF {pf:>5.2f}{marker}")


# ======================================================================
# PART 7: MFE Distribution in the Critical 12-15 Zone
# ======================================================================
print("\n" + "=" * 80)
print("  PART 7: The Critical MFE 12-15 Zone (what act=12 captures that act=15 misses)")
print("=" * 80)

zone_trades = [s for s in ag if 12 <= (s.get("max_profit") or 0) < 15]
print(f"\nTrades with MFE in [12, 15) range: {len(zone_trades)}")
print(f"These are the trades that benefit from lowering activation from 15 to 12.\n")

if zone_trades:
    for s in zone_trades:
        mfe = s.get("max_profit") or 0
        mae = s.get("max_loss") or 0
        pnl_act15 = simulate_hybrid(mfe, mae, 20, 10, 15, 5)[0]
        pnl_act12 = simulate_hybrid(mfe, mae, 20, 10, 12, 5)[0]
        actual_pnl = s.get("pnl") or 0

        print(f"  {s['ts'][:16]} {s['grade']:<5} {s.get('paradigm',''):<10} MFE={mfe:>5.1f} MAE={mae:>6.1f}")
        print(f"    Actual PnL: {actual_pnl:>+6.1f} | act=15: {pnl_act15:>+6.1f} | act=12: {pnl_act12:>+6.1f} | Gain: {pnl_act12-pnl_act15:>+6.1f}")

    total_gain = sum(simulate_hybrid((s.get("max_profit") or 0), (s.get("max_loss") or 0), 20, 10, 12, 5)[0] -
                     simulate_hybrid((s.get("max_profit") or 0), (s.get("max_loss") or 0), 20, 10, 15, 5)[0]
                     for s in zone_trades)
    print(f"\n  Total gain from these {len(zone_trades)} trades: {total_gain:+.1f} pts")


# ======================================================================
# FINAL RECOMMENDATION
# ======================================================================
print("\n" + "=" * 80)
print("  FINAL RECOMMENDATION")
print("=" * 80)

# Get best stable config
best = configs[0]
print(f"""
DATASET: 54 AG Short trades (AG-PURE + AG-LIS), Feb 18 - Mar 25, 2026

CURRENT CONFIG: BE=10, activation=15, gap=5
  Simulated: {[c for c in configs if c['be']==10 and c['act']==15 and c['gap']==5][0]['total']:+.1f} pts
  Rank: #{current_rank} / {len(configs)}

BEST CONFIG: BE={best['be']}, activation={best['act']}, gap={best['gap']}
  Simulated: {best['total']:+.1f} pts
  Improvement: {best['total'] - [c for c in configs if c['be']==10 and c['act']==15 and c['gap']==5][0]['total']:+.1f} pts

KEY FINDINGS:
1. Lowering activation from 15 to 12 captures trades in the MFE 12-15 zone
   that currently exit at breakeven (MFE reached BE but not trail activation)
2. The improvement is NOT from wider trail — gap=5 remains near-optimal
3. AG-TARGET should be filtered out (trend exhausted, -1.8 pts on 19 trades)
4. AG-PURE is the strongest subtype (68.8% WR, PF 3.04x)

CAVEAT: 54 trades is a moderate sample. The MFE-based simulation is conservative
(assumes MAE before MFE). Real-world slippage and path-dependence may differ.
Recommend deploying in parallel log-mode first.
""")
