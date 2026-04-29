"""
Backtest: Paradigm Trend Rider (PTR) vs Current Setups
Uses cached setup_log data with MFE/MAE to simulate PTR risk management.

Approach:
- Current setups: SL=8 (AG/GEX) or 20 (SC/DD), T=10pts, 100% exit at target
- PTR simulation: SL=15, T1=10 (50% exit), T2=trail with 10pt gap on remaining 50%

Path-dependence handling:
- If MAE > SL_PTR (before or after target): stop hit, full loss
- Conservative assumption: MAE happened FIRST, then MFE (worst case for PTR)
- This UNDERESTIMATES PTR performance (in reality, some MAE happens after T1 fills)
"""
import json
from datetime import datetime, time as dtime
from collections import defaultdict

# -- Load data --
with open("tmp_setups_full.json") as f:
    full_data = json.load(f)

print("=" * 80)
print("  PARADIGM TREND RIDER (PTR) BACKTEST")
print("  Data: 1,080 setup signals | Feb 18 - Mar 25, 2026")
print("=" * 80)

# -- PTR Parameters --
PTR_STOP = 15.0       # wider stop than current 8pt
PTR_T1 = 10.0         # first target (50% exit)
PTR_TRAIL_GAP = 10.0  # trailing gap after T1
PTR_T1_PCT = 0.50     # % position closed at T1

# Current setup parameters
CURRENT_STOP = {"AG Short": 8, "GEX Long": 8, "Skew Charm": 20, "DD Exhaustion": 20,
                "ES Absorption": 8, "BofA Scalp": 8, "Paradigm Reversal": 8,
                "SB Absorption": 8, "SB2 Absorption": 8, "SB10 Absorption": 8}
CURRENT_TARGET = 10.0

# PTR qualifying paradigms
PTR_GOOD_PARADIGMS = {"AG-PURE", "AG-LIS", "GEX-PURE", "GEX-LIS"}
PTR_TREND_SETUPS = {"AG Short", "GEX Long"}  # Only these are trend candidates


def parse_time_et(ts_str):
    """Parse UTC timestamp and return ET hour (rough: UTC-5 for EST)."""
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        # Rough EST offset (good enough for hour-level filtering)
        et_hour = (dt.hour - 5) % 24
        et_min = dt.minute
        return et_hour, et_min
    except:
        return None, None


def simulate_ptr(mfe, mae, direction="short"):
    """
    Simulate PTR outcome using MFE (max favorable excursion) and MAE (max adverse excursion).

    Conservative path assumption: MAE happens BEFORE MFE.
    This means if MAE > PTR_STOP, the trade is a loss even if MFE was huge.
    This UNDERESTIMATES PTR performance.

    Returns: (pnl, result_str, details)
    """
    if mfe is None:
        mfe = 0
    if mae is None:
        mae = 0

    # MAE is stored as negative in the data
    actual_mae = abs(mae) if mae < 0 else mae

    # Step 1: Did stop get hit? (conservative: assume MAE happened first)
    if actual_mae >= PTR_STOP:
        return -PTR_STOP, "STOP", f"MAE={mae:.1f}, stopped at -{PTR_STOP}"

    # Step 2: Did T1 get hit?
    if mfe < PTR_T1:
        # Didn't reach T1. Use actual MFE - trail or actual PnL
        # In this case, trade either timed out or partially worked
        # Conservative: assume it ended near 0 or small loss
        # Use actual pnl as proxy since path was similar
        return 0, "TIMEOUT", f"MFE={mfe:.1f}, didn't reach T1"

    # Step 3: T1 hit! Calculate blended P&L
    t1_pnl = PTR_T1 * PTR_T1_PCT  # +10 * 0.5 = +5 pts

    # Step 4: T2 trailing stop capture
    # After T1, stop moves to BE (entry price)
    # Trail gap = 10pt from high water mark
    # T2 capture = MFE - trail_gap (what's left after trail triggers)
    # But T2 can't be less than 0 (stop at BE after T1)
    if mfe >= PTR_T1 + PTR_TRAIL_GAP:
        # Trail would have activated
        t2_capture = max(0, mfe - PTR_TRAIL_GAP)
    else:
        # Trail never activated (MFE between 10-20)
        # T2 exits at BE or wherever it reverses
        # Conservative: T2 exits at midpoint between 10 and MFE
        t2_capture = max(0, (mfe + PTR_T1) / 2 - PTR_T1) + PTR_T1
        # Actually simpler: if MFE was 15, trail hasn't activated,
        # price reverses back to entry -> T2 at BE = 0
        # If MFE was 18, trail not active, price might give back to ~10 -> T2 ~= MFE/2
        t2_capture = max(0, mfe * 0.5) if mfe < PTR_T1 + PTR_TRAIL_GAP else 0

    # Recalculate T2 more simply
    if mfe >= 20:  # trail activates at MFE=20 (T1=10 + gap=10)
        t2_capture = mfe - PTR_TRAIL_GAP  # trail locks in MFE - gap
    else:
        t2_capture = 0  # trail never activated, T2 exits at BE

    t2_pnl = t2_capture * (1 - PTR_T1_PCT)  # remaining 50%

    total_pnl = t1_pnl + t2_pnl
    result = "WIN" if total_pnl > 0 else "LOSS"

    return round(total_pnl, 1), result, f"T1=+{PTR_T1}, T2_capture={t2_capture:.1f}, MFE={mfe:.1f}"


def simulate_current(mfe, mae, stop, target=10.0):
    """Simulate current setup outcome (fixed stop/target, 100% exit)."""
    if mae is None:
        mae = 0
    if mfe is None:
        mfe = 0
    actual_mae = abs(mae) if mae < 0 else mae

    # Conservative: MAE first
    if actual_mae >= stop:
        return -stop, "STOP"
    if mfe >= target:
        return target, "WIN"
    return 0, "TIMEOUT"


# ══════════════════════════════════════════════════════════════════════════
# ANALYSIS 1: All AG Short + GEX Long -- PTR vs Current
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "-" * 80)
print("ANALYSIS 1: AG Short + GEX Long -- PTR vs Current (ALL signals)")
print("-" * 80)

trend_signals = [d for d in full_data if d["setup_name"] in PTR_TREND_SETUPS]
print(f"Total trend signals: {len(trend_signals)}")
print(f"  AG Short: {sum(1 for d in trend_signals if d['setup_name'] == 'AG Short')}")
print(f"  GEX Long: {sum(1 for d in trend_signals if d['setup_name'] == 'GEX Long')}")

# Run both simulations
current_results = []
ptr_results = []

for sig in trend_signals:
    mfe = sig.get("max_profit") or 0
    mae = sig.get("max_loss") or 0
    stop = CURRENT_STOP.get(sig["setup_name"], 8)

    c_pnl, c_result = simulate_current(mfe, mae, stop)
    p_pnl, p_result, p_detail = simulate_ptr(mfe, mae, sig["direction"])

    current_results.append({"pnl": c_pnl, "result": c_result, "sig": sig})
    ptr_results.append({"pnl": p_pnl, "result": p_result, "detail": p_detail, "sig": sig})

# Summary
c_wins = sum(1 for r in current_results if r["result"] == "WIN")
c_losses = sum(1 for r in current_results if r["result"] == "STOP")
c_timeouts = sum(1 for r in current_results if r["result"] == "TIMEOUT")
c_total_pnl = sum(r["pnl"] for r in current_results)
c_wr = c_wins / len(current_results) * 100 if current_results else 0

p_wins = sum(1 for r in ptr_results if r["result"] == "WIN")
p_losses = sum(1 for r in ptr_results if r["result"] == "STOP")
p_timeouts = sum(1 for r in ptr_results if r["result"] == "TIMEOUT")
p_total_pnl = sum(r["pnl"] for r in ptr_results)
p_wr = p_wins / len(ptr_results) * 100 if ptr_results else 0

print(f"\n{'Metric':<25} {'Current (SL=8,T=10)':<25} {'PTR (SL=15,T1=10,Trail)':<25}")
print(f"{'-'*25} {'-'*25} {'-'*25}")
print(f"{'Trades':<25} {len(current_results):<25} {len(ptr_results):<25}")
print(f"{'Wins':<25} {c_wins:<25} {p_wins:<25}")
print(f"{'Losses':<25} {c_losses:<25} {p_losses:<25}")
print(f"{'Timeouts':<25} {c_timeouts:<25} {p_timeouts:<25}")
print(f"{'Win Rate':<25} {f'{c_wr:.1f}%':<25} {f'{p_wr:.1f}%':<25}")
print(f"{'Total P&L (pts)':<25} {f'{c_total_pnl:+.1f}':<25} {f'{p_total_pnl:+.1f}':<25}")
print(f"{'Avg P&L/trade':<25} {f'{c_total_pnl/len(current_results):+.1f}':<25} {f'{p_total_pnl/len(ptr_results):+.1f}':<25}")

# Profit factor
c_gross_win = sum(r["pnl"] for r in current_results if r["pnl"] > 0)
c_gross_loss = abs(sum(r["pnl"] for r in current_results if r["pnl"] < 0))
p_gross_win = sum(r["pnl"] for r in ptr_results if r["pnl"] > 0)
p_gross_loss = abs(sum(r["pnl"] for r in ptr_results if r["pnl"] < 0))
c_pf = c_gross_win / c_gross_loss if c_gross_loss > 0 else float('inf')
p_pf = p_gross_win / p_gross_loss if p_gross_loss > 0 else float('inf')
print(f"{'Profit Factor':<25} {f'{c_pf:.2f}x':<25} {f'{p_pf:.2f}x':<25}")


# ══════════════════════════════════════════════════════════════════════════
# ANALYSIS 2: PTR-Qualifying Signals Only (quality paradigm + high MFE)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "-" * 80)
print("ANALYSIS 2: PTR-Qualifying Signals (good paradigm + grade A/A+)")
print("-" * 80)

ptr_qualifying = []
for sig in trend_signals:
    # PTR filters
    paradigm = sig.get("paradigm", "")
    grade = sig.get("grade", "")
    et_h, et_m = parse_time_et(sig.get("ts", ""))

    # Quality paradigm
    if paradigm not in PTR_GOOD_PARADIGMS:
        continue
    # Good grade
    if grade not in ("A+", "A"):
        continue
    # Time window: 10:00-13:00 ET (paradigm confirmed, max edge)
    if et_h is not None and (et_h < 10 or et_h > 13):
        continue

    ptr_qualifying.append(sig)

print(f"Qualifying signals: {len(ptr_qualifying)} / {len(trend_signals)}")
print(f"  By paradigm: {defaultdict(int, {s.get('paradigm',''): 1 for s in ptr_qualifying})}")

if ptr_qualifying:
    # Group by paradigm for analysis
    by_paradigm = defaultdict(list)
    for s in ptr_qualifying:
        by_paradigm[s["paradigm"]].append(s)

    for para, sigs in sorted(by_paradigm.items()):
        print(f"\n  {para}: {len(sigs)} signals")

    # Simulate PTR on qualifying
    q_current = []
    q_ptr = []
    for sig in ptr_qualifying:
        mfe = sig.get("max_profit") or 0
        mae = sig.get("max_loss") or 0
        stop = CURRENT_STOP.get(sig["setup_name"], 8)

        c_pnl, c_result = simulate_current(mfe, mae, stop)
        p_pnl, p_result, p_detail = simulate_ptr(mfe, mae, sig["direction"])

        q_current.append({"pnl": c_pnl, "result": c_result, "sig": sig})
        q_ptr.append({"pnl": p_pnl, "result": p_result, "detail": p_detail, "sig": sig})

    qc_wins = sum(1 for r in q_current if r["result"] == "WIN")
    qc_losses = sum(1 for r in q_current if r["result"] == "STOP")
    qc_total = sum(r["pnl"] for r in q_current)
    qc_wr = qc_wins / len(q_current) * 100 if q_current else 0

    qp_wins = sum(1 for r in q_ptr if r["result"] == "WIN")
    qp_losses = sum(1 for r in q_ptr if r["result"] == "STOP")
    qp_total = sum(r["pnl"] for r in q_ptr)
    qp_wr = qp_wins / len(q_ptr) * 100 if q_ptr else 0

    print(f"\n{'Metric':<25} {'Current':<25} {'PTR':<25}")
    print(f"{'-'*25} {'-'*25} {'-'*25}")
    print(f"{'Trades':<25} {len(q_current):<25} {len(q_ptr):<25}")
    print(f"{'Win Rate':<25} {f'{qc_wr:.1f}%':<25} {f'{qp_wr:.1f}%':<25}")
    print(f"{'Total P&L':<25} {f'{qc_total:+.1f}':<25} {f'{qp_total:+.1f}':<25}")
    print(f"{'Avg P&L/trade':<25} {f'{qc_total/len(q_current):+.1f}':<25} {f'{qp_total/len(q_ptr):+.1f}':<25}")


# ══════════════════════════════════════════════════════════════════════════
# ANALYSIS 3: "Saved by Wider Stop" -- Trades where SL=8 failed but SL=15 would survive
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "-" * 80)
print("ANALYSIS 3: 'Saved by Wider Stop' -- Current lost at -8, PTR survives")
print("-" * 80)

saved = []
for i, sig in enumerate(trend_signals):
    mfe = sig.get("max_profit") or 0
    mae = sig.get("max_loss") or 0
    actual_mae = abs(mae) if mae < 0 else mae
    stop = CURRENT_STOP.get(sig["setup_name"], 8)

    # Current stop hit, but PTR stop would NOT be hit
    if actual_mae >= stop and actual_mae < PTR_STOP:
        # And MFE shows the trade would have eventually been profitable
        if mfe >= PTR_T1:
            p_pnl, p_result, p_detail = simulate_ptr(mfe, mae, sig["direction"])
            saved.append({
                "ts": sig["ts"][:16],
                "setup": sig["setup_name"],
                "grade": sig["grade"],
                "paradigm": sig["paradigm"],
                "mfe": mfe,
                "mae": mae,
                "current_pnl": -stop,
                "ptr_pnl": p_pnl,
                "detail": p_detail
            })

print(f"\nTrades saved: {len(saved)} / {len(trend_signals)}")
if saved:
    total_recovered = sum(s["ptr_pnl"] - s["current_pnl"] for s in saved)
    print(f"Total P&L recovered: {total_recovered:+.1f} pts")
    print(f"\n{'Timestamp':<18} {'Setup':<12} {'Grade':<6} {'Paradigm':<14} {'MFE':>6} {'MAE':>7} {'Current':>9} {'PTR':>7} {'Delta':>7}")
    for s in saved[:15]:
        delta = s["ptr_pnl"] - s["current_pnl"]
        print(f"{s['ts']:<18} {s['setup']:<12} {s['grade']:<6} {s['paradigm']:<14} {s['mfe']:>6.1f} {s['mae']:>7.1f} {s['current_pnl']:>+9.1f} {s['ptr_pnl']:>+7.1f} {delta:>+7.1f}")


# ══════════════════════════════════════════════════════════════════════════
# ANALYSIS 4: "Money Left on Table" -- Won at +10 but MFE was much higher
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "-" * 80)
print("ANALYSIS 4: 'Money Left on Table' -- Won +10 but MFE >> 10")
print("-" * 80)

left_on_table = []
for sig in trend_signals:
    mfe = sig.get("max_profit") or 0
    mae = sig.get("max_loss") or 0
    actual_mae = abs(mae) if mae < 0 else mae
    stop = CURRENT_STOP.get(sig["setup_name"], 8)

    # Current setup won (MFE >= target, MAE < stop)
    if mfe >= CURRENT_TARGET and actual_mae < stop:
        extra = mfe - CURRENT_TARGET
        if extra >= 10:  # At least 10 extra pts available
            p_pnl, p_result, p_detail = simulate_ptr(mfe, mae, sig["direction"])
            left_on_table.append({
                "ts": sig["ts"][:16],
                "setup": sig["setup_name"],
                "grade": sig["grade"],
                "paradigm": sig["paradigm"],
                "mfe": mfe,
                "current_pnl": CURRENT_TARGET,
                "ptr_pnl": p_pnl,
                "extra": extra,
            })

left_on_table.sort(key=lambda x: -x["extra"])
total_extra = sum(s["ptr_pnl"] - s["current_pnl"] for s in left_on_table)
print(f"\nTrades with 10+ pts left on table: {len(left_on_table)}")
print(f"Total additional P&L captured by PTR: {total_extra:+.1f} pts")

if left_on_table:
    print(f"\n{'Timestamp':<18} {'Setup':<12} {'Grade':<6} {'Paradigm':<14} {'MFE':>6} {'Current':>9} {'PTR':>7} {'Extra':>7}")
    for s in left_on_table[:20]:
        extra = s["ptr_pnl"] - s["current_pnl"]
        print(f"{s['ts']:<18} {s['setup']:<12} {s['grade']:<6} {s['paradigm']:<14} {s['mfe']:>6.1f} {s['current_pnl']:>+9.1f} {s['ptr_pnl']:>+7.1f} {extra:>+7.1f}")


# ══════════════════════════════════════════════════════════════════════════
# ANALYSIS 5: MFE Distribution -- How far do winning trades actually run?
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "-" * 80)
print("ANALYSIS 5: MFE Distribution (How far do trend trades run?)")
print("-" * 80)

for setup_name in ["AG Short", "GEX Long"]:
    sigs = [d for d in full_data if d["setup_name"] == setup_name]
    mfes = [s.get("max_profit", 0) or 0 for s in sigs]

    if not mfes:
        continue

    print(f"\n{setup_name} ({len(sigs)} signals):")
    buckets = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 30), (30, 50), (50, 999)]
    for lo, hi in buckets:
        count = sum(1 for m in mfes if lo <= m < hi)
        pct = count / len(mfes) * 100
        bar = "#" * int(pct / 2)
        label = f"{lo}-{hi}" if hi < 999 else f"{lo}+"
        print(f"  MFE {label:>6} pts: {count:>3} ({pct:>5.1f}%) {bar}")

    avg_mfe = sum(mfes) / len(mfes)
    med_mfe = sorted(mfes)[len(mfes) // 2]
    print(f"  Average MFE: {avg_mfe:.1f} pts | Median MFE: {med_mfe:.1f} pts")

    # By grade
    print(f"\n  By Grade:")
    grades = defaultdict(list)
    for s in sigs:
        grades[s["grade"]].append(s.get("max_profit", 0) or 0)
    for g in ["A+", "A", "B", "C", "LOG"]:
        if g in grades:
            gm = grades[g]
            avg = sum(gm) / len(gm)
            pct_over20 = sum(1 for m in gm if m >= 20) / len(gm) * 100
            print(f"    {g:<5}: n={len(gm):<4} avg_MFE={avg:>5.1f} | MFE>=20: {pct_over20:.0f}%")

    # By paradigm
    print(f"\n  By Paradigm:")
    paradigms = defaultdict(list)
    for s in sigs:
        paradigms[s.get("paradigm", "?")].append(s.get("max_profit", 0) or 0)
    for p, pm in sorted(paradigms.items(), key=lambda x: -sum(x[1])/len(x[1])):
        if len(pm) >= 2:
            avg = sum(pm) / len(pm)
            pct_over20 = sum(1 for m in pm if m >= 20) / len(pm) * 100
            print(f"    {p:<16}: n={len(pm):<4} avg_MFE={avg:>5.1f} | MFE>=20: {pct_over20:.0f}%")


# ══════════════════════════════════════════════════════════════════════════
# ANALYSIS 6: PTR applied to ALL setups (including SC, DD, etc.)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "-" * 80)
print("ANALYSIS 6: PTR RM (SL=15, T1=10, Trail) applied to ALL setup types")
print("-" * 80)

for setup_name in ["AG Short", "GEX Long", "Skew Charm", "ES Absorption", "DD Exhaustion", "BofA Scalp", "Paradigm Reversal"]:
    sigs = [d for d in full_data if d["setup_name"] == setup_name]
    if len(sigs) < 5:
        continue

    stop = CURRENT_STOP.get(setup_name, 8)

    c_pnls = []
    p_pnls = []
    for sig in sigs:
        mfe = sig.get("max_profit") or 0
        mae = sig.get("max_loss") or 0

        c_pnl, _ = simulate_current(mfe, mae, stop)
        p_pnl, _, _ = simulate_ptr(mfe, mae, sig["direction"])
        c_pnls.append(c_pnl)
        p_pnls.append(p_pnl)

    c_total = sum(c_pnls)
    p_total = sum(p_pnls)
    c_wr = sum(1 for p in c_pnls if p > 0) / len(c_pnls) * 100
    p_wr = sum(1 for p in p_pnls if p > 0) / len(p_pnls) * 100
    c_avg = c_total / len(c_pnls)
    p_avg = p_total / len(p_pnls)
    delta = p_total - c_total

    print(f"\n{setup_name} ({len(sigs)} trades):")
    print(f"  Current: WR {c_wr:.0f}% | Total {c_total:+.1f} pts | Avg {c_avg:+.1f}")
    print(f"  PTR RM:  WR {p_wr:.0f}% | Total {p_total:+.1f} pts | Avg {p_avg:+.1f}")
    print(f"  Delta:   {delta:+.1f} pts ({'BETTER' if delta > 0 else 'WORSE'})")


# ══════════════════════════════════════════════════════════════════════════
# ANALYSIS 7: Optimal Stop Loss Analysis
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "-" * 80)
print("ANALYSIS 7: Optimal Stop Size for AG Short + GEX Long")
print("-" * 80)

for stop_test in [6, 8, 10, 12, 15, 18, 20]:
    pnls = []
    for sig in trend_signals:
        mfe = sig.get("max_profit") or 0
        mae = sig.get("max_loss") or 0
        actual_mae = abs(mae) if mae < 0 else mae

        if actual_mae >= stop_test:
            pnls.append(-stop_test)
        elif mfe >= CURRENT_TARGET:
            pnls.append(CURRENT_TARGET)
        else:
            pnls.append(0)

    total = sum(pnls)
    wr = sum(1 for p in pnls if p > 0) / len(pnls) * 100
    losses = sum(1 for p in pnls if p < 0)
    avg = total / len(pnls) if pnls else 0
    pf_win = sum(p for p in pnls if p > 0)
    pf_loss = abs(sum(p for p in pnls if p < 0))
    pf = pf_win / pf_loss if pf_loss > 0 else float('inf')
    print(f"  SL={stop_test:>2}: WR {wr:>5.1f}% | Losses {losses:>3} | Total {total:>+7.1f} | Avg {avg:>+5.1f} | PF {pf:.2f}x")


# ══════════════════════════════════════════════════════════════════════════
# ANALYSIS 8: Skew Charm with PTR RM (most traded setup -- 240 signals)
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "-" * 80)
print("ANALYSIS 8: Skew Charm -- PTR RM by Grade (240 signals)")
print("-" * 80)

sc_sigs = [d for d in full_data if d["setup_name"] == "Skew Charm"]
for grade in ["A+", "A", "B", "C", "LOG"]:
    g_sigs = [s for s in sc_sigs if s["grade"] == grade]
    if len(g_sigs) < 3:
        continue

    c_pnls = []
    p_pnls = []
    for sig in g_sigs:
        mfe = sig.get("max_profit") or 0
        mae = sig.get("max_loss") or 0
        c_pnl, _ = simulate_current(mfe, mae, 20)  # SC uses SL=20
        p_pnl, _, _ = simulate_ptr(mfe, mae, sig["direction"])
        c_pnls.append(c_pnl)
        p_pnls.append(p_pnl)

    c_total = sum(c_pnls)
    p_total = sum(p_pnls)
    c_wr = sum(1 for p in c_pnls if p > 0) / len(c_pnls) * 100
    p_wr = sum(1 for p in p_pnls if p > 0) / len(p_pnls) * 100
    delta = p_total - c_total

    avg_mfe = sum((s.get("max_profit") or 0) for s in g_sigs) / len(g_sigs)

    print(f"  {grade:<5} (n={len(g_sigs):>3}): Current WR {c_wr:>5.1f}% PnL {c_total:>+7.1f} | PTR WR {p_wr:>5.1f}% PnL {p_total:>+7.1f} | Delta {delta:>+7.1f} | Avg MFE {avg_mfe:.1f}")


# ══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("  BACKTEST SUMMARY")
print("=" * 80)

print("""
Key Findings:
""")

# Calculate key stats for summary
ag_sigs = [d for d in full_data if d["setup_name"] == "AG Short"]
gex_sigs = [d for d in full_data if d["setup_name"] == "GEX Long"]
ag_mfes = [(s.get("max_profit") or 0) for s in ag_sigs]
gex_mfes = [(s.get("max_profit") or 0) for s in gex_sigs]

ag_over20 = sum(1 for m in ag_mfes if m >= 20) / len(ag_mfes) * 100 if ag_mfes else 0
gex_over20 = sum(1 for m in gex_mfes if m >= 20) / len(gex_mfes) * 100 if gex_mfes else 0

print(f"1. AG Short: {ag_over20:.0f}% of trades had MFE >= 20 pts (avg MFE: {sum(ag_mfes)/len(ag_mfes):.1f})")
print(f"   -> Current exits at +10, missing {sum(ag_mfes)/len(ag_mfes) - 10:.1f} pts average additional upside")
if gex_mfes:
    print(f"2. GEX Long: {gex_over20:.0f}% of trades had MFE >= 20 pts (avg MFE: {sum(gex_mfes)/len(gex_mfes):.1f})")
    print(f"   -> Current exits at +10, missing {sum(gex_mfes)/len(gex_mfes) - 10:.1f} pts average additional upside")

print(f"3. 'Saved by wider stop': {len(saved)} trades where SL=8 lost but SL=15 + MFE>=10 would have won")
print(f"   -> Recovery: {sum(s['ptr_pnl'] - s['current_pnl'] for s in saved):+.1f} pts")
print(f"4. 'Money left on table': {len(left_on_table)} trades where MFE exceeded target by 10+ pts")
print(f"   -> Additional capture: {total_extra:+.1f} pts")

print(f"""
Methodology Note:
- Conservative path assumption: MAE assumed to happen BEFORE MFE (worst case for PTR)
- This UNDERESTIMATES PTR performance because in reality, some MAE occurs after T1 fills
- T2 trail estimation: MFE - 10pt gap = trailing capture. Assumes perfect trail execution.
- Data: {len(full_data)} signals across Feb 18 - Mar 25, 2026 (25 trading days)
""")
