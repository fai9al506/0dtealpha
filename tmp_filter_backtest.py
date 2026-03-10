"""
Filter backtest on 476 trades from setup_log.
Tests individual filters and combinations to find optimal deployment strategy.
"""
import json
import re
from collections import defaultdict

with open("C:/Users/Faisa/AppData/Local/Temp/trade_data.json") as f:
    trades = json.load(f)

def stats(subset):
    n = len(subset)
    if n == 0:
        return {"n": 0, "w": 0, "l": 0, "e": 0, "wr": 0, "pnl": 0, "avg": 0, "pf": 0}
    w = sum(1 for t in subset if t["result"] == "WIN")
    l = sum(1 for t in subset if t["result"] == "LOSS")
    e = n - w - l
    pnl = sum(t["pnl"] for t in subset)
    wr = w / (w + l) * 100 if (w + l) > 0 else 0
    avg = pnl / n
    gross_w = sum(t["pnl"] for t in subset if t["pnl"] > 0)
    gross_l = abs(sum(t["pnl"] for t in subset if t["pnl"] < 0))
    pf = gross_w / gross_l if gross_l > 0 else float("inf")
    return {"n": n, "w": w, "l": l, "e": e, "wr": wr, "pnl": pnl, "avg": avg, "pf": pf}

def max_dd(subset):
    if not subset:
        return 0
    cum = 0
    peak = 0
    dd = 0
    for t in sorted(subset, key=lambda x: x["ts"]):
        cum += t["pnl"]
        if cum > peak:
            peak = cum
        if peak - cum > dd:
            dd = peak - cum
    return dd

def daily_stats(subset):
    """Compute per-day PnL for winning days %"""
    by_date = defaultdict(float)
    for t in subset:
        by_date[t["trade_date"]] += t["pnl"]
    if not by_date:
        return 0, 0
    winning = sum(1 for v in by_date.values() if v > 0)
    return winning, len(by_date)

def fmt(name, s, dd=None):
    dd_str = f"  DD={dd:.1f}" if dd is not None else ""
    return f"  {name:<40} N={s['n']:>3}  W={s['w']:>3}  L={s['l']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+8.1f}  Avg={s['avg']:>+6.1f}  PF={s['pf']:.2f}{dd_str}"


# ============================================================
print("=" * 100)
print("BASELINE — ALL 476 TRADES (no filters)")
print("=" * 100)

s = stats(trades)
dd = max_dd(trades)
wd, td = daily_stats(trades)
print(fmt("ALL", s, dd))
print(f"  Winning days: {wd}/{td} ({wd/td*100:.0f}%)")
print()

# Per-setup breakdown
print("  Per-setup baseline:")
for setup in sorted(set(t["setup"] for t in trades)):
    sub = [t for t in trades if t["setup"] == setup]
    s = stats(sub)
    print(fmt(f"    {setup}", s))


# ============================================================
print("\n" + "=" * 100)
print("INDIVIDUAL FILTER TESTS")
print("=" * 100)


# --- FILTER 1: Greek alignment >= +1 (universal) ---
print("\n--- FILTER 1: Greek alignment >= +1 (universal, aggressive) ---")
f1_pass = [t for t in trades if t.get("greek_alignment") is not None and t["greek_alignment"] >= 1]
f1_block = [t for t in trades if t.get("greek_alignment") is not None and t["greek_alignment"] < 1]
f1_na = [t for t in trades if t.get("greek_alignment") is None]
print(fmt("PASSED (align >= +1)", stats(f1_pass), max_dd(f1_pass)))
print(fmt("BLOCKED (align < +1)", stats(f1_block)))
print(fmt("NO DATA", stats(f1_na)))
# Per-setup
for setup in sorted(set(t["setup"] for t in trades)):
    sub_pass = [t for t in f1_pass if t["setup"] == setup]
    sub_block = [t for t in f1_block if t["setup"] == setup]
    if sub_pass or sub_block:
        sp = stats(sub_pass)
        sb = stats(sub_block)
        print(f"    {setup:<25} PASS: N={sp['n']:>3} WR={sp['wr']:>5.1f}% PnL={sp['pnl']:>+7.1f}  |  BLOCK: N={sb['n']:>3} WR={sb['wr']:>5.1f}% PnL={sb['pnl']:>+7.1f}")


# --- FILTER 2: GEX Long alignment >= +1 (setup-specific) ---
print("\n--- FILTER 2: GEX Long alignment >= +1 ---")
f2_gex = [t for t in trades if t["setup"] == "GEX Long"]
f2_pass_gex = [t for t in f2_gex if t.get("greek_alignment") is not None and t["greek_alignment"] >= 1]
f2_block_gex = [t for t in f2_gex if t.get("greek_alignment") is None or t["greek_alignment"] < 1]
print(fmt("GEX Long PASSED (align >= +1)", stats(f2_pass_gex)))
print(fmt("GEX Long BLOCKED (align < +1)", stats(f2_block_gex)))


# --- FILTER 3: AG Short alignment != -3 ---
print("\n--- FILTER 3: AG Short alignment != -3 ---")
f3_ag = [t for t in trades if t["setup"] == "AG Short"]
f3_pass = [t for t in f3_ag if t.get("greek_alignment") != -3]
f3_block = [t for t in f3_ag if t.get("greek_alignment") == -3]
print(fmt("AG Short PASSED (align != -3)", stats(f3_pass)))
print(fmt("AG Short BLOCKED (align == -3)", stats(f3_block)))


# --- FILTER 4: DD SVB weak-negative block ---
print("\n--- FILTER 4: DD Exhaustion SVB weak-negative [-0.5, 0) block ---")
dd_trades = [t for t in trades if t["setup"] == "DD Exhaustion"]
f4_pass = [t for t in dd_trades if not (t.get("svb") is not None and -0.5 <= t["svb"] < 0)]
f4_block = [t for t in dd_trades if t.get("svb") is not None and -0.5 <= t["svb"] < 0]
print(fmt("DD PASSED (SVB ok)", stats(f4_pass)))
print(fmt("DD BLOCKED (SVB weak-neg)", stats(f4_block)))


# --- FILTER 5: DD after 14:00 ET block ---
print("\n--- FILTER 5: DD Exhaustion after 14:00 ET block ---")
f5_pass = [t for t in dd_trades if not (t.get("hour_et") and t["hour_et"] >= 14)]
f5_block = [t for t in dd_trades if t.get("hour_et") and t["hour_et"] >= 14]
print(fmt("DD PASSED (before 14:00)", stats(f5_pass)))
print(fmt("DD BLOCKED (after 14:00)", stats(f5_block)))

# Deeper: DD by time bracket
print("  DD by time bracket:")
for start, end, label in [(9.5, 11, "09:30-11:00"), (11, 12, "11:00-12:00"), (12, 13, "12:00-13:00"), (13, 14, "13:00-14:00"), (14, 15, "14:00-15:00"), (15, 16, "15:00-16:00")]:
    sub = [t for t in dd_trades if t.get("hour_et") and start <= t["hour_et"] < end]
    if sub:
        s = stats(sub)
        print(f"    {label}: N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}  Avg={s['avg']:>+6.1f}")


# --- FILTER 6: DD BOFA-PURE paradigm block ---
print("\n--- FILTER 6: DD BOFA-PURE paradigm block ---")
def is_bofa_pure(t):
    p = (t.get("paradigm") or "").upper()
    return "BOFA" in p and "PURE" in p
f6_pass = [t for t in dd_trades if not is_bofa_pure(t)]
f6_block = [t for t in dd_trades if is_bofa_pure(t)]
print(fmt("DD PASSED (not BOFA-PURE)", stats(f6_pass)))
print(fmt("DD BLOCKED (BOFA-PURE)", stats(f6_block)))


# --- FILTER 7: Time-of-day all setups ---
print("\n--- FILTER 7: Block ALL setups after 14:00 ET ---")
f7_pass = [t for t in trades if not (t.get("hour_et") and t["hour_et"] >= 14)]
f7_block = [t for t in trades if t.get("hour_et") and t["hour_et"] >= 14]
print(fmt("PASSED (before 14:00)", stats(f7_pass), max_dd(f7_pass)))
print(fmt("BLOCKED (after 14:00)", stats(f7_block)))
# Per setup in afternoon
print("  Per-setup after 14:00:")
for setup in sorted(set(t["setup"] for t in f7_block)):
    sub = [t for t in f7_block if t["setup"] == setup]
    s = stats(sub)
    print(f"    {setup:<25} N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}")


# --- FILTER 8: ES Absorption analysis ---
print("\n--- FILTER 8: ES Absorption (CVD Divergence) deep dive ---")
abs_trades = [t for t in trades if t["setup"] in ("ES Absorption", "CVD Divergence")]
print(fmt("ES Absorption/CVD all", stats(abs_trades)))
# By alignment
for align in sorted(set(t.get("greek_alignment") for t in abs_trades if t.get("greek_alignment") is not None)):
    sub = [t for t in abs_trades if t.get("greek_alignment") == align]
    s = stats(sub)
    print(f"    align={align:+d}: N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}")
# By time
print("  ES Abs by time:")
for start, end, label in [(9.5, 11, "09:30-11"), (11, 13, "11-13"), (13, 14, "13-14"), (14, 16, "14-16")]:
    sub = [t for t in abs_trades if t.get("hour_et") and start <= t["hour_et"] < end]
    if sub:
        s = stats(sub)
        print(f"    {label}: N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}")


# --- FILTER 9: BofA Scalp analysis ---
print("\n--- FILTER 9: BofA Scalp deep dive ---")
bofa_trades = [t for t in trades if t["setup"] == "BofA Scalp"]
print(fmt("BofA all", stats(bofa_trades)))
for align in sorted(set(t.get("greek_alignment") for t in bofa_trades if t.get("greek_alignment") is not None)):
    sub = [t for t in bofa_trades if t.get("greek_alignment") == align]
    s = stats(sub)
    print(f"    align={align:+d}: N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}")


# --- FILTER 10: Paradigm Reversal analysis ---
print("\n--- FILTER 10: Paradigm Reversal deep dive ---")
para_trades = [t for t in trades if t["setup"] == "Paradigm Reversal"]
print(fmt("Paradigm Rev all", stats(para_trades)))
for align in sorted(set(t.get("greek_alignment") for t in para_trades if t.get("greek_alignment") is not None)):
    sub = [t for t in para_trades if t.get("greek_alignment") == align]
    s = stats(sub)
    print(f"    align={align:+d}: N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}")


# --- FILTER 11: Skew Charm analysis ---
print("\n--- FILTER 11: Skew Charm deep dive ---")
skew_trades = [t for t in trades if t["setup"] == "Skew Charm"]
print(fmt("Skew Charm all", stats(skew_trades)))
# By time
print("  Skew Charm by time:")
for start, end, label in [(9.5, 11, "09:30-11"), (11, 13, "11-13"), (13, 14, "13-14"), (14, 16, "14-16")]:
    sub = [t for t in skew_trades if t.get("hour_et") and start <= t["hour_et"] < end]
    if sub:
        s = stats(sub)
        print(f"    {label}: N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}  Avg={s['avg']:>+6.1f}")


# ============================================================
print("\n" + "=" * 100)
print("COMBINED FILTER SCENARIOS")
print("=" * 100)

# Scenario A: Currently deployed (F2+F3+F4+F5+F6)
def scenario_current(t):
    setup = t["setup"]
    ga = t.get("greek_alignment")
    svb = t.get("svb")
    hour = t.get("hour_et")
    paradigm = (t.get("paradigm") or "").upper()

    if setup == "GEX Long" and (ga is None or ga < 1):
        return False
    if setup == "AG Short" and ga == -3:
        return False
    if setup == "DD Exhaustion":
        if svb is not None and -0.5 <= svb < 0:
            return False
        if hour and hour >= 14:
            return False
        if "BOFA" in paradigm and "PURE" in paradigm:
            return False
    return True

# Scenario B: Current + block ES Absorption alignment < 0
def scenario_b(t):
    if not scenario_current(t):
        return False
    if t["setup"] == "ES Absorption" and t.get("greek_alignment") is not None and t["greek_alignment"] < 0:
        return False
    return True

# Scenario C: Current + block ALL setups after 14:00 (not just DD)
def scenario_c(t):
    if not scenario_current(t):
        return False
    if t.get("hour_et") and t["hour_et"] >= 14:
        return False
    return True

# Scenario D: Current + block ES Absorption entirely (it's -38.9 net)
def scenario_d(t):
    if not scenario_current(t):
        return False
    if t["setup"] == "ES Absorption":
        return False
    return True

# Scenario E: Current + universal alignment >= 0 gate
def scenario_e(t):
    if not scenario_current(t):
        return False
    ga = t.get("greek_alignment")
    if ga is not None and ga < 0:
        return False
    return True

# Scenario F: Current + block BofA Scalp (marginal PnL, lots of expired)
def scenario_f(t):
    if not scenario_current(t):
        return False
    if t["setup"] == "BofA Scalp":
        return False
    return True

# Scenario G: Keep only profitable setups (DD, Skew, AG, Paradigm) + GEX with filter
def scenario_g(t):
    if not scenario_current(t):
        return False
    if t["setup"] in ("ES Absorption", "BofA Scalp"):
        return False
    return True

# Scenario H: Scenario G + universal alignment >= 0
def scenario_h(t):
    if not scenario_g(t):
        return False
    ga = t.get("greek_alignment")
    if ga is not None and ga < 0:
        return False
    return True

# Scenario I: Block after 13:00 (even more aggressive time filter)
def scenario_i(t):
    if not scenario_current(t):
        return False
    if t.get("hour_et") and t["hour_et"] >= 13:
        # Exception: Skew Charm and Paradigm work all day
        if t["setup"] not in ("Skew Charm", "Paradigm Reversal"):
            return False
    return True

scenarios = [
    ("A: CURRENT DEPLOYED (F2+F3+F4+DD time/paradigm)", scenario_current),
    ("B: Current + ES Abs align < 0 block", scenario_b),
    ("C: Current + ALL setups block after 14:00", scenario_c),
    ("D: Current + DROP ES Absorption entirely", scenario_d),
    ("E: Current + universal alignment >= 0", scenario_e),
    ("F: Current + DROP BofA Scalp", scenario_f),
    ("G: Current + DROP ES Abs + BofA", scenario_g),
    ("H: G + universal alignment >= 0", scenario_h),
    ("I: Current + block non-Skew/Para after 13:00", scenario_i),
]

print(f"\n{'Scenario':<55} {'N':>4} {'W':>4} {'L':>4} {'WR':>6} {'PnL':>8} {'Avg':>6} {'PF':>5} {'DD':>6} {'W%D':>5}")
print("-" * 110)

# Baseline
s = stats(trades)
dd = max_dd(trades)
wd, td = daily_stats(trades)
print(f"{'BASELINE (no filter)':<55} {s['n']:>4} {s['w']:>4} {s['l']:>4} {s['wr']:>5.1f}% {s['pnl']:>+8.1f} {s['avg']:>+6.1f} {s['pf']:>5.2f} {dd:>6.1f} {wd/td*100:>4.0f}%")

for name, fn in scenarios:
    passed = [t for t in trades if fn(t)]
    s = stats(passed)
    dd = max_dd(passed)
    wd, td = daily_stats(passed)
    wd_pct = wd / td * 100 if td > 0 else 0
    print(f"{name:<55} {s['n']:>4} {s['w']:>4} {s['l']:>4} {s['wr']:>5.1f}% {s['pnl']:>+8.1f} {s['avg']:>+6.1f} {s['pf']:>5.2f} {dd:>6.1f} {wd_pct:>4.0f}%")


# ============================================================
print("\n" + "=" * 100)
print("SCENARIO DETAILS — Per-setup breakdown for top scenarios")
print("=" * 100)

for name, fn in [scenarios[0], scenarios[3], scenarios[6], scenarios[7]]:
    passed = [t for t in trades if fn(t)]
    print(f"\n--- {name} ---")
    for setup in sorted(set(t["setup"] for t in passed)):
        sub = [t for t in passed if t["setup"] == setup]
        s = stats(sub)
        print(f"  {setup:<25} N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}  PF={s['pf']:.2f}")


# ============================================================
print("\n" + "=" * 100)
print("WHAT EVAL vs SIM SHOULD RUN")
print("=" * 100)

# Eval real: conservative, capital preservation
# SIM: aggressive, data collection

print("\nEval real config enabled setups:")
eval_setups = ["AG Short", "DD Exhaustion", "Paradigm Reversal", "Skew Charm", "ES Absorption", "Vanna Pivot Bounce"]
for setup in eval_setups:
    sub = [t for t in trades if t["setup"] == setup and scenario_current(t)]
    s = stats(sub)
    print(f"  {setup:<25} N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}  PF={s['pf']:.2f}")

print("\nSIM config enabled setups (all):")
sim_setups = ["GEX Long", "AG Short", "BofA Scalp", "DD Exhaustion", "Paradigm Reversal", "Skew Charm", "ES Absorption", "CVD Divergence"]
for setup in sim_setups:
    sub = [t for t in trades if t["setup"] == setup and scenario_current(t)]
    s = stats(sub)
    if s["n"] > 0:
        print(f"  {setup:<25} N={s['n']:>3}  WR={s['wr']:>5.1f}%  PnL={s['pnl']:>+7.1f}  PF={s['pf']:.2f}")


# ============================================================
print("\n" + "=" * 100)
print("MONTHLY INCOME PROJECTIONS (top scenarios)")
print("=" * 100)

for name, fn in [scenarios[0], scenarios[3], scenarios[6], scenarios[7]]:
    passed = [t for t in trades if fn(t)]
    s = stats(passed)
    wd, td = daily_stats(passed)
    daily_avg = s["pnl"] / td if td > 0 else 0
    monthly_pts = daily_avg * 21  # ~21 trading days

    print(f"\n  {name}")
    print(f"  Daily avg: {daily_avg:+.1f} pts  |  Monthly: {monthly_pts:+.0f} pts")
    for label, multiplier in [("10 MES ($50/pt)", 50), ("2 ES ($100/pt)", 100), ("4 ES ($200/pt)", 200)]:
        monthly = monthly_pts * multiplier
        print(f"    {label}: ${monthly:,.0f}/mo")
