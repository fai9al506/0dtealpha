"""Test different filtering strategies for SHORT trades.
Longs: keep alignment +3 (proven).
Shorts: test alternatives to find the best filter."""

import sqlalchemy as sa
import os
from collections import defaultdict

engine = sa.create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    rows = conn.execute(sa.text("""
        SELECT id, ts::date as dt, setup_name, direction, grade, score,
               spot, lis, paradigm, outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss,
               vanna_all, vanna_weekly, vanna_monthly,
               spot_vol_beta, greek_alignment,
               max_plus_gex, max_minus_gex,
               EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') as hour_et
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND greek_alignment IS NOT NULL
        ORDER BY ts
    """)).fetchall()

# Parse into dicts
trades = []
for r in rows:
    t = {
        "id": r.id, "dt": str(r.dt), "setup": r.setup_name,
        "dir": r.direction, "grade": r.grade, "score": float(r.score),
        "spot": float(r.spot) if r.spot else 0,
        "lis": float(r.lis) if r.lis else None,
        "paradigm": r.paradigm or "",
        "result": r.outcome_result, "pnl": float(r.outcome_pnl or 0),
        "max_profit": float(r.outcome_max_profit or 0),
        "vanna": float(r.vanna_all) if r.vanna_all else None,
        "vanna_w": float(r.vanna_weekly) if r.vanna_weekly else None,
        "vanna_m": float(r.vanna_monthly) if r.vanna_monthly else None,
        "svb": float(r.spot_vol_beta) if r.spot_vol_beta else None,
        "align": int(r.greek_alignment),
        "plus_gex": float(r.max_plus_gex) if r.max_plus_gex else None,
        "minus_gex": float(r.max_minus_gex) if r.max_minus_gex else None,
        "hour": int(r.hour_et),
    }
    t["is_short"] = t["dir"] in ("short", "bearish")
    t["is_long"] = not t["is_short"]
    t["is_win"] = "WIN" in t["result"]
    t["is_loss"] = "LOSS" in t["result"]
    trades.append(t)

longs = [t for t in trades if t["is_long"]]
shorts = [t for t in trades if t["is_short"]]

def stats(subset, label=""):
    if not subset:
        return {"n": 0, "pnl": 0, "wr": 0, "w": 0, "l": 0}
    w = sum(1 for t in subset if t["is_win"])
    l = sum(1 for t in subset if t["is_loss"])
    pnl = sum(t["pnl"] for t in subset)
    wr = w / (w + l) * 100 if (w + l) else 0
    return {"n": len(subset), "pnl": pnl, "wr": wr, "w": w, "l": l}

def print_stats(s, label):
    print("  {:45s}: {:3d} trades, {:+7.1f} pts, {:.0f}% WR ({} W / {} L)".format(
        label, s["n"], s["pnl"], s["wr"], s["w"], s["l"]))

# =====================================================
print("=" * 75)
print("BASELINE: Current system (no filtering)")
print("=" * 75)
print_stats(stats(longs), "All LONGS")
print_stats(stats(shorts), "All SHORTS")
print_stats(stats(trades), "ALL TRADES")

# =====================================================
print("")
print("=" * 75)
print("LONG FILTER (proven): alignment >= +3")
print("=" * 75)
long_a3 = [t for t in longs if t["align"] >= 3]
long_not_a3 = [t for t in longs if t["align"] < 3]
print_stats(stats(long_a3), "Longs align >= +3 (KEEP)")
print_stats(stats(long_not_a3), "Longs align < +3 (BLOCKED)")

# =====================================================
# SHORT FILTER TESTS
# =====================================================
print("")
print("=" * 75)
print("SHORT FILTER TESTS")
print("=" * 75)

# --- Filter S0: No filter (all shorts) ---
print("\n--- S0: No filter (all shorts) ---")
print_stats(stats(shorts), "All shorts")

# --- Filter S1: alignment <= -1 (at least charm opposes = supports short) ---
print("\n--- S1: alignment <= -1 (at least 1 Greek bearish) ---")
s1_keep = [t for t in shorts if t["align"] <= -1]
s1_block = [t for t in shorts if t["align"] > -1]
print_stats(stats(s1_keep), "KEEP (align <= -1)")
print_stats(stats(s1_block), "BLOCKED (align > -1)")

# --- Filter S2: charm negative (charm opposes = bearish) ---
# We don't have charm stored directly, but we can infer:
# For a short trade: align = charm_component + vanna_component + gex_component
# Each is +1 (agrees with short=bearish) or -1 (opposes)
# charm_component for short: +1 if charm < 0, -1 if charm > 0
# vanna_component for short: +1 if vanna < 0, -1 if vanna > 0
# gex_component for short: +1 if spot > +GEX, -1 if spot <= +GEX
# So we can reconstruct charm_component:
# align = charm_c + vanna_c + gex_c
# vanna_c = +1 if vanna < 0 else -1 (for short)
# gex_c = +1 if spot > +GEX else -1 (for short)
# charm_c = align - vanna_c - gex_c

def infer_charm_bearish(t):
    """For a short trade, infer whether charm was bearish (negative)."""
    if t["vanna"] is None:
        return None
    vanna_c = 1 if t["vanna"] < 0 else -1  # short: vanna negative = agrees
    if t["plus_gex"] and t["spot"]:
        gex_c = 1 if t["spot"] > t["plus_gex"] else -1  # short: above +GEX = agrees
    else:
        # GEX unknown, can't determine
        return None
    charm_c = t["align"] - vanna_c - gex_c
    return charm_c > 0  # charm_c = +1 means charm agrees with short = charm is negative = bearish

print("\n--- S2: Charm bearish (inferred: charm < 0, supports the short) ---")
s2_keep = [t for t in shorts if infer_charm_bearish(t) == True]
s2_block = [t for t in shorts if infer_charm_bearish(t) == False]
s2_unknown = [t for t in shorts if infer_charm_bearish(t) is None]
print_stats(stats(s2_keep), "KEEP (charm bearish)")
print_stats(stats(s2_block), "BLOCKED (charm bullish)")
print_stats(stats(s2_unknown), "UNKNOWN (no data)")

# --- Filter S3: SVB negative (bearish spot-vol correlation) ---
print("\n--- S3: SVB < 0 (bearish spot-vol) ---")
s3_keep = [t for t in shorts if t["svb"] is not None and t["svb"] < 0]
s3_block = [t for t in shorts if t["svb"] is not None and t["svb"] >= 0]
s3_none = [t for t in shorts if t["svb"] is None]
print_stats(stats(s3_keep), "KEEP (SVB < 0)")
print_stats(stats(s3_block), "BLOCKED (SVB >= 0)")

# --- Filter S4: SVB < -0.5 (strongly bearish SVB) ---
print("\n--- S4: SVB < -0.5 (strongly bearish SVB) ---")
s4_keep = [t for t in shorts if t["svb"] is not None and t["svb"] < -0.5]
s4_block = [t for t in shorts if t["svb"] is not None and t["svb"] >= -0.5]
print_stats(stats(s4_keep), "KEEP (SVB < -0.5)")
print_stats(stats(s4_block), "BLOCKED (SVB >= -0.5)")

# --- Filter S5: Paradigm contains AG or SIDIAL (bearish paradigms) ---
print("\n--- S5: Bearish paradigm (AG-*, SIDIAL-*) ---")
bearish_paras = ["AG-LIS", "AG-PURE", "AG-TARGET", "SIDIAL-EXTREME", "SIDIAL-BALANCE", "SIDIAL-MESSY"]
s5_keep = [t for t in shorts if t["paradigm"] in bearish_paras]
s5_block = [t for t in shorts if t["paradigm"] not in bearish_paras]
print_stats(stats(s5_keep), "KEEP (AG/SIDIAL paradigm)")
print_stats(stats(s5_block), "BLOCKED (other paradigm)")

# --- Filter S6: Paradigm AG-* only (strongest bearish) ---
print("\n--- S6: AG paradigm only ---")
s6_keep = [t for t in shorts if t["paradigm"].startswith("AG-")]
s6_block = [t for t in shorts if not t["paradigm"].startswith("AG-")]
print_stats(stats(s6_keep), "KEEP (AG-*)")
print_stats(stats(s6_block), "BLOCKED (not AG)")

# --- Filter S7: Vanna negative ---
print("\n--- S7: Vanna negative (bearish) ---")
s7_keep = [t for t in shorts if t["vanna"] is not None and t["vanna"] < 0]
s7_block = [t for t in shorts if t["vanna"] is not None and t["vanna"] >= 0]
print_stats(stats(s7_keep), "KEEP (vanna < 0)")
print_stats(stats(s7_block), "BLOCKED (vanna >= 0)")

# --- Filter S8: DD Exh specific — alignment +2 (contrarian sweet spot) ---
print("\n--- S8: DD Exhaustion shorts only at align +2 ---")
dd_shorts = [t for t in shorts if t["setup"] == "DD Exhaustion"]
s8_keep = [t for t in dd_shorts if t["align"] == 2]
s8_block = [t for t in dd_shorts if t["align"] != 2]
print_stats(stats(s8_keep), "DD Short align=+2 (KEEP)")
print_stats(stats(s8_block), "DD Short align!=+2 (BLOCKED)")

# --- Filter S9: Per-setup best alignment ---
print("\n--- S9: Per-setup optimal alignment for shorts ---")
for setup in ["DD Exhaustion", "Skew Charm", "AG Short", "ES Absorption", "BofA Scalp", "Paradigm Reversal"]:
    setup_shorts = [t for t in shorts if t["setup"] == setup]
    if not setup_shorts:
        continue
    print("  {}:".format(setup))
    for a in sorted(set(t["align"] for t in setup_shorts)):
        sub = [t for t in setup_shorts if t["align"] == a]
        s = stats(sub)
        print("    align={:+d}: {:3d} trades, {:+7.1f} pts, {:.0f}% WR".format(
            a, s["n"], s["pnl"], s["wr"]))

# --- Filter S10: Charm bearish + per-setup specific ---
print("\n--- S10: Charm bearish only ---")
s10_keep = [t for t in shorts if infer_charm_bearish(t) == True]
print_stats(stats(s10_keep), "KEEP (charm bearish)")
# breakdown
for setup in ["DD Exhaustion", "Skew Charm", "AG Short", "ES Absorption"]:
    sub = [t for t in s10_keep if t["setup"] == setup]
    if sub:
        print_stats(stats(sub), "  " + setup)

# --- Filter S11: Charm bearish OR SVB < 0 ---
print("\n--- S11: Charm bearish OR SVB < 0 ---")
s11_keep = [t for t in shorts if infer_charm_bearish(t) == True or (t["svb"] is not None and t["svb"] < 0)]
s11_block = [t for t in shorts if t not in s11_keep]
print_stats(stats(s11_keep), "KEEP")
print_stats(stats(s11_block), "BLOCKED")

# --- Filter S12: Charm bearish AND SVB < 0 ---
print("\n--- S12: Charm bearish AND SVB < 0 ---")
s12_keep = [t for t in shorts if infer_charm_bearish(t) == True and t["svb"] is not None and t["svb"] < 0]
s12_block = [t for t in shorts if t not in s12_keep]
print_stats(stats(s12_keep), "KEEP")
print_stats(stats(s12_block), "BLOCKED")

# --- Filter S13: Spot above LIS (bearish position) ---
print("\n--- S13: Spot above LIS (bearish side) ---")
s13_keep = [t for t in shorts if t["lis"] is not None and t["spot"] > t["lis"]]
s13_block = [t for t in shorts if t["lis"] is not None and t["spot"] <= t["lis"]]
s13_none = [t for t in shorts if t["lis"] is None]
print_stats(stats(s13_keep), "KEEP (above LIS)")
print_stats(stats(s13_block), "BLOCKED (below LIS)")
print_stats(stats(s13_none), "No LIS data")

# --- Filter S14: Spot near or above +GEX (bearish GEX position) ---
print("\n--- S14: Spot within 30 pts of +GEX or above ---")
s14_keep = [t for t in shorts if t["plus_gex"] and (t["spot"] >= t["plus_gex"] - 30)]
s14_block = [t for t in shorts if t["plus_gex"] and (t["spot"] < t["plus_gex"] - 30)]
print_stats(stats(s14_keep), "KEEP (near/above +GEX)")
print_stats(stats(s14_block), "BLOCKED (far below +GEX)")

# =====================================================
# COMBINED STRATEGIES: Long filter + Short filter
# =====================================================
print("")
print("=" * 75)
print("COMBINED: Best long filter + each short filter")
print("=" * 75)

# Base: Longs at align >= +3
base_longs = [t for t in longs if t["align"] >= 3]
base_long_stats = stats(base_longs)

combos = [
    ("C0: Long +3 + ALL shorts (no short filter)", shorts),
    ("C1: Long +3 + Short align <= -1", s1_keep),
    ("C2: Long +3 + Short charm bearish", s2_keep),
    ("C3: Long +3 + Short SVB < 0", s3_keep),
    ("C4: Long +3 + Short SVB < -0.5", s4_keep),
    ("C5: Long +3 + Short AG/SIDIAL paradigm", s5_keep),
    ("C6: Long +3 + Short AG paradigm only", s6_keep),
    ("C7: Long +3 + Short vanna < 0", s7_keep),
    ("C11: Long +3 + Short charm bearish OR SVB<0", s11_keep),
    ("C12: Long +3 + Short charm bearish AND SVB<0", s12_keep),
    ("C13: Long +3 + Short above LIS", s13_keep),
    ("C14: Long +3 + Short near/above +GEX", s14_keep),
]

for label, short_subset in combos:
    combined = base_longs + short_subset
    s = stats(combined)
    print_stats(s, label)

# =====================================================
# Also test: Long align >= +2 instead of +3
# =====================================================
print("")
print("=" * 75)
print("ALTERNATIVE: Long align >= +2 + short filters")
print("=" * 75)
base_longs_2 = [t for t in longs if t["align"] >= 2]
combos2 = [
    ("D0: Long +2 + ALL shorts", shorts),
    ("D1: Long +2 + Short align <= -1", s1_keep),
    ("D2: Long +2 + Short charm bearish", s2_keep),
    ("D3: Long +2 + Short SVB < 0", s3_keep),
    ("D5: Long +2 + Short AG/SIDIAL paradigm", s5_keep),
    ("D11: Long +2 + Short charm bearish OR SVB<0", s11_keep),
]
for label, short_subset in combos2:
    combined = base_longs_2 + short_subset
    s = stats(combined)
    print_stats(s, label)

# =====================================================
# WHAT-IF: Today's trades under each filter
# =====================================================
print("")
print("=" * 75)
print("TODAY (2026-03-11) under each combined filter")
print("=" * 75)
today = [t for t in trades if t["dt"] == "2026-03-11"]
today_longs = [t for t in today if t["is_long"]]
today_shorts = [t for t in today if t["is_short"]]

print("Today baseline: {} longs, {} shorts".format(len(today_longs), len(today_shorts)))
print_stats(stats(today_longs), "Today all longs")
print_stats(stats(today_shorts), "Today all shorts")
print_stats(stats(today), "Today all trades")

# Apply filters to today
today_long_a3 = [t for t in today_longs if t["align"] >= 3]
print("")
print("Today Long +3: {}".format(stats(today_long_a3)["pnl"]))

for label, filter_fn in [
    ("Today C0: +3 long + all short", lambda t: True),
    ("Today C1: +3 long + short align<=-1", lambda t: t["align"] <= -1),
    ("Today C2: +3 long + short charm bearish", lambda t: infer_charm_bearish(t) == True),
    ("Today C3: +3 long + short SVB<0", lambda t: t["svb"] is not None and t["svb"] < 0),
    ("Today C5: +3 long + short AG/SIDIAL para", lambda t: t["paradigm"] in bearish_paras),
    ("Today C11: +3 long + short charm bear OR SVB<0", lambda t: infer_charm_bearish(t) == True or (t["svb"] is not None and t["svb"] < 0)),
]:
    filtered_shorts = [t for t in today_shorts if filter_fn(t)]
    combined = today_long_a3 + filtered_shorts
    s = stats(combined)
    print_stats(s, label)

# =====================================================
# WORST DAYS: How does each filter do on losing days?
# =====================================================
print("")
print("=" * 75)
print("PER-DAY PnL under best combined filters")
print("=" * 75)
by_day = defaultdict(list)
for t in trades:
    by_day[t["dt"]].append(t)

print("Date        | Baseline | C0(+3L,allS) | C1(+3L,a<=-1S) | C2(charm) | C5(AG/SID) | C11(chm|svb)")
for dt in sorted(by_day.keys()):
    day = by_day[dt]
    dl = [t for t in day if t["is_long"]]
    ds = [t for t in day if t["is_short"]]
    dl3 = [t for t in dl if t["align"] >= 3]

    baseline = sum(t["pnl"] for t in day)
    c0 = sum(t["pnl"] for t in dl3) + sum(t["pnl"] for t in ds)
    c1 = sum(t["pnl"] for t in dl3) + sum(t["pnl"] for t in ds if t["align"] <= -1)
    c2 = sum(t["pnl"] for t in dl3) + sum(t["pnl"] for t in ds if infer_charm_bearish(t) == True)
    c5 = sum(t["pnl"] for t in dl3) + sum(t["pnl"] for t in ds if t["paradigm"] in bearish_paras)
    c11 = sum(t["pnl"] for t in dl3) + sum(t["pnl"] for t in ds if infer_charm_bearish(t) == True or (t["svb"] is not None and t["svb"] < 0))

    print("{} | {:>+7.1f} | {:>+7.1f}      | {:>+7.1f}          | {:>+7.1f}     | {:>+7.1f}      | {:>+7.1f}".format(
        dt, baseline, c0, c1, c2, c5, c11))

# Totals
print("-" * 100)
all_base = sum(t["pnl"] for t in trades)
for label, filter_fn in [
    ("C0 total", lambda t: True),
    ("C1 total", lambda t: t["align"] <= -1),
    ("C2 total", lambda t: infer_charm_bearish(t) == True),
    ("C5 total", lambda t: t["paradigm"] in bearish_paras),
    ("C11 total", lambda t: infer_charm_bearish(t) == True or (t["svb"] is not None and t["svb"] < 0)),
]:
    long_pts = sum(t["pnl"] for t in longs if t["align"] >= 3)
    short_pts = sum(t["pnl"] for t in shorts if filter_fn(t))
    print("{:12s}: {:+.1f} (longs {:+.1f} + shorts {:+.1f})".format(label, long_pts + short_pts, long_pts, short_pts))

# =====================================================
# MAX DRAWDOWN per strategy
# =====================================================
print("")
print("=" * 75)
print("CUMULATIVE PnL + MAX DRAWDOWN per strategy")
print("=" * 75)

def max_drawdown(subset):
    """Calculate max drawdown from a list of trades in order."""
    if not subset:
        return 0
    cum = 0
    peak = 0
    dd = 0
    for t in sorted(subset, key=lambda x: x["id"]):
        cum += t["pnl"]
        if cum > peak:
            peak = cum
        if peak - cum > dd:
            dd = peak - cum
    return dd

strategies = {
    "Baseline (all)": trades,
    "Long +3 only": [t for t in longs if t["align"] >= 3],
    "C0: +3L + all S": [t for t in longs if t["align"] >= 3] + shorts,
    "C1: +3L + S a<=-1": [t for t in longs if t["align"] >= 3] + [t for t in shorts if t["align"] <= -1],
    "C2: +3L + S charm bear": [t for t in longs if t["align"] >= 3] + [t for t in shorts if infer_charm_bearish(t) == True],
    "C3: +3L + S SVB<0": [t for t in longs if t["align"] >= 3] + [t for t in shorts if t["svb"] is not None and t["svb"] < 0],
    "C5: +3L + S AG/SIDIAL": [t for t in longs if t["align"] >= 3] + [t for t in shorts if t["paradigm"] in bearish_paras],
    "C11: +3L + S chm|svb": [t for t in longs if t["align"] >= 3] + [t for t in shorts if infer_charm_bearish(t) == True or (t["svb"] is not None and t["svb"] < 0)],
}

for name, subset in strategies.items():
    s = stats(subset)
    dd = max_drawdown(subset)
    daily_avg = s["pnl"] / len(set(t["dt"] for t in subset)) if subset else 0
    print("{:30s}: {:3d}t {:+7.1f}pts {:.0f}%WR DD={:.1f} avg/day={:+.1f}".format(
        name, s["n"], s["pnl"], s["wr"], dd, daily_avg))
