"""Part 2: Test per-setup short filters + missing DD calculations."""
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

trades = []
for r in rows:
    t = {
        "id": r.id, "dt": str(r.dt), "setup": r.setup_name,
        "dir": r.direction, "grade": r.grade, "score": float(r.score),
        "spot": float(r.spot) if r.spot else 0,
        "lis": float(r.lis) if r.lis else None,
        "paradigm": r.paradigm or "",
        "result": r.outcome_result, "pnl": float(r.outcome_pnl or 0),
        "vanna": float(r.vanna_all) if r.vanna_all else None,
        "svb": float(r.spot_vol_beta) if r.spot_vol_beta else None,
        "align": int(r.greek_alignment),
        "plus_gex": float(r.max_plus_gex) if r.max_plus_gex else None,
        "hour": int(r.hour_et),
    }
    t["is_short"] = t["dir"] in ("short", "bearish")
    t["is_long"] = not t["is_short"]
    t["is_win"] = "WIN" in t["result"]
    t["is_loss"] = "LOSS" in t["result"]
    trades.append(t)

longs = [t for t in trades if t["is_long"]]
shorts = [t for t in trades if t["is_short"]]

def stats(subset):
    if not subset:
        return {"n": 0, "pnl": 0, "wr": 0, "w": 0, "l": 0}
    w = sum(1 for t in subset if t["is_win"])
    l = sum(1 for t in subset if t["is_loss"])
    pnl = sum(t["pnl"] for t in subset)
    wr = w / (w + l) * 100 if (w + l) else 0
    return {"n": len(subset), "pnl": pnl, "wr": wr, "w": w, "l": l}

def max_drawdown(subset):
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

def print_strat(name, subset):
    s = stats(subset)
    dd = max_drawdown(subset)
    days = len(set(t["dt"] for t in subset)) if subset else 1
    avg_day = s["pnl"] / days
    print("{:45s}: {:3d}t {:+7.1f}pts {:.0f}%WR DD={:5.1f} {:+.1f}/day".format(
        name, s["n"], s["pnl"], s["wr"], dd, avg_day))

def infer_charm_bearish(t):
    if t["vanna"] is None:
        return None
    vanna_c = 1 if t["vanna"] < 0 else -1
    if t["plus_gex"] and t["spot"]:
        gex_c = 1 if t["spot"] > t["plus_gex"] else -1
    else:
        return None
    charm_c = t["align"] - vanna_c - gex_c
    return charm_c > 0

# =====================================================
print("=" * 75)
print("PER-SETUP SHORT FILTER: Which shorts to keep?")
print("=" * 75)

# ES Absorption shorts — are they ever worth it?
print("\n--- ES Absorption shorts (all negative!) ---")
es_shorts = [t for t in shorts if t["setup"] == "ES Absorption"]
print_strat("ES Abs shorts ALL", es_shorts)
for svb_cut in [0, -0.5, -1.0]:
    sub = [t for t in es_shorts if t["svb"] is not None and t["svb"] < svb_cut]
    print_strat("  ES Abs shorts SVB < {}".format(svb_cut), sub)
for a in sorted(set(t["align"] for t in es_shorts)):
    sub = [t for t in es_shorts if t["align"] == a]
    s = stats(sub)
    print("    align={:+d}: {:3d}t {:+.1f}pts {:.0f}%WR".format(a, s["n"], s["pnl"], s["wr"]))

# BofA shorts
print("\n--- BofA Scalp shorts ---")
bofa_shorts = [t for t in shorts if t["setup"] == "BofA Scalp"]
print_strat("BofA shorts ALL", bofa_shorts)

# =====================================================
print("")
print("=" * 75)
print("STRATEGY: Per-setup short rules + Long +3")
print("=" * 75)

# Build per-setup optimal short filter
def per_setup_short_filter(t):
    """Return True if this short trade passes the per-setup filter."""
    if t["setup"] == "ES Absorption":
        return False  # Block ALL — negative at every alignment
    if t["setup"] == "BofA Scalp":
        return t["align"] >= 0  # Only keep align 0 and +2 (positive buckets)
    if t["setup"] == "DD Exhaustion":
        return t["align"] != 0  # Block align=0 (28% WR, -97 pts)
    if t["setup"] == "AG Short":
        return t["align"] != -3  # Block -3 (42% WR, already in current filter)
    # Skew Charm, Paradigm Reversal: allow all
    return True

filtered_shorts_v1 = [t for t in shorts if per_setup_short_filter(t)]
print_strat("V1: Per-setup rules + Long +3", [t for t in longs if t["align"] >= 3] + filtered_shorts_v1)
print_strat("  (shorts only)", filtered_shorts_v1)

# V2: V1 + SVB < -0.5
filtered_shorts_v2 = [t for t in shorts if t["svb"] is not None and t["svb"] < -0.5]
print_strat("V2: SVB < -0.5 + Long +3", [t for t in longs if t["align"] >= 3] + filtered_shorts_v2)
print_strat("  (shorts only)", filtered_shorts_v2)

# V3: V1 per-setup + SVB < -0.5 combined
filtered_shorts_v3 = [t for t in shorts if per_setup_short_filter(t) and (t["svb"] is None or t["svb"] < -0.5)]
print_strat("V3: Per-setup + SVB<-0.5 + Long +3", [t for t in longs if t["align"] >= 3] + filtered_shorts_v3)

# V4: Keep only DD Exh (align -1/+2) + Skew Charm + AG (align -1) shorts
def v4_filter(t):
    if t["setup"] == "DD Exhaustion":
        return t["align"] in (-1, 1, 2)
    if t["setup"] == "Skew Charm":
        return True
    if t["setup"] == "AG Short":
        return t["align"] in (-1, 1, 3)
    return False

filtered_shorts_v4 = [t for t in shorts if v4_filter(t)]
print_strat("V4: DD(a!=0)+SC+AG(a!=-3) + Long +3", [t for t in longs if t["align"] >= 3] + filtered_shorts_v4)
print_strat("  (shorts only)", filtered_shorts_v4)

# V5: V4 + SVB < -0.5
filtered_shorts_v5 = [t for t in shorts if v4_filter(t) and (t["svb"] is None or t["svb"] < -0.5)]
print_strat("V5: V4 + SVB<-0.5 + Long +3", [t for t in longs if t["align"] >= 3] + filtered_shorts_v5)

# V6: Only Skew Charm + DD Exh shorts (drop AG, ES Abs, BofA entirely)
filtered_shorts_v6 = [t for t in shorts if t["setup"] in ("Skew Charm", "DD Exhaustion")]
print_strat("V6: Only SC+DD shorts + Long +3", [t for t in longs if t["align"] >= 3] + filtered_shorts_v6)
print_strat("  (shorts only)", filtered_shorts_v6)

# V7: SC + DD shorts, DD block align=0
filtered_shorts_v7 = [t for t in shorts if t["setup"] == "Skew Charm" or (t["setup"] == "DD Exhaustion" and t["align"] != 0)]
print_strat("V7: SC + DD(a!=0) shorts + Long +3", [t for t in longs if t["align"] >= 3] + filtered_shorts_v7)
print_strat("  (shorts only)", filtered_shorts_v7)

# V8: V7 + SVB<-0.5
filtered_shorts_v8 = [t for t in shorts if (t["setup"] == "Skew Charm" or (t["setup"] == "DD Exhaustion" and t["align"] != 0)) and (t["svb"] is None or t["svb"] < -0.5)]
print_strat("V8: V7 + SVB<-0.5 + Long +3", [t for t in longs if t["align"] >= 3] + filtered_shorts_v8)

# =====================================================
print("")
print("=" * 75)
print("ALSO: What if we use Long >= +2 instead of +3?")
print("=" * 75)
longs_a2 = [t for t in longs if t["align"] >= 2]
print_strat("Long +2 only", longs_a2)
print_strat("Long +2 + V4 shorts", longs_a2 + filtered_shorts_v4)
print_strat("Long +2 + V7 shorts", longs_a2 + filtered_shorts_v7)

# =====================================================
print("")
print("=" * 75)
print("TODAY under each strategy")
print("=" * 75)
today = [t for t in trades if t["dt"] == "2026-03-11"]
today_l = [t for t in today if t["is_long"]]
today_s = [t for t in today if t["is_short"]]
today_l3 = [t for t in today_l if t["align"] >= 3]

for label, short_fn in [
    ("Today baseline", lambda t: True),
    ("Today V1 (per-setup)", per_setup_short_filter),
    ("Today V4 (DD+SC+AG)", v4_filter),
    ("Today V7 (SC+DD a!=0)", lambda t: t["setup"] == "Skew Charm" or (t["setup"] == "DD Exhaustion" and t["align"] != 0)),
]:
    fs = [t for t in today_s if short_fn(t)]
    combined = today_l3 + fs
    s = stats(combined)
    shorts_pnl = sum(t["pnl"] for t in fs)
    print("  {:35s}: {:2d} trades, {:+7.1f} pts, {:.0f}% WR (shorts: {:2d}t {:+.1f})".format(
        label, s["n"], s["pnl"], s["wr"], len(fs), shorts_pnl))

# =====================================================
print("")
print("=" * 75)
print("FINAL: Comparison table of all strategies")
print("=" * 75)

all_strategies = [
    ("Baseline (unfiltered)", trades),
    ("Long +3 only (no shorts)", [t for t in longs if t["align"] >= 3]),
    ("Current filter (+3 both)", [t for t in trades if t["align"] >= 3]),
    ("C0: +3L + all shorts", [t for t in longs if t["align"] >= 3] + shorts),
    ("C4: +3L + SVB<-0.5 shorts", [t for t in longs if t["align"] >= 3] + [t for t in shorts if t["svb"] is not None and t["svb"] < -0.5]),
    ("C7: +3L + vanna<0 shorts", [t for t in longs if t["align"] >= 3] + [t for t in shorts if t["vanna"] is not None and t["vanna"] < 0]),
    ("V1: +3L + per-setup shorts", [t for t in longs if t["align"] >= 3] + filtered_shorts_v1),
    ("V4: +3L + DD+SC+AG shorts", [t for t in longs if t["align"] >= 3] + filtered_shorts_v4),
    ("V7: +3L + SC+DD(a!=0) shorts", [t for t in longs if t["align"] >= 3] + filtered_shorts_v7),
    ("D5: +2L + AG/SID shorts", [t for t in longs if t["align"] >= 2] + [t for t in shorts if t["paradigm"] in ["AG-LIS", "AG-PURE", "AG-TARGET", "SIDIAL-EXTREME", "SIDIAL-BALANCE", "SIDIAL-MESSY"]]),
    ("Long +2 + V7 shorts", longs_a2 + filtered_shorts_v7),
]

print("{:40s} | {:>4s} | {:>8s} | {:>4s} | {:>6s} | {:>7s} | {:>6s}".format(
    "Strategy", "N", "PnL", "WR%", "DD", "PnL/dy", "Sharpe"))
print("-" * 90)
for name, subset in all_strategies:
    s = stats(subset)
    dd = max_drawdown(subset)
    days = len(set(t["dt"] for t in subset)) if subset else 1
    avg_day = s["pnl"] / days
    # Simple sharpe proxy: avg daily pnl / stdev daily pnl
    daily_pnls = defaultdict(float)
    for t in subset:
        daily_pnls[t["dt"]] += t["pnl"]
    if len(daily_pnls) > 1:
        import statistics
        vals = list(daily_pnls.values())
        mean = statistics.mean(vals)
        std = statistics.stdev(vals)
        sharpe = mean / std if std > 0 else 0
    else:
        sharpe = 0
    print("{:40s} | {:>4d} | {:>+8.1f} | {:>4.0f} | {:>6.1f} | {:>+7.1f} | {:>6.2f}".format(
        name, s["n"], s["pnl"], s["wr"], dd, avg_day, sharpe))
