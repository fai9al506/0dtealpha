"""FINAL COMPARISON: Current filter vs Proposed new filter.

Current (PROD): abs(alignment) >= 3
  -> Longs: alignment +3 only
  -> Shorts: alignment -3 only

Proposed: Asymmetric filter
  -> Longs: alignment +3 (SAME)
  -> Shorts: NO alignment filter. Only block toxic setups:
    - ES Absorption shorts: BLOCK
    - BofA Scalp shorts: BLOCK
    - Everything else: ALLOW (Skew Charm, DD Exh, AG Short, Paradigm Rev)
"""
import sqlalchemy as sa
import os
from collections import defaultdict
import statistics

engine = sa.create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    rows = conn.execute(sa.text("""
        SELECT id, ts::date as dt, setup_name, direction, grade, score,
               spot, paradigm, outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss,
               vanna_all, spot_vol_beta, greek_alignment
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND greek_alignment IS NOT NULL
        ORDER BY ts
    """)).fetchall()

trades = []
for r in rows:
    t = {
        "id": r.id, "dt": str(r.dt), "setup": r.setup_name,
        "dir": r.direction, "grade": r.grade,
        "spot": float(r.spot) if r.spot else 0,
        "paradigm": r.paradigm or "",
        "result": r.outcome_result, "pnl": float(r.outcome_pnl or 0),
        "max_profit": float(r.outcome_max_profit or 0),
        "max_loss": float(r.outcome_max_loss or 0),
        "align": int(r.greek_alignment),
    }
    t["is_short"] = t["dir"] in ("short", "bearish")
    t["is_long"] = not t["is_short"]
    t["is_win"] = "WIN" in t["result"]
    t["is_loss"] = "LOSS" in t["result"]
    trades.append(t)

# =====================================================
# FILTER DEFINITIONS
# =====================================================

def current_filter(t):
    """Current production: abs(alignment) >= 3"""
    return abs(t["align"]) >= 3

def new_filter(t):
    """Proposed: +3 longs + no alignment filter on shorts, just block toxic setups"""
    if t["is_long"]:
        return t["align"] >= 3  # Same as current
    # Shorts: NO alignment filter. Only block toxic setups.
    if t["setup"] == "ES Absorption":
        return False  # Block ALL shorts (toxic: -168 pts)
    if t["setup"] == "BofA Scalp":
        return False  # Block ALL shorts (toxic: -26 pts)
    # Everything else: ALLOW regardless of alignment
    return True

# =====================================================
# APPLY FILTERS
# =====================================================
current_trades = [t for t in trades if current_filter(t)]
new_trades = [t for t in trades if new_filter(t)]
baseline_trades = trades

def full_stats(subset, label):
    if not subset:
        print("  {:35s}: NO TRADES".format(label))
        return
    w = sum(1 for t in subset if t["is_win"])
    l = sum(1 for t in subset if t["is_loss"])
    pnl = sum(t["pnl"] for t in subset)
    wr = w / (w + l) * 100 if (w + l) else 0

    # Max drawdown
    cum = 0; peak = 0; dd = 0
    for t in sorted(subset, key=lambda x: x["id"]):
        cum += t["pnl"]
        if cum > peak: peak = cum
        if peak - cum > dd: dd = peak - cum

    # Max consecutive losses
    max_streak = 0; streak = 0
    for t in sorted(subset, key=lambda x: x["id"]):
        if t["is_loss"]:
            streak += 1
            if streak > max_streak: max_streak = streak
        else:
            streak = 0

    # Daily stats
    daily = defaultdict(float)
    for t in subset:
        daily[t["dt"]] += t["pnl"]
    days = len(daily)
    avg_day = pnl / days if days else 0
    losing_days = sum(1 for v in daily.values() if v < 0)
    winning_days = sum(1 for v in daily.values() if v > 0)
    worst_day = min(daily.values()) if daily else 0
    best_day = max(daily.values()) if daily else 0

    # Sharpe
    if len(daily) > 1:
        vals = list(daily.values())
        sharpe = statistics.mean(vals) / statistics.stdev(vals)
    else:
        sharpe = 0

    # Profit factor
    gross_wins = sum(t["pnl"] for t in subset if t["pnl"] > 0)
    gross_losses = abs(sum(t["pnl"] for t in subset if t["pnl"] < 0))
    pf = gross_wins / gross_losses if gross_losses > 0 else 999

    print("  {:35s}: {:3d}t {:+7.1f}pts {:.0f}%WR PF={:.2f} DD={:.0f} Sharpe={:.2f}".format(
        label, len(subset), pnl, wr, pf, dd, sharpe))
    print("  {:35s}  {}W/{}L maxStreak={} | {:.1f}/day | {}W/{}L days | worst={:+.1f} best={:+.1f}".format(
        "", w, l, max_streak, avg_day, winning_days, losing_days, worst_day, best_day))

# =====================================================
print("=" * 80)
print("OVERALL COMPARISON")
print("=" * 80)
full_stats(baseline_trades, "Baseline (no filter)")
print("")
full_stats(current_trades, "CURRENT (abs(align) >= 3)")
print("")
full_stats(new_trades, "PROPOSED (new asymmetric)")
print("")

# Breakdown: what's in each
current_longs = [t for t in current_trades if t["is_long"]]
current_shorts = [t for t in current_trades if t["is_short"]]
new_longs = [t for t in new_trades if t["is_long"]]
new_shorts = [t for t in new_trades if t["is_short"]]

print("--- LONGS ---")
full_stats(current_longs, "CURRENT longs (+3)")
full_stats(new_longs, "PROPOSED longs (+3) [same]")

print("\n--- SHORTS ---")
full_stats(current_shorts, "CURRENT shorts (-3)")
full_stats(new_shorts, "PROPOSED shorts (per-setup)")

# What's different?
print("\n--- WHAT CHANGES FOR SHORTS ---")
print("Current filter passes {} shorts (alignment -3 only)".format(len(current_shorts)))
print("Proposed filter passes {} shorts (per-setup rules)".format(len(new_shorts)))

# Show the -3 shorts that current keeps
print("\n  Current -3 shorts breakdown:")
for setup in sorted(set(t["setup"] for t in current_shorts)):
    sub = [t for t in current_shorts if t["setup"] == setup]
    s_w = sum(1 for t in sub if t["is_win"])
    s_l = sum(1 for t in sub if t["is_loss"])
    s_pnl = sum(t["pnl"] for t in sub)
    print("    {:20s}: {:2d}t, {:+6.1f} pts, {:.0f}% WR".format(
        setup, len(sub), s_pnl,
        s_w/(s_w+s_l)*100 if (s_w+s_l) else 0))

# Show the proposed shorts breakdown
print("\n  Proposed shorts breakdown:")
for setup in sorted(set(t["setup"] for t in new_shorts)):
    sub = [t for t in new_shorts if t["setup"] == setup]
    s_w = sum(1 for t in sub if t["is_win"])
    s_l = sum(1 for t in sub if t["is_loss"])
    s_pnl = sum(t["pnl"] for t in sub)
    print("    {:20s}: {:2d}t, {:+6.1f} pts, {:.0f}% WR".format(
        setup, len(sub), s_pnl,
        s_w/(s_w+s_l)*100 if (s_w+s_l) else 0))

# Blocked shorts comparison
print("\n  BLOCKED by current but ALLOWED by proposed:")
only_new = [t for t in new_shorts if not current_filter(t)]
for setup in sorted(set(t["setup"] for t in only_new)):
    sub = [t for t in only_new if t["setup"] == setup]
    s_w = sum(1 for t in sub if t["is_win"])
    s_l = sum(1 for t in sub if t["is_loss"])
    s_pnl = sum(t["pnl"] for t in sub)
    print("    {:20s}: {:2d}t, {:+6.1f} pts (GAINED)".format(setup, len(sub), s_pnl))

print("\n  ALLOWED by current but BLOCKED by proposed:")
only_current = [t for t in current_shorts if not new_filter(t)]
for setup in sorted(set(t["setup"] for t in only_current)):
    sub = [t for t in only_current if t["setup"] == setup]
    s_w = sum(1 for t in sub if t["is_win"])
    s_l = sum(1 for t in sub if t["is_loss"])
    s_pnl = sum(t["pnl"] for t in sub)
    print("    {:20s}: {:2d}t, {:+6.1f} pts (REMOVED)".format(setup, len(sub), s_pnl))

# =====================================================
print("")
print("=" * 80)
print("DAILY COMPARISON")
print("=" * 80)
all_dates = sorted(set(t["dt"] for t in trades))
print("{:12s} | {:>10s} {:>10s} {:>10s} | {:>10s} {:>10s} {:>10s} | {:>6s}".format(
    "Date", "Cur PnL", "Cur #", "Cur Sh#",
    "New PnL", "New #", "New Sh#", "Delta"))
print("-" * 95)

cur_total = 0
new_total = 0
cur_losing_days = 0
new_losing_days = 0

for dt in all_dates:
    day = [t for t in trades if t["dt"] == dt]

    cur_day = [t for t in day if current_filter(t)]
    new_day = [t for t in day if new_filter(t)]

    cur_pnl = sum(t["pnl"] for t in cur_day)
    new_pnl = sum(t["pnl"] for t in new_day)
    cur_total += cur_pnl
    new_total += new_pnl

    cur_shorts_n = sum(1 for t in cur_day if t["is_short"])
    new_shorts_n = sum(1 for t in new_day if t["is_short"])

    if cur_pnl < 0: cur_losing_days += 1
    if new_pnl < 0: new_losing_days += 1

    delta = new_pnl - cur_pnl
    delta_str = "{:+.1f}".format(delta) if delta != 0 else "="

    print("{:12s} | {:>+10.1f} {:>10d} {:>10d} | {:>+10.1f} {:>10d} {:>10d} | {:>6s}".format(
        dt, cur_pnl, len(cur_day), cur_shorts_n,
        new_pnl, len(new_day), new_shorts_n, delta_str))

print("-" * 95)
print("{:12s} | {:>+10.1f} {:>10d} {:>10s} | {:>+10.1f} {:>10d} {:>10s} | {:>+6.1f}".format(
    "TOTAL", cur_total, len(current_trades), "",
    new_total, len(new_trades), "", new_total - cur_total))
print("")
print("Current: {} losing days / {} total".format(cur_losing_days, len(all_dates)))
print("Proposed: {} losing days / {} total".format(new_losing_days, len(all_dates)))

# =====================================================
print("")
print("=" * 80)
print("TODAY (2026-03-11) DETAIL")
print("=" * 80)
today = [t for t in trades if t["dt"] == "2026-03-11"]
today_cur = [t for t in today if current_filter(t)]
today_new = [t for t in today if new_filter(t)]

print("TODAY current: {} trades, {:+.1f} pts".format(len(today_cur), sum(t["pnl"] for t in today_cur)))
for t in sorted(today_cur, key=lambda x: x["id"]):
    print("  #{} {:20s} {:8s} a={:+d} {:8s} {:+.1f}".format(
        t["id"], t["setup"], t["dir"], t["align"], t["result"], t["pnl"]))

print("")
print("TODAY proposed: {} trades, {:+.1f} pts".format(len(today_new), sum(t["pnl"] for t in today_new)))
for t in sorted(today_new, key=lambda x: x["id"]):
    status = "SAME" if current_filter(t) else "NEW"
    print("  #{} {:20s} {:8s} a={:+d} {:8s} {:+.1f} [{}]".format(
        t["id"], t["setup"], t["dir"], t["align"], t["result"], t["pnl"], status))

# Show what proposed blocks vs current
print("")
print("TODAY trades BLOCKED by current but ALLOWED by proposed:")
for t in sorted(today, key=lambda x: x["id"]):
    if new_filter(t) and not current_filter(t):
        print("  #{} {:20s} {:8s} a={:+d} {:8s} {:+.1f} [GAINED]".format(
            t["id"], t["setup"], t["dir"], t["align"], t["result"], t["pnl"]))

print("")
print("TODAY trades ALLOWED by current but BLOCKED by proposed:")
for t in sorted(today, key=lambda x: x["id"]):
    if current_filter(t) and not new_filter(t):
        print("  #{} {:20s} {:8s} a={:+d} {:8s} {:+.1f} [REMOVED]".format(
            t["id"], t["setup"], t["dir"], t["align"], t["result"], t["pnl"]))

# =====================================================
print("")
print("=" * 80)
print("IMPROVEMENT SUMMARY")
print("=" * 80)
cur_s = sum(t["pnl"] for t in current_trades)
new_s = sum(t["pnl"] for t in new_trades)
print("Current filter PnL:  {:+.1f} pts ({} trades)".format(cur_s, len(current_trades)))
print("Proposed filter PnL: {:+.1f} pts ({} trades)".format(new_s, len(new_trades)))
print("Improvement:         {:+.1f} pts ({} more trades)".format(new_s - cur_s, len(new_trades) - len(current_trades)))
print("")
print("What changes:")
print("  Longs: IDENTICAL (alignment +3, no change)")
print("  Shorts: Current keeps {} shorts at -3 ({:+.1f} pts)".format(
    len(current_shorts), sum(t["pnl"] for t in current_shorts)))
print("  Shorts: Proposed keeps {} shorts per-setup ({:+.1f} pts)".format(
    len(new_shorts), sum(t["pnl"] for t in new_shorts)))
print("  Net short improvement: {:+.1f} pts".format(
    sum(t["pnl"] for t in new_shorts) - sum(t["pnl"] for t in current_shorts)))
