"""
Backtest 5 Discord-inspired recommendations against V9-SC baseline.
Tests:
  1. FOMC/Major Event calendar gate
  2. Sidial paradigm filter
  3. Overvix regime direction tracking
  4. Friday (CPF) risk reduction
  5. Combined best filters
"""

import json, os, sys
from datetime import datetime, date
from collections import defaultdict

# Load trade data
with open("tmp_backtest_data.json") as f:
    trades = json.load(f)

print(f"Loaded {len(trades)} trades ({trades[0]['trade_date']} to {trades[-1]['trade_date']})")

# ── FOMC / Major USD Event dates ──
# From economic_events table: High-impact USD events during market hours
FOMC_DECISION_DAYS = {"2026-03-18"}  # Only actual FOMC decision day in our data range

# Major USD high-impact event dates (NFP, CPI, PPI, GDP, ISM, FOMC)
MAJOR_USD_EVENT_DAYS = {
    "2026-02-27",  # Core PPI, PPI
    "2026-03-04",  # ADP NFP, ISM Services PMI
    "2026-03-06",  # NFP + Retail Sales + Unemployment Rate (big day)
    "2026-03-11",  # CPI + Core CPI
    "2026-03-13",  # Prelim GDP + Core PCE + JOLTS
    "2026-03-18",  # PPI + FOMC Decision + Projections + Press Conference
}

# OPEX weeks (third Friday of month) — the whole week leading up
OPEX_FRIDAYS = {"2026-02-20", "2026-03-20"}  # Feb 20 and Mar 20 are 3rd Fridays
OPEX_WEEKS = set()
for opex_fri in OPEX_FRIDAYS:
    d = datetime.strptime(opex_fri, "%Y-%m-%d").date()
    for offset in range(5):  # Mon-Fri of opex week
        from datetime import timedelta
        day = d - timedelta(days=d.weekday()) + timedelta(days=offset)
        OPEX_WEEKS.add(str(day))


# ── V9-SC baseline filter (current production) ──
def v9sc_filter(t):
    """Current V9-SC filter logic"""
    direction = t["direction"]
    setup = t["setup_name"]
    alignment = t.get("alignment") or 0
    vix = t.get("vix")
    overvix = t.get("overvix")

    if direction in ("long", "bullish"):
        # Longs: alignment >= +2 AND (Skew Charm OR VIX <= 22 OR overvix >= +2)
        if alignment < 2:
            return False
        is_sc = setup == "Skew Charm"
        vix_ok = vix is not None and vix <= 22
        overvix_ok = overvix is not None and overvix >= 2
        if not (is_sc or vix_ok or overvix_ok):
            return False
        return True
    else:
        # Shorts whitelist: SC (all), AG (all), DD (align!=0)
        if setup == "Skew Charm":
            return True
        if setup == "AG Short":
            return True
        if setup == "DD Exhaustion":
            return alignment != 0
        return False


# ── Helper functions ──
def compute_stats(filtered_trades, label=""):
    wins = [t for t in filtered_trades if t["outcome"] == "WIN"]
    losses = [t for t in filtered_trades if t["outcome"] == "LOSS"]
    total_pnl = sum(t["outcome_pnl"] for t in filtered_trades)
    win_pnl = sum(t["outcome_pnl"] for t in wins)
    loss_pnl = sum(abs(t["outcome_pnl"]) for t in losses)
    wr = len(wins) / len(filtered_trades) * 100 if filtered_trades else 0
    pf = win_pnl / loss_pnl if loss_pnl > 0 else float("inf")

    # Max drawdown
    running = 0
    peak = 0
    max_dd = 0
    for t in sorted(filtered_trades, key=lambda x: x["ts_et"]):
        running += t["outcome_pnl"]
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    # Daily PnL
    daily = defaultdict(float)
    for t in filtered_trades:
        daily[t["trade_date"]] += t["outcome_pnl"]
    days = len(daily)
    avg_daily = total_pnl / days if days > 0 else 0

    return {
        "label": label,
        "trades": len(filtered_trades),
        "wins": len(wins),
        "losses": len(losses),
        "wr": wr,
        "pnl": total_pnl,
        "pf": pf,
        "max_dd": max_dd,
        "days": days,
        "avg_daily": avg_daily,
    }


def print_stats(s):
    print(
        f"  {s['label']:45s} | {s['trades']:4d} trades | {s['wins']}W/{s['losses']}L | "
        f"WR {s['wr']:5.1f}% | PnL {s['pnl']:+8.1f} | PF {s['pf']:5.2f} | "
        f"MaxDD {s['max_dd']:6.1f} | {s['days']}d @ {s['avg_daily']:+.1f}/day"
    )


def compare(baseline, test, label):
    """Print delta between baseline and test"""
    delta_trades = test["trades"] - baseline["trades"]
    delta_pnl = test["pnl"] - baseline["pnl"]
    delta_wr = test["wr"] - baseline["wr"]
    delta_dd = test["max_dd"] - baseline["max_dd"]
    blocked = baseline["trades"] - test["trades"]
    print(
        f"  DELTA: {delta_trades:+d} trades | WR {delta_wr:+.1f}% | "
        f"PnL {delta_pnl:+.1f} | MaxDD {delta_dd:+.1f} | "
        f"Blocked {blocked} trades"
    )
    if delta_pnl > 0:
        print(f"  >>> IMPROVEMENT: +{delta_pnl:.1f} pts")
    else:
        print(f"  >>> WORSE: {delta_pnl:.1f} pts")
    return delta_pnl


# ══════════════════════════════════════════════════════════
# BASELINE: V9-SC (current production filter)
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 100)
print("BASELINE: V9-SC (current production filter)")
print("=" * 100)

# Note: Many older trades don't have VIX/overvix data (VIX3M bug fixed Mar 17).
# We need to handle this carefully.
# For trades without VIX data, we can't apply V9-SC filter accurately.
# Let's first check data availability.

has_vix = [t for t in trades if t.get("vix") is not None]
no_vix = [t for t in trades if t.get("vix") is None]
print(f"Trades with VIX data: {len(has_vix)}, without: {len(no_vix)}")

# For fair comparison, use ALL trades as "unfiltered baseline"
# Then apply V9-SC to see what it would pass
# BUT since most trades lack VIX data (bug before Mar 17), V9-SC can't be tested fairly
# on the full dataset. Let's show both:

# Unfiltered baseline (all trades as-is from production)
baseline_all = compute_stats(trades, "ALL TRADES (unfiltered)")
print_stats(baseline_all)

# V9-SC filtered (for trades with VIX data only — Mar 17-18)
v9sc_trades = [t for t in trades if v9sc_filter(t)]
v9sc_stats = compute_stats(v9sc_trades, "V9-SC filtered (VIX data available)")
print_stats(v9sc_stats)

# For the rest of the backtest, we'll test filters on ALL trades
# (since the Discord recommendations don't all require VIX data)

print("\n" + "=" * 100)
print("TEST 1: FOMC Decision Day Gate")
print("  Block ALL trades on FOMC decision days")
print("=" * 100)

fomc_blocked = [t for t in trades if t["trade_date"] in FOMC_DECISION_DAYS]
fomc_passed = [t for t in trades if t["trade_date"] not in FOMC_DECISION_DAYS]
fomc_stats = compute_stats(fomc_passed, "FOMC gate (block FOMC days)")
print_stats(fomc_stats)
compare(baseline_all, fomc_stats, "FOMC gate")

# Show what we blocked
fomc_blocked_stats = compute_stats(fomc_blocked, "BLOCKED (FOMC day trades)")
print_stats(fomc_blocked_stats)

print("\n" + "=" * 100)
print("TEST 1b: Major USD Event Day Gate")
print("  Block ALL trades on days with high-impact USD events")
print("=" * 100)

event_blocked = [t for t in trades if t["trade_date"] in MAJOR_USD_EVENT_DAYS]
event_passed = [t for t in trades if t["trade_date"] not in MAJOR_USD_EVENT_DAYS]
event_stats = compute_stats(event_passed, "Major event gate (block event days)")
print_stats(event_stats)
compare(baseline_all, event_stats, "Event gate")

event_blocked_stats = compute_stats(event_blocked, "BLOCKED (event day trades)")
print_stats(event_blocked_stats)

# Per-event-day breakdown
print("\n  Per event day breakdown:")
for d in sorted(MAJOR_USD_EVENT_DAYS):
    day_trades = [t for t in trades if t["trade_date"] == d]
    if day_trades:
        day_pnl = sum(t["outcome_pnl"] for t in day_trades)
        day_wins = len([t for t in day_trades if t["outcome"] == "WIN"])
        day_losses = len([t for t in day_trades if t["outcome"] == "LOSS"])
        wr = day_wins / len(day_trades) * 100 if day_trades else 0
        print(f"    {d}: {len(day_trades)} trades, {day_wins}W/{day_losses}L, WR {wr:.0f}%, PnL {day_pnl:+.1f}")

print("\n" + "=" * 100)
print("TEST 1c: OPEX Week Gate")
print("  Block ALL trades during OPEX week")
print("=" * 100)

opex_blocked = [t for t in trades if t["trade_date"] in OPEX_WEEKS]
opex_passed = [t for t in trades if t["trade_date"] not in OPEX_WEEKS]
if opex_blocked:
    opex_stats = compute_stats(opex_passed, "OPEX week gate")
    print_stats(opex_stats)
    compare(baseline_all, opex_stats, "OPEX gate")
    opex_blocked_stats = compute_stats(opex_blocked, "BLOCKED (OPEX week)")
    print_stats(opex_blocked_stats)
else:
    print("  No trades fall in identified OPEX weeks in our data range")
    # Check which dates we have
    all_dates = sorted(set(t["trade_date"] for t in trades))
    print(f"  Our data range: {all_dates[0]} to {all_dates[-1]}")
    print(f"  OPEX weeks checked: {sorted(OPEX_WEEKS)}")

print("\n" + "=" * 100)
print("TEST 2: Sidial Paradigm Filter")
print("  Various approaches to handling Sidial paradigm")
print("=" * 100)

# 2a: Block ALL trades in Sidial paradigm
sidial_types = {"SIDIAL-EXTREME", "SIDIAL-MESSY", "SIDIAL-BALANCE"}
sidial_blocked = [t for t in trades if t.get("paradigm", "") in sidial_types]
sidial_passed = [t for t in trades if t.get("paradigm", "") not in sidial_types]

test2a = compute_stats(sidial_passed, "2a: Block ALL Sidial trades")
print_stats(test2a)
compare(baseline_all, test2a, "Block all Sidial")

sidial_stats = compute_stats(sidial_blocked, "BLOCKED Sidial trades")
print_stats(sidial_stats)

# 2b: Block only Sidial LONGS (keep shorts — Sidial is choppy, shorts may work)
sidial_longs = [t for t in trades if t.get("paradigm", "") in sidial_types and t["direction"] in ("long", "bullish")]
sidial_shorts = [t for t in trades if t.get("paradigm", "") in sidial_types and t["direction"] in ("short", "bearish")]

print(f"\n  Sidial longs: {len(sidial_longs)} trades, PnL {sum(t['outcome_pnl'] for t in sidial_longs):+.1f}")
print(f"  Sidial shorts: {len(sidial_shorts)} trades, PnL {sum(t['outcome_pnl'] for t in sidial_shorts):+.1f}")

test2b_trades = [t for t in trades if not (t.get("paradigm", "") in sidial_types and t["direction"] in ("long", "bullish"))]
test2b = compute_stats(test2b_trades, "2b: Block Sidial LONGS only")
print_stats(test2b)
compare(baseline_all, test2b, "Block Sidial longs")

# 2c: Block only specific setups in Sidial (ES Absorption + DD in Sidial)
sidial_es_dd = [
    t for t in trades
    if t.get("paradigm", "") in sidial_types
    and t["setup_name"] in ("ES Absorption", "DD Exhaustion")
]
test2c_trades = [t for t in trades if t not in sidial_es_dd]
test2c = compute_stats(test2c_trades, "2c: Block ES Abs + DD in Sidial")
print_stats(test2c)
compare(baseline_all, test2c, "Block ES+DD in Sidial")

# 2d: Per-setup breakdown in Sidial
print("\n  Per-setup breakdown in Sidial:")
for setup in sorted(set(t["setup_name"] for t in sidial_blocked)):
    st = [t for t in sidial_blocked if t["setup_name"] == setup]
    wins = len([t for t in st if t["outcome"] == "WIN"])
    losses = len([t for t in st if t["outcome"] == "LOSS"])
    pnl = sum(t["outcome_pnl"] for t in st)
    wr = wins / len(st) * 100 if st else 0
    print(f"    {setup:20s}: {len(st):3d} trades, {wins}W/{losses}L, WR {wr:.0f}%, PnL {pnl:+.1f}")

# 2e: Sidial by subtype
print("\n  Sidial by subtype:")
for subtype in sorted(sidial_types):
    st = [t for t in trades if t.get("paradigm", "") == subtype]
    if st:
        wins = len([t for t in st if t["outcome"] == "WIN"])
        losses = len([t for t in st if t["outcome"] == "LOSS"])
        pnl = sum(t["outcome_pnl"] for t in st)
        wr = wins / len(st) * 100 if st else 0
        print(f"    {subtype:20s}: {len(st):3d} trades, {wins}W/{losses}L, WR {wr:.0f}%, PnL {pnl:+.1f}")

print("\n" + "=" * 100)
print("TEST 3: Overvix Regime Direction (VIX Trend)")
print("  Since overvix data only available Mar 17-18, use VIX level trend instead")
print("  Test: Block longs when VIX was rising (higher than previous day)")
print("=" * 100)

# Build daily average VIX
daily_vix = defaultdict(list)
for t in trades:
    if t.get("vix") is not None:
        daily_vix[t["trade_date"]].append(t["vix"])

avg_vix = {}
for d, vixes in sorted(daily_vix.items()):
    avg_vix[d] = sum(vixes) / len(vixes)

# Determine VIX trend (rising/falling)
sorted_dates = sorted(avg_vix.keys())
vix_rising = set()
vix_falling = set()
for i, d in enumerate(sorted_dates):
    if i == 0:
        continue
    prev = sorted_dates[i - 1]
    if avg_vix[d] > avg_vix[prev] + 0.5:  # Rising by >0.5
        vix_rising.add(d)
    elif avg_vix[d] < avg_vix[prev] - 0.5:  # Falling by >0.5
        vix_falling.add(d)

print(f"  VIX rising days: {len(vix_rising)}, falling days: {len(vix_falling)}")
print(f"  Rising: {sorted(vix_rising)}")
print(f"  Falling: {sorted(vix_falling)}")

# 3a: Block longs on VIX-rising days
test3a_trades = [
    t for t in trades
    if not (t["trade_date"] in vix_rising and t["direction"] in ("long", "bullish"))
]
test3a = compute_stats(test3a_trades, "3a: Block longs on VIX-rising days")
print_stats(test3a)
compare(baseline_all, test3a, "Block longs VIX rising")

# 3b: Block ALL trades on VIX-rising days
test3b_trades = [t for t in trades if t["trade_date"] not in vix_rising]
test3b = compute_stats(test3b_trades, "3b: Block ALL trades on VIX-rising days")
print_stats(test3b)
compare(baseline_all, test3b, "Block all VIX rising")

# Per VIX-rising day breakdown
print("\n  Per VIX-rising day breakdown:")
for d in sorted(vix_rising):
    day_trades = [t for t in trades if t["trade_date"] == d]
    if day_trades:
        day_pnl = sum(t["outcome_pnl"] for t in day_trades)
        wr = len([t for t in day_trades if t["outcome"] == "WIN"]) / len(day_trades) * 100
        print(f"    {d}: VIX {avg_vix[d]:.1f} (prev {avg_vix.get(sorted_dates[sorted_dates.index(d)-1], 0):.1f}), {len(day_trades)} trades, WR {wr:.0f}%, PnL {day_pnl:+.1f}")

# 3c: Block longs when VIX > 24 AND rising
test3c_trades = [
    t for t in trades
    if not (
        t["trade_date"] in vix_rising
        and t.get("vix") is not None
        and t["vix"] > 24
        and t["direction"] in ("long", "bullish")
    )
]
test3c = compute_stats(test3c_trades, "3c: Block longs when VIX>24 AND rising")
print_stats(test3c)
compare(baseline_all, test3c, "Block longs VIX>24+rising")

print("\n" + "=" * 100)
print("TEST 4: Friday (CPF) Risk Reduction")
print("  Block or reduce trades on Fridays (DOW=5)")
print("=" * 100)

# 4a: Block ALL Friday trades
friday_trades = [t for t in trades if t["dow"] == 5]
non_friday = [t for t in trades if t["dow"] != 5]

if friday_trades:
    fri_stats = compute_stats(friday_trades, "Friday trades only")
    print_stats(fri_stats)

    test4a = compute_stats(non_friday, "4a: Block ALL Friday trades")
    print_stats(test4a)
    compare(baseline_all, test4a, "Block Fridays")
else:
    print("  No Friday trades in dataset")

# 4b: Block Friday LONGS only (shorts may work on opex Fridays)
friday_longs = [t for t in trades if t["dow"] == 5 and t["direction"] in ("long", "bullish")]
test4b_trades = [t for t in trades if not (t["dow"] == 5 and t["direction"] in ("long", "bullish"))]
if friday_longs:
    test4b = compute_stats(test4b_trades, "4b: Block Friday longs only")
    print_stats(test4b)
    compare(baseline_all, test4b, "Block Friday longs")

# Day-of-week breakdown
print("\n  Day-of-week breakdown:")
dow_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
for dow in sorted(set(t["dow"] for t in trades)):
    dt = [t for t in trades if t["dow"] == dow]
    wins = len([t for t in dt if t["outcome"] == "WIN"])
    losses = len([t for t in dt if t["outcome"] == "LOSS"])
    pnl = sum(t["outcome_pnl"] for t in dt)
    wr = wins / len(dt) * 100 if dt else 0
    # Our dow is from EXTRACT(DOW) which is 0=Sun, 1=Mon, ..., 5=Fri, 6=Sat
    name = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}.get(dow, "?")
    print(f"    {name}: {len(dt):3d} trades, {wins}W/{losses}L, WR {wr:.0f}%, PnL {pnl:+.1f}, avg {pnl/len(dt):+.1f}/trade")

print("\n" + "=" * 100)
print("TEST 5: Combined Filters")
print("  Test best combinations from above")
print("=" * 100)

# 5a: FOMC gate + Sidial longs block
test5a_trades = [
    t for t in trades
    if t["trade_date"] not in FOMC_DECISION_DAYS
    and not (t.get("paradigm", "") in sidial_types and t["direction"] in ("long", "bullish"))
]
test5a = compute_stats(test5a_trades, "5a: FOMC gate + block Sidial longs")
print_stats(test5a)
compare(baseline_all, test5a, "FOMC + Sidial longs")

# 5b: Major events + block longs on VIX-rising
test5b_trades = [
    t for t in trades
    if t["trade_date"] not in MAJOR_USD_EVENT_DAYS
    and not (t["trade_date"] in vix_rising and t["direction"] in ("long", "bullish"))
]
test5b = compute_stats(test5b_trades, "5b: Major events gate + block longs VIX-rising")
print_stats(test5b)
compare(baseline_all, test5b, "Events + VIX-rising longs")

# 5c: FOMC + Sidial longs + Friday longs
test5c_trades = [
    t for t in trades
    if t["trade_date"] not in FOMC_DECISION_DAYS
    and not (t.get("paradigm", "") in sidial_types and t["direction"] in ("long", "bullish"))
    and not (t["dow"] == 5 and t["direction"] in ("long", "bullish"))
]
test5c = compute_stats(test5c_trades, "5c: FOMC + Sidial longs + Friday longs")
print_stats(test5c)
compare(baseline_all, test5c, "FOMC + Sidial longs + Fri longs")

# 5d: Aggressive — all events + Sidial block + VIX-rising longs + Friday longs
test5d_trades = [
    t for t in trades
    if t["trade_date"] not in MAJOR_USD_EVENT_DAYS
    and t.get("paradigm", "") not in sidial_types
    and not (t["trade_date"] in vix_rising and t["direction"] in ("long", "bullish"))
    and not (t["dow"] == 5 and t["direction"] in ("long", "bullish"))
]
test5d = compute_stats(test5d_trades, "5d: All events + no Sidial + VIX-rising + Fri longs")
print_stats(test5d)
compare(baseline_all, test5d, "Kitchen sink")

print("\n" + "=" * 100)
print("SUPPLEMENTARY: Hour-of-day analysis")
print("=" * 100)

for h in range(9, 17):
    ht = [t for t in trades if t["hour"] == h]
    if ht:
        wins = len([t for t in ht if t["outcome"] == "WIN"])
        losses = len([t for t in ht if t["outcome"] == "LOSS"])
        pnl = sum(t["outcome_pnl"] for t in ht)
        wr = wins / len(ht) * 100 if ht else 0
        print(f"  {h:02d}:xx  {len(ht):3d} trades, {wins}W/{losses}L, WR {wr:.0f}%, PnL {pnl:+.1f}, avg {pnl/len(ht):+.1f}/trade")

print("\n" + "=" * 100)
print("SUPPLEMENTARY: VIX bucket analysis")
print("=" * 100)

vix_buckets = [(0, 18), (18, 20), (20, 22), (22, 24), (24, 26), (26, 28), (28, 32)]
for lo, hi in vix_buckets:
    bt = [t for t in trades if t.get("vix") is not None and lo <= t["vix"] < hi]
    if bt:
        wins = len([t for t in bt if t["outcome"] == "WIN"])
        losses = len([t for t in bt if t["outcome"] == "LOSS"])
        pnl = sum(t["outcome_pnl"] for t in bt)
        wr = wins / len(bt) * 100 if bt else 0
        print(f"  VIX {lo}-{hi}: {len(bt):3d} trades, {wins}W/{losses}L, WR {wr:.0f}%, PnL {pnl:+.1f}, avg {pnl/len(bt):+.1f}/trade")

    # Longs vs shorts in this bucket
    longs = [t for t in bt if t["direction"] in ("long", "bullish")]
    shorts = [t for t in bt if t["direction"] in ("short", "bearish")]
    if longs:
        lpnl = sum(t["outcome_pnl"] for t in longs)
        lwr = len([t for t in longs if t["outcome"] == "WIN"]) / len(longs) * 100
        print(f"         Longs: {len(longs)} trades, WR {lwr:.0f}%, PnL {lpnl:+.1f}")
    if shorts:
        spnl = sum(t["outcome_pnl"] for t in shorts)
        swr = len([t for t in shorts if t["outcome"] == "WIN"]) / len(shorts) * 100
        print(f"         Shorts: {len(shorts)} trades, WR {swr:.0f}%, PnL {spnl:+.1f}")

print("\n" + "=" * 100)
print("SUPPLEMENTARY: Paradigm analysis (all paradigms)")
print("=" * 100)

paradigm_groups = defaultdict(list)
for t in trades:
    p = t.get("paradigm", "None") or "None"
    paradigm_groups[p].append(t)

for p in sorted(paradigm_groups.keys(), key=lambda x: sum(t["outcome_pnl"] for t in paradigm_groups[x]), reverse=True):
    pt = paradigm_groups[p]
    wins = len([t for t in pt if t["outcome"] == "WIN"])
    losses = len([t for t in pt if t["outcome"] == "LOSS"])
    pnl = sum(t["outcome_pnl"] for t in pt)
    wr = wins / len(pt) * 100 if pt else 0
    print(f"  {p:20s}: {len(pt):3d} trades, {wins}W/{losses}L, WR {wr:.0f}%, PnL {pnl:+.1f}, avg {pnl/len(pt):+.1f}/trade")

print("\n\nDone.")
