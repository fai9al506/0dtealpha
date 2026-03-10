"""
E2T sizing analysis: How many MES to pass fastest without blowing daily limit?
"""
import json
from collections import defaultdict
from datetime import datetime, timedelta

with open("C:/Users/Faisa/AppData/Local/Temp/trade_data.json") as f:
    trades = json.load(f)

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
            "ES Absorption": 20}.get(t["setup"], 30)

PRIORITY = {"Skew Charm": 1, "Paradigm Reversal": 2, "AG Short": 3,
            "DD Exhaustion": 4, "ES Absorption": 5, "GEX Long": 6, "BofA Scalp": 7}

eval_setups = {"Paradigm Reversal", "DD Exhaustion", "AG Short", "Skew Charm"}
n_days = len(by_date)

# ===== Build daily PnL for RECOMMENDED config (Proven + align >= +1) =====
daily_pts = []
daily_trades_detail = []
for date in sorted(by_date.keys()):
    day_trades = [t for t in by_date[date] if t["setup"] in eval_setups]
    taken = []
    pos_free = None
    for t in sorted(day_trades, key=lambda x: (x["ts"], PRIORITY.get(x["setup"], 99))):
        ga = t.get("greek_alignment")
        if ga is None or ga < 1:
            continue
        tt = datetime.fromisoformat(t["ts"])
        te = tt + timedelta(minutes=estimate_hold(t))
        if pos_free is None or tt >= pos_free:
            taken.append(t)
            pos_free = te
    day_pnl = sum(t["pnl"] for t in taken)
    daily_pts.append(day_pnl)
    daily_trades_detail.append(taken)

# ===== Also build for CURRENT config =====
daily_pts_cur = []
for date in sorted(by_date.keys()):
    day_trades = [t for t in by_date[date] if t["setup"] in eval_setups]
    taken = []
    pos_free = None
    for t in sorted(day_trades, key=lambda x: (x["ts"], PRIORITY.get(x["setup"], 99))):
        tt = datetime.fromisoformat(t["ts"])
        te = tt + timedelta(minutes=estimate_hold(t))
        if pos_free is None or tt >= pos_free:
            taken.append(t)
            pos_free = te
    daily_pts_cur.append(sum(t["pnl"] for t in taken))

# E2T 50K TCP rules
DAILY_LOSS_LIMIT = 1100
DAILY_LOSS_BUFFER = 100
EFFECTIVE_DAILY_LIMIT = DAILY_LOSS_LIMIT - DAILY_LOSS_BUFFER  # $1,000
TRAILING_DD = 2000
PROFIT_TARGET = 3000
MES_PER_PT = 5
MAX_STOP = 12
BE_TRIGGER = 5

print("=" * 110)
print("E2T SIZING ANALYSIS: How many MES to pass fastest without blowing daily limit?")
print("=" * 110)
print()

# Daily PnL distribution
print("Daily PnL distribution (recommended config, SPX pts):")
for i, (pts, tl) in enumerate(zip(daily_pts, daily_trades_detail)):
    n = len(tl)
    worst = min((t["pnl"] for t in tl), default=0)
    tag = ""
    if pts < -15: tag = " *** BAD"
    elif pts < 0: tag = " * red"
    print("  Day %2d: %+6.1f pts (%d trades, worst single: %+.1f)%s" % (i+1, pts, n, worst, tag))

print()
worst_day = min(daily_pts)
best_day = max(daily_pts)
avg_day = sum(daily_pts) / len(daily_pts)
print("Worst day: %+.1f pts | Best day: %+.1f pts | Avg: %+.1f pts" % (worst_day, best_day, avg_day))

all_taken = [t for day in daily_trades_detail for t in day]
if all_taken:
    worst_trade = min(t["pnl"] for t in all_taken)
    print("Worst single trade: %+.1f pts" % worst_trade)
print()

# ===== SIZING TABLE =====
print("=" * 110)
print("SIZING SCENARIOS (Recommended config: Proven + align >= +1)")
print("=" * 110)
print()

header = "%-5s %-7s %-11s %-11s %-10s %-10s %-10s %-11s %-8s" % (
    "QTY", "$/pt", "Worst Day$", "Limit$", "Blow Days", "Avg$/day", "Days Pass", "Max Trail$", "Safe?")
print(header)
print("-" * 105)

results = []
for qty in [4, 6, 8, 10, 12, 14, 16, 20]:
    dpp = qty * MES_PER_PT
    daily_dollars = [p * dpp for p in daily_pts]

    blow_days = sum(1 for d in daily_dollars if d < -EFFECTIVE_DAILY_LIMIT)
    worst_day_d = min(daily_dollars)
    avg_daily_d = sum(daily_dollars) / len(daily_dollars)

    # Simulate equity curve with daily limit cap
    cum = 0
    peak = 0
    max_trail = 0
    days_to_pass = None
    for i, d in enumerate(daily_dollars):
        actual = max(d, -EFFECTIVE_DAILY_LIMIT)
        cum += actual
        peak = max(peak, cum)
        trail = peak - cum
        max_trail = max(max_trail, trail)
        if cum >= PROFIT_TARGET and days_to_pass is None:
            days_to_pass = i + 1

    days_str = str(days_to_pass) if days_to_pass else ">21"

    if blow_days > 1:
        safe = "NO"
    elif blow_days == 1:
        safe = "RISKY"
    elif max_trail >= TRAILING_DD * 0.8:
        safe = "TIGHT"
    else:
        safe = "YES"

    results.append((qty, dpp, worst_day_d, blow_days, avg_daily_d, days_to_pass, max_trail, safe))

    print("%-5d $%-6d $%-10.0f $%-10d %-10d $%-9.0f %-10s $%-10.0f %-8s" % (
        qty, dpp, worst_day_d, EFFECTIVE_DAILY_LIMIT, blow_days, avg_daily_d, days_str, max_trail, safe))

print()
print("KEY:")
print("  Blow Days = days where loss > daily limit ($1,000) = ACCOUNT VIOLATION")
print("  Max Trail$ = max trailing drawdown hit (E2T limit: $2,000)")
print("  Days Pass = trading days to reach $3,000 profit target")
print("  Safe = YES (no violations), TIGHT (close to trail DD), RISKY (1 blow day), NO (multiple violations)")
print()

# ===== PER-TRADE RISK =====
print("=" * 110)
print("PER-TRADE RISK AT KEY SIZES")
print("=" * 110)
print()

print("%-6s %-10s %-14s %-14s %-14s %-18s %-16s" % (
    "QTY", "$/pt", "Max Loss/Trd", "Typical Win", "2 Max Losses", "Losses to Limit", "Avg Daily"))
print("-" * 100)

for qty in [6, 8, 10, 12, 14, 16]:
    dpp = qty * MES_PER_PT
    max_loss = MAX_STOP * dpp
    typical_win = 10 * dpp
    two_losses = 2 * max_loss
    losses_to_limit = EFFECTIVE_DAILY_LIMIT / max_loss
    avg_d = sum(p * dpp for p in daily_pts) / len(daily_pts)

    print("%-6d $%-9d $%-13d $%-13d $%-13d %-18.1f $%-15.0f" % (
        qty, dpp, max_loss, typical_win, two_losses, losses_to_limit, avg_d))

print()

# ===== WORST-CASE SEQUENCES =====
print("=" * 110)
print("WORST-CASE CONSECUTIVE DAY SEQUENCES")
print("=" * 110)
print()

for window in [2, 3, 5]:
    worst_seq = float("inf")
    worst_start = 0
    for i in range(len(daily_pts) - window + 1):
        s = sum(daily_pts[i:i+window])
        if s < worst_seq:
            worst_seq = s
            worst_start = i

    print("Worst %d-day streak: %+.1f pts (days %d-%d)" % (window, worst_seq, worst_start+1, worst_start+window))
    for qty in [8, 10, 12, 14]:
        dollar_loss = worst_seq * qty * MES_PER_PT
        pct_dd = abs(dollar_loss) / TRAILING_DD * 100 if dollar_loss < 0 else 0
        print("  @ %d MES: $%+,.0f (%d%% of $2K trail DD)" % (qty, dollar_loss, pct_dd))
    print()

# ===== ALSO COMPARE: current config sizing =====
print("=" * 110)
print("COMPARISON: Current config (no alignment gate) sizing")
print("=" * 110)
print()

print("%-5s %-10s %-11s %-10s %-10s %-10s %-8s" % (
    "QTY", "Avg$/day", "Worst Day$", "Blow Days", "Days Pass", "Max Trail$", "Safe?"))
print("-" * 75)

for qty in [8, 10, 12, 14]:
    dpp = qty * MES_PER_PT
    daily_dollars = [p * dpp for p in daily_pts_cur]
    blow_days = sum(1 for d in daily_dollars if d < -EFFECTIVE_DAILY_LIMIT)
    worst_day_d = min(daily_dollars)
    avg_daily_d = sum(daily_dollars) / len(daily_dollars)

    cum = 0
    peak = 0
    max_trail = 0
    days_to_pass = None
    for i, d in enumerate(daily_dollars):
        actual = max(d, -EFFECTIVE_DAILY_LIMIT)
        cum += actual
        peak = max(peak, cum)
        trail = peak - cum
        max_trail = max(max_trail, trail)
        if cum >= PROFIT_TARGET and days_to_pass is None:
            days_to_pass = i + 1

    days_str = str(days_to_pass) if days_to_pass else ">21"

    if blow_days > 1: safe = "NO"
    elif blow_days == 1: safe = "RISKY"
    elif max_trail >= TRAILING_DD * 0.8: safe = "TIGHT"
    else: safe = "YES"

    print("%-5d $%-9.0f $%-10.0f %-10d %-10s $%-9.0f %-8s" % (
        qty, avg_daily_d, worst_day_d, blow_days, days_str, max_trail, safe))

print()
print("=" * 110)
print("RECOMMENDATION")
print("=" * 110)
print()
print("With RECOMMENDED config (Proven + align >= +1):")
print("  Worst day = %+.1f pts" % min(daily_pts))
print("  At 10 MES: worst day = $%+,.0f (within $1,000 limit)" % (min(daily_pts) * 50))
print("  At 12 MES: worst day = $%+,.0f" % (min(daily_pts) * 60))
print("  At 14 MES: worst day = $%+,.0f" % (min(daily_pts) * 70))
print()
print("With CURRENT config (no alignment gate):")
print("  Worst day = %+.1f pts" % min(daily_pts_cur))
print("  At 10 MES: worst day = $%+,.0f" % (min(daily_pts_cur) * 50))
print("  At 12 MES: worst day = $%+,.0f" % (min(daily_pts_cur) * 60))
