"""Backtest: pinning regime filter for eval-eligible trades.

Tests whether blocking trades in the last 2 hours (14:00-16:00 ET)
improves results, and whether proximity to max +GEX makes it worse.
"""
import os, sys
from collections import defaultdict
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# ── 1. Get all eval-eligible trades with alignment filter ──
trades = c.execute(text("""
    SELECT sl.id,
           sl.ts AT TIME ZONE 'America/New_York' as ts_et,
           to_char(sl.ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t,
           sl.ts::date as trade_date,
           sl.setup_name, sl.direction, sl.grade, sl.score, sl.spot,
           sl.outcome_result, sl.outcome_pnl,
           sl.outcome_max_profit, sl.outcome_max_loss,
           sl.greek_alignment
    FROM setup_log sl
    WHERE sl.grade != 'LOG'
      AND sl.setup_name IN ('Skew Charm', 'DD Exhaustion', 'Paradigm Reversal', 'AG Short')
      AND ABS(COALESCE(sl.greek_alignment, 0)) >= 3
      AND sl.outcome_result IS NOT NULL
    ORDER BY sl.ts
""")).fetchall()

print("=" * 100)
print("PINNING REGIME BACKTEST - Eval-Eligible Trades")
print("Setups: Skew Charm, DD Exhaustion, Paradigm Reversal, AG Short")
print("Greek filter: |alignment| >= 3")
print("=" * 100)

# ── 2. Split by time buckets ──
def bucket(time_str):
    h, m = int(time_str[:2]), int(time_str[3:5])
    mins = h * 60 + m
    if mins < 11 * 60:     return "09:30-11:00"
    elif mins < 12 * 60:   return "11:00-12:00"
    elif mins < 13 * 60:   return "12:00-13:00"
    elif mins < 14 * 60:   return "13:00-14:00"
    elif mins < 15 * 60:   return "14:00-15:00"
    else:                   return "15:00-16:00"

buckets = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "n": 0, "trades": []})
all_stats = {"w": 0, "l": 0, "pnl": 0, "n": 0}
late_stats = {"w": 0, "l": 0, "pnl": 0, "n": 0}  # 14:00+
early_stats = {"w": 0, "l": 0, "pnl": 0, "n": 0}  # before 14:00

for t in trades:
    res = t[9]
    pnl = float(t[10]) if t[10] is not None else 0
    time_str = t[2]
    b = bucket(time_str)
    is_win = res == 'WIN' or (res == 'EXPIRED' and pnl > 0)
    is_loss = res == 'LOSS' or (res == 'EXPIRED' and pnl < 0)

    buckets[b]["n"] += 1
    buckets[b]["pnl"] += pnl
    if is_win: buckets[b]["w"] += 1
    elif is_loss: buckets[b]["l"] += 1

    all_stats["n"] += 1
    all_stats["pnl"] += pnl
    if is_win: all_stats["w"] += 1
    elif is_loss: all_stats["l"] += 1

    h = int(time_str[:2])
    if h >= 14:
        late_stats["n"] += 1
        late_stats["pnl"] += pnl
        if is_win: late_stats["w"] += 1
        elif is_loss: late_stats["l"] += 1
    else:
        early_stats["n"] += 1
        early_stats["pnl"] += pnl
        if is_win: early_stats["w"] += 1
        elif is_loss: early_stats["l"] += 1

print("\n-- TIME BUCKET ANALYSIS --")
print("%-15s %5s %5s %5s %6s %8s %8s" % ("Period", "Total", "Wins", "Loss", "WR%", "PnL", "Avg PnL"))
print("-" * 65)
for b in sorted(buckets.keys()):
    s = buckets[b]
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    avg = s["pnl"] / s["n"] if s["n"] > 0 else 0
    print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % (b, s["n"], s["w"], s["l"], wr, s["pnl"], avg))

print("-" * 65)
wr_all = all_stats["w"] / (all_stats["w"] + all_stats["l"]) * 100 if (all_stats["w"] + all_stats["l"]) > 0 else 0
print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % ("ALL", all_stats["n"], all_stats["w"], all_stats["l"], wr_all, all_stats["pnl"], all_stats["pnl"]/all_stats["n"] if all_stats["n"] else 0))

# ── 3. Before 14:00 vs 14:00+ comparison ──
print("\n-- EARLY vs LATE COMPARISON --")
print("%-20s %5s %5s %5s %6s %8s %8s" % ("Period", "Total", "Wins", "Loss", "WR%", "PnL", "Avg PnL"))
print("-" * 65)
for label, s in [("Before 14:00 ET", early_stats), ("14:00-16:00 ET", late_stats)]:
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    avg = s["pnl"] / s["n"] if s["n"] > 0 else 0
    print("%-20s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % (label, s["n"], s["w"], s["l"], wr, s["pnl"], avg))

# ── 4. Per-date breakdown for late trades ──
print("\n-- LATE TRADES (14:00+) BY DATE --")
date_late = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "n": 0})
for t in trades:
    h = int(t[2][:2])
    if h < 14:
        continue
    d = str(t[3])
    res = t[9]
    pnl = float(t[10]) if t[10] is not None else 0
    is_win = res == 'WIN' or (res == 'EXPIRED' and pnl > 0)
    is_loss = res == 'LOSS' or (res == 'EXPIRED' and pnl < 0)
    date_late[d]["n"] += 1
    date_late[d]["pnl"] += pnl
    if is_win: date_late[d]["w"] += 1
    elif is_loss: date_late[d]["l"] += 1

print("%-12s %5s %5s %5s %6s %8s" % ("Date", "Total", "Wins", "Loss", "WR%", "PnL"))
print("-" * 50)
for d in sorted(date_late.keys()):
    s = date_late[d]
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    print("%-12s %5d %5d %5d %5.0f%% %+8.1f" % (d, s["n"], s["w"], s["l"], wr, s["pnl"]))

# ── 5. Per-setup breakdown for late trades ──
print("\n-- LATE TRADES (14:00+) BY SETUP --")
setup_late = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "n": 0})
for t in trades:
    h = int(t[2][:2])
    if h < 14:
        continue
    res = t[9]
    pnl = float(t[10]) if t[10] is not None else 0
    is_win = res == 'WIN' or (res == 'EXPIRED' and pnl > 0)
    is_loss = res == 'LOSS' or (res == 'EXPIRED' and pnl < 0)
    setup_late[t[4]]["n"] += 1
    setup_late[t[4]]["pnl"] += pnl
    if is_win: setup_late[t[4]]["w"] += 1
    elif is_loss: setup_late[t[4]]["l"] += 1

print("%-20s %5s %5s %5s %6s %8s" % ("Setup", "Total", "Wins", "Loss", "WR%", "PnL"))
print("-" * 55)
for s_name in sorted(setup_late.keys()):
    s = setup_late[s_name]
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    print("%-20s %5d %5d %5d %5.0f%% %+8.1f" % (s_name, s["n"], s["w"], s["l"], wr, s["pnl"]))

# ── 6. Test different cutoff times ──
print("\n-- CUTOFF TIME OPTIMIZATION --")
print("If we stop taking trades after X time, what's the impact?")
print("%-12s %6s %5s %5s %6s %8s %10s" % ("Cutoff", "Taken", "Wins", "Loss", "WR%", "PnL", "Blocked PnL"))
print("-" * 70)
for cutoff_h, cutoff_m in [(13,0),(13,30),(14,0),(14,30),(15,0),(15,30),(16,0)]:
    taken = {"w":0,"l":0,"pnl":0,"n":0}
    blocked = {"pnl":0,"n":0}
    for t in trades:
        h, m = int(t[2][:2]), int(t[2][3:5])
        res = t[9]
        pnl = float(t[10]) if t[10] is not None else 0
        is_win = res == 'WIN' or (res == 'EXPIRED' and pnl > 0)
        is_loss = res == 'LOSS' or (res == 'EXPIRED' and pnl < 0)
        if h * 60 + m < cutoff_h * 60 + cutoff_m:
            taken["n"] += 1
            taken["pnl"] += pnl
            if is_win: taken["w"] += 1
            elif is_loss: taken["l"] += 1
        else:
            blocked["n"] += 1
            blocked["pnl"] += pnl
    wr = taken["w"] / (taken["w"] + taken["l"]) * 100 if (taken["w"] + taken["l"]) > 0 else 0
    print("%02d:%02d ET    %6d %5d %5d %5.0f%% %+8.1f   %+8.1f (%d blocked)" % (
        cutoff_h, cutoff_m, taken["n"], taken["w"], taken["l"], wr, taken["pnl"], blocked["pnl"], blocked["n"]))

# ── 7. Direction analysis for late trades ──
print("\n-- LATE TRADES (14:00+) BY DIRECTION --")
dir_late = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "n": 0})
for t in trades:
    h = int(t[2][:2])
    if h < 14:
        continue
    res = t[9]
    pnl = float(t[10]) if t[10] is not None else 0
    is_win = res == 'WIN' or (res == 'EXPIRED' and pnl > 0)
    is_loss = res == 'LOSS' or (res == 'EXPIRED' and pnl < 0)
    d = t[5]
    align = t[13]
    # Check if direction matches alignment
    aligned = (d in ('long','bullish') and align and align > 0) or (d in ('short','bearish') and align and align < 0)
    key = f"{d} (align={'WITH' if aligned else 'AGAINST'})"
    dir_late[key]["n"] += 1
    dir_late[key]["pnl"] += pnl
    if is_win: dir_late[key]["w"] += 1
    elif is_loss: dir_late[key]["l"] += 1

print("%-30s %5s %5s %5s %6s %8s" % ("Direction", "Total", "Wins", "Loss", "WR%", "PnL"))
print("-" * 60)
for d in sorted(dir_late.keys()):
    s = dir_late[d]
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    print("%-30s %5d %5d %5d %5.0f%% %+8.1f" % (d, s["n"], s["w"], s["l"], wr, s["pnl"]))

print("\n-- SUMMARY --")
saved = late_stats["pnl"] if late_stats["pnl"] < 0 else 0
print("Blocking after 14:00 ET would save %+.1f pts (if late trades net negative)" % (-late_stats["pnl"]))
print("Early-only PnL: %+.1f pts across %d trades" % (early_stats["pnl"], early_stats["n"]))
print("Late PnL: %+.1f pts across %d trades" % (late_stats["pnl"], late_stats["n"]))
avg_early = early_stats["pnl"] / early_stats["n"] if early_stats["n"] else 0
avg_late = late_stats["pnl"] / late_stats["n"] if late_stats["n"] else 0
print("Avg PnL per trade: early=%+.2f, late=%+.2f" % (avg_early, avg_late))

c.close()
