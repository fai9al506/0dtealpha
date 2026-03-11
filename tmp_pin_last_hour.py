"""Backtest: last hour (15:00+ ET) vs rest, with finer granularity after 14:00"""
import os, sys
from collections import defaultdict
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# All trades (not just eval-eligible) to get bigger sample
trades = c.execute(text("""
    SELECT id, to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t,
           ts::date as trade_date,
           setup_name, direction, grade, spot,
           outcome_result, outcome_pnl, greek_alignment
    FROM setup_log
    WHERE grade != 'LOG' AND outcome_result IS NOT NULL
    ORDER BY ts
""")).fetchall()

# Also get eval-eligible only
eval_trades = [t for t in trades
    if t[3] in ('Skew Charm', 'DD Exhaustion', 'Paradigm Reversal', 'AG Short')
    and t[9] is not None and abs(t[9]) >= 3]

def analyze(label, data):
    print("\n" + "=" * 90)
    print(label)
    print("=" * 90)

    # Fine-grained time buckets for afternoon
    buckets = [
        ("09:30-11:00", 9*60+30, 11*60),
        ("11:00-13:00", 11*60, 13*60),
        ("13:00-14:00", 13*60, 14*60),
        ("14:00-14:30", 14*60, 14*60+30),
        ("14:30-15:00", 14*60+30, 15*60),
        ("15:00-15:30", 15*60, 15*60+30),
        ("15:30-16:00", 15*60+30, 16*60),
    ]

    print("\n%-15s %5s %5s %5s %6s %8s %8s" % ("Period", "Total", "Wins", "Loss", "WR%", "PnL", "Avg"))
    print("-" * 65)
    for bname, bstart, bend in buckets:
        w = l = n = 0
        pnl = 0.0
        for t in data:
            h, m = int(t[1][:2]), int(t[1][3:5])
            mins = h * 60 + m
            if mins < bstart or mins >= bend:
                continue
            res = t[7]
            p = float(t[8]) if t[8] is not None else 0
            n += 1
            pnl += p
            is_win = res == 'WIN' or (res == 'EXPIRED' and p > 0)
            is_loss = res == 'LOSS' or (res == 'EXPIRED' and p < 0)
            if is_win: w += 1
            elif is_loss: l += 1
        if n == 0:
            continue
        wr = w / (w + l) * 100 if (w + l) > 0 else 0
        avg = pnl / n
        print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % (bname, n, w, l, wr, pnl, avg))

    # Last hour vs rest
    print("\n--- Last Hour Comparison ---")
    for cutoff_label, cutoff_mins in [("Before 14:30", 14*60+30), ("Before 15:00", 15*60), ("All day", 24*60)]:
        w = l = n = 0
        pnl = 0.0
        for t in data:
            h, m = int(t[1][:2]), int(t[1][3:5])
            if h * 60 + m >= cutoff_mins:
                continue
            res = t[7]
            p = float(t[8]) if t[8] is not None else 0
            n += 1
            pnl += p
            is_win = res == 'WIN' or (res == 'EXPIRED' and p > 0)
            is_loss = res == 'LOSS' or (res == 'EXPIRED' and p < 0)
            if is_win: w += 1
            elif is_loss: l += 1
        wr = w / (w + l) * 100 if (w + l) > 0 else 0
        avg = pnl / n if n else 0
        print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % (cutoff_label, n, w, l, wr, pnl, avg))

    # Per-date last hour
    print("\n--- Last Hour (15:00+) Per Date ---")
    date_stats = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "n": 0})
    for t in data:
        h = int(t[1][:2])
        if h < 15:
            continue
        d = str(t[2])
        res = t[7]
        p = float(t[8]) if t[8] is not None else 0
        is_win = res == 'WIN' or (res == 'EXPIRED' and p > 0)
        is_loss = res == 'LOSS' or (res == 'EXPIRED' and p < 0)
        date_stats[d]["n"] += 1
        date_stats[d]["pnl"] += p
        if is_win: date_stats[d]["w"] += 1
        elif is_loss: date_stats[d]["l"] += 1

    print("%-12s %5s %5s %5s %6s %8s" % ("Date", "Total", "Wins", "Loss", "WR%", "PnL"))
    print("-" * 50)
    total_late_pnl = 0
    for d in sorted(date_stats.keys()):
        s = date_stats[d]
        wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
        print("%-12s %5d %5d %5d %5.0f%% %+8.1f" % (d, s["n"], s["w"], s["l"], wr, s["pnl"]))
        total_late_pnl += s["pnl"]
    winning_days = sum(1 for s in date_stats.values() if s["pnl"] > 0)
    losing_days = sum(1 for s in date_stats.values() if s["pnl"] <= 0)
    print("-" * 50)
    print("Last-hour: %d winning days / %d losing days, total %+.1f pts" % (
        winning_days, losing_days, total_late_pnl))

analyze("ALL TRADES (no filter)", trades)
analyze("EVAL-ELIGIBLE (Skew/DD/Para/AG, |align|>=3)", eval_trades)

c.close()
