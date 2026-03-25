#!/usr/bin/env python3
"""Query the 16 concurrency-skipped SC trades from March 2026 real_trader simulation."""

import psycopg2, os
from datetime import datetime, timedelta, time as dtime, date
from collections import defaultdict
import zoneinfo

NY = zoneinfo.ZoneInfo("US/Eastern")
SL14_CUTOFF = date(2026, 3, 20)

DB_URL = os.environ.get("DATABASE_URL", "")
if not DB_URL:
    DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("""
    SELECT id, ts, setup_name, direction, grade, score, paradigm, spot,
           outcome_result, outcome_pnl, outcome_target_level, outcome_stop_level,
           outcome_max_profit, outcome_max_loss, outcome_first_event, outcome_elapsed_min,
           greek_alignment, charm_limit_entry, vix, overvix
    FROM setup_log
    WHERE setup_name = 'Skew Charm'
      AND ts >= '2026-03-01'::date
      AND ts < '2026-04-01'::date
    ORDER BY ts ASC
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
all_trades = [dict(zip(cols, r)) for r in rows]
conn.close()

for t in all_trades:
    t["ts_et"] = t["ts"].astimezone(NY)
    t["trade_date"] = t["ts_et"].date()
    t["time_et"] = t["ts_et"].time()
    # Use SL=14 throughout (Scenario B)
    if (t["trade_date"] < SL14_CUTOFF
            and t["outcome_result"] == "LOSS"
            and t["outcome_pnl"] == -20.0):
        t["pnl"] = -14.0
    else:
        t["pnl"] = t["outcome_pnl"] or 0.0


def passes_v11(t):
    grade = t["grade"]
    direction = t["direction"]
    paradigm = t["paradigm"]
    align = t["greek_alignment"] or 0
    time_et = t["time_et"]
    if grade in ("C", "LOG"):
        return False, "grade_blocked"
    if dtime(14, 30) <= time_et < dtime(15, 0):
        return False, "time_1430_1500"
    if time_et >= dtime(15, 30):
        return False, "time_1530+"
    is_long = direction.lower() in ("long", "bullish")
    if is_long:
        if align < 2:
            return False, "align_low"
        return True, "pass"
    else:
        if paradigm == "GEX-LIS":
            return False, "gex_lis_blocked"
        return True, "pass"


MAX_CONCURRENT_PER_DIR = 2


def est_close(t):
    fired = t["ts_et"]
    elapsed = t["outcome_elapsed_min"]
    if elapsed and elapsed > 0:
        return fired + timedelta(minutes=elapsed)
    out = t["outcome_result"]
    if out == "WIN":
        return fired + timedelta(minutes=30)
    elif out == "LOSS":
        return fired + timedelta(minutes=15)
    return fired.replace(hour=16, minute=0, second=0, microsecond=0)


def count_open(positions, is_long, at_time):
    c = 0
    for p in positions:
        p_long = p["direction"].lower() in ("long", "bullish")
        if p_long == is_long and p["fired_at"] <= at_time < p["close_at"]:
            c += 1
    return c


open_positions = []
skip_conc = []
taken = []

for t in all_trades:
    ok, reason = passes_v11(t)
    if not ok:
        continue

    fired = t["ts_et"]
    is_long = t["direction"].lower() in ("long", "bullish")

    if count_open(open_positions, is_long, fired) >= MAX_CONCURRENT_PER_DIR:
        skip_conc.append(t)
        continue

    close_at = est_close(t)
    open_positions.append({
        "id": t["id"], "direction": t["direction"],
        "fired_at": fired, "close_at": close_at,
    })
    taken.append(t)


print("Total SC trades in March DB:", len(all_trades))
print("Passed V11 filter:", len(taken) + len(skip_conc))
print("Taken (within concurrency cap):", len(taken))
print("Concurrency-skipped:", len(skip_conc))
print()

winners = [t for t in skip_conc if t["outcome_result"] == "WIN"]
losers = [t for t in skip_conc if t["outcome_result"] == "LOSS"]
expired = [t for t in skip_conc if t["outcome_result"] == "EXPIRED"]

total_pnl = sum(t["pnl"] for t in skip_conc)
win_pnl = sum(t["pnl"] for t in winners)
loss_pnl = sum(t["pnl"] for t in losers)
exp_pnl = sum(t["pnl"] for t in expired)

print("=" * 130)
print("ALL CONCURRENCY-SKIPPED TRADES (SL=14 throughout)")
print("=" * 130)

header = "{:>3s} {:>5s} {:>12s} {:>8s} {:>6s} {:>5s} {:>12s} {:>5s} {:>9s} {:>8s} {:>8s} {:>7s} {:>7s} {:>7s}".format(
    "#", "ID", "Date", "Time", "Dir", "Grade", "Paradigm", "Align", "Spot", "Outcome", "PnL", "MaxPrf", "MaxLss", "Elaps"
)
print(header)
print("-" * 130)

for i, t in enumerate(skip_conc, 1):
    mp = t["outcome_max_profit"] or 0
    ml = t["outcome_max_loss"] or 0
    el = t["outcome_elapsed_min"] or 0
    al = t["greek_alignment"] or 0
    line = "{:3d} {:5d} {:>12s} {:>8s} {:>6s} {:>5s} {:>12s} {:5d} {:9.2f} {:>8s} {:>+8.1f} {:7.1f} {:7.1f} {:7.0f}".format(
        i, t["id"], t["ts_et"].strftime("%Y-%m-%d"), t["ts_et"].strftime("%H:%M"),
        t["direction"], t["grade"], t["paradigm"], al,
        t["spot"], t["outcome_result"], t["pnl"],
        mp, ml, el
    )
    print(line)

print("-" * 130)
print()

print("--- WINNERS ({} trades) ---".format(len(winners)))
for i, t in enumerate(winners, 1):
    al = t["greek_alignment"] or 0
    mp = t["outcome_max_profit"] or 0
    el = t["outcome_elapsed_min"] or 0
    print("  {}. ID={} {} {} {} grade={} paradigm={} align={} pnl={:+.1f} maxprof={:.1f} elapsed={:.0f}min".format(
        i, t["id"], t["ts_et"].strftime("%m/%d %H:%M"), t["direction"], t["spot"],
        t["grade"], t["paradigm"], al, t["pnl"], mp, el
    ))

print()
print("--- LOSERS ({} trades) ---".format(len(losers)))
for i, t in enumerate(losers, 1):
    al = t["greek_alignment"] or 0
    ml = t["outcome_max_loss"] or 0
    el = t["outcome_elapsed_min"] or 0
    print("  {}. ID={} {} {} {} grade={} paradigm={} align={} pnl={:+.1f} maxloss={:.1f} elapsed={:.0f}min".format(
        i, t["id"], t["ts_et"].strftime("%m/%d %H:%M"), t["direction"], t["spot"],
        t["grade"], t["paradigm"], al, t["pnl"], ml, el
    ))

if expired:
    print()
    print("--- EXPIRED ({} trades) ---".format(len(expired)))
    for i, t in enumerate(expired, 1):
        al = t["greek_alignment"] or 0
        print("  {}. ID={} {} {} {} grade={} paradigm={} align={} pnl={:+.1f}".format(
            i, t["id"], t["ts_et"].strftime("%m/%d %H:%M"), t["direction"], t["spot"],
            t["grade"], t["paradigm"], al, t["pnl"]
        ))

print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)
print("Total skipped:     {}".format(len(skip_conc)))
print("  Winners:         {}  total PnL: {:+.1f} pts (${:+,.2f})".format(len(winners), win_pnl, win_pnl * 5))
print("  Losers:          {}  total PnL: {:+.1f} pts (${:+,.2f})".format(len(losers), loss_pnl, loss_pnl * 5))
print("  Expired:         {}  total PnL: {:+.1f} pts (${:+,.2f})".format(len(expired), exp_pnl, exp_pnl * 5))
print("  NET MISSED PnL:  {:+.1f} pts (${:+,.2f})".format(total_pnl, total_pnl * 5))
print("  Win Rate:        {:.1f}%".format(len(winners) / len(skip_conc) * 100 if skip_conc else 0))
print("  Avg per trade:   {:+.2f} pts".format(total_pnl / len(skip_conc) if skip_conc else 0))

# Also show which positions were blocking each skipped trade
print()
print("=" * 130)
print("BLOCKING CONTEXT: What was open when each trade was skipped")
print("=" * 130)
for i, t in enumerate(skip_conc, 1):
    fired = t["ts_et"]
    is_long = t["direction"].lower() in ("long", "bullish")
    blockers = []
    for p in open_positions:
        p_long = p["direction"].lower() in ("long", "bullish")
        if p_long == is_long and p["fired_at"] <= fired < p["close_at"]:
            blockers.append(p)
    blocker_ids = ", ".join("ID={}".format(b["id"]) for b in blockers)
    print("  #{} ID={} ({} {}) blocked by: [{}]".format(
        i, t["id"], t["ts_et"].strftime("%m/%d %H:%M"), t["direction"], blocker_ids
    ))
