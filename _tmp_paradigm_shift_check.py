"""Test: are trades fired soon after intraday paradigm shift more dangerous?

For each trade, compute time since LAST paradigm change. If recent shifts predict
worse outcomes, that's a different rule than time-of-day.
"""
import psycopg2
from collections import defaultdict

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB); cur = conn.cursor()

# Pull paradigm history per day from setup_log (consecutive different paradigms)
# Then for each trade, compute min-since-last-shift on the same day.
cur.execute("""
    WITH per_log AS (
        SELECT id, paradigm, ts, direction, setup_name, outcome_pnl,
               (ts AT TIME ZONE 'America/New_York')::date AS et_date,
               (ts AT TIME ZONE 'America/New_York') AS et_ts
        FROM setup_log
        WHERE ts >= '2026-02-01'
          AND direction IN ('long', 'bullish')
          AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'ES Absorption')
          AND outcome_pnl IS NOT NULL
          AND paradigm IS NOT NULL
    )
    SELECT id, paradigm, et_date, et_ts, direction, setup_name, outcome_pnl
    FROM per_log
    ORDER BY et_date, et_ts
""")
rows = cur.fetchall()
print(f"Long trades with paradigm: {len(rows)}\n")

# Group by date and compute paradigm change timestamps
by_date = defaultdict(list)
for r in rows:
    lid, para, et_date, et_ts, direction, setup, pnl = r
    by_date[et_date].append((lid, para, et_ts, float(pnl), setup))

# For each trade, find time-since-last-paradigm-change
# We need to also use chain_snapshots / volland_snapshots for the actual
# paradigm timeline (not just from setup_log which only has trades).
# But as a quick approximation, use setup_log: paradigm shifts are detected
# when consecutive trades on same day have different paradigms.
all_results = []
for et_date, trades in by_date.items():
    trades.sort(key=lambda t: t[2])
    prev_para = None
    last_shift_ts = None
    for lid, para, et_ts, pnl, setup in trades:
        if prev_para is None:
            last_shift_ts = et_ts
        elif para != prev_para:
            last_shift_ts = et_ts
        min_since_shift = 0 if last_shift_ts is None else (et_ts - last_shift_ts).total_seconds() / 60
        all_results.append((lid, para, pnl, min_since_shift, setup, et_ts))
        prev_para = para

# Bucket by minutes-since-shift
buckets = {
    "0-15min": [],
    "15-60min": [],
    "1-3hr": [],
    "3hr+": [],
}
for lid, para, pnl, mins, setup, ts in all_results:
    if mins < 15: buckets["0-15min"].append((lid, para, pnl, setup))
    elif mins < 60: buckets["15-60min"].append((lid, para, pnl, setup))
    elif mins < 180: buckets["1-3hr"].append((lid, para, pnl, setup))
    else: buckets["3hr+"].append((lid, para, pnl, setup))

def fmt(trades):
    n = len(trades)
    if n == 0: return "n=0"
    pnls = [t[2] for t in trades]
    wr = sum(1 for p in pnls if p > 0)/n*100
    total = sum(pnls)
    return f"n={n:4d} WR={wr:5.1f}% total={total:+8.1f}pt mean={total/n:+5.2f}pt"

print("LONGS by time-since-last-paradigm-shift (intraday):")
for label in ("0-15min", "15-60min", "1-3hr", "3hr+"):
    print(f"  {label:>10s}  {fmt(buckets[label])}")

# Same but only for GEX-TARGET
print("\nGEX-TARGET LONGS by time-since-shift:")
for label, items in buckets.items():
    gt = [t for t in items if t[1] == "GEX-TARGET"]
    print(f"  {label:>10s}  {fmt(gt)}")

# Today's pattern: paradigm shift TO GEX-TARGET, then losses
# Check: when paradigm SHIFTS TO GEX-TARGET specifically, are longs that fire
# in the first 60 min worse than after?
print("\nLongs fired within 60 min of SHIFT TO GEX-TARGET (any setup):")
shift_to_gt_60min = []
for et_date, trades in by_date.items():
    trades.sort(key=lambda t: t[2])
    prev_para = None
    last_shift_to_gt_ts = None
    for lid, para, et_ts, pnl, setup in trades:
        if prev_para is not None and para == "GEX-TARGET" and prev_para != "GEX-TARGET":
            last_shift_to_gt_ts = et_ts
        if para == "GEX-TARGET" and last_shift_to_gt_ts is not None:
            mins_since = (et_ts - last_shift_to_gt_ts).total_seconds() / 60
            if mins_since <= 60:
                shift_to_gt_60min.append((lid, pnl, setup, mins_since))
        prev_para = para
print(f"  {fmt([(t[0], None, t[1], t[2]) for t in shift_to_gt_60min])}")
print()
for t in shift_to_gt_60min:
    print(f"    lid={t[0]} {t[2]:22s} mins_since={t[3]:.1f}  pnl={t[1]:+.1f}")

conn.close()
