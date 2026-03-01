"""
Deep Delta Decay Analysis
=========================
1. How DD exposure distributes across strikes (profile shape)
2. How the DD total (sum across strikes) correlates with price movement
3. Whether DD shifts predict price direction
4. DD as a magnet/pinning effect at peak DD strike
5. Time-of-day patterns
"""
import json, os, psycopg2, statistics, math
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict
import pytz

DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

NY = pytz.timezone("US/Eastern")
out = []
def p(s=""):
    out.append(str(s))

# ============================================================
# STEP 1: Load all DD snapshots with spot price
# ============================================================
p("=" * 120)
p("DEEP DELTA DECAY ANALYSIS")
p("=" * 120)

p("\n=== Loading DD exposure data ===")
cur.execute("""
    SELECT ts_utc, strike::float, value::float, current_price::float
    FROM volland_exposure_points
    WHERE greek = 'deltaDecay'
    ORDER BY ts_utc, strike
""")
raw_rows = cur.fetchall()
p(f"Total DD exposure rows: {len(raw_rows)}")

# Group by timestamp
snapshots = defaultdict(list)
snap_spots = {}
for ts, strike, value, spot in raw_rows:
    snapshots[ts].append((strike, value))
    snap_spots[ts] = spot

timestamps = sorted(snapshots.keys())
p(f"Total DD snapshots: {len(timestamps)}")
p(f"Date range: {timestamps[0]} to {timestamps[-1]}")

# ============================================================
# STEP 2: Per-snapshot analysis
# ============================================================
p("\n=== Per-Snapshot DD Profile ===")

snap_analysis = []
for ts in timestamps:
    points = snapshots[ts]
    spot = snap_spots[ts]
    ts_et = ts.astimezone(NY)

    # Skip pre/post market
    if ts_et.time() < dtime(9, 30) or ts_et.time() > dtime(16, 0):
        continue

    total_dd = sum(v for _, v in points)

    # Find peak DD strike (most negative = strongest decay)
    sorted_by_val = sorted(points, key=lambda x: x[1])
    peak_neg_strike = sorted_by_val[0][0]
    peak_neg_value = sorted_by_val[0][1]

    # Find peak positive DD strike
    sorted_by_val_pos = sorted(points, key=lambda x: x[1], reverse=True)
    peak_pos_strike = sorted_by_val_pos[0][0]
    peak_pos_value = sorted_by_val_pos[0][1]

    # DD above vs below spot
    dd_above = sum(v for s, v in points if s > spot)
    dd_below = sum(v for s, v in points if s < spot)
    dd_at_spot = sum(v for s, v in points if abs(s - spot) <= 5)

    # DD concentration: what % of total |DD| is within 25 pts of spot?
    total_abs = sum(abs(v) for _, v in points)
    near_abs = sum(abs(v) for s, v in points if abs(s - spot) <= 25)
    concentration = near_abs / total_abs * 100 if total_abs > 0 else 0

    # Net DD bias: positive total = bullish dealer positioning, negative = bearish
    snap_analysis.append({
        "ts": ts,
        "ts_et": ts_et,
        "date": ts_et.date(),
        "time": ts_et.time(),
        "spot": spot,
        "total_dd": total_dd,
        "peak_neg_strike": peak_neg_strike,
        "peak_neg_value": peak_neg_value,
        "peak_pos_strike": peak_pos_strike,
        "peak_pos_value": peak_pos_value,
        "dd_above": dd_above,
        "dd_below": dd_below,
        "dd_at_spot": dd_at_spot,
        "concentration": concentration,
        "num_strikes": len(points),
    })

p(f"Market-hours snapshots: {len(snap_analysis)}")

# ============================================================
# STEP 3: DD Total (Aggregated) vs Price Direction
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 1: DD Total (Aggregated) vs Subsequent Price Movement")
p("=" * 120)

# For each snapshot, compute forward price change at 5min, 15min, 30min
for i, sa in enumerate(snap_analysis):
    # Find future snapshots on the same day
    sa["fwd_5"] = None
    sa["fwd_15"] = None
    sa["fwd_30"] = None
    sa["fwd_60"] = None

    for j in range(i + 1, len(snap_analysis)):
        future = snap_analysis[j]
        if future["date"] != sa["date"]:
            break
        delta_min = (future["ts"] - sa["ts"]).total_seconds() / 60

        if sa["fwd_5"] is None and delta_min >= 4:
            sa["fwd_5"] = future["spot"] - sa["spot"]
        if sa["fwd_15"] is None and delta_min >= 14:
            sa["fwd_15"] = future["spot"] - sa["spot"]
        if sa["fwd_30"] is None and delta_min >= 28:
            sa["fwd_30"] = future["spot"] - sa["spot"]
        if sa["fwd_60"] is None and delta_min >= 55:
            sa["fwd_60"] = future["spot"] - sa["spot"]

# Compute DD shift (change from previous snapshot)
for i, sa in enumerate(snap_analysis):
    if i == 0 or snap_analysis[i-1]["date"] != sa["date"]:
        sa["dd_shift"] = None
    else:
        sa["dd_shift"] = sa["total_dd"] - snap_analysis[i-1]["total_dd"]

# Bucket by DD total sign and magnitude
p("\n--- DD Total (positive=bullish, negative=bearish) vs 15-min forward price ---")
buckets = [
    ("Strong Bearish (< -$5B)", lambda x: x["total_dd"] < -5e9),
    ("Bearish (-$5B to -$1B)", lambda x: -5e9 <= x["total_dd"] < -1e9),
    ("Mild Bearish (-$1B to $0)", lambda x: -1e9 <= x["total_dd"] < 0),
    ("Mild Bullish ($0 to $1B)", lambda x: 0 <= x["total_dd"] < 1e9),
    ("Bullish ($1B to $5B)", lambda x: 1e9 <= x["total_dd"] < 5e9),
    ("Strong Bullish (> $5B)", lambda x: x["total_dd"] >= 5e9),
]

p(f"{'Bucket':35} {'n':>5} {'Avg5m':>8} {'Avg15m':>8} {'Avg30m':>8} {'Avg60m':>8} {'PctUp15':>8}")
p("-" * 100)
for name, filt in buckets:
    subset = [sa for sa in snap_analysis if filt(sa) and sa["fwd_15"] is not None]
    if not subset:
        continue
    avg5 = statistics.mean([s["fwd_5"] for s in subset if s["fwd_5"] is not None]) if any(s["fwd_5"] is not None for s in subset) else 0
    avg15 = statistics.mean([s["fwd_15"] for s in subset])
    avg30 = statistics.mean([s["fwd_30"] for s in subset if s["fwd_30"] is not None]) if any(s["fwd_30"] is not None for s in subset) else 0
    avg60 = statistics.mean([s["fwd_60"] for s in subset if s["fwd_60"] is not None]) if any(s["fwd_60"] is not None for s in subset) else 0
    pct_up = sum(1 for s in subset if s["fwd_15"] > 0) / len(subset) * 100
    p(f"{name:35} {len(subset):5} {avg5:+8.2f} {avg15:+8.2f} {avg30:+8.2f} {avg60:+8.2f} {pct_up:7.1f}%")

# ============================================================
# STEP 4: DD Shift (Change) vs Price Direction
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 2: DD Shift (Change Between Snapshots) vs Price Direction")
p("=" * 120)

shift_buckets = [
    ("Big Down (< -$1B)", lambda x: x["dd_shift"] is not None and x["dd_shift"] < -1e9),
    ("Down (-$1B to -$200M)", lambda x: x["dd_shift"] is not None and -1e9 <= x["dd_shift"] < -2e8),
    ("Small Down (-$200M to $0)", lambda x: x["dd_shift"] is not None and -2e8 <= x["dd_shift"] < 0),
    ("Small Up ($0 to $200M)", lambda x: x["dd_shift"] is not None and 0 <= x["dd_shift"] < 2e8),
    ("Up ($200M to $1B)", lambda x: x["dd_shift"] is not None and 2e8 <= x["dd_shift"] < 1e9),
    ("Big Up (> $1B)", lambda x: x["dd_shift"] is not None and x["dd_shift"] >= 1e9),
]

p(f"{'Bucket':35} {'n':>5} {'Avg5m':>8} {'Avg15m':>8} {'Avg30m':>8} {'PctUp15':>8}")
p("-" * 100)
for name, filt in shift_buckets:
    subset = [sa for sa in snap_analysis if filt(sa) and sa["fwd_15"] is not None]
    if not subset:
        continue
    avg5 = statistics.mean([s["fwd_5"] for s in subset if s["fwd_5"] is not None]) if any(s["fwd_5"] is not None for s in subset) else 0
    avg15 = statistics.mean([s["fwd_15"] for s in subset])
    avg30 = statistics.mean([s["fwd_30"] for s in subset if s["fwd_30"] is not None]) if any(s["fwd_30"] is not None for s in subset) else 0
    pct_up = sum(1 for s in subset if s["fwd_15"] > 0) / len(subset) * 100
    p(f"{name:35} {len(subset):5} {avg5:+8.2f} {avg15:+8.2f} {avg30:+8.2f} {pct_up:7.1f}%")

# ============================================================
# STEP 5: Peak DD Strike as Magnet/Pin
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 3: Peak DD Strike (Most Negative DD) as Price Magnet")
p("=" * 120)

# The strike with most negative DD is where the most decay happens
# Hypothesis: price gets pulled toward it (dealer hedging creates gravity)

p("\n--- Does price move toward the peak negative DD strike? ---")
dist_buckets = [
    ("< 5 pts", 0, 5),
    ("5-15 pts", 5, 15),
    ("15-30 pts", 15, 30),
    ("> 30 pts", 30, 9999),
]

p(f"{'Distance':15} {'n':>5} {'AvgMove15':>10} {'Toward15':>10} {'AvgMoveEOD':>12}")
p("-" * 70)
for name, lo, hi in dist_buckets:
    subset = [sa for sa in snap_analysis
              if lo <= abs(sa["peak_neg_strike"] - sa["spot"]) < hi
              and sa["fwd_15"] is not None]
    if not subset:
        continue

    # "Toward" means price moved closer to peak DD strike
    toward_count = 0
    moves = []
    for sa in subset:
        init_dist = sa["peak_neg_strike"] - sa["spot"]
        if sa["fwd_15"] is not None:
            new_spot = sa["spot"] + sa["fwd_15"]
            new_dist = sa["peak_neg_strike"] - new_spot
            if abs(new_dist) < abs(init_dist):
                toward_count += 1
            moves.append(sa["fwd_15"])

    avg_move = statistics.mean(moves) if moves else 0
    pct_toward = toward_count / len(subset) * 100 if subset else 0
    p(f"{name:15} {len(subset):5} {avg_move:+10.2f} {pct_toward:9.1f}% {'***' if pct_toward > 55 else ''}")

# ============================================================
# STEP 6: DD Concentration Effect
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 4: DD Concentration (% within 25pts of spot) vs Price Volatility")
p("=" * 120)

conc_buckets = [
    ("Low (<50%)", lambda x: x["concentration"] < 50),
    ("Medium (50-75%)", lambda x: 50 <= x["concentration"] < 75),
    ("High (75-90%)", lambda x: 75 <= x["concentration"] < 90),
    ("Very High (>90%)", lambda x: x["concentration"] >= 90),
]

p(f"{'Concentration':25} {'n':>5} {'Avg|Move15|':>12} {'Avg|Move30|':>12} {'AvgConc':>10}")
p("-" * 80)
for name, filt in conc_buckets:
    subset = [sa for sa in snap_analysis if filt(sa) and sa["fwd_15"] is not None]
    if not subset:
        continue
    avg_abs_15 = statistics.mean([abs(s["fwd_15"]) for s in subset])
    avg_abs_30 = statistics.mean([abs(s["fwd_30"]) for s in subset if s["fwd_30"] is not None]) if any(s["fwd_30"] is not None for s in subset) else 0
    avg_conc = statistics.mean([s["concentration"] for s in subset])
    p(f"{name:25} {len(subset):5} {avg_abs_15:12.2f} {avg_abs_30:12.2f} {avg_conc:9.1f}%")

# ============================================================
# STEP 7: DD Above vs Below Spot (Asymmetry)
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 5: DD Asymmetry (Above vs Below Spot) as Direction Signal")
p("=" * 120)
p("Hypothesis: If DD is heavier above spot, price decays upward. If below, decays downward.")

for sa in snap_analysis:
    total_abs_above = abs(sa["dd_above"])
    total_abs_below = abs(sa["dd_below"])
    total = total_abs_above + total_abs_below
    if total > 0:
        sa["dd_bias"] = (sa["dd_above"] - sa["dd_below"]) / total  # -1 to +1
    else:
        sa["dd_bias"] = 0

bias_buckets = [
    ("Strong Below Bias (<-0.5)", lambda x: x["dd_bias"] < -0.5),
    ("Below Bias (-0.5 to -0.1)", lambda x: -0.5 <= x["dd_bias"] < -0.1),
    ("Neutral (-0.1 to +0.1)", lambda x: -0.1 <= x["dd_bias"] < 0.1),
    ("Above Bias (+0.1 to +0.5)", lambda x: 0.1 <= x["dd_bias"] < 0.5),
    ("Strong Above Bias (>+0.5)", lambda x: x["dd_bias"] >= 0.5),
]

p(f"{'Bias':35} {'n':>5} {'Avg15m':>8} {'Avg30m':>8} {'PctUp15':>8} {'PctUp30':>8}")
p("-" * 80)
for name, filt in bias_buckets:
    subset = [sa for sa in snap_analysis if filt(sa) and sa["fwd_15"] is not None]
    if not subset:
        continue
    avg15 = statistics.mean([s["fwd_15"] for s in subset])
    avg30 = statistics.mean([s["fwd_30"] for s in subset if s["fwd_30"] is not None]) if any(s["fwd_30"] is not None for s in subset) else 0
    pup15 = sum(1 for s in subset if s["fwd_15"] > 0) / len(subset) * 100
    pup30 = sum(1 for s in subset if s["fwd_30"] is not None and s["fwd_30"] > 0) / max(1, sum(1 for s in subset if s["fwd_30"] is not None)) * 100
    p(f"{name:35} {len(subset):5} {avg15:+8.2f} {avg30:+8.2f} {pup15:7.1f}% {pup30:7.1f}%")

# ============================================================
# STEP 8: Time-of-Day DD Effect
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 6: DD Total by Time of Day")
p("=" * 120)

hour_data = defaultdict(list)
for sa in snap_analysis:
    h = sa["time"].hour
    hour_data[h].append(sa)

p(f"{'Hour':>6} {'n':>5} {'AvgTotalDD':>15} {'AvgConc':>10} {'Avg|Fwd15|':>12}")
p("-" * 60)
for h in sorted(hour_data.keys()):
    data = hour_data[h]
    avg_dd = statistics.mean([d["total_dd"] for d in data])
    avg_conc = statistics.mean([d["concentration"] for d in data])
    fwd = [d["fwd_15"] for d in data if d["fwd_15"] is not None]
    avg_fwd = statistics.mean([abs(f) for f in fwd]) if fwd else 0
    p(f"{h:6} {len(data):5} {avg_dd/1e9:+14.2f}B {avg_conc:9.1f}% {avg_fwd:12.2f}")

# ============================================================
# STEP 9: Day-Level DD Analysis
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 7: Daily DD Profile vs Daily Price Range")
p("=" * 120)

date_data = defaultdict(list)
for sa in snap_analysis:
    date_data[sa["date"]].append(sa)

p(f"{'Date':12} {'Snaps':>6} {'Open':>8} {'Close':>8} {'Move':>7} {'AvgDD':>14} {'DDStart':>14} {'DDEnd':>14} {'DDShift':>14} {'AvgConc':>8}")
p("-" * 130)
for date in sorted(date_data.keys()):
    snaps = date_data[date]
    spots = [s["spot"] for s in snaps]
    dds = [s["total_dd"] for s in snaps]
    concs = [s["concentration"] for s in snaps]

    p(f"{date} {len(snaps):6} "
      f"{spots[0]:8.1f} {spots[-1]:8.1f} {spots[-1]-spots[0]:+7.1f} "
      f"{statistics.mean(dds)/1e9:+13.2f}B "
      f"{dds[0]/1e9:+13.2f}B "
      f"{dds[-1]/1e9:+13.2f}B "
      f"{(dds[-1]-dds[0])/1e9:+13.2f}B "
      f"{statistics.mean(concs):7.1f}%")

# ============================================================
# STEP 10: DD Peak Strike vs Actual EOD Close
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 8: Peak Negative DD Strike at Open vs EOD Close (Magnet Test)")
p("=" * 120)

p(f"{'Date':12} {'Spot Open':>10} {'PeakDD':>8} {'Dist':>8} {'EODClose':>10} {'EODDist':>8} {'Move':>8} {'Toward':>8}")
p("-" * 100)
for date in sorted(date_data.keys()):
    snaps = date_data[date]
    # Use first few snapshots to find "early" peak DD strike
    early = snaps[:3]  # First ~6 min
    if not early:
        continue

    # Find consensus peak neg DD strike in early session
    peak_counts = defaultdict(int)
    for sa in early:
        peak_counts[sa["peak_neg_strike"]] += 1
    peak_strike = max(peak_counts, key=peak_counts.get)

    init_dist = peak_strike - snaps[0]["spot"]
    eod_dist = peak_strike - snaps[-1]["spot"]

    toward = "TOWARD" if abs(eod_dist) < abs(init_dist) else "AWAY"
    if abs(init_dist) < 3:
        toward = "PINNED"

    p(f"{date} {snaps[0]['spot']:10.1f} {peak_strike:8.0f} {init_dist:+8.1f} "
      f"{snaps[-1]['spot']:10.1f} {eod_dist:+8.1f} {snaps[-1]['spot']-snaps[0]['spot']:+8.1f} {toward:>8}")

# ============================================================
# STEP 11: Correlation Analysis
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 9: Correlations")
p("=" * 120)

# Pearson correlation helper
def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return 0, 0
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    sx = statistics.stdev(xs) if n > 1 else 1
    sy = statistics.stdev(ys) if n > 1 else 1
    if sx == 0 or sy == 0:
        return 0, 0
    r = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / ((n - 1) * sx * sy)
    return r, n

# DD total vs forward price
valid = [(sa["total_dd"], sa["fwd_15"]) for sa in snap_analysis if sa["fwd_15"] is not None]
if valid:
    r, n = pearson([v[0] for v in valid], [v[1] for v in valid])
    p(f"  DD Total vs 15-min fwd price: r={r:+.4f}  n={n}")

valid = [(sa["total_dd"], sa["fwd_30"]) for sa in snap_analysis if sa["fwd_30"] is not None]
if valid:
    r, n = pearson([v[0] for v in valid], [v[1] for v in valid])
    p(f"  DD Total vs 30-min fwd price: r={r:+.4f}  n={n}")

# DD shift vs forward price
valid = [(sa["dd_shift"], sa["fwd_15"]) for sa in snap_analysis if sa["dd_shift"] is not None and sa["fwd_15"] is not None]
if valid:
    r, n = pearson([v[0] for v in valid], [v[1] for v in valid])
    p(f"  DD Shift vs 15-min fwd price: r={r:+.4f}  n={n}")

# DD bias vs forward price
valid = [(sa["dd_bias"], sa["fwd_15"]) for sa in snap_analysis if sa["fwd_15"] is not None]
if valid:
    r, n = pearson([v[0] for v in valid], [v[1] for v in valid])
    p(f"  DD Bias vs 15-min fwd price: r={r:+.4f}  n={n}")

# Concentration vs abs(forward price) (higher concentration = less movement?)
valid = [(sa["concentration"], abs(sa["fwd_15"])) for sa in snap_analysis if sa["fwd_15"] is not None]
if valid:
    r, n = pearson([v[0] for v in valid], [v[1] for v in valid])
    p(f"  DD Concentration vs |15-min fwd|: r={r:+.4f}  n={n}")

# DD at spot vs forward price
valid = [(sa["dd_at_spot"], sa["fwd_15"]) for sa in snap_analysis if sa["fwd_15"] is not None]
if valid:
    r, n = pearson([v[0] for v in valid], [v[1] for v in valid])
    p(f"  DD at Spot vs 15-min fwd:      r={r:+.4f}  n={n}")

# ============================================================
# STEP 12: DD Regime Change Detection
# ============================================================
p("\n" + "=" * 120)
p("ANALYSIS 10: DD Regime Changes (Sign Flips in Total DD)")
p("=" * 120)

p("When total DD flips from negative to positive or vice versa:")
regime_changes = []
for i in range(1, len(snap_analysis)):
    prev = snap_analysis[i-1]
    curr = snap_analysis[i]
    if prev["date"] != curr["date"]:
        continue

    # Sign flip
    if (prev["total_dd"] < 0 and curr["total_dd"] >= 0) or \
       (prev["total_dd"] >= 0 and curr["total_dd"] < 0):
        regime_changes.append(curr)

p(f"Total regime changes: {len(regime_changes)}")
if regime_changes:
    p(f"\n{'Date':12} {'Time':6} {'Spot':>8} {'DDFrom':>14} {'DDto':>14} {'Fwd15':>8} {'Fwd30':>8}")
    p("-" * 80)
    for rc in regime_changes:
        idx = snap_analysis.index(rc)
        prev_dd = snap_analysis[idx-1]["total_dd"]
        direction = "BEAR->BULL" if prev_dd < 0 else "BULL->BEAR"
        f15 = f"{rc['fwd_15']:+8.2f}" if rc["fwd_15"] is not None else "    N/A"
        f30 = f"{rc['fwd_30']:+8.2f}" if rc["fwd_30"] is not None else "    N/A"
        p(f"{rc['date']} {rc['time'].strftime('%H:%M'):6} {rc['spot']:8.1f} "
          f"{prev_dd/1e9:+13.2f}B {rc['total_dd']/1e9:+13.2f}B {f15} {f30}  {direction}")

    # Stats for regime changes
    bull_flips = [rc for rc in regime_changes if snap_analysis[snap_analysis.index(rc)-1]["total_dd"] < 0]
    bear_flips = [rc for rc in regime_changes if snap_analysis[snap_analysis.index(rc)-1]["total_dd"] >= 0]

    for name, flips in [("BEAR->BULL flips", bull_flips), ("BULL->BEAR flips", bear_flips)]:
        fwd = [f["fwd_15"] for f in flips if f["fwd_15"] is not None]
        if fwd:
            p(f"\n  {name}: n={len(fwd)}, avg 15m fwd={statistics.mean(fwd):+.2f}, "
              f"pct up={sum(1 for f in fwd if f > 0)/len(fwd)*100:.0f}%")

cur.close()
conn.close()

with open("tmp_dd_deep_output.txt", "w") as f:
    f.write("\n".join(out))
print(f"Done. {len(out)} lines -> tmp_dd_deep_output.txt")
