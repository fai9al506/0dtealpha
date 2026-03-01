"""
GEX Magnet Finder
Looks for strikes where ONE strike dominates both call AND put GEX
and tests the "magnet" hypothesis: does price gravitate toward it?
"""
import json, os, psycopg2, statistics
from datetime import datetime, timedelta
from collections import defaultdict

DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Column layout (from DB): left=calls, center=strike, right=puts
# [Volume, Open Int, IV, Gamma, Delta, BID, BID QTY, ASK, ASK QTY, LAST, Strike,
#  LAST, ASK, ASK QTY, BID, BID QTY, Delta, Gamma, IV, Open Int, Volume]
# Indices: Call Gamma=3, Call OI=1, Put Gamma=17, Put OI=19, Strike=10

C_GAMMA = 3
C_OI = 1
P_GAMMA = 17
P_OI = 19
STRIKE = 10

out = []

def p(s):
    out.append(str(s))

p("Fetching chain snapshots...")
cur.execute("""
    SELECT ts, exp, spot, rows
    FROM chain_snapshots
    WHERE spot > 0
    ORDER BY ts
""")
rows_all = cur.fetchall()
p(f"Total snapshots: {len(rows_all)}")

# Process each snapshot
all_events = []
date_spots = defaultdict(list)

for ts, exp, spot, rows_json in rows_all:
    data_rows = json.loads(rows_json) if isinstance(rows_json, str) else rows_json

    if not data_rows or len(data_rows) < 5:
        continue

    date_spots[ts.strftime("%Y-%m-%d")].append((ts, spot))

    strike_data = []
    for row in data_rows:
        try:
            strike = float(row[STRIKE]) if row[STRIKE] else 0
            c_gamma = float(row[C_GAMMA]) if row[C_GAMMA] else 0
            c_oi = float(row[C_OI]) if row[C_OI] else 0
            p_gamma = float(row[P_GAMMA]) if row[P_GAMMA] else 0
            p_oi = float(row[P_OI]) if row[P_OI] else 0
        except (TypeError, ValueError, IndexError):
            continue

        call_gex = c_gamma * c_oi * 100.0
        put_gex = p_gamma * p_oi * 100.0  # absolute magnitude

        if strike > 0:
            strike_data.append({
                "strike": strike,
                "call_gex": call_gex,
                "put_gex": put_gex,
                "min_side": min(call_gex, put_gex),
                "c_oi": c_oi,
                "p_oi": p_oi,
            })

    if len(strike_data) < 10:
        continue

    # Find the strike with largest min(call_gex, put_gex) = BOTH sides large
    strike_data.sort(key=lambda x: x["min_side"], reverse=True)
    top = strike_data[0]

    if top["call_gex"] <= 0 or top["put_gex"] <= 0:
        continue

    rest = strike_data[1:]
    rest_mins = sorted([s["min_side"] for s in rest], reverse=True)
    second_best = rest_mins[0] if rest_mins else 0
    rest_call = [s["call_gex"] for s in rest if s["call_gex"] > 0]
    rest_put = [s["put_gex"] for s in rest if s["put_gex"] > 0]
    avg_rest_call = statistics.mean(rest_call) if rest_call else 1
    avg_rest_put = statistics.mean(rest_put) if rest_put else 1

    ratio_vs_second = top["min_side"] / second_best if second_best > 0 else 999

    all_events.append({
        "ts": ts,
        "exp": exp,
        "spot": spot,
        "magnet_strike": top["strike"],
        "call_gex": top["call_gex"],
        "put_gex": top["put_gex"],
        "min_side": top["min_side"],
        "c_oi": top["c_oi"],
        "p_oi": top["p_oi"],
        "ratio_vs_second": ratio_vs_second,
        "call_dom": top["call_gex"] / avg_rest_call,
        "put_dom": top["put_gex"] / avg_rest_put,
        "dist": top["strike"] - spot,
    })

p(f"Events with both call+put GEX > 0: {len(all_events)}")

if all_events:
    ratios = [e["ratio_vs_second"] for e in all_events]
    p(f"\nDominance ratio (top vs 2nd best min_side):")
    p(f"  Min={min(ratios):.2f}x  Max={max(ratios):.2f}x  Mean={statistics.mean(ratios):.2f}x  Med={statistics.median(ratios):.2f}x")
    p(f"  >= 1.5x: {sum(1 for r in ratios if r >= 1.5)}")
    p(f"  >= 2.0x: {sum(1 for r in ratios if r >= 2.0)}")
    p(f"  >= 3.0x: {sum(1 for r in ratios if r >= 3.0)}")
    p(f"  >= 5.0x: {sum(1 for r in ratios if r >= 5.0)}")

# Use threshold that gives reasonable sample
# Start with 2x (top is twice the second best on the "both sides" metric)
for thresh in [3.0, 2.0, 1.5]:
    magnets = [e for e in all_events if e["ratio_vs_second"] >= thresh]
    if len(magnets) >= 5:
        p(f"\nUsing threshold: {thresh}x  ({len(magnets)} events)")
        break
else:
    magnets = all_events
    thresh = 0
    p(f"\nUsing all events: {len(magnets)}")

# Deduplicate: keep FIRST and LAST per date+strike (first appearance, show persistence)
seen = set()
unique = []
for e in sorted(magnets, key=lambda x: x["ts"]):
    key = f"{e['ts'].strftime('%Y-%m-%d')}_{e['magnet_strike']:.0f}"
    if key not in seen:
        seen.add(key)
        unique.append(e)

p(f"Unique date+strike: {len(unique)}")

# Show all magnet events
p("\n" + "=" * 140)
p(f"{'Date':12} {'Time':6} {'Spot':>8} {'Magnet':>8} {'Dist':>8} {'CallGEX':>12} {'PutGEX':>12} "
  f"{'vs2nd':>7} {'CDom':>7} {'PDom':>7} {'C_OI':>8} {'P_OI':>8}")
p("=" * 140)

for e in unique:
    p(f"{e['ts'].strftime('%Y-%m-%d'):12} "
      f"{e['ts'].strftime('%H:%M'):6} "
      f"{e['spot']:8.1f} "
      f"{e['magnet_strike']:8.0f} "
      f"{e['dist']:+8.1f} "
      f"{e['call_gex']:12.2f} "
      f"{e['put_gex']:12.2f} "
      f"{e['ratio_vs_second']:7.2f}x "
      f"{e['call_dom']:7.2f}x "
      f"{e['put_dom']:7.2f}x "
      f"{e['c_oi']:8.0f} "
      f"{e['p_oi']:8.0f}")

# --- OUTCOME ---
p("\n\n" + "=" * 160)
p("OUTCOME ANALYSIS: Does price gravitate toward the magnet?")
p("=" * 160)

results = []
for e in unique:
    d = e["ts"].strftime("%Y-%m-%d")
    spots = date_spots.get(d, [])
    later = [(t, s) for t, s in spots if t > e["ts"]]
    if not later:
        continue

    init_dist = e["dist"]
    target_30 = e["ts"] + timedelta(minutes=30)
    target_60 = e["ts"] + timedelta(minutes=60)

    s30 = [s for t, s in later if t <= target_30]
    s60 = [s for t, s in later if t <= target_60]
    spot_30 = s30[-1] if s30 else later[0][1]
    spot_60 = s60[-1] if s60 else later[-1][1]
    spot_eod = later[-1][1]

    d30 = e["magnet_strike"] - spot_30
    d60 = e["magnet_strike"] - spot_60
    deod = e["magnet_strike"] - spot_eod
    min_d = min(abs(e["magnet_strike"] - s) for _, s in later)

    def cl(id, ld):
        if abs(id) < 3:
            return "PINNED"
        return "TOWARD" if abs(ld) < abs(id) else "AWAY"

    results.append({
        "date": d, "time": e["ts"].strftime("%H:%M"),
        "spot": e["spot"], "magnet": e["magnet_strike"],
        "init_dist": init_dist,
        "spot_30": spot_30, "d30": d30, "r30": cl(init_dist, d30),
        "spot_60": spot_60, "d60": d60, "r60": cl(init_dist, d60),
        "spot_eod": spot_eod, "deod": deod, "reod": cl(init_dist, deod),
        "min_d": min_d, "touched": min_d < 3,
        "ratio": e["ratio_vs_second"],
    })

p(f"\n{'Date':12} {'Time':6} {'Spot':>8} {'Mag':>8} {'iDist':>7} "
  f"{'S30':>8} {'D30':>7} {'R30':>7} "
  f"{'S60':>8} {'D60':>7} {'R60':>7} "
  f"{'EOD':>8} {'DEOD':>7} {'REOD':>7} "
  f"{'MinD':>6} {'Touch':>6}")
p("-" * 160)

for r in results:
    p(f"{r['date']:12} {r['time']:6} {r['spot']:8.1f} {r['magnet']:8.0f} {r['init_dist']:+7.1f} "
      f"{r['spot_30']:8.1f} {r['d30']:+7.1f} {r['r30']:>7} "
      f"{r['spot_60']:8.1f} {r['d60']:+7.1f} {r['r60']:>7} "
      f"{r['spot_eod']:8.1f} {r['deod']:+7.1f} {r['reod']:>7} "
      f"{r['min_d']:6.1f} {'YES' if r['touched'] else 'no':>6}")

# Summaries
for label, key in [("30-min", "r30"), ("60-min", "r60"), ("EOD", "reod")]:
    tw = sum(1 for r in results if r[key] == "TOWARD")
    aw = sum(1 for r in results if r[key] == "AWAY")
    pn = sum(1 for r in results if r[key] == "PINNED")
    total = tw + aw + pn
    if total > 0:
        p(f"\n--- {label} (n={total}) ---")
        p(f"  TOWARD: {tw} ({tw/total*100:.0f}%)")
        p(f"  AWAY:   {aw} ({aw/total*100:.0f}%)")
        p(f"  PINNED: {pn} ({pn/total*100:.0f}%)")

if results:
    touched = sum(1 for r in results if r["touched"])
    p(f"\n--- Magnet Touch Rate ---")
    p(f"  Price came within 3 pts: {touched}/{len(results)} ({touched/len(results)*100:.0f}%)")
    avg_min = statistics.mean([r["min_d"] for r in results])
    med_min = statistics.median([r["min_d"] for r in results])
    p(f"  Avg closest approach: {avg_min:.1f} pts")
    p(f"  Median closest approach: {med_min:.1f} pts")

# By initial distance
p(f"\n--- Touch Rate by Initial Distance ---")
for name, lo, hi in [("< 5 pts", 0, 5), ("5-15 pts", 5, 15), ("15-30 pts", 15, 30), ("> 30 pts", 30, 9999)]:
    bk = [r for r in results if lo <= abs(r["init_dist"]) < hi]
    if bk:
        tc = sum(1 for r in bk if r["touched"])
        tw = sum(1 for r in bk if r["reod"] == "TOWARD")
        p(f"  {name:12} n={len(bk):3}  Touched={tc}/{len(bk)} ({tc/len(bk)*100:.0f}%)  EOD-TOWARD={tw}/{len(bk)} ({tw/len(bk)*100:.0f}%)")

# By dominance strength
p(f"\n--- By Dominance Strength ---")
for name, lo, hi in [("1.5-2x", 1.5, 2), ("2-3x", 2, 3), ("3-5x", 3, 5), ("5x+", 5, 9999)]:
    bk = [r for r in results if lo <= r["ratio"] < hi]
    if bk:
        tc = sum(1 for r in bk if r["touched"])
        tw = sum(1 for r in bk if r["reod"] == "TOWARD")
        p(f"  {name:12} n={len(bk):3}  Touched={tc}/{len(bk)} ({tc/len(bk)*100:.0f}%)  EOD-TOWARD={tw}/{len(bk)} ({tw/len(bk)*100:.0f}%)")

# Show top 5 most extreme magnets with outcomes
p(f"\n--- Top 5 Most Extreme Magnet Events ---")
extreme = sorted([r for r in results], key=lambda x: x["ratio"], reverse=True)[:5]
for r in extreme:
    p(f"  {r['date']} {r['time']} | Spot={r['spot']:.1f} Magnet={r['magnet']:.0f} Dist={r['init_dist']:+.1f} | "
      f"Ratio={r['ratio']:.1f}x | 30m={r['r30']} 60m={r['r60']} EOD={r['reod']} | "
      f"MinDist={r['min_d']:.1f} Touched={'YES' if r['touched'] else 'no'}")

cur.close()
conn.close()

# Write output
with open("tmp_gex_magnet_output.txt", "w") as f:
    f.write("\n".join(out))

print(f"Done. Output written to tmp_gex_magnet_output.txt ({len(out)} lines)")
