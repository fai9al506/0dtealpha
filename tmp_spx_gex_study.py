"""SPX GEX Raw Price Action Study — What actually happens when price goes below -GEX?

For every day where price dips below strongest -GEX:
1. How far below -GEX did it go? (max dip)
2. Did it recover back above -GEX? When?
3. Did it reach +GEX? When?
4. What was the full MFE from the low?

NO trading logic — just raw observation of what the market does around GEX levels.
"""
import json, os, sys, io
from collections import defaultdict
from datetime import date as dt_date

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATA_DIR = r"C:\Users\Faisa\stock_gex_data"
SPX_DIR = os.path.join(DATA_DIR, "spx")

# Load data
with open(os.path.join(SPX_DIR, "prices", "SPX.json")) as f:
    prices = {int(b["date"]): b for b in json.load(f)}
with open(os.path.join(SPX_DIR, "intraday", "SPY_5min.json")) as f:
    spy_bars_raw = json.load(f)
intraday = defaultdict(list)
for b in spy_bars_raw:
    intraday[b["date"]].append(b)
for d in intraday:
    intraday[d].sort(key=lambda x: x["ms_of_day"])

def compute_gex(records):
    gex = {}
    for r in records:
        k = r.get("strike_dollars", r.get("strike", 0) / 1000.0)
        g = r.get("gamma", 0) * r.get("open_interest", 0) * 100
        if r.get("right") == "P": g = -g
        gex[k] = gex.get(k, 0) + g
    return gex

def extract_levels(gex, spot):
    neg = [(k, v) for k, v in gex.items() if v < 0]
    pos = [(k, v) for k, v in gex.items() if v > 0]
    if not neg or not pos: return None
    neg.sort(key=lambda x: x[1]); pos.sort(key=lambda x: x[1], reverse=True)
    top_neg = neg[:5]; mx = abs(top_neg[0][1])
    top_neg = [(k, v) for k, v in top_neg if abs(v) >= mx * 0.10]
    top_pos = pos[:5]; mx = top_pos[0][1]
    top_pos = [(k, v) for k, v in top_pos if v >= mx * 0.10]
    total_gex = sum(v for _, v in gex.items())
    return {
        "strongest_neg": top_neg[0][0], "strongest_pos": top_pos[0][0],
        "regime": "positive" if total_gex > 0 else "negative",
    }

# Build dataset
dataset = []
options_dir = os.path.join(SPX_DIR, "options")
for f in sorted(os.listdir(options_dir)):
    if not f.endswith("_0dte.json"): continue
    d = f.replace("_0dte.json", ""); d_int = int(d.replace("-", ""))
    bar = prices.get(d_int)
    if not bar: continue
    with open(os.path.join(options_dir, f)) as fh:
        records = json.load(fh)
    if not records: continue
    spot = bar["open"]; gex = compute_gex(records)
    levels = extract_levels(gex, spot)
    if not levels: continue
    dataset.append({"date": d, "date_int": d_int, "open": bar["open"],
                    "high": bar["high"], "low": bar["low"], "close": bar["close"], **levels})

print(f"Dataset: {len(dataset)} days")
print()

# ================================================================
# RAW STUDY: For each day price goes below -GEX, what happens?
# ================================================================
events = []
dow_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}

for day in dataset:
    neg = day["strongest_neg"]
    pos = day["strongest_pos"]
    spot = day["open"]
    bars = intraday.get(day["date_int"], [])
    if not bars or not neg: continue

    ratio = spot / bars[0]["open"] if bars[0]["open"] > 0 else 0
    if ratio <= 0: continue

    gap = spot - neg  # how far open is above -GEX
    neg_to_pos = pos - neg  # distance between levels

    # Scan bars: did price go below -GEX?
    went_below = False
    first_below_time = None
    max_dip_below = 0  # max pts below -GEX
    max_dip_time = None
    low_price = None  # actual lowest SPX price while below -GEX

    recovered_above_neg = False
    recovery_time = None

    reached_pos = False
    pos_time = None

    # Track MFE from the lowest point after going below -GEX
    mfe_from_low = 0

    # Track overall low of the day below -GEX
    overall_low_below = 999999
    overall_low_time = None

    for b in bars:
        lo_spx = b["low"] * ratio
        hi_spx = b["high"] * ratio
        close_spx = b["close"] * ratio

        if not went_below:
            # Check if this bar goes below -GEX
            if lo_spx < neg:
                went_below = True
                first_below_time = b["time"]
                dip = neg - lo_spx
                max_dip_below = dip
                max_dip_time = b["time"]
                low_price = lo_spx
                overall_low_below = lo_spx
                overall_low_time = b["time"]
        else:
            # Already below — track deeper dip
            if lo_spx < overall_low_below:
                overall_low_below = lo_spx
                overall_low_time = b["time"]
                max_dip_below = neg - lo_spx
                max_dip_time = b["time"]
                low_price = lo_spx

        # After going below, check for recovery above -GEX
        if went_below and not recovered_above_neg:
            if hi_spx > neg:
                recovered_above_neg = True
                recovery_time = b["time"]

        # Check if reached +GEX
        if went_below and not reached_pos:
            if hi_spx >= pos:
                reached_pos = True
                pos_time = b["time"]

    if not went_below:
        continue

    # MFE from lowest point to highest point after
    # Re-scan from the low point forward
    found_low = False
    highest_after_low = overall_low_below
    for b in bars:
        lo_spx = b["low"] * ratio
        hi_spx = b["high"] * ratio
        if not found_low:
            if b["time"] == overall_low_time:
                found_low = True
                highest_after_low = hi_spx
        else:
            highest_after_low = max(highest_after_low, hi_spx)

    mfe_from_low = highest_after_low - overall_low_below

    # Close vs -GEX
    last_close_spx = bars[-1]["close"] * ratio
    close_vs_neg = last_close_spx - neg

    d = dt_date.fromisoformat(day["date"])
    events.append({
        "date": day["date"],
        "dow": dow_map[d.weekday()],
        "spot": spot,
        "neg": neg,
        "pos": pos,
        "gap": round(gap, 1),
        "neg_to_pos": round(neg_to_pos, 1),
        "first_below": first_below_time,
        "max_dip": round(max_dip_below, 1),
        "max_dip_time": max_dip_time,
        "low_price": round(overall_low_below, 1),
        "recovered": recovered_above_neg,
        "recovery_time": recovery_time,
        "reached_pos": reached_pos,
        "pos_time": pos_time,
        "mfe_from_low": round(mfe_from_low, 1),
        "close_vs_neg": round(close_vs_neg, 1),
        "regime": day["regime"],
        # Potential PnL if entered at low
        "t1_pnl": round(neg - overall_low_below, 1) if recovered_above_neg else round(last_close_spx - overall_low_below, 1),
        "t2_pnl": round(pos - overall_low_below, 1) if reached_pos else None,
    })


# ================================================================
# SUMMARY STATS
# ================================================================
print("=" * 70)
print("  RAW STUDY: What happens when SPX drops below -GEX?")
print("=" * 70)
print()

n = len(events)
n_recovered = sum(1 for e in events if e["recovered"])
n_reached_pos = sum(1 for e in events if e["reached_pos"])
avg_dip = sum(e["max_dip"] for e in events) / n
avg_mfe = sum(e["mfe_from_low"] for e in events) / n
avg_gap = sum(e["gap"] for e in events) / n
avg_n2p = sum(e["neg_to_pos"] for e in events) / n

print(f"  Days price went below -GEX: {n}/{len(dataset)} ({n/len(dataset)*100:.0f}%)")
print(f"  Recovered above -GEX: {n_recovered}/{n} ({n_recovered/n*100:.0f}%)")
print(f"  Reached +GEX: {n_reached_pos}/{n} ({n_reached_pos/n*100:.0f}%)")
print()
print(f"  Avg gap at open (spot - neg): {avg_gap:+.1f} pts")
print(f"  Avg -GEX to +GEX distance: {avg_n2p:.1f} pts")
print(f"  Avg max dip below -GEX: {avg_dip:.1f} pts")
print(f"  Avg MFE from low: {avg_mfe:.1f} pts")
print()

# Distribution of max dip
print("  --- Max Dip Distribution ---")
dip_buckets = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 30), (30, 50), (50, 999)]
for lo, hi in dip_buckets:
    bucket = [e for e in events if lo <= e["max_dip"] < hi]
    if not bucket: continue
    rec = sum(1 for e in bucket if e["recovered"])
    pos_r = sum(1 for e in bucket if e["reached_pos"])
    avg_mfe_b = sum(e["mfe_from_low"] for e in bucket) / len(bucket)
    label = f"{lo}-{hi}" if hi < 999 else f"{lo}+"
    print(f"  Dip {label:>5} pts: {len(bucket):>3} days, recovered={rec/len(bucket)*100:>4.0f}%, reached +GEX={pos_r/len(bucket)*100:>4.0f}%, avg MFE={avg_mfe_b:>5.1f}")
print()

# T1 PnL (recovery to -GEX) distribution
print("  --- T1 PnL (entry at low -> recovery to -GEX) ---")
t1_pnls = [e["max_dip"] for e in events if e["recovered"]]  # dip = profit if entered at low and exited at -GEX
if t1_pnls:
    print(f"  Trades that recovered: {len(t1_pnls)}")
    print(f"  Avg T1 PnL: {sum(t1_pnls)/len(t1_pnls):+.1f} pts")
    print(f"  Min: {min(t1_pnls):+.1f} | Max: {max(t1_pnls):+.1f}")
    t1_above_15 = sum(1 for p in t1_pnls if p >= 15)
    t1_above_10 = sum(1 for p in t1_pnls if p >= 10)
    print(f"  T1 >= 10pts: {t1_above_10}/{len(t1_pnls)} ({t1_above_10/len(t1_pnls)*100:.0f}%)")
    print(f"  T1 >= 15pts: {t1_above_15}/{len(t1_pnls)} ({t1_above_15/len(t1_pnls)*100:.0f}%)")
print()

# T2 PnL (entry at low -> +GEX) distribution
print("  --- T2 PnL (entry at low -> reached +GEX) ---")
t2_pnls = [e["t2_pnl"] for e in events if e["t2_pnl"] is not None]
if t2_pnls:
    print(f"  Trades that reached +GEX: {len(t2_pnls)}")
    print(f"  Avg T2 PnL: {sum(t2_pnls)/len(t2_pnls):+.1f} pts")
    print(f"  Min: {min(t2_pnls):+.1f} | Max: {max(t2_pnls):+.1f}")
print()

# By regime
print("  --- By Regime ---")
for regime in ["positive", "negative"]:
    re = [e for e in events if e["regime"] == regime]
    if not re: continue
    rec = sum(1 for e in re if e["recovered"])
    pos_r = sum(1 for e in re if e["reached_pos"])
    avg_d = sum(e["max_dip"] for e in re) / len(re)
    avg_m = sum(e["mfe_from_low"] for e in re) / len(re)
    print(f"  {regime.upper():>10}: {len(re)} days, recovered={rec/len(re)*100:.0f}%, +GEX={pos_r/len(re)*100:.0f}%, avg dip={avg_d:.1f}, avg MFE={avg_m:.1f}")
print()

# By gap at open
print("  --- By Gap at Open ---")
gap_buckets = [("Gap<0 (open below -GEX)", -999, 0), ("Gap 0-10", 0, 10), ("Gap 10-20", 10, 20),
               ("Gap 20-40", 20, 40), ("Gap 40+", 40, 999)]
for label, lo, hi in gap_buckets:
    ge = [e for e in events if lo <= e["gap"] < hi]
    if not ge: continue
    rec = sum(1 for e in ge if e["recovered"])
    pos_r = sum(1 for e in ge if e["reached_pos"])
    avg_d = sum(e["max_dip"] for e in ge) / len(ge)
    avg_m = sum(e["mfe_from_low"] for e in ge) / len(ge)
    print(f"  {label:<25} {len(ge):>3} days, rec={rec/len(ge)*100:>4.0f}%, +GEX={pos_r/len(ge)*100:>4.0f}%, dip={avg_d:>5.1f}, MFE={avg_m:>5.1f}")
print()

# By day of week
print("  --- By Day of Week ---")
for dow in ["Mon", "Tue", "Wed", "Thu", "Fri"]:
    de = [e for e in events if e["dow"] == dow]
    if not de: continue
    rec = sum(1 for e in de if e["recovered"])
    pos_r = sum(1 for e in de if e["reached_pos"])
    avg_d = sum(e["max_dip"] for e in de) / len(de)
    avg_m = sum(e["mfe_from_low"] for e in de) / len(de)
    print(f"  {dow:>6}: {len(de):>3} days, rec={rec/len(de)*100:>4.0f}%, +GEX={pos_r/len(de)*100:>4.0f}%, dip={avg_d:>5.1f}, MFE={avg_m:>5.1f}")
print()

# By first_below time
print("  --- By Time First Below -GEX ---")
time_b = defaultdict(list)
for e in events:
    h = int(e["first_below"].split(":")[0])
    if h < 11: bk = "10:00-10:59"
    elif h < 12: bk = "11:00-11:59"
    elif h < 13: bk = "12:00-12:59"
    elif h < 14: bk = "13:00-13:59"
    else: bk = "14:00+"
    time_b[bk].append(e)

for bk in sorted(time_b.keys()):
    te = time_b[bk]
    rec = sum(1 for e in te if e["recovered"])
    pos_r = sum(1 for e in te if e["reached_pos"])
    avg_d = sum(e["max_dip"] for e in te) / len(te)
    avg_m = sum(e["mfe_from_low"] for e in te) / len(te)
    print(f"  {bk:>14}: {len(te):>3} days, rec={rec/len(te)*100:>4.0f}%, +GEX={pos_r/len(te)*100:>4.0f}%, dip={avg_d:>5.1f}, MFE={avg_m:>5.1f}")
print()

# ================================================================
# FULL EVENT LOG
# ================================================================
print("=" * 70)
print("  FULL EVENT LOG (every day price went below -GEX)")
print("=" * 70)
print()
print(f"  {'#':>3} {'Date':>10} {'D':>3} {'Open':>6} {'-GEX':>6} {'+GEX':>6} {'Gap':>5} {'Dip':>5} {'DipT':>5} {'Rec?':>4} {'RecT':>5} {'+GEX?':>5} {'+T':>5} {'MFE':>5} {'ClvsN':>6} {'Reg':>4}")
print(f"  {'-'*3} {'-'*10} {'-'*3} {'-'*6} {'-'*6} {'-'*6} {'-'*5} {'-'*5} {'-'*5} {'-'*4} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*6} {'-'*4}")

for i, e in enumerate(events, 1):
    rec_mark = "YES" if e["recovered"] else "no"
    pos_mark = "YES" if e["reached_pos"] else "no"
    rec_t = e["recovery_time"][:5] if e["recovery_time"] else "-"
    pos_t = e["pos_time"][:5] if e["pos_time"] else "-"
    dip_t = e["max_dip_time"][:5] if e["max_dip_time"] else "-"
    reg = "+" if e["regime"] == "positive" else "-"
    print(f"  {i:>3} {e['date']:>10} {e['dow']:>3} {e['spot']:>6.0f} {e['neg']:>6.0f} {e['pos']:>6.0f} {e['gap']:>+5.0f} {e['max_dip']:>5.1f} {dip_t:>5} {rec_mark:>4} {rec_t:>5} {pos_mark:>5} {pos_t:>5} {e['mfe_from_low']:>5.1f} {e['close_vs_neg']:>+6.1f} {reg:>4}")
