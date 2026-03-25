"""Dip quality check: was the dip real and tradeable?
- How many pts below -GEX?
- How many 5-min bars stayed below?
- Did it dip AND stay below for at least 1-2 bars (time to react)?
- Then did it recover?"""
import json, os, sys, io
from collections import defaultdict
from datetime import date as dt_date

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATA_DIR = r"C:\Users\Faisa\stock_gex_data"
SPX_DIR = os.path.join(DATA_DIR, "spx")

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
    return {"strongest_neg": top_neg[0][0], "strongest_pos": top_pos[0][0],
            "regime": "positive" if total_gex > 0 else "negative"}

dow_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}
events = []
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
    neg = levels["strongest_neg"]; pos = levels["strongest_pos"]
    bars_day = intraday.get(d_int, [])
    if not bars_day: continue
    ratio = spot / bars_day[0]["open"] if bars_day[0]["open"] > 0 else 0
    if ratio < 5 or ratio > 15: continue
    gap = spot - neg
    if gap <= 0: continue  # only days opened above -GEX
    n2p = pos - neg

    # Detailed bar-by-bar tracking
    went_below = False
    first_below_idx = None
    bars_below = 0  # how many consecutive bars had low < -GEX
    max_dip = 0
    max_dip_time = None
    total_bars_below = 0  # total bars where close was below -GEX
    recovered = False
    rec_time = None
    rec_idx = None
    reached_pos = False
    pos_time = None

    for bi, b in enumerate(bars_day):
        lo = b["low"] * ratio
        hi = b["high"] * ratio
        cl = b["close"] * ratio

        if not went_below and lo < neg:
            went_below = True
            first_below_idx = bi
            first_below_time = b["time"]

        if went_below:
            # Track max dip
            dip = neg - lo
            if dip > max_dip:
                max_dip = dip
                max_dip_time = b["time"]

            # Count bars where close is below -GEX
            if cl < neg:
                total_bars_below += 1

            # Recovery: a bar closes above -GEX after we went below
            if not recovered and cl > neg and bi > first_below_idx:
                recovered = True
                rec_time = b["time"]
                rec_idx = bi

            if not reached_pos and hi >= pos:
                reached_pos = True
                pos_time = b["time"]

    if not went_below: continue
    if max_dip > 500: continue  # bad data

    # Bars between first dip and recovery
    bars_to_recover = (rec_idx - first_below_idx) if recovered and rec_idx else None
    time_below_min = bars_to_recover * 5 if bars_to_recover else None  # 5 min bars

    dt = dt_date.fromisoformat(d)
    events.append({
        "date": d, "dow": dow_map[dt.weekday()],
        "spot": spot, "neg": neg, "pos": pos,
        "gap": round(gap, 1), "n2p": round(n2p, 1),
        "max_dip": round(max_dip, 1),
        "max_dip_time": max_dip_time,
        "first_below": first_below_time,
        "bars_below": total_bars_below,
        "bars_to_rec": bars_to_recover,
        "time_below": time_below_min,
        "recovered": recovered, "rec_time": rec_time,
        "reached_pos": reached_pos, "pos_time": pos_time,
        "regime": levels["regime"],
    })

n = len(events)
print("=" * 85)
print("  DIP QUALITY CHECK: Was it a real tradeable dip?")
print("  (Only days where spot opened ABOVE -GEX, then dipped below)")
print("=" * 85)
print()
print(f"  Total events: {n}")
print()

def rpt(label, filt):
    if not filt:
        print(f"  {label:<55}   0")
        return
    t = len(filt)
    r = sum(1 for e in filt if e["recovered"])
    p = sum(1 for e in filt if e["reached_pos"])
    ad = sum(e["max_dip"] for e in filt) / t
    ab = sum(e["bars_below"] for e in filt) / t
    print(f"  {label:<55} {t:>3}  T1={r/t*100:>5.1f}%  T2={p/t*100:>4.0f}%  dip={ad:>5.1f}  bars_below={ab:>4.1f}")

# By minimum dip size
print("--- BY MINIMUM DIP (how deep below -GEX) ---")
print("  (Must dip at least X pts to count as tradeable)")
print()
for min_dip in [0, 1, 2, 3, 5, 8, 10, 15, 20]:
    filt = [e for e in events if e["max_dip"] >= min_dip]
    rpt(f"Dip >= {min_dip} pts", filt)
print()

# By bars spent below -GEX (time to react)
print("--- BY TIME BELOW -GEX (bars with close < -GEX) ---")
print("  (More bars below = more time to enter)")
print()
for min_bars in [0, 1, 2, 3, 5, 10]:
    filt = [e for e in events if e["bars_below"] >= min_bars]
    rpt(f"Bars below >= {min_bars} (>={min_bars*5} min)", filt)
print()

# Combined: dip >= 5 AND bars >= 2
print("--- TRADEABLE DIPS (deep enough + long enough) ---")
print()
rpt("ALL", events)
rpt("Dip>=3 + Bars>=1", [e for e in events if e["max_dip"] >= 3 and e["bars_below"] >= 1])
rpt("Dip>=5 + Bars>=1", [e for e in events if e["max_dip"] >= 5 and e["bars_below"] >= 1])
rpt("Dip>=5 + Bars>=2", [e for e in events if e["max_dip"] >= 5 and e["bars_below"] >= 2])
rpt("Dip>=5 + Bars>=3", [e for e in events if e["max_dip"] >= 5 and e["bars_below"] >= 3])
rpt("Dip>=8 + Bars>=2", [e for e in events if e["max_dip"] >= 8 and e["bars_below"] >= 2])
rpt("Dip>=10 + Bars>=2", [e for e in events if e["max_dip"] >= 10 and e["bars_below"] >= 2])
rpt("Dip>=10 + Bars>=3", [e for e in events if e["max_dip"] >= 10 and e["bars_below"] >= 3])
rpt("Dip>=15 + Bars>=3", [e for e in events if e["max_dip"] >= 15 and e["bars_below"] >= 3])
print()

# FULL LOG with dip quality details
print("=" * 85)
print("  FULL LOG (with dip quality)")
print("=" * 85)
print()
print(f"  {'#':>3} {'Date':>10} {'D':>3} {'-GEX':>6} {'Gap':>5} {'Dip':>5} {'DipT':>5} {'BarsB':>5} {'TimeB':>5} {'T1':>3} {'T1T':>5} {'T2':>3} {'T2T':>5} {'Quality':>8}")
for i, e in enumerate(events, 1):
    r = "Y" if e["recovered"] else "N"
    p = "Y" if e["reached_pos"] else "N"
    rt = e["rec_time"][:5] if e["rec_time"] else "  -  "
    pt = e["pos_time"][:5] if e["pos_time"] else "  -  "
    tb = f"{e['time_below']}m" if e["time_below"] else "-"

    # Quality label
    if e["max_dip"] >= 10 and e["bars_below"] >= 3:
        q = "STRONG"
    elif e["max_dip"] >= 5 and e["bars_below"] >= 2:
        q = "GOOD"
    elif e["max_dip"] >= 3 and e["bars_below"] >= 1:
        q = "OK"
    elif e["max_dip"] >= 1:
        q = "weak"
    else:
        q = "noise"

    print(f"  {i:>3} {e['date']:>10} {e['dow']:>3} {e['neg']:>6.0f} {e['gap']:>+5.0f} {e['max_dip']:>5.1f} {e['max_dip_time']:>5} {e['bars_below']:>5} {tb:>5} {r:>3} {rt:>5} {p:>3} {pt:>5} {q:>8}")
