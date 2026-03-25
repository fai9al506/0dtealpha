"""Intraday recovery study: after dipping below -GEX, did price come back above -GEX?
T1 = recovery to -GEX level (our first target with options)
Uses SPY 5-min bars * ratio for intraday tracking."""
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
    total_neg_mag = abs(sum(v for _, v in neg))
    total_pos_mag = sum(v for _, v in pos)
    return {
        "strongest_neg": top_neg[0][0], "strongest_pos": top_pos[0][0],
        "regime": "positive" if total_gex > 0 else "negative",
        "pos_neg_ratio": round(total_pos_mag / max(total_neg_mag, 1), 2),
        "neg_gex_mag": abs(top_neg[0][1]),
    }

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

    neg = levels["strongest_neg"]
    pos = levels["strongest_pos"]
    bars_day = intraday.get(d_int, [])
    if not bars_day: continue
    ratio = spot / bars_day[0]["open"] if bars_day[0]["open"] > 0 else 0
    if ratio < 5 or ratio > 15: continue

    gap = spot - neg
    n2p = pos - neg

    # Intraday tracking
    went_below = False
    first_below_time = None
    low_below = None
    low_time = None
    recovered = False
    rec_time = None
    reached_pos = False
    pos_time = None

    for b in bars_day:
        lo = b["low"] * ratio
        hi = b["high"] * ratio

        if not went_below and lo < neg:
            went_below = True
            first_below_time = b["time"]
            low_below = lo
            low_time = b["time"]

        if went_below and lo < (low_below or 999999):
            low_below = lo
            low_time = b["time"]

        # Recovery = high came back above -GEX AFTER we went below
        if went_below and not recovered and hi > neg + 1:  # +1 to confirm above
            recovered = True
            rec_time = b["time"]

        if went_below and not reached_pos and hi >= pos:
            reached_pos = True
            pos_time = b["time"]

    if not went_below: continue

    dip = neg - low_below
    if dip > 500: continue  # bad data

    # MFE from low
    found_low = False
    highest_after = low_below
    for b in bars_day:
        if not found_low:
            if b["time"] == low_time: found_low = True; highest_after = b["high"] * ratio
        else:
            highest_after = max(highest_after, b["high"] * ratio)
    mfe = highest_after - low_below

    dt = dt_date.fromisoformat(d)
    events.append({
        "date": d, "dow": dow_map[dt.weekday()], "dow_num": dt.weekday(),
        "spot": spot, "neg": neg, "pos": pos,
        "gap": round(gap, 1), "n2p": round(n2p, 1),
        "dip": round(dip, 1),
        "first_below": first_below_time,
        "low_time": low_time,
        "recovered": recovered, "rec_time": rec_time,
        "reached_pos": reached_pos, "pos_time": pos_time,
        "mfe": round(mfe, 1),
        "close": bar["close"], "close_above": bar["close"] > neg,
        "regime": levels["regime"],
        "ratio_pn": levels["pos_neg_ratio"],
        "neg_mag": levels["neg_gex_mag"],
    })

n = len(events)
n_rec = sum(1 for e in events if e["recovered"])
n_pos = sum(1 for e in events if e["reached_pos"])

print("=" * 85)
print("  INTRADAY RECOVERY STUDY")
print("  T1 = price recovers above -GEX (at any point after dipping)")
print("  T2 = price reaches +GEX")
print("=" * 85)
print()
print(f"  Total events: {n} ({n/52:.1f}/week)")
print(f"  T1 hit (recovered above -GEX): {n_rec}/{n} ({n_rec/n*100:.0f}%)")
print(f"  T2 hit (reached +GEX):         {n_pos}/{n} ({n_pos/n*100:.0f}%)")
print(f"  Close above -GEX:              {sum(1 for e in events if e['close_above'])}/{n} ({sum(1 for e in events if e['close_above'])/n*100:.0f}%)")
print()

def rpt(label, filt):
    if not filt:
        print(f"  {label:<50}   0")
        return
    t = len(filt)
    r = sum(1 for e in filt if e["recovered"])
    p = sum(1 for e in filt if e["reached_pos"])
    ad = sum(e["dip"] for e in filt) / t
    am = sum(e["mfe"] for e in filt) / t
    print(f"  {label:<50} {t:>3}  T1={r/t*100:>5.1f}%  T2={p/t*100:>4.0f}%  dip={ad:>5.1f}  MFE={am:>5.1f}")

# SINGLE FILTERS
print("--- REGIME ---")
rpt("ALL", events)
rpt("Positive GEX", [e for e in events if e["regime"] == "positive"])
rpt("Negative GEX", [e for e in events if e["regime"] == "negative"])
print()

print("--- GAP AT OPEN ---")
rpt("ALL", events)
rpt("Gap < 0 (opened below)", [e for e in events if e["gap"] < 0])
rpt("Gap 0-10", [e for e in events if 0 <= e["gap"] < 10])
rpt("Gap 10-30", [e for e in events if 10 <= e["gap"] < 30])
rpt("Gap 30-50", [e for e in events if 30 <= e["gap"] < 50])
rpt("Gap 50+", [e for e in events if e["gap"] >= 50])
print()

print("--- N2P DISTANCE ---")
rpt("ALL", events)
rpt("N2P < 20", [e for e in events if e["n2p"] < 20])
rpt("N2P 20-40", [e for e in events if 20 <= e["n2p"] < 40])
rpt("N2P 40-60", [e for e in events if 40 <= e["n2p"] < 60])
rpt("N2P 60+", [e for e in events if e["n2p"] >= 60])
print()

print("--- POS/NEG RATIO ---")
rpt("ALL", events)
rpt("Ratio < 0.5", [e for e in events if e["ratio_pn"] < 0.5])
rpt("Ratio 0.5-1.0", [e for e in events if 0.5 <= e["ratio_pn"] < 1.0])
rpt("Ratio 1.0-2.0", [e for e in events if 1.0 <= e["ratio_pn"] < 2.0])
rpt("Ratio 2.0+", [e for e in events if e["ratio_pn"] >= 2.0])
print()

print("--- -GEX WALL STRENGTH ---")
mags = sorted(e["neg_mag"] for e in events)
p50 = mags[len(mags)//2]; p75 = mags[3*len(mags)//4]
rpt("ALL", events)
rpt(f"Neg mag < median ({p50:.0f})", [e for e in events if e["neg_mag"] < p50])
rpt(f"Neg mag >= median ({p50:.0f})", [e for e in events if e["neg_mag"] >= p50])
rpt(f"Neg mag >= 75th ({p75:.0f})", [e for e in events if e["neg_mag"] >= p75])
print()

print("--- DAY OF WEEK ---")
rpt("ALL", events)
for d in ["Mon", "Tue", "Wed", "Thu", "Fri"]:
    rpt(d, [e for e in events if e["dow"] == d])
print()

print("--- TIME FIRST BELOW ---")
rpt("ALL", events)
rpt("Before 10:00", [e for e in events if int(e["first_below"].split(":")[0]) < 10])
rpt("10:00-10:59", [e for e in events if int(e["first_below"].split(":")[0]) == 10])
rpt("11:00-11:59", [e for e in events if int(e["first_below"].split(":")[0]) == 11])
rpt("12:00-12:59", [e for e in events if int(e["first_below"].split(":")[0]) == 12])
rpt("13:00+", [e for e in events if int(e["first_below"].split(":")[0]) >= 13])
print()

# COMBINED
print("=" * 85)
print("  BEST COMBOS (target: 80%+ T1, 7+/month)")
print("=" * 85)
print()

combos = [
    ("ALL", events),
    ("Neg mag >= median", [e for e in events if e["neg_mag"] >= p50]),
    ("Neg mag >= 75th", [e for e in events if e["neg_mag"] >= p75]),
    ("N2P >= 40", [e for e in events if e["n2p"] >= 40]),
    ("N2P >= 60", [e for e in events if e["n2p"] >= 60]),
    ("Gap > 0", [e for e in events if e["gap"] > 0]),
    ("Gap > 10", [e for e in events if e["gap"] > 10]),
    ("Negative regime", [e for e in events if e["regime"] == "negative"]),
    ("Monday", [e for e in events if e["dow"] == "Mon"]),
    ("", None),
    ("Neg mag>=median + N2P>=40", [e for e in events if e["neg_mag"] >= p50 and e["n2p"] >= 40]),
    ("Neg mag>=median + Gap>0", [e for e in events if e["neg_mag"] >= p50 and e["gap"] > 0]),
    ("Neg mag>=75th + N2P>=40", [e for e in events if e["neg_mag"] >= p75 and e["n2p"] >= 40]),
    ("Neg mag>=75th + Gap>0", [e for e in events if e["neg_mag"] >= p75 and e["gap"] > 0]),
    ("N2P>=40 + Gap>0", [e for e in events if e["n2p"] >= 40 and e["gap"] > 0]),
    ("N2P>=40 + Neg regime", [e for e in events if e["n2p"] >= 40 and e["regime"] == "negative"]),
    ("N2P>=40 + Mon", [e for e in events if e["n2p"] >= 40 and e["dow"] == "Mon"]),
    ("Neg regime + Gap>0", [e for e in events if e["regime"] == "negative" and e["gap"] > 0]),
    ("Neg regime + Mon", [e for e in events if e["regime"] == "negative" and e["dow"] == "Mon"]),
    ("", None),
    ("Neg mag>=median + N2P>=40 + Gap>0", [e for e in events if e["neg_mag"] >= p50 and e["n2p"] >= 40 and e["gap"] > 0]),
    ("Neg mag>=75th + N2P>=40 + Gap>0", [e for e in events if e["neg_mag"] >= p75 and e["n2p"] >= 40 and e["gap"] > 0]),
    ("N2P>=40 + Gap>0 + Neg regime", [e for e in events if e["n2p"] >= 40 and e["gap"] > 0 and e["regime"] == "negative"]),
    ("N2P>=40 + Gap>0 + Mon", [e for e in events if e["n2p"] >= 40 and e["gap"] > 0 and e["dow"] == "Mon"]),
    ("Neg mag>=median + Gap>0 + Neg regime", [e for e in events if e["neg_mag"] >= p50 and e["gap"] > 0 and e["regime"] == "negative"]),
    ("Neg regime + N2P>=40 + Mon", [e for e in events if e["regime"] == "negative" and e["n2p"] >= 40 and e["dow"] == "Mon"]),
]

for label, filt in combos:
    if filt is None: print(); continue
    if label == "": continue
    rpt(label, filt)

# Full log
print()
print("=" * 85)
print("  FULL LOG")
print("=" * 85)
print()
print(f"  {'#':>3} {'Date':>10} {'D':>3} {'Spot':>6} {'-GEX':>6} {'+GEX':>6} {'Gap':>5} {'N2P':>4} {'1stB':>5} {'Dip':>5} {'DipT':>5} {'T1':>3} {'T1T':>5} {'T2':>3} {'T2T':>5} {'MFE':>5} {'Reg':>3}")
for i, e in enumerate(events, 1):
    r = "Y" if e["recovered"] else "N"
    p = "Y" if e["reached_pos"] else "N"
    rt = e["rec_time"][:5] if e["rec_time"] else "  -  "
    pt = e["pos_time"][:5] if e["pos_time"] else "  -  "
    reg = "+" if e["regime"] == "positive" else "-"
    print(f"  {i:>3} {e['date']:>10} {e['dow']:>3} {e['spot']:>6.0f} {e['neg']:>6.0f} {e['pos']:>6.0f} {e['gap']:>+5.0f} {e['n2p']:>4.0f} {e['first_below']:>5} {e['dip']:>5.1f} {e['low_time']:>5} {r:>3} {rt:>5} {p:>3} {pt:>5} {e['mfe']:>5.1f} {reg:>3}")
