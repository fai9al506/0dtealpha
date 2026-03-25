"""Clean raw study — no trading logic. Just facts about price vs GEX levels."""
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

dow_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}
events = []

for day in dataset:
    neg = day["strongest_neg"]
    pos = day["strongest_pos"]
    spot = day["open"]
    bars = intraday.get(day["date_int"], [])
    if not bars or not neg: continue
    ratio = spot / bars[0]["open"] if bars[0]["open"] > 0 else 0
    if ratio < 5 or ratio > 15: continue  # sanity check

    gap = spot - neg
    n2p = pos - neg

    went_below = False
    first_below_time = None
    low_below = None
    low_time = None
    recovered = False
    rec_time = None
    reached_pos = False
    pos_time = None

    for b in bars:
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

        if went_below and not recovered and hi > neg:
            recovered = True
            rec_time = b["time"]

        if went_below and not reached_pos and hi >= pos:
            reached_pos = True
            pos_time = b["time"]

    if not went_below:
        continue

    max_dip = neg - low_below

    # MFE from low
    found_low = False
    highest_after = low_below
    for b in bars:
        if not found_low:
            if b["time"] == low_time:
                found_low = True
                highest_after = b["high"] * ratio
        else:
            highest_after = max(highest_after, b["high"] * ratio)
    mfe = highest_after - low_below

    last_close = bars[-1]["close"] * ratio
    d = dt_date.fromisoformat(day["date"])

    events.append({
        "date": day["date"], "dow": dow_map[d.weekday()],
        "spot": spot, "neg": neg, "pos": pos,
        "gap": round(gap, 1), "n2p": round(n2p, 1),
        "first_below": first_below_time,
        "max_dip": round(max_dip, 1), "dip_time": low_time,
        "recovered": recovered, "rec_time": rec_time,
        "reached_pos": reached_pos, "pos_time": pos_time,
        "mfe": round(mfe, 1),
        "close_vs_neg": round(last_close - neg, 1),
        "regime": day["regime"],
    })

n = len(events)
n_rec = sum(1 for e in events if e["recovered"])
n_pos = sum(1 for e in events if e["reached_pos"])

print("=" * 80)
print("  RAW STUDY: What happens when SPX drops below -GEX?")
print("=" * 80)
print()
print(f"  Total days below -GEX: {n}/{len(dataset)} ({n / len(dataset) * 100:.0f}%) = {n / 52:.1f}/week")
print(f"  Recovered above -GEX:  {n_rec}/{n} ({n_rec / n * 100:.0f}%)")
print(f"  Reached +GEX:          {n_pos}/{n} ({n_pos / n * 100:.0f}%)")
print()
print(f"  Avg max dip below -GEX:   {sum(e['max_dip'] for e in events) / n:.1f} pts")
print(f"  Avg -GEX to +GEX dist:    {sum(e['n2p'] for e in events) / n:.1f} pts")
print(f"  Avg MFE from low:          {sum(e['mfe'] for e in events) / n:.1f} pts")
print()

# Dip distribution
print("  --- Max Dip Below -GEX Distribution ---")
for lo, hi in [(0, 5), (5, 10), (10, 15), (15, 20), (20, 30), (30, 50), (50, 999)]:
    b = [e for e in events if lo <= e["max_dip"] < hi]
    if not b: continue
    rec = sum(1 for e in b if e["recovered"])
    pr = sum(1 for e in b if e["reached_pos"])
    mfe = sum(e["mfe"] for e in b) / len(b)
    lbl = f"{lo}-{hi}" if hi < 999 else f"{lo}+"
    print(f"    Dip {lbl:>5}: {len(b):>3} days  rec={rec}/{len(b)} ({rec / len(b) * 100:.0f}%)  +GEX={pr}/{len(b)} ({pr / len(b) * 100:.0f}%)  avgMFE={mfe:.1f}")
print()

# T1 (entry at low -> -GEX)
rec_ev = [e for e in events if e["recovered"]]
t1 = [e["max_dip"] for e in rec_ev]
print(f"  --- T1: Low -> -GEX recovery ({len(rec_ev)} trades) ---")
print(f"    Avg:    {sum(t1) / len(t1):.1f} pts")
print(f"    Median: {sorted(t1)[len(t1) // 2]:.1f} pts")
print(f"    >= 5:   {sum(1 for p in t1 if p >= 5)}/{len(t1)} ({sum(1 for p in t1 if p >= 5) / len(t1) * 100:.0f}%)")
print(f"    >= 10:  {sum(1 for p in t1 if p >= 10)}/{len(t1)} ({sum(1 for p in t1 if p >= 10) / len(t1) * 100:.0f}%)")
print(f"    >= 15:  {sum(1 for p in t1 if p >= 15)}/{len(t1)} ({sum(1 for p in t1 if p >= 15) / len(t1) * 100:.0f}%)")
print(f"    >= 20:  {sum(1 for p in t1 if p >= 20)}/{len(t1)} ({sum(1 for p in t1 if p >= 20) / len(t1) * 100:.0f}%)")
print()

# T2 (entry at low -> +GEX)
pos_ev = [e for e in events if e["reached_pos"]]
t2 = [e["max_dip"] + e["n2p"] for e in pos_ev]
print(f"  --- T2: Low -> +GEX reached ({len(pos_ev)} trades) ---")
print(f"    Avg:    {sum(t2) / len(t2):.1f} pts")
print(f"    Median: {sorted(t2)[len(t2) // 2]:.1f} pts")
print(f"    Min:    {min(t2):.1f} | Max: {max(t2):.1f}")
print()

# By gap
print("  --- By Gap at Open ---")
for label, glo, ghi in [("Gap<0", -999, 0), ("Gap 0-10", 0, 10), ("Gap 10-25", 10, 25),
                          ("Gap 25-50", 25, 50), ("Gap 50+", 50, 999)]:
    g = [e for e in events if glo <= e["gap"] < ghi]
    if not g: continue
    rec = sum(1 for e in g if e["recovered"])
    pr = sum(1 for e in g if e["reached_pos"])
    dip = sum(e["max_dip"] for e in g) / len(g)
    mfe = sum(e["mfe"] for e in g) / len(g)
    print(f"    {label:<12} {len(g):>3}d  rec={rec / len(g) * 100:>3.0f}%  +GEX={pr / len(g) * 100:>3.0f}%  dip={dip:>5.1f}  MFE={mfe:>5.1f}")
print()

# By time
print("  --- By Time First Below ---")
tb = defaultdict(list)
for e in events:
    h = int(e["first_below"].split(":")[0])
    if h < 10: bk = "09:30-09:59"
    elif h < 11: bk = "10:00-10:59"
    elif h < 12: bk = "11:00-11:59"
    elif h < 13: bk = "12:00-12:59"
    else: bk = "13:00+"
    tb[bk].append(e)
for bk in sorted(tb.keys()):
    te = tb[bk]
    rec = sum(1 for e in te if e["recovered"])
    pr = sum(1 for e in te if e["reached_pos"])
    dip = sum(e["max_dip"] for e in te) / len(te)
    mfe = sum(e["mfe"] for e in te) / len(te)
    print(f"    {bk:>14}: {len(te):>3}d  rec={rec / len(te) * 100:>3.0f}%  +GEX={pr / len(te) * 100:>3.0f}%  dip={dip:>5.1f}  MFE={mfe:>5.1f}")
print()

# FULL LOG
print("=" * 80)
print("  FULL LOG: Every day price went below -GEX")
print("=" * 80)
print()
hdr = f"  {'#':>3} {'Date':>10} {'D':>3} {'Spot':>6} {'-GEX':>6} {'+GEX':>6} {'Gap':>5} {'N2P':>4} {'1stB':>5} {'MaxDip':>6} {'DipT':>5} {'Rec':>3} {'RecT':>5} {'+G':>3} {'+GT':>5} {'MFE':>5} {'Cl-N':>5}"
print(hdr)
print(f"  {'---':>3} {'----------':>10} {'---':>3} {'------':>6} {'------':>6} {'------':>6} {'-----':>5} {'----':>4} {'-----':>5} {'------':>6} {'-----':>5} {'---':>3} {'-----':>5} {'---':>3} {'-----':>5} {'-----':>5} {'-----':>5}")

for i, e in enumerate(events, 1):
    rec = "Y" if e["recovered"] else "n"
    rpos = "Y" if e["reached_pos"] else "n"
    rt = e["rec_time"][:5] if e["rec_time"] else "  -  "
    pt = e["pos_time"][:5] if e["pos_time"] else "  -  "
    dt2 = e["dip_time"][:5]
    fb = e["first_below"][:5]
    print(f"  {i:>3} {e['date']:>10} {e['dow']:>3} {e['spot']:>6.0f} {e['neg']:>6.0f} {e['pos']:>6.0f} {e['gap']:>+5.0f} {e['n2p']:>4.0f} {fb:>5} {e['max_dip']:>6.1f} {dt2:>5} {rec:>3} {rt:>5} {rpos:>3} {pt:>5} {e['mfe']:>5.1f} {e['close_vs_neg']:>+5.0f}")
