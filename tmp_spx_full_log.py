"""Full 12-month log: SPX price vs GEX levels. Pure SPX data, no SPY."""
import json, os, sys, io
from collections import defaultdict
from datetime import date as dt_date

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATA_DIR = r"C:\Users\Faisa\stock_gex_data"
SPX_DIR = os.path.join(DATA_DIR, "spx")

with open(os.path.join(SPX_DIR, "prices", "SPX.json")) as f:
    prices = {int(b["date"]): b for b in json.load(f)}

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
    spot = bar["open"]
    gex = compute_gex(records)
    levels = extract_levels(gex, spot)
    if not levels: continue

    neg = levels["strongest_neg"]
    pos = levels["strongest_pos"]
    o = bar["open"]
    h = bar["high"]
    l = bar["low"]
    c = bar["close"]

    went_below = l < neg
    dip = neg - l if went_below else 0
    recovered = h > neg if went_below else False  # high went above -GEX after dipping
    reached_pos = h >= pos
    gap = o - neg
    n2p = pos - neg

    # For "recovered" — if open was already above -GEX and low dipped below,
    # recovered means high came back above -GEX (which is almost always true
    # since high >= open > -GEX in most cases). More meaningful: did CLOSE end above -GEX?
    close_above_neg = c > neg

    dt = dt_date.fromisoformat(d)
    events.append({
        "date": d, "dow": dow_map[dt.weekday()],
        "open": o, "high": h, "low": l, "close": c,
        "neg": neg, "pos": pos,
        "gap": round(gap, 1), "n2p": round(n2p, 1),
        "went_below": went_below,
        "dip": round(dip, 1),
        "recovered_hi": recovered,      # high came back above -GEX
        "close_above": close_above_neg,  # close above -GEX
        "reached_pos": reached_pos,
        "regime": levels["regime"],
    })

# Split into below-GEX events
below = [e for e in events if e["went_below"]]
above = [e for e in events if not e["went_below"]]

print("=" * 95)
print("  SPX vs GEX LEVELS — 12 MONTH STUDY (pure SPX daily data)")
print("=" * 95)
print()
print(f"  Total days: {len(events)}")
print(f"  Days low went below -GEX: {len(below)} ({len(below)/len(events)*100:.0f}%)")
print(f"  Days stayed above -GEX:   {len(above)} ({len(above)/len(events)*100:.0f}%)")
print()

# Of those that went below:
hi_rec = sum(1 for e in below if e["recovered_hi"])
cl_rec = sum(1 for e in below if e["close_above"])
pos_hit = sum(1 for e in below if e["reached_pos"])
print(f"  Of the {len(below)} days that dipped below -GEX:")
print(f"    High came back above -GEX:  {hi_rec}/{len(below)} ({hi_rec/len(below)*100:.0f}%)")
print(f"    CLOSE ended above -GEX:     {cl_rec}/{len(below)} ({cl_rec/len(below)*100:.0f}%)")
print(f"    High reached +GEX:          {pos_hit}/{len(below)} ({pos_hit/len(below)*100:.0f}%)")
print()

# Dip distribution
dips = [e["dip"] for e in below]
dips.sort()
print(f"  Dip distribution:")
print(f"    Min: {dips[0]:.1f} | Median: {dips[len(dips)//2]:.1f} | Avg: {sum(dips)/len(dips):.1f} | Max: {dips[-1]:.1f}")
print()

buckets = [(0,5),(5,10),(10,15),(15,20),(20,30),(30,50),(50,100),(100,999)]
print(f"  {'Dip':>8} {'#':>4} {'CloseAbove':>11} {'Cl%':>5} {'+GEX hit':>9} {'+%':>5}")
for lo, hi in buckets:
    b = [e for e in below if lo <= e["dip"] < hi]
    if not b: continue
    cr = sum(1 for e in b if e["close_above"])
    pr = sum(1 for e in b if e["reached_pos"])
    lbl = f"{lo}-{hi}" if hi < 999 else f"{lo}+"
    print(f"  {lbl:>8} {len(b):>4} {cr:>7}/{len(b):<3} {cr/len(b)*100:>4.0f}% {pr:>5}/{len(b):<3} {pr/len(b)*100:>4.0f}%")
print()

# T1 PnL = dip size (if entered at low and exited at -GEX)
cl_above_events = [e for e in below if e["close_above"]]
print(f"  T1 potential (dip = profit if entered at low, exit at -GEX):")
t1 = [e["dip"] for e in cl_above_events]
print(f"    {len(cl_above_events)} trades closed above -GEX")
print(f"    Avg: {sum(t1)/len(t1):.1f} | Median: {sorted(t1)[len(t1)//2]:.1f}")
print(f"    >=10: {sum(1 for p in t1 if p>=10)}/{len(t1)} ({sum(1 for p in t1 if p>=10)/len(t1)*100:.0f}%)")
print(f"    >=15: {sum(1 for p in t1 if p>=15)}/{len(t1)} ({sum(1 for p in t1 if p>=15)/len(t1)*100:.0f}%)")
print(f"    >=20: {sum(1 for p in t1 if p>=20)}/{len(t1)} ({sum(1 for p in t1 if p>=20)/len(t1)*100:.0f}%)")
print()

# T2 PnL = dip + n2p (if entered at low, price reached +GEX)
pos_events = [e for e in below if e["reached_pos"]]
t2 = [e["dip"] + e["n2p"] for e in pos_events]
print(f"  T2 potential (low to +GEX):")
print(f"    {len(pos_events)} trades reached +GEX")
if t2:
    print(f"    Avg: {sum(t2)/len(t2):.1f} | Median: {sorted(t2)[len(t2)//2]:.1f}")
print()

# ================================================================
# FULL LOG — every day that went below -GEX
# ================================================================
print("=" * 95)
print("  FULL LOG: Every day SPX low went below -GEX")
print("=" * 95)
print()
print(f"  {'#':>3} {'Date':>10} {'D':>3} {'Open':>7} {'High':>7} {'Low':>7} {'Close':>7} {'-GEX':>6} {'+GEX':>6} {'Gap':>5} {'N2P':>4} {'Dip':>5} {'ClAbv':>5} {'+GEX':>4} {'Reg':>3}")
print(f"  {'---':>3} {'----------':>10} {'---':>3} {'-------':>7} {'-------':>7} {'-------':>7} {'-------':>7} {'------':>6} {'------':>6} {'-----':>5} {'----':>4} {'-----':>5} {'-----':>5} {'----':>4} {'---':>3}")

for i, e in enumerate(below, 1):
    ca = "Y" if e["close_above"] else "N"
    rp = "Y" if e["reached_pos"] else "N"
    reg = "+" if e["regime"] == "positive" else "-"
    print(f"  {i:>3} {e['date']:>10} {e['dow']:>3} {e['open']:>7.1f} {e['high']:>7.1f} {e['low']:>7.1f} {e['close']:>7.1f} {e['neg']:>6.0f} {e['pos']:>6.0f} {e['gap']:>+5.0f} {e['n2p']:>4.0f} {e['dip']:>5.1f} {ca:>5} {rp:>4} {reg:>3}")

# Also show days that did NOT close above -GEX
print()
print(f"  --- Days that CLOSED BELOW -GEX ({len(below) - cl_rec}) ---")
for e in below:
    if not e["close_above"]:
        print(f"    {e['date']} {e['dow']} Open={e['open']:.0f} Low={e['low']:.0f} Close={e['close']:.0f} -GEX={e['neg']:.0f} Dip={e['dip']:.1f}")
