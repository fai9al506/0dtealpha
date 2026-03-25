"""Dip >= 10 pts study: only count if price went 10+ pts below -GEX.
Track: recovery rate, time to recover, time at the low, full timeline."""
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
    if gap <= 0: continue  # opened above -GEX only
    n2p = pos - neg

    # Track the full timeline bar by bar
    went_below = False
    hit_10_below = False
    first_below_time = None
    first_below_ms = None
    hit_10_time = None
    hit_10_ms = None
    max_dip = 0
    max_dip_time = None
    max_dip_ms = None
    recovered = False  # close of a bar > -GEX, after hitting 10 below
    rec_time = None
    rec_ms = None
    reached_pos = False
    pos_time = None

    for b in bars_day:
        lo = b["low"] * ratio
        hi = b["high"] * ratio
        cl = b["close"] * ratio
        ms = b["ms_of_day"]

        if not went_below and lo < neg:
            went_below = True
            first_below_time = b["time"]
            first_below_ms = ms

        if went_below:
            dip = neg - lo
            if dip > max_dip:
                max_dip = dip
                max_dip_time = b["time"]
                max_dip_ms = ms

            if not hit_10_below and dip >= 10:
                hit_10_below = True
                hit_10_time = b["time"]
                hit_10_ms = ms

            # Recovery = bar CLOSES above -GEX, AFTER we hit 10 below
            if hit_10_below and not recovered and cl > neg:
                recovered = True
                rec_time = b["time"]
                rec_ms = ms

            if went_below and not reached_pos and hi >= pos:
                reached_pos = True
                pos_time = b["time"]

    if not hit_10_below: continue  # skip if never dipped 10+
    if max_dip > 500: continue  # bad data

    # Time calculations
    time_to_10 = (hit_10_ms - first_below_ms) / 60000 if first_below_ms and hit_10_ms else 0
    time_10_to_rec = (rec_ms - hit_10_ms) / 60000 if rec_ms and hit_10_ms else None
    time_dip_to_rec = (rec_ms - max_dip_ms) / 60000 if rec_ms and max_dip_ms else None
    time_total = (rec_ms - first_below_ms) / 60000 if rec_ms and first_below_ms else None

    dt = dt_date.fromisoformat(d)
    events.append({
        "date": d, "dow": dow_map[dt.weekday()],
        "spot": spot, "neg": neg, "pos": pos,
        "gap": round(gap, 1), "n2p": round(n2p, 1),
        "max_dip": round(max_dip, 1),
        "first_below": first_below_time,
        "hit_10_time": hit_10_time,
        "max_dip_time": max_dip_time,
        "recovered": recovered, "rec_time": rec_time,
        "reached_pos": reached_pos, "pos_time": pos_time,
        "time_to_10": round(time_to_10),
        "time_10_to_rec": round(time_10_to_rec) if time_10_to_rec else None,
        "time_dip_to_rec": round(time_dip_to_rec) if time_dip_to_rec else None,
        "time_total": round(time_total) if time_total else None,
        "regime": levels["regime"],
    })

n = len(events)
n_rec = sum(1 for e in events if e["recovered"])
n_pos = sum(1 for e in events if e["reached_pos"])

print("=" * 85)
print("  DIP >= 10 PTS STUDY (opened above -GEX, dipped 10+ below)")
print("=" * 85)
print()
print(f"  Total events:      {n}  ({n/12:.1f}/month, {n/52:.1f}/week)")
print(f"  Recovered (T1):    {n_rec}/{n} ({n_rec/n*100:.1f}%)")
print(f"  Reached +GEX (T2): {n_pos}/{n} ({n_pos/n*100:.1f}%)")
print(f"  NOT recovered:     {n - n_rec}/{n}")
print()

# Dip stats
dips = [e["max_dip"] for e in events]
print(f"  Dip stats: min={min(dips):.1f}  median={sorted(dips)[len(dips)//2]:.1f}  avg={sum(dips)/len(dips):.1f}  max={max(dips):.1f}")
print()

# Recovery time stats (only for recovered trades)
rec_events = [e for e in events if e["recovered"]]
if rec_events:
    t10r = [e["time_10_to_rec"] for e in rec_events if e["time_10_to_rec"] is not None]
    tdr = [e["time_dip_to_rec"] for e in rec_events if e["time_dip_to_rec"] is not None]
    ttot = [e["time_total"] for e in rec_events if e["time_total"] is not None]

    print("  --- Recovery Time (minutes) ---")
    if t10r:
        print(f"  From hitting -10 to recovery:")
        print(f"    Min={min(t10r):.0f}m  Median={sorted(t10r)[len(t10r)//2]:.0f}m  Avg={sum(t10r)/len(t10r):.0f}m  Max={max(t10r):.0f}m")
        # Distribution
        for lo, hi in [(0, 15), (15, 30), (30, 60), (60, 120), (120, 240), (240, 999)]:
            c = sum(1 for t in t10r if lo <= t < hi)
            if c: print(f"    {lo}-{hi}min: {c} trades ({c/len(t10r)*100:.0f}%)")
    print()
    if tdr:
        print(f"  From max dip to recovery:")
        print(f"    Min={min(tdr):.0f}m  Median={sorted(tdr)[len(tdr)//2]:.0f}m  Avg={sum(tdr)/len(tdr):.0f}m  Max={max(tdr):.0f}m")
        for lo, hi in [(0, 15), (15, 30), (30, 60), (60, 120), (120, 240), (240, 999)]:
            c = sum(1 for t in tdr if lo <= t < hi)
            if c: print(f"    {lo}-{hi}min: {c} trades ({c/len(tdr)*100:.0f}%)")
    print()
    if ttot:
        print(f"  Total time (first below to recovery):")
        print(f"    Min={min(ttot):.0f}m  Median={sorted(ttot)[len(ttot)//2]:.0f}m  Avg={sum(ttot)/len(ttot):.0f}m  Max={max(ttot):.0f}m")
    print()

# Failures
fails = [e for e in events if not e["recovered"]]
if fails:
    print(f"  --- FAILURES ({len(fails)} trades that did NOT recover) ---")
    for e in fails:
        print(f"    {e['date']} {e['dow']}  -GEX={e['neg']:.0f}  gap={e['gap']:+.0f}  dip={e['max_dip']:.1f}  dipTime={e['max_dip_time']}  regime={e['regime']}")
    print()

# FULL LOG
print("=" * 85)
print("  FULL LOG: Dip >= 10 pts")
print("  1stB=first below  -10T=hit -10  DipT=max dip time  RecT=recovery time")
print("  t10R=min from -10 to rec  tDR=min from max dip to rec")
print("=" * 85)
print()
hdr = f"  {'#':>3} {'Date':>10} {'D':>3} {'-GEX':>6} {'Gap':>4} {'Dip':>5} {'1stB':>5} {'-10T':>5} {'DipT':>5} {'T1':>2} {'RecT':>5} {'t10R':>5} {'tDR':>5} {'T2':>2} {'+GT':>5} {'Reg':>3}"
print(hdr)

for i, e in enumerate(events, 1):
    r = "Y" if e["recovered"] else "N"
    p = "Y" if e["reached_pos"] else "N"
    rt = e["rec_time"][:5] if e["rec_time"] else "  -  "
    pt = e["pos_time"][:5] if e["pos_time"] else "  -  "
    t10r = f"{e['time_10_to_rec']:>4.0f}m" if e["time_10_to_rec"] is not None else "    -"
    tdr = f"{e['time_dip_to_rec']:>4.0f}m" if e["time_dip_to_rec"] is not None else "    -"
    reg = "+" if e["regime"] == "positive" else "-"
    print(f"  {i:>3} {e['date']:>10} {e['dow']:>3} {e['neg']:>6.0f} {e['gap']:>+4.0f} {e['max_dip']:>5.1f} {e['first_below']:>5} {e['hit_10_time']:>5} {e['max_dip_time']:>5} {r:>2} {rt:>5} {t10r:>5} {tdr:>5} {p:>2} {pt:>5} {reg:>3}")
