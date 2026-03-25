"""Actionable filters only — things we know at 10 AM BEFORE the dip."""
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
    total_neg_mag = abs(sum(v for _, v in neg))
    total_pos_mag = sum(v for _, v in pos)
    ratio = total_pos_mag / max(total_neg_mag, 1)
    return {
        "strongest_neg": top_neg[0][0], "strongest_pos": top_pos[0][0],
        "regime": "positive" if total_gex > 0 else "negative",
        "pos_neg_ratio": round(ratio, 2),
        "neg_count": len(top_neg),
        "strongest_neg_gex": abs(top_neg[0][1]),
        "strongest_pos_gex": top_pos[0][1],
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
    neg = levels["strongest_neg"]; pos = levels["strongest_pos"]
    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    if l >= neg: continue  # only days that dipped below
    dt = dt_date.fromisoformat(d)
    events.append({
        "date": d, "dow": dow_map[dt.weekday()], "dow_num": dt.weekday(),
        "o": o, "h": h, "l": l, "c": c,
        "neg": neg, "pos": pos,
        "gap": round(o - neg, 1), "n2p": round(pos - neg, 1),
        "dip": round(neg - l, 1),
        "close_above": c > neg, "reached_pos": h >= pos,
        "regime": levels["regime"],
        "ratio": levels["pos_neg_ratio"],
        "neg_count": levels["neg_count"],
        "neg_mag": levels["strongest_neg_gex"],
        "pos_mag": levels["strongest_pos_gex"],
    })

n = len(events)
w = sum(1 for e in events if e["close_above"])

def rpt(label, filt):
    if not filt:
        print(f"  {label:<55}   0")
        return
    t = len(filt)
    w = sum(1 for e in filt if e["close_above"])
    pg = sum(1 for e in filt if e["reached_pos"])
    ad = sum(e["dip"] for e in filt) / t
    print(f"  {label:<55} {t:>3}  rec={w/t*100:>5.1f}%  +GEX={pg/t*100:>4.0f}%  avgDip={ad:>5.1f}")

print("=" * 80)
print("  ACTIONABLE FILTERS (known at 10 AM, BEFORE the dip)")
print("=" * 80)
print()
print(f"  Base: {n} events, {w} recovered ({w/n*100:.0f}%)")
print()

print("--- REGIME ---")
rpt("ALL", events)
rpt("Positive", [e for e in events if e["regime"] == "positive"])
rpt("Negative", [e for e in events if e["regime"] == "negative"])
print()

print("--- GAP AT OPEN ---")
rpt("ALL", events)
rpt("Gap < 0 (opened below -GEX)", [e for e in events if e["gap"] < 0])
rpt("Gap 0-5", [e for e in events if 0 <= e["gap"] < 5])
rpt("Gap 5-15", [e for e in events if 5 <= e["gap"] < 15])
rpt("Gap 15-30", [e for e in events if 15 <= e["gap"] < 30])
rpt("Gap 30-50", [e for e in events if 30 <= e["gap"] < 50])
rpt("Gap 50+", [e for e in events if e["gap"] >= 50])
print()

print("--- -GEX to +GEX DISTANCE ---")
rpt("ALL", events)
rpt("N2P < 20", [e for e in events if e["n2p"] < 20])
rpt("N2P 20-40", [e for e in events if 20 <= e["n2p"] < 40])
rpt("N2P 40-60", [e for e in events if 40 <= e["n2p"] < 60])
rpt("N2P 60-100", [e for e in events if 60 <= e["n2p"] < 100])
rpt("N2P 100+", [e for e in events if e["n2p"] >= 100])
print()

print("--- POS/NEG RATIO ---")
rpt("ALL", events)
rpt("Ratio < 0.5 (neg dominant)", [e for e in events if e["ratio"] < 0.5])
rpt("Ratio 0.5-1.0", [e for e in events if 0.5 <= e["ratio"] < 1.0])
rpt("Ratio 1.0-2.0", [e for e in events if 1.0 <= e["ratio"] < 2.0])
rpt("Ratio 2.0+", [e for e in events if e["ratio"] >= 2.0])
print()

print("--- NEG CLUSTER ---")
rpt("ALL", events)
rpt("Cluster <= 2", [e for e in events if e["neg_count"] <= 2])
rpt("Cluster = 3", [e for e in events if e["neg_count"] == 3])
rpt("Cluster = 4", [e for e in events if e["neg_count"] == 4])
rpt("Cluster = 5", [e for e in events if e["neg_count"] == 5])
print()

print("--- STRONGEST -GEX MAGNITUDE ---")
rpt("ALL", events)
mags = sorted(e["neg_mag"] for e in events)
p25 = mags[len(mags)//4]; p50 = mags[len(mags)//2]; p75 = mags[3*len(mags)//4]
print(f"  (quartiles: 25%={p25:.0f}  50%={p50:.0f}  75%={p75:.0f})")
rpt("Neg mag < 25th pct (weak wall)", [e for e in events if e["neg_mag"] < p25])
rpt("Neg mag 25-50th", [e for e in events if p25 <= e["neg_mag"] < p50])
rpt("Neg mag 50-75th", [e for e in events if p50 <= e["neg_mag"] < p75])
rpt("Neg mag > 75th pct (strong wall)", [e for e in events if e["neg_mag"] >= p75])
print()

print("--- DAY OF WEEK ---")
rpt("ALL", events)
for d in ["Mon", "Tue", "Wed", "Thu", "Fri"]:
    rpt(d, [e for e in events if e["dow"] == d])
print()

# ================================================================
print("=" * 80)
print("  BEST COMBOS")
print("=" * 80)
print()

combos = [
    ("ALL", events),
    ("--- Single best ---", None),
    ("N2P >= 60", [e for e in events if e["n2p"] >= 60]),
    ("Monday", [e for e in events if e["dow"] == "Mon"]),
    ("Neg regime", [e for e in events if e["regime"] == "negative"]),
    ("Gap 30-50", [e for e in events if 30 <= e["gap"] < 50]),
    ("Ratio < 0.5", [e for e in events if e["ratio"] < 0.5]),
    ("Neg mag > 75th", [e for e in events if e["neg_mag"] >= p75]),
    ("", None),
    ("--- Double combos ---", None),
    ("N2P>=60 + Neg regime", [e for e in events if e["n2p"] >= 60 and e["regime"] == "negative"]),
    ("N2P>=60 + Gap>0", [e for e in events if e["n2p"] >= 60 and e["gap"] > 0]),
    ("N2P>=40 + Gap>0", [e for e in events if e["n2p"] >= 40 and e["gap"] > 0]),
    ("N2P>=40 + Neg regime", [e for e in events if e["n2p"] >= 40 and e["regime"] == "negative"]),
    ("N2P>=40 + Monday", [e for e in events if e["n2p"] >= 40 and e["dow"] == "Mon"]),
    ("N2P>=40 + Ratio<0.5", [e for e in events if e["n2p"] >= 40 and e["ratio"] < 0.5]),
    ("Monday + Gap>0", [e for e in events if e["dow"] == "Mon" and e["gap"] > 0]),
    ("Monday + Neg regime", [e for e in events if e["dow"] == "Mon" and e["regime"] == "negative"]),
    ("Neg regime + Gap>0", [e for e in events if e["regime"] == "negative" and e["gap"] > 0]),
    ("Neg regime + Ratio<0.5", [e for e in events if e["regime"] == "negative" and e["ratio"] < 0.5]),
    ("Neg mag>75th + N2P>=40", [e for e in events if e["neg_mag"] >= p75 and e["n2p"] >= 40]),
    ("Neg mag>75th + Gap>0", [e for e in events if e["neg_mag"] >= p75 and e["gap"] > 0]),
    ("", None),
    ("--- Triple combos ---", None),
    ("N2P>=60 + Neg + Gap>0", [e for e in events if e["n2p"] >= 60 and e["regime"] == "negative" and e["gap"] > 0]),
    ("N2P>=40 + Neg + Gap>0", [e for e in events if e["n2p"] >= 40 and e["regime"] == "negative" and e["gap"] > 0]),
    ("N2P>=40 + Neg + Mon", [e for e in events if e["n2p"] >= 40 and e["regime"] == "negative" and e["dow"] == "Mon"]),
    ("N2P>=40 + Gap>0 + Mon", [e for e in events if e["n2p"] >= 40 and e["gap"] > 0 and e["dow"] == "Mon"]),
    ("N2P>=60 + Gap>0 + Ratio<0.5", [e for e in events if e["n2p"] >= 60 and e["gap"] > 0 and e["ratio"] < 0.5]),
    ("Neg + Gap>0 + Mon", [e for e in events if e["regime"] == "negative" and e["gap"] > 0 and e["dow"] == "Mon"]),
    ("Neg + N2P>=40 + Ratio<0.5", [e for e in events if e["regime"] == "negative" and e["n2p"] >= 40 and e["ratio"] < 0.5]),
    ("Neg mag>75th + N2P>=40 + Gap>0", [e for e in events if e["neg_mag"] >= p75 and e["n2p"] >= 40 and e["gap"] > 0]),
    ("Neg mag>75th + Neg + N2P>=40", [e for e in events if e["neg_mag"] >= p75 and e["regime"] == "negative" and e["n2p"] >= 40]),
]

for label, filt in combos:
    if filt is None:
        if label: print(f"\n  {label}")
        continue
    if label == "":
        continue
    rpt(label, filt)
