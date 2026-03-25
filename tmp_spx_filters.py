"""Filter study: What increases the chance of recovering above -GEX after dipping below?"""
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

    # Total magnitude of negative and positive GEX
    total_neg_mag = abs(sum(v for _, v in neg))
    total_pos_mag = sum(v for _, v in pos)

    # Ratio: how dominant is +GEX vs -GEX
    ratio = total_pos_mag / max(total_neg_mag, 1)

    # Number of strong neg levels (cluster strength)
    neg_count = len(top_neg)

    return {
        "strongest_neg": top_neg[0][0], "strongest_pos": top_pos[0][0],
        "regime": "positive" if total_gex > 0 else "negative",
        "total_gex": total_gex,
        "total_neg_mag": total_neg_mag,
        "total_pos_mag": total_pos_mag,
        "pos_neg_ratio": round(ratio, 2),
        "neg_count": neg_count,
        "neg_levels": [k for k, v in top_neg],
        "pos_levels": [k for k, v in top_pos],
    }

# Build dataset
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
    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    vix = bar.get("volume", 0)  # we don't have VIX in price bar, check chain

    # Check if we have VIX from the chain_snapshots or price data
    # We'll use daily range as volatility proxy
    daily_range = h - l
    daily_range_pct = daily_range / o * 100

    went_below = l < neg
    if not went_below: continue

    dip = neg - l
    gap = o - neg
    n2p = pos - neg
    close_above = c > neg
    reached_pos = h >= pos

    # How many -GEX levels are between spot and low? (cluster below)
    neg_below_spot = [lv for lv in levels["neg_levels"] if lv <= o]
    neg_cluster = len(neg_below_spot)

    # Is there +GEX magnet above -GEX? (pull strength)
    pos_above_neg = [lv for lv in levels["pos_levels"] if lv > neg]
    pos_magnets = len(pos_above_neg)

    # Spot opened above or below -GEX
    opened_above = o > neg

    dt = dt_date.fromisoformat(d)
    events.append({
        "date": d, "dow": dow_map[dt.weekday()], "dow_num": dt.weekday(),
        "open": o, "high": h, "low": l, "close": c,
        "neg": neg, "pos": pos,
        "gap": round(gap, 1), "n2p": round(n2p, 1),
        "dip": round(dip, 1),
        "close_above": close_above,
        "reached_pos": reached_pos,
        "regime": levels["regime"],
        "total_gex": levels["total_gex"],
        "pos_neg_ratio": levels["pos_neg_ratio"],
        "total_neg_mag": levels["total_neg_mag"],
        "total_pos_mag": levels["total_pos_mag"],
        "neg_count": neg_cluster,
        "pos_magnets": pos_magnets,
        "opened_above": opened_above,
        "daily_range": round(daily_range, 1),
        "daily_range_pct": round(daily_range_pct, 2),
    })

n = len(events)
wins = sum(1 for e in events if e["close_above"])
print(f"Total events (dipped below -GEX): {n}")
print(f"Close above -GEX (WIN): {wins}/{n} ({wins/n*100:.0f}%)")
print()

def report(label, filtered):
    if not filtered:
        print(f"  {label:<55} {'--':>4}")
        return
    total = len(filtered)
    w = sum(1 for e in filtered if e["close_above"])
    wr = w / total * 100
    pg = sum(1 for e in filtered if e["reached_pos"])
    avg_dip = sum(e["dip"] for e in filtered) / total
    print(f"  {label:<55} {total:>4}  WR={wr:>5.1f}%  +GEX={pg/total*100:>4.0f}%  avgDip={avg_dip:>5.1f}")


# ================================================================
# FILTER: GEX Regime
# ================================================================
print("=" * 80)
print("  FILTER: GEX Regime (total GEX positive vs negative)")
print("=" * 80)
print()
report("ALL", events)
report("Positive GEX regime", [e for e in events if e["regime"] == "positive"])
report("Negative GEX regime", [e for e in events if e["regime"] == "negative"])
print()

# ================================================================
# FILTER: Pos/Neg GEX Ratio
# ================================================================
print("=" * 80)
print("  FILTER: +GEX / -GEX Ratio (how dominant is positive over negative)")
print("=" * 80)
print()
report("ALL", events)
for lo, hi in [(0, 0.5), (0.5, 0.8), (0.8, 1.0), (1.0, 1.5), (1.5, 2.0), (2.0, 999)]:
    lbl = f"Ratio {lo}-{hi}" if hi < 999 else f"Ratio {lo}+"
    report(lbl, [e for e in events if lo <= e["pos_neg_ratio"] < hi])
print()

# ================================================================
# FILTER: Gap at Open (spot vs -GEX)
# ================================================================
print("=" * 80)
print("  FILTER: Gap at Open (how far spot opened above -GEX)")
print("=" * 80)
print()
report("ALL", events)
report("Gap < 0 (opened BELOW -GEX)", [e for e in events if e["gap"] < 0])
report("Gap 0-5", [e for e in events if 0 <= e["gap"] < 5])
report("Gap 5-15", [e for e in events if 5 <= e["gap"] < 15])
report("Gap 15-30", [e for e in events if 15 <= e["gap"] < 30])
report("Gap 30-50", [e for e in events if 30 <= e["gap"] < 50])
report("Gap 50+", [e for e in events if e["gap"] >= 50])
report("Opened ABOVE -GEX (gap > 0)", [e for e in events if e["opened_above"]])
print()

# ================================================================
# FILTER: -GEX to +GEX distance
# ================================================================
print("=" * 80)
print("  FILTER: -GEX to +GEX Distance (room for the bounce)")
print("=" * 80)
print()
report("ALL", events)
for lo, hi in [(0, 15), (15, 25), (25, 40), (40, 60), (60, 100), (100, 999)]:
    lbl = f"N2P {lo}-{hi}" if hi < 999 else f"N2P {lo}+"
    report(lbl, [e for e in events if lo <= e["n2p"] < hi])
print()

# ================================================================
# FILTER: Number of -GEX levels clustered (wall strength)
# ================================================================
print("=" * 80)
print("  FILTER: -GEX Cluster (# of strong neg levels below spot)")
print("=" * 80)
print()
report("ALL", events)
for c in sorted(set(e["neg_count"] for e in events)):
    report(f"Neg cluster = {c}", [e for e in events if e["neg_count"] == c])
print()

# ================================================================
# FILTER: +GEX Magnets above -GEX
# ================================================================
print("=" * 80)
print("  FILTER: +GEX Magnets above -GEX (pull strength)")
print("=" * 80)
print()
report("ALL", events)
for c in sorted(set(e["pos_magnets"] for e in events)):
    report(f"Pos magnets = {c}", [e for e in events if e["pos_magnets"] == c])
print()

# ================================================================
# FILTER: Day of Week
# ================================================================
print("=" * 80)
print("  FILTER: Day of Week")
print("=" * 80)
print()
report("ALL", events)
for dow in ["Mon", "Tue", "Wed", "Thu", "Fri"]:
    report(dow, [e for e in events if e["dow"] == dow])
print()

# ================================================================
# FILTER: Daily Range (volatility proxy — lower range = calmer = more likely to recover)
# ================================================================
print("=" * 80)
print("  FILTER: Daily Range % (volatility proxy)")
print("=" * 80)
print()
report("ALL", events)
for lo, hi in [(0, 0.5), (0.5, 1.0), (1.0, 1.5), (1.5, 2.0), (2.0, 999)]:
    lbl = f"Range {lo}-{hi}%" if hi < 999 else f"Range {lo}%+"
    report(lbl, [e for e in events if lo <= e["daily_range_pct"] < hi])
print()

# ================================================================
# FILTER: Dip Size (shallow vs deep)
# ================================================================
print("=" * 80)
print("  FILTER: Dip Size")
print("=" * 80)
print()
report("ALL", events)
for lo, hi in [(0, 10), (10, 20), (20, 30), (30, 50), (50, 999)]:
    lbl = f"Dip {lo}-{hi}" if hi < 999 else f"Dip {lo}+"
    report(lbl, [e for e in events if lo <= e["dip"] < hi])
print()

# ================================================================
# COMBINED FILTERS — best combos
# ================================================================
print("=" * 80)
print("  COMBINED FILTERS")
print("=" * 80)
print()

report("ALL", events)
print()
print("  --- Regime + Gap ---")
report("Positive + Gap>0", [e for e in events if e["regime"] == "positive" and e["gap"] > 0])
report("Positive + Gap>10", [e for e in events if e["regime"] == "positive" and e["gap"] > 10])
report("Negative + Gap>0", [e for e in events if e["regime"] == "negative" and e["gap"] > 0])
report("Negative + Gap>10", [e for e in events if e["regime"] == "negative" and e["gap"] > 10])
print()

print("  --- Regime + N2P ---")
report("Positive + N2P>=25", [e for e in events if e["regime"] == "positive" and e["n2p"] >= 25])
report("Positive + N2P>=40", [e for e in events if e["regime"] == "positive" and e["n2p"] >= 40])
report("Negative + N2P>=25", [e for e in events if e["regime"] == "negative" and e["n2p"] >= 25])
report("Negative + N2P>=40", [e for e in events if e["regime"] == "negative" and e["n2p"] >= 40])
print()

print("  --- Regime + Range ---")
report("Positive + Range<1%", [e for e in events if e["regime"] == "positive" and e["daily_range_pct"] < 1.0])
report("Positive + Range<1.5%", [e for e in events if e["regime"] == "positive" and e["daily_range_pct"] < 1.5])
report("Negative + Range<1%", [e for e in events if e["regime"] == "negative" and e["daily_range_pct"] < 1.0])
report("Negative + Range<1.5%", [e for e in events if e["regime"] == "negative" and e["daily_range_pct"] < 1.5])
print()

print("  --- Gap + Range ---")
report("Gap>0 + Range<1%", [e for e in events if e["gap"] > 0 and e["daily_range_pct"] < 1.0])
report("Gap>0 + Range<1.5%", [e for e in events if e["gap"] > 0 and e["daily_range_pct"] < 1.5])
report("Gap>10 + Range<1.5%", [e for e in events if e["gap"] > 10 and e["daily_range_pct"] < 1.5])
print()

print("  --- Ratio + Gap ---")
report("Ratio>=1.0 + Gap>0", [e for e in events if e["pos_neg_ratio"] >= 1.0 and e["gap"] > 0])
report("Ratio>=1.0 + Gap>10", [e for e in events if e["pos_neg_ratio"] >= 1.0 and e["gap"] > 10])
report("Ratio>=1.5 + Gap>0", [e for e in events if e["pos_neg_ratio"] >= 1.5 and e["gap"] > 0])
print()

print("  --- DOW + Regime ---")
report("Wed-Fri + Positive", [e for e in events if e["dow_num"] >= 2 and e["regime"] == "positive"])
report("Wed-Fri + Positive + Gap>0", [e for e in events if e["dow_num"] >= 2 and e["regime"] == "positive" and e["gap"] > 0])
report("Not Mon + Positive", [e for e in events if e["dow_num"] >= 1 and e["regime"] == "positive"])
report("Not Mon + Positive + Gap>0", [e for e in events if e["dow_num"] >= 1 and e["regime"] == "positive" and e["gap"] > 0])
print()

print("  --- Triple combos ---")
report("Positive + Gap>0 + N2P>=25", [e for e in events if e["regime"] == "positive" and e["gap"] > 0 and e["n2p"] >= 25])
report("Positive + Gap>0 + N2P>=40", [e for e in events if e["regime"] == "positive" and e["gap"] > 0 and e["n2p"] >= 40])
report("Positive + Gap>0 + Range<1.5%", [e for e in events if e["regime"] == "positive" and e["gap"] > 0 and e["daily_range_pct"] < 1.5])
report("Positive + Gap>10 + N2P>=25", [e for e in events if e["regime"] == "positive" and e["gap"] > 10 and e["n2p"] >= 25])
report("Positive + Gap>10 + Range<1.5%", [e for e in events if e["regime"] == "positive" and e["gap"] > 10 and e["daily_range_pct"] < 1.5])
report("Ratio>=1 + Gap>0 + N2P>=25", [e for e in events if e["pos_neg_ratio"] >= 1.0 and e["gap"] > 0 and e["n2p"] >= 25])
report("Ratio>=1 + Gap>0 + Range<1.5%", [e for e in events if e["pos_neg_ratio"] >= 1.0 and e["gap"] > 0 and e["daily_range_pct"] < 1.5])
report("Gap>0 + N2P>=25 + Range<1.5%", [e for e in events if e["gap"] > 0 and e["n2p"] >= 25 and e["daily_range_pct"] < 1.5])
report("Gap>0 + N2P>=40 + Range<1.5%", [e for e in events if e["gap"] > 0 and e["n2p"] >= 40 and e["daily_range_pct"] < 1.5])
report("Positive + Gap>0 + N2P>=25 + Range<1.5%", [e for e in events if e["regime"] == "positive" and e["gap"] > 0 and e["n2p"] >= 25 and e["daily_range_pct"] < 1.5])
print()

print("  --- Quad combos ---")
report("Pos + Gap>0 + N2P>=25 + Range<1% ", [e for e in events if e["regime"] == "positive" and e["gap"] > 0 and e["n2p"] >= 25 and e["daily_range_pct"] < 1.0])
report("Pos + Gap>0 + N2P>=25 + Not Mon", [e for e in events if e["regime"] == "positive" and e["gap"] > 0 and e["n2p"] >= 25 and e["dow_num"] >= 1])
report("Pos + Gap>0 + N2P>=25 + Wed-Fri", [e for e in events if e["regime"] == "positive" and e["gap"] > 0 and e["n2p"] >= 25 and e["dow_num"] >= 2])
report("Pos + Gap>10 + N2P>=25 + Range<1.5%", [e for e in events if e["regime"] == "positive" and e["gap"] > 10 and e["n2p"] >= 25 and e["daily_range_pct"] < 1.5])
