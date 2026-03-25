"""Clean multi-symbol summary. T1 = high goes above -GEX (can sell option)."""
import json, os, sys, io
from collections import defaultdict

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATA_DIR = r"C:\Users\Faisa\stock_gex_data"
CONFIGS = {
    "SPX": {"dir": "spx", "min_dip": 10, "proxy": "SPY", "rlo": 5, "rhi": 15},
    "SPY": {"dir": "spy", "min_dip": 1,  "proxy": "SPY", "rlo": 0.9, "rhi": 1.1},
    "QQQ": {"dir": "qqq", "min_dip": 1,  "proxy": "QQQ", "rlo": 0.9, "rhi": 1.1},
    "IWM": {"dir": "iwm", "min_dip": 0.5,"proxy": "IWM", "rlo": 0.9, "rhi": 1.1},
}

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
    return {"strongest_neg": top_neg[0][0], "strongest_pos": top_pos[0][0]}

results = {}
for sym, cfg in CONFIGS.items():
    sym_dir = os.path.join(DATA_DIR, cfg["dir"])
    odir = os.path.join(sym_dir, "options")
    if not os.path.exists(odir): continue
    pf = os.path.join(sym_dir, "prices", f"{sym}.json")
    if not os.path.exists(pf): continue
    with open(pf) as f:
        prices = {int(b["date"]): b for b in json.load(f)}
    ifile = os.path.join(sym_dir, "intraday", f"{cfg['proxy']}_5min.json")
    if not os.path.exists(ifile): continue
    with open(ifile) as f:
        iraw = json.load(f)
    intraday = defaultdict(list)
    for b in iraw:
        intraday[b["date"]].append(b)
    for d in intraday:
        intraday[d].sort(key=lambda x: x["ms_of_day"])

    events = []
    for fn in sorted(os.listdir(odir)):
        if not fn.endswith("_0dte.json"): continue
        d = fn.replace("_0dte.json", ""); d_int = int(d.replace("-", ""))
        bar = prices.get(d_int)
        if not bar: continue
        with open(os.path.join(odir, fn)) as fh:
            records = json.load(fh)
        if not records: continue
        spot = bar["open"]; gex = compute_gex(records)
        levels = extract_levels(gex, spot)
        if not levels: continue
        neg = levels["strongest_neg"]; pos = levels["strongest_pos"]
        gap = spot - neg
        if gap <= 0: continue

        bars_day = intraday.get(d_int, [])
        if not bars_day: continue
        ratio = spot / bars_day[0]["open"] if bars_day[0]["open"] > 0 else 0
        if not (cfg["rlo"] < ratio < cfg["rhi"]): continue

        went_below = False; hit_min = False
        hit_min_time = None; hit_min_ms = None
        max_dip = 0; max_dip_time = None
        recovered_hi = False; rec_hi_time = None; rec_hi_ms = None
        reached_pos = False

        for b in bars_day:
            lo = b["low"] * ratio; hi = b["high"] * ratio
            ms = b["ms_of_day"]
            if not went_below and lo < neg:
                went_below = True
            if went_below:
                dd = neg - lo
                if dd > max_dip:
                    max_dip = dd; max_dip_time = b["time"]
                if not hit_min and dd >= cfg["min_dip"]:
                    hit_min = True; hit_min_time = b["time"]; hit_min_ms = ms
                if hit_min and not recovered_hi and hi > neg:
                    recovered_hi = True; rec_hi_time = b["time"]; rec_hi_ms = ms
                if went_below and not reached_pos and hi >= pos:
                    reached_pos = True

        if not hit_min: continue
        if max_dip > 500: continue

        time_to_rec = round((rec_hi_ms - hit_min_ms) / 60000) if rec_hi_ms and hit_min_ms else None
        h = int(hit_min_time.split(":")[0]) if hit_min_time else 99
        events.append({
            "date": d, "dip": round(max_dip, 1), "gap": round(gap, 1),
            "hit_min_time": hit_min_time, "hit_hour": h,
            "max_dip_time": max_dip_time,
            "recovered": recovered_hi, "rec_time": rec_hi_time,
            "time_to_rec": time_to_rec,
            "reached_pos": reached_pos,
        })
    results[sym] = events

# ================================================================
# PRINT RESULTS
# ================================================================
print("=" * 70)
print("  ALL SYMBOLS: GEX Dip Study")
print("  T1 = HIGH goes back above -GEX (can sell option at profit)")
print("=" * 70)
print()

# All events
print("--- ALL EVENTS (any time) ---")
header = f"{'Sym':>5} {'MinDip':>6} {'Events':>6} {'/mo':>5} {'/wk':>5} {'T1':>8} {'T1%':>6} {'T2':>8} {'T2%':>6} {'AvgDip':>7}"
print(header)
t_n = 0; t_r = 0; t_p = 0
for sym in ["SPX", "SPY", "QQQ", "IWM"]:
    evts = results.get(sym, [])
    n = len(evts); rec = sum(1 for e in evts if e["recovered"])
    pos = sum(1 for e in evts if e["reached_pos"])
    avg = sum(e["dip"] for e in evts) / n if n else 0
    print(f"{sym:>5} {CONFIGS[sym]['min_dip']:>6} {n:>6} {n/12:>5.1f} {n/52:>5.1f} {rec:>4}/{n:<3} {rec/n*100:>5.1f}% {pos:>4}/{n:<3} {pos/n*100:>5.1f}% {avg:>7.1f}")
    t_n += n; t_r += rec; t_p += pos
print(f"{'TOTAL':>5} {'':>6} {t_n:>6} {t_n/12:>5.1f} {t_n/52:>5.1f} {t_r:>4}/{t_n:<3} {t_r/t_n*100:>5.1f}% {t_p:>4}/{t_n:<3} {t_p/t_n*100:>5.1f}%")
print()

# Before 13:00 only
print("--- BEFORE 13:00 ET ONLY ---")
print(header)
t_n = 0; t_r = 0; t_p = 0
for sym in ["SPX", "SPY", "QQQ", "IWM"]:
    evts = [e for e in results.get(sym, []) if e["hit_hour"] < 13]
    n = len(evts); rec = sum(1 for e in evts if e["recovered"])
    pos = sum(1 for e in evts if e["reached_pos"])
    avg = sum(e["dip"] for e in evts) / n if n else 0
    print(f"{sym:>5} {CONFIGS[sym]['min_dip']:>6} {n:>6} {n/12:>5.1f} {n/52:>5.1f} {rec:>4}/{n:<3} {rec/n*100:>5.1f}% {pos:>4}/{n:<3} {pos/n*100:>5.1f}% {avg:>7.1f}")
    t_n += n; t_r += rec; t_p += pos
print(f"{'TOTAL':>5} {'':>6} {t_n:>6} {t_n/12:>5.1f} {t_n/52:>5.1f} {t_r:>4}/{t_n:<3} {t_r/t_n*100:>5.1f}% {t_p:>4}/{t_n:<3} {t_p/t_n*100:>5.1f}%")
print()

# Before 12:00 only
print("--- BEFORE 12:00 ET ONLY ---")
print(header)
t_n = 0; t_r = 0; t_p = 0
for sym in ["SPX", "SPY", "QQQ", "IWM"]:
    evts = [e for e in results.get(sym, []) if e["hit_hour"] < 12]
    n = len(evts); rec = sum(1 for e in evts if e["recovered"])
    pos = sum(1 for e in evts if e["reached_pos"])
    avg = sum(e["dip"] for e in evts) / n if n else 0
    if n == 0: continue
    print(f"{sym:>5} {CONFIGS[sym]['min_dip']:>6} {n:>6} {n/12:>5.1f} {n/52:>5.1f} {rec:>4}/{n:<3} {rec/n*100:>5.1f}% {pos:>4}/{n:<3} {pos/n*100:>5.1f}% {avg:>7.1f}")
    t_n += n; t_r += rec; t_p += pos
if t_n: print(f"{'TOTAL':>5} {'':>6} {t_n:>6} {t_n/12:>5.1f} {t_n/52:>5.1f} {t_r:>4}/{t_n:<3} {t_r/t_n*100:>5.1f}% {t_p:>4}/{t_n:<3} {t_p/t_n*100:>5.1f}%")
print()

# Recovery time
print("--- RECOVERY TIME (from hitting min dip to high > -GEX) ---")
for sym in ["SPX", "SPY", "QQQ", "IWM"]:
    times = [e["time_to_rec"] for e in results.get(sym, []) if e["time_to_rec"] is not None and e["hit_hour"] < 13]
    if not times: continue
    s = sorted(times)
    print(f"  {sym}: n={len(times)}  min={s[0]}m  25th={s[len(s)//4]}m  median={s[len(s)//2]}m  75th={s[3*len(s)//4]}m  max={s[-1]}m")
print()

# Failures (before 13:00)
print("--- FAILURES (before 13:00, did NOT recover) ---")
for sym in ["SPX", "SPY", "QQQ", "IWM"]:
    fails = [e for e in results.get(sym, []) if e["hit_hour"] < 13 and not e["recovered"]]
    if not fails:
        print(f"  {sym}: 0 failures")
        continue
    total_13 = len([e for e in results.get(sym, []) if e["hit_hour"] < 13])
    print(f"  {sym}: {len(fails)}/{total_13} failures ({len(fails)/total_13*100:.0f}%)")
    for e in fails:
        print(f"    {e['date']}  dip={e['dip']:>5.1f}  hitTime={e['hit_min_time']}  maxDipTime={e['max_dip_time']}")
