"""Multi-symbol GEX Dip Study: SPX, SPY, QQQ, IWM
Runs the same 10pt dip analysis on all available symbols.
Entry window: 10:00-13:00 ET only.

Run after all downloads complete:
  python tmp_all_symbols_study.py
"""
import json, os, sys, io
from collections import defaultdict
from datetime import date as dt_date

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATA_DIR = r"C:\Users\Faisa\stock_gex_data"

# Symbol configs: min_dip in pts, intraday file
SYMBOLS = {
    "SPX": {"dir": "spx", "min_dip": 10, "intraday_proxy": "SPY", "ratio_range": (5, 15)},
    "SPY": {"dir": "spy", "min_dip": 1,  "intraday_proxy": "SPY", "ratio_range": (0.9, 1.1)},
    "QQQ": {"dir": "qqq", "min_dip": 1,  "intraday_proxy": "QQQ", "ratio_range": (0.9, 1.1)},
    "IWM": {"dir": "iwm", "min_dip": 0.5, "intraday_proxy": "IWM", "ratio_range": (0.9, 1.1)},
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
    total_gex = sum(v for _, v in gex.items())
    return {"strongest_neg": top_neg[0][0], "strongest_pos": top_pos[0][0],
            "regime": "positive" if total_gex > 0 else "negative"}


def load_intraday(sym_dir, proxy_sym):
    """Load 5-min intraday bars. For SPX uses SPY proxy, for others uses own bars."""
    # Try symbol's own intraday first
    for fname in [f"{proxy_sym}_5min.json", f"SPY_5min.json"]:
        path = os.path.join(sym_dir, "intraday", fname)
        if os.path.exists(path):
            with open(path) as f:
                bars = json.load(f)
            by_date = defaultdict(list)
            for b in bars:
                by_date[b["date"]].append(b)
            for d in by_date:
                by_date[d].sort(key=lambda x: x["ms_of_day"])
            return dict(by_date)
    return {}


def analyze_symbol(symbol, cfg):
    sym_dir = os.path.join(DATA_DIR, cfg["dir"])
    options_dir = os.path.join(sym_dir, "options")
    min_dip = cfg["min_dip"]
    ratio_lo, ratio_hi = cfg["ratio_range"]

    if not os.path.exists(options_dir):
        print(f"  {symbol}: No data directory")
        return None

    # Load prices
    price_file = os.path.join(sym_dir, "prices", f"{symbol}.json")
    if not os.path.exists(price_file):
        print(f"  {symbol}: No price file")
        return None
    with open(price_file) as f:
        prices = {int(b["date"]): b for b in json.load(f)}

    # Load intraday
    intraday = load_intraday(sym_dir, cfg["intraday_proxy"])

    # Count files
    files = [f for f in os.listdir(options_dir) if f.endswith("_0dte.json")]
    print(f"  {symbol}: {len(files)} chain files, {len(intraday)} intraday days, {len(prices)} price days")

    if not files:
        return None

    dow_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}
    events = []

    for f in sorted(files):
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
        gap = spot - neg
        if gap <= 0: continue  # opened above -GEX only
        n2p = pos - neg

        # Use daily bar for basic analysis
        o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
        if l >= neg: continue  # didn't dip below

        dip = neg - l
        if dip < min_dip: continue  # not deep enough

        # Intraday timing (if available)
        bars_day = intraday.get(d_int, [])
        first_below_time = None
        hit_min_time = None
        rec_time = None
        max_dip_time = None
        time_to_rec = None
        intraday_recovered = None

        if bars_day:
            ratio = spot / bars_day[0]["open"] if bars_day[0]["open"] > 0 else 0
            if ratio_lo < ratio < ratio_hi:
                went_below = False
                hit_min = False
                max_d = 0
                first_below_ms = None
                hit_min_ms = None
                rec_ms = None

                for b in bars_day:
                    lo_px = b["low"] * ratio
                    cl_px = b["close"] * ratio
                    ms = b["ms_of_day"]

                    if not went_below and lo_px < neg:
                        went_below = True
                        first_below_time = b["time"]
                        first_below_ms = ms

                    if went_below:
                        dd = neg - lo_px
                        if dd > max_d:
                            max_d = dd
                            max_dip_time = b["time"]
                        if not hit_min and dd >= min_dip:
                            hit_min = True
                            hit_min_time = b["time"]
                            hit_min_ms = ms
                        # Recovery after hitting min dip: bar closes above -GEX
                        # Only count if within 10:00-13:00 entry window
                        if hit_min and not rec_time and cl_px > neg:
                            if hit_min_ms and hit_min_ms <= 46800000:  # 13:00
                                rec_time = b["time"]
                                rec_ms = ms
                                intraday_recovered = True

                if hit_min and not rec_time:
                    intraday_recovered = False

                if rec_ms and hit_min_ms:
                    time_to_rec = round((rec_ms - hit_min_ms) / 60000)

        # Daily-based recovery
        daily_recovered = h > neg  # high came back above
        close_above = c > neg
        reached_pos = h >= pos

        dt = dt_date.fromisoformat(d)
        events.append({
            "date": d, "dow": dow_map[dt.weekday()],
            "spot": spot, "neg": neg, "pos": pos,
            "gap": round(gap, 1), "n2p": round(n2p, 1),
            "dip": round(dip, 1),
            "first_below": first_below_time,
            "hit_min_time": hit_min_time,
            "max_dip_time": max_dip_time,
            "intraday_rec": intraday_recovered,
            "rec_time": rec_time,
            "time_to_rec": time_to_rec,
            "daily_rec": daily_recovered,
            "close_above": close_above,
            "reached_pos": reached_pos,
            "regime": levels["regime"],
        })

    return events


def print_summary(symbol, events, min_dip):
    if not events:
        print(f"\n  {symbol}: No qualifying events")
        return

    n = len(events)
    # Use intraday recovery if available, else daily
    has_intraday = any(e["intraday_rec"] is not None for e in events)

    if has_intraday:
        intra = [e for e in events if e["intraday_rec"] is not None]
        n_rec = sum(1 for e in intra if e["intraday_rec"])
        rec_label = "Intraday T1 (close above -GEX)"
        rec_base = len(intra)
    else:
        n_rec = sum(1 for e in events if e["daily_rec"])
        rec_label = "Daily high above -GEX"
        rec_base = n

    n_close = sum(1 for e in events if e["close_above"])
    n_pos = sum(1 for e in events if e["reached_pos"])
    avg_dip = sum(e["dip"] for e in events) / n

    print(f"\n{'='*75}")
    print(f"  {symbol} | Dip >= {min_dip} pts | Opened above -GEX")
    print(f"{'='*75}")
    print(f"  Events: {n} ({n/12:.1f}/mo, {n/52:.1f}/wk)")
    print(f"  {rec_label}: {n_rec}/{rec_base} ({n_rec/rec_base*100:.1f}%)")
    print(f"  Close above -GEX: {n_close}/{n} ({n_close/n*100:.1f}%)")
    print(f"  Reached +GEX: {n_pos}/{n} ({n_pos/n*100:.1f}%)")
    print(f"  Avg dip: {avg_dip:.1f} pts")

    # Recovery time (if intraday available)
    if has_intraday:
        times = [e["time_to_rec"] for e in events if e["time_to_rec"] is not None]
        if times:
            print(f"\n  Recovery time (from hitting -{min_dip} to close above -GEX):")
            print(f"    Min={min(times)}m  Median={sorted(times)[len(times)//2]}m  Avg={sum(times)//len(times)}m  Max={max(times)}m")
            for lo, hi in [(0, 15), (15, 30), (30, 60), (60, 120), (120, 999)]:
                c = sum(1 for t in times if lo <= t < hi)
                if c: print(f"    {lo}-{hi}min: {c} ({c/len(times)*100:.0f}%)")

    # Failures
    if has_intraday:
        fails = [e for e in events if e["intraday_rec"] == False]
    else:
        fails = [e for e in events if not e["daily_rec"]]
    if fails:
        print(f"\n  Failures ({len(fails)}):")
        for e in fails:
            mt = e["max_dip_time"] or "-"
            print(f"    {e['date']} {e['dow']}  -GEX={e['neg']:.0f}  dip={e['dip']:.1f}  maxDipTime={mt}")

    # Full log
    print(f"\n  {'#':>3} {'Date':>10} {'D':>3} {'-GEX':>7} {'Gap':>5} {'Dip':>5} {'HitT':>5} {'DipT':>5} {'T1':>3} {'RecT':>5} {'tRec':>5} {'T2':>3} {'Reg':>3}")
    for i, e in enumerate(events, 1):
        if has_intraday:
            r = "Y" if e["intraday_rec"] else ("N" if e["intraday_rec"] == False else "?")
        else:
            r = "Y" if e["daily_rec"] else "N"
        p = "Y" if e["reached_pos"] else "N"
        ht = e["hit_min_time"][:5] if e["hit_min_time"] else "  -  "
        mt = e["max_dip_time"][:5] if e["max_dip_time"] else "  -  "
        rt = e["rec_time"][:5] if e["rec_time"] else "  -  "
        tr = f"{e['time_to_rec']:>4}m" if e["time_to_rec"] is not None else "    -"
        reg = "+" if e["regime"] == "positive" else "-"
        print(f"  {i:>3} {e['date']:>10} {e['dow']:>3} {e['neg']:>7.1f} {e['gap']:>+5.0f} {e['dip']:>5.1f} {ht:>5} {mt:>5} {r:>3} {rt:>5} {tr:>5} {p:>3} {reg:>3}")


# ================================================================
# MAIN
# ================================================================
print("=" * 75)
print("  MULTI-SYMBOL GEX DIP STUDY")
print("  Entry window: opened above -GEX, dipped below during 10:00-13:00")
print("=" * 75)
print()

# Check what data we have
print("--- Data Status ---")
all_events = {}
for symbol, cfg in SYMBOLS.items():
    events = analyze_symbol(symbol, cfg)
    if events:
        all_events[symbol] = events
print()

# Also download intraday for SPY/QQQ/IWM if missing
for sym in ["SPY", "QQQ", "IWM"]:
    cfg = SYMBOLS[sym]
    idir = os.path.join(DATA_DIR, cfg["dir"], "intraday")
    ifile = os.path.join(idir, f"{sym}_5min.json")
    if not os.path.exists(ifile):
        print(f"  NOTE: {sym} intraday 5-min bars not downloaded yet.")
        print(f"  Analysis will use daily bars only. Download later for full intraday study.")

print()

# Print summaries
for symbol, cfg in SYMBOLS.items():
    if symbol in all_events:
        print_summary(symbol, all_events[symbol], cfg["min_dip"])

# Grand total
print(f"\n{'='*75}")
print(f"  GRAND TOTAL (all symbols combined)")
print(f"{'='*75}")
total_events = sum(len(v) for v in all_events.values())
total_rec = 0
total_with_intraday = 0
for sym, evts in all_events.items():
    for e in evts:
        if e["intraday_rec"] is not None:
            total_with_intraday += 1
            if e["intraday_rec"]: total_rec += 1
        elif e["daily_rec"]:
            total_rec += 1

print(f"  Total events: {total_events} ({total_events/12:.1f}/mo, {total_events/52:.1f}/wk)")
print(f"  Symbols: {', '.join(all_events.keys())}")
for sym, evts in all_events.items():
    n = len(evts)
    print(f"    {sym}: {n} events ({n/12:.1f}/mo)")
