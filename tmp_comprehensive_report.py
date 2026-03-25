"""Comprehensive GEX Dip Report — All 4 Symbols.

Entry: price drops below the HIGHEST STRIKE -GEX (first support wall below spot).
Tracks: dip depth, time below, recovery to -GEX (T1), reach +GEX (T2).
PNL calculated for recovered trades only.
"""
import json, os, sys, io
from collections import defaultdict
from datetime import date as dt_date

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATA_DIR = r"C:\Users\Faisa\stock_gex_data"

CONFIGS = {
    "SPX": {"dir": "spx", "proxy": "SPY", "rlo": 5, "rhi": 15, "strike_ivl": 5},
    "SPY": {"dir": "spy", "proxy": "SPY", "rlo": 0.9, "rhi": 1.1, "strike_ivl": 1},
    "QQQ": {"dir": "qqq", "proxy": "QQQ", "rlo": 0.9, "rhi": 1.1, "strike_ivl": 1},
    "IWM": {"dir": "iwm", "proxy": "IWM", "rlo": 0.9, "rhi": 1.1, "strike_ivl": 1},
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
    """Get highest-strike -GEX below/near spot (first support wall)
    and lowest-strike +GEX above spot (first magnet)."""
    neg = [(k, v) for k, v in gex.items() if v < 0]
    pos = [(k, v) for k, v in gex.items() if v > 0]
    if not neg or not pos:
        return None

    # Filter for significance: >= 10% of strongest
    neg.sort(key=lambda x: x[1])
    mx_neg = abs(neg[0][1])
    sig_neg = [(k, v) for k, v in neg if abs(v) >= mx_neg * 0.10]

    pos.sort(key=lambda x: x[1], reverse=True)
    mx_pos = pos[0][1]
    sig_pos = [(k, v) for k, v in pos if v >= mx_pos * 0.10]

    # Highest strike -GEX (closest to spot from below/at spot)
    neg_below_spot = [(k, v) for k, v in sig_neg if k <= spot + 5]
    if not neg_below_spot:
        neg_below_spot = sig_neg  # fallback
    neg_below_spot.sort(key=lambda x: x[0], reverse=True)  # highest strike first
    highest_neg = neg_below_spot[0]

    # Lowest strike +GEX above the -GEX level (first magnet up)
    pos_above_neg = [(k, v) for k, v in sig_pos if k > highest_neg[0]]
    if not pos_above_neg:
        pos_above_neg = sig_pos
    pos_above_neg.sort(key=lambda x: x[0])  # lowest strike first
    first_pos = pos_above_neg[0]

    return {
        "neg_strike": highest_neg[0],
        "neg_gex": highest_neg[1],
        "pos_strike": first_pos[0],
        "pos_gex": first_pos[1],
        "n2p": first_pos[0] - highest_neg[0],
    }


def analyze_symbol(sym, cfg):
    sym_dir = os.path.join(DATA_DIR, cfg["dir"])
    odir = os.path.join(sym_dir, "options")
    if not os.path.exists(odir):
        return []

    # Load prices
    pf = os.path.join(sym_dir, "prices", f"{sym}.json")
    if not os.path.exists(pf):
        return []
    with open(pf) as f:
        prices = {int(b["date"]): b for b in json.load(f)}

    # Load intraday
    ifile = os.path.join(sym_dir, "intraday", f"{cfg['proxy']}_5min.json")
    if not os.path.exists(ifile):
        return []
    with open(ifile) as f:
        iraw = json.load(f)
    intraday = defaultdict(list)
    for b in iraw:
        intraday[b["date"]].append(b)
    for d in intraday:
        intraday[d].sort(key=lambda x: x["ms_of_day"])

    dow_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}
    events = []

    for fn in sorted(os.listdir(odir)):
        if not fn.endswith("_0dte.json"):
            continue
        d = fn.replace("_0dte.json", "")
        d_int = int(d.replace("-", ""))
        bar = prices.get(d_int)
        if not bar:
            continue
        with open(os.path.join(odir, fn)) as fh:
            records = json.load(fh)
        if not records:
            continue

        spot = bar["open"]
        gex = compute_gex(records)
        levels = extract_levels(gex, spot)
        if not levels:
            continue

        neg = levels["neg_strike"]
        pos = levels["pos_strike"]
        n2p = levels["n2p"]
        gap = spot - neg

        bars_day = intraday.get(d_int, [])
        if not bars_day:
            continue
        ratio = spot / bars_day[0]["open"] if bars_day[0]["open"] > 0 else 0
        if not (cfg["rlo"] < ratio < cfg["rhi"]):
            continue

        # Bar-by-bar tracking
        went_below = False
        first_below_time = None
        first_below_ms = None
        max_dip = 0
        max_dip_time = None
        max_dip_ms = None
        low_price = None

        # Recovery T1: high goes above -GEX after dipping
        t1_hit = False
        t1_time = None
        t1_ms = None

        # T2: high reaches +GEX
        t2_hit = False
        t2_time = None
        t2_ms = None

        # Count bars with close < neg (time spent below)
        bars_close_below = 0

        # Track last bar below before recovery (for time below calculation)
        last_below_ms = None

        for b in bars_day:
            lo = b["low"] * ratio
            hi = b["high"] * ratio
            cl = b["close"] * ratio
            ms = b["ms_of_day"]

            if not went_below and lo < neg:
                went_below = True
                first_below_time = b["time"]
                first_below_ms = ms
                low_price = lo

            if went_below:
                dd = neg - lo
                if dd > max_dip:
                    max_dip = dd
                    max_dip_time = b["time"]
                    max_dip_ms = ms
                    low_price = lo

                if cl < neg:
                    bars_close_below += 1
                    last_below_ms = ms

                if not t1_hit and hi > neg:
                    t1_hit = True
                    t1_time = b["time"]
                    t1_ms = ms

                if not t2_hit and hi >= pos:
                    t2_hit = True
                    t2_time = b["time"]
                    t2_ms = ms

        if not went_below:
            continue
        if max_dip > 500:
            continue  # bad data

        # Time calculations
        time_below_min = bars_close_below * 5  # each bar is 5 min
        time_first_to_t1 = round((t1_ms - first_below_ms) / 60000) if t1_ms and first_below_ms else None
        time_dip_to_t1 = round((t1_ms - max_dip_ms) / 60000) if t1_ms and max_dip_ms else None
        time_first_to_t2 = round((t2_ms - first_below_ms) / 60000) if t2_ms and first_below_ms else None

        # PNL calculations (for recovered T1 trades)
        # From dip low to -GEX = max_dip pts
        # From -10 below to -GEX = 10 pts (if dip >= 10)
        t1_pnl_from_low = round(max_dip, 1) if t1_hit else None
        t1_pnl_from_10 = min(10.0, round(max_dip, 1)) if t1_hit and max_dip >= 10 else None

        # T2 PNL: from low to +GEX
        t2_pnl_from_low = round(max_dip + n2p, 1) if t2_hit else None
        t2_pnl_from_10 = round(10 + n2p, 1) if t2_hit and max_dip >= 10 else None

        last_close = bars_day[-1]["close"] * ratio
        dt = dt_date.fromisoformat(d)

        events.append({
            "date": d,
            "dow": dow_map[dt.weekday()],
            "spot": round(spot, 2),
            "neg": neg,
            "pos": pos,
            "gap": round(gap, 1),
            "n2p": round(n2p, 1),
            "max_dip": round(max_dip, 1),
            "max_dip_time": max_dip_time,
            "first_below": first_below_time,
            "bars_below": bars_close_below,
            "time_below": time_below_min,
            "t1_hit": t1_hit,
            "t1_time": t1_time,
            "t1_min": time_first_to_t1,
            "t1_from_dip": time_dip_to_t1,
            "t2_hit": t2_hit,
            "t2_time": t2_time,
            "t2_min": time_first_to_t2,
            "t1_pnl_low": t1_pnl_from_low,
            "t1_pnl_10": t1_pnl_from_10,
            "t2_pnl_low": t2_pnl_from_low,
            "t2_pnl_10": t2_pnl_from_10,
            "close": round(last_close, 2),
            "close_above": last_close > neg,
        })

    return events


def print_report(sym, events):
    if not events:
        print(f"\n  {sym}: No data")
        return

    n = len(events)
    t1_count = sum(1 for e in events if e["t1_hit"])
    t2_count = sum(1 for e in events if e["t2_hit"])
    t1_wr = t1_count / n * 100
    t2_wr = t2_count / n * 100

    print(f"\n{'#' * 80}")
    print(f"##  {sym} -- COMPREHENSIVE GEX DIP REPORT")
    print(f"{'#' * 80}")
    print()
    print(f"  Total events (price went below highest -GEX): {n}")
    print(f"  Frequency: {n / 12:.1f}/month, {n / 52:.1f}/week")
    print()

    # T1 and T2 win rates
    print(f"  --- WIN RATES ---")
    print(f"  T1 (recovered above -GEX):  {t1_count}/{n}  WR = {t1_wr:.1f}%")
    print(f"  T2 (reached first +GEX):    {t2_count}/{n}  WR = {t2_wr:.1f}%")
    print(f"  Close above -GEX (EOD):     {sum(1 for e in events if e['close_above'])}/{n}  ({sum(1 for e in events if e['close_above'])/n*100:.1f}%)")
    print()

    # Dip statistics
    dips = [e["max_dip"] for e in events]
    print(f"  --- DIP DEPTH ---")
    print(f"  Min: {min(dips):.1f}  Median: {sorted(dips)[len(dips)//2]:.1f}  Avg: {sum(dips)/len(dips):.1f}  Max: {max(dips):.1f}")
    for lo, hi in [(0, 5), (5, 10), (10, 20), (20, 50), (50, 999)]:
        c = sum(1 for d in dips if lo <= d < hi)
        lbl = f"{lo}-{hi}" if hi < 999 else f"{lo}+"
        if c: print(f"    Dip {lbl}: {c} events ({c/n*100:.0f}%)")
    print()

    # Time below -GEX
    times_below = [e["time_below"] for e in events]
    print(f"  --- TIME SPENT BELOW -GEX ---")
    print(f"  Min: {min(times_below)}m  Median: {sorted(times_below)[len(times_below)//2]}m  Avg: {sum(times_below)//len(times_below)}m  Max: {max(times_below)}m")
    for lo, hi in [(0, 5), (5, 15), (15, 30), (30, 60), (60, 120), (120, 999)]:
        c = sum(1 for t in times_below if lo <= t < hi)
        lbl = f"{lo}-{hi}m" if hi < 999 else f"{lo}m+"
        if c: print(f"    {lbl}: {c} events ({c/n*100:.0f}%)")
    print()

    # Recovery time T1
    t1_times = [e["t1_from_dip"] for e in events if e["t1_from_dip"] is not None]
    if t1_times:
        s = sorted(t1_times)
        print(f"  --- T1 RECOVERY TIME (from max dip to high > -GEX) ---")
        print(f"  n={len(s)}  Min: {s[0]}m  25th: {s[len(s)//4]}m  Median: {s[len(s)//2]}m  75th: {s[3*len(s)//4]}m  Max: {s[-1]}m")
        for lo, hi in [(0, 5), (5, 15), (15, 30), (30, 60), (60, 120), (120, 999)]:
            c = sum(1 for t in s if lo <= t < hi)
            lbl = f"{lo}-{hi}m" if hi < 999 else f"{lo}m+"
            if c: print(f"    {lbl}: {c} ({c/len(s)*100:.0f}%)")
        print()

    # T2 time
    t2_times = [e["t2_min"] for e in events if e["t2_min"] is not None]
    if t2_times:
        s = sorted(t2_times)
        print(f"  --- T2 TIME (from first below to reaching +GEX) ---")
        print(f"  n={len(s)}  Min: {s[0]}m  Median: {s[len(s)//2]}m  Max: {s[-1]}m")
        print()

    # PNL for T1 recovered trades
    t1_pnls_low = [e["t1_pnl_low"] for e in events if e["t1_pnl_low"] is not None]
    t1_pnls_10 = [e["t1_pnl_10"] for e in events if e["t1_pnl_10"] is not None]
    print(f"  --- T1 PNL (recovered trades only, all positive) ---")
    if t1_pnls_low:
        print(f"  From low to -GEX (max possible):  n={len(t1_pnls_low)}  Avg={sum(t1_pnls_low)/len(t1_pnls_low):.1f}  Median={sorted(t1_pnls_low)[len(t1_pnls_low)//2]:.1f}  Total={sum(t1_pnls_low):.1f}")
    if t1_pnls_10:
        print(f"  From -10 entry to -GEX (fixed):   n={len(t1_pnls_10)}  Each=+10.0  Total={sum(t1_pnls_10):.1f}")
    print()

    # PNL for T2 trades
    t2_pnls_low = [e["t2_pnl_low"] for e in events if e["t2_pnl_low"] is not None]
    t2_pnls_10 = [e["t2_pnl_10"] for e in events if e["t2_pnl_10"] is not None]
    print(f"  --- T2 PNL (reached +GEX trades only) ---")
    if t2_pnls_low:
        print(f"  From low to +GEX:    n={len(t2_pnls_low)}  Avg={sum(t2_pnls_low)/len(t2_pnls_low):.1f}  Median={sorted(t2_pnls_low)[len(t2_pnls_low)//2]:.1f}  Total={sum(t2_pnls_low):.1f}")
    if t2_pnls_10:
        print(f"  From -10 to +GEX:    n={len(t2_pnls_10)}  Avg={sum(t2_pnls_10)/len(t2_pnls_10):.1f}  Median={sorted(t2_pnls_10)[len(t2_pnls_10)//2]:.1f}  Total={sum(t2_pnls_10):.1f}")
    print()

    # N2P distance
    n2ps = [e["n2p"] for e in events]
    print(f"  --- -GEX to +GEX DISTANCE ---")
    print(f"  Min: {min(n2ps):.1f}  Median: {sorted(n2ps)[len(n2ps)//2]:.1f}  Avg: {sum(n2ps)/len(n2ps):.1f}  Max: {max(n2ps):.1f}")
    print()

    # Gap at open
    gaps = [e["gap"] for e in events]
    print(f"  --- GAP AT OPEN (spot - neg) ---")
    print(f"  Min: {min(gaps):.1f}  Median: {sorted(gaps)[len(gaps)//2]:.1f}  Avg: {sum(gaps)/len(gaps):.1f}  Max: {max(gaps):.1f}")
    opened_below = sum(1 for g in gaps if g <= 0)
    print(f"  Opened below -GEX: {opened_below}/{n} ({opened_below/n*100:.0f}%)")
    print()

    # Failures
    fails = [e for e in events if not e["t1_hit"]]
    print(f"  --- FAILURES (T1 NOT recovered): {len(fails)}/{n} ({len(fails)/n*100:.1f}%) ---")
    for e in fails:
        mt = e["max_dip_time"] or "-"
        print(f"    {e['date']} {e['dow']}  -GEX={e['neg']:.1f}  gap={e['gap']:+.1f}  dip={e['max_dip']:.1f}  dipTime={mt}  below={e['time_below']}m")
    print()

    # FULL TRADE LOG
    print(f"  --- FULL LOG ---")
    print(f"  {'#':>3} {'Date':>10} {'D':>3} {'Spot':>7} {'-GEX':>7} {'+GEX':>7} {'Gap':>5} {'N2P':>5} {'Dip':>6} {'Below':>5} {'T1':>2} {'T1time':>6} {'T1rec':>5} {'T2':>2} {'T2time':>6} {'T1pnl':>6} {'T2pnl':>6}")
    for i, e in enumerate(events, 1):
        t1 = "Y" if e["t1_hit"] else "N"
        t2 = "Y" if e["t2_hit"] else "N"
        t1t = e["t1_time"][:5] if e["t1_time"] else "  -  "
        t2t = e["t2_time"][:5] if e["t2_time"] else "  -  "
        t1r = f"{e['t1_from_dip']:>4}m" if e["t1_from_dip"] is not None else "    -"
        t1p = f"{e['t1_pnl_low']:>+5.1f}" if e["t1_pnl_low"] is not None else "    -"
        t2p = f"{e['t2_pnl_low']:>+5.1f}" if e["t2_pnl_low"] is not None else "    -"
        dip = e["max_dip"]
        print(f"  {i:>3} {e['date']:>10} {e['dow']:>3} {e['spot']:>7.1f} {e['neg']:>7.1f} {e['pos']:>7.1f} {e['gap']:>+5.0f} {e['n2p']:>5.0f} {dip:>6.1f} {e['time_below']:>4}m {t1:>2} {t1t:>6} {t1r:>5} {t2:>2} {t2t:>6} {t1p:>6} {t2p:>6}")


# ================================================================
# MAIN
# ================================================================
print("=" * 80)
print("  COMPREHENSIVE GEX DIP REPORT -- ALL SYMBOLS")
print("  Entry: price goes below HIGHEST STRIKE -GEX (first support wall)")
print("  T1 = HIGH recovers above -GEX  |  T2 = HIGH reaches first +GEX")
print("=" * 80)

all_results = {}
for sym, cfg in CONFIGS.items():
    events = analyze_symbol(sym, cfg)
    all_results[sym] = events
    print(f"  {sym}: {len(events)} events loaded")

for sym in ["SPX", "SPY", "QQQ", "IWM"]:
    print_report(sym, all_results[sym])

# GRAND SUMMARY
print(f"\n{'#' * 80}")
print(f"##  GRAND SUMMARY")
print(f"{'#' * 80}")
print()
print(f"  {'Sym':>5} {'Events':>7} {'/mo':>5} {'/wk':>5} {'T1 WR':>8} {'T2 WR':>8} {'AvgDip':>7} {'T1 AvgPnl':>10} {'T2 AvgPnl':>10}")
t_n = 0; t_t1 = 0; t_t2 = 0
for sym in ["SPX", "SPY", "QQQ", "IWM"]:
    evts = all_results[sym]
    n = len(evts)
    if n == 0: continue
    t1 = sum(1 for e in evts if e["t1_hit"])
    t2 = sum(1 for e in evts if e["t2_hit"])
    avg_dip = sum(e["max_dip"] for e in evts) / n
    t1_pnls = [e["t1_pnl_low"] for e in evts if e["t1_pnl_low"] is not None]
    t2_pnls = [e["t2_pnl_low"] for e in evts if e["t2_pnl_low"] is not None]
    t1_avg = sum(t1_pnls) / len(t1_pnls) if t1_pnls else 0
    t2_avg = sum(t2_pnls) / len(t2_pnls) if t2_pnls else 0
    print(f"  {sym:>5} {n:>7} {n/12:>5.1f} {n/52:>5.1f} {t1/n*100:>7.1f}% {t2/n*100:>7.1f}% {avg_dip:>7.1f} {t1_avg:>+10.1f} {t2_avg:>+10.1f}")
    t_n += n; t_t1 += t1; t_t2 += t2

print(f"  {'TOTAL':>5} {t_n:>7} {t_n/12:>5.1f} {t_n/52:>5.1f} {t_t1/t_n*100:>7.1f}% {t_t2/t_n*100:>7.1f}%")
print()
print(f"  Combined: {t_n} events/year = {t_n/12:.1f}/month = {t_n/52:.1f}/week")
print(f"  T1 recovery: {t_t1}/{t_n} = {t_t1/t_n*100:.1f}%")
print(f"  T2 hit rate: {t_t2}/{t_n} = {t_t2/t_n*100:.1f}%")
