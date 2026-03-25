"""SPX GEX Bounce v3 — Entry BELOW -GEX, target = recovery above.

Like stock GEX: price dips below -GEX support -> enter long -> target recovery.
Entry: when price drops X pts below strongest -GEX
T1: recovery back to -GEX level
T2: +GEX magnet or open price
Stop: fixed pts below entry
"""
import json, os, sys, io
from collections import defaultdict, Counter
from datetime import date as dt_date

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATA_DIR = r"C:\Users\Faisa\stock_gex_data"
SPX_DIR = os.path.join(DATA_DIR, "spx")

# Load data
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
    return {
        "strongest_neg": top_neg[0][0], "strongest_pos": top_pos[0][0],
        "second_neg": top_neg[1][0] if len(top_neg) > 1 else None,
        "regime": "positive" if total_gex > 0 else "negative",
        "neg_count": len(top_neg),
    }

# Build dataset
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

print(f"Dataset: {len(dataset)} days, Intraday: {len(intraday)} days")
print()


def backtest(dataset, entry_below=5, stop_pts=8, t1_mode="neg_recovery", t2_mode="pos_gex",
             trail_activation=None, trail_gap=None, be_trigger=None,
             entry_start="10:00", entry_end="14:00"):
    """
    Entry: price drops entry_below pts BELOW strongest -GEX -> enter long at that level
    T1: recovery back to -GEX (= entry_below pts profit)
    T2: +GEX magnet or open price
    Stop: stop_pts below entry
    Trail: optional continuous trail
    """
    start_ms = int(entry_start.split(":")[0]) * 3600000 + int(entry_start.split(":")[1]) * 60000
    end_ms = int(entry_end.split(":")[0]) * 3600000 + int(entry_end.split(":")[1]) * 60000

    trades = []
    for day in dataset:
        neg = day["strongest_neg"]
        pos = day["strongest_pos"]
        spot = day["open"]
        bars = intraday.get(day["date_int"], [])
        if not bars or not neg: continue

        ratio = spot / bars[0]["open"] if bars[0]["open"] > 0 else 0
        if ratio <= 0: continue

        gap = spot - neg  # how far open is above -GEX

        # Entry trigger level = -GEX minus entry_below
        entry_trigger = neg - entry_below
        entry_price = entry_trigger  # fill at trigger

        # Targets
        t1 = neg  # recovery back to -GEX level
        t2 = None
        if t2_mode == "pos_gex":
            t2 = pos
        elif t2_mode == "open":
            t2 = spot

        # Use T2 as the main target (bigger move)
        target = t2 if t2 else t1
        target_pts = target - entry_price

        stop = entry_price - stop_pts

        # Bar-by-bar
        entered = False
        entry_time = None
        max_profit = 0
        current_stop = stop

        for b in bars:
            lo_spx = b["low"] * ratio
            hi_spx = b["high"] * ratio
            ms = b["ms_of_day"]

            if not entered:
                if ms < start_ms or ms > end_ms: continue
                if lo_spx <= entry_trigger:
                    entered = True
                    entry_time = b["time"]
                    current_stop = stop
                    max_profit = 0

            if entered:
                bar_profit = hi_spx - entry_price
                max_profit = max(max_profit, bar_profit)

                # BE trigger
                if be_trigger and max_profit >= be_trigger:
                    current_stop = max(current_stop, entry_price)

                # Trail
                if trail_activation and trail_gap and max_profit >= trail_activation:
                    trail_level = entry_price + max_profit - trail_gap
                    current_stop = max(current_stop, trail_level)

                # Check stop
                if lo_spx <= current_stop:
                    pnl = current_stop - entry_price
                    outcome = "STOP" if pnl < 0 else "TRAIL" if pnl > 0 else "BE"
                    trades.append({"date": day["date"], "pnl": round(pnl, 1),
                        "outcome": outcome, "entry": round(entry_price, 0),
                        "neg": neg, "pos": pos, "spot": spot,
                        "entry_time": entry_time, "exit_time": b["time"],
                        "regime": day["regime"], "gap": round(gap, 0),
                        "max_profit": round(max_profit, 1),
                        "target_pts": round(target_pts, 0)})
                    break

                # Check target
                if hi_spx >= target:
                    pnl = target - entry_price
                    trades.append({"date": day["date"], "pnl": round(pnl, 1),
                        "outcome": "TARGET", "entry": round(entry_price, 0),
                        "neg": neg, "pos": pos, "spot": spot,
                        "entry_time": entry_time, "exit_time": b["time"],
                        "regime": day["regime"], "gap": round(gap, 0),
                        "max_profit": round(max(max_profit, pnl), 1),
                        "target_pts": round(target_pts, 0)})
                    break

        # EOD
        if entered and (not trades or trades[-1]["date"] != day["date"]):
            last_spx = bars[-1]["close"] * ratio
            pnl = last_spx - entry_price
            trades.append({"date": day["date"], "pnl": round(pnl, 1),
                "outcome": "EOD", "entry": round(entry_price, 0),
                "neg": neg, "pos": pos, "spot": spot,
                "entry_time": entry_time, "exit_time": bars[-1]["time"],
                "regime": day["regime"], "gap": round(gap, 0),
                "max_profit": round(max_profit, 1),
                "target_pts": round(target_pts, 0)})

    return trades


def stats(trades):
    if not trades: return None
    n = len(trades)
    w = sum(1 for t in trades if t["pnl"] > 0)
    l = sum(1 for t in trades if t["pnl"] < 0)
    pnl = sum(t["pnl"] for t in trades)
    gw = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gl = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    pf = gw / max(gl, 0.01)
    avg_w = gw / max(w, 1)
    avg_l = gl / max(l, 1)
    avg = pnl / n
    eq = 0; pk = 0; mdd = 0
    for t in trades:
        eq += t["pnl"]; pk = max(pk, eq); mdd = max(mdd, pk - eq)
    return n, w, l, pnl, pf, avg_w, avg_l, avg, mdd


def report_line(label, trades):
    s = stats(trades)
    if not s: print(f"  {label}: 0 trades"); return
    n, w, l, pnl, pf, aw, al, avg, mdd = s
    wr = w/n*100
    print(f"  {label:<50} {n:>3}t {n/52:.1f}/wk WR={wr:>4.0f}% PnL={pnl:>+6.0f} PF={pf:>5.2f} DD={mdd:>4.0f} AvgW={aw:>+5.1f} AvgL={al:>5.1f} Avg={avg:>+5.1f}")


def report_full(trades, label):
    s = stats(trades)
    if not s: print(f"  {label}: 0 trades"); return
    n, w, l, pnl, pf, aw, al, avg, mdd = s
    f = sum(1 for t in trades if t["pnl"] == 0)
    oc = Counter(t["outcome"] for t in trades)
    print(f"\n  {label}")
    print(f"  Trades: {n} ({n/52:.1f}/wk) | W:{w} L:{l} F:{f} | WR:{w/n*100:.0f}%")
    print(f"  PnL: {pnl:+.0f} pts | PF: {pf:.2f} | DD: {mdd:.0f}")
    print(f"  Avg Win: {aw:+.1f} | Avg Loss: {-al:+.1f} | Avg/trade: {avg:+.1f}")
    print(f"  Outcomes: {dict(oc)}")
    # Monthly
    monthly = defaultdict(lambda: {"pnl": 0, "n": 0, "w": 0})
    for t in trades:
        m = t["date"][:7]; monthly[m]["pnl"] += t["pnl"]; monthly[m]["n"] += 1
        if t["pnl"] > 0: monthly[m]["w"] += 1
    print(f"\n  {'Month':>8} {'#':>3} {'WR':>5} {'PnL':>8} {'Cum':>8}")
    cum = 0; grn = 0
    for m in sorted(monthly.keys()):
        d = monthly[m]; cum += d["pnl"]; wr = d["w"]/d["n"]*100
        if d["pnl"] >= 0: grn += 1
        print(f"  {m:>8} {d['n']:>3} {wr:>4.0f}% {d['pnl']:>+8.1f} {cum:>+8.1f} {'+'if d['pnl']>=0 else '-'}")
    print(f"  Green: {grn}/{len(monthly)} ({grn/len(monthly)*100:.0f}%)")


def trade_log(trades, label):
    print(f"\n  Trade Log: {label}")
    dow_map = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri"}
    print(f"  {'#':>3} {'Date':>10} {'D':>3} {'Entry':>6} {'-GEX':>6} {'Gap':>4} {'TP':>4} {'Time':>14} {'PnL':>6} {'MFE':>5} {'Cum':>7} {'Out':>6}")
    cum = 0
    for i, t in enumerate(trades, 1):
        cum += t["pnl"]; d = dt_date.fromisoformat(t["date"])
        dow = dow_map[d.weekday()]; tr = f"{t['entry_time']}->{t['exit_time']}"
        wm = "W" if t["pnl"] > 0 else "L" if t["pnl"] < 0 else "F"
        print(f"  {i:>3} {t['date']:>10} {dow:>3} {t['entry']:>6.0f} {t['neg']:>6.0f} {t['gap']:>4.0f} {t['target_pts']:>4.0f} {tr:>14} {t['pnl']:>+6.1f} {t['max_profit']:>5.1f} {cum:>+7.1f} {t['outcome']:>6} {wm}")


# ================================================================
# SWEEP: Entry below -GEX, target = recovery to +GEX/open
# ================================================================
print("=" * 70)
print("  V3: ENTER BELOW -GEX -> TARGET RECOVERY")
print("  Entry = -GEX minus X pts, Target = +GEX or Open")
print("=" * 70)
print()

# A. Vary entry_below depth and stop, T2=+GEX
print("--- A. Target = +GEX magnet, no trail ---")
print(f"  {'Below':>5} {'SL':>3} {'#':>4} {'/wk':>4} {'WR':>5} {'PnL':>7} {'PF':>6} {'DD':>5} {'AvgW':>6} {'Avg':>6}")
for below in [3, 5, 8, 10, 15]:
    for sl in [5, 8, 10, 12]:
        t = backtest(dataset, entry_below=below, stop_pts=sl, t2_mode="pos_gex")
        s = stats(t)
        if not s: continue
        n, w, l, pnl, pf, aw, al, avg, mdd = s
        print(f"  {below:>5} {sl:>3} {n:>4} {n/52:>4.1f} {w/n*100:>4.0f}% {pnl:>+7.0f} {pf:>6.2f} {mdd:>5.0f} {aw:>+6.1f} {avg:>+6.1f}")
    print()

# B. Target = open price (recovery back to where price was)
print("--- B. Target = Open (full recovery) ---")
print(f"  {'Below':>5} {'SL':>3} {'#':>4} {'/wk':>4} {'WR':>5} {'PnL':>7} {'PF':>6} {'DD':>5} {'AvgW':>6} {'Avg':>6}")
for below in [3, 5, 8, 10, 15]:
    for sl in [5, 8, 10]:
        t = backtest(dataset, entry_below=below, stop_pts=sl, t2_mode="open")
        s = stats(t)
        if not s: continue
        n, w, l, pnl, pf, aw, al, avg, mdd = s
        print(f"  {below:>5} {sl:>3} {n:>4} {n/52:>4.1f} {w/n*100:>4.0f}% {pnl:>+7.0f} {pf:>6.2f} {mdd:>5.0f} {aw:>+6.1f} {avg:>+6.1f}")
    print()

# C. T2=+GEX with trail (BE@recovery to -GEX, then trail)
print("--- C. Target = +GEX, with BE@T1(-GEX) + Trail ---")
print(f"  {'Below':>5} {'SL':>3} {'#':>4} {'/wk':>4} {'WR':>5} {'PnL':>7} {'PF':>6} {'DD':>5} {'AvgW':>6} {'Avg':>6}")
for below in [3, 5, 8, 10]:
    for sl in [5, 8, 10]:
        # BE at recovery to -GEX (= entry_below pts profit), trail after +20 with gap 10
        t = backtest(dataset, entry_below=below, stop_pts=sl, t2_mode="pos_gex",
                     be_trigger=below, trail_activation=below+15, trail_gap=10)
        s = stats(t)
        if not s: continue
        n, w, l, pnl, pf, aw, al, avg, mdd = s
        print(f"  {below:>5} {sl:>3} {n:>4} {n/52:>4.1f} {w/n*100:>4.0f}% {pnl:>+7.0f} {pf:>6.02f} {mdd:>5.0f} {aw:>+6.1f} {avg:>+6.1f}")
    print()

# ================================================================
# BEST CONFIGS — detailed
# ================================================================
print("=" * 70)
print("  BEST CONFIGS — Detailed Reports")
print("=" * 70)

# Pick a few promising ones
configs = [
    ("Below=5 SL=8 T=+GEX", dict(entry_below=5, stop_pts=8, t2_mode="pos_gex")),
    ("Below=5 SL=8 T=Open", dict(entry_below=5, stop_pts=8, t2_mode="open")),
    ("Below=5 SL=8 T=+GEX BE@5 Trail@20/10", dict(entry_below=5, stop_pts=8, t2_mode="pos_gex",
        be_trigger=5, trail_activation=20, trail_gap=10)),
    ("Below=8 SL=8 T=+GEX", dict(entry_below=8, stop_pts=8, t2_mode="pos_gex")),
    ("Below=8 SL=10 T=+GEX BE@8 Trail@23/10", dict(entry_below=8, stop_pts=10, t2_mode="pos_gex",
        be_trigger=8, trail_activation=23, trail_gap=10)),
    ("Below=10 SL=8 T=+GEX", dict(entry_below=10, stop_pts=8, t2_mode="pos_gex")),
]

for label, kw in configs:
    t = backtest(dataset, **kw)
    report_full(t, label)

# Trade log for the best one
print()
print("=" * 70)
best_label = "Below=5 SL=8 T=+GEX"
best = backtest(dataset, entry_below=5, stop_pts=8, t2_mode="pos_gex")
trade_log(best, best_label)
