"""SPX GEX Bounce v2 — bigger targets, trail, split-target."""
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
        "regime": "positive" if total_gex > 0 else "negative",
        "neg_levels": top_neg, "pos_levels": top_pos,
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


def backtest(dataset, stop_pts=10, target_pts=None, target_mode="fixed",
             trail_activation=None, trail_gap=None, be_trigger=None,
             gap_filter=None, entry_start="10:00", entry_end="15:00"):
    """
    Flexible backtest with:
    - Fixed target or +GEX target
    - Optional trailing stop (activation + gap)
    - Optional breakeven trigger
    - Gap filter (min gap from open to -GEX)
    """
    start_ms = int(entry_start.split(":")[0]) * 3600000 + int(entry_start.split(":")[1]) * 60000
    end_ms = int(entry_end.split(":")[0]) * 3600000 + int(entry_end.split(":")[1]) * 60000

    trades = []
    for day in dataset:
        neg = day["strongest_neg"]
        pos = day["strongest_pos"]
        bars = intraday.get(day["date_int"], [])
        if not bars or not neg: continue

        ratio = day["open"] / bars[0]["open"] if bars[0]["open"] > 0 else 0
        if ratio <= 0: continue

        gap = day["open"] - neg
        if gap_filter is not None and gap <= gap_filter: continue

        # Determine target
        if target_mode == "pos_gex":
            target = pos
        elif target_pts:
            target = neg + target_pts
        else:
            target = neg + 15

        stop = neg - stop_pts

        # Bar-by-bar simulation
        entered = False
        entry_price = None
        entry_time = None
        max_profit = 0
        current_stop = stop  # can move with trail/BE

        for b in bars:
            lo_spx = b["low"] * ratio
            hi_spx = b["high"] * ratio
            close_spx = b["close"] * ratio
            ms = b["ms_of_day"]

            if not entered:
                if ms < start_ms or ms > end_ms: continue
                if lo_spx <= neg + 2:
                    entered = True
                    entry_price = neg
                    entry_time = b["time"]
                    current_stop = stop
                    max_profit = 0

            if entered:
                # Update max profit (use high of bar)
                bar_profit = hi_spx - entry_price
                max_profit = max(max_profit, bar_profit)

                # Breakeven trigger
                if be_trigger and max_profit >= be_trigger:
                    current_stop = max(current_stop, entry_price)

                # Trailing stop
                if trail_activation and trail_gap and max_profit >= trail_activation:
                    trail_level = entry_price + max_profit - trail_gap
                    current_stop = max(current_stop, trail_level)

                # Check stop (use current_stop which may have moved)
                if lo_spx <= current_stop:
                    pnl = current_stop - entry_price
                    trades.append({"date": day["date"], "pnl": round(pnl, 1),
                        "outcome": "STOP" if pnl < 0 else "TRAIL" if pnl > 0 else "BE",
                        "entry": neg, "entry_time": entry_time, "exit_time": b["time"],
                        "regime": day["regime"], "gap": gap, "max_profit": round(max_profit, 1)})
                    break

                # Check target
                if hi_spx >= target:
                    pnl = target - entry_price
                    trades.append({"date": day["date"], "pnl": round(pnl, 1),
                        "outcome": "TARGET", "entry": neg, "entry_time": entry_time,
                        "exit_time": b["time"], "regime": day["regime"], "gap": gap,
                        "max_profit": round(max(max_profit, pnl), 1)})
                    break

        # EOD exit
        if entered and (not trades or trades[-1]["date"] != day["date"]):
            last_spx = bars[-1]["close"] * ratio
            pnl = last_spx - entry_price
            trades.append({"date": day["date"], "pnl": round(pnl, 1),
                "outcome": "EOD", "entry": neg, "entry_time": entry_time,
                "exit_time": bars[-1]["time"], "regime": day["regime"], "gap": gap,
                "max_profit": round(max_profit, 1)})

    return trades


def report(trades, label):
    if not trades:
        print(f"  {label}: 0 trades")
        return
    n = len(trades)
    w = sum(1 for t in trades if t["pnl"] > 0)
    l = sum(1 for t in trades if t["pnl"] < 0)
    pnl = sum(t["pnl"] for t in trades)
    gw = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gl = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    pf = gw / max(gl, 0.01)
    avg = pnl / n
    avg_w = gw / max(w, 1)
    avg_l = gl / max(l, 1) * -1
    eq = 0; pk = 0; mdd = 0
    for t in trades:
        eq += t["pnl"]; pk = max(pk, eq); mdd = max(mdd, pk - eq)
    oc = Counter(t["outcome"] for t in trades)
    per_wk = n / 52
    print(f"  {label}")
    print(f"    {n} trades ({per_wk:.1f}/wk) | W:{w} L:{l} | WR:{w/n*100:.0f}% | PnL:{pnl:+.0f} | PF:{pf:.2f} | DD:{mdd:.0f} | AvgW:{avg_w:+.1f} AvgL:{avg_l:+.1f} Avg:{avg:+.1f}")
    print(f"    Outcomes: {dict(oc)}")


def report_monthly(trades, label):
    if not trades: return
    monthly = defaultdict(lambda: {"pnl": 0, "n": 0, "w": 0})
    for t in trades:
        m = t["date"][:7]; monthly[m]["pnl"] += t["pnl"]; monthly[m]["n"] += 1
        if t["pnl"] > 0: monthly[m]["w"] += 1
    print(f"\n  Monthly ({label}):")
    print(f"  {'Month':>8} {'#':>3} {'WR':>5} {'PnL':>8} {'Cum':>8}")
    cum = 0; grn = 0
    for m in sorted(monthly.keys()):
        d = monthly[m]; cum += d["pnl"]; wr = d["w"]/d["n"]*100
        if d["pnl"] >= 0: grn += 1
        f = "+" if d["pnl"] >= 0 else "-"
        print(f"  {m:>8} {d['n']:>3} {wr:>4.0f}% {d['pnl']:>+8.1f} {cum:>+8.1f} {f}")
    print(f"  Green: {grn}/{len(monthly)} ({grn/len(monthly)*100:.0f}%)")


def trade_log(trades, label):
    print(f"\n  Trade Log ({label}):")
    dow_map = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri"}
    print(f"  {'#':>3} {'Date':>10} {'D':>3} {'Entry':>6} {'Gap':>4} {'Time':>12} {'PnL':>6} {'MFE':>5} {'Cum':>7} {'Out':>6}")
    cum = 0
    for i, t in enumerate(trades, 1):
        cum += t["pnl"]; d = dt_date.fromisoformat(t["date"])
        dow = dow_map[d.weekday()]; tr = f"{t['entry_time']}->{t['exit_time']}"
        wm = "W" if t["pnl"] > 0 else "L" if t["pnl"] < 0 else "F"
        print(f"  {i:>3} {t['date']:>10} {dow:>3} {t['entry']:>6.0f} {t['gap']:>4.0f} {tr:>12} {t['pnl']:>+6.1f} {t['max_profit']:>5.1f} {cum:>+7.1f} {t['outcome']:>6} {wm}")


# ================================================================
# SWEEP: Find best target/trail combos
# ================================================================
print("=" * 70)
print("  PARAMETER SWEEP — Finding your 15+ pts/trade sweet spot")
print("=" * 70)
print()

# 1. Fixed targets, no filter
print("--- A. No gap filter (all trades, 10:00-13:00) ---")
for sl in [8, 10, 12]:
    for tp in [15, 20, 25, 30]:
        t = backtest(dataset, stop_pts=sl, target_pts=tp, entry_end="13:00")
        n = len(t); w = sum(1 for x in t if x["pnl"]>0)
        pnl = sum(x["pnl"] for x in t)
        gw = sum(x["pnl"] for x in t if x["pnl"]>0)
        gl = abs(sum(x["pnl"] for x in t if x["pnl"]<0))
        pf = gw/max(gl,0.01)
        avg = pnl/max(n,1)
        eq=0;pk=0;dd=0
        for x in t: eq+=x["pnl"];pk=max(pk,eq);dd=max(dd,pk-eq)
        print(f"  SL={sl:>2} T={tp:>2}  {n:>3}t  WR={w/max(n,1)*100:>4.0f}%  PnL={pnl:>+6.0f}  PF={pf:>5.2f}  DD={dd:>4.0f}  Avg={avg:>+5.1f}  /wk={n/52:.1f}")
    print()

# 2. With Gap>10 filter
print("--- B. Gap>10 filter ---")
for sl in [8, 10, 12]:
    for tp in [15, 20, 25, 30]:
        t = backtest(dataset, stop_pts=sl, target_pts=tp, gap_filter=10, entry_end="13:00")
        n = len(t); w = sum(1 for x in t if x["pnl"]>0)
        pnl = sum(x["pnl"] for x in t)
        gw = sum(x["pnl"] for x in t if x["pnl"]>0)
        gl = abs(sum(x["pnl"] for x in t if x["pnl"]<0))
        pf = gw/max(gl,0.01); avg = pnl/max(n,1)
        eq=0;pk=0;dd=0
        for x in t: eq+=x["pnl"];pk=max(pk,eq);dd=max(dd,pk-eq)
        print(f"  SL={sl:>2} T={tp:>2}  {n:>3}t  WR={w/max(n,1)*100:>4.0f}%  PnL={pnl:>+6.0f}  PF={pf:>5.2f}  DD={dd:>4.0f}  Avg={avg:>+5.1f}  /wk={n/52:.1f}")
    print()

# 3. Trail approach: T1=15 fixed, then trail
print("--- C. Trail approach: BE@10, trail activation=15 gap=8 ---")
for sl in [8, 10, 12]:
    for tp in [25, 30, 40, 50]:
        t = backtest(dataset, stop_pts=sl, target_pts=tp, be_trigger=10,
                     trail_activation=15, trail_gap=8, entry_end="13:00")
        n = len(t); w = sum(1 for x in t if x["pnl"]>0)
        pnl = sum(x["pnl"] for x in t)
        gw = sum(x["pnl"] for x in t if x["pnl"]>0)
        gl = abs(sum(x["pnl"] for x in t if x["pnl"]<0))
        pf = gw/max(gl,0.01); avg = pnl/max(n,1)
        eq=0;pk=0;dd=0
        for x in t: eq+=x["pnl"];pk=max(pk,eq);dd=max(dd,pk-eq)
        oc = Counter(x["outcome"] for x in t)
        print(f"  SL={sl:>2} T={tp:>2}  {n:>3}t  WR={w/max(n,1)*100:>4.0f}%  PnL={pnl:>+6.0f}  PF={pf:>5.2f}  DD={dd:>4.0f}  Avg={avg:>+5.1f}  /wk={n/52:.1f}  {dict(oc)}")
    print()

# 4. Trail with Gap>10
print("--- D. Trail + Gap>10 ---")
for sl in [8, 10, 12]:
    for tp in [25, 30, 40, 50]:
        t = backtest(dataset, stop_pts=sl, target_pts=tp, be_trigger=10,
                     trail_activation=15, trail_gap=8, gap_filter=10, entry_end="13:00")
        n = len(t); w = sum(1 for x in t if x["pnl"]>0)
        pnl = sum(x["pnl"] for x in t)
        gw = sum(x["pnl"] for x in t if x["pnl"]>0)
        gl = abs(sum(x["pnl"] for x in t if x["pnl"]<0))
        pf = gw/max(gl,0.01); avg = pnl/max(n,1)
        eq=0;pk=0;dd=0
        for x in t: eq+=x["pnl"];pk=max(pk,eq);dd=max(dd,pk-eq)
        oc = Counter(x["outcome"] for x in t)
        print(f"  SL={sl:>2} T={tp:>2}  {n:>3}t  WR={w/max(n,1)*100:>4.0f}%  PnL={pnl:>+6.0f}  PF={pf:>5.02f}  DD={dd:>4.0f}  Avg={avg:>+5.1f}  /wk={n/52:.1f}  {dict(oc)}")
    print()

# 5. +GEX target with trail
print("--- E. +GEX target + trail ---")
for sl in [8, 10, 12]:
    t = backtest(dataset, stop_pts=sl, target_mode="pos_gex", be_trigger=10,
                 trail_activation=15, trail_gap=8, entry_end="13:00")
    n = len(t); w = sum(1 for x in t if x["pnl"]>0)
    pnl = sum(x["pnl"] for x in t)
    gw = sum(x["pnl"] for x in t if x["pnl"]>0)
    gl = abs(sum(x["pnl"] for x in t if x["pnl"]<0))
    pf = gw/max(gl,0.01); avg = pnl/max(n,1)
    eq=0;pk=0;dd=0
    for x in t: eq+=x["pnl"];pk=max(pk,eq);dd=max(dd,pk-eq)
    oc = Counter(x["outcome"] for x in t)
    print(f"  SL={sl:>2} T=+GEX  {n:>3}t  WR={w/max(n,1)*100:>4.0f}%  PnL={pnl:>+6.0f}  PF={pf:>5.2f}  DD={dd:>4.0f}  Avg={avg:>+5.1f}  /wk={n/52:.1f}  {dict(oc)}")
print()

# ================================================================
# BEST CONFIG — detailed report + trade log
# ================================================================
print()
print("=" * 70)
print("  BEST CONFIGS — Detailed")
print("=" * 70)
print()

# Config 1: No filter, SL=10, T=20
t1 = backtest(dataset, stop_pts=10, target_pts=20, entry_end="13:00")
report(t1, "No filter | SL=10 T=20")
report_monthly(t1, "SL=10 T=20")

# Config 2: No filter, trail approach
t2 = backtest(dataset, stop_pts=10, target_pts=40, be_trigger=10,
              trail_activation=15, trail_gap=8, entry_end="13:00")
report(t2, "No filter | SL=10 T=40 BE@10 Trail@15/8")
report_monthly(t2, "SL=10 Trail")

# Config 3: Gap>10, trail
t3 = backtest(dataset, stop_pts=10, target_pts=40, be_trigger=10,
              trail_activation=15, trail_gap=8, gap_filter=10, entry_end="13:00")
report(t3, "Gap>10 | SL=10 T=40 BE@10 Trail@15/8")
report_monthly(t3, "Gap>10 Trail")

# Show trade log for the best one
print()
best = t2  # pick the one with highest avg PnL
best_label = "SL=10 T=40 BE@10 Trail@15/8"
trade_log(best, best_label)
