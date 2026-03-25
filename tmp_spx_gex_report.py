"""Full detailed report for SPX GEX Support Bounce backtest — with filters & trade log."""
import json, os, sys, io
from collections import defaultdict, Counter
from datetime import date as dt_date

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DATA_DIR = r"C:\Users\Faisa\stock_gex_data"
SPX_DIR = os.path.join(DATA_DIR, "spx")

# Load prices
with open(os.path.join(SPX_DIR, "prices", "SPX.json")) as f:
    price_bars = json.load(f)
prices = {int(b["date"]): b for b in price_bars}

# Load intraday
with open(os.path.join(SPX_DIR, "intraday", "SPY_5min.json")) as f:
    spy_bars = json.load(f)
intraday = defaultdict(list)
for b in spy_bars:
    intraday[b["date"]].append(b)
for d in intraday:
    intraday[d].sort(key=lambda x: x["ms_of_day"])

# GEX computation
def compute_gex(records):
    gex = {}
    for r in records:
        k = r.get("strike_dollars", r.get("strike", 0) / 1000.0)
        g = r.get("gamma", 0) * r.get("open_interest", 0) * 100
        if r.get("right") == "P":
            g = -g
        gex[k] = gex.get(k, 0) + g
    return gex

def extract_levels(gex, spot):
    neg = [(k, v) for k, v in gex.items() if v < 0]
    pos = [(k, v) for k, v in gex.items() if v > 0]
    if not neg or not pos:
        return None
    neg.sort(key=lambda x: x[1])
    pos.sort(key=lambda x: x[1], reverse=True)
    top_neg = neg[:5]
    mx = abs(top_neg[0][1])
    top_neg = [(k, v) for k, v in top_neg if abs(v) >= mx * 0.10]
    top_pos = pos[:5]
    mx = top_pos[0][1]
    top_pos = [(k, v) for k, v in top_pos if v >= mx * 0.10]
    total_gex = sum(v for _, v in gex.items())
    # zero-gamma line
    sorted_gex = sorted(gex.items(), key=lambda x: x[0])
    zero_gamma = None
    for i in range(len(sorted_gex) - 1):
        if sorted_gex[i][1] < 0 and sorted_gex[i+1][1] > 0:
            zero_gamma = (sorted_gex[i][0] + sorted_gex[i+1][0]) / 2.0
            break
    return {
        "strongest_neg": top_neg[0][0] if top_neg else None,
        "strongest_pos": top_pos[0][0] if top_pos else None,
        "regime": "positive" if total_gex > 0 else "negative",
        "total_gex": total_gex,
        "neg_count": len(top_neg),
        "neg_levels": top_neg,
        "pos_levels": top_pos,
        "zero_gamma": zero_gamma,
    }

# Build dataset
dataset = []
options_dir = os.path.join(SPX_DIR, "options")
for f in sorted(os.listdir(options_dir)):
    if not f.endswith("_0dte.json"):
        continue
    d = f.replace("_0dte.json", "")
    d_int = int(d.replace("-", ""))
    bar = prices.get(d_int)
    if not bar:
        continue
    with open(os.path.join(options_dir, f)) as fh:
        records = json.load(fh)
    if not records:
        continue
    spot = bar["open"]
    gex = compute_gex(records)
    levels = extract_levels(gex, spot)
    if not levels or not levels["strongest_neg"]:
        continue
    dataset.append({"date": d, "date_int": d_int, "open": bar["open"], "high": bar["high"],
                    "low": bar["low"], "close": bar["close"], **levels})

print(f"Dataset: {len(dataset)} days, Intraday: {len(intraday)} days")

# Intraday backtest
STOP = 8
TARGET = 10
trades = []
for day in dataset:
    neg = day["strongest_neg"]
    bars = intraday.get(day["date_int"], [])
    if not bars:
        continue
    ratio = day["open"] / bars[0]["open"] if bars[0]["open"] > 0 else 0
    if ratio <= 0:
        continue

    entered = False
    entry_price = None
    entry_time = None
    start_ms = 36000000  # 10:00
    end_ms = 54000000    # 15:00

    for b in bars:
        lo = b["low"] * ratio
        hi = b["high"] * ratio
        ms = b["ms_of_day"]

        if not entered:
            if ms < start_ms or ms > end_ms:
                continue
            if lo <= neg + 2:
                entered = True
                entry_price = neg
                entry_time = b["time"]
                if lo <= neg - STOP:
                    trades.append({"date": day["date"], "pnl": -STOP, "outcome": "STOP",
                        "entry": neg, "entry_time": entry_time, "exit_time": b["time"],
                        "regime": day["regime"], "neg_count": day["neg_count"]})
                    break
                if hi >= neg + TARGET:
                    trades.append({"date": day["date"], "pnl": TARGET, "outcome": "TARGET",
                        "entry": neg, "entry_time": entry_time, "exit_time": b["time"],
                        "regime": day["regime"], "neg_count": day["neg_count"]})
                    break
        else:
            if lo <= neg - STOP:
                trades.append({"date": day["date"], "pnl": -STOP, "outcome": "STOP",
                    "entry": neg, "entry_time": entry_time, "exit_time": b["time"],
                    "regime": day["regime"], "neg_count": day["neg_count"]})
                break
            if hi >= neg + TARGET:
                trades.append({"date": day["date"], "pnl": TARGET, "outcome": "TARGET",
                    "entry": neg, "entry_time": entry_time, "exit_time": b["time"],
                    "regime": day["regime"], "neg_count": day["neg_count"]})
                break

    if entered and (not trades or trades[-1]["date"] != day["date"]):
        last_spx = bars[-1]["close"] * ratio
        trades.append({"date": day["date"], "pnl": round(last_spx - entry_price, 1),
            "outcome": "EOD", "entry": neg, "entry_time": entry_time,
            "exit_time": bars[-1]["time"], "regime": day["regime"], "neg_count": day["neg_count"]})

# === FULL REPORT ===
print()
print("=" * 65)
print(f"  SPX GEX SUPPORT BOUNCE | SL={STOP} / T={TARGET} | INTRADAY 5-min")
print("=" * 65)
print()

wins = [t for t in trades if t["pnl"] > 0]
losses = [t for t in trades if t["pnl"] < 0]
flat = [t for t in trades if t["pnl"] == 0]
total_pnl = sum(t["pnl"] for t in trades)
gross_win = sum(t["pnl"] for t in wins)
gross_loss = abs(sum(t["pnl"] for t in losses))

print(f"  Total trades:  {len(trades)}")
print(f"  Trading days:  {len(dataset)}")
print(f"  Signal rate:   {len(trades)/len(dataset)*100:.0f}% of days ({len(trades)/len(dataset)*5:.1f}/week)")
print()
print(f"  Wins: {len(wins)} | Losses: {len(losses)} | Flat: {len(flat)}")
print(f"  Win Rate: {len(wins)/len(trades)*100:.1f}%")
print(f"  Total PnL: {total_pnl:+.1f} pts")
print(f"  Avg Win:   {gross_win/max(len(wins),1):+.1f} pts")
print(f"  Avg Loss:  {gross_loss/max(len(losses),1)*-1:+.1f} pts")
print(f"  PF: {gross_win/max(gross_loss,0.01):.2f}")
print(f"  Avg PnL/trade: {total_pnl/len(trades):+.2f} pts")
print()

outcomes = Counter(t["outcome"] for t in trades)
print(f"  Outcomes: TARGET={outcomes.get('TARGET',0)} STOP={outcomes.get('STOP',0)} EOD={outcomes.get('EOD',0)}")
print()

# Equity curve
equity = 0; peak = 0; max_dd = 0
consec_w = 0; consec_l = 0; max_cw = 0; max_cl = 0
for t in trades:
    equity += t["pnl"]
    peak = max(peak, equity)
    max_dd = max(max_dd, peak - equity)
    if t["pnl"] > 0:
        consec_w += 1; consec_l = 0; max_cw = max(max_cw, consec_w)
    elif t["pnl"] < 0:
        consec_l += 1; consec_w = 0; max_cl = max(max_cl, consec_l)

print(f"  Max Drawdown:      {max_dd:.1f} pts")
print(f"  Max Consec Wins:   {max_cw}")
print(f"  Max Consec Losses: {max_cl}")
print(f"  Final Equity:      {equity:+.1f} pts")
print()

# Monthly
print("  --- Monthly Breakdown ---")
monthly = defaultdict(lambda: {"pnl": 0, "n": 0, "w": 0, "l": 0})
for t in trades:
    m = t["date"][:7]
    monthly[m]["pnl"] += t["pnl"]
    monthly[m]["n"] += 1
    if t["pnl"] > 0:
        monthly[m]["w"] += 1
    elif t["pnl"] < 0:
        monthly[m]["l"] += 1

print(f"  {'Month':>8} {'Trades':>6} {'W':>4} {'L':>4} {'WR':>6} {'PnL':>8} {'Cumul':>8}")
cum = 0
green = 0
for m in sorted(monthly.keys()):
    d = monthly[m]
    wr = d["w"] / d["n"] * 100 if d["n"] else 0
    cum += d["pnl"]
    flag = " +" if d["pnl"] >= 0 else " -"
    if d["pnl"] >= 0:
        green += 1
    print(f"  {m:>8} {d['n']:>6} {d['w']:>4} {d['l']:>4} {wr:>5.0f}% {d['pnl']:>+8.1f} {cum:>+8.1f}{flag}")
print(f"  Green months: {green}/{len(monthly)} ({green/len(monthly)*100:.0f}%)")
print()

# By regime
print("  --- By GEX Regime ---")
for regime in ["positive", "negative"]:
    rt = [t for t in trades if t["regime"] == regime]
    if not rt:
        continue
    rw = sum(1 for t in rt if t["pnl"] > 0)
    rpnl = sum(t["pnl"] for t in rt)
    gw = sum(t["pnl"] for t in rt if t["pnl"] > 0)
    gl = abs(sum(t["pnl"] for t in rt if t["pnl"] < 0))
    pf = gw / max(gl, 0.01)
    print(f"  {regime.upper():>10}: {len(rt):>3} trades, WR={rw/len(rt)*100:.0f}%, PnL={rpnl:>+7.1f}, PF={pf:.2f}")
print()

# By entry time
print("  --- By Entry Time ---")
tb = defaultdict(lambda: {"n": 0, "w": 0, "pnl": 0})
for t in trades:
    et = t.get("entry_time", "")
    if not et:
        continue
    h = int(et.split(":")[0])
    if h < 11:
        bk = "10:00-10:59"
    elif h < 12:
        bk = "11:00-11:59"
    elif h < 13:
        bk = "12:00-12:59"
    elif h < 14:
        bk = "13:00-13:59"
    elif h < 15:
        bk = "14:00-14:59"
    else:
        bk = "15:00+"
    tb[bk]["n"] += 1
    tb[bk]["pnl"] += t["pnl"]
    if t["pnl"] > 0:
        tb[bk]["w"] += 1

print(f"  {'Time':>14} {'Trades':>6} {'WR':>6} {'PnL':>8} {'Avg':>7}")
for bk in sorted(tb.keys()):
    d = tb[bk]
    wr = d["w"] / d["n"] * 100 if d["n"] else 0
    avg = d["pnl"] / d["n"]
    print(f"  {bk:>14} {d['n']:>6} {wr:>5.0f}% {d['pnl']:>+8.1f} {avg:>+7.2f}")
print()

# By day of week
print("  --- By Day of Week ---")
dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri"]
db = defaultdict(lambda: {"n": 0, "w": 0, "pnl": 0})
for t in trades:
    d = dt_date.fromisoformat(t["date"])
    dow = dow_names[d.weekday()]
    db[dow]["n"] += 1
    db[dow]["pnl"] += t["pnl"]
    if t["pnl"] > 0:
        db[dow]["w"] += 1

print(f"  {'Day':>6} {'Trades':>6} {'WR':>6} {'PnL':>8} {'Avg':>7}")
for dow in dow_names:
    d = db[dow]
    if d["n"] == 0:
        continue
    wr = d["w"] / d["n"] * 100
    avg = d["pnl"] / d["n"]
    print(f"  {dow:>6} {d['n']:>6} {wr:>5.0f}% {d['pnl']:>+8.1f} {avg:>+7.2f}")
print()

# By neg_count (cluster strength)
print("  --- By -GEX Cluster Strength ---")
nc = defaultdict(lambda: {"n": 0, "w": 0, "pnl": 0})
for t in trades:
    c = t.get("neg_count", 0)
    nc[c]["n"] += 1
    nc[c]["pnl"] += t["pnl"]
    if t["pnl"] > 0:
        nc[c]["w"] += 1

print(f"  {'#NegLevels':>10} {'Trades':>6} {'WR':>6} {'PnL':>8} {'Avg':>7}")
for c in sorted(nc.keys()):
    d = nc[c]
    wr = d["w"] / d["n"] * 100 if d["n"] else 0
    avg = d["pnl"] / d["n"]
    print(f"  {c:>10} {d['n']:>6} {wr:>5.0f}% {d['pnl']:>+8.1f} {avg:>+7.2f}")
print()

# Top 10 best/worst
print("  --- Top 10 Winners ---")
st = sorted(trades, key=lambda t: t["pnl"], reverse=True)
for t in st[:10]:
    print(f"  {t['date']} entry=${t['entry']:.0f} {t['entry_time']}->{t['exit_time']} pnl={t['pnl']:+.1f} {t['outcome']}")

print()
print("  --- Top 10 Losers ---")
for t in st[-10:]:
    print(f"  {t['date']} entry=${t['entry']:.0f} {t['entry_time']}->{t['exit_time']} pnl={t['pnl']:+.1f} {t['outcome']}")

# ================================================================
# FILTER COMBINATIONS
# ================================================================
print()
print("=" * 65)
print("  FILTER ANALYSIS")
print("=" * 65)
print()

dow_names_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}

def get_entry_hour(t):
    et = t.get("entry_time", "")
    if not et:
        return -1
    return int(et.split(":")[0])

def get_dow(t):
    return dt_date.fromisoformat(t["date"]).weekday()

def report_filter(label, filtered):
    if not filtered:
        print(f"  {label}: 0 trades")
        return
    n = len(filtered)
    w = sum(1 for t in filtered if t["pnl"] > 0)
    wr = w / n * 100
    pnl = sum(t["pnl"] for t in filtered)
    gw = sum(t["pnl"] for t in filtered if t["pnl"] > 0)
    gl = abs(sum(t["pnl"] for t in filtered if t["pnl"] < 0))
    pf = gw / max(gl, 0.01)
    eq = 0; pk = 0; mdd = 0
    for t in filtered:
        eq += t["pnl"]; pk = max(pk, eq); mdd = max(mdd, pk - eq)
    avg = pnl / n
    print(f"  {label:<45} {n:>3}t  WR={wr:>5.1f}%  PnL={pnl:>+7.1f}  PF={pf:>5.2f}  DD={mdd:>5.1f}  Avg={avg:>+5.2f}")

# Time filters
print("  --- Time Filters ---")
report_filter("All (10:00-15:00)", trades)
report_filter("10:00-12:59", [t for t in trades if 10 <= get_entry_hour(t) <= 12])
report_filter("10:00-11:59", [t for t in trades if 10 <= get_entry_hour(t) <= 11])
report_filter("11:00-12:59", [t for t in trades if 11 <= get_entry_hour(t) <= 12])
report_filter("10:00-10:59 only", [t for t in trades if get_entry_hour(t) == 10])
report_filter("13:00+ only", [t for t in trades if get_entry_hour(t) >= 13])
print()

# Day filters
print("  --- Day of Week Filters ---")
report_filter("All days", trades)
report_filter("Wed-Fri", [t for t in trades if get_dow(t) >= 2])
report_filter("Wed-Thu", [t for t in trades if get_dow(t) in (2, 3)])
report_filter("Mon-Tue", [t for t in trades if get_dow(t) <= 1])
report_filter("Not Mon", [t for t in trades if get_dow(t) >= 1])
print()

# Regime filters
print("  --- Regime Filters ---")
report_filter("Positive GEX", [t for t in trades if t["regime"] == "positive"])
report_filter("Negative GEX", [t for t in trades if t["regime"] == "negative"])
print()

# Combined filters
print("  --- Combined Filters ---")
report_filter("10:00-12:59 + Positive GEX",
    [t for t in trades if 10 <= get_entry_hour(t) <= 12 and t["regime"] == "positive"])
report_filter("10:00-12:59 + Wed-Fri",
    [t for t in trades if 10 <= get_entry_hour(t) <= 12 and get_dow(t) >= 2])
report_filter("10:00-12:59 + Wed-Fri + Positive GEX",
    [t for t in trades if 10 <= get_entry_hour(t) <= 12 and get_dow(t) >= 2 and t["regime"] == "positive"])
report_filter("10:00-12:59 + Not Mon",
    [t for t in trades if 10 <= get_entry_hour(t) <= 12 and get_dow(t) >= 1])
report_filter("10:00-12:59 + Not Mon + Positive GEX",
    [t for t in trades if 10 <= get_entry_hour(t) <= 12 and get_dow(t) >= 1 and t["regime"] == "positive"])
report_filter("11:00-12:59 + Wed-Fri",
    [t for t in trades if 11 <= get_entry_hour(t) <= 12 and get_dow(t) >= 2])
report_filter("11:00-12:59 + Positive GEX",
    [t for t in trades if 11 <= get_entry_hour(t) <= 12 and t["regime"] == "positive"])
print()

# ================================================================
# FULL TRADE LOG
# ================================================================
print("=" * 65)
# ================================================================
# DEEPER FILTERS on 10:00-12:59 window
# ================================================================
print()
print("=" * 65)
print("  DEEPER FILTERS (10:00-12:59 base)")
print("=" * 65)
print()

base = [t for t in trades if 10 <= get_entry_hour(t) <= 12]
print(f"  Base: {len(base)} trades, WR={sum(1 for t in base if t['pnl']>0)/len(base)*100:.0f}%, PnL={sum(t['pnl'] for t in base):+.0f}")
print()

# Enrich trades with gap_at_open, neg_to_pos distance
enriched = []
for day in dataset:
    neg = day["strongest_neg"]
    pos = day["strongest_pos"]
    if neg is None:
        continue
    bars_day = intraday.get(day["date_int"], [])
    if not bars_day:
        continue
    r = day["open"] / bars_day[0]["open"] if bars_day[0]["open"] > 0 else 0
    if r <= 0:
        continue

    gap_at_open = day["open"] - neg
    neg_to_pos = (pos - neg) if pos else 0

    entered = False
    for bi, b in enumerate(bars_day):
        lo = b["low"] * r
        hi = b["high"] * r
        ms = b["ms_of_day"]
        if ms < 36000000 or ms > 46800000:
            continue
        if not entered and lo <= neg + 2:
            entered = True
            entry_time = b["time"]
            result = None
            # Check this bar and all subsequent
            for b2 in bars_day[bi:]:
                lo2 = b2["low"] * r
                hi2 = b2["high"] * r
                if lo2 <= neg - STOP:
                    result = {"pnl": -STOP, "outcome": "STOP", "exit_time": b2["time"]}
                    break
                if hi2 >= neg + TARGET:
                    result = {"pnl": TARGET, "outcome": "TARGET", "exit_time": b2["time"]}
                    break
            if not result:
                last_spx = bars_day[-1]["close"] * r
                result = {"pnl": round(last_spx - neg, 1), "outcome": "EOD", "exit_time": bars_day[-1]["time"]}
            enriched.append({"date": day["date"], "entry": neg, "entry_time": entry_time,
                "regime": day["regime"], "gap": gap_at_open, "neg_to_pos": neg_to_pos,
                "neg_count": day["neg_count"], **result})
            break

print(f"  Enriched trades (10:00-12:59): {len(enriched)}")
ew = sum(1 for t in enriched if t["pnl"] > 0)
print(f"  WR: {ew/len(enriched)*100:.0f}%, PnL: {sum(t['pnl'] for t in enriched):+.0f}")
print()

# Gap at open
print("  --- By Gap at Open (how far spot above -GEX) ---")
for label, lo, hi in [("Gap 0-10 (near -GEX)", 0, 10), ("Gap 10-20", 10, 20),
                       ("Gap 20-40", 20, 40), ("Gap 40-60", 40, 60), ("Gap 60+", 60, 999)]:
    ft = [t for t in enriched if lo <= t["gap"] < hi]
    report_filter(label, ft)
print()

# -GEX to +GEX distance
print("  --- By -GEX to +GEX Distance ---")
for label, lo, hi in [("Dist 0-15 (tight)", 0, 15), ("Dist 15-30", 15, 30),
                       ("Dist 30-50", 30, 50), ("Dist 50+", 50, 999)]:
    ft = [t for t in enriched if lo <= t["neg_to_pos"] < hi]
    report_filter(label, ft)
print()

# Combined
print("  --- Best Combos ---")
report_filter("All (base)", enriched)
report_filter("Positive GEX", [t for t in enriched if t["regime"] == "positive"])
report_filter("Gap<=30", [t for t in enriched if t["gap"] <= 30])
report_filter("Gap<=20", [t for t in enriched if t["gap"] <= 20])
report_filter("Positive + Gap<=30", [t for t in enriched if t["regime"] == "positive" and t["gap"] <= 30])
report_filter("Positive + Gap<=20", [t for t in enriched if t["regime"] == "positive" and t["gap"] <= 20])
report_filter("Not Mon", [t for t in enriched if get_dow(t) >= 1])
report_filter("Not Mon + Positive", [t for t in enriched if get_dow(t) >= 1 and t["regime"] == "positive"])
report_filter("Not Mon + Gap<=30", [t for t in enriched if get_dow(t) >= 1 and t["gap"] <= 30])
report_filter("Not Mon + Positive + Gap<=30",
    [t for t in enriched if get_dow(t) >= 1 and t["regime"] == "positive" and t["gap"] <= 30])
report_filter("Wed-Fri", [t for t in enriched if get_dow(t) >= 2])
report_filter("Wed-Fri + Positive", [t for t in enriched if get_dow(t) >= 2 and t["regime"] == "positive"])
report_filter("Wed-Fri + Gap<=30", [t for t in enriched if get_dow(t) >= 2 and t["gap"] <= 30])
report_filter("Wed-Fri + Positive + Gap<=30",
    [t for t in enriched if get_dow(t) >= 2 and t["regime"] == "positive" and t["gap"] <= 30])
report_filter("NegToPosGap<=25 (tight spread)",
    [t for t in enriched if t["neg_to_pos"] <= 25])
report_filter("NegToPosGap<=25 + Positive",
    [t for t in enriched if t["neg_to_pos"] <= 25 and t["regime"] == "positive"])
print()

# ================================================================
# RECOMMENDED FILTER: Gap > 10
# ================================================================
print("=" * 65)
print("  RECOMMENDED FILTER: Gap > 10 (spot opened >10 pts above -GEX)")
print("=" * 65)
print()

rec = [t for t in enriched if t["gap"] > 10]
rw = sum(1 for t in rec if t["pnl"] > 0)
rl = sum(1 for t in rec if t["pnl"] < 0)
rpnl = sum(t["pnl"] for t in rec)
gw = sum(t["pnl"] for t in rec if t["pnl"] > 0)
gl = abs(sum(t["pnl"] for t in rec if t["pnl"] < 0))

print(f"  Trades: {len(rec)} | W: {rw} L: {rl} | WR: {rw/len(rec)*100:.1f}%")
print(f"  PnL: {rpnl:+.1f} pts | PF: {gw/max(gl,0.01):.2f}")
print(f"  Avg/trade: {rpnl/len(rec):+.2f} pts | Per month: ~{len(rec)/12:.1f} trades")
eq=0;pk=0;mdd=0;mcw=0;mcl=0;cw=0;cl=0
for t in rec:
    eq+=t["pnl"];pk=max(pk,eq);mdd=max(mdd,pk-eq)
    if t["pnl"]>0: cw+=1;cl=0;mcw=max(mcw,cw)
    elif t["pnl"]<0: cl+=1;cw=0;mcl=max(mcl,cl)
print(f"  MaxDD: {mdd:.1f} pts | Max consec W: {mcw} | Max consec L: {mcl}")
print()

# Monthly for recommended
print("  --- Monthly ---")
rm = defaultdict(lambda: {"pnl":0,"n":0,"w":0,"l":0})
for t in rec:
    m=t["date"][:7]; rm[m]["pnl"]+=t["pnl"]; rm[m]["n"]+=1
    if t["pnl"]>0: rm[m]["w"]+=1
    elif t["pnl"]<0: rm[m]["l"]+=1
print(f"  {'Month':>8} {'Trades':>6} {'W':>4} {'L':>4} {'WR':>6} {'PnL':>8} {'Cumul':>8}")
cum=0;grn=0
for m in sorted(rm.keys()):
    d=rm[m]; wr=d["w"]/d["n"]*100 if d["n"] else 0; cum+=d["pnl"]
    if d["pnl"]>=0: grn+=1
    flag=" +" if d["pnl"]>=0 else " -"
    print(f"  {m:>8} {d['n']:>6} {d['w']:>4} {d['l']:>4} {wr:>5.0f}% {d['pnl']:>+8.1f} {cum:>+8.1f}{flag}")
print(f"  Green months: {grn}/{len(rm)} ({grn/len(rm)*100:.0f}%)")
print()

# Sub-filters on recommended
print("  --- Sub-filters on Gap>10 ---")
report_filter("Gap>10 (base)", rec)
report_filter("+ Positive GEX", [t for t in rec if t["regime"]=="positive"])
report_filter("+ Negative GEX", [t for t in rec if t["regime"]=="negative"])
report_filter("+ Not Mon", [t for t in rec if get_dow(t)>=1])
report_filter("+ Wed-Fri", [t for t in rec if get_dow(t)>=2])
report_filter("+ NegToPos>=30", [t for t in rec if t["neg_to_pos"]>=30])
report_filter("+ NegToPos>=30 + Positive", [t for t in rec if t["neg_to_pos"]>=30 and t["regime"]=="positive"])
report_filter("+ NegToPos>=30 + Not Mon", [t for t in rec if t["neg_to_pos"]>=30 and get_dow(t)>=1])
report_filter("+ Gap>20", [t for t in enriched if t["gap"]>20])
report_filter("+ Gap>20 + Positive", [t for t in enriched if t["gap"]>20 and t["regime"]=="positive"])
report_filter("+ Gap>20 + Not Mon", [t for t in enriched if t["gap"]>20 and get_dow(t)>=1])
print()

# By entry time on recommended
print("  --- By Entry Time (Gap>10 only) ---")
rtb = defaultdict(lambda: {"n":0,"w":0,"pnl":0})
for t in rec:
    h=get_entry_hour(t)
    if h<11: bk="10:00-10:59"
    elif h<12: bk="11:00-11:59"
    elif h<13: bk="12:00-12:59"
    else: bk="13:00+"
    rtb[bk]["n"]+=1; rtb[bk]["pnl"]+=t["pnl"]
    if t["pnl"]>0: rtb[bk]["w"]+=1
print(f"  {'Time':>14} {'Trades':>6} {'WR':>6} {'PnL':>8} {'Avg':>7}")
for bk in sorted(rtb.keys()):
    d=rtb[bk]; wr=d["w"]/d["n"]*100 if d["n"] else 0; avg=d["pnl"]/d["n"]
    print(f"  {bk:>14} {d['n']:>6} {wr:>5.0f}% {d['pnl']:>+8.1f} {avg:>+7.2f}")
print()

# By DOW on recommended
print("  --- By Day of Week (Gap>10 only) ---")
rdb = defaultdict(lambda: {"n":0,"w":0,"pnl":0})
dow_names_list = ["Mon","Tue","Wed","Thu","Fri"]
for t in rec:
    dow=dow_names_list[get_dow(t)]
    rdb[dow]["n"]+=1; rdb[dow]["pnl"]+=t["pnl"]
    if t["pnl"]>0: rdb[dow]["w"]+=1
print(f"  {'Day':>6} {'Trades':>6} {'WR':>6} {'PnL':>8} {'Avg':>7}")
for dow in dow_names_list:
    d=rdb[dow]
    if d["n"]==0: continue
    wr=d["w"]/d["n"]*100; avg=d["pnl"]/d["n"]
    print(f"  {dow:>6} {d['n']:>6} {wr:>5.0f}% {d['pnl']:>+8.1f} {avg:>+7.2f}")
print()

# Full trade log for recommended filter
dow_letter = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}
print("  --- Trade Log (Gap>10 only) ---")
print(f"  {'#':>3} {'Date':>10} {'Day':>3} {'Entry':>7} {'Gap':>5} {'Time':>12} {'PnL':>6} {'Cum':>7} {'Result':>7} {'Regime':>8}")
print(f"  {'-'*3} {'-'*10} {'-'*3} {'-'*7} {'-'*5} {'-'*12} {'-'*6} {'-'*7} {'-'*7} {'-'*8}")
rcum=0
for i,t in enumerate(rec,1):
    rcum+=t["pnl"]
    d=dt_date.fromisoformat(t["date"])
    dow=dow_letter[d.weekday()]
    tr=f"{t['entry_time']}->{t['exit_time']}"
    wm="W" if t["pnl"]>0 else "L"
    print(f"  {i:>3} {t['date']:>10} {dow:>3} ${t['entry']:>6.0f} {t['gap']:>5.0f} {tr:>12} {t['pnl']:>+6.1f} {rcum:>+7.1f} {t['outcome']:>7} {t['regime']:>8} {wm}")

# ================================================================
# FULL TRADE LOG (ALL)
# ================================================================
print()
print("=" * 65)
print("  FULL TRADE LOG (every trade)")
print("=" * 65)
print()

dow_letter = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri"}
cum_pnl = 0
print(f"  {'#':>3} {'Date':>10} {'Day':>3} {'Entry':>7} {'Time':>12} {'PnL':>6} {'Cum':>7} {'Result':>7} {'Regime':>8}")
print(f"  {'-'*3} {'-'*10} {'-'*3} {'-'*7} {'-'*12} {'-'*6} {'-'*7} {'-'*7} {'-'*8}")
for i, t in enumerate(trades, 1):
    cum_pnl += t["pnl"]
    d = dt_date.fromisoformat(t["date"])
    dow = dow_letter[d.weekday()]
    time_range = f"{t['entry_time']}->{t['exit_time']}"
    win_mark = "W" if t["pnl"] > 0 else "L" if t["pnl"] < 0 else "F"
    print(f"  {i:>3} {t['date']:>10} {dow:>3} ${t['entry']:>6.0f} {time_range:>12} {t['pnl']:>+6.1f} {cum_pnl:>+7.1f} {t['outcome']:>7} {t['regime']:>8} {win_mark}")
