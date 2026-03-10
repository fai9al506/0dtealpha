"""
Compare current vs recommended configs for Eval Real and SIM.
"""
import json
from collections import defaultdict
from datetime import datetime, timedelta

with open("C:/Users/Faisa/AppData/Local/Temp/trade_data.json") as f:
    trades = json.load(f)

def r1_filter(t):
    setup = t["setup"]
    ga = t.get("greek_alignment")
    svb = t.get("svb")
    if setup == "GEX Long" and (ga is None or ga < 1): return False
    if setup == "AG Short" and ga == -3: return False
    if setup == "DD Exhaustion":
        if svb is not None and -0.5 <= svb < 0: return False
    if setup == "ES Absorption" and ga is not None and ga < 0: return False
    return True

filtered = [t for t in trades if r1_filter(t)]
filtered.sort(key=lambda x: x["ts"])

by_date = defaultdict(list)
for t in filtered:
    by_date[t["trade_date"]].append(t)

def estimate_hold(t):
    e = t.get("elapsed_min")
    if e and e > 0: return e
    return {"Skew Charm": 30, "DD Exhaustion": 45, "AG Short": 30,
            "GEX Long": 30, "BofA Scalp": 20, "Paradigm Reversal": 20,
            "ES Absorption": 20, "CVD Divergence": 20, "Vanna Pivot Bounce": 30}.get(t["setup"], 30)

PRIORITY = {
    "Skew Charm": 1, "Paradigm Reversal": 2, "AG Short": 3,
    "DD Exhaustion": 4, "ES Absorption": 5, "GEX Long": 6, "BofA Scalp": 7,
}

n_days = len(by_date)

def sim_single_pos(day_trades, quality_fn=None):
    taken = []
    pos_free = None
    for t in sorted(day_trades, key=lambda x: (x["ts"], PRIORITY.get(x["setup"], 99))):
        tt = datetime.fromisoformat(t["ts"])
        te = tt + timedelta(minutes=estimate_hold(t))
        if quality_fn and not quality_fn(t):
            continue
        if pos_free is None or tt >= pos_free:
            taken.append(t)
            pos_free = te
    return taken

def run_config(name, setup_filter, quality_fn=None):
    total_pnl = 0
    total_n = 0
    wins = 0
    losses = 0
    daily_pnls = []
    setup_stats = defaultdict(lambda: {"n": 0, "w": 0, "l": 0, "pnl": 0})

    for date in sorted(by_date.keys()):
        day_trades = [t for t in by_date[date] if t["setup"] in setup_filter]
        taken = sim_single_pos(day_trades, quality_fn)
        pnl = sum(t["pnl"] for t in taken)
        total_pnl += pnl
        total_n += len(taken)
        wins += sum(1 for t in taken if t["result"] == "WIN")
        losses += sum(1 for t in taken if t["result"] == "LOSS")
        daily_pnls.append(pnl)
        for t in taken:
            s = setup_stats[t["setup"]]
            s["n"] += 1
            s["pnl"] += t["pnl"]
            if t["result"] == "WIN": s["w"] += 1
            elif t["result"] == "LOSS": s["l"] += 1

    wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    cum = peak = dd = 0
    for p in daily_pnls:
        cum += p; peak = max(peak, cum); dd = max(dd, peak - cum)
    green = sum(1 for p in daily_pnls if p > 0)
    red = sum(1 for p in daily_pnls if p < 0)

    return {
        "name": name, "n": total_n, "w": wins, "l": losses, "wr": wr,
        "pnl": total_pnl, "daily": total_pnl / n_days, "dd": dd,
        "green": green, "red": red, "setup_stats": dict(setup_stats),
    }

# ===== EVAL REAL =====
eval_cur_setups = {"Paradigm Reversal", "DD Exhaustion", "AG Short", "Skew Charm"}
eval_rec_setups = {"Paradigm Reversal", "DD Exhaustion", "AG Short", "Skew Charm"}

eval_cur = run_config("CURRENT", eval_cur_setups)
eval_rec = run_config("RECOMMENDED", eval_rec_setups,
    quality_fn=lambda t: t.get("greek_alignment") is not None and t["greek_alignment"] >= 1)

# ===== SIM =====
sim_cur_setups = {"AG Short", "Paradigm Reversal", "ES Absorption", "Skew Charm", "DD Exhaustion"}
sim_rec_setups = {"AG Short", "Paradigm Reversal", "Skew Charm", "DD Exhaustion", "BofA Scalp", "GEX Long"}

sim_cur = run_config("CURRENT", sim_cur_setups)
sim_rec = run_config("RECOMMENDED", sim_rec_setups)

# ===== PRINT =====
def fmt_pct(v):
    return "%.1f%%" % v

def fmt_pts(v):
    return "%+.1f pts" % v

def fmt_num(v):
    return "%+.1f" % v

def fmt_money(v):
    return "$%s" % "{:,.0f}".format(v)

def print_comparison(title, cur, rec, cur_desc, rec_desc, scale_label, scale_mult):
    print("=" * 100)
    print(title)
    print("=" * 100)
    print()

    print("%-30s %-25s %-25s %-15s" % ("Metric", "CURRENT", "RECOMMENDED", "CHANGE"))
    print("-" * 95)
    print("%-30s %-25s %-25s" % ("Config", cur_desc, rec_desc))
    print("%-30s %-25s %-25s %-15s" % ("Trades (21 days)", cur["n"], rec["n"], "%+d" % (rec["n"]-cur["n"])))
    print("%-30s %-25s %-25s %-15s" % ("Trades/day", "%.1f" % (cur["n"]/n_days), "%.1f" % (rec["n"]/n_days), "%+.1f" % ((rec["n"]-cur["n"])/n_days)))
    print("%-30s %-25s %-25s %-15s" % ("Win Rate", fmt_pct(cur["wr"]), fmt_pct(rec["wr"]), "%+.1f%%" % (rec["wr"]-cur["wr"])))
    print("%-30s %-25s %-25s %-15s" % ("Total PnL", fmt_pts(cur["pnl"]), fmt_pts(rec["pnl"]), fmt_num(rec["pnl"]-cur["pnl"])))
    print("%-30s %-25s %-25s %-15s" % ("Pts/day", fmt_num(cur["daily"]), fmt_num(rec["daily"]), fmt_num(rec["daily"]-cur["daily"])))
    print("%-30s %-25s %-25s %-15s" % ("Max Drawdown", "%.1f pts" % cur["dd"], "%.1f pts" % rec["dd"], fmt_num(rec["dd"]-cur["dd"])))
    print("%-30s %-25s %-25s" % ("Green/Red days", "%dG / %dR" % (cur["green"], cur["red"]), "%dG / %dR" % (rec["green"], rec["red"])))

    cur_mo = cur["daily"] * 21 * scale_mult
    rec_mo = rec["daily"] * 21 * scale_mult
    delta_str = "$%s" % "{:+,.0f}".format(rec_mo-cur_mo)
    print("%-30s %-25s %-25s %-15s" % ("Monthly @ " + scale_label, fmt_money(cur_mo), fmt_money(rec_mo), delta_str))

    print()
    print("Per-setup breakdown:")
    print("  %-23s %-35s %-35s" % ("Setup", "CURRENT", "RECOMMENDED"))
    print("  " + "-" * 93)
    all_s = sorted(set(list(cur["setup_stats"].keys()) + list(rec["setup_stats"].keys())))
    for setup in all_s:
        c = cur["setup_stats"].get(setup, {"n": 0, "w": 0, "l": 0, "pnl": 0})
        r = rec["setup_stats"].get(setup, {"n": 0, "w": 0, "l": 0, "pnl": 0})
        def fmt(s):
            if s["n"] == 0: return "(off/filtered)"
            wr = s["w"]/(s["w"]+s["l"])*100 if (s["w"]+s["l"]) > 0 else 0
            return "N=%2d WR=%5.1f%% PnL=%+7.1f" % (s["n"], wr, s["pnl"])
        print("  %-23s %-35s %-35s" % (setup, fmt(c), fmt(r)))
    print()

print_comparison(
    "EVAL REAL: Current vs Recommended (add alignment >= +1 gate)",
    eval_cur, eval_rec,
    "Para/DD/AG/Skew + Greek", "Same + align >= +1",
    "10 MES", 50
)

print()
print_comparison(
    "SIM: Current vs Recommended (drop ES Absorption, add BofA + GEX)",
    sim_cur, sim_rec,
    "AG/Para/Abs/Skew/DD", "AG/Para/Skew/DD/BofA/GEX",
    "10 MES", 50
)

# SUMMARY
print("=" * 100)
print("BOTTOM LINE")
print("=" * 100)
print()

print("EVAL REAL:")
if eval_rec["pnl"] > eval_cur["pnl"]:
    print("  Alignment gate IMPROVES: %+.1f -> %+.1f pts (%+.1f)" % (eval_cur["pnl"], eval_rec["pnl"], eval_rec["pnl"]-eval_cur["pnl"]))
else:
    print("  Alignment gate COSTS: %+.1f -> %+.1f pts (%+.1f)" % (eval_cur["pnl"], eval_rec["pnl"], eval_rec["pnl"]-eval_cur["pnl"]))
print("  BUT: WR %.0f%% -> %.0f%%, DD %.0f -> %.0f pts, trades/day %.1f -> %.1f" % (
    eval_cur["wr"], eval_rec["wr"], eval_cur["dd"], eval_rec["dd"],
    eval_cur["n"]/n_days, eval_rec["n"]/n_days))
print()

print("SIM:")
if sim_rec["pnl"] > sim_cur["pnl"]:
    print("  Dropping Absorption IMPROVES: %+.1f -> %+.1f pts (%+.1f)" % (sim_cur["pnl"], sim_rec["pnl"], sim_rec["pnl"]-sim_cur["pnl"]))
else:
    print("  Dropping Absorption COSTS: %+.1f -> %+.1f pts (%+.1f)" % (sim_cur["pnl"], sim_rec["pnl"], sim_rec["pnl"]-sim_cur["pnl"]))
print("  WR: %.0f%% -> %.0f%%, DD %.0f -> %.0f pts, trades/day %.1f -> %.1f" % (
    sim_cur["wr"], sim_rec["wr"], sim_cur["dd"], sim_rec["dd"],
    sim_cur["n"]/n_days, sim_rec["n"]/n_days))
