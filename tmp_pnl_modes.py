import os
from sqlalchemy import create_engine, text
from collections import defaultdict

engine = create_engine(os.environ["DATABASE_URL"])

with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, ts, setup_name, direction, outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss, outcome_stop_level, spot,
               abs_es_price, grade
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND outcome_pnl IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

print(f"Total trades with outcomes: {len(rows)}\n")

trailing_setups = ("DD Exhaustion", "GEX Long", "AG Short")

results = {}
for m in ("A", "B", "C"):
    results[m] = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "expired": 0, "pnl": 0.0})

for r in rows:
    setup = r["setup_name"]
    pnl_a = float(r["outcome_pnl"] or 0)
    max_p = float(r["outcome_max_profit"] or 0)
    res = r["outcome_result"]

    is_trailing = setup in trailing_setups
    is_absorption = setup in ("ES Absorption", "Skew Charm")
    t1_hit = max_p >= 10

    # Mode A: full position (as-is)
    pnl_mode_a = pnl_a
    res_mode_a = res

    # Mode B: split target (T1=+10, T2=trail, average)
    if (is_trailing or is_absorption) and t1_hit:
        pnl_mode_b = round((10.0 + pnl_a) / 2, 1)
        res_mode_b = "WIN" if pnl_mode_b > 0 else ("LOSS" if pnl_mode_b < 0 else "WIN")
    else:
        pnl_mode_b = pnl_a
        res_mode_b = res

    # Mode C: BE-only (T1=+10, T2=breakeven, average = +5 always)
    if (is_trailing or is_absorption) and t1_hit:
        pnl_mode_c = 5.0
        res_mode_c = "WIN"
    else:
        pnl_mode_c = pnl_a
        res_mode_c = res

    for mode, pnl_m, res_m in [("A", pnl_mode_a, res_mode_a),
                                 ("B", pnl_mode_b, res_mode_b),
                                 ("C", pnl_mode_c, res_mode_c)]:
        d = results[mode][setup]
        d["trades"] += 1
        d["pnl"] += pnl_m
        if res_m == "WIN":
            d["wins"] += 1
        elif res_m == "LOSS":
            d["losses"] += 1
        else:
            d["expired"] += 1

        t = results[mode]["_TOTAL"]
        t["trades"] += 1
        t["pnl"] += pnl_m
        if res_m == "WIN":
            t["wins"] += 1
        elif res_m == "LOSS":
            t["losses"] += 1
        else:
            t["expired"] += 1

# Print comparison tables
for mode_name, mode_desc in [("A", "FULL POSITION (current - trail on 100%)"),
                               ("B", "SPLIT TARGET (T1=+10 half, T2=trail half, average)"),
                               ("C", "BE-ONLY (T1=+10 half, T2=BE half, always +5 if T1 hit)")]:
    print(f"\n{'='*70}")
    print(f"MODE {mode_name}: {mode_desc}")
    print(f"{'='*70}")
    hdr = f"{'Setup':20s} {'Trades':>6s} {'W':>4s} {'L':>4s} {'E':>4s} {'WR':>6s} {'PnL':>8s} {'Avg':>6s}"
    print(hdr)
    print("-" * 60)
    for setup in ["AG Short", "BofA Scalp", "DD Exhaustion", "ES Absorption",
                   "GEX Long", "Paradigm Reversal", "Skew Charm"]:
        d = results[mode_name].get(setup)
        if not d or d["trades"] == 0:
            continue
        wr = d["wins"] / max(d["wins"] + d["losses"], 1) * 100
        avg = d["pnl"] / max(d["trades"], 1)
        print(f"{setup:20s} {d['trades']:6d} {d['wins']:4d} {d['losses']:4d} {d['expired']:4d} {wr:5.0f}% {d['pnl']:+8.1f} {avg:+6.1f}")

    t = results[mode_name]["_TOTAL"]
    wr = t["wins"] / max(t["wins"] + t["losses"], 1) * 100
    avg = t["pnl"] / max(t["trades"], 1)
    print("-" * 60)
    print(f"{'TOTAL':20s} {t['trades']:6d} {t['wins']:4d} {t['losses']:4d} {t['expired']:4d} {wr:5.0f}% {t['pnl']:+8.1f} {avg:+6.1f}")

# Setup-by-setup delta comparison
print(f"\n{'='*70}")
print(f"SETUP COMPARISON: A vs B vs C")
print(f"{'='*70}")
for setup in ["DD Exhaustion", "AG Short", "GEX Long", "ES Absorption"]:
    a = results["A"].get(setup, {"trades": 0, "pnl": 0, "wins": 0, "losses": 0, "expired": 0})
    b = results["B"].get(setup, {"trades": 0, "pnl": 0, "wins": 0, "losses": 0, "expired": 0})
    c = results["C"].get(setup, {"trades": 0, "pnl": 0, "wins": 0, "losses": 0, "expired": 0})

    wr_a = a["wins"] / max(a["wins"] + a["losses"], 1) * 100
    wr_b = b["wins"] / max(b["wins"] + b["losses"], 1) * 100
    wr_c = c["wins"] / max(c["wins"] + c["losses"], 1) * 100

    print(f"\n  {setup} ({a['trades']} trades):")
    print(f"    Full:      {a['wins']:3d}W/{a['losses']:2d}L  WR={wr_a:4.0f}%  PnL={a['pnl']:+7.1f}  avg={a['pnl']/max(a['trades'],1):+5.1f}")
    print(f"    Split:     {b['wins']:3d}W/{b['losses']:2d}L  WR={wr_b:4.0f}%  PnL={b['pnl']:+7.1f}  avg={b['pnl']/max(b['trades'],1):+5.1f}")
    print(f"    BE-only:   {c['wins']:3d}W/{c['losses']:2d}L  WR={wr_c:4.0f}%  PnL={c['pnl']:+7.1f}  avg={c['pnl']/max(c['trades'],1):+5.1f}")

# T1 hit breakdown
print(f"\n{'='*70}")
print(f"T1 HIT RATE (max_profit >= 10)")
print(f"{'='*70}")
for setup in ["DD Exhaustion", "AG Short", "GEX Long"]:
    all_trades = [r for r in rows if r["setup_name"] == setup]
    t1_trades = [r for r in all_trades if float(r["outcome_max_profit"] or 0) >= 10]
    no_t1 = [r for r in all_trades if float(r["outcome_max_profit"] or 0) < 10]

    t1_w = len([t for t in t1_trades if t["outcome_result"] == "WIN"])
    t1_l = len([t for t in t1_trades if t["outcome_result"] == "LOSS"])
    no_w = len([t for t in no_t1 if t["outcome_result"] == "WIN"])
    no_l = len([t for t in no_t1 if t["outcome_result"] == "LOSS"])

    t1_pnl = sum(float(t["outcome_pnl"] or 0) for t in t1_trades)
    no_pnl = sum(float(t["outcome_pnl"] or 0) for t in no_t1)

    t1_pct = len(t1_trades) / max(len(all_trades), 1) * 100
    print(f"\n  {setup}:")
    print(f"    T1 hit:    {len(t1_trades):3d}/{len(all_trades)} ({t1_pct:.0f}%)  {t1_w}W/{t1_l}L  full_pnl={t1_pnl:+.1f}  BE_pnl={5.0*len(t1_trades):+.1f}")
    print(f"    T1 missed: {len(no_t1):3d}/{len(all_trades)}           {no_w}W/{no_l}L  pnl={no_pnl:+.1f}")

# Daily breakdown
print(f"\n{'='*70}")
print(f"DAILY P&L (all 3 modes)")
print(f"{'='*70}")

daily = defaultdict(lambda: {"a": 0.0, "b": 0.0, "c": 0.0, "n": 0})
for r in rows:
    d = r["ts"].strftime("%Y-%m-%d")
    pnl_a = float(r["outcome_pnl"] or 0)
    max_p = float(r["outcome_max_profit"] or 0)
    setup = r["setup_name"]
    is_t = setup in trailing_setups or setup in ("ES Absorption", "Skew Charm")
    t1 = max_p >= 10

    pnl_b = round((10.0 + pnl_a) / 2, 1) if is_t and t1 else pnl_a
    pnl_c = 5.0 if is_t and t1 else pnl_a

    daily[d]["a"] += pnl_a
    daily[d]["b"] += pnl_b
    daily[d]["c"] += pnl_c
    daily[d]["n"] += 1

print(f"  {'Date':>12s} {'N':>4s} {'Full(A)':>8s} {'Split(B)':>8s} {'BE(C)':>8s}  {'Best':>6s}")
print(f"  {'-'*52}")
sum_a = sum_b = sum_c = 0
neg_days = {"a": 0, "b": 0, "c": 0}
for d in sorted(daily.keys()):
    v = daily[d]
    best = "A" if v["a"] >= v["b"] and v["a"] >= v["c"] else ("B" if v["b"] >= v["c"] else "C")
    print(f"  {d:>12s} {v['n']:4d} {v['a']:+8.1f} {v['b']:+8.1f} {v['c']:+8.1f}  {best:>6s}")
    sum_a += v["a"]
    sum_b += v["b"]
    sum_c += v["c"]
    if v["a"] < 0: neg_days["a"] += 1
    if v["b"] < 0: neg_days["b"] += 1
    if v["c"] < 0: neg_days["c"] += 1

n_days = len(daily)
print(f"  {'-'*52}")
print(f"  {'TOTAL':>12s} {sum(v['n'] for v in daily.values()):4d} {sum_a:+8.1f} {sum_b:+8.1f} {sum_c:+8.1f}")
print(f"  {'Per day':>12s} {'':4s} {sum_a/n_days:+8.1f} {sum_b/n_days:+8.1f} {sum_c/n_days:+8.1f}")
print(f"  {'Neg days':>12s} {'':4s} {neg_days['a']:8d} {neg_days['b']:8d} {neg_days['c']:8d}")
print(f"  {'Days':>12s} {'':4s} {n_days:8d} {n_days:8d} {n_days:8d}")
