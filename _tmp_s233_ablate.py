# -*- coding: utf-8 -*-
"""S233 — per-rule marginal value of the V16 filter.

Two independent measurements per rule:
  LOO  (leave-one-out): full V16 minus that rule.  Delta = what the rule COSTS us.
  AOI  (add-one-in):    no filter plus that rule.   Delta = what the rule EARNS alone.
Plus the raw blocked bucket (what the rule discards, ungated).
"""
import sys, collections
from _tmp_s233_sim import load, sim, fmt, HDR
from _tmp_s233_rules import passes, RULES, WHITELIST

CAP = int(sys.argv[1]) if len(sys.argv) > 1 else 2
SIZING = sys.argv[2] if len(sys.argv) > 2 else "basket"
START = sys.argv[3] if len(sys.argv) > 3 else "2026-03-16"
END = sys.argv[4] if len(sys.argv) > 4 else "2026-08-07"

rows, gaps = load()
pool = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and START <= r["ts"].astimezone(__import__("_tmp_s233_sim").ET).date().isoformat() < END]

ALL = frozenset(RULES.keys())


def cands(off):
    return [r for r in pool if passes(r, gaps, off)[0]]


def run(off):
    return sim(cands(off), cap_l=CAP, cap_s=CAP, sizing=SIZING)


print(f"### S233 per-rule ablation | {START} -> {END} | cap {CAP}/{CAP} | sizing={SIZING} "
      f"| pool {len(pool)} resolved signals")
base = run(frozenset())
nofilt = run(ALL)
print("\n" + HDR)
print(fmt(base, "V16 (all rules ON)"))
print(fmt(nofilt, "NO filter (all rules OFF)"))
print(f"\n  concentration  V16: top3 {base['top3_share']:.0f}%  ex-top3 ${base['ex_top3']:,.0f}  "
      f"median day ${base['median_day']:,.0f}")
print(f"  concentration  OFF: top3 {nofilt['top3_share']:.0f}%  ex-top3 ${nofilt['ex_top3']:,.0f}  "
      f"median day ${nofilt['median_day']:,.0f}")

# ── raw blocked bucket per rule (first-blocking-rule attribution on the V16 path) ──
blocked = collections.defaultdict(list)
for r in pool:
    ok, why = passes(r, gaps)
    if not ok and why:
        blocked[why].append(r)

print("\n\n### A. RAW BUCKET each rule discards (ungated: no cap/dedup/breaker) — chain pts, 1 MES")
print(f"  {'rule':<20}{'n':>5}{'WR':>6}{'pts':>9}{'$@1MES':>9}{'pts/t':>7}   monthly $ (1 MES)")
agg = []
for rule, rs in sorted(blocked.items(), key=lambda kv: -sum(float(x['outcome_pnl']) for x in kv[1])):
    n = len(rs)
    pts = sum(float(x["outcome_pnl"]) for x in rs)
    wr = sum(1 for x in rs if float(x["outcome_pnl"]) > 0) / n * 100
    mo = collections.defaultdict(float)
    for x in rs:
        mo[x["ts"].astimezone(__import__("_tmp_s233_sim").ET).strftime("%Y-%m")] += float(x["outcome_pnl"]) * 5
    ms = " ".join(f"{k[-2:]}:{v:+,.0f}" for k, v in sorted(mo.items()))
    print(f"  {rule:<20}{n:>5}{wr:>5.0f}%{pts:>9,.1f}{pts*5:>9,.0f}{pts/n:>7.2f}   {ms}")
    agg.append((rule, n, pts))

# ── LOO / AOI portfolio deltas ──
print("\n\n### B. PORTFOLIO marginal value (same gates, same sizing, cap %d/%d)" % (CAP, CAP))
print(f"  {'rule':<20}{'LOO $':>10}{'dLOO':>9}{'dDD':>9}{'dTrades':>9} | {'AOI $':>9}{'dAOI':>9}")
res = []
for rule in sorted(RULES):
    if rule not in blocked:
        continue  # rule never fires in this window
    loo = run(frozenset([rule]))
    aoi = run(ALL - frozenset([rule]))
    res.append((rule, loo, aoi))
for rule, loo, aoi in sorted(res, key=lambda x: -(x[1]["total"] - base["total"])):
    d = loo["total"] - base["total"]
    ddd = loo["maxdd"] - base["maxdd"]
    dt = loo["trades"] - base["trades"]
    da = nofilt["total"] - aoi["total"]   # >0 => adding the rule back COSTS money
    print(f"  {rule:<20}{loo['total']:>10,.0f}{d:>+9,.0f}{ddd:>+9,.0f}{dt:>+9} | "
          f"{aoi['total']:>9,.0f}{da:>+9,.0f}")
print("\n  dLOO  > 0  => removing the rule MAKES money (rule is costing us)")
print("  dAOI  > 0  => adding the rule to a no-filter book COSTS money (consistent with dLOO>0)")
