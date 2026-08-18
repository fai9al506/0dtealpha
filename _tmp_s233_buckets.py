# -*- coding: utf-8 -*-
"""S233 part 6 — what EXACTLY does each candidate relaxation re-admit?

For each rule: the trades that the portfolio would actually TAKE if the rule were removed
(not the raw blocked bucket) — count, WR, points, monthly, grade/paradigm mix, time of day.
"""
import sys, collections, statistics
from _tmp_s233_sim import load, sim, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

CAP = int(sys.argv[1]) if len(sys.argv) > 1 else 2
SIZING = sys.argv[2] if len(sys.argv) > 2 else "basket"
rows, gaps = load()
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]


def taken(off):
    s = sim([r for r in POOL if passes(r, gaps, off)[0]], CAP, CAP, SIZING)
    return {t["id"]: t for t in s["trade_rows"]}, s


base_ids, base_s = taken(frozenset())
FOCUS = ["DD_SHORT", "ESABS_ALIGN", "V13VANNA", "VPB_GRADEB", "ESABS_PARA", "V11_LATE",
         "DDLONG_GRADEC", "DDLONG_ALIGN_LO", "SC_GRADE", "GAP_LONG", "V11_DEADZONE",
         "SCDD_SHORT_GEXLIS", "ESABS_GRADE", "AG_TARGET"]

print(f"### S233 what each relaxation actually TRADES (cap {CAP}/{CAP}, sizing={SIZING}, 100 sessions)")
print("    'new' = trades the portfolio takes that V16 did not; 'lost' = V16 trades displaced by cap/breaker\n")
for rule in FOCUS:
    ids, s = taken(frozenset([rule]))
    new = [t for i, t in ids.items() if i not in base_ids]
    lost = [t for i, t in base_ids.items() if i not in ids]
    if not new and not lost:
        continue
    npnl = sum(t["pnl"] for t in new); lpnl = sum(t["pnl"] for t in lost)
    wr = (sum(1 for t in new if t["pts"] > 0) / len(new) * 100) if new else 0
    mo = collections.defaultdict(float)
    for t in new:
        mo[t["date"].strftime("%m")] += t["pnl"]
    for t in lost:
        mo[t["date"].strftime("%m")] -= t["pnl"]
    gr = collections.Counter(t["grade"] for t in new)
    hrs = collections.Counter(t["et"].hour for t in new)
    setups = collections.Counter((t["setup"], "L" if t["long"] else "S") for t in new)
    print(f"  {rule}   net {s['total']-base_s['total']:+,.0f}$")
    print(f"    new {len(new)}t  WR {wr:.0f}%  ${npnl:+,.0f}   |   displaced {len(lost)}t ${lpnl:+,.0f}")
    print(f"    by month: " + " ".join(f"{k}:{v:+,.0f}" for k, v in sorted(mo.items())))
    print(f"    setups: " + ", ".join(f"{a} {b}={c}" for (a, b), c in setups.most_common()))
    print(f"    grades: {dict(gr)}   hours: {dict(sorted(hrs.items()))}")
    print()
