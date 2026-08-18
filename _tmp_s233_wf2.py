# -*- coding: utf-8 -*-
"""S233 part 5 — walk-forward of the SELECTION METHOD (stability, not greedy-on-total)
plus era checks (the DD trail changed 2026-06-22; S217 shipped 2026-06-13).
"""
import sys, collections, statistics
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

CAP = int(sys.argv[1]) if len(sys.argv) > 1 else 2
SIZING = sys.argv[2] if len(sys.argv) > 2 else "basket"
ALL = frozenset(RULES.keys())
rows, gaps = load()
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]


def m_of(r):
    return r["ts"].astimezone(ET).strftime("%Y-%m")


def run(pool, off):
    return sim([r for r in pool if passes(r, gaps, off)[0]], CAP, CAP, SIZING)


def select_by_stability(pool, min_pos=3, min_total=50.0):
    """Drop a rule only if removing it helps in >= min_pos months AND helps more often
    than it hurts AND is net positive. Selection uses ONLY the pool it is given."""
    months = sorted({m_of(r) for r in pool})
    base = {m: run([r for r in pool if m_of(r) == m], frozenset())["total"] for m in months}
    chosen, detail = set(), []
    for rule in sorted(RULES):
        d = {}
        for m in months:
            d[m] = run([r for r in pool if m_of(r) == m], frozenset([rule]))["total"] - base[m]
        tot = sum(d.values())
        pos = sum(1 for v in d.values() if v > 1); neg = sum(1 for v in d.values() if v < -1)
        if tot >= min_total and pos >= min_pos and pos > neg:
            chosen.add(rule); detail.append((rule, tot, pos, neg))
    return chosen, detail


TRAIN_A = ["2026-03", "2026-04", "2026-05"]; TEST_A = ["2026-06", "2026-07", "2026-08"]
print(f"### S233 walk-forward of the STABILITY selector | cap {CAP}/{CAP} | sizing={SIZING}")
for tr_m, te_m, lab in ((TRAIN_A, TEST_A, "train Mar-May -> test Jun-Aug"),
                        (TEST_A, TRAIN_A, "train Jun-Aug -> test Mar-May")):
    tr = [r for r in POOL if m_of(r) in tr_m]; te = [r for r in POOL if m_of(r) in te_m]
    ch, det = select_by_stability(tr, min_pos=2)
    b, o, n = run(te, frozenset()), run(te, frozenset(ch)), run(te, ALL)
    print(f"\n-- {lab} --")
    print("  selected (train-only):", ", ".join(sorted(ch)) or "(none)")
    print(f"  TEST  V16 ${b['total']:,.0f} (DD {b['maxdd']:,.0f})  ->  selected ${o['total']:,.0f} "
          f"(DD {o['maxdd']:,.0f})   {o['total']-b['total']:+,.0f}      no-filter ${n['total']:,.0f} (DD {n['maxdd']:,.0f})")

# ── leave-one-month-out: 6 folds, select on 5 months, score the held-out month ──
print("\n\n### leave-one-month-out (select on the other 5 months, score the held-out one)")
months = sorted({m_of(r) for r in POOL})
tot_b = tot_o = tot_n = 0.0
print(f"  {'held-out':<10}{'V16':>9}{'V17(sel)':>10}{'diff':>9}{'no-filter':>11}   selected on the other 5")
for m in months:
    tr = [r for r in POOL if m_of(r) != m]; te = [r for r in POOL if m_of(r) == m]
    ch, _ = select_by_stability(tr, min_pos=3)
    b, o, n = run(te, frozenset()), run(te, frozenset(ch)), run(te, ALL)
    tot_b += b["total"]; tot_o += o["total"]; tot_n += n["total"]
    print(f"  {m:<10}{b['total']:>9,.0f}{o['total']:>10,.0f}{o['total']-b['total']:>+9,.0f}"
          f"{n['total']:>11,.0f}   {len(ch)} rules: {','.join(sorted(ch))[:70]}")
print(f"  {'TOTAL':<10}{tot_b:>9,.0f}{tot_o:>10,.0f}{tot_o-tot_b:>+9,.0f}{tot_n:>11,.0f}")
print("  ^ this is the honest out-of-sample number: every month scored by a filter chosen without it")

# ── era check: outcome model changed 2026-06-13 (S217) and 2026-06-22 (DD trail 20/5 -> 10/10) ──
ROBUST = frozenset({"DD_SHORT", "V13VANNA", "ESABS_ALIGN", "VPB_GRADEB", "DDLONG_ALIGN_LO",
                    "ESABS_PARA", "V11_LATE", "DDLONG_GRADEC"})
print("\n\n### era check (DD trail params changed 2026-06-22; chain sim validated post-S217 2026-06-13)")
print(HDR)
for lo, hi, lab in (("2026-03-16", "2026-06-13", "pre-S217"),
                    ("2026-06-13", "2026-06-23", "S217->trail"),
                    ("2026-06-23", "2026-08-07", "post-trail-change")):
    p = [r for r in POOL if lo <= r["ts"].astimezone(ET).date().isoformat() < hi]
    for off, nm in ((frozenset(), "V16"), (ROBUST, "V17b robust8"), (ALL, "no filter")):
        print(fmt(run(p, off), f"{lab:<18}{nm}"))
    print()
