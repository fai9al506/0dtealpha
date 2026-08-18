# -*- coding: utf-8 -*-
"""S233 part 4 — candidate V17 filters + WALK-FORWARD validation.

Overfit guard: rules are SELECTED on a train window and SCORED on a test window
it never saw, in both directions (Mar-May -> Jun-Aug and Jun-Aug -> Mar-May).
"""
import sys, collections, itertools
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

CAP = int(sys.argv[1]) if len(sys.argv) > 1 else 2
SIZING = sys.argv[2] if len(sys.argv) > 2 else "basket"
ALL = frozenset(RULES.keys())

rows, gaps = load()
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]


def sub(pool, months=None, days=None):
    if months:
        return [r for r in pool if r["ts"].astimezone(ET).strftime("%Y-%m") in months]
    return [r for r in pool if r["ts"].astimezone(ET).date() in days]


def run(pool, off, vix_gate=None):
    """vix_gate=T: rules stay ON for signals with vix >= T, relaxed below (no lookahead —
    each signal is judged on its own VIX, which is known at detection)."""
    c = []
    for r in pool:
        use = frozenset() if (vix_gate is not None and (r["vix"] or 0) >= vix_gate) else off
        if passes(r, gaps, use)[0]:
            c.append(r)
    return sim(c, CAP, CAP, SIZING)


# -- greedy forward selection: repeatedly drop the rule with the best marginal gain --
def greedy(pool, max_drop=14, min_gain=25.0):
    off = set(); seq = []
    cur = run(pool, frozenset(off))["total"]
    while len(off) < max_drop:
        best = None
        for r in RULES:
            if r in off:
                continue
            t = run(pool, frozenset(off | {r}))["total"]
            if best is None or t > best[1]:
                best = (r, t)
        if best is None or best[1] - cur < min_gain:
            break
        off.add(best[0]); seq.append((best[0], best[1] - cur)); cur = best[1]
    return off, seq


TRAIN_A = ["2026-03", "2026-04", "2026-05"]
TEST_A = ["2026-06", "2026-07", "2026-08"]

print(f"### S233 walk-forward | cap {CAP}/{CAP} | sizing={SIZING}")
for train_m, test_m, lab in ((TRAIN_A, TEST_A, "Mar-May -> Jun-Aug"),
                             (TEST_A, TRAIN_A, "Jun-Aug -> Mar-May")):
    tr, te = sub(POOL, train_m), sub(POOL, test_m)
    off, seq = greedy(tr)
    b_tr, b_te = run(tr, frozenset()), run(te, frozenset())
    o_tr, o_te = run(tr, frozenset(off)), run(te, frozenset(off))
    n_te = run(te, ALL)
    print(f"\n-- train {lab} --")
    print("  greedy drop order:", ", ".join(f"{r}(+{g:,.0f})" for r, g in seq))
    print(f"  TRAIN  V16 ${b_tr['total']:,.0f} -> selected ${o_tr['total']:,.0f}  ({o_tr['total']-b_tr['total']:+,.0f})")
    print(f"  TEST   V16 ${b_te['total']:,.0f} -> selected ${o_te['total']:,.0f}  ({o_te['total']-b_te['total']:+,.0f})"
          f"   [no-filter on test = ${n_te['total']:,.0f}]")
    print(f"  TEST   MaxDD  V16 ${b_te['maxdd']:,.0f} -> selected ${o_te['maxdd']:,.0f}")

# -- named candidate filters, scored on the full window --
ROBUST = frozenset({"DD_SHORT", "V13VANNA", "ESABS_ALIGN", "VPB_GRADEB", "DDLONG_ALIGN_LO",
                    "ESABS_PARA", "V11_LATE", "DDLONG_GRADEC"})
MINIMAL = frozenset({"DD_SHORT"})
WIDE = ROBUST | {"SC_GRADE", "V11_DEADZONE", "GAP_LONG", "ESABS_GRADE", "SCDD_SHORT_GEXLIS",
                 "AG_TARGET", "DDLONG_ALIGN_HI", "DDLONG_VIX22", "VPB_HOUR11"}

CANDS = [("V16 (current live)", frozenset(), None),
         ("V17a  = V16 - DD_SHORT", MINIMAL, None),
         ("V17b  = V16 - robust8", ROBUST, None),
         ("V17c  = V16 - wide17", WIDE, None),
         ("V17d  = no filter", ALL, None),
         ("V17b + VIX>=20 full-V16", ROBUST, 20),
         ("V17b + VIX>=22 full-V16", ROBUST, 22),
         ("V17c + VIX>=22 full-V16", WIDE, 22),
         ("V17d + VIX>=22 full-V16", ALL, 22),
         ("V17d + VIX>=20 full-V16", ALL, 20)]

print(f"\n\n### candidates, full window 2026-03-16 -> 2026-08-07 (100 sessions)")
print(HDR)
store = {}
for lab, off, vg in CANDS:
    s = run(POOL, off, vg); store[lab] = s
    print(fmt(s, lab))

print(f"\n  {'candidate':<28}{'top3%':>7}{'ex-top3$':>10}{'medDay':>8}{'$/mo':>8}{'DD%eq':>7}")
for lab, _o, _v in CANDS:
    s = store[lab]
    print(f"  {lab:<28}{s['top3_share']:>6.0f}%{s['ex_top3']:>10,.0f}{s['median_day']:>8,.0f}"
          f"{s['total']/(s['sessions']/21):>8,.0f}{abs(s['maxdd'])/5161*100:>6.0f}%")

print("\n\n### per-month for the leading candidates")
labs = ["V16 (current live)", "V17a  = V16 - DD_SHORT", "V17b  = V16 - robust8",
        "V17c  = V16 - wide17", "V17d  = no filter", "V17c + VIX>=22 full-V16"]
ms = sorted(store["V16 (current live)"]["month"])
print(f"  {'candidate':<28}" + "".join(f"{m[-2:]:>9}" for m in ms))
for lab in labs:
    print(f"  {lab:<28}" + "".join(f"{store[lab]['month'].get(m,0):>9,.0f}" for m in ms))
