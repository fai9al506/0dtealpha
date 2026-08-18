# -*- coding: utf-8 -*-
"""V18 step 3 — (a) a FAIRER selector before declaring fitting impossible,
(b) the real CEILING: how much money is physically available to any filter."""
import collections, statistics
from _tmp_v18_data import load, enrich, REAL_SETUPS, wr, tot, ppt, ET
from _tmp_v18_engine import ALLR, MONTHS, gen_conditions, apply_rules, raw_stats, gaps
from _tmp_s233_rules import passes as v16pass
from _tmp_s233_sim import sim

BASE = [r for r in ALLR if r["setup_name"] in REAL_SETUPS]


# ── (a) a consensus selector: a condition must help in MOST train months ──
def select_consensus(train, min_months=4, min_keep=0.5, max_rules=3):
    months = sorted({x["month"] for x in train})
    conds = gen_conditions(train)
    chosen, cur = [], list(train)
    for _ in range(max_rules):
        best = None
        for name, fn in conds:
            if any(name == c[0] for c in chosen):
                continue
            kept = [x for x in cur if fn(x)]
            if len(kept) < len(train) * min_keep or len(kept) < 30:
                continue
            # must improve pts/trade in at least min_months of the train months
            good = 0
            for m in months:
                a = [x for x in cur if x["month"] == m]
                b = [x for x in kept if x["month"] == m]
                if len(a) >= 8 and len(b) >= 4 and ppt(b) > ppt(a):
                    good += 1
            if good < min_months:
                continue
            gain = tot(kept) - tot(cur)
            if gain > 0 and (best is None or gain > best[0]):
                best = (gain, name, fn, kept)
        if best is None:
            break
        chosen.append((best[1], best[2])); cur = best[3]
    return chosen


def lomo_consensus(min_months, min_keep, max_rules):
    oos = []
    for m in MONTHS:
        tr_all = [r for r in BASE if r["month"] != m]
        te_all = [r for r in BASE if r["month"] == m]
        for sn in REAL_SETUPS:
            for isl in (True, False):
                tr = [r for r in tr_all if r["setup_name"] == sn and r["is_long"] == isl]
                te = [r for r in te_all if r["setup_name"] == sn and r["is_long"] == isl]
                if len(tr) < 60:
                    oos.extend(te); continue
                oos.extend(apply_rules(te, select_consensus(tr, min_months, min_keep, max_rules)))
    return oos


print("### A. a FAIRER selector — a condition must help in most TRAIN months before it is used")
print(raw_stats(BASE, "no filter"))
print(raw_stats([r for r in BASE if v16pass(r, gaps)[0]], "V16 (today)"))
for mm, mk, mr in ((5, 0.55, 2), (4, 0.5, 3), (4, 0.6, 2), (3, 0.5, 3)):
    print(raw_stats(lomo_consensus(mm, mk, mr),
                    f"consensus OOS: helps {mm}/5 months, keep>={mk:.0%}, <={mr} rules"))

print("\n\n### B. THE CEILING — how much is physically available?")
print("    raw chain points over 6 months (2026-03-01 -> 08-06), all 6 real setups\n")
allpts = tot(BASE)
wins = [r for r in BASE if r["pts"] > 0]
print(f"  every signal, no filter, no cap        {len(BASE):>5}t   {allpts:>+9.1f} pts  = ${allpts*5:>8,.0f} @1 MES")
print(f"  PERFECT foresight (only the winners)   {len(wins):>5}t   {tot(wins):>+9.1f} pts  = ${tot(wins)*5:>8,.0f}  <- impossible, upper bound")
print(f"  V16 today                              "
      f"{len([r for r in BASE if v16pass(r,gaps)[0]]):>5}t   {tot([r for r in BASE if v16pass(r,gaps)[0]]):>+9.1f} pts"
      f"  = ${tot([r for r in BASE if v16pass(r,gaps)[0]])*5:>8,.0f}")
print(f"\n  V16 captures {tot([r for r in BASE if v16pass(r,gaps)[0]])/allpts*100:.0f}% of ALL available points "
      f"using only {len([r for r in BASE if v16pass(r,gaps)[0]])/len(BASE)*100:.0f}% of the signals.")
print("  => the filter is not the bottleneck. There is only so much money in the signal set.")

print("\n\n### C. what the CAP costs — the portfolio can't take everything anyway")
POOL = [r for r in BASE if r["date"].isoformat() >= "2026-03-16"]
for cap in (1, 2, 3, 4, 6, 99):
    s = sim([r for r in POOL if v16pass(r, gaps)[0]], cap, cap, "flat1")
    n = sim(POOL, cap, cap, "flat1")
    print(f"  cap {cap if cap<99 else 'inf':<3}  V16 {s['total']:>8,.0f}$ ({s['trades']:>4}t)   "
          f"no filter {n['total']:>8,.0f}$ ({n['trades']:>4}t)")

print("\n\n### D. THE PATH TO $5,000/MONTH — what each lever is actually worth")
print("    (100 sessions = 4.76 months; all figures after the 0.81 broker haircut)\n")
from _tmp_s233_rules import RULES
ALL = frozenset(RULES.keys())
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
RELAX = {"Skew Charm", "ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"}


def build_relaxed():
    out = []
    for r in POOL:
        sn = r["setup_name"]
        if sn not in RELAX or (r["vix"] or 0) >= 22:
            if v16pass(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL - KEEP_DD if (sn == "DD Exhaustion" and not r["is_long"]) else ALL
        if v16pass(r, gaps, use)[0]:
            out.append(r)
    return out


REL = build_relaxed()
V16P = [r for r in POOL if v16pass(r, gaps)[0]]
print(f"  {'configuration':<46}{'$/mo':>9}{'MaxDD':>10}{'DD % of equity':>16}")
scen = [
    ("V16, 1 MES base + basket 2x  (LIVE TODAY)", V16P, 2, "basket", 5161),
    ("S233 relaxed, 1 MES base + basket 2x", REL, 2, "basket", 5161),
    ("S233 relaxed, cap 3/3", REL, 3, "basket", 5161),
    ("S233 relaxed, 2 MES base + basket 2x (=4x peak)", REL, 2, "flat2", 5161),
    ("  same, but judged against a $12k account", REL, 2, "flat2", 12000),
    ("S233 relaxed, cap 3/3, 2 MES base", REL, 3, "flat2", 12000),
]
for lab, c, cap, sz, eq in scen:
    s = sim(c, cap, cap, sz, haircut=0.81)
    mo = s["sessions"] / 21
    print(f"  {lab:<46}{s['total']/mo:>9,.0f}{s['maxdd']:>10,.0f}{abs(s['maxdd'])/eq*100:>15.0f}%")
