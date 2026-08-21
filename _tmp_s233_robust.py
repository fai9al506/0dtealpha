# -*- coding: utf-8 -*-
"""S233 part 7 — robustness matrix. Does the relaxation gain survive changes to
cap, sizing, daily breaker and the 0.81 broker-capture haircut?

Also isolates the DISPLACEMENT artifact: at a high cap almost nothing is displaced,
so a gain that survives cap 4/4 comes from the new trades themselves, not from
crowding out V16 losers.
"""
import sys, collections
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
ALL = frozenset(RULES.keys())
CORE = frozenset({"DD_SHORT", "V13VANNA", "ESABS_ALIGN"})
CORE2 = frozenset({"DD_SHORT", "V13VANNA"})
ROBUST = frozenset({"DD_SHORT", "V13VANNA", "ESABS_ALIGN", "VPB_GRADEB", "DDLONG_ALIGN_LO",
                    "ESABS_PARA", "V11_LATE", "DDLONG_GRADEC"})
WIDE = ROBUST | {"SC_GRADE", "V11_DEADZONE", "GAP_LONG", "ESABS_GRADE", "SCDD_SHORT_GEXLIS",
                 "AG_TARGET", "DDLONG_ALIGN_HI", "DDLONG_VIX22", "VPB_HOUR11"}
SETS = [("V16 live", frozenset()), ("V17-core2 (DD_S+VANNA)", CORE2),
        ("V17-core3 (+ESABS_ALIGN)", CORE), ("V17b robust8", ROBUST),
        ("V17c wide17", WIDE), ("V17d no filter", ALL)]


def R(off, **kw):
    return sim([r for r in POOL if passes(r, gaps, off)[0]], **kw)


print("### A. CAP sweep (sizing=basket) -- does the gain survive when displacement is removed?")
print(f"  {'filter':<26}" + "".join(f"{'cap'+str(c):>12}" for c in (1, 2, 3, 4)))
for lab, off in SETS:
    line = f"  {lab:<26}"
    for c in (1, 2, 3, 4):
        s = R(off, cap_l=c, cap_s=c, sizing="basket")
        line += f"{s['total']:>8,.0f}/{abs(s['maxdd'])/1000:>3.1f}k"
    print(line)
print("  (cell = total$ / MaxDD)")

print("\n### B. SIZING (cap 2/2) -- basket 2x only exists from Jun 11; flat1 removes that confound")
print(f"  {'filter':<26}{'flat 1 MES':>14}{'basket 2x':>14}{'flat 2 MES':>14}")
for lab, off in SETS:
    a = R(off, cap_l=2, cap_s=2, sizing="flat1")
    b = R(off, cap_l=2, cap_s=2, sizing="basket")
    c = R(off, cap_l=2, cap_s=2, sizing="flat2")
    print(f"  {lab:<26}{a['total']:>14,.0f}{b['total']:>14,.0f}{c['total']:>14,.0f}")

print("\n### C. DAILY LOSS BREAKER (cap 2/2, basket) -- a bigger book hits the $300 breaker more often")
print(f"  {'filter':<26}{'$200':>11}{'$300 live':>11}{'$500':>11}{'$1000':>11}{'none':>11}")
for lab, off in SETS:
    line = f"  {lab:<26}"
    for lim in (200, 300, 500, 1000, 10 ** 9):
        s = R(off, cap_l=2, cap_s=2, sizing="basket", limit=lim)
        line += f"{s['total']:>11,.0f}"
    print(line)

print("\n### D. BROKER-CAPTURE HAIRCUT 0.81 (cap 2/2, basket) + commission sensitivity")
print(f"  {'filter':<26}{'chain':>11}{'x0.81':>11}{'x0.81 $2c':>11}{'x0.70':>11}{'$/mo x0.81':>12}")
for lab, off in SETS:
    a = R(off, cap_l=2, cap_s=2, sizing="basket")
    b = R(off, cap_l=2, cap_s=2, sizing="basket", haircut=0.81)
    c = R(off, cap_l=2, cap_s=2, sizing="basket", haircut=0.81, comm=2.0)
    d = R(off, cap_l=2, cap_s=2, sizing="basket", haircut=0.70)
    print(f"  {lab:<26}{a['total']:>11,.0f}{b['total']:>11,.0f}{c['total']:>11,.0f}{d['total']:>11,.0f}"
          f"{b['total']/(b['sessions']/21):>12,.0f}")

print("\n### E. DISPLACEMENT DECOMPOSITION at cap 2/2 vs cap 4/4 (basket)")
base2 = R(frozenset(), cap_l=2, cap_s=2, sizing="basket")
base4 = R(frozenset(), cap_l=4, cap_s=4, sizing="basket")
print(f"  {'filter':<26}{'d vs V16 @2/2':>16}{'d vs V16 @4/4':>16}")
for lab, off in SETS[1:]:
    a = R(off, cap_l=2, cap_s=2, sizing="basket")
    b = R(off, cap_l=4, cap_s=4, sizing="basket")
    print(f"  {lab:<26}{a['total']-base2['total']:>+16,.0f}{b['total']-base4['total']:>+16,.0f}")
print("  a gain that holds at 4/4 is NOT a cap-displacement artifact")
