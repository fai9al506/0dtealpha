# -*- coding: utf-8 -*-
"""S233 part 8 — final candidate set, full distribution, and execution feasibility.

Adds V17e = the COMPLEMENT approach: drop every rule except the ones that measurably
EARN money (negative LOO on 100 sessions). Scores everything with the 0.81 broker haircut.
"""
import sys, collections, statistics
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
ALL = frozenset(RULES.keys())

CORE = frozenset({"DD_SHORT", "V13VANNA", "ESABS_ALIGN"})
ROBUST = CORE | {"VPB_GRADEB", "DDLONG_ALIGN_LO", "ESABS_PARA", "V11_LATE", "DDLONG_GRADEC"}
WIDE = ROBUST | {"SC_GRADE", "V11_DEADZONE", "GAP_LONG", "ESABS_GRADE", "SCDD_SHORT_GEXLIS",
                 "AG_TARGET", "DDLONG_ALIGN_HI", "DDLONG_VIX22", "VPB_HOUR11"}
# rules that measurably EARN money (LOO < 0 over 100 sessions) -- keep these, drop the rest
KEEPERS = {"ESABS_LATE", "SIDIAL_PM", "SC_LONG_OPEX", "GEXTARGET_PM", "AG_OPEX", "V13BULL",
           "VIXDIV_GEXPARA", "SC_LONG_A3PARA", "DDLONG_PARA", "ESABS_SHORT"}
KEEPONLY = ALL - KEEPERS

CANDS = [("V16 live", frozenset(), None),
         ("V17-core3", CORE, None),
         ("V17b robust8", ROBUST, None),
         ("V17c wide17", WIDE, None),
         ("V17e keep-good-only", KEEPONLY, None),
         ("V17d no filter", ALL, None),
         ("V17c + VIX>=22 V16", WIDE, 22),
         ("V17e + VIX>=22 V16", KEEPONLY, 22),
         ("V17d + VIX>=22 V16", ALL, 22)]


def R(off, vg=None, cap=2, **kw):
    c = []
    for r in POOL:
        use = frozenset() if (vg is not None and (r["vix"] or 0) >= vg) else off
        if passes(r, gaps, use)[0]:
            c.append(r)
    return sim(c, cap_l=cap, cap_s=cap, sizing="basket", **kw)


for cap in (2, 3):
    print(f"\n### CAP {cap}/{cap} -- chain outcomes")
    print(HDR)
    for lab, off, vg in CANDS:
        print(fmt(R(off, vg, cap), lab))
    print(f"\n  {'candidate':<22}{'x0.81 $':>10}{'$/mo':>8}{'DD':>9}{'DD%eq':>7}{'top3%':>7}"
          f"{'exTop3/mo':>11}{'medDay':>8}{'green':>8}")
    for lab, off, vg in CANDS:
        s = R(off, vg, cap, haircut=0.81)
        mo = s["sessions"] / 21
        print(f"  {lab:<22}{s['total']:>10,.0f}{s['total']/mo:>8,.0f}{s['maxdd']:>9,.0f}"
              f"{abs(s['maxdd'])/5161*100:>6.0f}%{s['top3_share']:>6.0f}%{s['ex_top3']/mo:>11,.0f}"
              f"{s['median_day']:>8,.0f}{s['green']:>5}/{s['sessions']:<3}")

print("\n\n### EXECUTION FEASIBILITY (cap 2/2 and 3/3, basket sizing)")
print(f"  {'candidate':<22}{'cap':>4}{'t/day':>7}{'max t/day':>10}{'maxConc':>9}{'maxMES':>8}"
      f"{'days>15t':>9}{'p95 t/day':>10}")
for lab, off, vg in CANDS:
    for cap in (2, 3):
        s = R(off, vg, cap)
        perday = collections.Counter(t["date"] for t in s["trade_rows"])
        vals = sorted(perday.values())
        p95 = vals[int(len(vals) * 0.95)] if vals else 0
        print(f"  {lab:<22}{cap:>4}{s['tpd']:>7.1f}{max(vals) if vals else 0:>10}"
              f"{s['max_conc']:>9}{s['max_conc']*2:>8}{sum(1 for v in vals if v > 15):>9}{p95:>10}")

print("\n\n### MONTHLY, leading candidates (chain, cap 2/2 and 3/3)")
for cap in (2, 3):
    print(f"\n  cap {cap}/{cap}")
    ms = sorted(R(frozenset(), None, cap)["month"])
    print(f"  {'candidate':<22}" + "".join(f"{m[-2:]:>9}" for m in ms) + f"{'TOTAL':>10}")
    for lab, off, vg in CANDS:
        s = R(off, vg, cap)
        print(f"  {lab:<22}" + "".join(f"{s['month'].get(m,0):>9,.0f}" for m in ms)
              + f"{s['total']:>10,.0f}")

print("\n\n### WORST-CASE: 5 worst days of each candidate (chain, cap 2/2)")
for lab, off, vg in CANDS:
    s = R(off, vg, 2)
    worst = sorted(s["daily"].items(), key=lambda kv: kv[1])[:5]
    print(f"  {lab:<22}" + " ".join(f"{d.strftime('%m-%d')}:{v:>+7,.0f}" for d, v in worst))
