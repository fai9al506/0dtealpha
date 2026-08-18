# -*- coding: utf-8 -*-
"""S233 part 15 — STAGED ROLLOUT, ordered by how much broker evidence each stage has.

Stage 1 relaxes only Skew Charm (24 real broker trades of history, best $/trade).
Stage 2 adds ES Absorption, Stage 3 adds DD Exhaustion. VPB relaxation is never added
(only consistently-negative bucket). Every stage keeps full V16 on VIX >= 22 signals
and keeps the existing V13 quality stack on DD shorts.
"""
import collections, statistics
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
ALL = frozenset(RULES.keys())
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
V16_IDS = {r["id"] for r in POOL if passes(r, gaps)[0]}


def build(relax_setups, vg=22, dd_v13=True):
    """Relax the filter ONLY for the named setups; every other setup keeps full V16."""
    out = []
    for r in POOL:
        il = r["direction"] in ("long", "bullish")
        sn = r["setup_name"]
        if sn not in relax_setups or (vg is not None and (r["vix"] or 0) >= vg):
            if passes(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL
        if dd_v13 and sn == "DD Exhaustion" and not il:
            use = ALL - KEEP_DD
        if passes(r, gaps, use)[0]:
            out.append(r)
    return out


SC = {"Skew Charm"}
STAGES = [
    ("S0  V16 live (today)", set()),
    ("S1  + Skew Charm relaxed", SC),
    ("S2  + ES Absorption", SC | {"ES Absorption"}),
    ("S3  + AG Short", SC | {"ES Absorption", "AG Short"}),
    ("S4  + DD Exhaustion", SC | {"ES Absorption", "AG Short", "DD Exhaustion"}),
    ("S5  + VIX Divergence", SC | {"ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"}),
    ("S6  + VPB (NOT advised)", SC | {"ES Absorption", "AG Short", "DD Exhaustion",
                                      "VIX Divergence", "Vanna Pivot Bounce"}),
]

for cap in (2, 3):
    print(f"\n### staged rollout, cap {cap}/{cap}, basket 2x sizing, CHAIN outcomes")
    print(HDR)
    prev = None
    for lab, st in STAGES:
        s = sim(build(st), cap_l=cap, cap_s=cap, sizing="basket")
        print(fmt(s, lab))
        prev = s
    print(f"\n  {'stage':<28}{'x0.81 $/mo':>12}{'DD':>9}{'DD%eq':>7}{'t/day':>7}{'green':>9}"
          f"{'exTop3/mo':>11}{'breakeven pt':>13}")
    v16 = sim(build(set()), cap_l=cap, cap_s=cap, sizing="basket", haircut=0.81)["total"]
    for lab, st in STAGES:
        c = build(st)
        s = sim(c, cap_l=cap, cap_s=cap, sizing="basket", haircut=0.81)
        mo = s["sessions"] / 21
        # break-even extra slippage on the NEW trades only
        lo, hi = 0.0, 25.0
        if s["total"] > v16:
            for _ in range(24):
                mid = (lo + hi) / 2
                pen = []
                for r in c:
                    q = dict(r)
                    if r["id"] not in V16_IDS:
                        q["outcome_pnl"] = float(r["outcome_pnl"]) - mid
                    pen.append(q)
                t = sim(pen, cap_l=cap, cap_s=cap, sizing="basket", haircut=0.81)["total"]
                if t > v16:
                    lo = mid
                else:
                    hi = mid
        print(f"  {lab:<28}{s['total']/mo:>12,.0f}{s['maxdd']:>9,.0f}{abs(s['maxdd'])/5161*100:>6.0f}%"
              f"{s['tpd']:>7.1f}{s['green']:>5}/{s['sessions']:<3}{s['ex_top3']/mo:>11,.0f}{lo:>13.2f}")

print("\n\n### monthly by stage (chain, cap 2/2) -- does every stage beat V16 in every month?")
ms = sorted(sim(build(set()), cap_l=2, cap_s=2, sizing="basket")["month"])
print(f"  {'stage':<28}" + "".join(f"{m[-2:]:>9}" for m in ms) + f"{'TOTAL':>10}")
for lab, st in STAGES:
    s = sim(build(st), cap_l=2, cap_s=2, sizing="basket")
    print(f"  {lab:<28}" + "".join(f"{s['month'].get(m,0):>9,.0f}" for m in ms) + f"{s['total']:>10,.0f}")

print("\n\n### the S1 (Skew Charm only) book in detail -- this is the one to ship first")
c = build(SC)
s = sim(c, cap_l=2, cap_s=2, sizing="basket")
v = sim(build(set()), cap_l=2, cap_s=2, sizing="basket")
new = [t for t in s["trade_rows"] if t["id"] not in V16_IDS]
print(f"  V16 ${v['total']:,.0f} / {v['trades']}t / WR {v['wr']:.0f}% / DD ${v['maxdd']:,.0f}")
print(f"  S1  ${s['total']:,.0f} / {s['trades']}t / WR {s['wr']:.0f}% / DD ${s['maxdd']:,.0f}"
      f"   ({s['total']-v['total']:+,.0f}, {s['trades']-v['trades']:+} trades)")
print(f"  the {len(new)} added SC trades: WR {sum(1 for t in new if t['pts']>0)/len(new)*100:.0f}%, "
      f"${sum(t['pnl'] for t in new):+,.0f}, {sum(t['pnl'] for t in new)/len(new):.1f}$/trade, "
      f"{statistics.mean(t['pts'] for t in new):+.2f} pts/trade")
byrule = collections.Counter()
for r in POOL:
    if r["id"] in {t["id"] for t in new}:
        byrule[passes(r, gaps)[1]] += 1
print("  which V16 rule had been blocking them:", dict(byrule.most_common()))
mo = collections.defaultdict(float)
for t in new:
    mo[t["date"].strftime("%Y-%m")] += t["pnl"]
print("  added-trade $ by month:", {k: round(v2) for k, v2 in sorted(mo.items())})
