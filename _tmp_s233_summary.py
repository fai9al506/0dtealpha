# -*- coding: utf-8 -*-
"""S233 — final summary numbers for the report. Recommended config vs V16, every angle."""
import collections, statistics
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
ALL = frozenset(RULES.keys())
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
V16_IDS = {r["id"] for r in POOL if passes(r, gaps)[0]}
STAGE = {"S1 SC only": {"Skew Charm"},
         "S2 +ES Abs": {"Skew Charm", "ES Absorption"},
         "S3 +AG": {"Skew Charm", "ES Absorption", "AG Short"},
         "S5 full (no VPB)": {"Skew Charm", "ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"}}


def build(relax, vg=22, pool=None):
    out = []
    for r in (pool or POOL):
        il = r["direction"] in ("long", "bullish")
        sn = r["setup_name"]
        if sn not in relax or (vg is not None and (r["vix"] or 0) >= vg):
            if passes(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL - KEEP_DD if (sn == "DD Exhaustion" and not il) else ALL
        if passes(r, gaps, use)[0]:
            out.append(r)
    return out


print("=" * 104)
print("S233 FINAL — 100 sessions 2026-03-16 -> 2026-08-06, chain outcomes, basket 2x sizing")
print("=" * 104)
for cap in (2, 3):
    print(f"\ncap {cap}/{cap}")
    print(HDR)
    print(fmt(sim([r for r in POOL if passes(r, gaps)[0]], cap_l=cap, cap_s=cap, sizing="basket"),
              "V16 (live today)"))
    for lab, st in STAGE.items():
        print(fmt(sim(build(st), cap_l=cap, cap_s=cap, sizing="basket"), lab))

print("\n\nAFTER the measured 0.81 broker-capture haircut  ($/month at 1 MES base + 2x on basket confirm)")
print(f"  {'config':<22}{'cap2 $/mo':>11}{'cap2 DD':>10}{'cap2 DD%':>10}"
      f"{'cap3 $/mo':>11}{'cap3 DD':>10}{'cap3 DD%':>10}")
line = {}
for lab, st in [("V16 (live today)", None)] + list(STAGE.items()):
    vals = []
    for cap in (2, 3):
        c = [r for r in POOL if passes(r, gaps)[0]] if st is None else build(st)
        s = sim(c, cap_l=cap, cap_s=cap, sizing="basket", haircut=0.81)
        vals += [s["total"] / (s["sessions"] / 21), s["maxdd"], abs(s["maxdd"]) / 5161 * 100]
    print(f"  {lab:<22}{vals[0]:>11,.0f}{vals[1]:>10,.0f}{vals[2]:>9.0f}%"
          f"{vals[3]:>11,.0f}{vals[4]:>10,.0f}{vals[5]:>9.0f}%")

print("\n\nSANITY FLOOR — ex-top-3-days run rate (the honest number per PROJECTION.md rule 3)")
print(f"  {'config':<22}{'top3 share':>12}{'ex-top3 $/mo (x0.81)':>24}{'median day':>12}{'green days':>12}")
for lab, st in [("V16 (live today)", None)] + list(STAGE.items()):
    c = [r for r in POOL if passes(r, gaps)[0]] if st is None else build(st)
    s = sim(c, cap_l=2, cap_s=2, sizing="basket", haircut=0.81)
    print(f"  {lab:<22}{s['top3_share']:>11.0f}%{s['ex_top3']/(s['sessions']/21):>24,.0f}"
          f"{s['median_day']:>12,.0f}{s['green']:>8}/{s['sessions']:<3}")

print("\n\nDOES IT BEAT V16 IN EVERY MONTH? (cap 2/2, chain)")
v = sim([r for r in POOL if passes(r, gaps)[0]], cap_l=2, cap_s=2, sizing="basket")
ms = sorted(v["month"])
print(f"  {'config':<22}" + "".join(f"{m[-2:]:>9}" for m in ms) + f"{'months>=V16':>13}")
print(f"  {'V16 (live today)':<22}" + "".join(f"{v['month'][m]:>9,.0f}" for m in ms))
for lab, st in STAGE.items():
    s = sim(build(st), cap_l=2, cap_s=2, sizing="basket")
    beat = sum(1 for m in ms if s["month"].get(m, 0) >= v["month"][m] - 1)
    print(f"  {lab:<22}" + "".join(f"{s['month'].get(m,0):>9,.0f}" for m in ms) + f"{beat:>10}/6")

print("\n\nEXPOSURE — peak position count is UNCHANGED (the cap is untouched)")
for lab, st in [("V16 (live today)", None)] + list(STAGE.items()):
    c = [r for r in POOL if passes(r, gaps)[0]] if st is None else build(st)
    s = sim(c, cap_l=2, cap_s=2, sizing="basket")
    per = collections.Counter(t["date"] for t in s["trade_rows"])
    vv = sorted(per.values())
    print(f"  {lab:<22} max concurrent {s['max_conc']} positions (<= {s['max_conc']*2} MES gross)   "
          f"median {statistics.median(vv):.0f} trades/day, p90 {vv[int(len(vv)*0.9)]}, max {max(vv)}")

print("\n\nRECENT-ERA CONFIRMATION (2026-06-23 -> 08-06, 32 sessions, current trail config)")
p = [r for r in POOL if r["ts"].astimezone(ET).date().isoformat() >= "2026-06-23"]
print(HDR)
print(fmt(sim([r for r in p if passes(r, gaps)[0]], cap_l=2, cap_s=2, sizing="basket"), "V16 cap2"))
for lab, st in STAGE.items():
    print(fmt(sim(build(st, pool=p), cap_l=2, cap_s=2, sizing="basket"), lab + " cap2"))
