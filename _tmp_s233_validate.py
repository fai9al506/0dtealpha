# -*- coding: utf-8 -*-
"""S233 part 16 — final validation of the staged recommendation.
1. era check (post 2026-06-22 DD trail change / post-S217)
2. sensitivity of the VIX>=22 overlay
3. cross-check with the MES-walk model (wrong model post-S217, but agreement adds confidence)
4. day-of-week and time-of-day sanity of the added trades
"""
import collections, statistics
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
ALL = frozenset(RULES.keys())
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
RELAX = {"Skew Charm", "ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"}


def build(pool, relax=RELAX, vg=22):
    out = []
    for r in pool:
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


print("### 1. ERA CHECK -- does the recommendation hold in the CURRENT code era?")
print("    (DD trail changed 2026-06-22; chain sim broker-validated from 2026-06-13)")
for lo, hi, lab in (("2026-03-16", "2026-06-13", "pre-S217   (63 sess)"),
                    ("2026-06-23", "2026-08-07", "post-trail (32 sess)")):
    p = [r for r in POOL if lo <= r["ts"].astimezone(ET).date().isoformat() < hi]
    print(f"\n  -- {lab} --")
    print(HDR)
    for cap in (2, 3):
        print(fmt(sim([r for r in p if passes(r, gaps)[0]], cap_l=cap, cap_s=cap, sizing="basket"),
                  f"V16 cap{cap}"))
        print(fmt(sim(build(p), cap_l=cap, cap_s=cap, sizing="basket"), f"V17-staged cap{cap}"))

print("\n\n### 2. VIX overlay sensitivity (cap 2/2 and 3/3, full window)")
print(f"  {'overlay':<22}{'cap2 $':>10}{'DD':>9}{'Mar':>8}  |{'cap3 $':>10}{'DD':>9}{'Mar':>8}")
for vg in (None, 19, 20, 21, 22, 23, 24, 26):
    a = sim(build(POOL, vg=vg), cap_l=2, cap_s=2, sizing="basket")
    b = sim(build(POOL, vg=vg), cap_l=3, cap_s=3, sizing="basket")
    lab = "none (never re-arm)" if vg is None else f"full V16 at VIX>={vg}"
    print(f"  {lab:<22}{a['total']:>10,.0f}{a['maxdd']:>9,.0f}{a['month'].get('2026-03',0):>8,.0f}  |"
          f"{b['total']:>10,.0f}{b['maxdd']:>9,.0f}{b['month'].get('2026-03',0):>8,.0f}")

print("\n\n### 3. CROSS-CHECK with the MES-walk model (only where mes_sim exists)")
have = [r for r in POOL if r.get("mes_sim_outcome_pnl") is not None]
print(f"  rows with mes_sim populated: {len(have)} of {len(POOL)}")
v16h = [r for r in have if passes(r, gaps)[0]]
rech = build(have)
for nm, cand in (("V16", v16h), ("V17-staged", rech)):
    ch = sim(cand, cap_l=2, cap_s=2, sizing="basket")
    mm = [dict(r, outcome_pnl=r["mes_sim_outcome_pnl"]) for r in cand]
    ms = sim(mm, cap_l=2, cap_s=2, sizing="basket")
    print(f"  {nm:<12} chain ${ch['total']:>8,.0f}   mes-walk ${ms['total']:>8,.0f}   "
          f"ratio {ms['total']/max(ch['total'],1):.2f}   n={ch['trades']}")
print("  both models must AGREE ON THE SIGN of the improvement, else stop")

print("\n\n### 4. when do the added trades happen? (cap 2/2, full window)")
v16_ids = {r["id"] for r in POOL if passes(r, gaps)[0]}
s = sim(build(POOL), cap_l=2, cap_s=2, sizing="basket")
new = [t for t in s["trade_rows"] if t["id"] not in v16_ids]
hr = collections.defaultdict(lambda: [0, 0.0])
for t in new:
    hr[t["et"].hour][0] += 1; hr[t["et"].hour][1] += t["pnl"]
print(f"  {'hour ET':<9}{'n':>5}{'$':>9}{'$/t':>7}")
for h in sorted(hr):
    print(f"  {h:02d}:00{'':<4}{hr[h][0]:>5}{hr[h][1]:>9,.0f}{hr[h][1]/hr[h][0]:>7.1f}")
dw = collections.defaultdict(lambda: [0, 0.0])
for t in new:
    dw[t["date"].strftime("%a")][0] += 1; dw[t["date"].strftime("%a")][1] += t["pnl"]
print("  by weekday:", {k: (v[0], round(v[1])) for k, v in dw.items()})

print("\n\n### 5. concurrency / order-rate reality (cap 2/2 and 3/3)")
for cap in (2, 3):
    s = sim(build(POOL), cap_l=cap, cap_s=cap, sizing="basket")
    per = collections.Counter(t["date"] for t in s["trade_rows"])
    v = sorted(per.values())
    print(f"  cap {cap}: median {statistics.median(v):.0f} trades/day, p90 {v[int(len(v)*0.9)]}, "
          f"max {max(v)}, max concurrent positions {s['max_conc']} "
          f"(= up to {s['max_conc']*2} MES gross with 2x sizing)")
