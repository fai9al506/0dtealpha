# -*- coding: utf-8 -*-
"""V20b — portfolio impact of adding the three excluded buckets to the real book.

The raw per-trade edge is not the answer: these signals fire alongside existing ones, so what
matters is what they do AFTER the concurrency cap, dedup, breaker and underwater guard.
"""
import collections, statistics
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
ALL = frozenset(RULES.keys())
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
RELAX = {"Skew Charm", "ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"}
W = "2026-03-16", "2026-08-07"
POOL = [r for r in rows if r["outcome_pnl"] is not None
        and W[0] <= r["ts"].astimezone(ET).date().isoformat() < W[1]]
REAL = [r for r in POOL if r["setup_name"] in WHITELIST]


def v16(pool):
    return [r for r in pool if passes(r, gaps)[0]]


def relaxed(pool):
    out = []
    for r in pool:
        il = r["direction"] in ("long", "bullish")
        sn = r["setup_name"]
        if sn not in RELAX or (r["vix"] or 0) >= 22:
            if passes(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL - KEEP_DD if (sn == "DD Exhaustion" and not il) else ALL
        if passes(r, gaps, use)[0]:
            out.append(r)
    return out


EXTRA = {
    "SB Absorption (all)": [r for r in POOL if r["setup_name"] == "SB Absorption"],
    "SB Absorption LONG only": [r for r in POOL if r["setup_name"] == "SB Absorption"
                                and r["direction"] in ("long", "bullish")],
    "VIX Div SHORT": [r for r in POOL if r["setup_name"] == "VIX Divergence"
                      and r["direction"] not in ("long", "bullish")],
    "VPB SHORT": [r for r in POOL if r["setup_name"] == "Vanna Pivot Bounce"
                  and r["direction"] not in ("long", "bullish")],
}

for basename, basefn in (("V16 (live today)", v16), ("S233-relaxed", relaxed)):
    base = basefn(REAL)
    print(f"\n### adding each bucket to the {basename} book (cap 2/2, basket sizing, 100 sessions)")
    print(HDR)
    b = sim(base, 2, 2, "basket")
    print(fmt(b, "baseline"))
    for lab, extra in EXTRA.items():
        cand = sorted(base + extra, key=lambda r: r["ts"])
        s = sim(cand, 2, 2, "basket")
        print(fmt(s, f"+ {lab}") + f"   [{s['total']-b['total']:+,.0f}]")
    # everything at once
    allx = sorted(base + EXTRA["SB Absorption (all)"] + EXTRA["VIX Div SHORT"] + EXTRA["VPB SHORT"],
                  key=lambda r: r["ts"])
    s = sim(allx, 2, 2, "basket")
    print(fmt(s, "+ all three") + f"   [{s['total']-b['total']:+,.0f}]")

print("\n\n### how many of the added signals actually get TAKEN once the cap is applied?")
base = relaxed(REAL)
b = sim(base, 2, 2, "basket")
for lab, extra in EXTRA.items():
    cand = sorted(base + extra, key=lambda r: r["ts"])
    s = sim(cand, 2, 2, "basket")
    ex_ids = {r["id"] for r in extra}
    took = [t for t in s["trade_rows"] if t["id"] in ex_ids]
    base_ids = {t["id"] for t in b["trade_rows"]}
    lost = [t for t in b["trade_rows"] if t["id"] not in {x["id"] for x in s["trade_rows"]}]
    took_pnl = sum(t["pnl"] for t in took)
    lost_pnl = sum(t["pnl"] for t in lost)
    print(f"  {lab:<24}offered {len(extra):>4}  taken {len(took):>4}  worth ${took_pnl:>+7,.0f}   "
          f"displaced {len(lost):>3} existing trades worth ${lost_pnl:>+7,.0f}   "
          f"net ${s['total']-b['total']:>+7,.0f}")

print("\n\n### month by month for the one bucket that cleared the confidence bar")
base = relaxed(REAL)
b = sim(base, 2, 2, "basket")
cand = sorted(base + EXTRA["SB Absorption (all)"], key=lambda r: r["ts"])
s = sim(cand, 2, 2, "basket")
ms = sorted(b["month"])
print(f"  {'':<22}" + "".join(f"{m[-2:]:>9}" for m in ms) + f"{'TOTAL':>10}{'MaxDD':>9}")
print(f"  {'relaxed baseline':<22}" + "".join(f"{b['month'].get(m,0):>9,.0f}" for m in ms)
      + f"{b['total']:>10,.0f}{b['maxdd']:>9,.0f}")
print(f"  {'+ SB Absorption':<22}" + "".join(f"{s['month'].get(m,0):>9,.0f}" for m in ms)
      + f"{s['total']:>10,.0f}{s['maxdd']:>9,.0f}")
print(f"  {'difference':<22}" + "".join(
    f"{s['month'].get(m,0)-b['month'].get(m,0):>+9,.0f}" for m in ms)
    + f"{s['total']-b['total']:>+10,.0f}")
