# -*- coding: utf-8 -*-
"""S233 part 17 — why do chain and MES-walk disagree by 23% on the RE-ADMITTED trades
but agree exactly on the V16 book? Either (a) the new trades really are more
execution-sensitive, or (b) the known mes_sim entry-fallback bug hits them harder.
"""
import collections, statistics
from _tmp_s233_sim import load, sim, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
ALL = frozenset(RULES.keys())
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
RELAX = {"Skew Charm", "ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"}
V16_IDS = {r["id"] for r in POOL if passes(r, gaps)[0]}


def build(pool, vg=22):
    out = []
    for r in pool:
        il = r["direction"] in ("long", "bullish")
        sn = r["setup_name"]
        if sn not in RELAX or (vg is not None and (r["vix"] or 0) >= vg):
            if passes(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL - KEEP_DD if (sn == "DD Exhaustion" and not il) else ALL
        if passes(r, gaps, use)[0]:
            out.append(r)
    return out


have = [r for r in POOL if r.get("mes_sim_outcome_pnl") is not None]
print(f"### per-trade chain vs MES-walk on rows where BOTH exist (n={len(have)})")
print("    the known bug injects a ~29pt phantom basis when no ES bar starts within 10 min,")
print("    so |chain - mes| > 20 pt is used as a bug proxy.\n")


def stats(rs, lab):
    d = [float(r["outcome_pnl"]) - float(r["mes_sim_outcome_pnl"]) for r in rs]
    big = [x for x in d if abs(x) > 20]
    clean = [x for x in d if abs(x) <= 20]
    print(f"  {lab:<26}n={len(rs):>5}  mean(chain-mes) {statistics.mean(d):>+6.2f} pt   "
          f"median {statistics.median(d):>+6.2f}   |>20pt| rows {len(big):>3} ({len(big)/len(rs)*100:>4.1f}%)   "
          f"mean excl. those {statistics.mean(clean) if clean else 0:>+6.2f}")


v16h = [r for r in have if r["id"] in V16_IDS]
newh = [r for r in have if r["id"] not in V16_IDS]
stats(v16h, "V16-passing trades")
stats(newh, "V16-BLOCKED trades")
for sn in sorted({r["setup_name"] for r in newh}):
    rs = [r for r in newh if r["setup_name"] == sn]
    if len(rs) >= 20:
        stats(rs, f"  blocked: {sn}")

print("\n### portfolio totals with the bug rows EXCLUDED from both models")
clean = [r for r in have if abs(float(r["outcome_pnl"]) - float(r["mes_sim_outcome_pnl"])) <= 20]
print(f"  clean rows: {len(clean)} of {len(have)} ({len(clean)/len(have)*100:.1f}%)")
for nm, cand in (("V16", [r for r in clean if r["id"] in V16_IDS]),
                 ("V17-staged", build(clean))):
    ch = sim(cand, cap_l=2, cap_s=2, sizing="basket")
    ms = sim([dict(r, outcome_pnl=r["mes_sim_outcome_pnl"]) for r in cand],
             cap_l=2, cap_s=2, sizing="basket")
    print(f"  {nm:<12} chain ${ch['total']:>8,.0f}   mes ${ms['total']:>8,.0f}   "
          f"ratio {ms['total']/max(ch['total'],1):.2f}   n={ch['trades']}")

print("\n### coverage check: are the BLOCKED signals under-covered by mes_sim?")
allv16 = [r for r in POOL if r["id"] in V16_IDS]
allnew = [r for r in POOL if r["id"] not in V16_IDS]
print(f"  V16-passing signals with mes_sim: {len(v16h)}/{len(allv16)} ({len(v16h)/len(allv16)*100:.0f}%)")
print(f"  V16-blocked signals with mes_sim: {len(newh)}/{len(allnew)} ({len(newh)/len(allnew)*100:.0f}%)")
print("  (mes_sim is only backfilled for the V14 whitelist, so a coverage gap is expected and")
print("   makes the MES cross-check a partial view, not a verdict)")
