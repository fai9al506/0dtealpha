# -*- coding: utf-8 -*-
"""S233 follow-up: (a) is the high-VIX case day-concentrated? (b) exactly what does
'relax Skew Charm' admit, month by month, new trades only (no displacement credit)?"""
import collections
from _tmp_s233_sim import load, sim, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
ALL = frozenset(RULES.keys())
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
V16 = [r for r in POOL if passes(r, gaps)[0]]
V16_IDS = {r["id"] for r in V16}

print("### A. HIGH-VIX days: is the filter's win concentrated in 1-2 days?")
hi_days = sorted({r["ts"].astimezone(ET).date() for r in POOL if (r["vix"] or 0) >= 22})
a = sim([r for r in V16 if r["ts"].astimezone(ET).date() in hi_days], 2, 2, "basket")
b = sim([r for r in POOL if passes(r, gaps, ALL)[0] and r["ts"].astimezone(ET).date() in hi_days], 2, 2, "basket")
print(f"  {len(hi_days)} sessions with a VIX>=22 signal.  V16 ${a['total']:,.0f}  vs no-filter ${b['total']:,.0f}"
      f"  -> filter worth ${a['total']-b['total']:+,.0f}")
diffs = sorted(((d, a["daily"].get(d, 0) - b["daily"].get(d, 0)) for d in hi_days), key=lambda x: -abs(x[1]))
print("  per-day (filter minus no-filter), biggest first:")
for d, v in diffs:
    print(f"    {d}  {v:>+8,.0f}   [V16 {a['daily'].get(d,0):>+7,.0f} / off {b['daily'].get(d,0):>+7,.0f}]")
wins = sum(1 for _, v in diffs if v > 0)
print(f"  filter better on {wins} of {len(diffs)} high-VIX days; "
      f"top-1 day = {abs(diffs[0][1])/max(abs(a['total']-b['total']),1)*100:.0f}% of the edge")

print("\n\n### B. 'relax Skew Charm' = which V16 rules stop applying to SC, and what they admit")
sc = [r for r in POOL if r["setup_name"] == "Skew Charm"]
blocked = collections.defaultdict(list)
for r in sc:
    ok, why = passes(r, gaps)
    if not ok and why:
        blocked[why].append(r)
print(f"  {'rule':<20}{'signals':>8}{'WR':>6}{'chain pts':>11}{'$@1MES':>9}   by month ($)")
for rule, rs in sorted(blocked.items(), key=lambda kv: -sum(float(x['outcome_pnl']) for x in kv[1])):
    pts = sum(float(x["outcome_pnl"]) for x in rs)
    wr = sum(1 for x in rs if float(x["outcome_pnl"]) > 0) / len(rs) * 100
    mo = collections.defaultdict(float)
    for x in rs:
        mo[x["ts"].astimezone(ET).strftime("%m")] += float(x["outcome_pnl"]) * 5
    print(f"  {rule:<20}{len(rs):>8}{wr:>5.0f}%{pts:>11,.1f}{pts*5:>9,.0f}   "
          + " ".join(f"{k}:{v:+,.0f}" for k, v in sorted(mo.items())))

print("\n\n### C. SC-only stage: NEW trades alone (no credit for displacing V16 losers)")
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})


def build(relax, vg=22):
    out = []
    for r in POOL:
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


for lab, st in (("SC only", {"Skew Charm"}),
                ("SC+ESAbs+AG", {"Skew Charm", "ES Absorption", "AG Short"}),
                ("full (no VPB)", {"Skew Charm", "ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"})):
    s = sim(build(st), 2, 2, "basket")
    v = sim(V16, 2, 2, "basket")
    new = [t for t in s["trade_rows"] if t["id"] not in V16_IDS]
    lost = [t for t in v["trade_rows"] if t["id"] not in {x["id"] for x in s["trade_rows"]}]
    mo_new = collections.defaultdict(float)
    for t in new:
        mo_new[t["date"].strftime("%m")] += t["pnl"]
    print(f"\n  {lab}: total {s['total']-v['total']:+,.0f}  = new trades {sum(t['pnl'] for t in new):+,.0f} "
          f"({len(new)}t, WR {sum(1 for t in new if t['pts']>0)/len(new)*100:.0f}%) "
          f"+ displaced V16 losers {-sum(t['pnl'] for t in lost):+,.0f} ({len(lost)}t)")
    print("    new-trade $ by month: " + " ".join(f"{k}:{v2:+,.0f}" for k, v2 in sorted(mo_new.items())))
    pos = sum(1 for v2 in mo_new.values() if v2 > 0)
    print(f"    new trades were profitable in {pos} of {len(mo_new)} months")
