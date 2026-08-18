# -*- coding: utf-8 -*-
"""S233 part 14 — stress test. The re-admitted trades have never been sent to a broker.
What if they execute WORSE than the validated ones?

Penalty is applied per trade, in points, only to trades V16 would have blocked.
Also isolates the DD bucket (worst measured sim bias, -2.59 pt/trade on n=6).
"""
import collections
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
ALL = frozenset(RULES.keys())
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})


def build(vg=22):
    out = []
    for r in POOL:
        il = r["direction"] in ("long", "bullish")
        if (r["vix"] or 0) >= vg:
            if passes(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL - KEEP_DD if (r["setup_name"] == "DD Exhaustion" and not il) else ALL
        if passes(r, gaps, use)[0]:
            out.append(r)
    return out


V16 = [r for r in POOL if passes(r, gaps)[0]]
V16_IDS = {r["id"] for r in V16}
REC = build()


def penalised(cands, pt_new=0.0, pt_all=0.0, dd_extra=0.0):
    """Return a copy of cands with outcome_pnl reduced. pt_new hits only V16-blocked trades."""
    out = []
    for r in cands:
        p = float(r["outcome_pnl"]) - pt_all
        if r["id"] not in V16_IDS:
            p -= pt_new
        if r["setup_name"] == "DD Exhaustion":
            p -= dd_extra
        q = dict(r); q["outcome_pnl"] = p
        out.append(q)
    return out


print("### A. penalty applied ONLY to the never-broker-tested (V16-blocked) trades")
print("    baseline for comparison: V16 live, cap 2/2, x0.81 haircut")
b2 = sim(penalised(V16), cap_l=2, cap_s=2, sizing="basket", haircut=0.81)
b3 = sim(penalised(V16), cap_l=3, cap_s=3, sizing="basket", haircut=0.81)
print(f"    V16 live: cap2/2 ${b2['total']:,.0f} (${b2['total']/(b2['sessions']/21):,.0f}/mo)   "
      f"cap3/3 ${b3['total']:,.0f} (${b3['total']/(b3['sessions']/21):,.0f}/mo)\n")
print(f"  {'penalty on new trades':<26}{'cap2 $':>10}{'$/mo':>8}{'DD':>9}  |{'cap3 $':>10}{'$/mo':>8}{'DD':>9}")
for pen in (0.0, 0.5, 1.0, 1.5, 2.0, 3.0, 4.0):
    a = sim(penalised(REC, pt_new=pen), cap_l=2, cap_s=2, sizing="basket", haircut=0.81)
    c = sim(penalised(REC, pt_new=pen), cap_l=3, cap_s=3, sizing="basket", haircut=0.81)
    print(f"  {'-'+str(pen)+' pt/trade':<26}{a['total']:>10,.0f}{a['total']/(a['sessions']/21):>8,.0f}"
          f"{a['maxdd']:>9,.0f}  |{c['total']:>10,.0f}{c['total']/(c['sessions']/21):>8,.0f}{c['maxdd']:>9,.0f}")

print("\n### B. DD-specific stress (measured sim bias for DD was -2.59 pt/trade, n=6)")
print(f"  {'extra DD penalty':<26}{'cap2 $':>10}{'$/mo':>8}  |{'cap3 $':>10}{'$/mo':>8}")
for dd in (0.0, 1.0, 2.0, 2.6, 4.0):
    a = sim(penalised(REC, dd_extra=dd), cap_l=2, cap_s=2, sizing="basket", haircut=0.81)
    c = sim(penalised(REC, dd_extra=dd), cap_l=3, cap_s=3, sizing="basket", haircut=0.81)
    print(f"  {'-'+str(dd)+' pt on every DD':<26}{a['total']:>10,.0f}{a['total']/(a['sessions']/21):>8,.0f}"
          f"  |{c['total']:>10,.0f}{c['total']/(c['sessions']/21):>8,.0f}")

print("\n### C. combined worst case: -1.5 pt on new trades AND -2.6 pt on every DD trade")
for cap in (2, 3):
    s = sim(penalised(REC, pt_new=1.5, dd_extra=2.6), cap_l=cap, cap_s=cap, sizing="basket", haircut=0.81)
    v = sim(penalised(V16), cap_l=cap, cap_s=cap, sizing="basket", haircut=0.81)
    print(f"  cap {cap}/{cap}: recommended ${s['total']:,.0f} (${s['total']/(s['sessions']/21):,.0f}/mo, "
          f"DD ${s['maxdd']:,.0f})  vs V16 ${v['total']:,.0f} (${v['total']/(v['sessions']/21):,.0f}/mo)"
          f"   -> still {'+' if s['total']>v['total'] else ''}{s['total']-v['total']:,.0f}")

print("\n### D. break-even: how bad would the new trades have to be to lose the whole gain?")
for cap in (2, 3):
    v = sim(penalised(V16), cap_l=cap, cap_s=cap, sizing="basket", haircut=0.81)["total"]
    lo, hi = 0.0, 20.0
    for _ in range(30):
        mid = (lo + hi) / 2
        t = sim(penalised(REC, pt_new=mid), cap_l=cap, cap_s=cap, sizing="basket", haircut=0.81)["total"]
        if t > v:
            lo = mid
        else:
            hi = mid
    print(f"  cap {cap}/{cap}: the re-admitted trades would each have to come in {lo:.1f} pt WORSE "
          f"than the chain sim before the change stops paying")
print("  (measured sim error on real fills is 0.18 pt/trade, MAE 1.73 pt)")

print("\n### E. composition of the recommended book (cap 2/2)")
s = sim(REC, cap_l=2, cap_s=2, sizing="basket")
g = collections.defaultdict(lambda: [0, 0, 0.0])
for t in s["trade_rows"]:
    k = (t["setup"], "LONG" if t["long"] else "SHORT", "V16" if t["id"] in V16_IDS else "NEW")
    g[k][0] += 1; g[k][1] += 1 if t["pts"] > 0 else 0; g[k][2] += t["pnl"]
print(f"  {'bucket':<34}{'n':>5}{'WR':>6}{'$':>10}{'$/t':>7}   broker history?")
BROKER = {("Skew Charm", "SHORT"): 15, ("Skew Charm", "LONG"): 9, ("DD Exhaustion", "LONG"): 6,
          ("ES Absorption", "LONG"): 3, ("AG Short", "SHORT"): 2, ("ES Absorption", "SHORT"): 2}
for k, v in sorted(g.items(), key=lambda kv: -kv[1][2]):
    bh = BROKER.get((k[0], k[1]), 0)
    print(f"  {k[0]+' '+k[1]+' ['+k[2]+']':<34}{v[0]:>5}{v[1]/v[0]*100:>5.0f}%{v[2]:>10,.0f}"
          f"{v[2]/v[0]:>7.1f}   {bh if bh else 'NONE'}")
