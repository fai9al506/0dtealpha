# -*- coding: utf-8 -*-
"""S233 part 11 — structure, not rule-picking.

Takes the fully-relaxed book and splits the trades V16 would have BLOCKED into
setup x direction buckets, month by month. A bucket that loses in most months is
one we should keep blocking; the rest should be re-admitted. This is a decision per
BUCKET (8-10 of them) rather than per RULE (34), so it is far harder to overfit.
"""
import collections
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
ALL = frozenset(RULES.keys())
CAP = 3


def R(pool, off, cap=CAP, vg=None, **kw):
    c = []
    for r in pool:
        use = frozenset() if (vg is not None and (r["vix"] or 0) >= vg) else off
        if passes(r, gaps, use)[0]:
            c.append(r)
    return sim(c, cap_l=cap, cap_s=cap, sizing="basket", **kw)


v16 = R(POOL, frozenset())
nof = R(POOL, ALL)
v16_ids = {t["id"] for t in v16["trade_rows"]}
months = sorted(v16["month"])

print(f"### A. the V16-BLOCKED trades the relaxed book takes, by setup x direction (cap {CAP}/{CAP})")
buck = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0, 0.0]))
for t in nof["trade_rows"]:
    if t["id"] in v16_ids:
        continue
    k = (t["setup"], "SHORT" if not t["long"] else "LONG")
    m = t["date"].strftime("%Y-%m")
    buck[k][m][0] += 1; buck[k][m][1] += 1 if t["pts"] > 0 else 0; buck[k][m][2] += t["pnl"]

print(f"  {'bucket':<27}{'n':>5}{'WR':>5}" + "".join(f"{m[-2:]:>8}" for m in months) + f"{'TOTAL':>9}{'mo+':>5}")
order = []
for k, mm in buck.items():
    tot = sum(v[2] for v in mm.values()); n = sum(v[0] for v in mm.values())
    w = sum(v[1] for v in mm.values())
    pos = sum(1 for m in months if mm.get(m, [0, 0, 0])[2] > 0)
    order.append((tot, k, n, w, mm, pos))
for tot, k, n, w, mm, pos in sorted(order):
    print(f"  {k[0]+' '+k[1]:<27}{n:>5}{w/max(n,1)*100:>4.0f}%"
          + "".join(f"{mm.get(m,[0,0,0])[2]:>+8,.0f}" for m in months) + f"{tot:>+9,.0f}{pos:>5}")

# ── build the structural filter: block only the consistently-losing buckets ──
BAD = {("DD Exhaustion", "SHORT")}
BAD2 = BAD | {("Skew Charm", "SHORT")}


def structural(off_bad, dd_v13=False):
    """Relaxed book, minus the named bad buckets. dd_v13=True re-admits DD shorts but only
    those that pass the existing V13 quality stack (the rules already in the code)."""
    keep_rules = frozenset()
    if dd_v13:
        keep_rules = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
    c = []
    for r in POOL:
        il = r["direction"] in ("long", "bullish")
        k = (r["setup_name"], "LONG" if il else "SHORT")
        if k in off_bad:
            continue
        use = ALL - keep_rules if (dd_v13 and r["setup_name"] == "DD Exhaustion" and not il) else ALL
        if passes(r, gaps, use)[0]:
            c.append(r)
    return c


print("\n\n### B. structural variants (cap 2/2 and 3/3, basket sizing)")
VARIANTS = [
    ("V16 live", lambda: [r for r in POOL if passes(r, gaps)[0]]),
    ("relaxed (no filter)", lambda: [r for r in POOL if passes(r, gaps, ALL)[0]]),
    ("relaxed - DD shorts", lambda: structural(BAD)),
    ("relaxed - DD&SC shorts", lambda: structural(BAD2)),
    ("relaxed, DD shorts via V13", lambda: structural(set(), dd_v13=True)),
]
for cap in (2, 3):
    print(f"\n  cap {cap}/{cap}")
    print(HDR)
    for lab, fn in VARIANTS:
        print(fmt(sim(fn(), cap_l=cap, cap_s=cap, sizing="basket"), lab))
    # with the VIX>=22 full-V16 overlay
    for lab, fn in VARIANTS[2:]:
        c = []
        base_ids = {r["id"] for r in fn()}
        for r in POOL:
            hi = (r["vix"] or 0) >= 22
            if hi:
                if passes(r, gaps)[0]:
                    c.append(r)
            elif r["id"] in base_ids:
                c.append(r)
        print(fmt(sim(c, cap_l=cap, cap_s=cap, sizing="basket"), lab + " + VIX>=22"))

print("\n\n### C. monthly for the structural leaders")
def monthly(c, cap):
    return sim(c, cap_l=cap, cap_s=cap, sizing="basket")
for cap in (2, 3):
    print(f"\n  cap {cap}/{cap}   " + "".join(f"{m[-2:]:>9}" for m in months) + f"{'TOTAL':>10}{'MaxDD':>9}")
    for lab, fn in VARIANTS:
        s = monthly(fn(), cap)
        print(f"  {lab:<22}" + "".join(f"{s['month'].get(m,0):>9,.0f}" for m in months)
              + f"{s['total']:>10,.0f}{s['maxdd']:>9,.0f}")
    for lab, fn in VARIANTS[2:]:
        base_ids = {r["id"] for r in fn()}
        c = []
        for r in POOL:
            if (r["vix"] or 0) >= 22:
                if passes(r, gaps)[0]:
                    c.append(r)
            elif r["id"] in base_ids:
                c.append(r)
        s = monthly(c, cap)
        print(f"  {lab+' +VIX22':<22}" + "".join(f"{s['month'].get(m,0):>9,.0f}" for m in months)
              + f"{s['total']:>10,.0f}{s['maxdd']:>9,.0f}")
