# -*- coding: utf-8 -*-
"""S233 part 9 — HONEST out-of-sample test of the 'keep-good-only' selector.

Leave-one-month-out: the keeper set is chosen using only the other 5 months, then the
held-out month is scored with it. Repeated at cap 2/2 and 3/3. Also scores the two
選-free baselines (no filter = 0 params, VIX-gated no filter = 1 param) the same way.
"""
import sys, collections
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
ALL = frozenset(RULES.keys())
MONTHS = sorted({r["ts"].astimezone(ET).strftime("%Y-%m") for r in POOL})


def m_of(r):
    return r["ts"].astimezone(ET).strftime("%Y-%m")


def R(pool, off, cap, vg=None, **kw):
    c = []
    for r in pool:
        use = frozenset() if (vg is not None and (r["vix"] or 0) >= vg) else off
        if passes(r, gaps, use)[0]:
            c.append(r)
    return sim(c, cap_l=cap, cap_s=cap, sizing="basket", **kw)


def keepers_from(pool, cap):
    """Keep a rule only if its leave-one-out delta on THIS pool is negative (it earns money)."""
    base = R(pool, frozenset(), cap)["total"]
    keep = set()
    for rule in RULES:
        if R(pool, frozenset([rule]), cap)["total"] - base < 0:
            keep.add(rule)
    return ALL - keep   # what we switch OFF


for cap in (2, 3):
    print(f"\n### leave-one-month-out, cap {cap}/{cap} -- keeper set chosen WITHOUT the scored month")
    print(f"  {'held-out':<9}{'V16':>9}{'V17e-oos':>10}{'diff':>8}{'noFilt':>9}{'noFilt+VIX22':>14}{'wide17*':>9}")
    t = collections.defaultdict(float)
    for m in MONTHS:
        tr = [r for r in POOL if m_of(r) != m]
        te = [r for r in POOL if m_of(r) == m]
        off = keepers_from(tr, cap)
        a = R(te, frozenset(), cap)["total"]
        b = R(te, off, cap)["total"]
        c = R(te, ALL, cap)["total"]
        d = R(te, ALL, cap, vg=22)["total"]
        t["v16"] += a; t["oos"] += b; t["nof"] += c; t["nof22"] += d
        print(f"  {m:<9}{a:>9,.0f}{b:>10,.0f}{b-a:>+8,.0f}{c:>9,.0f}{d:>14,.0f}   {len(off)} off")
    print(f"  {'TOTAL':<9}{t['v16']:>9,.0f}{t['oos']:>10,.0f}{t['oos']-t['v16']:>+8,.0f}"
          f"{t['nof']:>9,.0f}{t['nof22']:>14,.0f}")
    print("  * no-filter and no-filter+VIX22 need NO selection, so their column IS out-of-sample")

# ── how stable is the keeper set itself? ──
print("\n\n### keeper-set stability (which rules survive as 'earns money' in each fold, cap 3/3)")
folds = {}
for m in MONTHS:
    tr = [r for r in POOL if m_of(r) != m]
    folds[m] = ALL - keepers_from(tr, 3)   # the KEEP set
cnt = collections.Counter()
for m, ks in folds.items():
    for k in ks:
        cnt[k] += 1
print("  rule kept in N of 6 folds:")
for k, v in cnt.most_common():
    print(f"    {v}/6  {k}")
unan = {k for k, v in cnt.items() if v == 6}
print(f"\n  unanimous keepers ({len(unan)}): {', '.join(sorted(unan))}")
UNAN_OFF = ALL - unan
print(f"  => 'V17u' switches OFF {len(UNAN_OFF)} rules")
print("\n" + HDR)
for cap in (2, 3):
    print(fmt(R(POOL, UNAN_OFF, cap), f"V17u unanimous cap{cap}"))
    print(fmt(R(POOL, UNAN_OFF, cap, vg=22), f"V17u + VIX>=22 cap{cap}"))
