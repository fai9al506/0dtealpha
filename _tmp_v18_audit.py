# -*- coding: utf-8 -*-
"""V18 step 1 — PER-SETUP BASELINE AUDIT. No filter at all. What is each setup actually worth,
by direction, month, and every available feature? This is the ground truth every filter decision
must be built from."""
import collections, statistics, sys
from _tmp_v18_data import load, enrich, REAL_SETUPS, wr, tot, ppt, summarise, ET

rows, gaps, daily = load()
R = enrich(rows, gaps, daily)
R = [r for r in R if r["pts"] is not None and r["date"].isoformat() >= "2026-03-01"]
MONTHS = sorted({r["month"] for r in R})
print(f"### V18 baseline audit — {len(R)} resolved signals, {MONTHS[0]} to {MONTHS[-1]}, "
      f"{len({r['date'] for r in R})} sessions.  Raw chain points, NO filter, NO gates.\n")

print("=" * 118)
print("PART 1 — every setup, ungated, by direction")
print("=" * 118)
print(f"  {'setup / direction':<30}{'n':>5}{'WR':>7}{'total':>10}{'pts/t':>8}   " +
      " ".join(f"{m[-2:]:>7}" for m in MONTHS) + "   mo+")
for sn in sorted({r["setup_name"] for r in R}, key=lambda s: -tot([x for x in R if x["setup_name"] == s])):
    for lab, sub in (("", [r for r in R if r["setup_name"] == sn]),
                     ("  LONG", [r for r in R if r["setup_name"] == sn and r["is_long"]]),
                     ("  SHORT", [r for r in R if r["setup_name"] == sn and not r["is_long"]])):
        if not sub:
            continue
        print(summarise(sub, (sn if not lab else lab), MONTHS))
    print()

print("=" * 118)
print("PART 2 — the six real setups: does each FEATURE actually separate winners from losers?")
print("=" * 118)


def numeric_split(rs, key, qs=(0.25, 0.5, 0.75)):
    v = sorted(x[key] for x in rs if x.get(key) is not None)
    if len(v) < 60:
        return []
    out = []
    for q in qs:
        thr = v[int(len(v) * q)]
        lo = [x for x in rs if x.get(key) is not None and x[key] < thr]
        hi = [x for x in rs if x.get(key) is not None and x[key] >= thr]
        if len(lo) >= 25 and len(hi) >= 25:
            out.append((thr, lo, hi))
    return out


NUMERIC = ["greek_alignment", "vix", "overvix", "score", "mins", "abs_vol_ratio",
           "v13_gex_above", "lis_abs", "tgt_dist", "from_open", "rr_ratio",
           "spot_vol_beta", "vanna_all", "gap"]
CATEG = ["grade", "paradigm", "vanna_cliff_side", "vanna_peak_side", "vanna_regime", "dow", "hour"]

for sn in REAL_SETUPS:
    for dlab, sub in (("LONG", [r for r in R if r["setup_name"] == sn and r["is_long"]]),
                      ("SHORT", [r for r in R if r["setup_name"] == sn and not r["is_long"]])):
        if len(sub) < 40:
            continue
        print(f"\n--- {sn} {dlab} --- {len(sub)} trades, WR {wr(sub):.0f}%, "
              f"{tot(sub):+.1f} pts, {ppt(sub):+.2f}/trade")
        found = []
        for key in NUMERIC:
            for thr, lo, hi in numeric_split(sub, key):
                gap = ppt(hi) - ppt(lo)
                if abs(gap) < 0.8:
                    continue
                # month consistency of the better side
                better, worse = (hi, lo) if gap > 0 else (lo, hi)
                mo = collections.defaultdict(float)
                for x in better:
                    mo[x["month"]] += x["pts"]
                pos = sum(1 for m in MONTHS if mo.get(m, 0) > 0)
                found.append((abs(gap), f"{key} {'>=' if gap>0 else '<'} {thr:g}",
                              len(better), wr(better), ppt(better), ppt(worse), pos))
        for key in CATEG:
            groups = collections.defaultdict(list)
            for x in sub:
                groups[x.get(key)].append(x)
            for k, g in groups.items():
                if len(g) < 25:
                    continue
                rest = [x for x in sub if x.get(key) != k]
                if len(rest) < 25:
                    continue
                gap = ppt(g) - ppt(rest)
                if abs(gap) < 0.8:
                    continue
                mo = collections.defaultdict(float)
                for x in (g if gap > 0 else rest):
                    mo[x["month"]] += x["pts"]
                pos = sum(1 for m in MONTHS if mo.get(m, 0) > 0)
                found.append((abs(gap), f"{key} {'==' if gap>0 else '!='} {k}",
                              len(g) if gap > 0 else len(rest),
                              wr(g if gap > 0 else rest), ppt(g if gap > 0 else rest),
                              ppt(rest if gap > 0 else g), pos))
        if not found:
            print("     no single feature separates by more than 0.8 pts/trade")
            continue
        print(f"     {'condition (the GOOD side)':<34}{'n':>5}{'WR':>6}{'good':>8}{'bad':>8}{'edge':>7}{'mo+':>5}")
        for g, cond, n, w, pg, pb, pos in sorted(found, reverse=True)[:8]:
            print(f"     {cond:<34}{n:>5}{w:>5.0f}%{pg:>+8.2f}{pb:>+8.2f}{g:>7.2f}{pos:>4}/{len(MONTHS)}")
