# -*- coding: utf-8 -*-
"""V19c — exit-parameter grid search with leave-one-month-out validation.

Same discipline as the filter study: parameters are chosen using only months the score
never sees. Two selection criteria are compared, because 'best total on train' is the
easiest way to overfit:
  TOTAL  — highest train total points
  RANK   — best average per-month rank on train (robust to one huge month)
"""
import collections, statistics, itertools, sys
import numpy as np
from _tmp_v19_exit import UNIV, live_params, SETUPS
from _tmp_v19_fast import fast_walk

MONTHS = sorted({u["month"] for u in UNIV})
SL = [6, 8, 10, 12, 14, 16, 18, 20]
ACT = [4, 6, 8, 10, 12, 15, 20, None]
GAP = [2, 3, 4, 5, 6, 8, 10]
BE = [(None, 0), (6, 0), (8, 0), (10, 0)]
GRID = [(sl, be[0], be[1], act, gap)
        for sl in SL for act in ACT for gap in GAP for be in BE
        if (act is None or gap < act)]          # a gap wider than activation is meaningless
print(f"grid size: {len(GRID)} parameter sets per setup", file=sys.stderr)

BYSETUP = collections.defaultdict(list)
for u in UNIV:
    BYSETUP[u["setup"]].append(u)

_cache = {}


def pnls(trades, params):
    key = (id(trades), params)
    if key in _cache:
        return _cache[key]
    out = [fast_walk(u["id"], u["is_long"], u["entry"], *params) for u in trades]
    out = [x for x in out if x is not None]
    _cache[key] = out
    return out


def score_total(trades, params):
    return sum(pnls(trades, params))


def per_month(trades, params):
    mo = collections.defaultdict(float)
    for u in trades:
        v = fast_walk(u["id"], u["is_long"], u["entry"], *params)
        if v is not None:
            mo[u["month"]] += v
    return mo


def pick_total(trades):
    best = None
    for p in GRID:
        t = score_total(trades, p)
        if best is None or t > best[0]:
            best = (t, p)
    return best[1]


def pick_rank(trades):
    """Rank every parameter set within each train month, choose the best average rank."""
    months = sorted({u["month"] for u in trades})
    tot = collections.defaultdict(float)
    for m in months:
        sub = [u for u in trades if u["month"] == m]
        if len(sub) < 15:
            continue
        vals = sorted(((score_total(sub, p), i) for i, p in enumerate(GRID)), reverse=True)
        for rank, (_, i) in enumerate(vals):
            tot[GRID[i]] += rank
    if not tot:
        return pick_total(trades)
    return min(tot.items(), key=lambda kv: kv[1])[0]


def lomo(picker):
    oos = collections.defaultdict(list)
    chosen = collections.defaultdict(list)
    for m in MONTHS:
        for s in SETUPS:
            tr = [u for u in BYSETUP[s] if u["month"] != m]
            te = [u for u in BYSETUP[s] if u["month"] == m]
            if not te:
                continue
            if len(tr) < 80:
                p = live_params(s)
            else:
                p = picker(tr)
            chosen[s].append((m, p))
            for u in te:
                v = fast_walk(u["id"], u["is_long"], u["entry"], *p)
                if v is not None:
                    oos[s].append((u, v))
    return oos, chosen


def summarise(pairs):
    if not pairs:
        return (0, 0.0, 0.0, 0.0)
    v = [x[1] for x in pairs]
    byday = collections.defaultdict(float)
    for u, x in pairs:
        byday[u["date"]] += x
    cum = peak = dd = 0.0
    for d in sorted(byday):
        cum += byday[d]; peak = max(peak, cum); dd = min(dd, cum - peak)
    return (len(v), sum(v), sum(1 for x in v if x > 0) / len(v) * 100, dd)


print("### V19 exit-parameter grid, leave-one-month-out\n")
live_pairs = collections.defaultdict(list)
for s in SETUPS:
    for u in BYSETUP[s]:
        v = fast_walk(u["id"], u["is_long"], u["entry"], *live_params(s))
        if v is not None:
            live_pairs[s].append((u, v))

results = {}
for name, picker in (("TOTAL", pick_total), ("RANK", pick_rank)):
    oos, chosen = lomo(picker)
    results[name] = (oos, chosen)

print(f"  {'setup':<20}{'live pts':>10}{'OOS TOTAL':>11}{'OOS RANK':>11}{'n':>6}   live params")
gl = gt = gr = 0.0
for s in SETUPS:
    a = summarise(live_pairs[s]); b = summarise(results["TOTAL"][0][s]); c = summarise(results["RANK"][0][s])
    gl += a[1]; gt += b[1]; gr += c[1]
    p = live_params(s)
    print(f"  {s:<20}{a[1]:>+10.1f}{b[1]:>+11.1f}{c[1]:>+11.1f}{a[0]:>6}   "
          f"sl{p[0]:g} be{p[1] if p[1] is not None else '-'} act{p[3]:g} gap{p[4]:g}")
print(f"  {'TOTAL':<20}{gl:>+10.1f}{gt:>+11.1f}{gr:>+11.1f}")
print(f"\n  in dollars at 1 MES: live ${gl*5:,.0f}   fitted-TOTAL ${gt*5:,.0f}   fitted-RANK ${gr*5:,.0f}")

print("\n### how stable are the chosen parameters across the 6 folds?")
for s in SETUPS:
    ch = results["RANK"][1][s]
    cnt = collections.Counter(p for _, p in ch)
    top = cnt.most_common(3)
    print(f"  {s:<20}" + "   ".join(
        f"sl{p[0]:g}/be{p[1] if p[1] is not None else '-'}/act{p[3] if p[3] is not None else '-'}/gap{p[4]:g} x{n}"
        for p, n in top))

print("\n### IN-SAMPLE best (what a naive study would report)")
ins = 0.0
for s in SETUPS:
    p = pick_total(BYSETUP[s])
    t = score_total(BYSETUP[s], p)
    ins += t
    print(f"  {s:<20}{t:>+10.1f} pts   with sl{p[0]:g}/be{p[1] if p[1] is not None else '-'}"
          f"/act{p[3] if p[3] is not None else '-'}/gap{p[4]:g}")
print(f"  {'TOTAL':<20}{ins:>+10.1f} pts = ${ins*5:,.0f}   <- fitted and scored on the same data")
