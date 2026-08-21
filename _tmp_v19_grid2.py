# -*- coding: utf-8 -*-
"""V19f — exit grid, REWRITTEN after a cache bug.

Bug found 2026-08-08: the first version keyed its memo on `id(trades)`. CPython recycles the
id of a garbage-collected list, so different train sets silently shared cached results. Every
number from _tmp_v19_grid.py / _tmp_v19_riskadj.py is void. Here the key is a stable
(setup, held-out-month, params) tuple and the trade lists are built ONCE up front.
"""
import collections, statistics, sys
from _tmp_v19_exit import UNIV, live_params, SETUPS
from _tmp_v19_fast import fast_walk

MONTHS = sorted({u["month"] for u in UNIV})
SL = [6, 8, 10, 12, 14, 16, 18, 20]
ACT = [4, 6, 8, 10, 12, 15, 20, None]
GAP = [2, 3, 4, 5, 6, 8, 10]
BE = [None, 6, 8, 10]
GRID = [(sl, be, 0, act, gap) for sl in SL for act in ACT for gap in GAP for be in BE
        if (act is None or gap < act)]
LIVE = {s: live_params(s) for s in SETUPS}

# built once, never rebuilt -> stable identity
BYSETUP = {s: [u for u in UNIV if u["setup"] == s] for s in SETUPS}
TRAIN = {(s, m): [u for u in BYSETUP[s] if u["month"] != m] for s in SETUPS for m in MONTHS}
TEST = {(s, m): [u for u in BYSETUP[s] if u["month"] == m] for s in SETUPS for m in MONTHS}

# per-trade pnl for every parameter set, computed once per (trade, params)
_PNL = {}


def pv(u, p):
    k = (u["id"], p)
    v = _PNL.get(k)
    if v is None:
        v = fast_walk(u["id"], u["is_long"], u["entry"], *p)
        _PNL[k] = v if v is not None else 0.0
        v = _PNL[k]
    return v


def metrics(trades, p):
    if not trades:
        return dict(n=0, tot=0.0, dd=0.0, wr=0.0, rdd=0.0)
    byday = collections.defaultdict(float)
    tot = 0.0; w = 0
    for u in trades:
        v = pv(u, p)
        byday[u["date"]] += v
        tot += v
        w += 1 if v > 0 else 0
    cum = peak = dd = 0.0
    for d in sorted(byday):
        cum += byday[d]; peak = max(peak, cum); dd = min(dd, cum - peak)
    return dict(n=len(trades), tot=tot, dd=dd, wr=w / len(trades) * 100,
                rdd=(tot / abs(dd)) if dd else 0.0)


def pick(trades, objective):
    live_dd = None
    if objective == "capped":
        live_dd = metrics(trades, LIVE[trades[0]["setup"]])["dd"]
    best = None
    for p in GRID:
        m = metrics(trades, p)
        if objective == "total":
            sc = m["tot"]
        elif objective == "retdd":
            if m["n"] < 30 or abs(m["dd"]) < 20:      # guard: tiny DD inflates the ratio
                continue
            sc = m["rdd"]
        else:  # capped: most points without a worse drawdown than live
            if live_dd is not None and m["dd"] < live_dd:
                continue
            sc = m["tot"]
        if best is None or sc > best[0]:
            best = (sc, p)
    return best[1] if best else LIVE[trades[0]["setup"]]


def evaluate(pairs):
    if not pairs:
        return dict(n=0, tot=0.0, dd=0.0, wr=0.0, rdd=0.0)
    byday = collections.defaultdict(float)
    for u, v in pairs:
        byday[u["date"]] += v
    cum = peak = dd = 0.0
    for d in sorted(byday):
        cum += byday[d]; peak = max(peak, cum); dd = min(dd, cum - peak)
    tot = sum(v for _, v in pairs)
    return dict(n=len(pairs), tot=tot, dd=dd,
                wr=sum(1 for _, v in pairs if v > 0) / len(pairs) * 100,
                rdd=(tot / abs(dd)) if dd else 0.0)


print("### V19 exit study, corrected harness. Leave-one-month-out, 1-min SPX.\n")
live_pairs = [(u, pv(u, LIVE[u["setup"]])) for u in UNIV]
res = {}
for obj in ("total", "retdd", "capped"):
    pairs = []
    chosen = collections.defaultdict(list)
    for m in MONTHS:
        for s in SETUPS:
            tr, te = TRAIN[(s, m)], TEST[(s, m)]
            if not te:
                continue
            p = pick(tr, obj) if len(tr) >= 80 else LIVE[s]
            chosen[s].append(p)
            pairs += [(u, pv(u, p)) for u in te]
    res[obj] = (pairs, chosen)

mo = len({u["date"] for u in UNIV}) / 21
print(f"  {'config':<26}{'trades':>7}{'WR':>7}{'points':>10}{'MaxDD':>9}{'ret/DD':>8}{'$/mo @1MES':>12}")
rows = [("live (today)", evaluate(live_pairs))]
for obj in ("total", "retdd", "capped"):
    rows.append((f"fitted OOS on {obj}", evaluate(res[obj][0])))
for lab, m in rows:
    print(f"  {lab:<26}{m['n']:>7}{m['wr']:>6.1f}%{m['tot']:>+10.1f}{m['dd']:>9.1f}"
          f"{m['rdd']:>8.1f}{m['tot']*5/mo:>12,.0f}")

base = evaluate(live_pairs)
print("\n### sized so every config carries TODAY's drawdown (the fair comparison)")
for lab, m in rows:
    if not m["dd"]:
        continue
    sc = abs(base["dd"]) / abs(m["dd"])
    print(f"  {lab:<26}scale {sc:>5.2f}x  ->  {m['tot']*sc:>+9.1f} pts  "
          f"= ${m['tot']*sc*5/mo:>7,.0f}/mo at the same risk")

print("\n### month by month, out of sample")
mm = {k: collections.defaultdict(float) for k in ("live", "total", "retdd", "capped")}
for u, v in live_pairs:
    mm["live"][u["month"]] += v
for obj in ("total", "retdd", "capped"):
    for u, v in res[obj][0]:
        mm[obj][u["month"]] += v
print(f"  {'month':<10}{'live':>10}{'total':>10}{'retdd':>10}{'capped':>10}")
for m in MONTHS:
    print(f"  {m:<10}{mm['live'][m]:>+10.1f}{mm['total'][m]:>+10.1f}"
          f"{mm['retdd'][m]:>+10.1f}{mm['capped'][m]:>+10.1f}")
print(f"  {'TOTAL':<10}" + "".join(f"{sum(mm[k].values()):>+10.1f}" for k in ("live", "total", "retdd", "capped")))

print("\n### what each objective picks, per setup (most frequent across the 6 folds)")
for s in SETUPS:
    line = f"  {s:<20}live sl{LIVE[s][0]:g}/be{LIVE[s][1] if LIVE[s][1] is not None else '-'}/act{LIVE[s][3]:g}/gap{LIVE[s][4]:g}"
    for obj in ("total", "retdd", "capped"):
        p = collections.Counter(res[obj][1][s]).most_common(1)[0][0]
        line += f"   | {obj}: sl{p[0]:g}/be{p[1] if p[1] is not None else '-'}/act{p[3] if p[3] is not None else '-'}/gap{p[4]:g}"
    print(line)

print("\n### per-setup, capped objective (most points at no worse drawdown) vs live")
print(f"  {'setup':<20}{'live pts':>10}{'live DD':>9}{'OOS pts':>10}{'OOS DD':>9}{'delta':>9}")
for s in SETUPS:
    a = evaluate([(u, pv(u, LIVE[s])) for u in BYSETUP[s]])
    b = evaluate([x for x in res["capped"][0] if x[0]["setup"] == s])
    print(f"  {s:<20}{a['tot']:>+10.1f}{a['dd']:>9.1f}{b['tot']:>+10.1f}{b['dd']:>9.1f}"
          f"{b['tot']-a['tot']:>+9.1f}")
