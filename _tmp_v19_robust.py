# -*- coding: utf-8 -*-
"""V19d — is the exit improvement robust?
1. month by month, out of sample
2. does ONE global parameter set (least fittable) beat per-setup fitting?
3. what it does to drawdown — wider stops mean bigger individual losses
4. the shape of the parameter surface: a plateau or a spike?
"""
import collections, statistics
from _tmp_v19_exit import UNIV, live_params, SETUPS
from _tmp_v19_fast import fast_walk
from _tmp_v19_grid import GRID, BYSETUP, MONTHS, pick_total, pick_rank, score_total, summarise


def apply(trades, pmap):
    out = []
    for u in trades:
        p = pmap.get(u["setup"]) if isinstance(pmap, dict) else pmap
        v = fast_walk(u["id"], u["is_long"], u["entry"], *p)
        if v is not None:
            out.append((u, v))
    return out


LIVE = {s: live_params(s) for s in SETUPS}

print("### 1. ONE GLOBAL parameter set for every setup (the least overfittable option)")
print("    chosen out-of-sample: fit on 5 months of the WHOLE book, score the 6th\n")
oos_g, chosen_g = [], []
for m in MONTHS:
    tr = [u for u in UNIV if u["month"] != m]
    te = [u for u in UNIV if u["month"] == m]
    best = None
    for p in GRID:
        t = sum(x for x in (fast_walk(u["id"], u["is_long"], u["entry"], *p) for u in tr) if x is not None)
        if best is None or t > best[0]:
            best = (t, p)
    chosen_g.append((m, best[1]))
    oos_g += apply(te, best[1])
print(f"  {'held-out month':<16}{'chosen on the other 5':<34}{'live pts':>10}{'global pts':>12}")
tl = tg = 0.0
for m, p in chosen_g:
    te = [u for u in UNIV if u["month"] == m]
    a = sum(x for x in (fast_walk(u["id"], u["is_long"], u["entry"], *LIVE[u["setup"]]) for u in te) if x is not None)
    b = sum(x for x in (fast_walk(u["id"], u["is_long"], u["entry"], *p) for u in te) if x is not None)
    tl += a; tg += b
    ps = f"sl{p[0]:g} be{p[1] if p[1] is not None else '-'} act{p[3] if p[3] is not None else '-'} gap{p[4]:g}"
    print(f"  {m:<16}{ps:<34}{a:>+10.1f}{b:>+12.1f}")
print(f"  {'TOTAL':<16}{'':<34}{tl:>+10.1f}{tg:>+12.1f}   ({(tg-tl):+.1f} pts = ${(tg-tl)*5:+,.0f} @1 MES)")

print("\n\n### 2. per-setup fitted, month by month (out of sample)")
oos_ps = []
for m in MONTHS:
    for s in SETUPS:
        tr = [u for u in BYSETUP[s] if u["month"] != m]
        te = [u for u in BYSETUP[s] if u["month"] == m]
        if not te:
            continue
        p = pick_rank(tr) if len(tr) >= 80 else LIVE[s]
        oos_ps += apply(te, p)
mo_live = collections.defaultdict(float); mo_ps = collections.defaultdict(float)
mo_g = collections.defaultdict(float)
for u, v in apply(UNIV, LIVE):
    mo_live[u["month"]] += v
for u, v in oos_ps:
    mo_ps[u["month"]] += v
for u, v in oos_g:
    mo_g[u["month"]] += v
print(f"  {'month':<10}{'live':>10}{'per-setup OOS':>16}{'global OOS':>13}{'best':>8}")
for m in MONTHS:
    best = max((mo_live[m], "live"), (mo_ps[m], "per-setup"), (mo_g[m], "global"))[1]
    print(f"  {m:<10}{mo_live[m]:>+10.1f}{mo_ps[m]:>+16.1f}{mo_g[m]:>+13.1f}{best:>10}")
print(f"  {'TOTAL':<10}{sum(mo_live.values()):>+10.1f}{sum(mo_ps.values()):>+16.1f}{sum(mo_g.values()):>+13.1f}")

print("\n\n### 3. DRAWDOWN — a wider stop means bigger single losses")
for lab, pairs in (("live params", apply(UNIV, LIVE)), ("per-setup OOS", oos_ps), ("global OOS", oos_g)):
    n, tot, wr, dd = summarise(pairs)
    v = [x[1] for x in pairs]
    w = [x for x in v if x > 0]; l = [x for x in v if x <= 0]
    print(f"  {lab:<16}{n:>5}t  WR {wr:>4.1f}%  {tot:>+8.1f} pts  MaxDD {dd:>7.1f}  "
          f"avg win {statistics.mean(w):>+6.2f}  avg loss {statistics.mean(l):>+6.2f}  "
          f"worst {min(v):>+6.1f}")

print("\n\n### 4. is the parameter surface a PLATEAU or a SPIKE? (whole book, in sample)")
allp = sorted(((sum(x for x in (fast_walk(u["id"], u["is_long"], u["entry"], *p) for u in UNIV)
                    if x is not None), p) for p in GRID), reverse=True)
print(f"  best in-sample: {allp[0][0]:+.1f} pts with sl{allp[0][1][0]:g}/"
      f"be{allp[0][1][1] if allp[0][1][1] is not None else '-'}/"
      f"act{allp[0][1][3] if allp[0][1][3] is not None else '-'}/gap{allp[0][1][4]:g}")
print(f"  live params rank: ", end="")
lp = None
for i, (t, p) in enumerate(allp):
    if p == LIVE["Skew Charm"]:
        lp = (i, t)
print(f"{lp[0]+1} of {len(allp)}  ({lp[1]:+.1f} pts)" if lp else "n/a")
print(f"  top 20 parameter sets — if they cluster, the result is a plateau:")
for t, p in allp[:20]:
    print(f"    {t:>+9.1f}  sl{p[0]:>2g}  be{str(p[1]) if p[1] is not None else '-':>4}  "
          f"act{str(p[3]) if p[3] is not None else '-':>4}  gap{p[4]:>2g}")
import collections as _c
print("\n  distribution of the top 50:")
for k, idx in (("sl", 0), ("be", 1), ("act", 3), ("gap", 4)):
    c = _c.Counter(p[idx] for _, p in allp[:50])
    print(f"    {k:<4}" + "  ".join(f"{v}:{n}" for v, n in sorted(c.items(), key=lambda x: -x[1])))
