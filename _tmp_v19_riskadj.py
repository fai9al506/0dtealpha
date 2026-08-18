# -*- coding: utf-8 -*-
"""V19e — the RIGHT objective.

Maximising total points just buys P&L with a wider stop. On a capital-constrained account the
thing that matters is return per unit of drawdown, because drawdown is what caps contract size.
Re-runs the search on three objectives and compares them out of sample.
"""
import collections, statistics
from _tmp_v19_exit import UNIV, live_params, SETUPS
from _tmp_v19_fast import fast_walk
from _tmp_v19_grid import GRID, BYSETUP, MONTHS

LIVE = {s: live_params(s) for s in SETUPS}
_C = {}


def series(trades, p):
    key = (id(trades), p)
    if key not in _C:
        _C[key] = [(u, fast_walk(u["id"], u["is_long"], u["entry"], *p)) for u in trades]
        _C[key] = [(u, v) for u, v in _C[key] if v is not None]
    return _C[key]


def metrics(pairs):
    if not pairs:
        return dict(n=0, tot=0.0, dd=0.0, wr=0.0, rdd=0.0)
    byday = collections.defaultdict(float)
    for u, v in pairs:
        byday[u["date"]] += v
    cum = peak = dd = 0.0
    for d in sorted(byday):
        cum += byday[d]; peak = max(peak, cum); dd = min(dd, cum - peak)
    tot = sum(v for _, v in pairs)
    return dict(n=len(pairs), tot=tot, dd=dd, wr=sum(1 for _, v in pairs if v > 0) / len(pairs) * 100,
                rdd=(tot / abs(dd)) if dd else 0.0)


def pick(trades, objective, live_dd=None):
    best = None
    for p in GRID:
        m = metrics(series(trades, p))
        if m["n"] < 20:
            continue
        if objective == "total":
            sc = m["tot"]
        elif objective == "retdd":
            sc = m["rdd"]
        elif objective == "capped":       # most points without exceeding the live drawdown
            if live_dd is not None and m["dd"] < live_dd * 1.05:
                continue
            sc = m["tot"]
        if best is None or sc > best[0]:
            best = (sc, p)
    return best[1] if best else LIVE.get(trades[0]["setup"], (14, None, 0, 10, 5))


print("### exit parameters chosen on three different objectives, all scored OUT OF SAMPLE\n")
out = {}
for obj in ("total", "retdd", "capped"):
    pairs = []
    chosen = collections.defaultdict(list)
    for m in MONTHS:
        for s in SETUPS:
            tr = [u for u in BYSETUP[s] if u["month"] != m]
            te = [u for u in BYSETUP[s] if u["month"] == m]
            if not te:
                continue
            if len(tr) < 80:
                p = LIVE[s]
            else:
                ldd = metrics(series(tr, LIVE[s]))["dd"]
                p = pick(tr, obj, ldd)
            chosen[s].append(p)
            pairs += series(te, p)
    out[obj] = (pairs, chosen)

livep = []
for s in SETUPS:
    livep += series(BYSETUP[s], LIVE[s])
rows = [("live (today)", metrics(livep))]
for obj in ("total", "retdd", "capped"):
    rows.append((f"fitted on {obj}", metrics(out[obj][0])))
print(f"  {'config':<22}{'trades':>7}{'WR':>7}{'points':>10}{'MaxDD':>9}{'ret/DD':>8}"
      f"{'$ @1MES':>10}{'$/mo':>8}")
for lab, m in rows:
    mo = 117 / 21
    print(f"  {lab:<22}{m['n']:>7}{m['wr']:>6.1f}%{m['tot']:>+10.1f}{m['dd']:>9.1f}{m['rdd']:>8.1f}"
          f"{m['tot']*5:>10,.0f}{m['tot']*5/mo:>8,.0f}")

print("\n  ret/DD is the number that decides contract size. A config with more points but a")
print("  worse ret/DD must be traded SMALLER, so it earns less on the same account.")

print("\n### equalised comparison — size each config so its drawdown matches today's")
base = metrics(livep)
for lab, m in rows:
    if not m["dd"]:
        continue
    scale = abs(base["dd"]) / abs(m["dd"])
    print(f"  {lab:<22}scale {scale:>5.2f}x  ->  {m['tot']*scale:>+9.1f} pts "
          f"(${m['tot']*scale*5:>8,.0f})  at the SAME drawdown as today")

print("\n\n### per-setup: what the risk-adjusted objective actually picks")
for s in SETUPS:
    c = collections.Counter(out["retdd"][1][s])
    lp = LIVE[s]
    top = c.most_common(2)
    print(f"  {s:<20}live sl{lp[0]:g}/be{lp[1] if lp[1] is not None else '-'}/act{lp[3]:g}/gap{lp[4]:g}"
          f"   ->  " + "   ".join(
              f"sl{p[0]:g}/be{p[1] if p[1] is not None else '-'}/act{p[3] if p[3] is not None else '-'}/gap{p[4]:g} x{n}"
              for p, n in top))

print("\n\n### per-setup out-of-sample, risk-adjusted objective vs live")
print(f"  {'setup':<20}{'live pts':>10}{'live DD':>9}{'fit pts':>10}{'fit DD':>9}{'verdict':>12}")
for s in SETUPS:
    a = metrics(series(BYSETUP[s], LIVE[s]))
    b = metrics([x for x in out["retdd"][0] if x[0]["setup"] == s])
    v = "better" if (b["rdd"] > a["rdd"] and b["tot"] > a["tot"]) else (
        "worse" if b["tot"] < a["tot"] else "mixed")
    print(f"  {s:<20}{a['tot']:>+10.1f}{a['dd']:>9.1f}{b['tot']:>+10.1f}{b['dd']:>9.1f}{v:>12}")
