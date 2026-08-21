# -*- coding: utf-8 -*-
"""S233 part 10 — the ONE parameter: at what VIX does the V16 filter start earning its keep?

A broad plateau = a real regime effect. A single spiky optimum = curve-fitting.
Also diagnoses April, the one month where the relaxed book loses.
"""
import collections, statistics
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
ALL = frozenset(RULES.keys())


def R(pool, off, cap, vg=None, **kw):
    c = []
    for r in pool:
        use = frozenset() if (vg is not None and (r["vix"] or 0) >= vg) else off
        if passes(r, gaps, use)[0]:
            c.append(r)
    return sim(c, cap_l=cap, cap_s=cap, sizing="basket", **kw)


print("### A. VIX threshold sweep (per-signal VIX, no lookahead). filter ON at/above T, OFF below")
for cap in (2, 3):
    print(f"\n  cap {cap}/{cap}")
    print(f"  {'T':>5}{'total$':>10}{'MaxDD':>9}{'trades':>8}{'WR':>5}{'r/DD':>7}   monthly")
    for T in (None, 17, 18, 19, 20, 21, 22, 23, 24, 26, 99):
        vg = None if T in (None,) else T
        off = frozenset() if T == 0 else ALL
        s = R(POOL, off, cap, vg=(None if T is None else T))
        lab = "V16 all" if T is None else ("none" if T >= 99 else str(T))
        if T is None:
            s = R(POOL, frozenset(), cap)
        ms = " ".join(f"{k[-2:]}:{v:>5,.0f}" for k, v in sorted(s['month'].items()))
        print(f"  {lab:>5}{s['total']:>10,.0f}{s['maxdd']:>9,.0f}{s['trades']:>8}{s['wr']:>4.0f}%"
              f"{s['ret_dd']:>7.1f}   {ms}")

print("\n\n### B. how much of the book does the VIX>=22 gate actually cover?")
tot = len(POOL)
for T in (19, 20, 21, 22, 24):
    n = sum(1 for r in POOL if (r["vix"] or 0) >= T)
    d = len({r["ts"].astimezone(ET).date() for r in POOL if (r["vix"] or 0) >= T})
    print(f"  VIX >= {T}: {n:>5} signals ({n/tot*100:>4.1f}%)  on {d} of 100 sessions")

print("\n\n### C. APRIL diagnostic -- the one month the relaxed book loses")
apr = [r for r in POOL if r["ts"].astimezone(ET).strftime("%Y-%m") == "2026-04"]
b = R(apr, frozenset(), 3); n = R(apr, ALL, 3)
bi = {t["id"] for t in b["trade_rows"]}; ni = {t["id"] for t in n["trade_rows"]}
new = [t for t in n["trade_rows"] if t["id"] not in bi]
lost = [t for t in b["trade_rows"] if t["id"] not in ni]
print(f"  V16 ${b['total']:,.0f} ({b['trades']}t)   no-filter ${n['total']:,.0f} ({n['trades']}t)")
print(f"  trades no-filter adds: {len(new)}  WR {sum(1 for t in new if t['pts']>0)/max(len(new),1)*100:.0f}%"
      f"  ${sum(t['pnl'] for t in new):+,.0f}")
print(f"  V16 trades it displaces: {len(lost)}  ${sum(t['pnl'] for t in lost):+,.0f}")
bysetup = collections.defaultdict(lambda: [0, 0, 0.0])
for t in new:
    k = (t["setup"], "L" if t["long"] else "S")
    bysetup[k][0] += 1; bysetup[k][1] += 1 if t["pts"] > 0 else 0; bysetup[k][2] += t["pnl"]
print("  added trades by setup:")
for k, v in sorted(bysetup.items(), key=lambda x: x[1][2]):
    print(f"    {k[0]:<20}{k[1]}  {v[0]:>4}t  WR {v[1]/v[0]*100:>3.0f}%  ${v[2]:>+8,.0f}")
worst = sorted(n["daily"].items(), key=lambda kv: kv[1])[:5]
print("  worst no-filter April days:", " ".join(f"{d.strftime('%m-%d')}:{v:+,.0f}" for d, v in worst))
print("  same days under V16:      ", " ".join(f"{d.strftime('%m-%d')}:{b['daily'].get(d,0):+,.0f}" for d, _ in worst))

print("\n\n### D. does an ALIGNMENT floor beat the VIX gate as the single relaxation guard?")
print("  (keep everything, but require greek_alignment >= A for the trades V16 would have blocked)")
for cap in (2, 3):
    line = f"  cap {cap}: "
    for A in (-3, -2, -1, 0, 1):
        c = []
        for r in POOL:
            if passes(r, gaps)[0]:
                c.append(r); continue
            if passes(r, gaps, ALL)[0] and (r["greek_alignment"] or 0) >= A:
                c.append(r)
        s = sim(c, cap_l=cap, cap_s=cap, sizing="basket")
        line += f"  A>={A}: ${s['total']:,.0f}/DD{s['maxdd']:,.0f}"
    print(line)
