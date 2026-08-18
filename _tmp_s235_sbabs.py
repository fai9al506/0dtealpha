# -*- coding: utf-8 -*-
"""S235 — should SB Absorption be added to the real-trade whitelist?

It is the best per-trade signal in the database (+3.87 pts/trade) and has never been traded.
n=49, so this is a validation study, not a green light.
IMPORTANT: signals on/after 2026-07-02 were computed from ES bars ~10 min stale (S236), so the
clean sample is pre-07-02 only.
"""
import collections, statistics, random
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

random.seed(23)
rows, gaps = load()
ALL = frozenset(RULES.keys())
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
RELAX = {"Skew Charm", "ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"}
POOL = [r for r in rows if r["outcome_pnl"] is not None]
SBA = [r for r in POOL if r["setup_name"] == "SB Absorption"]
CLEAN = [r for r in SBA if r["ts"].astimezone(ET).date().isoformat() < "2026-07-02"]


def boot(v, n=5000):
    m = sorted(sum(v[random.randrange(len(v))] for _ in range(len(v))) / len(v) for _ in range(n))
    return m[int(n * .05)], m[int(n * .95)]


def line(lab, rs):
    if len(rs) < 3:
        return f"  {lab:<30}{len(rs):>4}t  (too few)"
    v = [float(r["outcome_pnl"]) for r in rs]
    lo, hi = boot(v)
    wr = sum(1 for x in v if x > 0) / len(v) * 100
    return (f"  {lab:<30}{len(v):>4}t  WR {wr:>3.0f}%  {sum(v):>+8.1f} pts  "
            f"{sum(v)/len(v):>+5.2f}/t   90% CI [{lo:+.2f}, {hi:+.2f}]")


print("### 1. the sample, and what S236 contamination removes")
print(line("ALL SB Absorption", SBA))
print(line("CLEAN (before 2026-07-02)", CLEAN))
print(line("contaminated (07-02 on)", [r for r in SBA if r not in CLEAN]))
print("\n### 2. clean sample by direction / grade / month")
print(line("clean LONG", [r for r in CLEAN if r["direction"] in ("long", "bullish")]))
print(line("clean SHORT", [r for r in CLEAN if r["direction"] not in ("long", "bullish")]))
for g in sorted({r["grade"] for r in CLEAN if r["grade"]}):
    print(line(f"clean grade {g}", [r for r in CLEAN if r["grade"] == g]))
mo = collections.defaultdict(list)
for r in CLEAN:
    mo[r["ts"].astimezone(ET).strftime("%Y-%m")].append(float(r["outcome_pnl"]))
print("\n  by month: " + "  ".join(f"{k[-2:]}:{sum(v):+.0f}({len(v)}t)" for k, v in sorted(mo.items())))
print(f"  positive months: {sum(1 for v in mo.values() if sum(v) > 0)} of {len(mo)}")

print("\n### 3. is it a few trades? (the usual killer)")
v = sorted((float(r["outcome_pnl"]) for r in CLEAN), reverse=True)
print(f"  total {sum(v):+.1f} pts   top 3 = {sum(v[:3]):+.1f} ({sum(v[:3])/sum(v)*100:.0f}%)   "
      f"top 5 = {sum(v[:5]):+.1f} ({sum(v[:5])/sum(v)*100:.0f}%)   ex-top-3 = {sum(v[3:]):+.1f}")
print(f"  median trade {statistics.median(v):+.2f} pts   "
      f"biggest win {max(v):+.1f}   biggest loss {min(v):+.1f}")
print(f"  ex-top-3 per trade: {sum(v[3:])/max(len(v)-3,1):+.2f}  "
      f"(vs Skew Charm's +1.67 over 1,378 trades)")

print("\n### 4. frequency — how much can it actually contribute?")
sess = len({r["ts"].astimezone(ET).date() for r in POOL
            if r["ts"].astimezone(ET).date().isoformat() < "2026-07-02"
            and r["ts"].astimezone(ET).date().isoformat() >= "2026-03-01"})
print(f"  {len(CLEAN)} clean signals over {sess} sessions = {len(CLEAN)/sess:.2f}/session")
print(f"  at 1 MES that is ${sum(v)*5/ (sess/21):,.0f}/month IF every signal were taken ungated")

print("\n### 5. PORTFOLIO impact — added to the V17 book, clean window only")
W0, W1 = "2026-03-16", "2026-07-02"
P = [r for r in POOL if r["setup_name"] in WHITELIST
     and W0 <= r["ts"].astimezone(ET).date().isoformat() < W1]


def v17(pool):
    out = []
    for r in pool:
        il = r["direction"] in ("long", "bullish"); sn = r["setup_name"]
        if sn not in RELAX or (r["vix"] or 0) >= 22:
            if passes(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL - KEEP_DD if (sn == "DD Exhaustion" and not il) else ALL
        if passes(r, gaps, use)[0]:
            out.append(r)
    return out


base = v17(P)
sb_in = [r for r in CLEAN if W0 <= r["ts"].astimezone(ET).date().isoformat() < W1]
print(HDR)
for cap in (2, 3):
    b = sim(base, cap, cap, "basket")
    s = sim(sorted(base + sb_in, key=lambda r: r["ts"]), cap, cap, "basket")
    print(fmt(b, f"V17 baseline cap{cap}"))
    print(fmt(s, f"+ SB Absorption cap{cap}") + f"   [{s['total']-b['total']:+,.0f}]")

b = sim(base, 2, 2, "basket"); s = sim(sorted(base + sb_in, key=lambda r: r["ts"]), 2, 2, "basket")
ids = {r["id"] for r in sb_in}
took = [t for t in s["trade_rows"] if t["id"] in ids]
lost = [t for t in b["trade_rows"] if t["id"] not in {x["id"] for x in s["trade_rows"]}]
print(f"\n  offered {len(sb_in)}, taken {len(took)}, worth ${sum(t['pnl'] for t in took):+,.0f}; "
      f"displaced {len(lost)} worth ${sum(t['pnl'] for t in lost):+,.0f}")
dd = {d: s["daily"].get(d, 0) - b["daily"].get(d, 0) for d in set(list(b["daily"]) + list(s["daily"]))}
top = sorted(dd.items(), key=lambda kv: -abs(kv[1]))[:5]
print("  biggest day changes: " + "  ".join(f"{d.strftime('%m-%d')}:{x:+,.0f}" for d, x in top))
print(f"  days changed: {sum(1 for x in dd.values() if abs(x) > 1)} of {len(dd)}")
days = sorted(dd)
wins = sum(1 for _ in range(3000)
           if sum(dd[days[random.randrange(len(days))]] for _ in range(len(days))) > 0)
print(f"  day-level bootstrap: better in {wins/3000*100:.0f}% of resamples")

print("\n### 6. per-month portfolio delta (the concentration test that killed VPB shorts)")
ms = sorted(b["month"])
print("  " + "  ".join(f"{m[-2:]}:{s['month'].get(m,0)-b['month'].get(m,0):+,.0f}" for m in ms))
