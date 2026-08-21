# -*- coding: utf-8 -*-
"""V20 — the signals we already generate and throw away.

Three buckets are excluded by an old structural decision, not by evidence:
  1. SB Absorption      — not in the real-trade whitelist at all
  2. VIX Divergence SHORT — setup is long-only
  3. Vanna Pivot Bounce SHORT — setup is long-only
Question: is each one real, or is it a small-sample mirage?
"""
import collections, statistics, random
from _tmp_v18_data import load, enrich, wr, tot, ppt, ET

rows, gaps, daily = load()
R = [r for r in enrich(rows, gaps, daily) if r["pts"] is not None]
MONTHS = sorted({r["month"] for r in R if r["date"].isoformat() >= "2026-03-01"})
random.seed(7)

BUCKETS = {
    "SB Absorption (all)":      lambda r: r["setup_name"] == "SB Absorption",
    "SB Absorption LONG":       lambda r: r["setup_name"] == "SB Absorption" and r["is_long"],
    "SB Absorption SHORT":      lambda r: r["setup_name"] == "SB Absorption" and not r["is_long"],
    "VIX Divergence SHORT":     lambda r: r["setup_name"] == "VIX Divergence" and not r["is_long"],
    "VPB SHORT":                lambda r: r["setup_name"] == "Vanna Pivot Bounce" and not r["is_long"],
    # references
    "[ref] Skew Charm (traded)": lambda r: r["setup_name"] == "Skew Charm",
    "[ref] VIX Divergence LONG": lambda r: r["setup_name"] == "VIX Divergence" and r["is_long"],
    "[ref] VPB LONG":            lambda r: r["setup_name"] == "Vanna Pivot Bounce" and r["is_long"],
}


def boot(vals, n=4000):
    """Bootstrap CI on mean points/trade — the honest read on a small sample."""
    if len(vals) < 5:
        return (None, None)
    m = []
    for _ in range(n):
        s = [vals[random.randrange(len(vals))] for _ in range(len(vals))]
        m.append(sum(s) / len(s))
    m.sort()
    return (m[int(n * 0.05)], m[int(n * 0.95)])


print("### 1. the buckets, raw chain points, ungated")
print(f"  {'bucket':<28}{'n':>4}{'WR':>6}{'total':>9}{'pts/t':>8}{'90% CI on pts/t':>22}{'mo+':>6}")
store = {}
for lab, fn in BUCKETS.items():
    sub = [r for r in R if fn(r)]
    if not sub:
        continue
    v = [r["pts"] for r in sub]
    lo, hi = boot(v)
    mo = collections.defaultdict(float)
    for r in sub:
        mo[r["month"]] += r["pts"]
    pos = sum(1 for m in MONTHS if mo.get(m, 0) > 0)
    ci = f"[{lo:+.2f}, {hi:+.2f}]" if lo is not None else "n too small"
    print(f"  {lab:<28}{len(v):>4}{wr(sub):>5.0f}%{sum(v):>+9.1f}{ppt(sub):>+8.2f}{ci:>22}"
          f"{pos:>4}/{len(MONTHS)}")
    store[lab] = sub
print("\n  If the 90% interval includes 0, the bucket is NOT established — it is a small sample")
print("  that happens to be positive.")

print("\n\n### 2. month by month")
print(f"  {'bucket':<28}" + "".join(f"{m[-2:]:>9}" for m in MONTHS))
for lab in BUCKETS:
    if lab not in store:
        continue
    mo = collections.defaultdict(lambda: [0, 0.0])
    for r in store[lab]:
        mo[r["month"]][0] += 1; mo[r["month"]][1] += r["pts"]
    print(f"  {lab:<28}" + "".join(
        f"{mo[m][1]:>+7.0f}({mo[m][0]:>1})" if mo[m][0] else f"{'-':>9}" for m in MONTHS))

print("\n\n### 3. how often do they even fire? (signals per session)")
sess = len({r["date"] for r in R if r["date"].isoformat() >= "2026-03-01"})
for lab in ("SB Absorption (all)", "VIX Divergence SHORT", "VPB SHORT"):
    sub = store.get(lab, [])
    d = len({r["date"] for r in sub})
    print(f"  {lab:<28}{len(sub):>4} signals on {d:>3} of {sess} sessions "
          f"= {len(sub)/sess:.2f}/session   (a trade every {sess/max(len(sub),1):.1f} sessions)")

print("\n\n### 4. quality split — is there a good half inside the bucket?")
for lab in ("SB Absorption (all)", "VIX Divergence SHORT", "VPB SHORT"):
    sub = store.get(lab, [])
    if len(sub) < 20:
        continue
    print(f"\n  -- {lab} ({len(sub)} trades) --")
    for key in ("grade", "paradigm", "hour", "greek_alignment"):
        g = collections.defaultdict(list)
        for r in sub:
            g[r.get(key)].append(r["pts"])
        parts = [f"{k}:{len(v)}t {sum(v)/len(v):+.1f}" for k, v in
                 sorted(g.items(), key=lambda kv: -sum(kv[1])) if len(v) >= 4]
        if parts:
            print(f"     {key:<18}" + "   ".join(parts[:6]))

print("\n\n### 5. TIME OVERLAP — would the concurrency cap have blocked them anyway?")
traded = [r for r in R if r["setup_name"] in
          ("Skew Charm", "AG Short", "ES Absorption", "DD Exhaustion", "VIX Divergence",
           "Vanna Pivot Bounce") and r["date"].isoformat() >= "2026-03-01"]
byday = collections.defaultdict(list)
for r in traded:
    byday[r["date"]].append(r)
for lab in ("SB Absorption (all)", "VIX Divergence SHORT", "VPB SHORT"):
    sub = [r for r in store.get(lab, []) if r["date"].isoformat() >= "2026-03-01"]
    if not sub:
        continue
    near = 0
    for r in sub:
        same = [x for x in byday.get(r["date"], [])
                if abs((x["et"] - r["et"]).total_seconds()) < 1800 and x["is_long"] == r["is_long"]]
        if same:
            near += 1
    print(f"  {lab:<28}{near}/{len(sub)} fire within 30 min of an existing same-direction signal "
          f"({near/len(sub)*100:.0f}% would compete for a cap slot)")
