# -*- coding: utf-8 -*-
"""S233 part 3 — is each rule's cost STABLE across months, or one-window luck?

For every rule: leave-one-out delta computed INDEPENDENTLY inside each month
(each month simulated as its own book, so daily breaker/cap behave normally).
A rule is only a relaxation candidate if it costs money in most months, not one.
"""
import sys, collections
from _tmp_s233_sim import load, sim, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

CAP = int(sys.argv[1]) if len(sys.argv) > 1 else 2
SIZING = sys.argv[2] if len(sys.argv) > 2 else "basket"
START, END = "2026-03-16", "2026-08-07"

rows, gaps = load()
pool = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and START <= r["ts"].astimezone(ET).date().isoformat() < END]
months = sorted({r["ts"].astimezone(ET).strftime("%Y-%m") for r in pool})


def by_month(off):
    out = {}
    for m in months:
        c = [r for r in pool if r["ts"].astimezone(ET).strftime("%Y-%m") == m and passes(r, gaps, off)[0]]
        out[m] = sim(c, CAP, CAP, SIZING)
    return out


base = by_month(frozenset())
print(f"### S233 per-rule LOO by month | cap {CAP}/{CAP} | sizing={SIZING} | {START}->{END}")
print("    dLOO > 0 = removing the rule that month would have MADE money\n")
hdr = f"  {'rule':<20}" + "".join(f"{m[-2:]:>8}" for m in months) + f"{'TOTAL':>9}{'mo+':>5}{'mo-':>5}"
print(hdr)
print(f"  {'V16 baseline $':<20}" + "".join(f"{base[m]['total']:>8,.0f}" for m in months)
      + f"{sum(base[m]['total'] for m in months):>9,.0f}")
print("  " + "-" * (len(hdr) - 2))

res = []
for rule in sorted(RULES):
    loo = by_month(frozenset([rule]))
    deltas = {m: loo[m]["total"] - base[m]["total"] for m in months}
    tot = sum(deltas.values())
    if all(abs(v) < 1 for v in deltas.values()):
        continue
    pos = sum(1 for v in deltas.values() if v > 1)
    neg = sum(1 for v in deltas.values() if v < -1)
    res.append((rule, deltas, tot, pos, neg))

for rule, deltas, tot, pos, neg in sorted(res, key=lambda x: -x[2]):
    print(f"  {rule:<20}" + "".join(f"{deltas[m]:>+8,.0f}" for m in months)
          + f"{tot:>+9,.0f}{pos:>5}{neg:>5}")

print("\n  mo+ = months where removing helps, mo- = months where removing hurts")
print("  RELAXATION CANDIDATES = total > 0 AND mo+ >= 3 AND mo+ > mo-")
print("  KEEP = total < 0 (rule earns its keep)")
