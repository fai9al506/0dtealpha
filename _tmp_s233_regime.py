# -*- coding: utf-8 -*-
"""S233 part 2 — is the V16 filter's value REGIME-dependent?

Tests the simplest possible relaxation: a single VIX switch (filter ON above the
threshold, OFF below) vs always-on / always-off. One parameter = hard to overfit.
Also reports per-month and per-VIX-bucket behaviour of both books.
"""
import sys, collections, statistics
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

CAP = int(sys.argv[1]) if len(sys.argv) > 1 else 2
SIZING = sys.argv[2] if len(sys.argv) > 2 else "basket"
START, END = "2026-03-16", "2026-08-07"
ALL = frozenset(RULES.keys())

rows, gaps = load()
pool = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and START <= r["ts"].astimezone(ET).date().isoformat() < END]

# daily VIX = first non-null vix seen that session (proxy for the regime known AT the open)
day_vix = {}
for r in sorted(pool, key=lambda x: x["ts"]):
    d = r["ts"].astimezone(ET).date()
    if d not in day_vix and r["vix"]:
        day_vix[d] = float(r["vix"])

print(f"### S233 regime test | {START} -> {END} | cap {CAP}/{CAP} | sizing={SIZING}")
print(f"daily open-VIX: min {min(day_vix.values()):.1f}  median {statistics.median(day_vix.values()):.1f}"
      f"  max {max(day_vix.values()):.1f}  sessions {len(day_vix)}")

on_c = [r for r in pool if passes(r, gaps)[0]]
off_c = [r for r in pool if passes(r, gaps, ALL)[0]]
base = sim(on_c, CAP, CAP, SIZING)
nof = sim(off_c, CAP, CAP, SIZING)

print("\n" + HDR)
print(fmt(base, "V16 always ON"))
print(fmt(nof, "V16 always OFF"))

print("\n\n### A. VIX switch: filter ON when open-VIX >= T, OFF below")
print(HDR)
best = None
for T in (0, 16, 17, 18, 19, 20, 21, 22, 24, 99):
    c = []
    for r in pool:
        d = r["ts"].astimezone(ET).date()
        hi = day_vix.get(d, 99) >= T
        if passes(r, gaps, frozenset() if hi else ALL)[0]:
            c.append(r)
    s = sim(c, CAP, CAP, SIZING)
    lab = f"T={T}" + ("  (=always OFF)" if T >= 99 else "  (=always ON)" if T == 0 else "")
    print(fmt(s, f"VIX switch {lab}"))
    if best is None or s["total"] > best[1]["total"]:
        best = (T, s)
print(f"\n  best threshold {best[0]} -> ${best[1]['total']:,.0f} (MaxDD ${best[1]['maxdd']:,.0f})")

print("\n\n### B. per-VIX-bucket, both books (bucket = open-VIX of the session)")
BUCKETS = [(0, 17, "VIX <17"), (17, 19, "17-19"), (19, 21, "19-21"), (21, 24, "21-24"), (24, 99, "24+")]
print(f"  {'bucket':<10}{'sess':>5} | {'V16 ON $':>10}{'trades':>8}{'WR':>5} | {'NO filter $':>12}{'trades':>8}{'WR':>5} | {'diff':>9}")
for lo, hi, lab in BUCKETS:
    days = {d for d, v in day_vix.items() if lo <= v < hi}
    if not days:
        continue
    a = sim([r for r in on_c if r["ts"].astimezone(ET).date() in days], CAP, CAP, SIZING)
    b = sim([r for r in off_c if r["ts"].astimezone(ET).date() in days], CAP, CAP, SIZING)
    print(f"  {lab:<10}{len(days):>5} | {a['total']:>10,.0f}{a['trades']:>8}{a['wr']:>4.0f}% | "
          f"{b['total']:>12,.0f}{b['trades']:>8}{b['wr']:>4.0f}% | {b['total']-a['total']:>+9,.0f}")
print("  (buckets are simulated INDEPENDENTLY: daily breaker/cap reset per day, so day-sets are additive)")

print("\n\n### C. per-month, both books")
print(f"  {'month':<9}{'sess':>5} | {'V16 ON':>9}{'DD':>8} | {'NO filter':>10}{'DD':>8} | {'diff':>9}  avg VIX")
for m in sorted(base["month"]):
    days = {d for d in day_vix if d.strftime("%Y-%m") == m}
    a = sim([r for r in on_c if r["ts"].astimezone(ET).date() in days], CAP, CAP, SIZING)
    b = sim([r for r in off_c if r["ts"].astimezone(ET).date() in days], CAP, CAP, SIZING)
    v = statistics.mean([day_vix[d] for d in days])
    print(f"  {m:<9}{len(days):>5} | {a['total']:>9,.0f}{a['maxdd']:>8,.0f} | {b['total']:>10,.0f}"
          f"{b['maxdd']:>8,.0f} | {b['total']-a['total']:>+9,.0f}  {v:>6.1f}")
