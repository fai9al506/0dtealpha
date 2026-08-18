# -*- coding: utf-8 -*-
"""V20c — VERIFY the two surprising claims.

A) Why did SB Absorption stop firing?
B) VPB shorts: +0.75 pts/trade but +$819 and a BETTER drawdown. That should not happen for a
   marginal bucket. Decompose it exactly and stress it.
"""
import os, collections, statistics, random
from sqlalchemy import create_engine, text
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

random.seed(11)
rows, gaps = load()
ALL = frozenset(RULES.keys())
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
RELAX = {"Skew Charm", "ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"}
W = ("2026-03-16", "2026-08-07")
POOL = [r for r in rows if r["outcome_pnl"] is not None
        and W[0] <= r["ts"].astimezone(ET).date().isoformat() < W[1]]
REAL = [r for r in POOL if r["setup_name"] in WHITELIST]


def v17(pool):
    out = []
    for r in pool:
        il = r["direction"] in ("long", "bullish")
        sn = r["setup_name"]
        if sn not in RELAX or (r["vix"] or 0) >= 22:
            if passes(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL - KEEP_DD if (sn == "DD Exhaustion" and not il) else ALL
        if passes(r, gaps, use)[0]:
            out.append(r)
    return out


BASE = v17(REAL)
VPBS = [r for r in POOL if r["setup_name"] == "Vanna Pivot Bounce"
        and r["direction"] not in ("long", "bullish")]
SBA = [r for r in POOL if r["setup_name"] == "SB Absorption"]
VIXS = [r for r in POOL if r["setup_name"] == "VIX Divergence"
        and r["direction"] not in ("long", "bullish")]

print("=" * 100)
print("A) WHY DID SB ABSORPTION STOP FIRING?")
print("=" * 100)
E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    print("\n  5-pt range bars produced per session (the detector's raw input):")
    for r in c.execute(text("""
        SELECT to_char(trade_date,'YYYY-MM') m, COUNT(*) bars, COUNT(DISTINCT trade_date) d
        FROM vps_es_range_bars WHERE range_pts=5 AND trade_date >= '2026-03-01'
        GROUP BY 1 ORDER BY 1""")).fetchall():
        print(f"    {r[0]}  {r[1]:>6} bars over {r[2]:>3} sessions = {r[1]/max(r[2],1):>5.0f} bars/session")
    print("\n  SB Absorption signals + the VIX of those months:")
    for r in c.execute(text("""
        SELECT to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM') m, COUNT(*) n,
               ROUND(AVG(vix)::numeric,1) v
        FROM setup_log WHERE setup_name='SB Absorption' GROUP BY 1 ORDER BY 1""")).fetchall():
        print(f"    {r[0]}  {r[1]:>3} signals   avg VIX {r[2]}")
    print("\n  is the whole 5-pt absorption family quiet, or just SB?")
    for r in c.execute(text("""
        SELECT setup_name, to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM') m, COUNT(*)
        FROM setup_log WHERE setup_name IN ('SB Absorption','ES Absorption','SB2 Absorption')
          AND ts>='2026-05-01' GROUP BY 1,2 ORDER BY 1,2""")).fetchall():
        print(f"    {r[0]:<18}{r[1]}  {r[2]:>4}")

print("\n" + "=" * 100)
print("B) VPB SHORTS — decomposing the +$819")
print("=" * 100)
b = sim(BASE, 2, 2, "basket")
cand = sorted(BASE + VPBS, key=lambda r: r["ts"])
s = sim(cand, 2, 2, "basket")
bid = {t["id"]: t for t in b["trade_rows"]}
sid = {t["id"]: t for t in s["trade_rows"]}
vpb_ids = {r["id"] for r in VPBS}
added = [t for i, t in sid.items() if i in vpb_ids]
lost = [t for i, t in bid.items() if i not in sid]
kept_same = [i for i in bid if i in sid]
print(f"\n  baseline ${b['total']:,.0f} ({b['trades']}t, DD ${b['maxdd']:,.0f})"
      f"  ->  with VPB shorts ${s['total']:,.0f} ({s['trades']}t, DD ${s['maxdd']:,.0f})")
print(f"  offered {len(VPBS)} VPB shorts, {len(added)} taken, worth ${sum(t['pnl'] for t in added):+,.0f}")
print(f"  existing trades displaced: {len(lost)}, they were worth ${sum(t['pnl'] for t in lost):+,.0f}"
      f"  (removing them adds ${-sum(t['pnl'] for t in lost):+,.0f})")
resid = (s["total"] - b["total"]) - sum(t["pnl"] for t in added) + sum(t["pnl"] for t in lost)
print(f"  arithmetic: {sum(t['pnl'] for t in added):+,.0f} (added) "
      f"{-sum(t['pnl'] for t in lost):+,.0f} (displaced losers) = "
      f"{sum(t['pnl'] for t in added)-sum(t['pnl'] for t in lost):+,.0f}")
print(f"  actual net {s['total']-b['total']:+,.0f}   -> unexplained residual ${resid:+,.0f}")
# sizing check
sz = collections.Counter(t["qty"] for t in added)
print(f"  size mix of the added VPB shorts: {dict(sz)}  "
      f"(2x means the tech basket confirmed the short)")

print("\n  concentration — is it a few trades?")
srt = sorted(added, key=lambda t: -t["pnl"])
print(f"    top 3 added trades = ${sum(t['pnl'] for t in srt[:3]):,.0f} of ${sum(t['pnl'] for t in added):,.0f}"
      f" ({sum(t['pnl'] for t in srt[:3])/max(sum(t['pnl'] for t in added),1)*100:.0f}%)")
dayd = collections.defaultdict(float)
for d in set(list(b["daily"]) + list(s["daily"])):
    dayd[d] = s["daily"].get(d, 0) - b["daily"].get(d, 0)
top = sorted(dayd.items(), key=lambda kv: -abs(kv[1]))[:6]
print("    biggest day-level changes: " + "  ".join(f"{d.strftime('%m-%d')}:{v:+,.0f}" for d, v in top))
print(f"    days changed at all: {sum(1 for v in dayd.values() if abs(v)>1)} of {len(dayd)}")

print("\n  WHY the drawdown improves — what happened on the drawdown days")
cum = peak = 0.0; trough_d = None; worst = 0.0
for d in sorted(b["daily"]):
    cum += b["daily"][d]; peak = max(peak, cum)
    if cum - peak < worst:
        worst = cum - peak; trough_d = d
print(f"    baseline trough on {trough_d} (DD ${b['maxdd']:,.0f}).")
print(f"    VPB-short contribution on the 10 worst baseline days:")
wd = sorted(b["daily"].items(), key=lambda kv: kv[1])[:10]
tot_bad = 0.0
for d, v in wd:
    delta = s["daily"].get(d, 0) - v
    tot_bad += delta
    print(f"      {d}  baseline {v:>+8,.0f}  ->  {s['daily'].get(d,0):>+8,.0f}   ({delta:>+7,.0f})")
print(f"    total change on the 10 worst days: ${tot_bad:+,.0f}")

print("\n\n  ROBUSTNESS of the VPB-short addition")
print(f"  {'variant':<34}{'baseline':>10}{'+VPB S':>10}{'delta':>9}{'baseDD':>9}{'newDD':>9}")
for lab, cap, sz2 in (("cap 2/2 basket (headline)", 2, "basket"), ("cap 3/3 basket", 3, "basket"),
                      ("cap 2/2 FLAT 1 MES", 2, "flat1"), ("cap 1/1 basket", 1, "basket"),
                      ("cap 4/4 basket", 4, "basket")):
    x = sim(BASE, cap, cap, sz2); y = sim(cand, cap, cap, sz2)
    print(f"  {lab:<34}{x['total']:>10,.0f}{y['total']:>10,.0f}{y['total']-x['total']:>+9,.0f}"
          f"{x['maxdd']:>9,.0f}{y['maxdd']:>9,.0f}")

print("\n  per month")
ms = sorted(b["month"])
print(f"  {'':<20}" + "".join(f"{m[-2:]:>9}" for m in ms))
print(f"  {'baseline':<20}" + "".join(f"{b['month'].get(m,0):>9,.0f}" for m in ms))
print(f"  {'+ VPB shorts':<20}" + "".join(f"{s['month'].get(m,0):>9,.0f}" for m in ms))
print(f"  {'delta':<20}" + "".join(f"{s['month'].get(m,0)-b['month'].get(m,0):>+9,.0f}" for m in ms))

print("\n  day-level bootstrap: resample the 100 sessions 3000x, how often does +VPB win?")
days = sorted(set(list(b["daily"]) + list(s["daily"])))
wins = 0; deltas = []
for _ in range(3000):
    samp = [days[random.randrange(len(days))] for _ in range(len(days))]
    d = sum(s["daily"].get(x, 0) - b["daily"].get(x, 0) for x in samp)
    deltas.append(d)
    wins += 1 if d > 0 else 0
deltas.sort()
print(f"    +VPB shorts is better in {wins/3000*100:.0f}% of resamples   "
      f"90% interval [{deltas[150]:+,.0f}, {deltas[2850]:+,.0f}]")
