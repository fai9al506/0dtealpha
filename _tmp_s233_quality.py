# -*- coding: utf-8 -*-
"""S233 follow-up 2: put V16 trades and re-admitted trades on the SAME scale (points, 1 contract),
then test the user's implication directly -- if V16 picks better trades, is sizing V16 UP better
than adding the extra trades?"""
import collections, statistics
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
ALL = frozenset(RULES.keys())
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]
V16 = [r for r in POOL if passes(r, gaps)[0]]
V16_IDS = {r["id"] for r in V16}
KEEP_DD = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
RELAX = {"Skew Charm", "ES Absorption", "AG Short", "DD Exhaustion", "VIX Divergence"}


def build(vg=22):
    out = []
    for r in POOL:
        il = r["direction"] in ("long", "bullish")
        sn = r["setup_name"]
        if sn not in RELAX or (vg is not None and (r["vix"] or 0) >= vg):
            if passes(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL - KEEP_DD if (sn == "DD Exhaustion" and not il) else ALL
        if passes(r, gaps, use)[0]:
            out.append(r)
    return out


print("### A. SAME SCALE: chain POINTS per trade at ONE contract (no sizing, no commission)")
s16 = sim(V16, 2, 2, "flat1")
srel = sim(build(), 2, 2, "flat1")
new = [t for t in srel["trade_rows"] if t["id"] not in V16_IDS]
kept = [t for t in srel["trade_rows"] if t["id"] in V16_IDS]
for lab, ts in (("V16 trades (the book we run today)", s16["trade_rows"]),
                ("the SAME V16 trades inside the wider book", kept),
                ("the trades V16 blocks (newly admitted)", new)):
    pts = [t["pts"] for t in ts]
    w = [p for p in pts if p > 0]; l = [p for p in pts if p <= 0]
    print(f"  {lab:<42}{len(ts):>5}t  {statistics.mean(pts):>+6.2f} pts/trade   "
          f"WR {len(w)/len(ts)*100:>3.0f}%   avg win {statistics.mean(w):>+6.2f}   "
          f"avg loss {statistics.mean(l):>+6.2f}   total {sum(pts):>+8.1f} pts")

print("\n  => V16 trades are better per trade. The question is by HOW MUCH, and whether")
print("     'better per trade' beats 'more trades' once the trade count is 2x.")

print("\n\n### B. THE DIRECT TEST OF YOUR LOGIC")
print("    If V16 picks the good trades, the right move is to SIZE V16 UP, not add weaker trades.")
print("    Same money at risk per position, three ways:\n")
print(HDR)
print(fmt(sim(V16, 2, 2, "flat1"), "V16, 1 MES flat"))
print(fmt(sim(V16, 2, 2, "basket"), "V16, basket 2x  <- LIVE TODAY"))
print(fmt(sim(V16, 2, 2, "flat2"), "V16, 2 MES flat (size up)"))
print(fmt(sim(build(), 2, 2, "flat1"), "relaxed, 1 MES flat"))
print(fmt(sim(build(), 2, 2, "basket"), "relaxed, basket 2x"))
print(fmt(sim(V16, 3, 3, "flat2"), "V16, 2 MES flat, cap 3/3"))

print("\n\n### C. risk-adjusted: dollars earned per dollar of drawdown")
for lab, c, sz, cap in (("V16 basket 2x (live today)", V16, "basket", 2),
                        ("V16 sized up to 2 MES flat", V16, "flat2", 2),
                        ("V16 2 MES flat, cap 3/3", V16, "flat2", 3),
                        ("relaxed basket 2x", build(), "basket", 2),
                        ("relaxed basket 2x, cap 3/3", build(), "basket", 3)):
    s = sim(c, cap, cap, sz)
    print(f"  {lab:<32}${s['total']:>8,.0f}  DD ${s['maxdd']:>8,.0f}  "
          f"return/DD {s['ret_dd']:>5.1f}  DD {abs(s['maxdd'])/5161*100:>3.0f}% of account  "
          f"green {s['green']}/{s['sessions']}")

print("\n\n### D. why 'worse per trade' still adds money: the LOSS side is what shrinks")
print("    (points at 1 contract, over 100 sessions)")
for lab, ts in (("V16 book", s16["trade_rows"]), ("wider book", srel["trade_rows"])):
    pts = [t["pts"] for t in ts]
    w = [p for p in pts if p > 0]; l = [p for p in pts if p <= 0]
    daily = collections.defaultdict(float)
    for t in ts:
        daily[t["date"]] += t["pts"]
    dv = sorted(daily.values())
    print(f"  {lab:<12}{len(ts):>5}t   total {sum(pts):>+8.1f} pts   worst day {dv[0]:>+7.1f}   "
          f"5 worst days {sum(dv[:5]):>+8.1f}   losing days {sum(1 for v in dv if v<0)}/{len(dv)}")
