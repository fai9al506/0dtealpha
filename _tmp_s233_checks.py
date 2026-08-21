# -*- coding: utf-8 -*-
"""S233 part 12 — targeted checks.

1. ES Absorption SHORTS: the S229 cut removed the ones V16 PASSED. Is the blocked
   population really different, or did I just re-find the same trades?
2. Setups excluded from the real-trade whitelist entirely (BofA Scalp, SB Absorption,
   Paradigm Reversal, Delta Absorption) — free upside or noise?
3. Does basket 2x sizing still pay on a much bigger book?
4. Real commission per round trip from broker truth (tsrt_daily_stmt gross vs net).
"""
import os, collections
from sqlalchemy import create_engine, text
from _tmp_s233_sim import load, sim, fmt, HDR, ET
from _tmp_s233_rules import passes, RULES, WHITELIST

rows, gaps = load()
ALL = frozenset(RULES.keys())
POOL = [r for r in rows if r["setup_name"] in WHITELIST and r["outcome_pnl"] is not None
        and "2026-03-16" <= r["ts"].astimezone(ET).date().isoformat() < "2026-08-07"]

print("### 1. ES Absorption SHORTS -- V16-passing vs V16-blocked (raw, ungated, chain pts)")
# 'passing' = would have passed V16 if the S229 short-cut had never been added
esabs_s = [r for r in POOL if r["setup_name"] == "ES Absorption" and r["direction"] not in ("long", "bullish")]
grp = {"passes V16 w/o S229 cut": [], "blocked by other V16 rules": []}
for r in esabs_s:
    ok, why = passes(r, gaps, frozenset({"ESABS_SHORT"}))
    grp["passes V16 w/o S229 cut" if ok else "blocked by other V16 rules"].append(r)
for k, rs in grp.items():
    if not rs:
        continue
    mo = collections.defaultdict(float)
    for r in rs:
        mo[r["ts"].astimezone(ET).strftime("%m")] += float(r["outcome_pnl"])
    n = len(rs); pts = sum(float(r["outcome_pnl"]) for r in rs)
    wr = sum(1 for r in rs if float(r["outcome_pnl"]) > 0) / n * 100
    print(f"  {k:<28}{n:>5}t  WR {wr:>3.0f}%  {pts:>+8.1f} pts (${pts*5:>+7,.0f})   "
          + " ".join(f"{m}:{v:+.0f}" for m, v in sorted(mo.items())))
print("  -> the S229 cut and this study are talking about DIFFERENT trades if the two rows differ")

print("\n### 2. setups OUTSIDE the real-trade whitelist (raw chain, ungated)")
E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    ex = c.execute(text("""
        SELECT setup_name, to_char(ts AT TIME ZONE 'America/New_York','MM') m,
               COUNT(*) n, SUM(outcome_pnl) pts,
               AVG(CASE WHEN outcome_pnl>0 THEN 1.0 ELSE 0 END)*100 wr
        FROM setup_log
        WHERE ts >= '2026-03-16' AND ts < '2026-08-07' AND outcome_pnl IS NOT NULL
          AND setup_name IN ('BofA Scalp','SB Absorption','Paradigm Reversal','Delta Absorption',
                             'SB2 Absorption','Dip-Buy','Dip-Buy v2','GEX Long')
        GROUP BY 1,2 ORDER BY 1,2""")).fetchall()
agg = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0.0]))
for sn, m, n, pts, wr in ex:
    agg[sn][m] = [n, float(pts or 0)]
mm = sorted({m for v in agg.values() for m in v})
print(f"  {'setup':<20}{'n':>5}{'pts':>9}{'$@1MES':>9}{'pts/t':>7}   " + " ".join(f"{m:>7}" for m in mm))
for sn, v in sorted(agg.items(), key=lambda kv: -sum(x[1] for x in kv[1].values())):
    n = sum(x[0] for x in v.values()); pts = sum(x[1] for x in v.values())
    print(f"  {sn:<20}{n:>5}{pts:>9,.1f}{pts*5:>9,.0f}{pts/max(n,1):>7.2f}   "
          + " ".join(f"{v.get(m,[0,0])[1]:>+7,.0f}" for m in mm))

print("\n### 3. does basket 2x still pay on the big relaxed book? (cap 3/3)")


def build(vg=22, dd_v13=True):
    keep = frozenset({"V13BULL", "V13VANNA", "V13DDQ", "SCDD_SHORT_GEXLIS"})
    out = []
    for r in POOL:
        il = r["direction"] in ("long", "bullish")
        if (r["vix"] or 0) >= vg:
            if passes(r, gaps)[0]:
                out.append(r)
            continue
        use = ALL - keep if (dd_v13 and r["setup_name"] == "DD Exhaustion" and not il) else ALL
        if passes(r, gaps, use)[0]:
            out.append(r)
    return out


c = build()
print(HDR)
for sz in ("flat1", "basket", "flat2"):
    for cap in (2, 3):
        print(fmt(sim(c, cap_l=cap, cap_s=cap, sizing=sz), f"V17 recommended {sz} cap{cap}"))

print("\n### 4. REAL commission per round trip (broker truth, tsrt_daily_stmt)")
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
    r = conn.execute(text("""SELECT SUM(gross), SUM(net), SUM(n_trades), COUNT(*)
                             FROM tsrt_daily_stmt""")).fetchone()
    print(f"  all days: gross ${float(r[0] or 0):,.2f}  net ${float(r[1] or 0):,.2f}  "
          f"round-trips {r[2]}  days {r[3]}")
    if r[2]:
        print(f"  => commission+fees = ${(float(r[0])-float(r[1]))/r[2]:,.2f} per round trip "
              f"(study assumes $1.00/contract)")
