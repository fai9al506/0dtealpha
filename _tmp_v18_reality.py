# -*- coding: utf-8 -*-
"""V18 step 4 — REALITY CHECK. Every number in this study is a chain simulation.
What did the broker ACTUALLY pay in the same window, on the same config?"""
import os, collections
from sqlalchemy import create_engine, text
from _tmp_v18_data import ET
from _tmp_s233_sim import sim
from _tmp_s233_rules import passes as v16pass, RULES
from _tmp_v18_engine import ALLR, gaps

E = create_engine(os.environ["DATABASE_URL"])
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    stmt = c.execute(text("""SELECT day, gross, net, n_trades, n_wins
                             FROM tsrt_daily_stmt ORDER BY day""")).fetchall()

print("### A. broker truth — every day TSRT actually traded (tsrt_daily_stmt)")
mo = collections.defaultdict(lambda: [0.0, 0.0, 0, 0])
for d, g, n, nt, nw in stmt:
    k = d.strftime("%Y-%m")
    mo[k][0] += float(g or 0); mo[k][1] += float(n or 0); mo[k][2] += int(nt or 0); mo[k][3] += 1
print(f"  {'month':<9}{'sessions':>9}{'trades':>8}{'gross':>10}{'NET':>10}")
tn = ts = 0
for k in sorted(mo):
    v = mo[k]
    print(f"  {k:<9}{v[3]:>9}{v[2]:>8}{v[0]:>10,.0f}{v[1]:>10,.0f}")
    tn += v[1]; ts += v[3]
print(f"  {'TOTAL':<9}{ts:>9}{sum(v[2] for v in mo.values()):>8}"
      f"{sum(v[0] for v in mo.values()):>10,.0f}{tn:>10,.0f}")
print(f"\n  => broker reality: ${tn:,.0f} net over {ts} live sessions = "
      f"${tn/(ts/21):,.0f} per month at the size actually traded")

print("\n### B. the same window, as the chain simulation scores it")
days = {d for d, *_ in stmt}
pool = [r for r in ALLR if r["date"] in days and r["setup_name"] in
        ("Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption", "DD Exhaustion", "VIX Divergence")]
v16 = [r for r in pool if v16pass(r, gaps)[0]]
s = sim(v16, 2, 2, "basket")
sh = sim(v16, 2, 2, "basket", haircut=0.81)
print(f"  chain sim on the SAME {len(days)} days, V16, cap 2/2, basket 2x:")
print(f"    chain            ${s['total']:>8,.0f}   ({s['trades']} trades)")
print(f"    chain x0.81      ${sh['total']:>8,.0f}")
print(f"    BROKER ACTUAL    ${tn:>8,.0f}   ({sum(v[2] for v in mo.values())} round trips)")
if s["total"]:
    print(f"\n  realised capture = {tn/s['total']*100:.0f}% of the chain simulation "
          f"(the per-trade haircut alone predicted 81%)")
print("\n  NOTE: the live era includes real incidents (the June auto-roll episode, manual")
print("  flattens, days TSRT was disabled). This ratio is the number every projection in")
print("  this study should be multiplied by before it is believed.")
