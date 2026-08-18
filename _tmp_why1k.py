import os, sys, importlib.util, io, contextlib, collections
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
m=importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(m)

# 1. did the filtering actually add value? unfiltered vs V16 base, same gates
from sqlalchemy import text
from datetime import timedelta
WL=m.WL
with m.E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    allrows=c.execute(text(f"""SELECT {m.COLS}, outcome_pnl, outcome_elapsed_min, spot
        FROM setup_log WHERE ts>=:a AND ts<:b ORDER BY ts"""),{"a":m.START,"b":m.END}).mappings().all()
raw=[r for r in allrows if r["setup_name"] in WL and r["outcome_pnl"] is not None]
print(f"=== {m.START} -> {m.END} ===")
print(f"  ALL signals from the traded setups (no V16 filter): {len(raw)}  ->  {sum(float(r['outcome_pnl']) for r in raw):+,.0f} pts"
      f" = ${sum(float(r['outcome_pnl']) for r in raw)*5:+,.0f} @1MES ungated")
print(f"  after V16 base filter: {len(m.cands)}  ->  {sum(float(r['outcome_pnl']) for r in m.cands):+,.0f} pts")

# 2. monthly, shipped config
r=m.run("base012",2,2)
mo=collections.defaultdict(float); ses=collections.Counter()
for d,v in r["daily"].items(): mo[d.strftime('%Y-%m')]+=v; ses[d.strftime('%Y-%m')]+=1
print(f"\n  monthly (shipped config, 1 MES, x0.81 capture applied):")
for k in sorted(mo):
    print(f"    {k}   {ses[k]:>2} sessions   ${mo[k]*0.81:>+8,.0f}   (${mo[k]*0.81/ses[k]*21:>+7,.0f}/mo rate)")

# 3. the edge in POINTS - is the strategy weak, or is the size small?
tot_pts=sum(float(x["outcome_pnl"]) for x in m.cands)
nses=len(r["daily"])
print(f"\n  EDGE IN POINTS (what the strategy actually produces, size-independent):")
print(f"    {len(m.cands)} signals / {nses} sessions = {len(m.cands)/nses:.1f} signals per day")
print(f"    gross {tot_pts:+,.0f} SPX pts over {nses} sessions = {tot_pts/nses:+.1f} pts/session")
print(f"    shipped config captures ${r['total']:,.0f} = {r['total']/5:,.0f} MES-pts over {nses} sessions"
      f" = {r['total']/5/nses:.1f} pts/session")
print(f"\n  same edge, different size (x0.81 capture, per month):")
for lbl,mult in [("1 MES  ($5/pt)",1),("3 MES",3),("5 MES",5),("1 ES  ($50/pt)",10),("3 ES",30)]:
    print(f"    {lbl:<16} ${r['total']*0.81/nses*21*mult:>9,.0f}/mo    MaxDD ${r['dd']*mult:>9,.0f}")
