"""Validate engine+mes layer against EVERY TSRT-enabled session (full broker history)."""
from engine import *
from sqlalchemy import text
import statistics, json
try: MESFILL={int(k):v for k,v in json.load(open('mesfill_cache.json')).items()}
except Exception: MESFILL={}
with conn() as c:
    rows=c.execute(text("""
      SELECT s.id,(s.ts AT TIME ZONE 'America/New_York') et,s.setup_name,s.direction,s.spot,
             s.outcome_stop_level,s.outcome_target_level,s.trail_sl,s.outcome_pnl,
             s.mes_sim_outcome_pnl,s.basket_pct
      FROM real_trade_orders o JOIN setup_log s ON s.id=o.setup_log_id
      ORDER BY s.ts""")).fetchall()
    stmt={r[0]:float(r[1]) for r in c.execute(text("SELECT day,net FROM tsrt_daily_stmt ORDER BY day"))}
print(f"placed trades in DB: {len(rows)}  (broker sessions: {len(stmt)})")
bars=load_bars(sorted({r[1].date() for r in rows}))
daily=defaultdict(float); dn=defaultdict(int); mes_cov=defaultdict(lambda:[0,0])
for (lid,et,setup,direction,spot,sl,tl,tsl,cp,mp,bp) in rows:
    il=direction.lower() in ("long","bullish")
    sp=stop_for(setup,il,tsl,float(spot),float(sl) if sl else None)
    tp=float(tl) if (setup=="Vanna Pivot Bounce" and tl) else None
    epts,_,_=walk(bars[et.date()],et,float(spot),il,sp,setup,tp)
    pts=float(mp) if mp is not None else (MESFILL[lid] if lid in MESFILL else epts)
    qty=2 if (bp is not None and abs(float(bp))>=0.15 and ((float(bp)>0)==il)) else 1
    d=et.date(); daily[d]+=pts*DOLLAR_PER_PT*qty-COMM_PER_CONTRACT*qty; dn[d]+=1
    mes_cov[d][0]+=1; mes_cov[d][1]+= 1 if (mp is not None or lid in MESFILL) else 0
common=sorted(set(daily)&set(stmt))
print(f"\n{'date':<12}{'sim$':>9}{'broker$':>9}{'diff':>8}{'n':>4}{'mes':>5}")
diffs=[]
for d in common:
    diffs.append(daily[d]-stmt[d])
    print(f"{str(d):<12}{daily[d]:>9.0f}{stmt[d]:>9.0f}{daily[d]-stmt[d]:>8.0f}{dn[d]:>4}{mes_cov[d][1]:>5}")
ts=sum(daily[d] for d in common); tb=sum(stmt[d] for d in common)
print(f"\nSIM total ${ts:,.0f}   BROKER total ${tb:,.0f}   diff ${ts-tb:,.0f}")
print(f"sessions {len(common)}   mean daily diff ${statistics.mean(diffs):+,.1f}   mean ABS diff ${statistics.mean(abs(x) for x in diffs):,.0f}")
print(f"median abs diff ${statistics.median(abs(x) for x in diffs):,.0f}")
import math
n=len(diffs); se=statistics.stdev(diffs)/math.sqrt(n)
print(f"bias t-stat = {statistics.mean(diffs)/se:+.2f}  (|t|<2 => no significant systematic bias)")
sd=[stmt[d] for d in common]; si=[daily[d] for d in common]
mx=sum((a-statistics.mean(si))*(b-statistics.mean(sd)) for a,b in zip(si,sd))
print(f"correlation sim vs broker = {mx/math.sqrt(sum((a-statistics.mean(si))**2 for a in si)*sum((b-statistics.mean(sd))**2 for b in sd)):.3f}")
print(f"sign agreement (both green or both red): {sum(1 for d in common if (daily[d]>0)==(stmt[d]>0))}/{len(common)}")
