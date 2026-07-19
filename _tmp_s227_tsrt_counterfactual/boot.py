from engine import *
from sqlalchemy import text
import random, statistics
random.seed(42)
CAP_L,CAP_S=1,2; DLL=300.0
with conn() as c:
    rows=c.execute(text("""
      SELECT s.id,(s.ts AT TIME ZONE 'America/New_York') et,s.setup_name,s.direction,s.spot,
             s.outcome_stop_level,s.outcome_target_level,s.trail_sl,s.mes_sim_outcome_pnl,s.basket_pct
      FROM setup_log s WHERE s.ts>='2026-07-01' AND s.real_trade_skip_reason='master_kill'
      ORDER BY s.ts""")).fetchall()
bars=load_bars(sorted({r[1].date() for r in rows}))
byday=defaultdict(list)
for r in rows: byday[r[1].date()].append(r)
daily={}; qty_counts=defaultdict(int); trades=[]
for d in sorted(byday):
    open_pos=[];realized=0.0;placed=[];dayp=0.0
    for (lid,et,setup,direction,spot,sl,tl,tsl,mp,bp) in byday[d]:
        il=direction.lower() in ("long","bullish")
        still=[]
        for p in open_pos:
            if p["exit_et"]<=et: realized+=p["pnl"]
            else: still.append(p)
        open_pos=still
        if any(s==setup and dl==il and (et-t).total_seconds()<90 for s,dl,t in placed): continue
        if sum(1 for p in open_pos if p["is_long"]==il)>=(CAP_L if il else CAP_S): continue
        if realized<=-DLL: continue
        stack=[p for p in open_pos if p["setup"]==setup and p["is_long"]==il]
        if len(stack)>=2:
            sgn=1.0 if il else -1.0
            if sum((float(spot)-p["entry"])*sgn for p in stack)<0: continue
        sp=stop_for(setup,il,tsl,float(spot),float(sl) if sl else None)
        tp=float(tl) if (setup=="Vanna Pivot Bounce" and tl) else None
        epts,_,xet=walk(bars[d],et,float(spot),il,sp,setup,tp)
        pts=float(mp) if mp is not None else epts
        qty=2 if (bp is not None and abs(float(bp))>=0.15 and ((float(bp)>0)==il)) else 1
        qty_counts[qty]+=1
        pnl=pts*DOLLAR_PER_PT*qty-COMM_PER_CONTRACT*qty
        open_pos.append({"setup":setup,"is_long":il,"entry":float(spot),"exit_et":xet,"pnl":pnl})
        placed.append((setup,il,et)); dayp+=pnl; trades.append(pnl)
    daily[d]=dayp
print("=== BASKET 2x ALREADY IN THE SIM? ===")
print(f"qty=1 trades: {qty_counts[1]}   qty=2 (basket-confirmed) trades: {qty_counts[2]}")
print(f"total contracts: {qty_counts[1]+2*qty_counts[2]} across {qty_counts[1]+qty_counts[2]} trades")
# what if sizing were OFF (all 1x)?
print()
dv=list(daily.values()); N=len(dv); tot=sum(dv)
print(f"=== DAILY $ (n={N}) ===")
print(f"total ${tot:,.0f}  mean/day ${statistics.mean(dv):+,.1f}  stdev ${statistics.stdev(dv):,.1f}")
# bootstrap over days -> 21-day month
sims=[]
for _ in range(20000):
    sims.append(sum(random.choice(dv) for _ in range(21)))
sims.sort()
print(f"\n=== BOOTSTRAP: 21-session month, resampling these 12 days ===")
for p in (5,25,50,75,95):
    print(f"  p{p:<3} ${sims[int(len(sims)*p/100)]:>8,.0f}")
print(f"  P(losing month) = {sum(1 for s in sims if s<0)/len(sims)*100:.0f}%")
# bootstrap over trades
ts=[]
for _ in range(20000):
    ts.append(sum(random.choice(trades) for _ in range(len(trades))))
ts.sort()
print(f"\n=== BOOTSTRAP: same 58-trade sample, 90% CI on the ${tot:,.0f} ===")
print(f"  p5 ${ts[1000]:,.0f}   p50 ${ts[10000]:,.0f}   p95 ${ts[19000]:,.0f}")
print(f"  P(this window was actually negative) = {sum(1 for s in ts if s<0)/len(ts)*100:.0f}%")
