"""Apples-to-apples: V16 filter ON vs OFF, SAME gates (dedup/cap/breaker/underwater), same sizing."""
import os, sys, collections, statistics
from sqlalchemy import create_engine, text
from datetime import timedelta
from zoneinfo import ZoneInfo
from app.live_filter import passes_v16, load_gaps, COLS
ET=ZoneInfo("America/New_York")
START=os.environ.get("CF_START","2026-06-01"); END="2026-08-07"
WL={"Skew Charm","AG Short","Vanna Pivot Bounce","ES Absorption","DD Exhaustion","VIX Divergence"}
E=create_engine(os.environ["DATABASE_URL"]); DPP,COMM,DEAD=5.0,1.0,0.15
with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    gaps=load_gaps(c)
    rows=c.execute(text(f"""SELECT {COLS}, outcome_pnl, outcome_elapsed_min, spot
        FROM setup_log WHERE ts>=:a AND ts<:b ORDER BY ts"""),{"a":START,"b":END}).mappings().all()
pool=[r for r in rows if r["setup_name"] in WL and r["outcome_pnl"] is not None]

def run(use_filter, cap):
    byday=collections.defaultdict(list)
    for r in pool:
        if use_filter and not passes_v16(r,gaps): continue
        byday[r["ts"].astimezone(ET).date()].append(r)
    daily={}; NT=NW=0
    for d in sorted(byday):
        openp=[];realized=0.0;placed=[];dayp=0.0
        for r in byday[d]:
            et=r["ts"].astimezone(ET); il=r["direction"] in ("long","bullish")
            still=[]
            for p in openp:
                if p["exit"]<=et: realized+=p["pnl"]
                else: still.append(p)
            openp=still
            if any(s==r["setup_name"] and dl==il and (et-t).total_seconds()<90 for s,dl,t in placed): continue
            if sum(1 for p in openp if p["is_long"]==il)>=cap: continue
            if realized<=-300: continue
            stack=[p for p in openp if p["setup"]==r["setup_name"] and p["is_long"]==il]
            if len(stack)>=2:
                sgn=1.0 if il else -1.0
                if sum((float(r["spot"])-p["entry"])*sgn for p in stack)<0: continue
            bp=r["basket_pct"]
            qty=2 if (bp is not None and abs(float(bp))>=DEAD and ((float(bp)>0)==il)) else 1
            pts=float(r["outcome_pnl"]); pnl=pts*DPP*qty-COMM*qty
            openp.append({"setup":r["setup_name"],"is_long":il,"entry":float(r["spot"]),
                          "exit":et+timedelta(minutes=int(r["outcome_elapsed_min"] or 60)),"pnl":pnl})
            placed.append((r["setup_name"],il,et)); NT+=1; NW+=1 if pts>0 else 0; dayp+=pnl
        for p in openp: realized+=p["pnl"]
        daily[d]=dayp
    peak=dd=cum=0
    for d in sorted(daily):
        cum+=daily[d]; peak=max(peak,cum); dd=min(dd,cum-peak)
    return sum(daily.values()),NT,NW/max(NT,1)*100,dd,len(daily),daily

print(f"=== {START} -> {END}   same gates, same sizing, only the V16 filter differs ===")
print(f"  {'':<22}{'total$':>9}{'trades':>8}{'WR':>6}{'MaxDD':>9}{'$/trade':>9}")
for cap in (2,3):
    for uf,lab in [(True,f"V16 filter ON  cap{cap}"),(False,f"NO filter      cap{cap}")]:
        t,n,wr,dd,ns,_dl=run(uf,cap)
        print(f"  {lab:<22}{t:>9,.0f}{n:>8}{wr:>5.0f}%{dd:>9,.0f}{t/max(n,1):>9.1f}")

import collections
print()
print("  MONTHLY, cap 2/2 (does 'no filter' win every month, or one period?)")
_,_,_,_,_,dON=run(True,2)
_,_,_,_,_,dOFF=run(False,2)
mo=collections.defaultdict(lambda:[0.0,0.0]); ses=collections.Counter()
for d,v in dON.items():
    mo[d.strftime('%Y-%m')][0]+=v; ses[d.strftime('%Y-%m')]+=1
for d,v in dOFF.items():
    mo[d.strftime('%Y-%m')][1]+=v
print(f"    {'month':<9}{'sess':>5}{'V16 ON':>10}{'NO filter':>11}{'diff':>10}")
for k in sorted(mo):
    a,b=mo[k]
    print(f"    {k:<9}{ses[k]:>5}{a:>10,.0f}{b:>11,.0f}{b-a:>+10,.0f}")
