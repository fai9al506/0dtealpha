"""Compare basket policies on the long V16 window. MES layer where available, SPX engine fallback."""
from engine import *
import pickle, statistics, math, sys, json
cands=pickle.load(open("cands.pkl","rb"))
try: MESFILL={int(k):v for k,v in json.load(open("mesfill_cache.json")).items()}
except Exception: MESFILL={}
for x in cands:
    if x["mes"] is None and x["id"] in MESFILL: x["mes"]=MESFILL[x["id"]]
bars=load_bars(sorted({x["et"].date() for x in cands}))
DEAD=0.15; CAP_L,CAP_S=1,2; DLL=300.0

# precompute outcome pts + exit time once
for x in cands:
    sp=stop_for(x["setup"],x["il"],x["tsl"],x["spot"],x["sl"])
    tp=x["tl"] if (x["setup"]=="Vanna Pivot Bounce" and x["tl"]) else None
    epts,_,xet=walk(bars[x["et"].date()],x["et"],x["spot"],x["il"],sp,x["setup"],tp)
    x["pts"]= x["mes"] if x["mes"] is not None else epts
    x["exit"]=xet; x["engine_only"]= x["mes"] is None

def basket_state(bp,il):
    if bp is None: return "nodata"
    if abs(bp)<DEAD: return "neutral"
    return "confirm" if ((bp>0)==il) else "contradict"

def run(name, take, qtyf):
    byday=defaultdict(list)
    for x in cands: byday[x["et"].date()].append(x)
    daily={}; NT=0; NW=0; ncontracts=0
    for d in sorted(byday):
        open_pos=[];realized=0.0;placed=[];dayp=0.0
        for x in sorted(byday[d],key=lambda z:z["et"]):
            st=basket_state(x["bp"],x["il"])
            still=[]
            for p in open_pos:
                if p["exit"]<=x["et"]: realized+=p["pnl"]
                else: still.append(p)
            open_pos=still
            if not take(st,x): continue
            if any(s==x["setup"] and dl==x["il"] and (x["et"]-t).total_seconds()<90 for s,dl,t in placed): continue
            if sum(1 for p in open_pos if p["il"]==x["il"])>=(CAP_L if x["il"] else CAP_S): continue
            if realized<=-DLL: continue
            stack=[p for p in open_pos if p["setup"]==x["setup"] and p["il"]==x["il"]]
            if len(stack)>=2:
                sgn=1.0 if x["il"] else -1.0
                if sum((x["spot"]-p["spot"])*sgn for p in stack)<0: continue
            q=qtyf(st,x)
            pnl=x["pts"]*DOLLAR_PER_PT*q-COMM_PER_CONTRACT*q
            open_pos.append({**x,"pnl":pnl}); placed.append((x["setup"],x["il"],x["et"]))
            NT+=1; NW+= 1 if x["pts"]>0 else 0; ncontracts+=q; dayp+=pnl
        for p in open_pos: realized+=p["pnl"]
        daily[d]=dayp
    dv=[daily[d] for d in sorted(daily)]
    tot=sum(dv); peak=0;dd=0;c2=0
    for v in dv: c2+=v;peak=max(peak,c2);dd=min(dd,c2-peak)
    g=sum(1 for v in dv if v>0)
    perday=tot/len(dv)
    se=statistics.stdev(dv)/math.sqrt(len(dv))
    print(f"{name:<34}{NT:>5}{ncontracts:>6}{tot:>9,.0f}{perday:>8.1f}{NW/NT*100 if NT else 0:>6.0f}%{dd:>9,.0f}{g:>4}/{len(dv)}{tot/se/len(dv) if se else 0:>7.2f}")
    return daily,dv

print(f"V16 window {min(x['et'].date() for x in cands)} -> {max(x['et'].date() for x in cands)}")
print(f"engine-only (no MES) candidates: {sum(1 for x in cands if x['engine_only'])}/{len(cands)}\n")
print(f"{'policy':<34}{'n':>5}{'ctr':>6}{'$':>9}{'$/day':>8}{'WR':>7}{'MaxDD':>9}{'green':>8}{'t':>7}")
res={}
res['base']=run("1. Baseline V16 (no basket, 1x)", lambda st,x: True, lambda st,x: 1)
res['011']=run("2. SB gate only (block contra, 1x)", lambda st,x: st!="contradict", lambda st,x: 1)
res['012']=run("3. SB 0/1/2 CURRENT (contra blk, 2x)", lambda st,x: st!="contradict", lambda st,x: 2 if st=="confirm" else 1)
res['001']=run("4. SB 0/0/1 (confirm only, 1x)", lambda st,x: st=="confirm" or st=="nodata", lambda st,x: 1)
res['002']=run("5. SB confirm only, 2x", lambda st,x: st=="confirm" or st=="nodata", lambda st,x: 2 if st=="confirm" else 1)
pickle.dump(res,open("policy_res.pkl","wb"))
