from engine import *
import pickle, json, statistics, collections
cands=pickle.load(open("cands.pkl","rb"))
try: MESFILL={int(k):v for k,v in json.load(open("mesfill_cache.json")).items()}
except Exception: MESFILL={}
for x in cands:
    if x["mes"] is None and x["id"] in MESFILL: x["mes"]=MESFILL[x["id"]]
bars=load_bars(sorted({x["et"].date() for x in cands}))
for x in cands:
    sp=stop_for(x["setup"],x["il"],x["tsl"],x["spot"],x["sl"])
    tp=x["tl"] if (x["setup"]=="Vanna Pivot Bounce" and x["tl"]) else None
    epts,_,xet=walk(bars[x["et"].date()],x["et"],x["spot"],x["il"],sp,x["setup"],tp)
    x["pts"]= x["mes"] if x["mes"] is not None else epts; x["exit"]=xet
DEAD=0.15;CAP_L,CAP_S=1,2;DLL=300.0
def bs(bp,il):
    if bp is None: return "nodata"
    if abs(bp)<DEAD: return "neutral"
    return "confirm" if ((bp>0)==il) else "contradict"
def run(take,qtyf):
    byday=collections.defaultdict(list)
    for x in cands: byday[x["et"].date()].append(x)
    daily={}
    for d in sorted(byday):
        op=[];rz=0.0;pl=[];dp=0.0
        for x in sorted(byday[d],key=lambda z:z["et"]):
            st=bs(x["bp"],x["il"]);still=[]
            for p in op:
                if p["exit"]<=x["et"]: rz+=p["pnl"]
                else: still.append(p)
            op=still
            if not take(st,x): continue
            if any(s==x["setup"] and dl==x["il"] and (x["et"]-t).total_seconds()<90 for s,dl,t in pl): continue
            if sum(1 for p in op if p["il"]==x["il"])>=(CAP_L if x["il"] else CAP_S): continue
            if rz<=-DLL: continue
            stk=[p for p in op if p["setup"]==x["setup"] and p["il"]==x["il"]]
            if len(stk)>=2:
                sg=1.0 if x["il"] else -1.0
                if sum((x["spot"]-p["spot"])*sg for p in stk)<0: continue
            q=qtyf(st,x); op.append({**x,"pnl":x["pts"]*DOLLAR_PER_PT*q-COMM_PER_CONTRACT*q})
            pl.append((x["setup"],x["il"],x["et"])); dp+=op[-1]["pnl"]
        for p in op: rz+=p["pnl"]
        daily[d]=dp
    return daily
POL={"Baseline V16 1x":(lambda st,x:True,lambda st,x:1),
     "SB gate 1x":(lambda st,x:st!="contradict",lambda st,x:1),
     "SB 0/1/2 (current)":(lambda st,x:st!="contradict",lambda st,x:2 if st=="confirm" else 1),
     "SB confirm-only 2x":(lambda st,x:st in("confirm","nodata"),lambda st,x:2 if st=="confirm" else 1)}
runs={k:run(*v) for k,v in POL.items()}
months=sorted({d.strftime('%Y-%m') for d in runs["Baseline V16 1x"]})
print(f"{'policy':<22}" + "".join(f"{m[-2:]+'/'+m[2:4]:>10}" for m in months) + f"{'TOTAL':>10}")
for k,dl in runs.items():
    mo=collections.defaultdict(float)
    for d,v in dl.items(): mo[d.strftime('%Y-%m')]+=v
    print(f"{k:<22}" + "".join(f"{mo[m]:>10,.0f}" for m in months) + f"{sum(mo.values()):>10,.0f}")
nd=collections.Counter(d.strftime('%Y-%m') for d in runs["Baseline V16 1x"])
print(f"{'(sessions)':<22}" + "".join(f"{nd[m]:>10}" for m in months))
print()
# post-May-15 only (the era TSRT actually ran / current config era)
for k,dl in runs.items():
    dv=[v for d,v in sorted(dl.items()) if d>=__import__('datetime').date(2026,5,15)]
    tot=sum(dv); peak=0;dd=0;c=0
    for v in dv: c+=v;peak=max(peak,c);dd=min(dd,c-peak)
    print(f"  {k:<22} post-05-15: ${tot:>7,.0f} over {len(dv)}d = ${tot/len(dv):>6.1f}/day  MaxDD ${dd:>7,.0f}")
