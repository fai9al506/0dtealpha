"""Policy comparison on LIVE SB data only (2026-06-11 -> 2026-07-17, 1-min real-time capture)."""
from engine import *
import pickle, json, statistics, collections, math, datetime
cands=pickle.load(open("cands.pkl","rb"))
try: MESFILL={int(k):v for k,v in json.load(open("mesfill_cache.json")).items()}
except Exception: MESFILL={}
for x in cands:
    if x["mes"] is None and x["id"] in MESFILL: x["mes"]=MESFILL[x["id"]]
START=datetime.date(2026,6,11)
cands=[x for x in cands if x["et"].date()>=START]
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
    daily={};NT=0;NW=0;ctr=0
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
            pl.append((x["setup"],x["il"],x["et"])); dp+=op[-1]["pnl"]; NT+=1; ctr+=q
            NW+= 1 if x["pts"]>0 else 0
        for p in op: rz+=p["pnl"]
        daily[d]=dp
    dv=[daily[d] for d in sorted(daily)]
    tot=sum(dv);peak=0;dd=0;c=0
    for v in dv: c+=v;peak=max(peak,c);dd=min(dd,c-peak)
    return dict(n=NT,ctr=ctr,tot=tot,perday=tot/len(dv),wr=NW/NT*100 if NT else 0,
                dd=dd,green=sum(1 for v in dv if v>0),nd=len(dv),dv=dv)
POL=[("Baseline V16 1x (no basket)",lambda st,x:True,lambda st,x:1),
     ("SB gate only, 1x",lambda st,x:st!="contradict",lambda st,x:1),
     ("SB 0/1/2  <- CURRENT LIVE",lambda st,x:st!="contradict",lambda st,x:2 if st=="confirm" else 1),
     ("SB confirm-only 1x",lambda st,x:st in("confirm","nodata"),lambda st,x:1),
     ("SB confirm-only 2x",lambda st,x:st in("confirm","nodata"),lambda st,x:2 if st=="confirm" else 1)]
print(f"LIVE-SB WINDOW: {START} -> {max(x['et'].date() for x in cands)}")
print(f"candidates {len(cands)}   engine-only (no MES) {sum(1 for x in cands if x['mes'] is None)}\n")
print(f"{'policy':<30}{'n':>4}{'ctr':>5}{'$':>8}{'$/day':>8}{'$/mo*':>8}{'WR':>6}{'MaxDD':>8}{'green':>8}")
R={}
for nm,tk,qf in POL:
    r=run(tk,qf); R[nm]=r
    print(f"{nm:<30}{r['n']:>4}{r['ctr']:>5}{r['tot']:>8,.0f}{r['perday']:>8.1f}{r['perday']*21:>8,.0f}{r['wr']:>5.0f}%{r['dd']:>8,.0f}{r['green']:>5}/{r['nd']}")
print("\n* $/mo = $/day x 21 sessions, BEFORE the ~$18/day sim-optimism haircut")
# significance
for nm,r in R.items():
    dv=r['dv']; se=statistics.stdev(dv)/math.sqrt(len(dv))
    print(f"  {nm:<30} t={r['perday']/se:+.2f}  {'SIGNIFICANT' if abs(r['perday']/se)>2 else 'not significant (noise)'}")
