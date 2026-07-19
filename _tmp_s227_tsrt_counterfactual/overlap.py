"""Decisive check: run the SAME policy framework over the days TSRT was actually enabled."""
from engine import *
import pickle, json, statistics
from sqlalchemy import text
cands=pickle.load(open("cands.pkl","rb"))
try: MESFILL={int(k):v for k,v in json.load(open("mesfill_cache.json")).items()}
except Exception: MESFILL={}
for x in cands:
    if x["mes"] is None and x["id"] in MESFILL: x["mes"]=MESFILL[x["id"]]
with conn() as c:
    stmt={r[0]:float(r[1]) for r in c.execute(text("SELECT day,net FROM tsrt_daily_stmt ORDER BY day"))}
    placed_ids={r[0] for r in c.execute(text("SELECT setup_log_id FROM real_trade_orders"))}
days=set(stmt)
sub=[x for x in cands if x["et"].date() in days]
bars=load_bars(sorted({x["et"].date() for x in sub}))
for x in sub:
    sp=stop_for(x["setup"],x["il"],x["tsl"],x["spot"],x["sl"])
    tp=x["tl"] if (x["setup"]=="Vanna Pivot Bounce" and x["tl"]) else None
    epts,_,xet=walk(bars[x["et"].date()],x["et"],x["spot"],x["il"],sp,x["setup"],tp)
    x["pts"]= x["mes"] if x["mes"] is not None else epts
    x["exit"]=xet
DEAD=0.15;CAP_L,CAP_S=1,2;DLL=300.0
def bs(bp,il):
    if bp is None: return "nodata"
    if abs(bp)<DEAD: return "neutral"
    return "confirm" if ((bp>0)==il) else "contradict"
def run(take,qtyf):
    byday=defaultdict(list)
    for x in sub: byday[x["et"].date()].append(x)
    daily={}
    for d in sorted(byday):
        op=[];rz=0.0;pl=[];dp=0.0
        for x in sorted(byday[d],key=lambda z:z["et"]):
            st=bs(x["bp"],x["il"]); still=[]
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
            q=qtyf(st,x); pnl=x["pts"]*DOLLAR_PER_PT*q-COMM_PER_CONTRACT*q
            op.append({**x,"pnl":pnl}); pl.append((x["setup"],x["il"],x["et"])); dp+=pnl
        for p in op: rz+=p["pnl"]
        daily[d]=dp
    return daily

print(f"Overlap window: {len(days)} TSRT-enabled sessions\n")
brk=sum(stmt[d] for d in sorted(days))
for nm,tk,qf in [("Baseline V16 1x",lambda st,x:True,lambda st,x:1),
                 ("SB 0/1/2 (current live policy)",lambda st,x:st!="contradict",lambda st,x:2 if st=="confirm" else 1)]:
    dl=run(tk,qf); tot=sum(dl.get(d,0) for d in days)
    print(f"{nm:<34} sim ${tot:>8,.0f}   broker ${brk:>7,.0f}   ratio {tot/brk if brk else 0:>5.1f}x")
# what did the REAL system actually select? replay ONLY actually-placed ids
real=[x for x in sub if x["id"] in placed_ids]
tot=sum(x["pts"]*DOLLAR_PER_PT*(2 if bs(x["bp"],x["il"])=="confirm" else 1)
        -COMM_PER_CONTRACT*(2 if bs(x["bp"],x["il"])=="confirm" else 1) for x in real)
print(f"\n{'Replay of ACTUALLY-PLACED ids only':<34} sim ${tot:>8,.0f}   broker ${brk:>7,.0f}   n={len(real)}")
print(f"\nV16 candidates in window: {len(sub)}   actually placed: {len(real)}  ({len(real)/len(sub)*100:.0f}%)")
