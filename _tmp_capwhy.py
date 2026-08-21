import os, sys, importlib.util, io, contextlib, collections
from datetime import timedelta
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
mm=importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(mm)

# month-by-month cap3 - cap2
for pol in ("base1",):
    r2=mm.run(pol,2,2); r3=mm.run(pol,3,3)
    mo=collections.defaultdict(float); n_better=collections.Counter(); n_worse=collections.Counter()
    for d in set(r2["daily"])|set(r3["daily"]):
        x=r3["daily"].get(d,0)-r2["daily"].get(d,0); k=d.strftime('%Y-%m'); mo[k]+=x
        if x>1: n_better[k]+=1
        elif x<-1: n_worse[k]+=1
    print(f"=== {pol}: value of going cap 2/2 -> 3/3, by month (1 MES flat) ===")
    for k in sorted(mo):
        print(f"  {k}   {mo[k]:>+9,.0f}   days better {n_better[k]:>2}  worse {n_worse[k]:>2}")
    print(f"  TOTAL {sum(mo.values()):>+9,.0f}\n")

# stack position by month
print("=== stack position pts/trade, BY MONTH (cap 8/8, 1 MES) ===")
byday=collections.defaultdict(list)
for r in mm.cands: byday[r["ts"].astimezone(mm.ET).date()].append(r)
sp=collections.defaultdict(lambda: collections.defaultdict(lambda:[0,0,0.0]))
for d in sorted(byday):
    openp=[]; placed=[]; realized=0.0
    for r in byday[d]:
        et=r["ts"].astimezone(mm.ET); il=r["direction"] in ("long","bullish")
        still=[]
        for p in openp:
            if p["exit"]<=et: realized+=p["pnl"]
            else: still.append(p)
        openp=still
        if any(s==r["setup_name"] and dl==il and (et-t).total_seconds()<90 for s,dl,t in placed): continue
        nopen=sum(1 for p in openp if p["is_long"]==il)
        if realized<=-300: continue
        stack=[p for p in openp if p["setup"]==r["setup_name"] and p["is_long"]==il]
        if len(stack)>=2:
            sgn=1.0 if il else -1.0
            if sum((float(r["spot"])-p["entry"])*sgn for p in stack)<0: continue
        pts=float(r["outcome_pnl"])
        openp.append({"setup":r["setup_name"],"is_long":il,"entry":float(r["spot"]),
                      "exit":et+timedelta(minutes=int(r["outcome_elapsed_min"] or 60)),"pnl":pts*5-1})
        placed.append((r["setup_name"],il,et))
        k=min(nopen+1,3); mo=d.strftime('%Y-%m')
        sp[mo][k][0]+=1; sp[mo][k][1]+= 1 if pts>0 else 0; sp[mo][k][2]+=pts
months=sorted(sp)
print(f"  {'month':<9}" + "".join(f"{('pos'+str(k)):>22}" for k in (1,2,3)))
for mo in months:
    line=f"  {mo:<9}"
    for k in (1,2,3):
        v=sp[mo][k]
        line += f"{(f'{v[0]}t {v[1]/v[0]*100:.0f}% {v[2]/v[0]:+.2f}p' if v[0] else '-'):>22}"
    print(line)
