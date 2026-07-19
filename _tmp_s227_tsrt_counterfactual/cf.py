"""Full TSRT counterfactual Jul 1 -> Jul 17, replicating real_trader gate order."""
from engine import *
from sqlalchemy import text
import sys, json
try: MESFILL={int(k):v for k,v in json.load(open("mesfill_cache.json")).items()}
except Exception: MESFILL={}

CAP_L=int(sys.argv[1]); CAP_S=int(sys.argv[2])
DAILY_LOSS_LIMIT=300.0
VERBOSE = len(sys.argv)>3

with conn() as c:
    rows=c.execute(text("""
      SELECT s.id,(s.ts AT TIME ZONE 'America/New_York') et,s.setup_name,s.direction,s.spot,
             s.outcome_stop_level,s.outcome_target_level,s.trail_sl,s.outcome_pnl,
             s.mes_sim_outcome_pnl,s.basket_pct
      FROM setup_log s
      WHERE s.ts>='2026-07-01' AND s.real_trade_skip_reason='master_kill'
      ORDER BY s.ts""")).fetchall()
bars=load_bars(sorted({r[1].date() for r in rows}))

byday=defaultdict(list)
for r in rows: byday[r[1].date()].append(r)

TOT=0.0; NT=0; NW=0; daily={}; per_setup=defaultdict(lambda:[0,0.0]); skips=defaultdict(int)
taken_all=[]
for d in sorted(byday):
    open_pos=[]      # dicts: setup,is_long,entry,exit_et,pts,qty
    realized=0.0     # $ realized today (net)
    placed=[]        # (setup,dir,ts) for dedup
    dayp=0.0
    for (lid,et,setup,direction,spot,sl,tl,tsl,cp,mp,bp) in byday[d]:
        il = direction.lower() in ("long","bullish")
        # retire finished positions & realize
        still=[]
        for p in open_pos:
            if p["exit_et"] <= et:
                realized += p["pnl"]
            else: still.append(p)
        open_pos=still
        # gate: dedup 90s
        if any(s==setup and dl==il and (et-t).total_seconds()<90 for s,dl,t in placed):
            skips["dedup_window"]+=1; continue
        # gate: concurrency cap
        cap = CAP_L if il else CAP_S
        if sum(1 for p in open_pos if p["is_long"]==il) >= cap:
            skips[f"cap_{'long' if il else 'short'}_full"]+=1; continue
        # gate: daily loss circuit breaker (net realized $, both accounts)
        if realized <= -DAILY_LOSS_LIMIT:
            skips["daily_loss_limit"]+=1; continue
        # gate: underwater stack (>=2 open same setup+dir, net unrealized < 0)
        stack=[p for p in open_pos if p["setup"]==setup and p["is_long"]==il]
        if len(stack)>=2:
            sgn=1.0 if il else -1.0
            unreal=sum((float(spot)-p["entry"])*sgn for p in stack)
            if unreal<0:
                skips["underwater_stack_block"]+=1; continue
        # PLACE
        sp=stop_for(setup,il,tsl,float(spot),float(sl) if sl else None)
        tp=float(tl) if (setup=="Vanna Pivot Bounce" and tl) else None
        epts,res,xet = walk(bars[d],et,float(spot),il,sp,setup,tp)
        pts = float(mp) if mp is not None else (MESFILL.get(lid) if MESFILL.get(lid) is not None else epts)
        qty = 2 if (bp is not None and abs(float(bp))>=0.15 and ((float(bp)>0)==il)) else 1
        pnl = pts*DOLLAR_PER_PT*qty - COMM_PER_CONTRACT*qty
        open_pos.append({"setup":setup,"is_long":il,"entry":float(spot),
                         "exit_et":xet,"pnl":pnl,"qty":qty})
        placed.append((setup,il,et))
        NT+=1; NW+= 1 if pts>0 else 0; dayp+=pnl
        per_setup[setup][0]+=1; per_setup[setup][1]+=pnl
        taken_all.append((d,lid,setup,"L" if il else "S",pts,qty,pnl,(mp is None and MESFILL.get(lid) is None)))
    for p in open_pos: realized+=p["pnl"]
    daily[d]=dayp; TOT+=dayp

print(f"=== CAP long={CAP_L} short={CAP_S} ===")
print(f"{'date':<12}{'$':>9}  cum")
cum=0
for d in sorted(daily):
    cum+=daily[d]; print(f"{str(d):<12}{daily[d]:>9.0f}{cum:>9.0f}")
print(f"\nTOTAL ${TOT:,.0f}   trades {NT}   WR {NW/NT*100:.0f}%   green days {sum(1 for v in daily.values() if v>0)}/{len(daily)}")
peak=0;dd=0;c2=0
for d in sorted(daily):
    c2+=daily[d]; peak=max(peak,c2); dd=min(dd,c2-peak)
print(f"MaxDD ${dd:,.0f}")
print(f"\n{'setup':<22}{'n':>4}{'$':>10}")
for s,a in sorted(per_setup.items(), key=lambda x:-x[1][1]):
    print(f"{s:<22}{a[0]:>4}{a[1]:>10.0f}")
print(f"\nskips: {dict(skips)}")
eng_only=[t for t in taken_all if t[7]]
print(f"engine-fallback (no mes_sim) trades: {len(eng_only)}/{NT}, their $ = {sum(t[6] for t in eng_only):,.0f}")
if VERBOSE:
    print()
    for t in taken_all: print(t)
