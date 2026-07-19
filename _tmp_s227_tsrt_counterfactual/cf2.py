"""Variants: baseline vs leak-fix candidates. Cap 1/2 (code default)."""
from engine import *
from sqlalchemy import text
CAP_L,CAP_S=1,2; DLL=300.0

with conn() as c:
    rows=c.execute(text("""
      SELECT s.id,(s.ts AT TIME ZONE 'America/New_York') et,s.setup_name,s.direction,s.spot,
             s.outcome_stop_level,s.outcome_target_level,s.trail_sl,s.outcome_pnl,
             s.mes_sim_outcome_pnl,s.basket_pct
      FROM setup_log s WHERE s.ts>='2026-07-01' AND s.real_trade_skip_reason='master_kill'
      ORDER BY s.ts""")).fetchall()
    # SPX 20-min slope at each signal ts (for the "falling SPX" guard)
    slope={}
    for lid,et in [(r[0],r[1]) for r in rows]:
        pass
bars=load_bars(sorted({r[1].date() for r in rows}))

def spx_slope(d, et, mins=20):
    seq=[b for b in bars[d] if b[0]<=et]
    if len(seq)<mins+1: return 0.0
    return seq[-1][4]-seq[-1-mins][4]

def run(name, allow):
    byday=defaultdict(list)
    for r in rows: byday[r[1].date()].append(r)
    TOT=0;NT=0;NW=0;daily={}
    for d in sorted(byday):
        open_pos=[];realized=0.0;placed=[];dayp=0.0
        for (lid,et,setup,direction,spot,sl,tl,tsl,cp,mp,bp) in byday[d]:
            il=direction.lower() in ("long","bullish")
            still=[]
            for p in open_pos:
                if p["exit_et"]<=et: realized+=p["pnl"]
                else: still.append(p)
            open_pos=still
            if not allow(setup,il,float(spot),bp,d,et): continue
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
            pnl=pts*DOLLAR_PER_PT*qty-COMM_PER_CONTRACT*qty
            open_pos.append({"setup":setup,"is_long":il,"entry":float(spot),"exit_et":xet,"pnl":pnl})
            placed.append((setup,il,et)); NT+=1; NW+=1 if pts>0 else 0; dayp+=pnl
        for p in open_pos: realized+=p["pnl"]
        daily[d]=dayp; TOT+=dayp
    peak=0;dd=0;c2=0
    for d in sorted(daily):
        c2+=daily[d];peak=max(peak,c2);dd=min(dd,c2-peak)
    g=sum(1 for v in daily.values() if v>0)
    print(f"{name:<42}{NT:>4}{TOT:>9.0f}{NW/NT*100 if NT else 0:>7.0f}%{dd:>9.0f}{g:>4}/{len(daily)}")
    return daily

print(f"{'variant':<42}{'n':>4}{'$':>9}{'WR':>8}{'MaxDD':>9}{'green':>8}")
run("A. Baseline (as-configured)", lambda s,il,sp,bp,d,et: True)
run("B. + block GEX Long entirely", lambda s,il,sp,bp,d,et: s!="GEX Long")
run("C. + block neutral-basket longs, SPX 20m<=-2",
    lambda s,il,sp,bp,d,et: not (il and (bp is None or abs(float(bp))<0.15) and spx_slope(d,et)<=-2))
run("D. B + C", lambda s,il,sp,bp,d,et: s!="GEX Long" and
    not (il and (bp is None or abs(float(bp))<0.15) and spx_slope(d,et)<=-2))
run("E. Shorts only", lambda s,il,sp,bp,d,et: not il)
run("F. Confirm-basket longs + all shorts",
    lambda s,il,sp,bp,d,et: (not il) or (bp is not None and abs(float(bp))>=0.15 and (float(bp)>0)==il))
