"""Can a VIX/vol-qualified down-regime isolate the genuinely bad down days in
EVERY era (not just June)? If a variant is negative across all 5 months -> robust
ship. If only June -> it's a recent/macro-regime effect (park, don't ship).
P&L: portal outcome_pnl AND mes_sim (broker-realistic) side by side.
"""
import os
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    spath = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') as et, spot, vix
        FROM setup_log WHERE spot IS NOT NULL
          AND (ts AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
        ORDER BY ts ASC""")).fetchall()
    day_rows = defaultdict(list)
    for et, spot, vix in spath:
        day_rows[et.date().isoformat()].append((et, float(spot), float(vix) if vix is not None else None))
    def feats(day, ts, spot, vix):
        path = day_rows.get(day, [])
        open_spot = path[0][1] if path else spot
        open_vix = next((v for (_,_,v) in path if v is not None), None)
        return spot-open_spot, ((vix-open_vix) if (vix is not None and open_vix is not None) else None)

    rows = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') as et, setup_name, grade, spot,
               greek_alignment, vix, outcome_pnl, mes_sim_outcome_pnl
        FROM setup_log
        WHERE setup_name IN ('Skew Charm','DD Exhaustion')
          AND direction IN ('long','bullish')
          AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
        ORDER BY ts ASC""")).fetchall()

last={}; sig=[]
for et,setup,grade,spot,align,vix,opnl,mpnl in rows:
    if setup in last and (et-last[setup])<timedelta(minutes=15): continue
    last[setup]=et
    if grade in ('C','LOG',None): continue
    if setup=='DD Exhaustion':
        a=align or 0
        if a<0 or a>=3: continue
        if vix is not None and float(vix)>=22: continue
    d=et.date().isoformat()
    fo,vc=feats(d,et,float(spot),float(vix) if vix is not None else None)
    sig.append({"mo":d[:7],"day":d,"fo":fo,"vc":vc,"vix":float(vix) if vix is not None else None,
                "pnl":float(opnl),"mpnl":float(mpnl) if mpnl is not None else None})

months=sorted(set(t['mo'] for t in sig))
def agg(ts,f):
    vals=[t[f] for t in ts if t[f] is not None]
    if not vals: return (0,0,0.0)
    return (len(vals), 100*sum(1 for v in vals if v>0)/len(vals), sum(vals))

def line(name, cond):
    sel=[t for t in sig if cond(t)]
    print(f"\n--- {name} ---  (blocked-bucket P&L by month; NEGATIVE every month = robust block)")
    print(f"  {'month':<8}{'portal':<32}{'mes_sim'}")
    allp=[]; allm=[]
    for mo in months:
        mm=[t for t in sel if t['mo']==mo]
        n,wr,s=agg(mm,'pnl'); nm,wrm,sm=agg(mm,'mpnl')
        allp+= [t for t in mm];
        ps=f"n={n:>3} WR={wr:3.0f}% sum={s:+7.1f}p (${s*5:+6.0f})"
        ms=f"n={nm:>3} WR={wrm:3.0f}% sum={sm:+6.1f}p (${sm*5:+5.0f})" if nm else "(no mes_sim)"
        print(f"  {mo:<8}{ps:<32}{ms}")
    n,wr,s=agg(sel,'pnl'); nm,wrm,sm=agg(sel,'mpnl')
    print(f"  {'TOTAL':<8}n={n:>3} WR={wr:3.0f}% sum={s:+7.1f}p (${s*5:+6.0f})   "
          f"mes_sim n={nm} sum={sm:+.1f}p (${sm*5:+.0f})")
    print(f"  distinct days: {len(set(t['day'] for t in sel))}")

line("V1: from_open<=-15 (crude)", lambda t: t['fo']<=-15)
line("V2: from_open<=-15 AND vix_chg>=+1.0", lambda t: t['fo']<=-15 and (t['vc'] or 0)>=1.0)
line("V3: from_open<=-15 AND vix_chg>=+0.5", lambda t: t['fo']<=-15 and (t['vc'] or 0)>=0.5)
line("V4: from_open<=-20 AND vix_chg>=+0.5", lambda t: t['fo']<=-20 and (t['vc'] or 0)>=0.5)
line("V5: vix_chg>=+1.0 (VIX rising, any price)", lambda t: (t['vc'] or 0)>=1.0)
