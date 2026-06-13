"""S208 daily-loss-cap backtest across FULL history (Feb-Jun 2026).
Circuit-breaker model (matches real_trader $300 breaker): per ET day, walk
trades in ENTRY-time order; realized P&L = sum of trades whose CLOSE time
(entry + outcome_elapsed_min) precedes this entry. Once realized <= -cap,
STOP for the rest of the day (block all later entries). Compare total vs no-cap.

Traded set approximation: quality SC/DD/ES Abs longs + SC/AG/DD shorts (A+/A/B,
DD gates). P&L = outcome_pnl (portal sim; $=5x at 1 MES). Close-time ordering
mitigates the realization-timing trap (per research_jun3_postmortem_s203).
"""
import os
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
               greek_alignment, vix, outcome_pnl, outcome_elapsed_min
        FROM setup_log
        WHERE outcome_pnl IS NOT NULL AND outcome_elapsed_min IS NOT NULL
          AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
        ORDER BY ts ASC""")).fetchall()

def tradeable(setup,direction,grade,align,vix):
    if grade in ('C','LOG',None): return False
    islong = direction in ('long','bullish')
    a = align or 0
    if setup=='ES Absorption' and grade not in ('A','A+'): return False
    if setup=='DD Exhaustion':
        if islong and (a<0 or a>=3): return False
        if vix is not None and float(vix)>=22 and islong: return False
    return True

# dedup 15min per (setup,direction)
last={}; T=[]
for et,setup,direction,grade,align,vix,pnl,elap in rows:
    islong = direction in ('long','bullish')
    key=(setup, 'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15): continue
    last[key]=et
    if not tradeable(setup,direction,grade,align,vix): continue
    T.append({"et":et,"day":et.date().isoformat(),"mo":et.date().isoformat()[:7],
              "close":et+timedelta(minutes=float(elap)),"pnl":float(pnl)})

def sim(cap):
    by_mo=defaultdict(float); total=0.0; blocked_n=0
    by_day=defaultdict(list)
    for t in T: by_day[t['day']].append(t)
    for day,ts in by_day.items():
        ts=sorted(ts,key=lambda x:x['et'])
        realized=0.0; tripped=False; taken=[]
        for t in ts:
            if tripped:
                blocked_n+=1; continue
            # realized from trades already taken that closed before this entry
            realized=sum(x['pnl'] for x in taken if x['close'] <= t['et'])
            if cap is not None and realized <= -cap/5.0:  # cap in $, pnl in pts
                tripped=True; blocked_n+=1; continue
            taken.append(t)
        for t in taken:
            by_mo[t['mo']]+=t['pnl']; total+=t['pnl']
    return total, by_mo, blocked_n

months=sorted(set(t['mo'] for t in T))
base,bm,_=sim(None)
print(f"Traded set: {len(T)} trades. P&L in points (x5 = $ at 1 MES).\n")
print(f"{'cap':>8}{'  '}{'total_pts':>10}{'total_$':>10}{'vs_nocap_$':>11}{'blocked':>8}   by-month $:")
for cap in [None, 300, 250, 200, 150, 100]:
    tot,bym,bn = sim(cap)
    delta = (tot-base)*5
    momstr = "  ".join(f"{m[5:]}:{bym.get(m,0)*5:+.0f}" for m in months)
    capn = "NO-CAP" if cap is None else f"${cap}"
    print(f"{capn:>8}  {tot:>10.1f}{tot*5:>10.0f}{delta:>+11.0f}{bn:>8}   {momstr}")
