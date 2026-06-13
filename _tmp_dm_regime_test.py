"""Decisive test of Dark Matter's framework core:
  EXTREME vol regime (VIX high / above baseline) -> polarity inverts ->
  mean-reversion dip-LONGS fail, fade-SHORTS work.
  NORMAL/LOW vol -> classical -> dip-longs work (our current edge).

Bucket every quality-traded signal by VIX regime, compare long vs short
outcomes by month. P&L = outcome_pnl (pts; $=5x at 1 MES). dedup 15min.
"""
import os
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
               greek_alignment, vix, outcome_pnl
        FROM setup_log
        WHERE outcome_pnl IS NOT NULL AND vix IS NOT NULL
          AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
        ORDER BY ts ASC""")).fetchall()

# daily VIX (median of the day's readings) + rolling baseline percentile
dayvix=defaultdict(list)
for et,_,_,_,_,vix,_ in rows:
    dayvix[et.date().isoformat()].append(float(vix))
day_med={d:sorted(v)[len(v)//2] for d,v in dayvix.items()}

def quality(setup,direction,grade,align,vix):
    if grade in ('C','LOG',None): return False
    islong=direction in ('long','bullish'); a=align or 0
    if setup=='ES Absorption' and grade not in ('A','A+'): return False
    if setup=='DD Exhaustion' and islong and (a<0 or a>=3): return False
    return True

last={}; T=[]
for et,setup,direction,grade,align,vix,pnl in rows:
    islong=direction in ('long','bullish')
    key=(setup,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15): continue
    last[key]=et
    if not quality(setup,direction,grade,align,vix): continue
    T.append({"mo":et.date().isoformat()[:7],"day":et.date().isoformat(),
              "islong":islong,"vix":float(vix),"pnl":float(pnl)})

def ag(ts):
    if not ts: return "n=  0"
    v=[t['pnl'] for t in ts]; w=sum(1 for x in v if x>0)
    return f"n={len(v):>3} WR={100*w/len(v):3.0f}% ${sum(v)*5:+7.0f}"

months=sorted(set(t['mo'] for t in T))
# how many EXTREME days per month
print("VIX regime distribution (day median):")
for mo in months:
    ds=[d for d in day_med if d[:7]==mo]
    ext=[d for d in ds if day_med[d]>=20]
    print(f"  {mo}: {len(ds)} days, EXTREME(VIX>=20): {len(ext)} {sorted(d[5:] for d in ext)}")
print()

for THR,name in [(20,"VIX>=20"),(19,"VIX>=19")]:
    print(f"\n############ EXTREME = {name} ############")
    for regime_name, cond in [("EXTREME", lambda t:t['vix']>=THR), ("NORMAL", lambda t:t['vix']<THR)]:
        sub=[t for t in T if cond(t)]
        L=[t for t in sub if t['islong']]; S=[t for t in sub if not t['islong']]
        print(f"\n  --- {regime_name} ({name}) ---  LONGS {ag(L)}   |   SHORTS {ag(S)}")
        for mo in months:
            Lm=[t for t in L if t['mo']==mo]; Sm=[t for t in S if t['mo']==mo]
            if Lm or Sm:
                print(f"      {mo}:  long {ag(Lm):<28} short {ag(Sm)}")
