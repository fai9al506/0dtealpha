"""SEMI-SIZING overlay on our EXISTING setups (the highest-leverage validated change).
Keep every quality trade, but size by semi-confirmation:
  CONFIRMED   (long & semis>=+T) or (short & semis<=-T)  -> 2x
  NEUTRAL     (|semis|<T)                                -> 1x (or 0.5x in scheme B)
  UNCONFIRMED (long & semis<=-T) or (short & semis>=+T)  -> 0.5x (or skip in scheme B)
Compare blended book vs flat-1x baseline. IS(Apr) / OOS(May-Jun). $@1MES base unit.
"""
import os
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")

basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bday=defaultdict(list)
for et,v in basket: bday[et.date().isoformat()].append((et,v))
def bstr(day,t):
    arr=bday.get(day,[]); prior=[v for (x,v) in arr if x<=t]
    return prior[-1] if prior else None

sig=C.execute(text("""
    SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
           greek_alignment, outcome_pnl
    FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE '2026-04-11'
      AND outcome_pnl IS NOT NULL
      AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
    ORDER BY ts ASC""")).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    islong=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and islong and (aa<0 or aa>=3): return False
    return True
last={}; sigs=[]
for et,s,d,g,a,pnl in sig:
    islong=d in ('long','bullish'); key=(s,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15): continue
    last[key]=et
    if not quality(s,d,g,a): continue
    bs=bstr(et.date().isoformat(), et.replace(tzinfo=None))
    sigs.append({"day":et.date().isoformat(),"islong":islong,"pnl":float(pnl)*5,"bs":bs})

def per(d): return "IS" if d<"2026-05-01" else "OOS"
def klass(s,T):
    if s["bs"] is None: return "neutral"
    conf = (s["islong"] and s["bs"]>=T) or ((not s["islong"]) and s["bs"]<=-T)
    unconf = (s["islong"] and s["bs"]<=-T) or ((not s["islong"]) and s["bs"]>=T)
    return "confirmed" if conf else ("unconfirmed" if unconf else "neutral")

for T in (0.0, 0.3):
    print(f"\n================ threshold T={T} ================")
    for s in sigs: s["k"]=klass(s,T)
    for label in ("IS","OOS"):
        P=[s for s in sigs if per(s["day"])==label]
        base=sum(s["pnl"] for s in P)
        # scheme A: 2x conf / 1x neu / 0.5x unconf
        a=sum(s["pnl"]*(2 if s["k"]=="confirmed" else 0.5 if s["k"]=="unconfirmed" else 1) for s in P)
        # scheme B: 2x conf / 0.5x neu / 0x unconf (skip)
        b=sum(s["pnl"]*(2 if s["k"]=="confirmed" else 0 if s["k"]=="unconfirmed" else 0.5) for s in P)
        # avg contracts (capital proxy) for scheme A
        aw=sum(2 if s["k"]=="confirmed" else 0.5 if s["k"]=="unconfirmed" else 1 for s in P)/len(P)
        nc=sum(1 for s in P if s["k"]=="confirmed"); nu=sum(1 for s in P if s["k"]=="unconfirmed"); nn=len(P)-nc-nu
        print(f"  {label}: n={len(P)} (conf {nc}/neu {nn}/unconf {nu})")
        print(f"     baseline 1x:     ${base:+.0f}")
        print(f"     scheme A (2/1/.5): ${a:+.0f}  (avg {aw:.2f} contracts; +{a-base:+.0f} vs base)")
        print(f"     scheme B (2/.5/0): ${b:+.0f}  (+{b-base:+.0f} vs base)")
