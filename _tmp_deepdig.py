"""DEEP MULTI-ANGLE test of Dark Matter framework on our setup outcomes.
Untested angles, each split IS(Apr)/OOS(May-Jun), WR + $:
 A) 0DTE CHARM-pocket alignment (his 'buy charm-positive pocket')
 B) Net 0DTE GAMMA regime (positive=pin/MR vs negative=trend)
 C) deltaDecay (DD) near spot sign
 D) semis (validated, control)
 E) COMPOSITE quality score (how many of charm/semi/gamma align) -> top vs bottom tier
Efficient: per-(day,greek,strike) latest as-of 09:40, near spot, one query/greek.
"""
import os
from datetime import timedelta, date as _date
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")

def pull(greek, expcond):
    q=text(f"""
      SELECT DISTINCT ON (d, strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
        SELECT ts_utc, strike, value, (ts_utc AT TIME ZONE 'America/New_York')::time tt
        FROM volland_exposure_points
        WHERE greek=:g AND {expcond}
          AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-04-11' AND DATE '2026-06-10'
          AND strike BETWEEN 6800 AND 7800
      ) q WHERE tt <= TIME '09:40'
      ORDER BY d, strike, ts_utc DESC""")
    m=defaultdict(dict)
    for d,s,v in C.execute(q,{"g":greek}).fetchall(): m[d.isoformat()][float(s)]=float(v)
    return m
print("pulling charm/gamma/deltaDecay maps...", flush=True)
charm=pull('charm',"expiration_option IS NULL")
gamma=pull('gamma',"expiration_option='TODAY'")
dd=pull('deltaDecay',"expiration_option='TODAY'")
print(f"days: charm {len(charm)}, gamma {len(gamma)}, dd {len(dd)}", flush=True)

basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
def bstr(day,t):
    a=bd.get(day,[]); p=[v for (x,v) in a if x<=t]; return p[-1] if p else None

def near_sum(m,day,spot,w):
    d=m.get(day,{})
    return sum(v for k,v in d.items() if abs(k-spot)<=w)

sig=C.execute(text("""
  SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade, greek_alignment, spot, outcome_pnl
  FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE '2026-04-11'
    AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
  ORDER BY ts ASC""")).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    L=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and L and (aa<0 or aa>=3): return False
    return True
last={}; sigs=[]
for et,s,d,g,a,spot,pnl in sig:
    L=d in ('long','bullish'); k=(s,'L' if L else 'S')
    if k in last and (et-last[k])<timedelta(minutes=15): continue
    last[k]=et
    if not quality(s,d,g,a): continue
    day=et.date().isoformat(); spot=float(spot); tn=et.replace(tzinfo=None)
    sigs.append({"day":day,"L":L,"pnl":float(pnl)*5,"spot":spot,
                 "charm":near_sum(charm,day,spot,12),"gnet":near_sum(gamma,day,spot,60),
                 "dd":near_sum(dd,day,spot,12),"sb":bstr(day,tn)})

def per(d): return "IS" if d<"2026-05-01" else "OOS"
def stt(ts):
    if not ts: return "n=  0"
    w=sum(1 for t in ts if t['pnl']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${sum(t['pnl'] for t in ts):+6.0f} avg${sum(t['pnl'] for t in ts)/len(ts):+5.1f}"

for label in ("IS","OOS"):
    P=[s for s in sigs if per(s["day"])==label]
    print(f"\n############ {label} — {len(P)} ############")
    # A) charm-pocket: long wants charm>0, short wants charm<0
    al=[s for s in P if (s['L'] and s['charm']>0) or (not s['L'] and s['charm']<0)]
    an=[s for s in P if (s['L'] and s['charm']<0) or (not s['L'] and s['charm']>0)]
    print("  A charm-ALIGNED:", stt(al), "| charm-ANTI:", stt(an))
    # B) gamma regime
    gp=[s for s in P if s['gnet']>0]; gn=[s for s in P if s['gnet']<0]
    print("  B +gamma day:", stt(gp), "| -gamma day:", stt(gn))
    print("    +gamma LONGS:", stt([s for s in gp if s['L']]), "| -gamma LONGS:", stt([s for s in gn if s['L']]))
    print("    +gamma SHORTS:", stt([s for s in gp if not s['L']]), "| -gamma SHORTS:", stt([s for s in gn if not s['L']]))
    # C) dd sign: long wants dd<0 (neg-DD floor = support), short wants dd>0
    dl=[s for s in P if (s['L'] and s['dd']<0) or (not s['L'] and s['dd']>0)]
    dn=[s for s in P if (s['L'] and s['dd']>0) or (not s['L'] and s['dd']<0)]
    print("  C dd-ALIGNED:", stt(dl), "| dd-ANTI:", stt(dn))
    # D) semis
    sa=[s for s in P if s['sb'] is not None and ((s['L'] and s['sb']>0) or (not s['L'] and s['sb']<0))]
    sn=[s for s in P if s['sb'] is not None and ((s['L'] and s['sb']<0) or (not s['L'] and s['sb']>0))]
    print("  D semi-ALIGNED:", stt(sa), "| semi-ANTI:", stt(sn))
    # E) composite: count aligned among charm/semi/gamma-favorable
    def score(s):
        sc=0
        if (s['L'] and s['charm']>0) or (not s['L'] and s['charm']<0): sc+=1
        if s['sb'] is not None and ((s['L'] and s['sb']>0) or (not s['L'] and s['sb']<0)): sc+=1
        # gamma: longs better in +gamma (MR), but allow either; reward +gamma for long, -gamma for short
        if (s['L'] and s['gnet']>0) or (not s['L'] and s['gnet']<0): sc+=1
        return sc
    for sc in (3,2,1,0):
        print(f"  E composite={sc}:", stt([s for s in P if score(s)==sc]))
    print("  E HIGH(>=2):", stt([s for s in P if score(s)>=2]), "| LOW(<=1):", stt([s for s in P if score(s)<=1]))
    # F) SIZING book impact
    base=sum(s['pnl'] for s in P)
    def semsz(s):
        if s['sb'] is None: return 1
        return 2 if ((s['L'] and s['sb']>0) or (not s['L'] and s['sb']<0)) else 0.5
    semi_book=sum(s['pnl']*semsz(s) for s in P)
    comp_book=sum(s['pnl']*(2 if score(s)>=2 else 0.5) for s in P)
    aw=sum(2 if score(s)>=2 else 0.5 for s in P)/len(P)
    print(f"  F SIZING: baseline ${base:+.0f} | semi-2/.5 ${semi_book:+.0f} | COMPOSITE-2/.5 ${comp_book:+.0f} (avg {aw:.2f} contracts)")
