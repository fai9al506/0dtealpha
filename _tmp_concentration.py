# -*- coding: utf-8 -*-
"""Concentration check: is the 2-factor uplift driven by one big day, or stable?
(A) REAL TSRT post-V16 (broker fills). (B) V16-ish portal set (broader, more days).
Audited no-look-ahead (semi+gamma lagged 20min)."""
import os, json
from datetime import timedelta, time as dtime
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
LAG=20
basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
def semi_at(day,t):
    co=t-timedelta(minutes=LAG); a=bd.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
def gmap(cut,a,b):
    q=text("""SELECT DISTINCT ON (d,strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
      SELECT ts_utc,strike,value,(ts_utc AT TIME ZONE 'America/New_York')::time tt FROM volland_exposure_points
      WHERE greek='gamma' AND expiration_option='TODAY' AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b
        AND strike BETWEEN 6800 AND 7800) q WHERE tt<=TIME :c ORDER BY d,strike,ts_utc DESC""")
    m=defaultdict(dict)
    for d,s,v in C.execute(q,{"a":a,"b":b,"c":cut}).fetchall(): m[d.isoformat()][float(s)]=float(v)
    return m
def mk_gnet(a,b):
    g940=gmap("09:40",a,b); g1230=gmap("12:30",a,b)
    def gnet(day,spot,t):
        et=(t-timedelta(minutes=LAG)).time(); m=None
        if et>=dtime(12,30): m=g1230.get(day) or g940.get(day)
        elif et>=dtime(9,40): m=g940.get(day)
        else: return None
        return sum(v for k,v in m.items() if abs(k-spot)<=60) if m else None
    return gnet
def size(L,sb,g):
    sm=1.0
    if sb is not None:
        if (L and sb>0) or (not L and sb<0): sm=2.0
        elif (L and sb<0) or (not L and sb>0): sm=0.5
    gm=(1.25 if g<0 else 0.75) if (L and g is not None) else 1.0
    return sm*gm

def analyze(name, perday):
    days=sorted(perday); deltas=[(d,perday[d][1]-perday[d][0]) for d in days]
    tot=sum(x[1] for x in deltas); pos=sum(1 for d,x in deltas if x>0); neg=sum(1 for d,x in deltas if x<-1)
    s=sorted(deltas,key=lambda x:-x[1])
    print(f"\n===== {name} — {len(days)} days =====")
    print(f"  total uplift = ${tot:+.0f} | positive days {pos}, negative {neg}, flat {len(days)-pos-neg}")
    print(f"  TOP day: {s[0][0]} ${s[0][1]:+.0f} ({100*s[0][1]/tot:.0f}% of total)")
    print(f"  total minus TOP day:  ${tot-s[0][1]:+.0f}")
    print(f"  total minus TOP 3:    ${tot-sum(x[1] for x in s[:3]):+.0f}")
    print("  top 5 +days:", " ".join(f"{d}:{x:+.0f}" for d,x in s[:5]))
    print("  worst 5 days:", " ".join(f"{d}:{x:+.0f}" for d,x in s[-5:]))

# (A) REAL TSRT
gnetA=mk_gnet("2026-05-18","2026-06-10")
rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY sl.ts ASC""")).fetchall()
pdA=defaultdict(lambda:[0.0,0.0])
for et,setup,direction,st,spot in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5
    d=et.date().isoformat(); etn=et.replace(tzinfo=None)
    sz=size(L,semi_at(d,etn),gnetA(d,float(spot),etn))
    pdA[d][0]+=usd; pdA[d][1]+=usd*sz
analyze("A) REAL TSRT post-V16 (broker $)", pdA)

# (B) Portal V16-ish, broader
gnetB=mk_gnet("2026-04-11","2026-06-10")
sig=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade, greek_alignment, spot, outcome_pnl
  FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-04-11' AND DATE '2026-06-10'
    AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short') ORDER BY ts ASC""")).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    L=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and L and (aa<0 or aa>=3): return False
    return True
last={}; pdB=defaultdict(lambda:[0.0,0.0])
for et,s,d,g,a,spot,pnl in sig:
    L=d in ('long','bullish'); k=(s,'L' if L else 'S')
    if k in last and (et-last[k])<timedelta(minutes=15): continue
    last[k]=et
    if not quality(s,d,g,a): continue
    day=et.date().isoformat(); etn=et.replace(tzinfo=None); usd=float(pnl)*5
    sz=size(L,semi_at(day,etn),gnetB(day,float(spot),etn))
    pdB[day][0]+=usd; pdB[day][1]+=usd*sz
analyze("B) Portal V16-ish set (broader, more days)", pdB)
