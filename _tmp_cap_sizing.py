# -*- coding: utf-8 -*-
"""CAP-AWARE sizing sim: real TSRT post-V16, apply the $300 daily-loss breaker on
the SIZED realized loss (chronological, close-time ordered). The breaker trips
EARLIER under 2x sizing -> caps the sized downside. Compare base(cap) vs sized(cap)."""
import os, json
from datetime import timedelta, time as dtime
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
LAG=20; CAP=300.0
basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
def semi_at(day,t):
    co=t-timedelta(minutes=LAG); a=bd.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
def gmap(cut):
    q=text("""SELECT DISTINCT ON (d,strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
      SELECT ts_utc,strike,value,(ts_utc AT TIME ZONE 'America/New_York')::time tt FROM volland_exposure_points
      WHERE greek='gamma' AND expiration_option='TODAY' AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10'
        AND strike BETWEEN 6800 AND 7800) q WHERE tt<=TIME :c ORDER BY d,strike,ts_utc DESC""")
    m=defaultdict(dict)
    for d,s,v in C.execute(q,{"c":cut}).fetchall(): m[d.isoformat()][float(s)]=float(v)
    return m
g940=gmap("09:40"); g1230=gmap("12:30")
def gnet(day,spot,t):
    et=(t-timedelta(minutes=LAG)).time(); m=None
    if et>=dtime(12,30): m=g1230.get(day) or g940.get(day)
    elif et>=dtime(9,40): m=g940.get(day)
    else: return None
    return sum(v for k,v in m.items() if abs(k-spot)<=60) if m else None
def fullsize(L,sb,g):
    sm=1.0
    if sb is not None:
        if (L and sb>0) or (not L and sb<0): sm=2.0
        elif (L and sb<0) or (not L and sb>0): sm=0.5
    gm=(1.25 if g<0 else 0.75) if (L and g is not None) else 1.0
    return sm*gm

rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot, sl.outcome_elapsed_min
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY sl.ts ASC""")).fetchall()
byday=defaultdict(list)
for et,setup,direction,st,spot,elap in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5
    etn=et.replace(tzinfo=None); d=etn.date().isoformat()
    sz=fullsize(L,semi_at(d,etn),gnet(d,float(spot),etn))
    close=etn+timedelta(minutes=float(elap) if elap is not None else 30)
    byday[d].append({"en_t":etn,"cl_t":close,"base":usd,"sz_pnl":usd*sz})

def sim_cap(day_trades, use_size):
    """chronological; breaker trips when realized loss >= CAP -> stop new entries."""
    taken=[]; tripped=False
    for tr in sorted(day_trades,key=lambda x:x["en_t"]):
        if tripped: continue
        realized=sum((t["sz_pnl"] if use_size else t["base"]) for t in taken if t["cl_t"]<=tr["en_t"])
        if realized<=-CAP: tripped=True; continue
        taken.append(tr)
    return sum((t["sz_pnl"] if use_size else t["base"]) for t in taken)

print(f"{'day':<12}{'base(cap)':>10}{'sized NO-cap':>13}{'sized(cap)':>11}{'delta(cap)':>11}")
tb=ts_nc=ts=0
for d in sorted(byday):
    bc=sim_cap(byday[d],False)             # base with cap (~ actual)
    snc=sum(t["sz_pnl"] for t in byday[d]) # sized no cap
    sc=sim_cap(byday[d],True)              # sized WITH cap
    tb+=bc; ts_nc+=snc; ts+=sc
    flag=" <--cap helped" if sc>snc+1 else (" <--cap hurt" if sc<snc-1 else "")
    print(f"{d:<12}{bc:>+10.0f}{snc:>+13.0f}{sc:>+11.0f}{sc-bc:>+11.0f}{flag}")
print(f"{'TOTAL':<12}{tb:>+10.0f}{ts_nc:>+13.0f}{ts:>+11.0f}{ts-tb:>+11.0f}")
print(f"\nsized NO-cap uplift vs base: ${ts_nc-tb:+.0f} | sized WITH-cap uplift vs base: ${ts-tb:+.0f}")
