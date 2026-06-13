# -*- coding: utf-8 -*-
"""Test user's gamma rule across post-V16: path in trade direction.
LONG: net gamma (spot, spot+PATH] -> negative=-G accelerator(GOOD), positive=+G barrier(BAD).
SHORT mirror below. Does it separate WIN/LOSS? Also nearest-barrier variant."""
import os, json
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
PATH=20; WALL=7.0; NEAR=12
# intraday gamma per day, near-spot, with ts
days=[r[0].isoformat() for r in C.execute(text("""SELECT DISTINCT (ts AT TIME ZONE 'America/New_York')::date
  FROM real_trade_orders r JOIN setup_log s ON s.id=r.setup_log_id
  WHERE (s.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY 1""")).fetchall()]
gday={}
for d in days:
    gr=C.execute(text("""SELECT (ts_utc AT TIME ZONE 'America/New_York') et, strike, value, current_price
      FROM volland_exposure_points WHERE greek='gamma' AND expiration_option='TODAY'
        AND (ts_utc AT TIME ZONE 'America/New_York')::date=DATE :d AND strike BETWEEN 7100 AND 7800 ORDER BY ts_utc"""),{"d":d}).fetchall()
    sn=defaultdict(dict); sp={}
    for et,k,v,cp in gr:
        key=et.replace(second=0,microsecond=0); sn[key][float(k)]=float(v)/1e6
        if cp: sp[key]=float(cp)
    gday[d]=(sorted(sn),sn,sp)
def path_feats(d,etn,L,spot):
    times,sn,sp=gday[d]; prior=[t for t in times if t<=etn]
    if not prior: return None,None
    t=prior[-1]; s0=sp.get(t) or spot; m=sn[t]
    if L: netpath=sum(v for k,v in m.items() if s0<k<=s0+PATH)
    else: netpath=sum(v for k,v in m.items() if s0-PATH<=k<s0)
    # nearest +G wall in direction
    if L: cand=sorted([(k,v) for k,v in m.items() if s0<k<=s0+40])
    else: cand=sorted([(k,v) for k,v in m.items() if s0-40<=k<s0],reverse=True)
    wall=next((abs(k-s0) for k,v in cand if v>=WALL),None)
    return netpath, wall

rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY sl.ts"""),{}).fetchall()
T=[]
for et,setup,direction,st,spot in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5
    np_,wall=path_feats(et.date().isoformat(),et.replace(tzinfo=None),L,float(spot))
    if np_ is None: continue
    T.append({"L":L,"pnl":usd,"netpath":np_,"wall":wall})
def stt(ts):
    if not ts: return "n=0"
    w=sum(1 for t in ts if t['pnl']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${sum(t['pnl'] for t in ts):+6.0f} avg${sum(t['pnl'] for t in ts)/len(ts):+5.1f}"
print(f"Post-V16 real trades with gamma data: {len(T)}")
print("\nRULE A — net path gamma in trade direction:")
print("  ACCELERATOR (path < 0, -G):", stt([t for t in T if t['netpath']<0]))
print("  BARRIER     (path > 0, +G):", stt([t for t in T if t['netpath']>0]))
print("\nRULE B — nearest +G wall in direction:")
print("  CLEAR (no +G wall <=12pt):", stt([t for t in T if t['wall'] is None or t['wall']>NEAR]))
print("  CAPPED (+G wall <=12pt):   ", stt([t for t in T if t['wall'] is not None and t['wall']<=NEAR]))
print("\n  (split by direction, RULE A)")
for L,nm in [(True,'LONGS'),(False,'SHORTS')]:
    sub=[t for t in T if t['L']==L]
    print(f"   {nm}: accel {stt([t for t in sub if t['netpath']<0])} | barrier {stt([t for t in sub if t['netpath']>0])}")
