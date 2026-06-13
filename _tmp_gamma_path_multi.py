# -*- coding: utf-8 -*-
"""Gamma-path rule with MULTI-EXPIRY gamma (0DTE+weekly+monthly summed, as Dark Matter reads it).
Path in trade direction: -G accelerator(GOOD) vs +G barrier(BAD). Post-V16. Compare to 0DTE-only."""
import os, json
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
PATH=20
EXPS=('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
days=[r[0].isoformat() for r in C.execute(text("""SELECT DISTINCT (s.ts AT TIME ZONE 'America/New_York')::date
  FROM real_trade_orders r JOIN setup_log s ON s.id=r.setup_log_id
  WHERE (s.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY 1""")).fetchall()]
# per day per expiration: sorted [(ts,{strike:val})]
gday={}
for d in days:
    perexp={e:defaultdict(dict) for e in EXPS}; sp={}
    gr=C.execute(text("""SELECT (ts_utc AT TIME ZONE 'America/New_York') et, expiration_option, strike, value, current_price
      FROM volland_exposure_points WHERE greek='gamma' AND expiration_option=ANY(:e)
        AND (ts_utc AT TIME ZONE 'America/New_York')::date=DATE :d AND strike BETWEEN 7100 AND 7800 ORDER BY ts_utc"""),
        {"d":d,"e":list(EXPS)}).fetchall()
    for et,exp,k,v,cp in gr:
        key=et.replace(second=0,microsecond=0); perexp[exp][key][float(k)]=float(v)/1e6
        if cp and exp=='TODAY': sp[key]=float(cp)
    gday[d]={e:sorted(perexp[e]) for e in EXPS}, perexp, sp
def summed_profile(d, etn, use_multi):
    keys,perexp,sp=gday[d]
    # spot as-of
    spk=[t for t in keys['TODAY'] if t<=etn]; s0=sp.get(spk[-1]) if spk else None
    prof=defaultdict(float)
    exps = EXPS if use_multi else ('TODAY',)
    for e in exps:
        prior=[t for t in keys[e] if t<=etn]
        if not prior: continue
        for k,v in perexp[e][prior[-1]].items(): prof[k]+=v
    return prof, s0
def feat(d,etn,L,spot,use_multi):
    prof,s0=summed_profile(d,etn,use_multi); s0=s0 or spot
    if not prof: return None
    if L: return sum(v for k,v in prof.items() if s0<k<=s0+PATH)
    return sum(v for k,v in prof.items() if s0-PATH<=k<s0)
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
    pts=(en-ex) if sh else (ex-en); usd=pts*5; etn=et.replace(tzinfo=None); d=etn.date().isoformat()
    f0=feat(d,etn,L,float(spot),False); fm=feat(d,etn,L,float(spot),True)
    if f0 is None or fm is None: continue
    T.append({"L":L,"pnl":usd,"g0":f0,"gm":fm})
def stt(ts):
    if not ts: return "n=0"
    w=sum(1 for t in ts if t['pnl']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${sum(t['pnl'] for t in ts):+6.0f} avg${sum(t['pnl'] for t in ts)/len(ts):+5.1f}"
print(f"trades: {len(T)}\n")
for lbl,key in [("0DTE-only","g0"),("MULTI-EXPIRY (0+W+M)","gm")]:
    print(f"=== {lbl} gamma path ===")
    print("  ALL  accel(-G):",stt([t for t in T if t[key]<0])," | barrier(+G):",stt([t for t in T if t[key]>0]))
    for L,nm in [(True,'LONGS '),(False,'SHORTS')]:
        sub=[t for t in T if t['L']==L]
        print(f"  {nm} accel:",stt([t for t in sub if t[key]<0])," | barrier:",stt([t for t in sub if t[key]>0]))
    print()
