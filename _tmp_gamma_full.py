# -*- coding: utf-8 -*-
"""Full structural gamma (user's complete rule, multi-expiry 0+W+M):
  LONG favorability = gamma_BELOW - gamma_ABOVE  (+G below=support, +G above=resist,
     -G above=accel up, -G below=no floor). SHORT = mirror.
Backtest as a quality split + sizing rule, and COMBINED with semi-sizing. Post-V16 real broker."""
import os, json
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
PATH=25; T=20.0; LAG=20
EXPS=('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
# semis
basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bdd=defaultdict(list)
for et,v in basket: bdd[et.date().isoformat()].append((et,v))
def semi_at(day,t):
    co=t-timedelta(minutes=LAG); a=bdd.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
days=[r[0].isoformat() for r in C.execute(text("""SELECT DISTINCT (s.ts AT TIME ZONE 'America/New_York')::date
  FROM real_trade_orders r JOIN setup_log s ON s.id=r.setup_log_id
  WHERE (s.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY 1""")).fetchall()]
gday={}
for d in days:
    perexp={e:defaultdict(dict) for e in EXPS}; sp={}
    gr=C.execute(text("""SELECT (ts_utc AT TIME ZONE 'America/New_York') et, expiration_option, strike, value, current_price
      FROM volland_exposure_points WHERE greek='gamma' AND expiration_option=ANY(:e)
        AND (ts_utc AT TIME ZONE 'America/New_York')::date=DATE :d AND strike BETWEEN 7100 AND 7800 ORDER BY ts_utc"""),{"d":d,"e":list(EXPS)}).fetchall()
    for et,exp,k,v,cp in gr:
        key=et.replace(second=0,microsecond=0); perexp[exp][key][float(k)]=float(v)/1e6
        if cp and exp=='TODAY': sp[key]=float(cp)
    gday[d]=({e:sorted(perexp[e]) for e in EXPS},perexp,sp)
def gfav(d,etn,L,spot):
    keys,perexp,sp=gday[d]; spk=[t for t in keys['TODAY'] if t<=etn]; s0=(sp.get(spk[-1]) if spk else None) or spot
    prof=defaultdict(float)
    for e in EXPS:
        pr=[t for t in keys[e] if t<=etn]
        if pr:
            for k,v in perexp[e][pr[-1]].items(): prof[k]+=v
    if not prof: return None
    above=sum(v for k,v in prof.items() if s0<k<=s0+PATH)
    below=sum(v for k,v in prof.items() if s0-PATH<=k<s0)
    return (below-above) if L else (above-below)
rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY sl.ts"""),{}).fetchall()
T_=[]
for et,setup,direction,st,spot in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5; etn=et.replace(tzinfo=None); d=etn.date().isoformat()
    fav=gfav(d,etn,L,float(spot))
    if fav is None: continue
    T_.append({"L":L,"pnl":usd,"fav":fav,"sb":semi_at(d,etn)})
def stt(ts):
    if not ts: return "n=0"
    w=sum(1 for t in ts if t['pnl']>0); s=sum(t['pnl'] for t in ts)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${s:+6.0f} avg${s/len(ts):+5.1f}"
print(f"trades {len(T_)}  (gamma favorability = gamma_below - gamma_above, multi-expiry)\n")
print("QUALITY SPLIT:")
print("  FAVORABLE (fav>+%d):  "%T, stt([t for t in T_ if t['fav']>T]))
print("  NEUTRAL:              ", stt([t for t in T_ if -T<=t['fav']<=T]))
print("  UNFAVORABLE (fav<-%d):"%T, stt([t for t in T_ if t['fav']<-T]))
# sizing
def gmult(t): return 2.0 if t['fav']>T else (0.5 if t['fav']<-T else 1.0)
def smult(t):
    sb=t['sb']
    if sb is None: return 1.0
    if (t['L'] and sb>0) or (not t['L'] and sb<0): return 2.0
    if (t['L'] and sb<0) or (not t['L'] and sb>0): return 0.5
    return 1.0
base=sum(t['pnl'] for t in T_)
gonly=sum(t['pnl']*gmult(t) for t in T_)
sonly=sum(t['pnl']*smult(t) for t in T_)
comb_avg=sum(t['pnl']*((smult(t)+gmult(t))/2) for t in T_)
comb_mult=sum(t['pnl']*max(0.375,min(2.5,smult(t)*gmult(t))) for t in T_)
# semi primary, gamma only breaks ties when semi neutral (1x)
def comb_tie(t):
    s=smult(t)
    return gmult(t) if s==1.0 else s
comb_tieb=sum(t['pnl']*comb_tie(t) for t in T_)
# semi primary, gamma only VETOES (caps to 1x) a semi-2x long that gamma says unfavorable
def comb_veto(t):
    s=smult(t)
    if s==2.0 and gmult(t)==0.5: return 1.0   # both strong-disagree -> neutralize
    return s
comb_v=sum(t['pnl']*comb_veto(t) for t in T_)
print(f"\nSIZING (post-V16 real broker $):")
print(f"  baseline 1x:              ${base:+.0f}")
print(f"  GAMMA-only:               ${gonly:+.0f}  (uplift ${gonly-base:+.0f})")
print(f"  SEMI-only:                ${sonly:+.0f}  (uplift ${sonly-base:+.0f})")
print(f"  combine: average mult:    ${comb_avg:+.0f}  (uplift ${comb_avg-base:+.0f})")
print(f"  combine: multiply(capped):${comb_mult:+.0f}  (uplift ${comb_mult-base:+.0f})")
print(f"  combine: gamma on semi-neutral: ${comb_tieb:+.0f}  (uplift ${comb_tieb-base:+.0f})")
print(f"  combine: gamma VETO semi-2x conflicts: ${comb_v:+.0f}  (uplift ${comb_v-base:+.0f})")
