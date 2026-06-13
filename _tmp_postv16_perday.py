# -*- coding: utf-8 -*-
"""Real post-V16 (May18-Jun10) per-day: baseline/semi/gamma/2-factor (BROKER fills).
Plus portal-vs-real contrast for Jun 9/10 (why the portal chart trends up on bleed days)."""
import os, json
from datetime import timedelta, time as dtime
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
PATH=25; T=20.0; LAG=20; EXPS=('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
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
    above=sum(v for k,v in prof.items() if s0<k<=s0+PATH); below=sum(v for k,v in prof.items() if s0-PATH<=k<s0)
    return (below-above) if L else (above-below)
rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-10' ORDER BY sl.ts"""),{}).fetchall()
def smult(L,sb):
    if sb is None: return 1.0
    if (L and sb>0) or (not L and sb<0): return 2.0
    if (L and sb<0) or (not L and sb>0): return 0.5
    return 1.0
def gadj(fav): return (1.25 if fav>T else (0.75 if fav<-T else 1.0)) if fav is not None else 1.0
pd=defaultdict(lambda:[0.0,0.0,0.0,0.0])
for et,setup,direction,st,spot in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5; etn=et.replace(tzinfo=None); d=etn.date().isoformat()
    sb=semi_at(d,etn); fav=gfav(d,etn,L,float(spot)); sm=smult(L,sb); two=max(0.375,min(2.5,sm*gadj(fav)))
    gm=(2.0 if (fav is not None and fav>T) else (0.5 if (fav is not None and fav<-T) else 1.0))
    P=pd[d]; P[0]+=usd; P[1]+=usd*sm; P[2]+=usd*gm; P[3]+=usd*two
print(f"REAL post-V16 per-day (broker $):")
print(f"{'day':<12}{'base':>8}{'semi':>8}{'gamma':>8}{'2factor':>9}{'d2f':>7}")
tb=ts=tg=t2=0
for d in sorted(pd):
    P=pd[d]; tb+=P[0]; ts+=P[1]; tg+=P[2]; t2+=P[3]
    print(f"{d:<12}{P[0]:>+8.0f}{P[1]:>+8.0f}{P[2]:>+8.0f}{P[3]:>+9.0f}{P[3]-P[0]:>+7.0f}")
print(f"{'TOTAL':<12}{tb:>+8.0f}{ts:>+8.0f}{tg:>+8.0f}{t2:>+9.0f}{t2-tb:>+7.0f}")
# portal vs real Jun 9/10
print("\n--- PORTAL (quality book) vs REAL broker, Jun 9 & 10 ---")
for d in ('2026-06-09','2026-06-10'):
    pr=C.execute(text("""SELECT COALESCE(SUM(outcome_pnl)*5,0), COUNT(*) FROM setup_log
      WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND outcome_pnl IS NOT NULL
        AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
        AND grade NOT IN ('C','LOG')"""),{"d":d}).fetchone()
    real=pd.get(d,[0])[0]
    print(f"  {d}: PORTAL quality book ${float(pr[0]):+.0f} ({pr[1]} signals)  |  REAL TSRT placed ${real:+.0f}")
# by setup direction on those days (portal)
for d in ('2026-06-09','2026-06-10'):
    bd=C.execute(text("""SELECT CASE WHEN setup_name='AG Short' OR direction IN ('short','bearish') THEN 'SHORT/AGS' ELSE 'LONG' END sd,
      setup_name, COALESCE(SUM(outcome_pnl)*5,0) FROM setup_log
      WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND outcome_pnl IS NOT NULL
        AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short') AND grade NOT IN ('C','LOG')
      GROUP BY 1,2 ORDER BY 3 DESC"""),{"d":d}).fetchall()
    print(f"  {d} portal by setup:", " | ".join(f"{r[1]}/{r[0]} ${float(r[2]):+.0f}" for r in bd))
