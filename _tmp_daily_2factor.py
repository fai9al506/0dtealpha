"""Per-day post-V16 (May18-Jun9): baseline broker$ vs 2-factor-sized broker$,
with the drivers (semi basket direction, longs/shorts, confirmed/anti) so we can
explain WHY each day changed.
"""
import os, json
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")

# gamma TODAY near-spot, as-of 09:40
gq=text("""SELECT DISTINCT ON (d,strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
   SELECT ts_utc,strike,value,(ts_utc AT TIME ZONE 'America/New_York')::time tt FROM volland_exposure_points
   WHERE greek='gamma' AND expiration_option='TODAY'
     AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-09'
     AND strike BETWEEN 6800 AND 7800) q WHERE tt<=TIME '09:40' ORDER BY d,strike,ts_utc DESC""")
gamma=defaultdict(dict)
for d,s,v in C.execute(gq).fetchall(): gamma[d.isoformat()][float(s)]=float(v)
def gnet(day,spot): dd=gamma.get(day,{}); return sum(v for k,v in dd.items() if abs(k-spot)<=60)

basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
def bstr(day,t): a=bd.get(day,[]); p=[v for (x,v) in a if x<=t]; return p[-1] if p else None
def bavg(day): a=bd.get(day,[]); return sum(v for _,v in a)/len(a) if a else None

def size(L,sb,day,spot):
    sm=1.0
    if sb is not None:
        if (L and sb>0) or (not L and sb<0): sm=2.0
        elif (L and sb<0) or (not L and sb>0): sm=0.5
    gm=1.0
    if L: gm=1.25 if gnet(day,spot)<0 else 0.75
    return sm

rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-09'
   ORDER BY sl.ts ASC""")).fetchall()
day=defaultdict(lambda:{"base":0.0,"two":0.0,"nL":0,"nS":0,"conf":0,"anti":0,"n":0})
for et,setup,direction,st,spot in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5
    d=et.date().isoformat(); sb=bstr(d,et.replace(tzinfo=None)); sz=size(L,sb,d,float(spot))
    D=day[d]; D["base"]+=usd; D["two"]+=usd*sz; D["n"]+=1
    D["nL"]+= 1 if L else 0; D["nS"]+= 0 if L else 1
    if sb is not None:
        if (L and sb>0) or (not L and sb<0): D["conf"]+=1
        elif (L and sb<0) or (not L and sb>0): D["anti"]+=1

print(f"{'day':<12}{'n':>3}{'L/S':>6}{'semi%':>7}{'cf/an':>7}{'BASE$':>8}{'2FAC$':>8}{'delta':>8}")
tb=tt=0
for d in sorted(day):
    D=day[d]; ba=bavg(d)
    print(f"{d:<12}{D['n']:>3}{str(D['nL'])+'/'+str(D['nS']):>6}{(f'{ba:+.1f}' if ba is not None else '-'):>7}"
          f"{str(D['conf'])+'/'+str(D['anti']):>7}{D['base']:>+8.0f}{D['two']:>+8.0f}{D['two']-D['base']:>+8.0f}")
    tb+=D['base']; tt+=D['two']
print(f"{'TOTAL':<12}{'':>3}{'':>6}{'':>7}{'':>7}{tb:>+8.0f}{tt:>+8.0f}{tt-tb:>+8.0f}")
