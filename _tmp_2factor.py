"""2-FACTOR SIZING (semi + gamma-for-longs) — the robust version.
  semi mult:  2.0 if semi-confirmed | 0.5 if semi-anti | 1.0 neutral/none
  gamma mult (LONGS only): 1.25 if near-spot 0DTE gamma NEGATIVE | 0.75 if positive
  size = semi_mult * gamma_mult   (shorts: gamma_mult=1)
PART A: portal sim (setup_log quality), IS(Apr)/OOS(May-Jun).
PART B: REAL TSRT placed trades post-V16 (broker fills), May18-Jun9.
"""
import os, json
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")

# gamma TODAY near-spot map, as-of 09:40/day
gq=text("""
  SELECT DISTINCT ON (d, strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
    SELECT ts_utc, strike, value, (ts_utc AT TIME ZONE 'America/New_York')::time tt
    FROM volland_exposure_points WHERE greek='gamma' AND expiration_option='TODAY'
      AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-04-11' AND DATE '2026-06-09'
      AND strike BETWEEN 6800 AND 7800
  ) q WHERE tt<=TIME '09:40' ORDER BY d, strike, ts_utc DESC""")
gamma=defaultdict(dict)
for d,s,v in C.execute(gq).fetchall(): gamma[d.isoformat()][float(s)]=float(v)
def gnet(day,spot):
    dd=gamma.get(day,{}); return sum(v for k,v in dd.items() if abs(k-spot)<=60)

basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
LAG=int(os.getenv("SEMI_LAG_MIN","0"))
def bstr(day,t):
    co=t-timedelta(minutes=LAG)
    a=bd.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None

def size(L, sb, day, spot):
    sm = 1.0
    if sb is not None:
        if (L and sb>0) or (not L and sb<0): sm=2.0
        elif (L and sb<0) or (not L and sb>0): sm=0.5
    gm = 1.0
    if L:
        g=gnet(day,spot); gm = 1.25 if g<0 else 0.75
    return sm*gm

def report(trades, title):
    base=sum(t['pnl'] for t in trades)
    blend=sum(t['pnl']*t['sz'] for t in trades)
    semi_only=sum(t['pnl']*t['ssz'] for t in trades)
    aw=sum(t['sz'] for t in trades)/len(trades) if trades else 0
    w=sum(1 for t in trades if t['pnl']>0)
    print(f"  {title}: n={len(trades)} WR={100*w/len(trades):.0f}%")
    print(f"     baseline 1x:      ${base:+.0f}")
    print(f"     semi-only sizing: ${semi_only:+.0f}")
    print(f"     2-FACTOR sizing:  ${blend:+.0f}  (avg {aw:.2f} contracts, +${blend-base:.0f} vs base)")

# ---------- PART A: portal sim ----------
sig=C.execute(text("""
  SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade, greek_alignment, spot, outcome_pnl
  FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE '2026-04-11'
    AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short') ORDER BY ts ASC""")).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    L=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and L and (aa<0 or aa>=3): return False
    return True
def ssize(L,sb):
    if sb is None: return 1.0
    if (L and sb>0) or (not L and sb<0): return 2.0
    if (L and sb<0) or (not L and sb>0): return 0.5
    return 1.0
last={}; A=[]
for et,s,d,g,a,spot,pnl in sig:
    L=d in ('long','bullish'); k=(s,'L' if L else 'S')
    if k in last and (et-last[k])<timedelta(minutes=15): continue
    last[k]=et
    if not quality(s,d,g,a): continue
    day=et.date().isoformat(); spot=float(spot); sb=bstr(day,et.replace(tzinfo=None))
    A.append({"day":day,"pnl":float(pnl)*5,"sz":size(L,sb,day,spot),"ssz":ssize(L,sb)})
print("===== PART A: PORTAL SIM =====")
report([t for t in A if t['day']<'2026-05-01'], "IS (Apr)")
report([t for t in A if t['day']>='2026-05-01'], "OOS (May-Jun)")

# ---------- PART B: REAL TSRT placed trades post-V16 (broker fills) ----------
rows=C.execute(text("""
  SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-09'
  ORDER BY sl.ts ASC""")).fetchall()
B=[]
for et,setup,direction,st,spot in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or '')) or (direction=='short') or (setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5
    day=et.date().isoformat(); sb=bstr(day,et.replace(tzinfo=None))
    B.append({"day":day,"pnl":usd,"sz":size(L,sb,day,float(spot)),"ssz":ssize(L,sb)})
print("\n===== PART B: REAL TSRT (broker $) post-V16 (May18-Jun9) =====")
report(B, "all placed")
