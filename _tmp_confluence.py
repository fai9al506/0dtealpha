"""CONFLUENCE test (user's 'cluster of expiries = sharper setups').
For each existing quality setup signal, count how many expiries (TODAY/THIS_WEEK/
THIRTY_NEXT_DAYS) have a SIGNIFICANT vanna node BACKING the trade direction near entry:
  LONG  -> negative-vanna SUPPORT floor within FLOORBAND below entry
  SHORT -> positive-vanna RESISTANCE wall within band above entry
Hypothesis: higher confluence count -> higher WR / PnL. IS (Mar-Apr) vs OOS (May-Jun).
No lookahead (per-expiry map as-of 09:40/noon). P&L=outcome_pnl*5.
"""
from datetime import datetime, timedelta, date as _date
from collections import defaultdict
import _tmp_l2l_engine as E
from sqlalchemy import text

MINV_EXP=3e7   # per-expiry significance
BAND=20        # how close the backing node must be to entry

_perexp_cache={}
def perexp_map(day, before_et):
    key=(day, before_et.hour)
    if key in _perexp_cache: return _perexp_cache[key]
    y,m,d=map(int,day.split("-")); d0=_date(y,m,d); d1=d0+timedelta(days=1)
    rows=E.CONN.execute(text("""
        SELECT DISTINCT ON (expiration_option, strike) expiration_option, strike, value
        FROM volland_exposure_points
        WHERE ts_utc >= :d0 AND ts_utc < :d1 AND greek='vanna'
          AND expiration_option IN ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
          AND (ts_utc AT TIME ZONE 'America/New_York') <= :bt
        ORDER BY expiration_option, strike, ts_utc DESC
    """),{"d0":d0.isoformat(),"d1":d1.isoformat(),"bt":before_et}).fetchall()
    mp=defaultdict(dict)
    for exp,strike,val in rows: mp[exp][float(strike)]=float(val)
    _perexp_cache[key]=mp; return mp

rows=E.CONN.execute(text("""
    SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
           greek_alignment, spot, outcome_pnl
    FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-03-01' AND DATE '2026-06-09'
      AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
      AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
    ORDER BY ts ASC""")).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    islong=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and islong and (aa<0 or aa>=3): return False
    return True
last={}; sigs=[]
for et,s,d,g,a,spot,pnl in rows:
    islong=d in ('long','bullish'); key=(s,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15): continue
    last[key]=et
    if not quality(s,d,g,a): continue
    sigs.append({"et":et,"day":et.date().isoformat(),"islong":islong,"spot":float(spot),"pnl":float(pnl)})

def confluence(sig):
    before = datetime.fromisoformat(sig["day"]).replace(hour=12,minute=30) if sig["et"].hour>=13 \
             else datetime.fromisoformat(sig["day"]).replace(hour=9,minute=40)
    mp=perexp_map(sig["day"], before); spot=sig["spot"]; cnt=0
    for exp in ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS'):
        d=mp.get(exp,{})
        if sig["islong"]:
            # support floor: significant NEGATIVE vanna within BAND below entry
            if any(spot-BAND<=k<=spot+5 and v<-MINV_EXP for k,v in d.items()): cnt+=1
        else:
            # resistance wall: significant POSITIVE vanna within BAND above entry
            if any(spot-5<=k<=spot+BAND and v>MINV_EXP for k,v in d.items()): cnt+=1
    return cnt

for s in sigs: s["conf"]=confluence(s)
def period(d): return "IS" if d<"2026-05-01" else "OOS"
def stt(ts):
    if not ts: return "n=  0"
    w=sum(1 for t in ts if t['pnl']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${sum(t['pnl'] for t in ts)*5:+7.0f} avg${sum(t['pnl'] for t in ts)*5/len(ts):+5.1f}"
for per in ("IS","OOS"):
    P=[s for s in sigs if period(s["day"])==per]
    print(f"\n===== {per} — {len(P)} signals — by confluence count (expiries backing the level) =====")
    for c in (0,1,2,3):
        print(f"  confluence={c}: {stt([s for s in P if s['conf']==c])}")
    print(f"  high (>=2):  {stt([s for s in P if s['conf']>=2])}")
    print(f"  low  (<=1):  {stt([s for s in P if s['conf']<=1])}")
