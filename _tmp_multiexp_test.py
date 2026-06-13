"""Fast combined test: does the MULTI-EXPIRY cluster sharpen our EXISTING setups?
Two refinements on SC/DD/ES-Abs/AG quality signals:
  (1) MAGNET-DIRECTION: trade dir agrees with dominant vanna node near spot (aligned vs counter)
  (2) CONFLUENCE: # of expiries (0DTE/weekly/monthly) backing the trade's level (term-stack)
One upfront query: per-(day,expiry,strike) latest vanna as-of ~09:40 ET (no lookahead
for signals after 9:40 — ~all of them). IS (Mar-Apr) vs OOS (May-Jun). P&L=outcome_pnl*5.
"""
from datetime import timedelta
from collections import defaultdict
import os
from sqlalchemy import create_engine, text
eng=create_engine(os.environ['DATABASE_URL'])
C=eng.connect().execution_options(isolation_level="AUTOCOMMIT")

print("pulling daily as-of-09:40 per-expiry vanna maps (one query)...", flush=True)
rows=C.execute(text("""
    SELECT DISTINCT ON (d, expiration_option, strike) d, expiration_option, strike, value FROM (
      SELECT (ts_utc AT TIME ZONE 'America/New_York')::date d,
             expiration_option, strike, value, ts_utc,
             (ts_utc AT TIME ZONE 'America/New_York')::time tt
      FROM volland_exposure_points
      WHERE greek='vanna' AND expiration_option IN ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
        AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-03-01' AND DATE '2026-06-09'
        AND strike BETWEEN 6800 AND 7800
    ) q WHERE tt <= TIME '09:40'
    ORDER BY d, expiration_option, strike, ts_utc DESC
""")).fetchall()
# day -> {strike: summed vanna}, and day -> {exp: {strike: value}}
summ=defaultdict(lambda: defaultdict(float)); perexp=defaultdict(lambda: defaultdict(dict))
for d,exp,strike,val in rows:
    ds=d.isoformat(); summ[ds][float(strike)]+=float(val); perexp[ds][exp][float(strike)]=float(val)
print(f"loaded maps for {len(summ)} days, {len(rows)} rows", flush=True)

sig_rows=C.execute(text("""
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
for et,s,d,g,a,spot,pnl in sig_rows:
    islong=d in ('long','bullish'); key=(s,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15): continue
    last[key]=et
    if not quality(s,d,g,a): continue
    sigs.append({"day":et.date().isoformat(),"islong":islong,"spot":float(spot),"pnl":float(pnl)})

MINV=6e7; BAND=70; MINV_EXP=3e7; FB=20
def magnet_align(s):
    m=summ.get(s["day"],{}); spot=s["spot"]
    cand=[(k,v) for k,v in m.items() if abs(k-spot)<=BAND and abs(v)>MINV and abs(k-spot)>=5]
    if not cand: return "none"
    k=max(cand,key=lambda x:abs(x[1]))[0]; md=1 if k>spot else -1
    return "aligned" if md==(1 if s["islong"] else -1) else "counter"
def conf(s):
    mp=perexp.get(s["day"],{}); spot=s["spot"]; c=0
    for exp in ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS'):
        d=mp.get(exp,{})
        if s["islong"]:
            if any(spot-FB<=k<=spot+5 and v<-MINV_EXP for k,v in d.items()): c+=1
        else:
            if any(spot-5<=k<=spot+FB and v>MINV_EXP for k,v in d.items()): c+=1
    return c
for s in sigs: s["align"]=magnet_align(s); s["conf"]=conf(s)
def per(d): return "IS" if d<"2026-05-01" else "OOS"
def stt(ts):
    if not ts: return "n=  0"
    w=sum(1 for t in ts if t['pnl']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${sum(t['pnl'] for t in ts)*5:+7.0f} avg${sum(t['pnl'] for t in ts)*5/len(ts):+5.1f}"

print(f"\nTotal quality signals: {len(sigs)}")
for label in ("IS","OOS"):
    P=[s for s in sigs if per(s["day"])==label]
    print(f"\n############ {label} ({'Mar-Apr' if label=='IS' else 'May-Jun'}) — {len(P)} signals ############")
    print(" -- MAGNET direction --")
    for al in ("aligned","counter","none"): print(f"   {al:<8}: {stt([s for s in P if s['align']==al])}")
    base=sum(s['pnl'] for s in P)*5; kept=sum(s['pnl'] for s in P if s['align']!='counter')*5
    print(f"   BASELINE all: ${base:+.0f} | drop counter: ${kept:+.0f} (delta ${kept-base:+.0f})")
    print(" -- CONFLUENCE (expiries backing the level) --")
    for c in (0,1,2,3): print(f"   conf={c}: {stt([s for s in P if s['conf']==c])}")
    print(f"   high>=2: {stt([s for s in P if s['conf']>=2])} | low<=1: {stt([s for s in P if s['conf']<=1])}")
