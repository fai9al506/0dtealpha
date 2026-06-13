# -*- coding: utf-8 -*-
"""Larger sample (Mar17-Jun10 portal): does gamma favorability ADD to semi sizing?
Multi-expiry gamma (0+W+M) as-of 09:40/12:30. 5m semis. By month + combos."""
import os, warnings, math
warnings.filterwarnings("ignore")
from datetime import timedelta, time as dtime
from collections import defaultdict
import yfinance as yf
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
PATH=25; T=20.0; EXPS=('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
print("fetch 5m semis...",flush=True)
df=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)
cl=df['Close'].copy()
if cl.index.tz is not None: cl.index=cl.index.tz_convert('America/New_York').tz_localize(None)
cl=cl.between_time("09:30","16:00")
sbk=defaultdict(list)
for day,g in cl.groupby(cl.index.normalize()):
    op={t:g[t].dropna().iloc[0] for t in TK if g[t].dropna().shape[0]>0}
    for ts,row in g.iterrows():
        p=[(row[t]-op[t])/op[t]*100 for t in TK if t in op and not math.isnan(row[t])]
        if p: sbk[day.date().isoformat()].append((ts,sum(p)/len(p)))
START=min(sbk); END=max(sbk)
def semi_at(day,t):
    co=t-timedelta(minutes=5); a=sbk.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
print("range",START,END,flush=True)
# per-day indexed gamma maps (ts_utc range bounds; EDT = UTC-4 for Mar17+)
def gmap_day(day, utc_cut):
    rows=C.execute(text("""SELECT DISTINCT ON (expiration_option, strike) expiration_option, strike, value
      FROM volland_exposure_points WHERE greek='gamma' AND expiration_option=ANY(:e)
        AND ts_utc >= :d0 AND ts_utc <= :cut AND strike BETWEEN 6700 AND 7800
      ORDER BY expiration_option, strike, ts_utc DESC"""),
      {"e":list(EXPS),"d0":day+" 00:00:00+00","cut":day+" "+utc_cut+"+00"}).fetchall()
    m=defaultdict(float)
    for exp,k,v in rows: m[float(k)]+=float(v)/1e6
    return m
print("loading gamma maps (per-day)...",flush=True)
alldays=sorted(sbk.keys())
g940={d:gmap_day(d,"13:40:00") for d in alldays}   # 09:40 ET
g1230={d:gmap_day(d,"16:30:00") for d in alldays}   # 12:30 ET
def gfav(day,etn,L,spot):
    m = g1230.get(day) if (etn.time()>=dtime(13,0) and g1230.get(day)) else g940.get(day)
    if not m: return None
    above=sum(v for k,v in m.items() if spot<k<=spot+PATH); below=sum(v for k,v in m.items() if spot-PATH<=k<spot)
    return (below-above) if L else (above-below)
sig=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade, greek_alignment, spot, outcome_pnl
  FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short') ORDER BY ts ASC"""),{"a":START,"b":END}).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    L=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and L and (aa<0 or aa>=3): return False
    return True
last={}; T_=[]
for et,s,d,g,a,spot,pnl in sig:
    L=d in ('long','bullish'); k=(s,'L' if L else 'S')
    if k in last and (et-last[k])<timedelta(minutes=15): continue
    last[k]=et
    if not quality(s,d,g,a): continue
    day=et.date().isoformat(); etn=et.replace(tzinfo=None)
    fav=gfav(day,etn,L,float(spot))
    T_.append({"mo":day[:7],"L":L,"pnl":float(pnl)*5,"fav":fav,"sb":semi_at(day,etn)})
def gmult(t): return (2.0 if t['fav']>T else (0.5 if t['fav']<-T else 1.0)) if t['fav'] is not None else 1.0
def smult(t):
    sb=t['sb']
    if sb is None: return 1.0
    if (t['L'] and sb>0) or (not t['L'] and sb<0): return 2.0
    if (t['L'] and sb<0) or (not t['L'] and sb>0): return 0.5
    return 1.0
def comb_tie(t):
    sm=smult(t); return gmult(t) if sm==1.0 else sm
base=sum(t['pnl'] for t in T_); g=sum(t['pnl']*gmult(t) for t in T_); s=sum(t['pnl']*smult(t) for t in T_); ct=sum(t['pnl']*comb_tie(t) for t in T_)
print(f"\nLARGE sample {len(T_)} trades ({START}..{END}):")
print(f"  baseline:        ${base:+.0f}")
print(f"  GAMMA-only:      ${g:+.0f}  (uplift ${g-base:+.0f})")
print(f"  SEMI-only:       ${s:+.0f}  (uplift ${s-base:+.0f})")
print(f"  combine gamma-on-semi-neutral: ${ct:+.0f}  (uplift ${ct-base:+.0f})")
# gamma quality split
fav=[t for t in T_ if t['fav'] is not None and t['fav']>T]; unf=[t for t in T_ if t['fav'] is not None and t['fav']<-T]
def wr(x): return f"n={len(x)} WR={100*sum(1 for t in x if t['pnl']>0)/len(x):.0f}% ${sum(t['pnl'] for t in x)*1:+.0f}" if x else "n=0"
print(f"\n  gamma FAVORABLE: {wr(fav)} | UNFAVORABLE: {wr(unf)}")
# incremental: within semi-confirmed, does gamma further split?
sc=[t for t in T_ if smult(t)==2.0]
print(f"  within SEMI-CONFIRMED ({len(sc)}): gamma-fav {wr([t for t in sc if t['fav'] is not None and t['fav']>T])} | gamma-unfav {wr([t for t in sc if t['fav'] is not None and t['fav']<-T])}")
