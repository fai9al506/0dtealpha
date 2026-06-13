# -*- coding: utf-8 -*-
"""DEFENSIVE-only sizing re-test, ALL MONTHS, no look-ahead, with concentration.
  defensive: semi 0.5x if fighting / else 1x ; gamma(longs) 0.75x if +gamma / else 1x  (range 0.375-1x, NEVER >1)
  full (compare): semi 0.5/1/2 ; gamma 0.75/1.25
1h semis lagged 60min (no look-ahead, uniform Mar-Jun). Portal P&L. Per-month + concentration.
"""
import os, warnings, math
warnings.filterwarnings("ignore")
from datetime import timedelta, time as dtime
from collections import defaultdict
import yfinance as yf
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
START,END="2026-03-01","2026-06-10"; SLAG=60

print("fetching 1h semis...",flush=True)
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
df=yf.download(TK,start="2026-02-20",end="2026-06-11",interval='1h',progress=False,auto_adjust=True)
cl=df['Close'].copy()
if cl.index.tz is not None: cl.index=cl.index.tz_convert('America/New_York').tz_localize(None)
cl=cl.between_time("09:30","16:00")
sb=defaultdict(list)
for day,g in cl.groupby(cl.index.normalize()):
    op={t:g[t].dropna().iloc[0] for t in TK if g[t].dropna().shape[0]>0}
    for ts,row in g.iterrows():
        p=[(row[t]-op[t])/op[t]*100 for t in TK if t in op and not math.isnan(row[t])]
        if p: sb[day.date().isoformat()].append((ts,sum(p)/len(p)))
def semi_at(day,t):
    co=t-timedelta(minutes=SLAG); a=sb.get(day,[]); pr=[v for (x,v) in a if x<=co]; return pr[-1] if pr else None

def gmap(cut):
    q=text("""SELECT DISTINCT ON (d,strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
      SELECT ts_utc,strike,value,(ts_utc AT TIME ZONE 'America/New_York')::time tt FROM volland_exposure_points
      WHERE greek='gamma' AND expiration_option='TODAY' AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b
        AND strike BETWEEN 6300 AND 7800) q WHERE tt<=TIME :c ORDER BY d,strike,ts_utc DESC""")
    m=defaultdict(dict)
    for d,s,v in C.execute(q,{"a":START,"b":END,"c":cut}).fetchall(): m[d.isoformat()][float(s)]=float(v)
    return m
g940=gmap("09:40"); g1230=gmap("12:30")
def gnet(day,spot,t):
    et=(t-timedelta(minutes=60)).time(); m=None
    if et>=dtime(12,30): m=g1230.get(day) or g940.get(day)
    elif et>=dtime(9,40): m=g940.get(day)
    else: return None
    return sum(v for k,v in m.items() if abs(k-spot)<=60) if m else None

def defensive(L,sb_,g):
    sm=0.5 if (sb_ is not None and ((L and sb_<0) or (not L and sb_>0))) else 1.0
    gm=0.75 if (L and g is not None and g>=0) else 1.0
    return sm*gm
def full(L,sb_,g):
    sm=1.0
    if sb_ is not None:
        if (L and sb_>0) or (not L and sb_<0): sm=2.0
        elif (L and sb_<0) or (not L and sb_>0): sm=0.5
    gm=(1.25 if g<0 else 0.75) if (L and g is not None) else 1.0
    return sm*gm

sig=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade, greek_alignment, spot, outcome_pnl
  FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b
    AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short') ORDER BY ts ASC"""),{"a":START,"b":END}).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    L=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and L and (aa<0 or aa>=3): return False
    return True
last={}; pd=defaultdict(lambda:[0.0,0.0,0.0])  # base, defensive, full
for et,s,d,g,a,spot,pnl in sig:
    L=d in ('long','bullish'); k=(s,'L' if L else 'S')
    if k in last and (et-last[k])<timedelta(minutes=15): continue
    last[k]=et
    if not quality(s,d,g,a): continue
    day=et.date().isoformat(); etn=et.replace(tzinfo=None); usd=float(pnl)*5
    sbv=semi_at(day,etn); gv=gnet(day,float(spot),etn)
    P=pd[day]; P[0]+=usd; P[1]+=usd*defensive(L,sbv,gv); P[2]+=usd*full(L,sbv,gv)

days=sorted(pd)
def concen(deltas):
    tot=sum(deltas.values()); s=sorted(deltas.values(),reverse=True)
    pos=sum(1 for v in deltas.values() if v>0); neg=sum(1 for v in deltas.values() if v<-1)
    top=s[0] if s else 0
    return tot, pos, neg, top, tot-sum(s[:3])
print(f"\n{'month':<9}{'base':>8}{'DEFENSIVE':>11}{'full':>8}   defensive-uplift concentration")
mo=defaultdict(lambda:[0.0,0.0,0.0]); ddef=defaultdict(dict); dful=defaultdict(dict)
for d in days:
    P=pd[d]; m=d[:7]; mo[m][0]+=P[0]; mo[m][1]+=P[1]; mo[m][2]+=P[2]
    ddef[m][d]=P[1]-P[0]; dful[m][d]=P[2]-P[0]
for m in sorted(mo):
    b,de,fu=mo[m]
    tot,pos,neg,top,mt3=concen(ddef[m])
    print(f"{m:<9}{b:>+8.0f}{de:>+11.0f}{fu:>+8.0f}   d-def ${tot:+.0f} (pos{pos}/neg{neg}, top day ${top:+.0f}, minus-top3 ${mt3:+.0f})")
# overall concentration of DEFENSIVE
alldef={d:pd[d][1]-pd[d][0] for d in days}
tot,pos,neg,top,mt3=concen(alldef)
allful={d:pd[d][2]-pd[d][0] for d in days}
tf,pf,nf,topf,mt3f=concen(allful)
print(f"\nFULL period ({len(days)} days):")
print(f"  baseline ${sum(p[0] for p in pd.values()):+.0f}")
print(f"  DEFENSIVE ${sum(p[1] for p in pd.values()):+.0f}  | uplift ${tot:+.0f}, pos {pos}/neg {neg}, top day ${top:+.0f} ({100*top/tot if tot else 0:.0f}%), minus-top3 ${mt3:+.0f}")
print(f"  FULL      ${sum(p[2] for p in pd.values()):+.0f}  | uplift ${tf:+.0f}, pos {pf}/neg {nf}, top day ${topf:+.0f} ({100*topf/tf if tf else 0:.0f}%), minus-top3 ${mt3f:+.0f}")
