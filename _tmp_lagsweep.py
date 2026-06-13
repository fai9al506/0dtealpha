# -*- coding: utf-8 -*-
"""Isolate how much of the degradation is the conservative 1h-bar lag vs real edge.
Sweep semi LAG on all-months portal full-2factor. Also post-V16 15m concentration."""
import os, warnings, math
warnings.filterwarnings("ignore")
from datetime import timedelta, time as dtime
from collections import defaultdict
import yfinance as yf
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
START,END="2026-03-01","2026-06-10"
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
print("fetch 1h semis...",flush=True)
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
def semi_at(day,t,lag):
    co=t-timedelta(minutes=lag); a=sb.get(day,[]); pr=[v for (x,v) in a if x<=co]; return pr[-1] if pr else None
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
def full(L,sb_,g):
    sm=1.0
    if sb_ is not None:
        if (L and sb_>0) or (not L and sb_<0): sm=2.0
        elif (L and sb_<0) or (not L and sb_>0): sm=0.5
    gm=(1.25 if g<0 else 0.75) if (L and g is not None) else 1.0
    return sm*gm
sig=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade, greek_alignment, spot, outcome_pnl
  FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
    AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short') ORDER BY ts ASC"""),{"a":START,"b":END}).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    L=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and L and (aa<0 or aa>=3): return False
    return True
trades=[]
last={}
for et,s,d,g,a,spot,pnl in sig:
    L=d in ('long','bullish'); k=(s,'L' if L else 'S')
    if k in last and (et-last[k])<timedelta(minutes=15): continue
    last[k]=et
    if not quality(s,d,g,a): continue
    trades.append((et.date().isoformat(),et.replace(tzinfo=None),L,float(spot),float(pnl)*5))
base=sum(t[4] for t in trades)
print(f"\nAll-months portal, {len(trades)} trades, baseline ${base:+.0f}")
print(f"{'lag(min)':>9}{'2factor$':>11}{'uplift':>9}{'posDays':>9}{'negDays':>9}{'topDay%':>9}")
for lag in (0,10,15,30,60):
    pd=defaultdict(lambda:[0.0,0.0])
    for day,etn,L,spot,usd in trades:
        sz=full(L,semi_at(day,etn,lag),gnet(day,spot,etn))
        pd[day][0]+=usd; pd[day][1]+=usd*sz
    tot=sum(p[1] for p in pd.values()); dl={d:pd[d][1]-pd[d][0] for d in pd}
    up=tot-base; pos=sum(1 for v in dl.values() if v>0); neg=sum(1 for v in dl.values() if v<-1)
    top=max(dl.values()); topp=100*top/up if up else 0
    print(f"{lag:>9}{tot:>+11.0f}{up:>+9.0f}{pos:>9}{neg:>9}{topp:>8.0f}%")
