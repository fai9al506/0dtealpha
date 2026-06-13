# -*- coding: utf-8 -*-
"""Clean 5-min re-stress Apr-Jun (yfinance 5m, lag 5min). Plain semi-only sizing.
By month + concentration -> does the defensive cost in grind months with clean data?"""
import os, warnings, math
warnings.filterwarnings("ignore")
from datetime import timedelta
from collections import defaultdict
import yfinance as yf
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
print("fetch 5m semis (60d)...",flush=True)
df=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)
cl=df['Close'].copy()
if cl.index.tz is not None: cl.index=cl.index.tz_convert('America/New_York').tz_localize(None)
cl=cl.between_time("09:30","16:00")
sb=defaultdict(list)
for day,g in cl.groupby(cl.index.normalize()):
    op={t:g[t].dropna().iloc[0] for t in TK if g[t].dropna().shape[0]>0}
    for ts,row in g.iterrows():
        p=[(row[t]-op[t])/op[t]*100 for t in TK if t in op and not math.isnan(row[t])]
        if p: sb[day.date().isoformat()].append((ts,sum(p)/len(p)))
mind=min(sb) if sb else None; maxd=max(sb) if sb else None
print("5m semi coverage:",mind,"->",maxd,flush=True)
def semi(day,t):
    co=t-timedelta(minutes=5); a=sb.get(day,[]); pr=[v for (x,v) in a if x<=co]; return pr[-1] if pr else None

sig=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade, greek_alignment, outcome_pnl
  FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b
    AND outcome_pnl IS NOT NULL AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short') ORDER BY ts ASC"""),
  {"a":mind,"b":maxd}).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    L=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and L and (aa<0 or aa>=3): return False
    return True
def sz(L,b):
    if b is None: return 1.0
    if (L and b>0) or (not L and b<0): return 2.0
    if (L and b<0) or (not L and b>0): return 0.5
    return 1.0
last={}; pd=defaultdict(lambda:[0.0,0.0])
for et,s,d,g,a,pnl in sig:
    L=d in ('long','bullish'); k=(s,'L' if L else 'S')
    if k in last and (et-last[k])<timedelta(minutes=15): continue
    last[k]=et
    if not quality(s,d,g,a): continue
    day=et.date().isoformat(); etn=et.replace(tzinfo=None); usd=float(pnl)*5
    P=pd[day]; P[0]+=usd; P[1]+=usd*sz(L,semi(day,etn))
mo=defaultdict(lambda:[0.0,0.0]); dl=defaultdict(dict)
for d in pd:
    m=d[:7]; mo[m][0]+=pd[d][0]; mo[m][1]+=pd[d][1]; dl[m][d]=pd[d][1]-pd[d][0]
print(f"\n{'month':<9}{'baseline':>9}{'sized':>8}{'uplift':>8}{'posD/negD':>11}{'topDay%':>9}")
for m in sorted(mo):
    b,sd=mo[m]; up=sd-b; dd=dl[m]
    pos=sum(1 for v in dd.values() if v>0); neg=sum(1 for v in dd.values() if v<-1)
    top=max(dd.values()) if dd else 0; topp=100*top/up if up else 0
    print(f"{m:<9}{b:>+9.0f}{sd:>+8.0f}{up:>+8.0f}{str(pos)+'/'+str(neg):>11}{topp:>8.0f}%")
tb=sum(p[0] for p in pd.values()); ts=sum(p[1] for p in pd.values())
alldl={d:pd[d][1]-pd[d][0] for d in pd}; up=ts-tb
pos=sum(1 for v in alldl.values() if v>0); neg=sum(1 for v in alldl.values() if v<-1)
s=sorted(alldl.values(),reverse=True)
print(f"\nFULL ({len(pd)} days): baseline ${tb:+.0f} | sized ${ts:+.0f} | uplift ${up:+.0f} ({100*ts/tb if tb else 0:.0f}% of base)")
print(f"  pos {pos}/neg {neg} days | top day {100*s[0]/up if up else 0:.0f}% | minus-top3 ${up-sum(s[:3]):+.0f}")
