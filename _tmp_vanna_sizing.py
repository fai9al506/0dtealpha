# -*- coding: utf-8 -*-
"""Test VANNA as a sizing factor on V16 (never tested before — only gamma was).
Vanna ALL bucket, favorability = below-above (and the INVERSE, since vanna sign
convention differs from gamma). Does vanna add over semi-only?"""
import os, warnings, math
warnings.filterwarnings("ignore")
from datetime import timedelta, time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict
import yfinance as yf
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
ET=ZoneInfo("America/New_York"); PATH=25; TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
def build(iv,pe):
    df=yf.download(TK,period=pe,interval=iv,progress=False,auto_adjust=True)['Close']
    if df.index.tz is not None: df.index=df.index.tz_convert(ET).tz_localize(None)
    df=df.between_time('09:30','16:00'); bk=defaultdict(list)
    for day,g in df.groupby(df.index.normalize()):
        op={t:g[t].dropna().iloc[0] for t in TK if g[t].dropna().shape[0]>0}
        for ts,row in g.iterrows():
            p=[(row[t]-op[t])/op[t]*100 for t in TK if t in op and not math.isnan(row[t])]
            if p: bk[day.date().isoformat()].append((ts,sum(p)/len(p)))
    return bk
print("semis...",flush=True); b5=build('5m','60d'); b1=build('1h','730d')
def semi_at(day,etn):
    src,lag=(b5,15) if day in b5 else (b1,60)
    co=etn-timedelta(minutes=lag); a=src.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
days=[r[0].isoformat() for r in C.execute(text("SELECT DISTINCT (ts AT TIME ZONE 'America/New_York')::date FROM setup_log WHERE live_pass=true ORDER BY 1")).fetchall()]
def daymap(day,utc_cut):
    rs=C.execute(text("""SELECT DISTINCT ON (strike) strike,value FROM volland_exposure_points
      WHERE greek='vanna' AND expiration_option='ALL' AND ts_utc>=:d0 AND ts_utc<=:cut AND strike BETWEEN 5500 AND 7800
      ORDER BY strike, ts_utc DESC"""),{"d0":day+" 00:00:00+00","cut":day+" "+utc_cut+"+00"}).fetchall()
    return {float(k):float(v)/1e6 for k,v in rs}
print(f"vanna ALL maps {len(days)}d...",flush=True)
M={d:(daymap(d,'13:40:00'),daymap(d,'16:30:00')) for d in days}
def vfav(day,etn,spot):
    mm=M.get(day);
    if not mm: return None
    m=mm[1] if (etn.time()>=dtime(13,0) and mm[1]) else mm[0]
    if not m: return None
    ab=sum(v for k,v in m.items() if spot<k<=spot+PATH); be=sum(v for k,v in m.items() if spot-PATH<=k<spot)
    return be-ab   # below - above (raw; we test both signs below)
rows=C.execute(text("""SELECT direction, ts, spot, outcome_pnl FROM setup_log
  WHERE live_pass=true AND outcome_pnl IS NOT NULL AND spot IS NOT NULL ORDER BY ts""")).mappings().all()
def sm(L,b):
    if b is None: return 1.0
    if (L and b>0) or (not L and b<0): return 2.0
    if (L and b<0) or (not L and b>0): return 0.5
    return 1.0
T_=[]
for r in rows:
    et=r['ts'].astimezone(ET); etn=et.replace(tzinfo=None); day=et.date().isoformat()
    L=r['direction'] in ('long','bullish'); usd=float(r['outcome_pnl'])*5
    T_.append({"L":L,"usd":usd,"sb":semi_at(day,etn),"vf":vfav(day,etn,float(r['spot']))})
# pick threshold ~ median abs vanna
import statistics
absv=[abs(t['vf']) for t in T_ if t['vf'] is not None]
TH=statistics.median(absv) if absv else 50
base=sum(t['usd'] for t in T_); semi=sum(t['usd']*sm(t['L'],t['sb']) for t in T_)
def wr(x): return f"n={len(x):>3} WR={100*sum(1 for t in x if t['usd']>0)/len(x):.0f}% ${sum(t['usd'] for t in x):+.0f}" if x else "n=0"
print(f"\n=== {len(T_)} V16 trades === baseline ${base:+.0f} | semi ${semi:+.0f} ({semi/base:.2f}x) | vanna-TH={TH:.0f}M")
for sign,lab in ((1,'vanna fav = below-above (like gamma)'),(-1,'vanna fav = above-below (inverse)')):
    def vm(L,vf):
        if vf is None: return 1.0
        f=sign*vf
        if (L and f>TH) or (not L and f<-TH): return 2.0
        if (L and f<-TH) or (not L and f>TH): return 0.5
        return 1.0
    vonly=sum(t['usd']*vm(t['L'],t['vf']) for t in T_)
    two=sum(t['usd']*max(0.375,min(2.5,sm(t['L'],t['sb'])*(1.25 if vm(t['L'],t['vf'])==2.0 else (0.75 if vm(t['L'],t['vf'])==0.5 else 1.0)))) for t in T_)
    fav=[t for t in T_ if t['vf'] is not None and sign*t['vf']*(1 if t['L'] else -1)>TH]
    unf=[t for t in T_ if t['vf'] is not None and sign*t['vf']*(1 if t['L'] else -1)<-TH]
    print(f"\n--- {lab} ---")
    print(f"  vanna-only ${vonly:+.0f} ({vonly/base:.2f}x) | 2-factor(semi+vanna) ${two:+.0f} ({two/base:.2f}x, vs semi {semi/base:.2f}x)")
    print(f"  FAV {wr(fav)} | UNFAV {wr(unf)}")
