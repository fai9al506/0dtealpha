# -*- coding: utf-8 -*-
"""Semi+gamma sizing on the CORRECT V16 set (setup_log.live_pass=true, 920 trades).
Semis: 5m (Mar17+) / 1h fallback (Feb+). Multi-expiry gamma favorability."""
import os, warnings, math
warnings.filterwarnings("ignore")
from datetime import timedelta, time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict
import yfinance as yf
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
ET=ZoneInfo("America/New_York"); PATH=25; TH=20.0; EXPS=('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS'); TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
rows=C.execute(text("""SELECT direction, ts, spot, outcome_pnl FROM setup_log
  WHERE live_pass=true AND outcome_pnl IS NOT NULL AND spot IS NOT NULL ORDER BY ts""")).mappings().all()
print(f"V16 trades (live_pass): {len(rows)}  pts {sum(float(r['outcome_pnl']) for r in rows):+.1f}",flush=True)
def build(interval,period):
    df=yf.download(TK,period=period,interval=interval,progress=False,auto_adjust=True)['Close']
    if df.index.tz is not None: df.index=df.index.tz_convert(ET).tz_localize(None)
    df=df.between_time("09:30","16:00"); bk=defaultdict(list)
    for day,g in df.groupby(df.index.normalize()):
        op={t:g[t].dropna().iloc[0] for t in TK if g[t].dropna().shape[0]>0}
        for ts,row in g.iterrows():
            p=[(row[t]-op[t])/op[t]*100 for t in TK if t in op and not math.isnan(row[t])]
            if p: bk[day.date().isoformat()].append((ts,sum(p)/len(p)))
    return bk
print("semis 5m+1h...",flush=True)
b5=build('5m','60d'); b1=build('1h','730d')
def semi_at(day,etn):
    src,lag=(b5,15) if day in b5 else (b1,60)
    co=etn-timedelta(minutes=lag); a=src.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None
def gmap_day(day,cut):
    rs=C.execute(text("""SELECT DISTINCT ON (expiration_option,strike) expiration_option,strike,value FROM volland_exposure_points
      WHERE greek='gamma' AND expiration_option=ANY(:e) AND ts_utc>=:d0 AND ts_utc<=:cut AND strike BETWEEN 5500 AND 7800
      ORDER BY expiration_option,strike,ts_utc DESC"""),{"e":list(EXPS),"d0":day+" 00:00:00+00","cut":day+" "+cut+"+00"}).fetchall()
    m=defaultdict(float)
    for e,k,v in rs: m[float(k)]+=float(v)/1e6
    return m
vdays=sorted({r['ts'].astimezone(ET).date().isoformat() for r in rows})
print(f"gamma maps {len(vdays)} days...",flush=True)
g940={d:gmap_day(d,"13:40:00") for d in vdays}; g1230={d:gmap_day(d,"16:30:00") for d in vdays}
def gfav(day,etn,L,spot):
    m=g1230.get(day) if (etn.time()>=dtime(13,0) and g1230.get(day)) else g940.get(day)
    if not m: return None
    return (sum(v for k,v in m.items() if spot-PATH<=k<spot)-sum(v for k,v in m.items() if spot<k<=spot+PATH)) if L \
           else (sum(v for k,v in m.items() if spot<k<=spot+PATH)-sum(v for k,v in m.items() if spot-PATH<=k<spot))
def sm(L,sb):
    if sb is None: return 1.0
    if (L and sb>0) or (not L and sb<0): return 2.0
    if (L and sb<0) or (not L and sb>0): return 0.5
    return 1.0
def gm(f): return (2.0 if f>TH else (0.5 if f<-TH else 1.0)) if f is not None else 1.0
def gadj(f): return (1.25 if f>TH else (0.75 if f<-TH else 1.0)) if f is not None else 1.0
pdv=defaultdict(lambda:[0.0,0.0,0.0,0.0]); T_=[]
for r in rows:
    et=r['ts'].astimezone(ET); etn=et.replace(tzinfo=None); day=et.date().isoformat()
    L=r['direction'] in ('long','bullish'); usd=float(r['outcome_pnl'])*5
    sb=semi_at(day,etn); f=gfav(day,etn,L,float(r['spot'])); s=sm(L,sb)
    P=pdv[day]; P[0]+=usd; P[1]+=usd*s; P[2]+=usd*gm(f); P[3]+=usd*max(0.375,min(2.5,s*gadj(f)))
    T_.append({"L":L,"usd":usd,"f":f,"s":s})
B,S,G,TW=[sum(p[i] for p in pdv.values()) for i in range(4)]
mo=defaultdict(lambda:[0,0,0,0])
for d in pdv:
    for i in range(4): mo[d[:7]][i]+=pdv[d][i]
print(f"\n===== SIZING on CORRECT V16 set ({len(rows)} trades, portal-pnl x$5) =====")
print(f"{'month':<9}{'base':>8}{'semi':>8}{'gamma':>8}{'2fac':>8}")
for m in sorted(mo): print(f"{m:<9}"+"".join(f"{mo[m][i]:>+8.0f}" for i in range(4)))
print(f"{'TOTAL':<9}{B:>+8.0f}{S:>+8.0f}{G:>+8.0f}{TW:>+8.0f}")
print(f"  semi {S/B:.2f}x (+${S-B:.0f}) | gamma +${G-B:.0f} | 2factor {TW/B:.2f}x (+${TW-B:.0f})")
dl=sorted((pdv[d][3]-pdv[d][0] for d in pdv),reverse=True); up=TW-B
print(f"  2factor concentration: top day {100*dl[0]/up if up else 0:.0f}%, minus-top3 ${up-sum(dl[:3]):+.0f}, pos{sum(1 for v in dl if v>0)}/neg{sum(1 for v in dl if v<-1)}")
sc=[t for t in T_ if t['s']==2.0]
def wr(x): return f"n={len(x)} WR={100*sum(1 for t in x if t['usd']>0)/len(x):.0f}% ${sum(t['usd'] for t in x):+.0f}" if x else "n=0"
print(f"  gamma FAV {wr([t for t in T_ if t['f'] is not None and t['f']>TH])} | UNFAV {wr([t for t in T_ if t['f'] is not None and t['f']<-TH])}")
print(f"  within semi-conf: g-fav {wr([t for t in sc if t['f'] is not None and t['f']>TH])} | g-unfav {wr([t for t in sc if t['f'] is not None and t['f']<-TH])}")
