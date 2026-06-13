# -*- coding: utf-8 -*-
"""REDO gamma study with CORRECT bucket. Old gamma_fav summed nested cumulative buckets
(TODAY+WEEK+30DAYS = double-count + missed ALL). Correct total = ALL (or 0DTE-only TODAY).
Re-test on V16 (live_pass): does properly-computed gamma add value over semi?"""
import os, warnings, math
warnings.filterwarnings("ignore")
from datetime import timedelta, time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict
import yfinance as yf
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
ET=ZoneInfo("America/New_York"); PATH=25; TH=20.0; TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
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
# gamma maps per bucket, as-of 09:40 / 12:30
def gmap(bucket,cut):
    rs=C.execute(text("""SELECT DISTINCT ON (strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
      SELECT ts_utc,strike,value,(ts_utc AT TIME ZONE 'America/New_York')::date dd,(ts_utc AT TIME ZONE 'America/New_York')::time tt
      FROM volland_exposure_points WHERE greek='gamma' AND expiration_option=:b AND strike BETWEEN 5500 AND 7800) q
      WHERE tt<=TIME :c ORDER BY strike, ts_utc DESC"""),{"b":bucket,"c":cut})
    # NOTE simplistic: latest per strike <= cut across all days -> wrong for history; fine for per-day below
    return rs
# per-day approach (correct): for each V16 day, fetch latest ALL/TODAY per strike <= 09:40 & 12:30
days=[r[0].isoformat() for r in C.execute(text("""SELECT DISTINCT (s.ts AT TIME ZONE 'America/New_York')::date
  FROM setup_log s WHERE s.live_pass=true ORDER BY 1""")).fetchall()]
def daymap(day,bucket,utc_cut):
    rs=C.execute(text("""SELECT DISTINCT ON (strike) strike,value FROM volland_exposure_points
      WHERE greek='gamma' AND expiration_option=:b AND ts_utc>=:d0 AND ts_utc<=:cut AND strike BETWEEN 5500 AND 7800
      ORDER BY strike, ts_utc DESC"""),{"b":bucket,"d0":day+" 00:00:00+00","cut":day+" "+utc_cut+"+00"}).fetchall()
    return {float(k):float(v)/1e6 for k,v in rs}
print(f"gamma maps {len(days)}d x2 buckets...",flush=True)
M={}
for d in days:
    M[d]={'ALL':(daymap(d,'ALL','13:40:00'),daymap(d,'ALL','16:30:00')),
          'TODAY':(daymap(d,'TODAY','13:40:00'),daymap(d,'TODAY','16:30:00'))}
def gfav(day,etn,L,spot,bucket):
    mm=M.get(day)
    if not mm: return None
    m=mm[bucket][1] if (etn.time()>=dtime(13,0) and mm[bucket][1]) else mm[bucket][0]
    if not m: return None
    ab=sum(v for k,v in m.items() if spot<k<=spot+PATH); be=sum(v for k,v in m.items() if spot-PATH<=k<spot)
    return (be-ab) if L else (ab-be)
rows=C.execute(text("""SELECT direction, ts, spot, outcome_pnl FROM setup_log
  WHERE live_pass=true AND outcome_pnl IS NOT NULL AND spot IS NOT NULL ORDER BY ts""")).mappings().all()
def sm(L,b):
    if b is None: return 1.0
    if (L and b>0) or (not L and b<0): return 2.0
    if (L and b<0) or (not L and b>0): return 0.5
    return 1.0
def gm(f): return (2.0 if f>TH else (0.5 if f<-TH else 1.0)) if f is not None else 1.0
def gadj(f): return (1.25 if f>TH else (0.75 if f<-TH else 1.0)) if f is not None else 1.0
T_=[]
for r in rows:
    et=r['ts'].astimezone(ET); etn=et.replace(tzinfo=None); day=et.date().isoformat()
    L=r['direction'] in ('long','bullish'); usd=float(r['outcome_pnl'])*5
    T_.append({"L":L,"usd":usd,"sb":semi_at(day,etn),
               "fALL":gfav(day,etn,L,float(r['spot']),'ALL'),
               "f0":gfav(day,etn,L,float(r['spot']),'TODAY')})
base=sum(t['usd'] for t in T_); semi=sum(t['usd']*sm(t['L'],t['sb']) for t in T_)
def wr(x): return f"n={len(x):>3} WR={100*sum(1 for t in x if t['usd']>0)/len(x):.0f}% ${sum(t['usd'] for t in x):+.0f}" if x else "n=0"
print(f"\n=== {len(T_)} V16 trades ===  baseline ${base:+.0f} | semi ${semi:+.0f} ({semi/base:.2f}x)")
for key,lab in [('fALL','GAMMA=ALL (correct total)'),('f0','GAMMA=TODAY (0DTE only)')]:
    g=sum(t['usd']*gm(t[key]) for t in T_)
    two=sum(t['usd']*max(0.375,min(2.5,sm(t['L'],t['sb'])*gadj(t[key]))) for t in T_)
    sc=[t for t in T_ if sm(t['L'],t['sb'])==2.0]
    print(f"\n--- {lab} ---")
    print(f"  gamma-only ${g:+.0f} ({g/base:.2f}x) | 2-factor ${two:+.0f} ({two/base:.2f}x, vs semi {semi/base:.2f}x)")
    print(f"  FAV {wr([t for t in T_ if t[key] is not None and t[key]>TH])} | UNFAV {wr([t for t in T_ if t[key] is not None and t[key]<-TH])}")
    print(f"  within semi-conf: g-fav {wr([t for t in sc if t[key] is not None and t[key]>TH])} | g-unfav {wr([t for t in sc if t[key] is not None and t[key]<-TH])}")
