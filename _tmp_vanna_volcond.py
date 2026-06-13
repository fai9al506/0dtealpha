# -*- coding: utf-8 -*-
"""VANNA as a VOL-CONDITIONAL magnet (per Discord framework, Wizard of Ops):
normal/falling vol -> +vanna is a MAGNET (price drawn to it); rising/high vol -> INVERTS
(+vanna above = resistance). Test on V16 (live_pass): magnet-alignment favorability,
split by VIX regime, + the vol-conditional rule vs fixed sign, + IS/OOS robustness."""
import os, warnings, math, statistics
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
def magnet_align(day,etn,L,spot):
    """+ = the dominant vanna magnet pulls in the trade's direction (above for long / below for short)."""
    mm=M.get(day)
    if not mm: return None
    m=mm[1] if (etn.time()>=dtime(13,0) and mm[1]) else mm[0]
    if not m: return None
    above=sum(v for k,v in m.items() if spot<k<=spot+PATH); below=sum(v for k,v in m.items() if spot-PATH<=k<spot)
    return (above-below) if L else (below-above)   # magnet in trade direction
rows=C.execute(text("""SELECT direction, ts, spot, outcome_pnl, vix FROM setup_log
  WHERE live_pass=true AND outcome_pnl IS NOT NULL AND spot IS NOT NULL ORDER BY ts""")).mappings().all()
T_=[]
for r in rows:
    et=r['ts'].astimezone(ET); etn=et.replace(tzinfo=None); day=et.date().isoformat()
    L=r['direction'] in ('long','bullish'); usd=float(r['outcome_pnl'])*5
    ma=magnet_align(day,etn,L,float(r['spot']))
    T_.append({"day":day,"L":L,"usd":usd,"ma":ma,"vix":float(r['vix']) if r['vix'] else None,"sb":semi_at(day,etn)})
TH=statistics.median([abs(t['ma']) for t in T_ if t['ma'] is not None])
def wr(x): return f"n={len(x):>3} WR={100*sum(1 for t in x if t['usd']>0)/len(x):3.0f}% ${sum(t['usd'] for t in x):+6.0f}" if x else "n=0"
print(f"\n=== {len(T_)} V16 trades · vanna magnet-align TH={TH:.0f}M ===")
print("\n[A] MAGNET aligned vs against, SPLIT by VIX regime (framework: magnet holds low-vol, inverts high-vol):")
for vlab,vfilt in [("VIX < 20 (normal)",lambda t:t['vix'] is not None and t['vix']<20),
                   ("VIX >= 20 (stress)",lambda t:t['vix'] is not None and t['vix']>=20)]:
    sub=[t for t in T_ if vfilt(t) and t['ma'] is not None]
    al=[t for t in sub if t['ma']>TH]; ag=[t for t in sub if t['ma']<-TH]
    print(f"  {vlab}: ALIGNED {wr(al)} | AGAINST {wr(ag)}")
print("\n[B] overall (no vol split): aligned vs against")
al=[t for t in T_ if t['ma'] is not None and t['ma']>TH]; ag=[t for t in T_ if t['ma'] is not None and t['ma']<-TH]
print(f"  ALIGNED {wr(al)} | AGAINST {wr(ag)}")
print("\n[C] IS/OOS robustness of the magnet sign (does ALIGNED>AGAINST hold both halves?):")
mid=days[len(days)//2]
for lab,df in [("IS (1st half)",lambda t:t['day']<mid),("OOS (2nd half)",lambda t:t['day']>=mid)]:
    sub=[t for t in T_ if df(t) and t['ma'] is not None]
    al=[t for t in sub if t['ma']>TH]; ag=[t for t in sub if t['ma']<-TH]
    print(f"  {lab}: ALIGNED {wr(al)} | AGAINST {wr(ag)}")
print("\n[D] SIZING — vol-conditional vanna (2x align/0.5 against, INVERT when VIX>=20) on top of semi:")
def sm(L,b):
    if b is None: return 1.0
    return 2.0 if ((L and b>0) or (not L and b<0)) else (0.5 if ((L and b<0) or (not L and b>0)) else 1.0)
def vmult(t):
    if t['ma'] is None or t['vix'] is None: return 1.0
    a=t['ma'] if t['vix']<20 else -t['ma']   # invert under stress
    return 1.25 if a>TH else (0.75 if a<-TH else 1.0)
base=sum(t['usd'] for t in T_); semi=sum(t['usd']*sm(t['L'],t['sb']) for t in T_)
sv=sum(t['usd']*max(0.375,min(2.5,sm(t['L'],t['sb'])*vmult(t))) for t in T_)
print(f"  baseline ${base:+.0f} | semi ${semi:+.0f} ({semi/base:.2f}x) | semi+vanna(volcond) ${sv:+.0f} ({sv/base:.2f}x)")
