# -*- coding: utf-8 -*-
"""Re-stress Jun 10 with TRUE 1-min semis (from TradingView CSVs) vs my 15m-lag-20.
Tests whether the lag/staleness changed the sizing on a real bleed day."""
import os, csv, json
from datetime import datetime, timezone, timedelta, time as dtime
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
DL=r"C:\Users\Faisa\Downloads"
FILES={'NVDA':'NASDAQ_NVDA, 1_10cd0.csv','AMD':'NASDAQ_AMD, 1_416bd.csv','AVGO':'NASDAQ_AVGO, 1_5af0e.csv',
       'META':'NASDAQ_META, 1_7e169.csv','MSFT':'NASDAQ_MSFT, 1_e5b30.csv','GOOGL':'NASDAQ_GOOGL, 1_6152c.csv'}
DAY="2026-06-10"
# per ticker: ET-minute -> close, on DAY ; session open = first DAY bar
series={}; opens={}
for tk,fn in FILES.items():
    rows=list(csv.reader(open(os.path.join(DL,fn))))[1:]
    s={}
    for r in rows:
        et=datetime.fromtimestamp(int(r[0]),timezone.utc).astimezone(timezone(timedelta(hours=-4)))  # EDT
        if et.date().isoformat()!=DAY: continue
        if dtime(9,30)<=et.time()<=dtime(16,0): s[et.replace(tzinfo=None)]=float(r[4])
    if s:
        series[tk]=s; opens[tk]=s[min(s)]
print("1-min coverage Jun10:",{tk:len(s) for tk,s in series.items()})
def basket_1min(t):
    vals=[]
    for tk,s in series.items():
        prior=[(m,c) for m,c in s.items() if m<=t]
        if prior:
            c=max(prior)[1]; vals.append((c-opens[tk])/opens[tk]*100)
    return sum(vals)/len(vals) if vals else None

# my 15m-lag-20 basket
basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket WHERE et::date=DATE :d ORDER BY et"),{"d":DAY}).fetchall()]
def semi_15m(t):
    co=t-timedelta(minutes=20); p=[v for (x,v) in basket if x<=co]; return p[-1] if p else None

rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date=DATE :d ORDER BY sl.ts"""),{"d":DAY}).fetchall()
print(f"\n{'time':<6}{'setup':<14}{'dir':<4}{'pnl$':>6}{'semi_15m':>9}{'semi_1min':>10}{'size_15m':>9}{'size_1min':>10}")
def sm(L,sb):
    if sb is None: return 1.0
    if (L and sb>0) or (not L and sb<0): return 2.0
    if (L and sb<0) or (not L and sb>0): return 0.5
    return 1.0
b=s15=s1m=0
for et,setup,direction,st,spot in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5; etn=et.replace(tzinfo=None)
    sb15=semi_15m(etn); sb1=basket_1min(etn-timedelta(minutes=1))
    z15=sm(L,sb15); z1=sm(L,sb1); b+=usd; s15+=usd*z15; s1m+=usd*z1
    print(f"{et.strftime('%H:%M'):<6}{setup[:13]:<14}{('L' if L else 'S'):<4}{usd:>+6.0f}"
          f"{('%+.2f'%sb15) if sb15 is not None else '-':>9}{('%+.2f'%sb1) if sb1 is not None else '-':>10}{z15:>9.2f}{z1:>10.2f}")
print(f"\nJun 10:  baseline ${b:+.0f} | sized(15m-lag20) ${s15:+.0f} | sized(TRUE 1-min) ${s1m:+.0f}")
