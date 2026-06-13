# -*- coding: utf-8 -*-
"""Re-stress Jun 3-10 with clean 5-min semis (TradingView CSVs). Semi-only (gamma dropped).
Compare baseline vs sized-PLAIN (sign) vs sized-THRESHOLD (|basket|>0.4 acts, else 1x)."""
import os, csv, json, glob
from datetime import datetime, timezone, timedelta, time as dtime
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
DL=r"C:\Users\Faisa\Downloads"
def find(pre):
    g=glob.glob(os.path.join(DL,pre+", 5_*.csv")); return g[0] if g else None
TKS={'NVDA':find('NASDAQ_NVDA'),'AMD':find('NASDAQ_AMD'),'AVGO':find('NASDAQ_AVGO'),
     'META':find('NASDAQ_META'),'MSFT':find('NASDAQ_MSFT'),'GOOGL':find('NASDAQ_GOOGL')}
EDT=timezone(timedelta(hours=-4))
# per ticker: day -> {et_min: close}, day -> open
ser=defaultdict(lambda:defaultdict(dict)); opn=defaultdict(dict)
for tk,fn in TKS.items():
    for r in list(csv.reader(open(fn)))[1:]:
        et=datetime.fromtimestamp(int(r[0]),timezone.utc).astimezone(EDT).replace(tzinfo=None)
        d=et.date().isoformat()
        if dtime(9,30)<=et.time()<=dtime(16,0): ser[tk][d][et]=float(r[4])
    for d in ser[tk]:
        if ser[tk][d]: opn[tk][d]=ser[tk][d][min(ser[tk][d])]
def basket(day,t):
    co=t-timedelta(minutes=5); vals=[]   # last completed 5-min bar (no look-ahead)
    for tk in TKS:
        s=ser[tk].get(day,{}); pr=[(m,c) for m,c in s.items() if m<=co]
        if pr and day in opn[tk]: c=max(pr)[1]; vals.append((c-opn[tk][day])/opn[tk][day]*100)
    return sum(vals)/len(vals) if vals else None

rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-06-03' AND DATE '2026-06-10' ORDER BY sl.ts"""),{}).fetchall()
def sz_plain(L,b):
    if b is None: return 1.0
    if (L and b>0) or (not L and b<0): return 2.0
    if (L and b<0) or (not L and b>0): return 0.5
    return 1.0
def sz_thr(L,b,T=0.4):
    if b is None or abs(b)<=T: return 1.0
    if (L and b>0) or (not L and b<0): return 2.0
    return 0.5
day=defaultdict(lambda:[0.0,0.0,0.0])
for et,setup,direction,st in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5; etn=et.replace(tzinfo=None); d=etn.date().isoformat()
    b=basket(d,etn); P=day[d]; P[0]+=usd; P[1]+=usd*sz_plain(L,b); P[2]+=usd*sz_thr(L,b)
print(f"{'day':<12}{'base':>8}{'plain':>9}{'threshold':>11}")
tb=tp=tt=0
for d in sorted(day):
    P=day[d]; tb+=P[0]; tp+=P[1]; tt+=P[2]
    print(f"{d:<12}{P[0]:>+8.0f}{P[1]:>+9.0f}{P[2]:>+11.0f}")
print(f"{'TOTAL':<12}{tb:>+8.0f}{tp:>+9.0f}{tt:>+11.0f}")
print(f"\nclean 5-min semis (Jun3-10): baseline ${tb:+.0f} | sized-PLAIN ${tp:+.0f} | sized-THRESHOLD(0.4) ${tt:+.0f}")
