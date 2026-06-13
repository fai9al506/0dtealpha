# -*- coding: utf-8 -*-
"""Test: does semi-sizing only work when the 6 tech names AGREE in direction?
If they disagree (split green/red, outlier-skewed mean), gate the multiplier to 1x.
On the CORRECT V16 set (live_pass). Per-symbol from yfinance (5m Mar17+, 1h Feb+)."""
import os, warnings, math
warnings.filterwarnings("ignore")
from datetime import timedelta, time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict
import yfinance as yf
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
ET=ZoneInfo("America/New_York"); TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
def build(interval,period):
    df=yf.download(TK,period=period,interval=interval,progress=False,auto_adjust=True)['Close']
    if df.index.tz is not None: df.index=df.index.tz_convert(ET).tz_localize(None)
    df=df.between_time("09:30","16:00")
    # per-day per-symbol series of (ts, {sym:pct})
    bk=defaultdict(list)
    for day,g in df.groupby(df.index.normalize()):
        op={t:g[t].dropna().iloc[0] for t in TK if g[t].dropna().shape[0]>0}
        for ts,row in g.iterrows():
            d={t:(row[t]-op[t])/op[t]*100 for t in TK if t in op and not math.isnan(row[t])}
            if d: bk[day.date().isoformat()].append((ts,d))
    return bk
print("fetch per-symbol 5m+1h...",flush=True)
b5=build('5m','60d'); b1=build('1h','730d')
def pcts_at(day,etn):
    src,lag=(b5,15) if day in b5 else (b1,60)
    co=etn-timedelta(minutes=lag); a=src.get(day,[]); pr=[d for (x,d) in a if x<=co]
    return pr[-1] if pr else None
rows=C.execute(text("""SELECT direction, ts, outcome_pnl FROM setup_log
  WHERE live_pass=true AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
print(f"V16 trades: {len(rows)}",flush=True)
T_=[]
for r in rows:
    et=r['ts'].astimezone(ET); etn=et.replace(tzinfo=None); day=et.date().isoformat()
    L=r['direction'] in ('long','bullish'); usd=float(r['outcome_pnl'])*5
    d=pcts_at(day,etn)
    if not d or len(d)<4: continue
    vals=list(d.values()); basket=sum(vals)/len(vals)
    npos=sum(1 for v in vals if v>0); nneg=sum(1 for v in vals if v<0)
    sign=1 if basket>0 else (-1 if basket<0 else 0)
    n_agree=sum(1 for v in vals if (v>0)==(basket>0)) if sign else 0   # share same sign as basket
    median=sorted(vals)[len(vals)//2]
    T_.append({"L":L,"usd":usd,"basket":basket,"median":median,"n_agree":n_agree,"npos":npos,"nneg":nneg,"n":len(vals)})
def sm(L,b):
    if b is None: return 1.0
    if (L and b>0) or (not L and b<0): return 2.0
    if (L and b<0) or (not L and b>0): return 0.5
    return 1.0
def stt(x):
    if not x: return "n=0"
    w=sum(1 for t in x if t['usd']>0); s=sum(t['usd'] for t in x)
    return f"n={len(x):>3} WR={100*w/len(x):3.0f}% base=${s:+6.0f}"
base=sum(t['usd'] for t in T_); semi=sum(t['usd']*sm(t['L'],t['basket']) for t in T_)
print(f"\n=== {len(T_)} V16 trades w/ per-symbol data ===")
print(f"baseline ${base:+.0f} | semi (ungated) ${semi:+.0f} ({semi/base:.2f}x)")
print("\n--- distribution of n_agree (how many of 6 share the basket sign) ---")
for na in (6,5,4,3):
    sub=[t for t in T_ if t['n_agree']==na];
    if sub:
        ss=sum(t['usd']*sm(t['L'],t['basket']) for t in sub); bb=sum(t['usd'] for t in sub)
        print(f"  n_agree={na}: {stt(sub)}  -> semi ${ss:+.0f}  (semi-base ${ss-bb:+.0f})")
print("\n--- AGREE (>=5 same sign) vs DISAGREE (<=4) ---")
for lbl,sub in [("AGREE  (>=5)",[t for t in T_ if t['n_agree']>=5]),("DISAGREE(<=4)",[t for t in T_ if t['n_agree']<=4])]:
    bb=sum(t['usd'] for t in sub); ss=sum(t['usd']*sm(t['L'],t['basket']) for t in sub)
    print(f"  {lbl}: n={len(sub)} baseline ${bb:+.0f} -> semi ${ss:+.0f} (uplift ${ss-bb:+.0f})")
print("\n--- GATED semi: apply 2x/0.5x ONLY when tech agrees, else 1x ---")
for thr in (6,5,4):
    g=sum(t['usd']*(sm(t['L'],t['basket']) if t['n_agree']>=thr else 1.0) for t in T_)
    print(f"  gate n_agree>={thr}: ${g:+.0f} ({g/base:.2f}x, vs ungated semi ${semi:+.0f})")
# bonus: median-based basket (outlier-robust) ungated
medsemi=sum(t['usd']*sm(t['L'],t['median']) for t in T_)
print(f"\nbonus: MEDIAN-basket semi (outlier-robust) ${medsemi:+.0f} ({medsemi/base:.2f}x)")

# robustness: TRUE median + per-month mean vs median
def truemed(v):
    s=sorted(v); n=len(s)
    return (s[n//2] if n%2 else (s[n//2-1]+s[n//2])/2)
from collections import defaultdict as dd
mo=dd(lambda:[0.0,0.0,0.0])  # base, mean-semi, median-semi
import datetime
# need month per trade -> recompute with month tag
