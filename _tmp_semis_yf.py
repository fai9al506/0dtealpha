"""SEMIS-CONFIRMATION test with REAL semi prices (yfinance 15m, ~Apr11-Jun9).
Basket = avg %-from-session-open across NVDA/AMD/AVGO/META/MSFT/GOOGL at each 15m bar.
At each ES/SPX setup entry, look up basket strength (latest bar <= entry, no lookahead).
LONGS should win when basket STRONG; SHORTS when WEAK. IS=April vs OOS=May-Jun.
"""
import os, warnings, math
warnings.filterwarnings("ignore")
from collections import defaultdict
from datetime import timedelta
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text

TKRS=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
print("fetching 15m semi prices...", flush=True)
df=yf.download(TKRS, period='60d', interval='15m', progress=False, auto_adjust=True)
close=df['Close'].copy()
# to naive ET
if close.index.tz is not None:
    close.index=close.index.tz_convert('America/New_York').tz_localize(None)
print("bars:", len(close), "range:", close.index.min(), "->", close.index.max(), flush=True)

# per ticker per day: session open (first bar >=09:30); pct-from-open per bar
close=close.between_time("09:30","16:00")
basket=defaultdict(list)   # day -> list of (ts, avg_pct)
for day, g in close.groupby(close.index.normalize()):
    opens={t: g[t].dropna().iloc[0] for t in TKRS if g[t].dropna().shape[0]>0}
    for ts,row in g.iterrows():
        pcts=[(row[t]-opens[t])/opens[t]*100 for t in TKRS if t in opens and not math.isnan(row[t])]
        if pcts: basket[day.date().isoformat()].append((ts,sum(pcts)/len(pcts)))

def strength(day, et):
    arr=basket.get(day,[])
    prior=[v for (ts,v) in arr if ts<=et]
    return prior[-1] if prior else None

C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
sig=C.execute(text("""
    SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
           greek_alignment, spot, outcome_pnl
    FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE '2026-04-11'
      AND outcome_pnl IS NOT NULL
      AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
    ORDER BY ts ASC""")).fetchall()
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    islong=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and islong and (aa<0 or aa>=3): return False
    return True
last={}; sigs=[]
for et,s,d,g,a,spot,pnl in sig:
    islong=d in ('long','bullish'); key=(s,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15): continue
    last[key]=et
    if not quality(s,d,g,a): continue
    bs=strength(et.date().isoformat(), et.replace(tzinfo=None))
    sigs.append({"day":et.date().isoformat(),"islong":islong,"pnl":float(pnl),"bs":bs})

def per(d): return "IS" if d<"2026-05-01" else "OOS"
def stt(ts):
    ts=[t for t in ts if t['bs'] is not None]
    if not ts: return "n=  0"
    w=sum(1 for t in ts if t['pnl']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${sum(t['pnl'] for t in ts)*5:+7.0f} avg${sum(t['pnl'] for t in ts)*5/len(ts):+5.1f}"
print(f"\nsignals w/ basket data: {sum(1 for s in sigs if s['bs'] is not None)}/{len(sigs)}")
for label in ("IS","OOS"):
    P=[s for s in sigs if per(s['day'])==label and s['bs'] is not None]
    L=[s for s in P if s['islong']]; S=[s for s in P if not s['islong']]
    print(f"\n############ {label} ({'Apr' if label=='IS' else 'May-Jun'}) — {len(P)} ############")
    print(" LONGS  STRONG(>0):", stt([s for s in L if s['bs']>0]), "| WEAK(<=0):", stt([s for s in L if s['bs']<=0]))
    print(" SHORTS STRONG(>0):", stt([s for s in S if s['bs']>0]), "| WEAK(<=0):", stt([s for s in S if s['bs']<=0]))
    print(" LONGS  >+0.5%:", stt([s for s in L if s['bs']>0.5]), "| <-0.5%:", stt([s for s in L if s['bs']<-0.5]))
