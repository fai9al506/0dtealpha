"""SEMIS-CONFIRMATION test (Dark Matter's clearest execution rule).
At each ES/SPX setup entry, is the semi/mega-cap basket STRONG (above session open)
or WEAK? Hypothesis: LONGS win when semis strong, SHORTS win when semis weak.
Basket = avg %-from-session-open across NVDA/AMD/AVGO/META/MSFT/GOOGL (those present
in stock_gex_scans, which starts ~Mar 21). IS (to Apr 30) vs OOS (May-Jun). No lookahead.
"""
import os
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")

SEMIS=('NVDA','AMD','AVGO','META','MSFT','GOOGL','SMCI')
cov=C.execute(text("""SELECT MIN(scan_date),MAX(scan_date),COUNT(*) FROM stock_gex_scans
    WHERE symbol = ANY(:s)"""),{"s":list(SEMIS)}).fetchone()
print("stock_gex_scans semis coverage:", cov)

# per (symbol, date): session open spot (first scan) ; and all (ts, spot)
rows=C.execute(text("""
    SELECT symbol, (scan_ts AT TIME ZONE 'America/New_York') et, scan_date, spot
    FROM stock_gex_scans WHERE symbol = ANY(:s) AND spot IS NOT NULL
    ORDER BY symbol, scan_ts"""),{"s":list(SEMIS)}).fetchall()
opens={}; series=defaultdict(list)   # (sym,date)->open ; (sym,date)->[(et,spot)]
for sym,et,d,spot in rows:
    key=(sym,d.isoformat())
    if key not in opens: opens[key]=float(spot)
    series[key].append((et,float(spot)))

def basket_strength(day, et):
    """avg %-from-open across semis, using latest scan <= et that day. None if no data."""
    vals=[]
    for sym in SEMIS:
        key=(sym,day)
        if key not in opens: continue
        prior=[s for (t,s) in series[key] if t<=et]
        if not prior: continue
        vals.append((prior[-1]-opens[key])/opens[key]*100)
    if not vals: return None
    return sum(vals)/len(vals)

sig=C.execute(text("""
    SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
           greek_alignment, spot, outcome_pnl
    FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE '2026-03-21'
      AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
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
    bs=basket_strength(et.date().isoformat(), et)
    sigs.append({"day":et.date().isoformat(),"islong":islong,"pnl":float(pnl),"bs":bs})

def per(d): return "IS" if d<"2026-05-01" else "OOS"
def stt(ts):
    ts=[t for t in ts if t['bs'] is not None]
    if not ts: return "n=  0"
    w=sum(1 for t in ts if t['pnl']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${sum(t['pnl'] for t in ts)*5:+7.0f} avg${sum(t['pnl'] for t in ts)*5/len(ts):+5.1f}"
print(f"\nsignals with basket data: {sum(1 for s in sigs if s['bs'] is not None)}/{len(sigs)}")
for label in ("IS","OOS"):
    P=[s for s in sigs if per(s['day'])==label and s['bs'] is not None]
    L=[s for s in P if s['islong']]; S=[s for s in P if not s['islong']]
    print(f"\n############ {label} ({'Mar21-Apr30' if label=='IS' else 'May-Jun'}) — {len(P)} signals ############")
    print(" LONGS  : semis STRONG(>0):", stt([s for s in L if s['bs']>0]), " | WEAK(<0):", stt([s for s in L if s['bs']<=0]))
    print(" SHORTS : semis STRONG(>0):", stt([s for s in S if s['bs']>0]), " | WEAK(<0):", stt([s for s in S if s['bs']<=0]))
    # stronger threshold
    print(" LONGS  : semis >+0.5%:", stt([s for s in L if s['bs']>0.5]), " | <-0.5%:", stt([s for s in L if s['bs']<-0.5]))
