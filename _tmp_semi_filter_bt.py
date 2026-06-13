"""STEP 2 — net book impact of the semi-confirmation filter (reads semi_basket table).
Rule: take LONGS only when basket>=THR_L; take SHORTS only when basket<=THR_S.
Compare baseline (all) vs filtered book, IS(April) vs OOS(May-Jun), plus a
'half-size the rejected' variant. P&L=outcome_pnl*5 ($@1MES).
"""
import os
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")

basket=[(r[0], float(r[1])) for r in C.execute(text(
    "SELECT et, basket_pct FROM semi_basket ORDER BY et")).fetchall()]
byday=defaultdict(list)
for et,v in basket: byday[et.date().isoformat()].append((et,v))
def bstr(day, et):
    arr=byday.get(day,[]); prior=[v for (t,v) in arr if t<=et]
    return prior[-1] if prior else None

sig=C.execute(text("""
    SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
           greek_alignment, outcome_pnl
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
for et,s,d,g,a,pnl in sig:
    islong=d in ('long','bullish'); key=(s,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15): continue
    last[key]=et
    if not quality(s,d,g,a): continue
    bs=bstr(et.date().isoformat(), et.replace(tzinfo=None))
    sigs.append({"day":et.date().isoformat(),"islong":islong,"pnl":float(pnl)*5,"bs":bs})

def per(d): return "IS" if d<"2026-05-01" else "OOS"
THR_L=0.0; THR_S=0.0
def decide(s):
    if s["bs"] is None: return "keep"        # no data -> keep (don't drop)
    if s["islong"]:  return "keep" if s["bs"]>=THR_L else "reject"
    else:            return "keep" if s["bs"]<=THR_S else "reject"
for s in sigs: s["d"]=decide(s)
for label in ("IS","OOS"):
    P=[s for s in sigs if per(s["day"])==label]
    base=sum(s["pnl"] for s in P)
    kept=[s for s in P if s["d"]=="keep"]; rej=[s for s in P if s["d"]=="reject"]
    filt=sum(s["pnl"] for s in kept)
    half=filt+0.5*sum(s["pnl"] for s in rej)   # half-size rejected instead of dropping
    def wr(x): return f"{100*sum(1 for t in x if t['pnl']>0)/len(x):.0f}%" if x else "-"
    print(f"\n### {label} ({'Apr' if label=='IS' else 'May-Jun'}) — {len(P)} trades ###")
    print(f"  BASELINE (all):       ${base:+.0f}  ({len(P)}t, WR {wr(P)})")
    print(f"  FILTERED (drop wrong-semi): ${filt:+.0f}  ({len(kept)}t, WR {wr(kept)})  delta ${filt-base:+.0f}")
    print(f"  HALF-SIZE rejected:   ${half:+.0f}  delta ${half-base:+.0f}")
    print(f"  rejected bucket: {len(rej)}t worth ${sum(s['pnl'] for s in rej):+.0f} (WR {wr(rej)})")
