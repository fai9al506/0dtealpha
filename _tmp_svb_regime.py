"""Dark Matter's #1 regime read = spot_vol_beta (spot & vol same direction =
dealers reactive = trend/risk-off). Test: do SC/DD LONGS lose when |SVB| or SVB
is elevated, across EVERY era? (NaN-filtered.) outcome_pnl, by month.
"""
import os, math
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, grade, spot,
               greek_alignment, vix, spot_vol_beta, overvix, outcome_pnl
        FROM setup_log
        WHERE setup_name IN ('Skew Charm','DD Exhaustion')
          AND direction IN ('long','bullish') AND outcome_pnl IS NOT NULL
        ORDER BY ts ASC""")).fetchall()
    # session open per day
    sp = conn.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, spot FROM setup_log
        WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::time>=TIME '09:30' ORDER BY ts""")).fetchall()
    dopen={}
    for et,s in sp:
        d=et.date().isoformat()
        if d not in dopen: dopen[d]=float(s)

last={}; sig=[]
for et,setup,grade,spot,align,vix,svb,ovx,pnl in rows:
    if setup in last and (et-last[setup])<timedelta(minutes=15): continue
    last[setup]=et
    if grade in ('C','LOG',None): continue
    if setup=='DD Exhaustion':
        a=align or 0
        if a<0 or a>=3: continue
        if vix is not None and float(vix)>=22: continue
    sv = float(svb) if svb is not None else None
    if sv is not None and (math.isnan(sv) or math.isinf(sv)): sv=None
    d=et.date().isoformat()
    fo=float(spot)-dopen.get(d,float(spot))
    sig.append({"mo":d[:7],"day":d,"svb":sv,"ovx":float(ovx) if ovx is not None else None,
                "fo":fo,"pnl":float(pnl)})

months=sorted(set(t['mo'] for t in sig))
def agg(ts):
    v=[t['pnl'] for t in ts]
    if not v: return "n=  0"
    return f"n={len(v):>3} WR={100*sum(1 for x in v if x>0)/len(v):3.0f}% ${sum(v)*5:+6.0f}"

print("SC/DD quality longs — does spot_vol_beta isolate bad days across eras?\n")
have=[t for t in sig if t['svb'] is not None]
print(f"(svb populated on {len(have)}/{len(sig)})\n")
for label,cond in [
    ("SVB >= +1.0", lambda t:t['svb'] is not None and t['svb']>=1.0),
    ("SVB >= +2.0 (Wiz vol-event)", lambda t:t['svb'] is not None and t['svb']>=2.0),
    ("SVB <= -1.0", lambda t:t['svb'] is not None and t['svb']<=-1.0),
    ("SVB in [-1,1] (calm)", lambda t:t['svb'] is not None and -1<=t['svb']<=1),
    ("down(fo<=-15) AND SVB>=+1", lambda t:t['fo']<=-15 and t['svb'] is not None and t['svb']>=1.0),
    ("overvix>=0 (VIX>=VIX3M)", lambda t:t['ovx'] is not None and t['ovx']>=0),
]:
    sel=[t for t in have if cond(t)]
    by=" ".join(f"{m[5:]}:{agg([t for t in sel if t['mo']==m])}" for m in months)
    print(f"--- {label} ---  TOTAL {agg(sel)}")
    for m in months:
        print(f"     {m}: {agg([t for t in sel if t['mo']==m])}")
    print()
