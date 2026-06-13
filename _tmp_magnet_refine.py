"""Magnet-bias refinement on our EXISTING setups.
Hypothesis: our SC/DD/ES-Abs/AG signals do better when the trade direction AGREES
with the dominant vanna magnet (strongest |vanna| node near spot). Test as a FILTER:
 - magnet ABOVE spot  -> bullish pull -> favors LONGS
 - magnet BELOW spot  -> bearish pull -> favors SHORTS
Compare aligned vs counter trades, IS (Mar-Apr) vs OOS (May-Jun). No lookahead
(level map as-of 09:40 / noon, like the engine). P&L=outcome_pnl*5 ($@1MES).
"""
from datetime import datetime, timedelta
from collections import defaultdict
import _tmp_l2l_engine as E
from sqlalchemy import text

MINV=6e7; BAND=70; MINDIST=5

rows=E.CONN.execute(text("""
    SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
           greek_alignment, spot, outcome_pnl
    FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-03-01' AND DATE '2026-06-09'
      AND outcome_pnl IS NOT NULL AND spot IS NOT NULL
      AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
    ORDER BY ts ASC""")).fetchall()

def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    islong=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and islong and (aa<0 or aa>=3): return False
    return True

# dedup 15min per (setup,dir)
last={}; sigs=[]
for et,s,d,g,a,spot,pnl in rows:
    islong=d in ('long','bullish'); key=(s,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15): continue
    last[key]=et
    if not quality(s,d,g,a): continue
    sigs.append({"et":et,"day":et.date().isoformat(),"setup":s,"islong":islong,
                 "spot":float(spot),"pnl":float(pnl)})

def magnet_dir(day, et, spot):
    before = datetime.fromisoformat(day).replace(hour=12,minute=30) if et.hour>=13 \
             else datetime.fromisoformat(day).replace(hour=9,minute=40)
    m=E.level_map(day, before)
    cand=[(k,v) for k,v in m.items() if abs(k-spot)<=BAND and abs(v)>MINV and abs(k-spot)>=MINDIST]
    if not cand: return 0
    k=max(cand,key=lambda x:abs(x[1]))[0]
    return 1 if k>spot else -1

for s in sigs:
    md=magnet_dir(s["day"], s["et"], s["spot"])
    tdir = 1 if s["islong"] else -1
    s["align"] = "none" if md==0 else ("aligned" if md==tdir else "counter")

def period(d): return "IS" if d<"2026-05-01" else "OOS"
def stt(ts):
    if not ts: return "n=  0"
    w=sum(1 for t in ts if t['pnl']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${sum(t['pnl'] for t in ts)*5:+7.0f} avg${sum(t['pnl'] for t in ts)*5/len(ts):+5.1f}"

for per in ("IS","OOS"):
    P=[s for s in sigs if period(s["day"])==per]
    print(f"\n===== {per} ({'Mar-Apr' if per=='IS' else 'May-Jun'}) — {len(P)} signals =====")
    for al in ("aligned","counter","none"):
        print(f"  {al:<8}: {stt([s for s in P if s['align']==al])}")
    base=[s for s in P]; kept=[s for s in P if s['align']!='counter']
    print(f"  BASELINE (all):        {stt(base)}")
    print(f"  DROP counter-aligned:  {stt(kept)}   delta ${ (sum(s['pnl'] for s in kept)-sum(s['pnl'] for s in base))*5:+.0f}")
    # split by direction
    print(f"    aligned LONGS : {stt([s for s in P if s['align']=='aligned' and s['islong']])}")
    print(f"    aligned SHORTS: {stt([s for s in P if s['align']=='aligned' and not s['islong']])}")
    print(f"    counter LONGS : {stt([s for s in P if s['align']=='counter' and s['islong']])}")
    print(f"    counter SHORTS: {stt([s for s in P if s['align']=='counter' and not s['islong']])}")
