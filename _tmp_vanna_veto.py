# -*- coding: utf-8 -*-
"""VANNA-MAGNET VETO idea (user): block SHORTS when a strong +vanna magnet sits above
spot (price gets pulled up into it -> short fights the magnet). Test on V16 shorts:
is the vetoed bucket a consistent LOSER (veto helps) across periods? Or just today?"""
import os
from datetime import time as dtime
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
DMAX=60  # look this many pts above spot for the magnet
days=[r[0].isoformat() for r in C.execute(text("SELECT DISTINCT (ts AT TIME ZONE 'America/New_York')::date FROM setup_log WHERE live_pass=true ORDER BY 1")).fetchall()]
def daymap(day,utc_cut):
    rs=C.execute(text("""SELECT DISTINCT ON (strike) strike,value FROM volland_exposure_points
      WHERE greek='vanna' AND expiration_option='ALL' AND ts_utc>=:d0 AND ts_utc<=:cut AND strike BETWEEN 5500 AND 7800
      ORDER BY strike, ts_utc DESC"""),{"d0":day+" 00:00:00+00","cut":day+" "+utc_cut+"+00"}).fetchall()
    return {float(k):float(v)/1e6 for k,v in rs}
M={d:(daymap(d,'13:40:00'),daymap(d,'16:30:00')) for d in days}
def magnet_above(day,etn,spot):
    mm=M.get(day)
    if not mm: return 0.0
    m=mm[1] if (etn.time()>=dtime(13,0) and mm[1]) else mm[0]
    if not m: return 0.0
    pos=[v for k,v in m.items() if spot<k<=spot+DMAX and v>0]
    return max(pos) if pos else 0.0
rows=C.execute(text("""SELECT direction, setup_name, ts, spot, outcome_pnl, vix FROM setup_log
  WHERE live_pass=true AND outcome_pnl IS NOT NULL AND spot IS NOT NULL ORDER BY ts""")).mappings().all()
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
S=[]
for r in rows:
    sh=('bear' in (r['direction'] or ''))or(r['direction']=='short')or(r['setup_name']=='AG Short')
    if not sh: continue  # shorts only
    et=r['ts'].astimezone(ET); etn=et.replace(tzinfo=None); day=et.date().isoformat()
    S.append({"day":day,"usd":float(r['outcome_pnl'])*5,"mag":magnet_above(day,etn,float(r['spot'])),
              "vix":float(r['vix']) if r['vix'] else None})
def stat(x):
    if not x: return "n=0"
    w=sum(1 for t in x if t['usd']>0); s=sum(t['usd'] for t in x)
    return f"n={len(x):>3} WR={100*w/len(x):3.0f}% ${s:+6.0f} avg${s/len(x):+5.1f}"
print(f"=== {len(S)} V16 SHORTS · vanna-magnet veto (block if +vanna magnet >TH within {DMAX}pt above) ===")
mid=days[len(days)//2]
for TH in (80,150,250):
    vetoed=[t for t in S if t['mag']>TH]; kept=[t for t in S if t['mag']<=TH]
    print(f"\n-- threshold {TH}M --")
    print(f"  VETOED (blocked) shorts: {stat(vetoed)}   <- veto helps only if this is a net LOSER")
    print(f"  KEPT shorts:             {stat(kept)}")
    # robustness of the vetoed bucket
    print(f"    IS  vetoed: {stat([t for t in vetoed if t['day']<mid])}")
    print(f"    OOS vetoed: {stat([t for t in vetoed if t['day']>=mid])}")
    # per-month vetoed
    mo=defaultdict(list)
    for t in vetoed: mo[t['day'][:7]].append(t)
    print("    by month vetoed:", " | ".join(f"{m} {stat(mo[m])}" for m in sorted(mo)))
