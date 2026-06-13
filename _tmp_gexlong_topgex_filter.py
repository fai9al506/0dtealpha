"""User's filter (2026-06-08) on the 18 v3.2 trades:
REMOVE if (total GEX < 0) AND (the +GEX magnet is NOT in the top-3 strikes by |GEX|).
Targets messy/negative-regime trades with a negligible magnet (#263, #798).
Re-sim SL14/target=magnet/trail15/5 (same as report). GEX window spot+/-40 (chart view).
"""
import json
from collections import defaultdict
from sqlalchemy import create_engine, text
from app.gex_long_v3 import _build_cache
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
SL,TACT,TGAP=14.0,15.0,5.0
eng=create_engine(DB)
overlay=_build_cache(eng)
v32=[lid for lid,o in overlay.items() if o.get('pass_v32') and o.get('result') is not None]
def Q(s,p=None):
    with eng.begin() as cx: return list(cx.execute(text(s),p or {}))
DP=defaultdict(list)
for d,ts,sp in Q("""SELECT (ts AT TIME ZONE 'America/New_York')::date,ts,spot FROM chain_snapshots
  WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-01' AND '2026-06-08'
  AND (ts AT TIME ZONE 'America/New_York')::time<'16:00' AND spot IS NOT NULL ORDER BY ts"""):
    DP[d].append((ts,float(sp)))
def sim(day,ts,entry,target):
    path=[sp for (t2,sp) in DP.get(day,[]) if t2>=ts]
    if not path: return None
    s=entry-SL;mfe=0;ta=False;tstop=s
    for sp in path:
        mfe=max(mfe,sp-entry);stop=tstop if ta else s
        if sp<=stop: return ('WIN' if stop-entry>0 else 'LOSS',round(stop-entry,1))
        if sp>=target: return ('WIN',round(target-entry,1))
        if not ta and mfe>=TACT: ta=True;tstop=entry+(mfe-TGAP)
        elif ta:
            nt=entry+(mfe-TGAP)
            if nt>tstop:tstop=nt
    return ('EXPIRED',round(path[-1]-entry,1))
import datetime as _dt
T=[]
for lid in v32:
    r=Q("SELECT id,ts,(ts AT TIME ZONE 'America/New_York') t, spot FROM setup_log WHERE id=:i",{"i":lid})
    if not r: continue
    _,ts,t,spot=r[0]; spot=float(spot); d=t.date()
    cr=Q("""SELECT rows FROM chain_snapshots WHERE ts BETWEEN :a AND :b
        ORDER BY abs(extract(epoch FROM (ts-:c))) LIMIT 1""",
        {"a":ts-_dt.timedelta(seconds=90),"b":ts+_dt.timedelta(seconds=90),"c":ts})
    if not cr or not cr[0][0]: continue
    chain=cr[0][0] if isinstance(cr[0][0],list) else json.loads(cr[0][0])
    gex=[]
    for rr in chain:
        try: s=float(rr[iS])
        except: continue
        if abs(s-spot)>40: continue
        gex.append((s,float(rr[iCG] or 0)*float(rr[iCOI] or 0)-float(rr[iPG] or 0)*float(rr[iPOI] or 0)))
    if not gex: continue
    total=sum(v for _,v in gex)
    ga=[(s,v) for s,v in gex if s>spot and v>0]
    if not ga: continue
    magnet,mval=max(ga,key=lambda x:x[1])
    ranked=sorted(gex,key=lambda x:-abs(x[1]))  # by |GEX| desc
    rank=[s for s,_ in ranked].index(magnet)+1   # 1-based rank of magnet by |GEX|
    res=sim(d,ts,spot,max(magnet,spot+5))
    if not res: continue
    remove = (total<0) and (rank>3)
    T.append(dict(lid=lid,date=str(d)[:10],ts=str(t)[11:16],res=res[0],pnl=res[1],
        total=total,magnet=int(magnet),mval=mval,rank=rank,remove=remove))
T.sort(key=lambda x:x['ts'] if False else x['date'])

def summ(rows,label):
    n=len(rows);w=sum(1 for r in rows if r['res']=='WIN');p=sum(r['pnl'] for r in rows)
    print(f"{label}: {n} trades | WR {w/n*100:.0f}% | {p:+.1f}p (${p*5:+,.0f} @1MES)")

print(f"{'lid':>5} {'date':10} {'res':5} {'pnl':>6} {'totalGEX':>9} {'magnet':>6} {'rank':>4}  FILTER")
for r in sorted(T,key=lambda x:x['date']):
    flag='*** REMOVE ***' if r['remove'] else ''
    print(f"{r['lid']:>5} {r['date']:10} {r['res']:5} {r['pnl']:+6.1f} {r['total']:+9.0f} {r['magnet']:>6} {r['rank']:>4}  {flag}")
print()
summ(T,"BEFORE filter (all v3.2)")
print("\n=== SWEEP: remove if (total GEX < 0) AND (magnet rank > N) ===")
for N in [3,4,5,6,8]:
    kept=[r for r in T if not ((r['total']<0) and (r['rank']>N))]
    rem=[r for r in T if (r['total']<0) and (r['rank']>N)]
    n=len(kept);w=sum(1 for r in kept if r['res']=='WIN');p=sum(r['pnl'] for r in kept)
    remstr=", ".join(f"{r['lid']}({r['res'][0]}{r['pnl']:+.0f})" for r in rem)
    print(f"  rank>{N}: keep {n}t | WR {w/n*100:.0f}% | {p:+.1f}p | removed: {remstr or 'none'}")
