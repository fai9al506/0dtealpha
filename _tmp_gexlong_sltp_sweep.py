"""Re-assess SL/TP for GEX Long on the clean v3.2 signals (overlay). Simulate each
signal's chain path once, evaluate multiple SL/TP/trail combos. outcome in POINTS.
"""
import json
from collections import defaultdict
from sqlalchemy import create_engine, text
from app.gex_long_v3 import _build_cache
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine=create_engine(DB)
overlay=_build_cache(engine)
v32_lids=[lid for lid,o in overlay.items() if o.get('pass_v32') and o.get('result') is not None]

# entry data for those lids
rowsmeta={}
with engine.begin() as cx:
    for lid,ts,spot,mpg in cx.execute(text("""SELECT id, ts, spot, max_plus_gex FROM setup_log
        WHERE id = ANY(:ids)"""),{"ids":v32_lids}):
        rowsmeta[lid]=(ts,float(spot),float(mpg) if mpg else None)

# day paths
DP=defaultdict(list)
with engine.begin() as cx:
    for d,ts,spot in cx.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York')::date, ts, spot
        FROM chain_snapshots WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-01' AND '2026-06-08'
        AND (ts AT TIME ZONE 'America/New_York')::time<'16:00' AND spot IS NOT NULL ORDER BY ts""")):
        DP[d].append((ts,float(spot)))
def path_from(ts):
    d=None
    with engine.begin() as cx:
        d=cx.execute(text("SELECT (:ts AT TIME ZONE 'America/New_York')::date"),{"ts":ts}).scalar()
    return [sp for (t2,sp) in DP.get(d,[]) if t2>=ts]

def sim(path,entry,target,sl,t_act,t_gap):
    s=entry-sl; mfe=0; ta=False; tstop=s
    for sp in path:
        mfe=max(mfe,sp-entry); stop=tstop if ta else s
        if sp<=stop: return ('WIN' if stop-entry>0 else 'LOSS', stop-entry)
        if target is not None and sp>=target: return ('WIN', target-entry)
        if t_act and not ta and mfe>=t_act: ta=True; tstop=entry+(mfe-t_gap)
        elif t_act and ta:
            nt=entry+(mfe-t_gap)
            if nt>tstop: tstop=nt
    return ('EXPIRED', path[-1]-entry)

# precompute path per lid
paths={}
for lid,(ts,spot,mpg) in rowsmeta.items():
    paths[lid]=path_from(ts)

def magnet_target(spot,mpg,floor):
    base = spot+floor
    return max(mpg,base) if mpg else base

COMBOS=[
 ("LIVE NOW: SL14 T=max(mag,+10) trail15/5", lambda s,m: (magnet_target(s,m,10),14,15,5)),
 ("VALIDATED: SL14 T=max(mag,+20) trail15/5",lambda s,m: (magnet_target(s,m,20),14,15,5)),
 ("SL14 T=magnet-only trail15/5",            lambda s,m: ((m if m else s+20),14,15,5)),
 ("SL14 trail-only(no tgt) 15/5",            lambda s,m: (None,14,15,5)),
 ("SL14 trail-only 10/5 (earlier trail)",    lambda s,m: (None,14,10,5)),
 ("SL10 T=+10 no-trail (tight scalp)",       lambda s,m: (s+10,10,None,None)),
 ("SL12 T=max(mag,+15) trail12/5",           lambda s,m: (magnet_target(s,m,15),12,12,5)),
 ("SL10 T=max(mag,+20) trail12/4",           lambda s,m: (magnet_target(s,m,20),10,12,4)),
]
print(f"clean v3.2 signals with path: {sum(1 for l in v32_lids if paths.get(l))}\n")
print(f"{'combo':42s} {'WR':>4s} {'TOTAL':>8s} {'avg':>6s} {'PF':>5s} {'maxDD':>7s}")
print("-"*82)
for name,fn in COMBOS:
    rows=[]
    for lid in v32_lids:
        p=paths.get(lid);
        if not p: continue
        ts,spot,mpg=rowsmeta[lid]
        tgt,sl,ta,tg=fn(spot,mpg)
        rows.append(sim(p,spot,tgt,sl,ta,tg))
    n=len(rows); w=sum(1 for r,_ in rows if r=='WIN'); tot=sum(x for _,x in rows)
    gw=sum(x for _,x in rows if x>0); gl=sum(x for _,x in rows if x<0)
    pf=gw/abs(gl) if gl<0 else 99
    eq=0;peak=0;dd=0
    for _,x in rows: eq+=x;peak=max(peak,eq);dd=min(dd,eq-peak)
    print(f"{name:42s} {w/n*100:3.0f}% {tot:+8.1f} {tot/n:+6.2f} {pf:5.2f} {dd:+7.1f}")
