"""HIGH-FREQUENCY intraday level-to-level engine (his actual style: jump strike to strike).
Build the full vanna multi-expiry GRID. Each 15-min bar:
  NORMAL regime (VIX<20): MEAN-REVERT between adjacent grid levels —
     LONG when bar dips to support S and closes back above it -> target next level up R, stop S-BUF
     SHORT when bar pops to resistance R and closes back below it -> target next level down S, stop R+BUF
     (re-enter freely -> many legs/day). Semi gate: don't fight the basket.
  EXTREME regime (VIX>=20 & semis directional): RIDE breaks (runner trail) — the cascade.
Tune on Mar16-Apr30 (IS), validate May-Jun (OOS). P&L pts (x5=$@1MES).
"""
import os
from datetime import timedelta
from collections import defaultdict
import _tmp_l2l_engine as E
from sqlalchemy import text

basket=[(r[0],float(r[1])) for r in E.CONN.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bday=defaultdict(list)
for et,v in basket: bday[et.date().isoformat()].append((et,v))
def bstr(day,t):
    arr=bday.get(day,[]); prior=[v for (x,v) in arr if x<=t]
    return prior[-1] if prior else None

def grid(day, P):
    spot0=E.bars15(day)[0]["o"]
    lmap=E.level_map(day, E.bars15(day)[0]["t"]+timedelta(minutes=10))
    return sorted(k for k,v in lmap.items() if abs(v)>P["minv"] and abs(k-spot0)<=P.get("band",90))

def run_day(day,P):
    bars=E.bars15(day)
    if len(bars)<6: return []
    G=grid(day,P)
    if len(G)<3: return []
    sticky="NORMAL"
    trades=[]; pos=None; best=None
    for bar in bars:
        t=bar["t"]; c=bar["c"]; tn=t.replace(tzinfo=None); v=bar; sb=bstr(day,tn)
        # regime (VIX sticky via setup_log vix series approximated by semi-day? use VIX from engine)
        vix=E.vix_series(day); vv=[x for (e,x) in vix if e<=t]; vnow=vv[-1] if vv else 18
        if vnow>=20: sticky="EXTREME"
        elif vnow<18: sticky="NORMAL"
        below=[g for g in G if g<c-1]; above=[g for g in G if g>c+1]
        S=max(below) if below else None; R=min(above) if above else None
        if pos:
            d,en,stop,tgt,mode=pos
            if d=="L":
                if mode=="trend":
                    best=max(best,v["h"]);
                    if best>=en+P["act"]: stop=max(stop,best-P["gap"])
                hs=v["l"]<=stop; ht=(tgt is not None and v["h"]>=tgt)
                if hs or ht or bar is bars[-1]:
                    px=stop if hs else (tgt if ht else c); trades.append({"day":day,"dir":d,"pts":px-en,"sb":sb,"m":mode}); pos=None
            else:
                if mode=="trend":
                    best=min(best,v["l"]);
                    if best<=en-P["act"]: stop=min(stop,best+P["gap"])
                hs=v["h"]>=stop; ht=(tgt is not None and v["l"]<=tgt)
                if hs or ht or bar is bars[-1]:
                    px=stop if hs else (tgt if ht else c); trades.append({"day":day,"dir":d,"pts":en-px,"sb":sb,"m":mode}); pos=None
            continue
        if sticky=="EXTREME" and sb is not None:
            # ride breaks (runner)
            if S is not None and c<S-P["confirm"] and sb<=-P["gate"]:
                pos=("S",c,c+P["istop"],None,"trend"); best=c; continue
            if R is not None and c>R+P["confirm"] and sb>=P["gate"]:
                pos=("L",c,c-P["istop"],None,"trend"); best=c; continue
        else:
            # NORMAL: mean-revert between S and R (many legs)
            if S is not None and R is not None:
                # long off support hold
                if v["l"]<=S+P["touch"] and c>S and (sb is None or sb>=-P["semi_block"]):
                    pos=("L",c,S-P["buf"],R,"mr"); continue
                # short off resistance reject
                if v["h"]>=R-P["touch"] and c<R and (sb is None or sb<=P["semi_block"]):
                    pos=("S",c,R+P["buf"],S,"mr"); continue
    return trades

def per(d): return "IS" if d<"2026-05-01" else "OOS"
def stt(ts):
    if not ts: return "n=0"
    w=sum(1 for t in ts if t['pts']>0); tot=sum(t['pts'] for t in ts)
    cum=0;pk=0;mdd=0
    for t in ts:
        cum+=t['pts']; pk=max(pk,cum); mdd=min(mdd,cum-pk)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% pts={tot:+6.0f}(${tot*5:+6.0f}) avg{tot/len(ts):+5.1f} mdd${mdd*5:+.0f}"

import itertools
print("=== IS (Mar16-Apr30) grid ===")
grids=[{"minv":m,"buf":b,"touch":5,"confirm":2,"istop":16,"act":12,"gap":16,"gate":0.5,"semi_block":1.0,"band":90}
       for m in (6e7,1e8) for b in (6,9)]
results=[]
for P in grids:
    t=[x for d in E.days_between("2026-03-16","2026-04-30") for x in run_day(d,P)]
    results.append((P,t)); print(f"  minv={P['minv']/1e6:.0f}M buf={P['buf']}: {stt(t)}")
# pick best positive
viab=[(P,t) for P,t in results if sum(x['pts'] for x in t)>0 and len(t)>=40]
if not viab:
    print("\nNo viable positive IS config (n>=40, $>0).")
else:
    best=max(viab,key=lambda x:sum(t['pts'] for t in x[1]))[0]
    print(f"\n>>> SELECTED: minv={best['minv']/1e6:.0f}M buf={best['buf']}")
    oos=[x for d in E.days_between("2026-05-01","2026-06-09") for x in run_day(d,best)]
    print(f"OOS: {stt(oos)}")
    print(f"  OOS mean-revert: {stt([x for x in oos if x['m']=='mr'])}")
    print(f"  OOS trend rides: {stt([x for x in oos if x['m']=='trend'])}")
    # trades/day
    days=len(set(x['day'] for x in oos))
    print(f"  OOS trades/day: {len(oos)/max(days,1):.1f}")
