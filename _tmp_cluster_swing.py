"""BIDIRECTIONAL CLUSTER-SWING setup (Dark Matter swing-ride, semi-gated, asymmetric).
MAJOR clusters = strong multi-expiry vanna strikes. At a support cluster:
  semis GREEN + price HOLDS (close back above)  -> LONG, ride to next resistance cluster
  semis RED   + price BREAKS (2-bar close below) -> SHORT, ride to next support cluster
Runner trail (let it run). Reports swings/MONTH by regime (honest frequency).
Tune MAJOR/gate on IS(Mar16-Apr30), validate OOS(May-Jun). pts x5=$@1MES.
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

ISTOP=16; ACT=12; GAP=16; CONFIRM=2; TOUCH=5
def run_day(day,P):
    bars=E.bars15(day)
    if len(bars)<6: return []
    spot0=bars[0]["o"]
    lmap=E.level_map(day, bars[0]["t"]+timedelta(minutes=10))
    sup=sorted((k for k,v in lmap.items() if v< -P["major"] and k<spot0+30), reverse=True)  # support clusters
    res=sorted(k for k,v in lmap.items() if v> P["major"] and k>spot0-30)                    # resistance clusters
    if not sup and not res: return []
    trades=[]; pos=None; best=None; took={"S":0,"L":0}; prev_c=None
    for bar in bars:
        t=bar["t"]; c=bar["c"]; tn=t.replace(tzinfo=None); sb=bstr(day,tn)
        vix=E.vix_series(day); vv=[x for (e,x) in vix if e<=t]; vnow=vv[-1] if vv else 18
        if pos:
            d,en,stop=pos
            if d=="S":
                best=min(best,bar["l"]);
                if best<=en-ACT: stop=min(stop,best+GAP)
                if bar["h"]>=stop or bar is bars[-1]:
                    px=stop if bar["h"]>=stop else c
                    trades.append({"day":day,"mo":day[:7],"dir":"S","pts":en-px,"sb":sb,"vix":vnow}); pos=None
            else:
                best=max(best,bar["h"]);
                if best>=en+ACT: stop=max(stop,best-GAP)
                if bar["l"]<=stop or bar is bars[-1]:
                    px=stop if bar["l"]<=stop else c
                    trades.append({"day":day,"mo":day[:7],"dir":"L","pts":px-en,"sb":sb,"vix":vnow}); pos=None
            prev_c=c; continue
        if sb is None: prev_c=c; continue
        # SHORT break-ride: 2-bar close below a support cluster + semis red
        brk=any(c<k-CONFIRM for k in sup); brk_p=prev_c is not None and any(prev_c<k-CONFIRM for k in sup)
        if brk and brk_p and sb<=-P["gate"] and took["S"]<P["maxe"]:
            pos=("S",c,c+ISTOP); best=c; took["S"]+=1; prev_c=c; continue
        # LONG hold-ride: bar dips to a support cluster and closes back above + semis green
        held=any(bar["l"]<=k+TOUCH and c>k for k in sup)
        if held and sb>=P["gate"] and took["L"]<P["maxe"]:
            pos=("L",c,c-ISTOP); best=c; took["L"]+=1
        prev_c=c
    return trades

def per(d): return "IS" if d<"2026-05-01" else "OOS"
def stt(ts):
    if not ts: return "n=0"
    w=sum(1 for t in ts if t['pts']>0); tot=sum(t['pts'] for t in ts)
    return f"n={len(ts):>2} WR={100*w/len(ts):3.0f}% pts={tot:+5.0f}(${tot*5:+5.0f}) avg{tot/len(ts):+5.1f} best{max((t['pts'] for t in ts),default=0):+.0f}"

print("=== IS grid (Mar16-Apr30) ===")
grids=[{"major":m,"gate":g,"maxe":3} for m in (1.2e8,1.6e8) for g in (0.4,0.7)]
results=[]
for P in grids:
    t=[x for d in E.days_between("2026-03-16","2026-04-30") for x in run_day(d,P)]
    results.append((P,t)); print(f"  major={P['major']/1e6:.0f}M gate={P['gate']}: {stt(t)}")
viab=[(P,t) for P,t in results if sum(x['pts'] for x in t)>0]
best=max(viab,key=lambda x:sum(t['pts'] for t in x[1]))[0] if viab else grids[0]
print(f"\n>>> using major={best['major']/1e6:.0f}M gate={best['gate']}")
oos=[x for d in E.days_between("2026-05-01","2026-06-09") for x in run_day(d,best)]
allt=[x for d in E.days_between("2026-03-16","2026-06-09") for x in run_day(d,best)]
print(f"\nOOS ALL: {stt(oos)}  | shorts {stt([x for x in oos if x['dir']=='S'])} | longs {stt([x for x in oos if x['dir']=='L'])}")
print("\n--- BY MONTH (frequency + regime) ---")
bm=defaultdict(list)
for x in allt: bm[x['mo']].append(x)
for mo in sorted(bm):
    hi=sum(1 for x in bm[mo] if x['vix']>=20)
    print(f"  {mo}: {stt(bm[mo])}  (high-VIX trades: {hi}/{len(bm[mo])})")
print("\nBiggest swings:")
for t in sorted(allt,key=lambda x:-x['pts'])[:8]:
    print(f"  {t['day']} {t['dir']} {t['pts']:+.0f}pts (semis {t['sb']:+.1f}%, vix {t['vix']:.0f})")
