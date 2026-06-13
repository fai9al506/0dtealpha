"""CASCADE-RIDE backtest — capture the BIG trend moves (his +60pt rides), gated by
semi-confirmation, with RUNNER exits (wide trailing stop, no tiny target).

SHORT  when a 15-min bar closes below a key vanna SUPPORT level AND semis are RED.
LONG   when a 15-min bar closes above a key vanna RESISTANCE level AND semis are GREEN.
Exit: wide initial stop; once +ACT in favor, trail by GAP from best; hold to 15:55.
This lets a 60pt cascade RUN while semi-confirm filters the fakeouts.
P&L in points (x5=$@1MES). IS(Mar16-Apr30) / OOS(May-Jun).
"""
import os
from datetime import timedelta, datetime
from collections import defaultdict
import _tmp_l2l_engine as E
from sqlalchemy import text

# semi basket
basket=[(r[0],float(r[1])) for r in E.CONN.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bday=defaultdict(list)
for et,v in basket: bday[et.date().isoformat()].append((et,v))
def bstr(day,t):
    arr=bday.get(day,[]); prior=[v for (x,v) in arr if x<=t]
    return prior[-1] if prior else None

MINV=8e7; CONFIRM=2; INIT_STOP=18; ACT=12; GAP=18; GATE=0.5   # semis decisively red/green
def run_day(day, P):
    bars=E.bars15(day)
    if len(bars)<6: return []
    spot0=bars[0]["o"]
    lmap=E.level_map(day, bars[0]["t"]+timedelta(minutes=10))
    res=sorted(k for k,v in lmap.items() if k>spot0-30 and v>MINV)   # resistance walls
    sup=sorted((k for k,v in lmap.items() if k<spot0+30 and v<-MINV), reverse=True)  # support floors
    trades=[]; pos=None; best=None; took={"S":0,"L":0}; MAXE=P.get("maxe",1)
    armed_s=False; armed_l=False; prev_c=None
    for bar in bars:
        t=bar["t"]; c=bar["c"]; tn=t.replace(tzinfo=None)
        sb=bstr(day, tn)
        if pos:
            d,en,stop=pos
            if d=="S":
                best=min(best,bar["l"])
                if best<=en-ACT: stop=min(stop, best+GAP)
                if bar["h"]>=stop or bar is bars[-1]:
                    px=stop if bar["h"]>=stop else c
                    trades.append({"day":day,"dir":d,"pts":en-px,"sb":sb}); pos=None
            else:
                best=max(best,bar["h"])
                if best>=en+ACT: stop=max(stop, best-GAP)
                if bar["l"]<=stop or bar is bars[-1]:
                    px=stop if bar["l"]<=stop else c
                    trades.append({"day":day,"dir":d,"pts":px-en,"sb":sb}); pos=None
            prev_c=c; continue
        if sb is None: prev_c=c; continue
        # 2-BAR CONFIRM cascade SHORT: prior bar AND this bar both close below a support floor + semis red
        brk_now=any(c<k-CONFIRM for k in sup)
        brk_prev=prev_c is not None and any(prev_c<k-CONFIRM for k in sup)
        if brk_now and brk_prev and sb<=-GATE and took["S"]<MAXE:
            pos=("S",c,c+INIT_STOP); best=c; took["S"]+=1; prev_c=c; continue
        # 2-BAR CONFIRM breakout LONG
        bru_now=any(c>k+CONFIRM for k in res)
        bru_prev=prev_c is not None and any(prev_c>k+CONFIRM for k in res)
        if bru_now and bru_prev and sb>=GATE and took["L"]<MAXE:
            pos=("L",c,c-INIT_STOP); best=c; took["L"]+=1
        prev_c=c
    return trades

def per(d): return "IS" if d<"2026-05-01" else "OOS"
def stt(ts):
    if not ts: return "n=0"
    w=sum(1 for t in ts if t['pts']>0); tot=sum(t['pts'] for t in ts)
    big=max((t['pts'] for t in ts), default=0)
    return f"n={len(ts):>2} WR={100*w/len(ts):3.0f}% pts={tot:+6.0f} (${tot*5:+6.0f}) avg{tot/len(ts):+5.1f} best{big:+.0f}"

allt=[]
for day in E.days_between("2026-03-16","2026-06-09"):
    allt+=run_day(day, {})
for label in ("IS","OOS"):
    P=[t for t in allt if per(t['day'])==label]
    S=[t for t in P if t['dir']=='S']; L=[t for t in P if t['dir']=='L']
    print(f"\n### {label} — {len(P)} trades ###")
    print("  ALL   :", stt(P)); print("  SHORTS:", stt(S)); print("  LONGS :", stt(L))
# show biggest single trades + the Jun 9 / Jun 5 days
print("\nBiggest trades:")
for t in sorted(allt,key=lambda x:-x['pts'])[:8]:
    print(f"  {t['day']} {t['dir']} {t['pts']:+.0f}pts (semis {t['sb']:+.1f}%)")
print("\nJun 5 & Jun 9:")
for t in [x for x in allt if x['day'] in ('2026-06-05','2026-06-09')]:
    print(f"  {t['day']} {t['dir']} {t['pts']:+.0f}pts (semis {t['sb']:+.1f}%)")
