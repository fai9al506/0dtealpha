"""DYNAMIC intraday regime backtest (post-V16: 2026-05-18 .. 2026-06-09).
Regime recomputed every 10 min from live VIX (his explicit gate) with sticky
hysteresis (his 'EXTREME until VIX compresses below ~18-19'):
   state -> EXTREME when VIX crosses >= 20
   state -> NORMAL  when VIX drops  <  18
   in between (18-20): HOLD prior state (sticky).
State carries across days (regime is multi-session, like his read).

Overlay tested: in EXTREME state, his playbook = fade rallies / dip-longs scout-only.
  A) drop SC/DD counter-trend LONGS (keep ES Abs reversal + all shorts)
  B) shorts only (drop ALL longs)
Compare to baseline (take everything). P&L = outcome_pnl*5 ($@1MES), quality set, dedup 15min.
"""
import os
from collections import defaultdict
from datetime import timedelta, datetime, time as dtime
from sqlalchemy import create_engine, text
engine=create_engine(os.environ['DATABASE_URL'])
ERA="2026-05-18"

with engine.connect() as conn:
    rows=conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
               greek_alignment, vix, outcome_pnl
        FROM setup_log
        WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE :era
          AND vix IS NOT NULL
        ORDER BY ts ASC"""), {"era":ERA}).fetchall()

# 1) build per-day 10-min VIX grid (forward-fill from any signal's vix)
vix_pts=defaultdict(list)
for et,_,_,_,_,vix,_ in rows:
    vix_pts[et.date().isoformat()].append((et,float(vix)))

def grid_regime():
    """Walk chronological 10-min marks across all post-V16 trading days; sticky state."""
    state="NORMAL"; timeline={}  # (day, 10min-bucket-start) -> state
    days=sorted(vix_pts.keys())
    for d in days:
        pts=sorted(vix_pts[d])
        # 10-min marks 09:30..16:00
        for h in range(9,16):
            for m in (range(30,60,10) if h==9 else range(0,60,10)):
                mark=datetime.fromisoformat(d).replace(hour=h,minute=m)
                # last vix at/before mark
                prior=[v for (t,v) in pts if t.replace(tzinfo=None)<=mark]
                if not prior:
                    timeline[(d,h,m)]=state; continue
                v=prior[-1]
                if v>=20: state="EXTREME"
                elif v<18: state="NORMAL"
                # else hold
                timeline[(d,h,m)]=state
    return timeline
TL=grid_regime()
def regime_at(et):
    d=et.date().isoformat(); h=et.hour; m=(et.minute//10)*10
    return TL.get((d,h,m),"NORMAL")

# 2) quality trades
def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    islong=d in ('long','bullish');aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and islong and (aa<0 or aa>=3): return False
    return s in ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')

last={};T=[]
for et,s,d,g,a,vix,p in rows:
    islong=d in ('long','bullish');key=(s,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15):continue
    last[key]=et
    if not quality(s,d,g,a):continue
    T.append({"et":et,"day":et.date().isoformat(),"setup":s,"islong":islong,
              "usd":float(p)*5,"vix":float(vix),"reg":regime_at(et)})

# 3) regime timeline summary (how much of each day was EXTREME)
print("=== Dynamic regime timeline (post-V16) — EXTREME windows ===")
ext_marks=defaultdict(int); tot_marks=defaultdict(int)
for (d,h,m),st in TL.items():
    tot_marks[d]+=1
    if st=="EXTREME": ext_marks[d]+=1
for d in sorted(tot_marks):
    if ext_marks[d]>0:
        print(f"  {d}: EXTREME {ext_marks[d]}/{tot_marks[d]} ten-min marks")
print("  (days not listed = fully NORMAL all session)\n")

# 4) trades by regime
def stt(ts):
    if not ts: return "n=  0"
    w=sum(1 for t in ts if t['usd']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% ${sum(t['usd'] for t in ts):+7.0f}"
EX=[t for t in T if t['reg']=="EXTREME"]; NO=[t for t in T if t['reg']=="NORMAL"]
print("Trades tagged by DYNAMIC regime at entry:")
print(f"  EXTREME: longs {stt([t for t in EX if t['islong']])}  shorts {stt([t for t in EX if not t['islong']])}")
print(f"  NORMAL : longs {stt([t for t in NO if t['islong']])}  shorts {stt([t for t in NO if not t['islong']])}")
print()

# 5) overlays vs baseline
def overlay(t, mode):
    if t['reg']!="EXTREME": return True  # normal regime: keep all
    if mode=="A":  # drop SC/DD longs only
        if t['islong'] and t['setup'] in ('Skew Charm','DD Exhaustion'): return False
        return True
    if mode=="B":  # shorts only
        return not t['islong']
    return True

base=sum(t['usd'] for t in T)
for mode in ("A","B"):
    kept=[t for t in T if overlay(t,mode)]
    dropped=[t for t in T if not overlay(t,mode)]
    print(f"OVERLAY {mode}: baseline ${base:+.0f} -> ${sum(t['usd'] for t in kept):+.0f}  "
          f"(delta ${sum(t['usd'] for t in kept)-base:+.0f}; dropped {len(dropped)} trades worth ${sum(t['usd'] for t in dropped):+.0f})")
# by day for overlay A
print("\nBy-day (overlay A = drop SC/DD longs in dynamic-EXTREME):")
days=sorted(set(t['day'] for t in T))
for d in days:
    b=[t for t in T if t['day']==d]; k=[t for t in b if overlay(t,'A')]
    bd=sum(t['usd'] for t in b); kd=sum(t['usd'] for t in k)
    flag=" *EXTREME-active*" if any(t['reg']=="EXTREME" for t in b) else ""
    if abs(kd-bd)>1 or flag:
        print(f"  {d}: base ${bd:+.0f} -> overlayA ${kd:+.0f} (delta ${kd-bd:+.0f}){flag}")
