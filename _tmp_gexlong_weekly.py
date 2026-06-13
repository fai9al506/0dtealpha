"""Decision summary: clean GEX Long v3.1 & v3.2 (portal overlay) — weekly PnL +
drawdown breakdown, totals, cadence, monthly projection. outcome in POINTS; $=x5 (1 MES)."""
import json
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text
from app.gex_long_v3 import _build_cache
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine=create_engine(DB)

overlay=_build_cache(engine)
liddate={}
with engine.begin() as cx:
    for lid,d in cx.execute(text("""SELECT id,(ts AT TIME ZONE 'America/New_York')::date
        FROM setup_log WHERE setup_name='GEX Long' AND grade!='LOG'""")):
        liddate[lid]=d

def weekstart(d): return d - timedelta(days=d.weekday())  # Monday

def collect(flag):
    rows=[]
    for lid,o in overlay.items():
        d=liddate.get(lid)
        if d is None or o.get('result') is None: continue
        if o.get(flag): rows.append((d,o['result'],o['pnl']))
    return sorted(rows)

def maxdd(seq):  # seq of pnl in order
    eq=0;peak=0;dd=0
    for p in seq: eq+=p;peak=max(peak,eq);dd=min(dd,eq-peak)
    return dd

def report(flag,label):
    rows=collect(flag)
    if not rows:
        print(f"{label}: no trades"); return
    byw=defaultdict(list)
    for d,res,pnl in rows: byw[weekstart(d)].append((d,res,pnl))
    print(f"\n{'='*78}\n{label}\n{'='*78}")
    print(f"{'week of':12s} {'trades':>6s} {'WR':>5s} {'PnL pts':>9s} {'$@1MES':>8s} {'weekDD':>8s}")
    cum=[]
    for wk in sorted(byw):
        w=byw[wk]; n=len(w); wins=sum(1 for _,r,_ in w if r=='WIN'); tot=sum(p for _,_,p in w)
        wdd=maxdd([p for _,_,p in w]); cum+=[p for _,_,p in w]
        print(f"{str(wk):12s} {n:6d} {wins/n*100:4.0f}% {tot:+9.1f} {tot*5:+8.0f} {wdd:+8.1f}")
    n=len(rows); wins=sum(1 for _,r,_ in rows if r=='WIN'); tot=sum(p for _,_,p in rows)
    odd=maxdd([p for _,_,p in rows])
    d0,d1=rows[0][0],rows[-1][0]; span_days=(d1-d0).days+1; months=span_days/30.4
    nweeks=len(byw)
    print(f"{'-'*78}")
    print(f"TOTAL: {n} trades / {len(set(d for d,_,_ in rows))} days / {nweeks} active weeks "
          f"over {d0} -> {d1} ({months:.1f} mo)")
    print(f"  WR={wins/n*100:.0f}%  TOTAL={tot:+.1f}p (${tot*5:+,.0f} @1MES)  overall maxDD={odd:+.1f}p (${odd*5:+,.0f})")
    print(f"  cadence: {n/months:.1f} trades/mo  |  avg {tot/n:+.2f}p/trade")
    print(f"  MONTHLY PROJECTION: {tot/months:+.1f}p/mo  =  ${tot/months*5:+,.0f}/mo @1MES  "
          f"(${tot/months*5*3:+,.0f}/mo @3MES, ${tot/months*5*5:+,.0f}/mo @5MES)")

report('pass',"GEX Long v3.1 (align>=0)  — the conservative keeper")
report('pass_v32',"GEX Long v3.2 (align>=0 OR bull-paradigm) — current shipped portal variant")
