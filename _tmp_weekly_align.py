"""Per Dark-Matter-week: his bias vs actual SPX move vs our system long/short PnL.
Weeks keyed to his plan Mondays. SPX move from setup_log spot (Mon first -> Fri last).
Our PnL = quality-traded outcome_pnl split long/short. Saves JSON for the report.
"""
import os, json
from collections import defaultdict
from datetime import timedelta, date
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])

# his weekly bias (extracted from the 8 plans)
HIS = [
 ("2026-04-13","LONG","NORMAL","buy dips, hold 6900; neg vanna but gamma dampening"),
 ("2026-04-20","SHORT/RANGE","NORMAL","short on break 7087; pullback week"),
 ("2026-04-27","RANGE","LOW","pin-and-fade; fade ceiling 7275-7300, buy 7150"),
 ("2026-05-04","NEUTRAL","LOW","pin-and-fade, bearish-tilt on break 7200"),
 ("2026-05-11","LONG","LOW","strong uptrend, buy dips"),
 ("2026-05-18","NEUTRAL","LOW","range 7300-7450, downside lean"),
 ("2026-05-25","LONG","LOW","range bullish, buy dips/break-go long"),
 ("2026-06-01","RANGE-BULL","LOW","corridor, mean-reversion, bullish lean"),
 ("2026-06-08","SHORT","EXTREME","amplification, fade rallies into 7575/7500"),
]

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
               greek_alignment, vix, spot, outcome_pnl
        FROM setup_log
        WHERE outcome_pnl IS NOT NULL
          AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
        ORDER BY ts ASC""")).fetchall()
    sp = conn.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, spot FROM setup_log
        WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::time>=TIME '09:30' ORDER BY ts""")).fetchall()

# spot by day
day_spots=defaultdict(list)
for et,s in sp: day_spots[et.date().isoformat()].append((et,float(s)))

def quality(setup,direction,grade,align):
    if grade in ('C','LOG',None): return False
    islong=direction in ('long','bullish'); a=align or 0
    if setup=='ES Absorption' and grade not in ('A','A+'): return False
    if setup=='DD Exhaustion' and islong and (a<0 or a>=3): return False
    return True

last={}; T=[]
for et,setup,direction,grade,align,vix,spot,pnl in rows:
    islong=direction in ('long','bullish'); key=(setup,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15): continue
    last[key]=et
    if not quality(setup,direction,grade,align): continue
    T.append({"et":et,"d":et.date().isoformat(),"islong":islong,"vix":float(vix) if vix else None,"pnl":float(pnl)})

def wk_range(mon):
    y,m,d=map(int,mon.split("-")); start=date(y,m,d); end=start+timedelta(days=4)
    return start.isoformat(), end.isoformat()

out=[]
for mon,bias,vol,note in HIS:
    s,e=wk_range(mon)
    days=[d for d in day_spots if s<=d<=e]
    spx_chg=None; vixavg=None
    if days:
        first=day_spots[min(days)][0][1]; last=day_spots[max(days)][-1][1]
        spx_chg=round(last-first,1)
    wkt=[t for t in T if s<=t['d']<=e]
    L=[t for t in wkt if t['islong']]; S=[t for t in wkt if not t['islong']]
    vv=[t['vix'] for t in wkt if t['vix']]
    vixavg=round(sum(vv)/len(vv),1) if vv else None
    out.append({"week":mon,"his_bias":bias,"his_vol":vol,"note":note,
        "spx_week_chg":spx_chg,"vix_avg":vixavg,
        "long_n":len(L),"long_pnl":round(sum(t['pnl'] for t in L)*5),
        "short_n":len(S),"short_pnl":round(sum(t['pnl'] for t in S)*5),
        "total_pnl":round(sum(t['pnl'] for t in wkt)*5)})

print(f"{'week':<12}{'his_bias':<12}{'vol':<8}{'SPXwk':>7}{'VIXavg':>7}{'L$':>7}{'S$':>7}{'tot$':>7}  note")
for o in out:
    print(f"{o['week']:<12}{o['his_bias']:<12}{o['his_vol']:<8}{str(o['spx_week_chg']):>7}{str(o['vix_avg']):>7}"
          f"{o['long_pnl']:>7}{o['short_pnl']:>7}{o['total_pnl']:>7}  {o['note'][:40]}")

with open("_tmp_weekly_align.json","w") as f: json.dump(out,f,indent=2)
print("\nsaved _tmp_weekly_align.json")
