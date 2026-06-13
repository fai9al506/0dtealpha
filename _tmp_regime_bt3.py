"""Final: pin the recommended rule. Block SC/DD longs on down-regime; quantify
exact era $ delta, winners-given-up, and leave ES Abs untouched. Compare thresholds.
"""
import os, json
from collections import defaultdict
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
ERA = "2026-05-18"
with engine.connect() as conn:
    spath = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') as et, spot, vix
        FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE :era
        AND spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
        ORDER BY ts ASC"""), {"era": ERA}).fetchall()
    day_path = defaultdict(list)
    for et, spot, vix in spath:
        day_path[et.date().isoformat()].append((et, float(spot), float(vix) if vix else None))
    def from_open(day, ts):
        path=day_path.get(day,[]); prior=[p for p in path if p[0]<=ts]
        if not prior: return None
        return prior[-1][1]-path[0][1]
    rows = conn.execute(text("""
        SELECT sl.id,(sl.ts AT TIME ZONE 'America/New_York') as et,sl.setup_name,sl.direction,rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date >= DATE :era ORDER BY sl.ts ASC
    """), {"era": ERA}).fetchall()
T=[]
for sid,et,setup,direction,st in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    e=st.get('fill_price'); x=st.get('close_fill_price')
    if e is None or x is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short')
    pts=(e-x) if sh else (x-e)
    T.append({"setup":setup,"sh":sh,"pts":pts,"usd":pts*5.0,"fo":from_open(et.date().isoformat(),et)})

def net(ts): return sum(t['usd'] for t in ts)
def wr(ts): return 100*sum(1 for t in ts if t['pts']>0)/len(ts) if ts else 0

base_total=net(T)
print(f"BASELINE all placed trades: net=${base_total:+.0f}  (longs ${net([t for t in T if not t['sh']]):+.0f} / shorts ${net([t for t in T if t['sh']]):+.0f})\n")

SCDD=("Skew Charm","DD Exhaustion")
for thr in [-12,-15,-18,-20]:
    blocked=[t for t in T if (not t['sh']) and t['setup'] in SCDD and t['fo'] is not None and t['fo']<=thr]
    losers=[t for t in blocked if t['pts']<=0]; winners=[t for t in blocked if t['pts']>0]
    saved=-net(blocked)
    print(f"Rule: block SC/DD longs when from_open<={thr}:  blocks {len(blocked)}t "
          f"({len(winners)}W/{len(losers)}L), net of blocked=${net(blocked):+.0f}  "
          f"-> era delta ${saved:+.0f}  (gives up ${net(winners):+.0f} winners, saves ${-net(losers):+.0f} losers)")
print()
# Recommended -15: show resulting book
thr=-15
blocked=set(id(t) for t in T if (not t['sh']) and t['setup'] in SCDD and t['fo'] is not None and t['fo']<=thr)
kept=[t for t in T if id(t) not in blocked]
print(f"With SC/DD-long down-block (<= -15): book net ${net(kept):+.0f}  (was ${base_total:+.0f}) = +${net(kept)-base_total:.0f}")
print(f"  kept longs:  WR={wr([t for t in kept if not t['sh']]):.0f}% net=${net([t for t in kept if not t['sh']]):+.0f}")
print(f"  ES Abs longs (untouched): {sum(1 for t in T if not t['sh'] and t['setup']=='ES Absorption')}t "
      f"net=${net([t for t in T if not t['sh'] and t['setup']=='ES Absorption']):+.0f}")
# verify ES Abs longs on down days specifically stay
esdown=[t for t in T if not t['sh'] and t['setup']=='ES Absorption' and t['fo'] is not None and t['fo']<=-15]
print(f"  ES Abs longs on down days (KEPT): {len(esdown)}t WR={wr(esdown):.0f}% net=${net(esdown):+.0f}")
