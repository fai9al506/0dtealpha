"""TEST: dominant negative DD floor + TREND confirmation.
Hypothesis: the floor is real 'support' only when price is already trending up;
on chop/pin days the same config whipsaws. So gate longs on (DD floor below) AND
(uptrend), and check if shorts above the floor in an uptrend are the toxic set.

Trend proxies (from chain_snapshots spot only):
  open_spot = first spot at/after 09:30 ET that day
  mom20     = spot(entry) - spot(~20 min before entry)
  uptrend   = (spot - open_spot) >= OPEN_TREND_PTS  AND  mom20 >= 0
"""
import os, psycopg2, bisect
from datetime import date
from collections import defaultdict
from zoneinfo import ZoneInfo
UTC = ZoneInfo("UTC"); ET = ZoneInfo("America/New_York")
START, END = date(2026,4,1), date(2026,6,1)
DD_RATIO_MIN, DD_NEAR, DD_VAL_MIN, DD_MAX_ABOVE = 2.0, 15.0, 1.0e9, 15.0
OPEN_TREND_PTS = 5.0   # spot must be >=5pt above session open to call 'uptrend'

c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

# --- DD snapshots in memory ---
cur.execute("""SELECT ts_utc, strike::float, value::float, current_price::float
  FROM volland_exposure_points WHERE greek='deltaDecay' AND expiration_option='TODAY'
  AND ticker='SPX' AND ts_utc::date BETWEEN %s AND %s ORDER BY ts_utc""", (START, END))
snaps = defaultdict(list); spot_at = {}
for ts,s,v,cp in cur.fetchall():
    snaps[ts].append((s,v))
    if cp is not None: spot_at[ts]=cp
sts = sorted(snaps); aw = [t if t.tzinfo else t.replace(tzinfo=UTC) for t in sts]
def to_a(t): return t if t.tzinfo else t.replace(tzinfo=UTC)
def nsnap(ts):
    ts=to_a(ts); i=bisect.bisect_left(aw,ts); cand=[]
    if i<len(aw): cand.append(sts[i])
    if i>0: cand.append(sts[i-1])
    if not cand: return None
    b=min(cand,key=lambda t:abs((to_a(t)-ts).total_seconds()))
    return b if abs((to_a(b)-ts).total_seconds())<=240 else None
def dd_support(ts, spot):
    sn=nsnap(ts)
    if sn is None: return None
    negs=sorted([p for p in snaps[sn] if p[1]<0], key=lambda x:x[1])
    if not negs: return None
    ds,dv=negs[0]; second=abs(negs[1][1]) if len(negs)>=2 else 1.0
    ratio=abs(dv)/max(second,1.0); dist=spot-ds
    return (ratio>=DD_RATIO_MIN and abs(dv)>=DD_VAL_MIN and 0<dist<=DD_MAX_ABOVE
            and abs(ds-spot)<=DD_NEAR)

# --- spot time series per day for trend ---
cur.execute("""SELECT ts, ts AT TIME ZONE 'America/New_York' as t_et, spot::float
  FROM chain_snapshots WHERE ts::date BETWEEN %s AND %s AND spot IS NOT NULL
  ORDER BY ts""", (START, END))
day_series = defaultdict(list)  # date -> [(t_et, spot)]
for ts,t_et,sp in cur.fetchall():
    day_series[t_et.date()].append((t_et, sp))
open_spot = {}
for d, ser in day_series.items():
    after = [(t,s) for t,s in ser if t.time() >= __import__('datetime').time(9,30)]
    if after: open_spot[d] = after[0][1]
def trend(t_et, spot):
    d = t_et.date()
    op = open_spot.get(d)
    if op is None: return None
    ser = day_series[d]
    times = [t for t,_ in ser]
    # spot ~20 min before
    target = t_et - __import__('datetime').timedelta(minutes=20)
    i = bisect.bisect_left(times, target)
    prior = ser[max(0,min(i,len(ser)-1))][1]
    mom20 = spot - prior
    up = (spot - op) >= OPEN_TREND_PTS and mom20 >= 0
    return up

# --- trades ---
cur.execute("""SELECT id, ts, ts AT TIME ZONE 'America/New_York' as t_et, setup_name,
                      direction, spot::float, outcome_result, outcome_pnl
  FROM setup_log WHERE ts::date BETWEEN %s AND %s
   AND outcome_result IN ('WIN','LOSS','EXPIRED') AND outcome_pnl IS NOT NULL
   AND spot IS NOT NULL ORDER BY ts""", (START, END))
trades = cur.fetchall()
LONG={"long","bullish"}; SHORT={"short","bearish"}
def ns(): return {"n":0,"w":0,"pts":0.0}
def add(st,res,pnl): st["n"]+=1; st["w"]+=(res=="WIN"); st["pts"]+=float(pnl)
def ln(name,st):
    if st["n"]==0: return f"  {name:<42} n=0"
    return f"  {name:<42} n={st['n']:<3} WR={100*st['w']/st['n']:>4.0f}% pts={st['pts']:>+7.1f} avg={st['pts']/st['n']:>+5.2f}"

B = {k:ns() for k in [
  "LONG  floor+uptrend","LONG  floor+NOuptrend","LONG  nofloor+uptrend","LONG  nofloor+NOuptrend",
  "SHORT floor+uptrend","SHORT floor+NOuptrend","SHORT nofloor+uptrend","SHORT nofloor+NOuptrend"]}
G = {k:ns() for k in ["GEXL floor+uptrend","GEXL floor+NOuptrend","GEXL nofloor+uptrend","GEXL nofloor+NOuptrend"]}
skip=0
for (lid,ts,t_et,setup,dir_,spot,res,pnl) in trades:
    sup = dd_support(ts, spot); up = trend(t_et, spot)
    if sup is None or up is None: skip+=1; continue
    fl = "floor" if sup else "nofloor"; tr = "uptrend" if up else "NOuptrend"
    if (dir_ or "").lower() in LONG:
        add(B[f"LONG  {fl}+{tr}"],res,pnl)
        if setup=="GEX Long": add(G[f"GEXL {fl}+{tr}"],res,pnl)
    elif (dir_ or "").lower() in SHORT:
        add(B[f"SHORT {fl}+{tr}"],res,pnl)

print(f"(skipped {skip} trades w/o DD or trend data)\n")
print(f"DD floor: ratio>={DD_RATIO_MIN} |val|>={DD_VAL_MIN/1e9}B spot 0-{DD_MAX_ABOVE}pt above;  uptrend: spot>=open+{OPEN_TREND_PTS} & mom20>=0\n")
print("ALL LONGS:")
for k in [k for k in B if k.startswith("LONG")]: print(ln(k,B[k]))
print("\nALL SHORTS:")
for k in [k for k in B if k.startswith("SHORT")]: print(ln(k,B[k]))
print("\nGEX LONG only:")
for k in G: print(ln(k,G[k]))
cur.close(); c.close()
