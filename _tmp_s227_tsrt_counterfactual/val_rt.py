"""Gate-2 for the FINAL estimator (mesfill3: basis-clean entry + real_trader trail config).

Replays every trade TSRT actually placed, sizes it the way the live code would
(basket 0/1/2), sums per ET day, and compares to tsrt_daily_stmt (broker truth).
"""
from engine import *
from sqlalchemy import text
import statistics, math, json

MES = {int(k): v for k, v in json.load(open("mesfill_rt_val.json")).items()}
with conn() as c:
    rows = c.execute(text("""
      SELECT s.id,(s.ts AT TIME ZONE 'America/New_York') et,s.setup_name,s.direction,s.spot,
             s.outcome_stop_level,s.outcome_target_level,s.trail_sl,s.basket_pct
      FROM real_trade_orders o JOIN setup_log s ON s.id=o.setup_log_id
      ORDER BY s.ts""")).fetchall()
    stmt = {r[0]: float(r[1]) for r in c.execute(text("SELECT day,net FROM tsrt_daily_stmt ORDER BY day"))}
bars = load_bars(sorted({r[1].date() for r in rows}))
daily = defaultdict(float); dn = defaultdict(int); cov = defaultdict(lambda: [0, 0])
for (lid, et, setup, direction, spot, sl, tl, tsl, bp) in rows:
    il = direction.lower() in ("long", "bullish")
    d = et.date(); cov[d][0] += 1
    if lid in MES:
        pts = MES[lid]; cov[d][1] += 1
    else:
        sp = stop_for(setup, il, tsl, float(spot), float(sl) if sl else None)
        tp = float(tl) if (setup == "Vanna Pivot Bounce" and tl) else None
        pts, _, _ = walk(bars[d], et, float(spot), il, sp, setup, tp)
    qty = 2 if (bp is not None and abs(float(bp)) >= 0.15 and ((float(bp) > 0) == il)) else 1
    daily[d] += pts * DOLLAR_PER_PT * qty - COMM_PER_CONTRACT * qty
    dn[d] += 1
common = sorted(set(daily) & set(stmt))
print(f"{'date':<12}{'sim$':>9}{'broker$':>9}{'diff':>8}{'n':>4}{'mes':>5}")
diffs = []
for d in common:
    diffs.append(daily[d] - stmt[d])
    print(f"{str(d):<12}{daily[d]:>9.0f}{stmt[d]:>9.0f}{daily[d]-stmt[d]:>8.0f}{dn[d]:>4}{cov[d][1]:>5}")
ts_ = sum(daily[d] for d in common); tb = sum(stmt[d] for d in common)
n = len(diffs); se = statistics.stdev(diffs)/math.sqrt(n)
print(f"\nSIM ${ts_:,.0f}  BROKER ${tb:,.0f}  diff ${ts_-tb:,.0f}")
print(f"sessions {n}  mean daily bias ${statistics.mean(diffs):+,.1f}  t={statistics.mean(diffs)/se:+.2f}")
print(f"mean abs ${statistics.mean(abs(x) for x in diffs):,.0f}  median abs ${statistics.median(abs(x) for x in diffs):,.0f}")
print(f"sign agreement {sum(1 for d in common if (daily[d]>0)==(stmt[d]>0))}/{n}")
print(f"mes coverage {sum(v[1] for v in cov.values())}/{sum(v[0] for v in cov.values())}")
