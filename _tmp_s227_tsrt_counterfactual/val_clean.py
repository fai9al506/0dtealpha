"""Gate-2: validate the BASIS-CLEAN mes estimator against broker truth (tsrt_daily_stmt).

Mimics counterfactual conditions exactly: no signal_es_price / no fill_price is passed
(the blocked candidates don't have one), spx_spot fallback disabled, entry resolved from
ES bars only. If this estimator's daily bias vs the broker is no worse than S227's
(+$9.3/day, t=0.24), the July-August counterfactual built on it is trustworthy.
"""
import os, sys, json, statistics, math
sys.path.insert(0, "G:/My Drive/Python/MyProject/GitHub/0dtealpha")
from engine import *
from sqlalchemy import text as _t
from datetime import timedelta
from app import mes_sim_backfill as M

M.V14_WHITELIST = set(M.V14_WHITELIST) | {"GEX Long"}
M._DEFAULT_PARAMS["GEX Long"] = {"sl": 14, "be_trigger": None, "be_lock": 0,
                                 "trail_act": 10, "trail_gap": 5}
_orig = M._first_es_open_after


def _entry_es(engine, t_utc, max_wait_minutes=10):
    v = _orig(engine, t_utc, max_wait_minutes)
    if v is not None:
        return v
    with engine.begin() as conn2:
        row = conn2.execute(_t("""
            SELECT bar_close FROM vps_es_range_bars
            WHERE range_pts=5 AND ts_start <= :t AND ts_start >= :lo
            ORDER BY ts_start DESC LIMIT 1"""),
            {"t": t_utc, "lo": t_utc - timedelta(minutes=60)}).fetchone()
    return float(row[0]) if row and row[0] is not None else None


M._first_es_open_after = _entry_es

with conn() as c:
    rows = c.execute(_t("""
      SELECT s.id,(s.ts AT TIME ZONE 'America/New_York') et,s.setup_name,s.direction,s.spot,
             s.outcome_stop_level,s.outcome_target_level,s.trail_sl,s.trail_activation,
             s.trail_gap,s.outcome_elapsed_min,s.basket_pct,s.ts
      FROM real_trade_orders o JOIN setup_log s ON s.id=o.setup_log_id
      ORDER BY s.ts""")).fetchall()
    stmt = {r[0]: float(r[1]) for r in c.execute(_t("SELECT day,net FROM tsrt_daily_stmt ORDER BY day"))}
print(f"placed trades {len(rows)}   broker sessions {len(stmt)}")
bars = load_bars(sorted({r[1].date() for r in rows}))

daily = defaultdict(float); dn = defaultdict(int); cov = defaultdict(lambda: [0, 0])
for (lid, et, setup, direction, spot, sl, tl, tsl, tact, tgap, em, bp, ts_utc) in rows:
    il = direction.lower() in ("long", "bullish")
    sim = M.compute_mes_sim_outcome(
        ENG, setup_log_id=lid, setup_name=setup, direction=direction, signal_ts=ts_utc,
        spx_spot=None,
        trail_sl=float(tsl) if tsl is not None else None,
        trail_activation=float(tact) if tact is not None else None,
        trail_gap=float(tgap) if tgap is not None else None,
        signal_es_price=None, fill_price=None,
        outcome_elapsed_min=em)
    d = et.date(); cov[d][0] += 1
    if sim and sim.get("mes_sim_outcome_pnl") is not None:
        pts = float(sim["mes_sim_outcome_pnl"]); cov[d][1] += 1
    else:
        sp = stop_for(setup, il, tsl, float(spot), float(sl) if sl else None)
        tp = float(tl) if (setup == "Vanna Pivot Bounce" and tl) else None
        pts, _, _ = walk(bars[d], et, float(spot), il, sp, setup, tp)
    qty = 2 if (bp is not None and abs(float(bp)) >= 0.15 and ((float(bp) > 0) == il)) else 1
    daily[d] += pts * DOLLAR_PER_PT * qty - COMM_PER_CONTRACT * qty
    dn[d] += 1

common = sorted(set(daily) & set(stmt))
print(f"\n{'date':<12}{'sim$':>9}{'broker$':>9}{'diff':>8}{'n':>4}{'mes':>5}")
diffs = []
for d in common:
    diffs.append(daily[d] - stmt[d])
    print(f"{str(d):<12}{daily[d]:>9.0f}{stmt[d]:>9.0f}{daily[d]-stmt[d]:>8.0f}{dn[d]:>4}{cov[d][1]:>5}")
ts_ = sum(daily[d] for d in common); tb = sum(stmt[d] for d in common)
print(f"\nSIM ${ts_:,.0f}   BROKER ${tb:,.0f}   diff ${ts_-tb:,.0f}")
n = len(diffs); se = statistics.stdev(diffs)/math.sqrt(n)
print(f"sessions {n}  mean daily bias ${statistics.mean(diffs):+,.1f}  t={statistics.mean(diffs)/se:+.2f}")
print(f"mean abs ${statistics.mean(abs(x) for x in diffs):,.0f}  median abs ${statistics.median(abs(x) for x in diffs):,.0f}")
print(f"sign agreement {sum(1 for d in common if (daily[d]>0)==(stmt[d]>0))}/{n}")
