# AG Short SL audit (2026-06-07): replay all AG Short signals Apr 1+ on MES 5pt
# range bars (S55 mes_walk) with a sweep of initial SLs, trail BE@10/act12/gap5
# (live config from main.py _trail_params). Era: Apr 9 = AG real-trade go-live;
# trail params constant (act=12 gap=5) across the whole sample per setup_log.
import sys, json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import sqlalchemy as sa

sys.path.insert(0, ".")
from app.mes_sim_backfill import mes_walk

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
e = sa.create_engine("postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway")

Q = """
SELECT sl.id, sl.ts, sl.grade, sl.spot, sl.lis, sl.max_plus_gex,
       sl.outcome_result, sl.outcome_pnl,
       (rto.state->>'stop_pts')::float AS stop_pts,
       (rto.state->>'fill_price')::float AS fill_px,
       (rto.state->>'stop_fill_price')::float AS stop_fill,
       (rto.state->>'close_fill_price')::float AS close_fill
FROM setup_log sl
LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
WHERE sl.setup_name = 'AG Short' AND sl.ts >= '2026-04-01'
  AND sl.grade IS NOT NULL AND sl.grade != 'LOG'
ORDER BY sl.ts
"""

BARS_Q = """
SELECT ts_start, ts_end, bar_open, bar_high, bar_low, bar_close
FROM vps_es_range_bars
WHERE range_pts = 5 AND ts_start >= :a AND ts_start <= :b
ORDER BY ts_start
"""

def lis_stop_dist(spot, lis, mpg):
    # mirror of main.py AG short stop: LIS+5, pushed to +GEX wall if beyond, cap spot+20
    if lis is None:
        return 20.0
    stop_lvl = float(lis) + 5
    if mpg is not None and float(mpg) > stop_lvl:
        stop_lvl = float(mpg)
    stop_lvl = min(stop_lvl, float(spot) + 20)
    return round(stop_lvl - float(spot), 2)

SWEEP = [8, 10, 12, 14, 16, 18, 20, "LIS"]

with e.connect() as c:
    rows = c.execute(sa.text(Q)).fetchall()
    trades = []
    skipped = []
    for r in rows:
        lid, ts, grade, spot, lis, mpg, o_res, o_pnl, stop_pts, fill, stop_fill, close_fill = r
        ts_utc = ts if ts.tzinfo else ts.replace(tzinfo=UTC)
        ts_et = ts_utc.astimezone(ET)
        eod_et = ts_et.replace(hour=15, minute=50, second=0, microsecond=0)
        if ts_et >= eod_et:
            skipped.append((lid, "after_1550"))
            continue
        max_min = int((eod_et - ts_et).total_seconds() // 60)
        bars = c.execute(sa.text(BARS_Q), {"a": ts_utc, "b": eod_et.astimezone(UTC)}).fetchall()
        bars = [(b[0], b[1], float(b[2]), float(b[3]), float(b[4]), float(b[5])) for b in bars if b[2] is not None]
        if not bars:
            skipped.append((lid, "no_bars"))
            continue
        entry = float(fill) if fill else bars[0][2]
        actual_dist = float(stop_pts) if stop_pts else lis_stop_dist(spot, lis, mpg)
        broker_pnl = None
        if fill:
            ex = stop_fill if stop_fill is not None else close_fill
            if ex is not None:
                broker_pnl = round(float(fill) - float(ex), 2)  # short
        trades.append({
            "lid": lid, "et": ts_et.strftime("%m-%d %H:%M"), "grade": grade,
            "placed": fill is not None, "entry": entry, "actual_dist": actual_dist,
            "broker_pnl": broker_pnl, "o_pnl": float(o_pnl) if o_pnl is not None else None,
            "bars": bars, "max_min": max_min,
        })

print(f"sample: {len(trades)} trades replayed, skipped={skipped}")
placed_n = sum(1 for t in trades if t["placed"])
print(f"placed={placed_n} unplaced={len(trades)-placed_n}")

# ---- Gate 2: baseline (actual LIS stop) sim vs broker truth on placed trades
print("\n=== GATE 2: baseline sim (actual stop) vs broker P&L (placed trades) ===")
diffs = []
for t in trades:
    if not t["placed"] or t["broker_pnl"] is None:
        continue
    sim = mes_walk(t["bars"], t["entry"], is_long=False, sl_pts=t["actual_dist"],
                   be_trigger=10, be_lock=0, trail_act=12, trail_gap=5,
                   max_minutes=t["max_min"])
    d = sim["pnl"] - t["broker_pnl"]
    diffs.append(abs(d))
    flag = "  <-- CHECK" if abs(d) > 5 else ""
    print(f"  lid={t['lid']} {t['et']} sim={sim['pnl']:+7.2f} broker={t['broker_pnl']:+7.2f} diff={d:+6.2f}{flag}")
if diffs:
    within5 = sum(1 for d in diffs if d <= 5)
    print(f"  mean|diff|={sum(diffs)/len(diffs):.2f}  within±5pt: {within5}/{len(diffs)} ({100*within5/len(diffs):.0f}%)")

# ---- SL sweep
print("\n=== SL SWEEP (n=%d, BE@10 act=12 gap=5, EOD 15:50) ===" % len(trades))
print(f"{'SL':>5} {'net':>8} {'WR':>5} {'wins':>4} {'loss':>4} {'exp':>4} {'avgL':>7} {'worstL':>7} {'maxDD':>7} {'fullSL':>6}")
results = {}
for sl in SWEEP:
    pnls = []
    for t in trades:
        d = t["actual_dist"] if sl == "LIS" else float(sl)
        sim = mes_walk(t["bars"], t["entry"], is_long=False, sl_pts=d,
                       be_trigger=10, be_lock=0, trail_act=12, trail_gap=5,
                       max_minutes=t["max_min"])
        pnls.append((t, d, sim))
    net = sum(s["pnl"] for _, _, s in pnls)
    wins = sum(1 for _, _, s in pnls if s["pnl"] > 0.5)
    losses = sum(1 for _, _, s in pnls if s["pnl"] < -0.5)
    exp = len(pnls) - wins - losses
    loss_vals = [s["pnl"] for _, _, s in pnls if s["pnl"] < -0.5]
    avg_l = sum(loss_vals)/len(loss_vals) if loss_vals else 0
    worst = min((s["pnl"] for _, _, s in pnls), default=0)
    full_sl = sum(1 for _, d, s in pnls if s["reason"] == "stop" and abs(s["pnl"] + d) < 0.6)
    eq, peak, mdd = 0.0, 0.0, 0.0
    for _, _, s in pnls:
        eq += s["pnl"]; peak = max(peak, eq); mdd = min(mdd, eq - peak)
    wr = 100*wins/max(1, wins+losses)
    results[sl] = pnls
    print(f"{str(sl):>5} {net:>+8.1f} {wr:>4.0f}% {wins:>4} {losses:>4} {exp:>4} {avg_l:>7.2f} {worst:>+7.1f} {mdd:>+7.1f} {full_sl:>6}")

# ---- variants: LIS-based stop with lower cap (min(LIS+5-dist, cap))
def run_subset(label, subset, sl_list):
    print(f"\n=== {label} (n={len(subset)}) ===")
    print(f"{'SL':>8} {'net':>8} {'WR':>5} {'wins':>4} {'loss':>4} {'avgL':>7} {'maxDD':>7}")
    for sl in sl_list:
        pnls = []
        for t in subset:
            if isinstance(sl, str) and sl.startswith("LIScap"):
                cap = float(sl[6:])
                d = min(t["actual_dist"], cap)
                d = max(d, 4.0)  # floor: never tighter than 4
            elif sl == "LIS":
                d = t["actual_dist"]
            else:
                d = float(sl)
            sim = mes_walk(t["bars"], t["entry"], is_long=False, sl_pts=d,
                           be_trigger=10, be_lock=0, trail_act=12, trail_gap=5,
                           max_minutes=t["max_min"])
            pnls.append(sim["pnl"])
        net = sum(pnls)
        wins = sum(1 for p in pnls if p > 0.5)
        losses = sum(1 for p in pnls if p < -0.5)
        lv = [p for p in pnls if p < -0.5]
        avg_l = sum(lv)/len(lv) if lv else 0
        eq, peak, mdd = 0.0, 0.0, 0.0
        for p in pnls:
            eq += p; peak = max(peak, eq); mdd = min(mdd, eq - peak)
        print(f"{str(sl):>8} {net:>+8.1f} {100*wins/max(1,wins+losses):>4.0f}% {wins:>4} {losses:>4} {avg_l:>7.2f} {mdd:>+7.1f}")

SLX = [10, 12, 14, "LIScap10", "LIScap12", "LIScap14", "LIS"]
run_subset("ALL Apr1+", trades, SLX)
run_subset("PLACED only", [t for t in trades if t["placed"]], SLX)
v16 = [t for t in trades if t["et"] >= "05-19"]
run_subset("V16.1 era (May19+)", v16, SLX)
may_early = [t for t in trades if "04-01" <= t["et"] < "05-19"]
run_subset("pre-V16.1 (Apr1-May18)", may_early, SLX)

# ---- per-trade detail for the decision SLs
print("\n=== PER-TRADE: LIS(actual) vs SL=14 vs SL=12 ===")
print(f"{'lid':>5} {'et':>12} {'gr':>3} {'plc':>3} {'dist':>5} | {'LIS':>7} {'SL14':>7} {'SL12':>7}")
for (tA, dA, sA), (_, _, s14), (_, _, s12) in zip(results["LIS"], results[14], results[12]):
    mark = ""
    if abs(s14["pnl"] - sA["pnl"]) > 3:
        mark = "  <-- changes"
    print(f"{tA['lid']:>5} {tA['et']:>12} {tA['grade']:>3} {'Y' if tA['placed'] else 'n':>3} {dA:>5.1f} | {sA['pnl']:>+7.2f} {s14['pnl']:>+7.2f} {s12['pnl']:>+7.2f}{mark}")
