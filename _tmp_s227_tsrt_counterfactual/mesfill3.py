"""S231 FINAL MES fill: basis-clean entry + real_trader's ACTUAL trail config.

Two corrections over mesfill.py / mesfill2.py:
  1. entry: no SPX-spot fallback (see mesfill2 docstring) — 7 rows had a ~29pt phantom basis.
  2. trail params: setup_log.trail_activation/gap are the DETECTOR's values and are stale for
     DD (20/5, real = 10/10), GEX Long (15/5, real = 10/5) and AG Short (12/5, real = 10/5).
     real_trader.py uses GLOBALS for every setup — BE_TRIGGER=10 / ACT=10 / GAP=5 / BE_BUF=0.25 —
     with one override: DD Exhaustion = continuous, no BE, act 10 / gap 10.
     Stop distance = the DISPATCHED stop (abs(spot - outcome_stop_level)), which is what
     main.py hands real_trader, falling back to setup_log.trail_sl.
  3. Vanna Pivot Bounce trades a FIXED target bracket, not trail-only: if the MES walk's
     max_fav reaches the dispatched target distance, the limit would have filled → cap at target.
"""
import os, sys, json
sys.path.insert(0, "G:/My Drive/Python/MyProject/GitHub/0dtealpha")
from sqlalchemy import create_engine, text
from datetime import timedelta
from app import mes_sim_backfill as M

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from engine import stop_for as _stop_for   # curated per-setup stop map

ENG = create_engine(os.environ["DATABASE_URL"])
_orig = M._first_es_open_after


def _entry_es(engine, t_utc, max_wait_minutes=10):
    v = _orig(engine, t_utc, max_wait_minutes)
    if v is not None:
        return v
    with engine.begin() as conn2:
        row = conn2.execute(text("""
            SELECT bar_close FROM vps_es_range_bars
            WHERE range_pts=5 AND ts_start <= :t AND ts_start >= :lo
            ORDER BY ts_start DESC LIMIT 1"""),
            {"t": t_utc, "lo": t_utc - timedelta(minutes=60)}).fetchone()
    return float(row[0]) if row and row[0] is not None else None


M._first_es_open_after = _entry_es

# real_trader globals (real_trader.py:176-188)
RT = {"be_trigger": 10.0, "be_lock": 0.25, "trail_act": 10.0, "trail_gap": 5.0}
RT_DD = {"be_trigger": None, "be_lock": 0, "trail_act": 10.0, "trail_gap": 10.0}
SETUPS = ("Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption",
          "DD Exhaustion", "GEX Long", "VIX Divergence")
for s in SETUPS:
    M._DEFAULT_PARAMS[s] = dict(RT_DD if s == "DD Exhaustion" else RT,
                                sl=12 if s == "DD Exhaustion" else 14)
M.V14_WHITELIST = set(SETUPS)


def run(where, out_file):
    with ENG.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        rows = c.execute(text(f"""
          SELECT id, ts, setup_name, direction, spot, trail_sl, abs_es_price,
                 outcome_elapsed_min, outcome_stop_level, outcome_target_level
          FROM setup_log WHERE {where} ORDER BY ts""")).fetchall()
    out, drop, capped = {}, [], 0
    for (lid, ts, setup, direction, spot, tsl, absp, em, stop_lvl, tgt_lvl) in rows:
        # Stop distance via engine.stop_for() — the curated per-setup map.
        # NOT abs(spot - outcome_stop_level): for ES Absorption the stop level is stored in
        # ES price space, so that subtraction returns the ES-SPX basis (~70pt), not a stop.
        il0 = (direction or "").lower() in ("long", "bullish")
        sl = _stop_for(setup, il0, float(tsl) if tsl is not None else None,
                       float(spot) if spot is not None else 0.0,
                       float(stop_lvl) if stop_lvl is not None else None)
        sim = M.compute_mes_sim_outcome(
            ENG, setup_log_id=lid, setup_name=setup, direction=direction, signal_ts=ts,
            spx_spot=None,
            trail_sl=sl, trail_activation=None, trail_gap=None,   # None -> forced RT defaults
            signal_es_price=float(absp) if absp is not None else None,
            outcome_elapsed_min=em)
        if not sim or sim.get("mes_sim_outcome_pnl") is None:
            drop.append(lid); continue
        pnl = float(sim["mes_sim_outcome_pnl"])
        if setup == "Vanna Pivot Bounce" and tgt_lvl is not None and spot is not None:
            tgt = abs(float(tgt_lvl) - float(spot))
            if float(sim["mes_sim_max_fav"]) >= tgt and pnl < tgt:
                pnl = tgt; capped += 1
        out[lid] = pnl
    json.dump({str(k): v for k, v in out.items()}, open(out_file, "w"))
    print(f"{out_file}: rows {len(rows)} computed {len(out)} dropped {len(drop)} "
          f"vpb-target-capped {capped}  sum {sum(out.values()):.1f} pts")
    return out


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "cf"
    if which == "cf":
        run("ts >= '2026-07-01' AND ts < '2026-08-07' AND real_trade_skip_reason='master_kill'",
            "mesfill_rt.json")
    else:  # validation set: every trade TSRT actually placed
        run("id IN (SELECT setup_log_id FROM real_trade_orders)", "mesfill_rt_val.json")
