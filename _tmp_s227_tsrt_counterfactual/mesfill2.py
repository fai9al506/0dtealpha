"""S231 basis-clean MES fill.

BUG FOUND: app/mes_sim_backfill._first_es_open_after only looks FORWARD 10 min for an
ES bar. On slow-bar stretches (5pt range bars can take >10 min) it returns None and
compute_mes_sim_outcome falls back to **SPX spot as the ES entry price** while walking
ES bars — a ~29pt basis error (lid 5654: entry 7724.58 = SPX spot, true ES ~7753 →
phantom +36.17 instead of ~+7).

This module recomputes every candidate with a basis-clean entry:
  1. abs_es_price if stamped
  2. first ES bar OPEN with ts_start in [signal, signal+10min]
  3. else last ES bar CLOSE with ts_start <= signal (within 60 min)  <-- the fix
  4. else drop the trade (no honest entry available)
SPX-spot fallback is disabled (spx_spot=None).
"""
import os, sys, json
sys.path.insert(0, "G:/My Drive/Python/MyProject/GitHub/0dtealpha")
from sqlalchemy import create_engine, text
from datetime import timedelta
from app import mes_sim_backfill as M

M.V14_WHITELIST = set(M.V14_WHITELIST) | {"GEX Long"}
M._DEFAULT_PARAMS["GEX Long"] = {"sl": 14, "be_trigger": None, "be_lock": 0,
                                 "trail_act": 10, "trail_gap": 5}

ENG = create_engine(os.environ["DATABASE_URL"])
_orig = M._first_es_open_after


def _entry_es(engine, t_utc, max_wait_minutes=10):
    v = _orig(engine, t_utc, max_wait_minutes)
    if v is not None:
        return v
    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT bar_close FROM vps_es_range_bars
            WHERE range_pts = 5 AND ts_start <= :t AND ts_start >= :lo
            ORDER BY ts_start DESC LIMIT 1"""),
            {"t": t_utc, "lo": t_utc - timedelta(minutes=60)}).fetchone()
    return float(row[0]) if row and row[0] is not None else None


M._first_es_open_after = _entry_es

WHERE = ("ts >= '2026-07-01' AND ts < '2026-08-07' "
         "AND real_trade_skip_reason = 'master_kill'")

out, drop, fb = {}, [], 0
with ENG.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    rows = c.execute(text(f"""
      SELECT id, ts, setup_name, direction, spot, trail_sl, trail_activation, trail_gap,
             abs_es_price, outcome_elapsed_min
      FROM setup_log WHERE {WHERE} ORDER BY ts""")).fetchall()

for r in rows:
    sim = M.compute_mes_sim_outcome(
        ENG, setup_log_id=r[0], setup_name=r[2], direction=r[3], signal_ts=r[1],
        spx_spot=None,                                   # kill the SPX fallback
        trail_sl=float(r[5]) if r[5] is not None else None,
        trail_activation=float(r[6]) if r[6] is not None else None,
        trail_gap=float(r[7]) if r[7] is not None else None,
        signal_es_price=float(r[8]) if r[8] is not None else None,
        outcome_elapsed_min=r[9])
    if sim and sim.get("mes_sim_outcome_pnl") is not None:
        out[r[0]] = float(sim["mes_sim_outcome_pnl"])
        if abs(float(sim["mes_sim_entry_es"]) - float(r[4])) < 0.02:
            fb += 1
    else:
        drop.append(r[0])

json.dump({str(k): v for k, v in out.items()}, open("mesfill_clean.json", "w"))
print(f"rows {len(rows)}  computed {len(out)}  dropped {len(drop)} {drop}")
print(f"entry within 0.02 of SPX spot (suspicious): {fb}")
