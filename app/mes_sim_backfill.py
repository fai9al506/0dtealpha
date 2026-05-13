"""S55: MES-driven trail simulation (portal realism, not new alpha).

Productionized from `_tmp_s55_mes_trail_prototype.py` (validated 80 V14 trades
Apr 15-May 12, mean |real - mes_sim| = 2.35pt vs |real - chain_sim| = 6.70pt).

This module is FEED-AGNOSTIC and SAFE-BY-DEFAULT:
  - All DB reads use defensive try/except so it never crashes the live cycle
    if the new mes_sim_* columns or vps_es_range_bars table are missing/empty.
  - Writes are best-effort: a failure to compute mes_sim outcome NEVER raises
    out — it just leaves the columns NULL.
  - It does not modify the existing chain-walk outcome path.

API:
  mes_walk(...)                      — pure simulator on a list of ES bars.
  compute_mes_sim_outcome(...)       — high-level wrapper that fetches bars
                                       from vps_es_range_bars and runs mes_walk.
  backfill_for_date(engine, date)    — backfills one trading day's worth
                                       of setup_log rows (V14 whitelist only).
  backfill_range(engine, start, end) — convenience loop over a date range.

Author: S55 ship (2026-05-13). Code shipped locally — pre-DB-migration.
"""
from __future__ import annotations

import bisect
from datetime import datetime, timedelta, date as _date_t
from typing import Optional, Tuple, Dict, Any, List

try:
    from sqlalchemy import text
except Exception:  # pragma: no cover - SA always present in production
    text = None

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    ET = None


# V14 real-trader whitelist (same as `app/real_trader.py`).
# MES-sim is only meaningful for setups whose real execution goes through
# the MES path. Other setups (DD/PR/GEX Velocity/etc.) are SIM/eval-only.
V14_WHITELIST = {
    "Skew Charm",
    "AG Short",
    "Vanna Pivot Bounce",
    "VIX Divergence",
    "ES Absorption",  # PURE-filtered live as of Apr 29
}


# Default trail params, mirrored from `_check_setup_outcomes()` in main.py.
# Per-row trail_sl/trail_activation/trail_gap on setup_log override these.
# Notes:
#   - SC/AG/VPB use trail-only (no BE), per V14 live config.
#   - VIX Divergence longs use hybrid BE@+6 + continuous trail (a=10 g=8 SL=8).
#   - VIX Divergence shorts use continuous (a=15 g=5 SL=12) — kept for parity.
#   - ES Absorption uses C6 (BE@5, a=8, g=3, SL=8) — shipped May 6.
_DEFAULT_PARAMS = {
    "Skew Charm":         {"sl": 14, "be_trigger": None, "be_lock": 0, "trail_act": 10, "trail_gap": 5},
    "AG Short":           {"sl": 12, "be_trigger": None, "be_lock": 0, "trail_act": 12, "trail_gap": 5},
    "Vanna Pivot Bounce": {"sl":  8, "be_trigger": None, "be_lock": 0, "trail_act": 10, "trail_gap": 5},
    "VIX Divergence":     {"sl":  8, "be_trigger": 6,    "be_lock": 1, "trail_act": 8,  "trail_gap": 8},
    "ES Absorption":      {"sl":  8, "be_trigger": 5,    "be_lock": 0, "trail_act": 8,  "trail_gap": 3},
}


# ---------------------------------------------------------------------------
# Pure simulator
# ---------------------------------------------------------------------------
def mes_walk(
    bars: List[Tuple[Any, Any, float, float, float, float]],
    entry_es: float,
    is_long: bool,
    sl_pts: float,
    be_trigger: Optional[float],
    be_lock: float,
    trail_act: Optional[float],
    trail_gap: float,
    max_minutes: int,
) -> Dict[str, Any]:
    """Walk a sequence of ES range bars (ts_start, ts_end, o, h, l, c) and
    apply BE + trail + stop rules. Returns a dict with pnl/mfe/mae/reason.

    Within-bar ordering is conservative (adverse-first): if both the stop
    and a favorable extreme are touched in the same bar, we assume the stop
    fills FIRST. This matches how a real stop-market order would behave on
    a price spike that whipsaws through both levels.
    """
    if not bars:
        return {
            "pnl": 0.0, "mfe": 0.0, "mae": 0.0,
            "reason": "no_bars", "exit_ts": None, "exit_price": entry_es,
        }

    sl_price = entry_es - sl_pts if is_long else entry_es + sl_pts
    stop = sl_price
    max_fav = 0.0
    max_adv = 0.0
    be_done = False
    start_ts = bars[0][0]
    cutoff = start_ts + timedelta(minutes=max_minutes)

    for ts_s, ts_e, b_o, b_h, b_l, b_c in bars:
        if ts_s > cutoff:
            break

        # Per-bar adverse/favorable extremes
        if is_long:
            fav_in_bar = b_h - entry_es
            adv_in_bar = entry_es - b_l
        else:
            fav_in_bar = entry_es - b_l
            adv_in_bar = b_h - entry_es
        if fav_in_bar > max_fav:
            max_fav = fav_in_bar
        if adv_in_bar > max_adv:
            max_adv = adv_in_bar

        # Check pre-bar stop FIRST (conservative — adverse-first ordering).
        pre_bar_stop = stop
        stopped = (
            (is_long and b_l <= pre_bar_stop)
            or ((not is_long) and b_h >= pre_bar_stop)
        )
        if stopped:
            pnl = (pre_bar_stop - entry_es) if is_long else (entry_es - pre_bar_stop)
            return {
                "pnl": pnl, "mfe": max_fav, "mae": max_adv,
                "reason": "stop", "exit_ts": ts_e, "exit_price": pre_bar_stop,
            }

        # Ratchet BE
        if (not be_done) and be_trigger is not None and max_fav >= be_trigger:
            be_done = True
            new_stop = entry_es + be_lock if is_long else entry_es - be_lock
            if is_long and new_stop > stop:
                stop = new_stop
            elif (not is_long) and new_stop < stop:
                stop = new_stop

        # Ratchet trail
        if trail_act is not None and max_fav >= trail_act:
            trail_stop = (
                (entry_es + (max_fav - trail_gap))
                if is_long
                else (entry_es - (max_fav - trail_gap))
            )
            if is_long and trail_stop > stop:
                stop = trail_stop
            elif (not is_long) and trail_stop < stop:
                stop = trail_stop

    # End of window — exit at last bar's close
    ts_s, ts_e, b_o, b_h, b_l, b_c = bars[-1]
    pnl = (b_c - entry_es) if is_long else (entry_es - b_c)
    return {
        "pnl": pnl, "mfe": max_fav, "mae": max_adv,
        "reason": "eod", "exit_ts": ts_e, "exit_price": b_c,
    }


# ---------------------------------------------------------------------------
# Param resolution
# ---------------------------------------------------------------------------
def _resolve_params(
    setup_name: str,
    is_long: bool,
    trail_sl: Optional[float],
    trail_act_db: Optional[float],
    trail_gap_db: Optional[float],
) -> Dict[str, float]:
    """Combine DB-stored trail params with the setup default fallback."""
    defaults = _DEFAULT_PARAMS.get(setup_name, {
        "sl": 14, "be_trigger": None, "be_lock": 0,
        "trail_act": 10, "trail_gap": 5,
    })
    sl_pts = float(trail_sl) if trail_sl is not None else defaults["sl"]
    t_act = float(trail_act_db) if trail_act_db is not None else defaults["trail_act"]
    t_gap = float(trail_gap_db) if trail_gap_db is not None else defaults["trail_gap"]
    be_trig = defaults["be_trigger"]
    be_lock = defaults["be_lock"]

    # VIX Divergence direction-aware override (matches main.py:4606).
    if setup_name == "VIX Divergence":
        if is_long:
            t_act, t_gap, be_trig = 10, 8, 6
            sl_pts = sl_pts or 8
        else:
            t_act, t_gap, be_trig = 15, 5, None
            sl_pts = sl_pts or 12

    return {
        "sl_pts": sl_pts,
        "be_trigger": be_trig,
        "be_lock": be_lock,
        "trail_act": t_act,
        "trail_gap": t_gap,
    }


# ---------------------------------------------------------------------------
# Bar fetcher (defensive)
# ---------------------------------------------------------------------------
def _fetch_bars_for_window(
    engine, signal_ts: datetime, max_minutes: int
) -> List[Tuple[Any, Any, float, float, float, float]]:
    """Fetch vps_es_range_bars rows (range_pts=5) within
    [signal_ts, signal_ts + max_minutes + 60min]. Returns empty list on any
    failure (missing table, no rows, DB error)."""
    if engine is None or text is None:
        return []
    end_ts = signal_ts + timedelta(minutes=max_minutes + 60)
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT ts_start, ts_end, bar_open, bar_high, bar_low, bar_close
                FROM vps_es_range_bars
                WHERE range_pts = 5
                  AND ts_start >= :start_ts
                  AND ts_start <= :end_ts
                ORDER BY ts_start ASC
            """), {"start_ts": signal_ts, "end_ts": end_ts}).fetchall()
    except Exception:
        return []
    return [
        (r[0], r[1], float(r[2]), float(r[3]), float(r[4]), float(r[5]))
        for r in rows if r[2] is not None
    ]


def _first_es_open_after(
    engine, t_utc: datetime, max_wait_minutes: int = 10
) -> Optional[float]:
    """Get the first bar's open price at/after t_utc. Used as a fallback when
    signal_es_price and fill_price are both missing."""
    if engine is None or text is None:
        return None
    end_ts = t_utc + timedelta(minutes=max_wait_minutes)
    try:
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT bar_open FROM vps_es_range_bars
                WHERE range_pts = 5
                  AND ts_start >= :start_ts
                  AND ts_start <= :end_ts
                ORDER BY ts_start ASC
                LIMIT 1
            """), {"start_ts": t_utc, "end_ts": end_ts}).fetchone()
    except Exception:
        return None
    return float(row[0]) if row and row[0] is not None else None


# ---------------------------------------------------------------------------
# High-level wrapper for a single setup_log row
# ---------------------------------------------------------------------------
def compute_mes_sim_outcome(
    engine,
    setup_log_id: int,
    setup_name: str,
    direction: str,
    signal_ts: datetime,
    spx_spot: Optional[float],
    trail_sl: Optional[float] = None,
    trail_activation: Optional[float] = None,
    trail_gap: Optional[float] = None,
    signal_es_price: Optional[float] = None,
    fill_price: Optional[float] = None,
    outcome_elapsed_min: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Compute MES-sim outcome for one setup_log row.

    Returns dict with keys:
      mes_sim_outcome_pnl    (float, points)
      mes_sim_outcome_result (str: WIN/LOSS/EXPIRED)
      mes_sim_max_fav        (float, points)
      mes_sim_max_adv        (float, points, informational)
      mes_sim_exit_reason    (str, informational)
      mes_sim_entry_es       (float, informational)
    Returns None if setup is not in whitelist or required inputs are missing.

    Never raises — caller is the live outcome path, and any failure should
    leave mes_sim_* columns NULL rather than crash the cycle.
    """
    try:
        if setup_name not in V14_WHITELIST:
            return None
        if signal_ts is None:
            return None
        if signal_ts.tzinfo is None:
            # Assume UTC if naive (shouldn't happen — setup_log.ts is timestamptz)
            try:
                from zoneinfo import ZoneInfo as _Z
                signal_ts = signal_ts.replace(tzinfo=_Z("UTC"))
            except Exception:
                return None

        is_long = (direction or "").lower() in ("long", "bullish")

        # If trail params not passed in, pull from setup_log row (live cycle
        # doesn't keep them in the in-memory trade dict; backfill path passes
        # them in). Best-effort — fallback to defaults if read fails.
        if trail_sl is None and trail_activation is None and trail_gap is None and engine is not None and text is not None:
            try:
                with engine.begin() as conn:
                    _r = conn.execute(text(
                        "SELECT trail_sl, trail_activation, trail_gap FROM setup_log WHERE id = :id"
                    ), {"id": setup_log_id}).fetchone()
                if _r:
                    trail_sl = _r[0]
                    trail_activation = _r[1]
                    trail_gap = _r[2]
            except Exception:
                pass

        params = _resolve_params(
            setup_name, is_long, trail_sl, trail_activation, trail_gap
        )

        # Resolve ES entry price.
        entry_es = None
        if signal_es_price and signal_es_price > 0:
            entry_es = float(signal_es_price)
        elif fill_price and fill_price > 0:
            entry_es = float(fill_price)
        else:
            entry_es = _first_es_open_after(engine, signal_ts, max_wait_minutes=10)
            if entry_es is None and spx_spot is not None:
                # Last resort: use SPX spot. This will produce a basis-biased
                # sim (SPX path on ES distances) but is better than nothing.
                # Bias is bounded by typical 25-35pt ES-SPX basis.
                entry_es = float(spx_spot)
        if entry_es is None:
            return None

        em = int(outcome_elapsed_min) if outcome_elapsed_min else 90
        max_min = max(em + 30, 60)
        bars = _fetch_bars_for_window(engine, signal_ts, max_min)
        if not bars:
            return None

        result = mes_walk(
            bars,
            entry_es=entry_es,
            is_long=is_long,
            sl_pts=params["sl_pts"],
            be_trigger=params["be_trigger"],
            be_lock=params["be_lock"],
            trail_act=params["trail_act"],
            trail_gap=params["trail_gap"],
            max_minutes=max_min,
        )

        pnl = round(float(result["pnl"]), 2)
        if pnl > 0:
            outcome_result = "WIN"
        elif pnl < 0:
            outcome_result = "LOSS"
        else:
            outcome_result = "EXPIRED"
        # If exit reason was end-of-window, classify as EXPIRED regardless of sign
        # to match the chain-walk semantics in _check_setup_outcomes().
        if result["reason"] == "eod":
            outcome_result = "EXPIRED"

        return {
            "mes_sim_outcome_pnl": pnl,
            "mes_sim_outcome_result": outcome_result,
            "mes_sim_max_fav": round(float(result["mfe"]), 2),
            "mes_sim_max_adv": round(float(result["mae"]), 2),
            "mes_sim_exit_reason": str(result["reason"]),
            "mes_sim_entry_es": round(float(entry_es), 2),
        }
    except Exception:
        # Never let this break the caller.
        return None


def write_mes_sim_columns(engine, setup_log_id: int, sim: Dict[str, Any]) -> bool:
    """Best-effort UPDATE of the three persisted mes_sim columns.

    Returns True on success, False on any failure. Failure is silent at the
    log level (caller may print) — by design, this is a non-critical path
    until the columns exist on prod (post-migration). Before migration this
    will fail with `column does not exist` and return False.
    """
    if engine is None or text is None or sim is None:
        return False
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE setup_log SET
                    mes_sim_outcome_pnl = :pnl,
                    mes_sim_outcome_result = :res,
                    mes_sim_max_fav = :mf
                WHERE id = :id
            """), {
                "pnl": sim.get("mes_sim_outcome_pnl"),
                "res": sim.get("mes_sim_outcome_result"),
                "mf": sim.get("mes_sim_max_fav"),
                "id": setup_log_id,
            })
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Batch backfill
# ---------------------------------------------------------------------------
def _v14_whitelist_array_sql() -> List[str]:
    return list(V14_WHITELIST)


def backfill_for_date(engine, trade_date: _date_t, dry_run: bool = False) -> Dict[str, Any]:
    """Backfill mes_sim_* columns for all V14-whitelist setup_log rows on
    a given trading day. Joins real_trade_orders for signal_es_price/fill_price
    when available. Returns a stats dict.

    dry_run=True → computes but does not write to DB (useful for verification).
    """
    if engine is None or text is None:
        return {"date": str(trade_date), "rows": 0, "computed": 0, "skipped": 0, "errors": []}

    rows = []
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT sl.id, sl.ts, sl.setup_name, sl.direction, sl.spot,
                       sl.trail_sl, sl.trail_activation, sl.trail_gap,
                       sl.outcome_elapsed_min,
                       (rto.state->>'signal_es_price')::float AS sig_es,
                       (rto.state->>'fill_price')::float AS fill_px
                FROM setup_log sl
                LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
                WHERE date(sl.ts AT TIME ZONE 'America/New_York') = :d
                  AND sl.setup_name = ANY(:wl)
                  AND sl.grade IS NOT NULL AND sl.grade != 'LOG'
                ORDER BY sl.ts ASC
            """), {"d": trade_date, "wl": _v14_whitelist_array_sql()}).fetchall()
    except Exception as e:
        return {"date": str(trade_date), "rows": 0, "computed": 0, "skipped": 0, "errors": [str(e)]}

    computed = 0
    skipped = 0
    written = 0
    errors: List[str] = []

    for r in rows:
        try:
            sim = compute_mes_sim_outcome(
                engine,
                setup_log_id=r[0],
                setup_name=r[2],
                direction=r[3],
                signal_ts=r[1],
                spx_spot=r[4],
                trail_sl=r[5],
                trail_activation=r[6],
                trail_gap=r[7],
                signal_es_price=r[9],
                fill_price=r[10],
                outcome_elapsed_min=r[8],
            )
            if sim is None:
                skipped += 1
                continue
            computed += 1
            if not dry_run:
                if write_mes_sim_columns(engine, r[0], sim):
                    written += 1
        except Exception as e:
            errors.append(f"lid={r[0]}: {e}")

    return {
        "date": str(trade_date),
        "rows": len(rows),
        "computed": computed,
        "written": written,
        "skipped": skipped,
        "dry_run": dry_run,
        "errors": errors[:10],
    }


def backfill_range(
    engine, start_date: _date_t, end_date: _date_t, dry_run: bool = False
) -> Dict[str, Any]:
    """Convenience: backfill all dates in [start_date, end_date] inclusive."""
    out = {"dates": [], "total_rows": 0, "total_computed": 0, "total_written": 0, "total_skipped": 0}
    d = start_date
    while d <= end_date:
        stats = backfill_for_date(engine, d, dry_run=dry_run)
        out["dates"].append(stats)
        out["total_rows"] += stats.get("rows", 0)
        out["total_computed"] += stats.get("computed", 0)
        out["total_written"] += stats.get("written", 0)
        out["total_skipped"] += stats.get("skipped", 0)
        d = d + timedelta(days=1)
    return out
