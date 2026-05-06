"""GEX Long v3 — server-side filter + exit simulator for portal overlay.

Lifted from `_tmp_gex_long_v3_validate.py` (verified 2026-05-06: 14 trades / 77% WR /
+$552 / PF 3.63 over Feb 23 - May 4). Used by `/api/setup/gex_long_v3_overlay`.

v3 specification:
  Visual classifier (per-strike GEX + charm features around spot):
    CORE_R3: max +GEX strike above spot has positive value
    CORE_R2: max -GEX strike below spot has negative value
    R5_align: bullish charm magnet (most-negative charm above spot) within 10pts of max +GEX above
    R_charm_bullish: total charm < 0
    R_gex_regime_pos: total gex >= 0
    R_VETO: >=80% of charm-above-spot strikes positive AND not R5_align

    Verdict:
      !CORE_R3 or R_VETO -> BAD
      CORE_R2 + R5_align + (R_charm_bullish or R_gex_regime_pos) -> A++
      CORE_R2 + (R5_align or R_charm_bullish) -> A
      CORE_R2 or R5_align -> B
      else -> C

  v3 filter (Config 6 + hour gate):
    verdict in (A++, A, B) AND alignment >= 0 AND hour_et < 15

  Exit re-simulation (chain_snapshots 30s spot path, entry -> 16:00 ET):
    SL = 14 pts (from entry)
    Target = max(GEX magnet strike above spot, entry + 10)
    Trail: activates at max_fav >= 15, gap = 5
"""
from __future__ import annotations

import time
import threading
from typing import Optional, Any

# Cache: dict[lid] = {"pass": bool, "verdict": str, "result": str, "pnl": float, "max_fav": float, "reason": str}
_v3_cache: dict[int, dict[str, Any]] = {}
_v3_cache_built_at: Optional[float] = None
_v3_cache_lock = threading.Lock()
_CACHE_TTL_SEC = 3600  # rebuild hourly during market hours

SL_PTS = 14.0
TARGET_FLOOR = 10.0
TRAIL_ACT = 15.0
TRAIL_GAP = 5.0


def _features(cur, t_utc, spot: float) -> Optional[dict]:
    spot_f = float(spot) if spot else 0
    if spot_f == 0:
        return None
    cur.execute("""SELECT ts_utc FROM volland_exposure_points
                   WHERE ts_utc BETWEEN %s - interval '5 min' AND %s
                     AND greek='gamma' AND expiration_option='TODAY'
                   ORDER BY ts_utc DESC LIMIT 1""", (t_utc, t_utc))
    g_snap = cur.fetchone()
    cur.execute("""SELECT ts_utc FROM volland_exposure_points
                   WHERE ts_utc BETWEEN %s - interval '5 min' AND %s
                     AND greek='charm' ORDER BY ts_utc DESC LIMIT 1""", (t_utc, t_utc))
    c_snap = cur.fetchone()
    if not g_snap or not c_snap:
        return None

    cur.execute("""SELECT strike, value FROM volland_exposure_points
                   WHERE ts_utc=%s AND greek='gamma' AND expiration_option='TODAY'
                     AND strike BETWEEN %s AND %s ORDER BY strike""",
                (g_snap[0], spot_f - 50, spot_f + 50))
    gex = [(float(s), float(v)) for s, v in cur.fetchall()]
    cur.execute("""SELECT strike, value FROM volland_exposure_points
                   WHERE ts_utc=%s AND greek='charm'
                     AND strike BETWEEN %s AND %s ORDER BY strike""",
                (c_snap[0], spot_f - 50, spot_f + 50))
    charm = [(float(s), float(v)) for s, v in cur.fetchall()]
    if not gex or not charm:
        return None

    gex_below = [(s, v) for s, v in gex if s < spot_f]
    gex_above = [(s, v) for s, v in gex if s > spot_f]
    charm_above = [(s, v) for s, v in charm if s > spot_f]
    sg_below = max(gex_below, key=lambda x: abs(x[1])) if gex_below else (None, 0)
    sg_above = max(gex_above, key=lambda x: abs(x[1])) if gex_above else (None, 0)
    neg_charm_above = [(s, v) for s, v in charm_above if v < 0]
    bullish_charm_magnet = min(neg_charm_above, key=lambda x: x[1])[0] if neg_charm_above else None

    total_gex = sum(v for _, v in gex)
    total_charm = sum(v for _, v in charm)
    above_charm_pos_pct = (sum(1 for _, v in charm_above if v > 0) /
                           max(len(charm_above), 1) * 100)

    R5_align = (bullish_charm_magnet is not None and sg_above[0] is not None
                and sg_above[1] > 0 and abs(bullish_charm_magnet - sg_above[0]) <= 10)
    return {
        'gex_magnet_strike': sg_above[0],
        'CORE_R3': sg_above[1] > 0,
        'CORE_R2': sg_below[1] < 0,
        'R5_align': R5_align,
        'R_charm_bullish': total_charm < 0,
        'R_gex_regime_pos': total_gex >= 0,
        'R_VETO': (above_charm_pos_pct >= 80) and (not R5_align),
    }


def _classify(f: Optional[dict]) -> str:
    if f is None:
        return 'NO_DATA'
    if not f['CORE_R3']:
        return 'BAD'
    if f['R_VETO']:
        return 'BAD'
    if f['CORE_R2'] and f['R5_align'] and (f['R_charm_bullish'] or f['R_gex_regime_pos']):
        return 'A++'
    if f['CORE_R2'] and (f['R5_align'] or f['R_charm_bullish']):
        return 'A'
    if f['CORE_R2'] or f['R5_align']:
        return 'B'
    return 'C'


def _simulate_exit(cur, t_utc, entry: float, target: float):
    """Walk chain_snapshots 30s path from entry through 16:00 ET. Long-only."""
    cur.execute("""SELECT ts AT TIME ZONE 'America/New_York' as t, spot
                   FROM chain_snapshots
                   WHERE ts >= %s
                     AND (ts AT TIME ZONE 'America/New_York')::date
                         = (%s AT TIME ZONE 'America/New_York')::date
                     AND (ts AT TIME ZONE 'America/New_York')::time < '16:00'
                     AND spot IS NOT NULL
                   ORDER BY ts""", (t_utc, t_utc))
    path = [(t, float(s)) for t, s in cur.fetchall()]
    if not path:
        return 'NO_PATH', 0.0, 0.0, 'no_path'

    sl_price = entry - SL_PTS
    max_fav = 0.0
    trail_active = False
    trail_stop = sl_price

    for _t, spot in path:
        fav = spot - entry
        if fav > max_fav:
            max_fav = fav

        active_stop = trail_stop if trail_active else sl_price
        if spot <= active_stop:
            pnl = active_stop - entry
            return ('WIN' if pnl > 0 else 'LOSS'), pnl, max_fav, ('trail' if trail_active else 'sl')
        if spot >= target:
            return 'WIN', target - entry, max_fav, 'target'
        if not trail_active and max_fav >= TRAIL_ACT:
            trail_active = True
            trail_stop = entry + (max_fav - TRAIL_GAP)
        elif trail_active:
            new_trail = entry + (max_fav - TRAIL_GAP)
            if new_trail > trail_stop:
                trail_stop = new_trail

    last_spot = path[-1][1]
    return 'EXPIRED', last_spot - entry, max_fav, 'eod'


def _build_cache(engine) -> dict[int, dict[str, Any]]:
    """Compute v3 verdict + simulated exit for all GEX Long graded signals."""
    out: dict[int, dict[str, Any]] = {}
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("""SELECT id, ts, ts AT TIME ZONE 'America/New_York' as t_et,
                              greek_alignment, spot
                       FROM setup_log
                       WHERE setup_name = 'GEX Long'
                         AND grade != 'LOG' AND grade IS NOT NULL
                       ORDER BY ts""")
        rows = cur.fetchall()
        for lid, t_utc, t_et, al, spot in rows:
            if not spot:
                out[lid] = {"pass": False, "verdict": "NO_DATA", "reason": "no_spot",
                            "result": None, "pnl": None, "max_fav": None}
                continue
            try:
                f = _features(cur, t_utc, spot)
            except Exception:
                f = None
            verdict = _classify(f)
            align = al if al is not None else 0
            hour = t_et.hour if t_et else 99
            v3_pass = (verdict in ('A++', 'A', 'B')) and (align >= 0) and (hour < 15)

            if not v3_pass or f is None:
                out[lid] = {"pass": v3_pass, "verdict": verdict, "reason": "filter_block",
                            "result": None, "pnl": None, "max_fav": None}
                continue

            entry = float(spot)
            magnet = f['gex_magnet_strike']
            target = max(magnet or 0, entry + TARGET_FLOOR)
            try:
                result, pnl, max_fav, reason = _simulate_exit(cur, t_utc, entry, target)
            except Exception as exc:
                out[lid] = {"pass": v3_pass, "verdict": verdict, "reason": f"sim_err:{exc}",
                            "result": None, "pnl": None, "max_fav": None}
                continue

            out[lid] = {
                "pass": True,
                "verdict": verdict,
                "result": result,
                "pnl": round(pnl, 2),
                "max_fav": round(max_fav, 2),
                "reason": reason,
            }
        cur.close()
    finally:
        raw.close()
    return out


def get_overlay(engine, force_rebuild: bool = False) -> dict[int, dict[str, Any]]:
    """Return cached v3 overlay; rebuild on TTL miss or first call."""
    global _v3_cache, _v3_cache_built_at
    with _v3_cache_lock:
        now = time.time()
        if (not force_rebuild and _v3_cache_built_at is not None
                and (now - _v3_cache_built_at) < _CACHE_TTL_SEC):
            return _v3_cache
        _v3_cache = _build_cache(engine)
        _v3_cache_built_at = now
        return _v3_cache


def overlay_meta() -> dict[str, Any]:
    return {
        "built_at": _v3_cache_built_at,
        "trade_count": len(_v3_cache),
        "ttl_sec": _CACHE_TTL_SEC,
        "params": {"sl": SL_PTS, "target_floor": TARGET_FLOOR,
                   "trail_act": TRAIL_ACT, "trail_gap": TRAIL_GAP},
    }
