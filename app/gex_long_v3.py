"""GEX Long v3.1 — server-side filter + exit simulator for portal overlay.

v3.0 verified 2026-05-06: 14 trades / 77% WR / +$552 / PF 3.63 over Feb 23 - May 4.
v3.1 patch (2026-05-18): tightens R_VETO carveout. v3.0 let one weak negative charm
strike at the GEX magnet override an 80%+ positive-charm wall above spot, admitting
fundamentally bearish setups (e.g. lid 2883: −100B regime, 80% pos charm wall,
neg_charm magnet only 29% of strongest +charm above — lost −14 pts). v3.1 only
allows the R5_align carveout if the negative charm magnet is at least 50% as strong
as the dominant +charm wall it has to overcome. Backtest: 17→16 trades, WR 75→80%,
PnL +155→+169 pts (+$70 MES) on 94-trade GEX Long sample Feb-May 2026.

v3.1 specification:
  Visual classifier (per-strike GEX + charm features around spot):
    CORE_R3: max +GEX strike above spot has positive value
    CORE_R2: max -GEX strike below spot has negative value
    R5_align: bullish charm magnet (most-negative charm above spot) within 10pts of max +GEX above
    R5_align_strong: R5_align AND |neg_charm_magnet| >= 50% of strongest +charm above (v3.1)
    R_charm_bullish: total charm < 0
    R_gex_regime_pos: total gex >= 0
    R_VETO (v3.1): >=80% of charm-above-spot strikes positive AND not R5_align_strong

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

import json
import time
import threading
from typing import Optional, Any

# Cache: dict[lid] = {"pass": bool, "verdict": str, "result": str, "pnl": float, "max_fav": float, "reason": str}
_v3_cache: dict[int, dict[str, Any]] = {}
_v3_cache_built_at: Optional[float] = None
_v3_cache_lock = threading.Lock()
_CACHE_TTL_SEC = 3600  # rebuild hourly during market hours

SL_PTS = 14.0
TARGET_FLOOR = 20.0  # raised 10→20 on 2026-05-18 (v3.1.1) — prevented premature +10 exits when magnet close to entry. Audit on 16 v3.1 trades: +14 pts (+$70 MES), zero regressions, WR unchanged 80%.
TRAIL_ACT = 15.0
TRAIL_GAP = 5.0

# v3.2 (2026-06-01, PORTAL-OBSERVATION ONLY): bullish-paradigm confluence override.
# Study: trend signals fail standalone, but as a confluence gate on GEX Long (our only
# directional setup) a bullish-drift paradigm can substitute for greek_alignment>=0.
# v3.2 pass = verdict ABC AND hour<15 AND (align>=0 OR paradigm in BULL_PARADIGMS).
# Backtest vs v3.1: 15->18 trades, +170.6->+192.2p, WR 80->78%, MaxDD UNCHANGED at -14
# (bull-paradigms naturally avoid the May-18 cluster that broke the apos route on DD).
# Paradigms chosen from H4 forward-drift study (bullish 60-min drift). Sim-only, never
# broker-traded. See research_trend_setups_refuted.md. NOT a real-trade flag.
BULL_PARADIGMS = {"BofA-LIS", "GEX-TARGET", "SIDIAL-MESSY", "BOFA-PURE"}


def _features(cur, t_utc, spot: float) -> Optional[dict]:
    spot_f = float(spot) if spot else 0
    if spot_f == 0:
        return None
    # GEX = TS chain gamma (C_Gamma*C_OI - P_Gamma*P_OI) from chain_snapshots — the
    # SAME source the LIVE detector uses (main._gex_long_v3_features). This previously
    # read Volland gamma, which disagreed with live (known bug, feedback_gex_means_ts_gamma):
    # the structure (CORE_R3/R2, magnet) and the R_BURIED_MAGNET veto were graded on the
    # wrong data, so the portal overlay didn't match what TSRT actually placed.
    # chain_snapshots stores a mirrored call|strike|put row layout; positional indices
    # (validated 2026-06-08): Strike=10, C_OpenInt=1, C_Gamma=3, P_Gamma=17, P_OpenInt=19.
    cur.execute("""SELECT rows FROM chain_snapshots
                   WHERE ts BETWEEN %s - interval '6 min' AND %s + interval '2 min'
                   ORDER BY abs(extract(epoch FROM (ts - %s))) LIMIT 1""",
                (t_utc, t_utc, t_utc))
    ch = cur.fetchone()
    cur.execute("""SELECT ts_utc FROM volland_exposure_points
                   WHERE ts_utc BETWEEN %s - interval '5 min' AND %s
                     AND greek='charm' ORDER BY ts_utc DESC LIMIT 1""", (t_utc, t_utc))
    c_snap = cur.fetchone()
    if not ch or not c_snap:
        return None
    _chain_rows = ch[0] if isinstance(ch[0], list) else json.loads(ch[0])
    gex = []
    for _row in _chain_rows:
        try:
            _s = float(_row[10])
            _cg = float(_row[3] or 0); _co = float(_row[1] or 0)
            _pg = float(_row[17] or 0); _po = float(_row[19] or 0)
        except (TypeError, ValueError, IndexError):
            continue
        if spot_f - 50 <= _s <= spot_f + 50:
            gex.append((_s, (_cg * _co) - (_pg * _po)))
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
    pos_charm_above = [(s, v) for s, v in charm_above if v > 0]
    bullish_charm_magnet = min(neg_charm_above, key=lambda x: x[1]) if neg_charm_above else None
    strongest_pos_charm_above = max(pos_charm_above, key=lambda x: x[1]) if pos_charm_above else (None, 0)

    total_gex = sum(v for _, v in gex)
    total_charm = sum(v for _, v in charm)
    above_charm_pos_pct = (sum(1 for _, v in charm_above if v > 0) /
                           max(len(charm_above), 1) * 100)

    R5_align = (bullish_charm_magnet is not None and sg_above[0] is not None
                and sg_above[1] > 0 and abs(bullish_charm_magnet[0] - sg_above[0]) <= 10)
    # v3.1: negative charm magnet must be at least 50% of strongest +charm above
    R5_align_strong = R5_align and (
        bullish_charm_magnet is not None
        and strongest_pos_charm_above[1] > 0
        and abs(bullish_charm_magnet[1]) >= 0.5 * strongest_pos_charm_above[1]
    )
    # R_BURIED_MAGNET veto (user 2026-06-08): negligible +GEX magnet in net-negative
    # regime (magnet below top-3 by |GEX|), unless a strong charm magnet (>=50 M$) sits
    # at the GEX magnet. Mirrors main._gex_long_v3_features. See that docstring.
    magnet_strike = sg_above[0]
    ranked = sorted(gex, key=lambda x: -abs(x[1]))
    magnet_rank = ([s for s, _ in ranked].index(magnet_strike) + 1
                   if magnet_strike is not None else 99)
    charm_rescue = False
    if charm_above and magnet_strike is not None:
        cs, cv = max(charm_above, key=lambda x: abs(x[1]))
        if abs(cs - magnet_strike) <= 10 and abs(cv) >= 50e6:
            charm_rescue = True
    R_BURIED_MAGNET = (total_gex < 0) and (magnet_rank > 3) and (not charm_rescue)
    # ── v6 features (2026-06-08, portal observation) ──────────────────────────
    # v6 magnet = strongest POSITIVE GEX strike above spot (NOT max-abs, which can be a
    # negative bar — fixes the #1642 mis-pick). Dominance = that magnet's GEX vs the
    # strongest negative wall in-band; a magnet dwarfed by the negative GEX is "fake"
    # (user insight, lid #762). dominance>=1.0 was OOS-stable (monotonic sweep, each
    # month >=80% WR). 99 sentinel when there is no negative bar at all.
    pos_gex_above = [(s, v) for s, v in gex_above if v > 0]
    v6_magnet = max(pos_gex_above, key=lambda x: x[1]) if pos_gex_above else (None, 0.0)
    v6_maxneg = abs(min((v for _, v in gex if v < 0), default=0.0))
    v6_dominance = (v6_magnet[1] / v6_maxneg) if v6_maxneg > 0 else 99.0
    return {
        'gex_magnet_strike': sg_above[0],
        'CORE_R3': sg_above[1] > 0,
        'CORE_R2': sg_below[1] < 0,
        'R5_align': R5_align,
        'R_charm_bullish': total_charm < 0,
        'R_gex_regime_pos': total_gex >= 0,
        # v3.1: carveout requires STRONG R5_align (neg_charm >= 50% of dominant +charm wall)
        'R_VETO': (above_charm_pos_pct >= 80) and (not R5_align_strong),
        'R_BURIED_MAGNET': R_BURIED_MAGNET,
        'v6_has_pos_magnet': v6_magnet[0] is not None,
        'v6_magnet_strike': v6_magnet[0],
        'v6_dominance': v6_dominance,
    }


def _classify(f: Optional[dict]) -> str:
    # NOTE: R_BURIED_MAGNET is intentionally NOT applied here. It is the v4-only
    # veto (separated 2026-06-08) so v3.1/v3.2 verdicts stay as they were (v3.2 = 18
    # trades). pass_v4 in _build_cache applies the buried-magnet veto on top of v3.2.
    # The LIVE detector (setup_detector._gex_long_v3_classify) DOES bake the veto into
    # its verdict — that's fine, because live fire == portal pass_v4 (v3.2 AND not buried).
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
                              greek_alignment, spot, paradigm
                       FROM setup_log
                       WHERE setup_name = 'GEX Long'
                         AND grade != 'LOG' AND grade IS NOT NULL
                       ORDER BY ts""")
        rows = cur.fetchall()
        for lid, t_utc, t_et, al, spot, paradigm in rows:
            # Commit per iteration: raw_connection() is non-autocommit, so without
            # this the whole rebuild is ONE transaction holding AccessShareLock on
            # chain_snapshots/volland_exposure_points for minutes — which blocked
            # db_init's ALTER on deploy and crash-looped the service (2026-06-03).
            # Reads only, so commit just ends the txn and releases locks.
            raw.commit()
            if not spot:
                out[lid] = {"pass": False, "pass_v32": False, "pass_v4": False, "pass_v6": False,
                            "verdict": "NO_DATA", "reason": "no_spot",
                            "result": None, "pnl": None, "max_fav": None,
                            "result_v4": None, "pnl_v4": None, "max_fav_v4": None,
                            "result_v6": None, "pnl_v6": None}
                continue
            try:
                f = _features(cur, t_utc, spot)
            except Exception:
                f = None
            verdict = _classify(f)
            align = al if al is not None else 0
            hour = t_et.hour if t_et else 99
            verdict_ok = verdict in ('A++', 'A', 'B')
            v3_pass = verdict_ok and (align >= 0) and (hour < 15)
            # v3.2: bullish-paradigm can substitute for align>=0 (portal observation)
            v32_pass = verdict_ok and (hour < 15) and (
                (align >= 0) or (paradigm in BULL_PARADIGMS))
            # v4 (the SHIPPED real-traded config) = v3.2 + R_BURIED_MAGNET veto. This is
            # exactly what TSRT/eval place — what the portal V16 (live) view must show.
            buried = bool(f.get('R_BURIED_MAGNET')) if f else False
            v4_pass = v32_pass and (not buried)
            # v6 (portal observation, 2026-06-08): TS GEX + a real POSITIVE magnet
            # (v6_has_pos_magnet) + magnet DOMINANCE >= 1.0 (not dwarfed by the negative
            # wall) + drop GEX-TARGET afternoon. Deliberately does NOT use the R_VETO/
            # CORE_R2/R5 grading — the dominance gate replaces it. Backtest 21t / 86% WR /
            # +270p trail-only, OOS-stable (monotonic dominance sweep; each month >=80%).
            # PORTAL-ONLY — NOT real-traded. See feedback_v16_equals_tsrt_placed / v6 study.
            _gtpm = (paradigm == 'GEX-TARGET') and (hour >= 13)
            v6_pass = bool(f and f.get('v6_has_pos_magnet')
                           and (f.get('v6_dominance', 0.0) >= 1.0)
                           and (not _gtpm) and (hour < 15)
                           and ((align >= 0) or (paradigm in BULL_PARADIGMS)))

            if (not (v3_pass or v32_pass or v6_pass)) or f is None:
                out[lid] = {"pass": v3_pass, "pass_v32": v32_pass, "pass_v4": v4_pass,
                            "pass_v6": v6_pass,
                            "verdict": verdict, "reason": "filter_block",
                            "result": None, "pnl": None, "max_fav": None,
                            "result_v4": None, "pnl_v4": None, "max_fav_v4": None,
                            "result_v6": None, "pnl_v6": None}
                continue

            entry = float(spot)
            magnet = f['gex_magnet_strike']
            target = max(magnet or 0, entry + TARGET_FLOOR)
            try:
                result, pnl, max_fav, reason = _simulate_exit(cur, t_utc, entry, target)
            except Exception as exc:
                out[lid] = {"pass": v3_pass, "pass_v32": v32_pass, "pass_v4": v4_pass,
                            "pass_v6": v6_pass,
                            "verdict": verdict, "reason": f"sim_err:{exc}",
                            "result": None, "pnl": None, "max_fav": None,
                            "result_v4": None, "pnl_v4": None, "max_fav_v4": None,
                            "result_v6": None, "pnl_v6": None}
                continue

            # TRAIL-ONLY outcome (SL14 + trail 15/5, NO fixed target) — the SHIPPED exit,
            # shared by v4 and v6 (both trail-only; the exit is identical, only the entry
            # filter differs). Sim with an unreachable target so only SL/trail/EOD close it.
            result_v4 = pnl_v4 = max_fav_v4 = None
            result_v6 = pnl_v6 = None
            if v4_pass or v6_pass:
                try:
                    _rt, _pt, _mt, _ = _simulate_exit(cur, t_utc, entry, entry + 1e9)
                    if v4_pass:
                        result_v4, pnl_v4, max_fav_v4 = _rt, round(_pt, 2), round(_mt, 2)
                    if v6_pass:
                        result_v6, pnl_v6 = _rt, round(_pt, 2)
                except Exception:
                    pass

            out[lid] = {
                "pass": v3_pass,
                "pass_v32": v32_pass,
                "pass_v4": v4_pass,
                "pass_v6": v6_pass,
                "verdict": verdict,
                "result": result,
                "pnl": round(pnl, 2),
                "max_fav": round(max_fav, 2),
                "reason": reason,
                "result_v4": result_v4,
                "pnl_v4": pnl_v4,
                "max_fav_v4": max_fav_v4,
                "result_v6": result_v6,
                "pnl_v6": pnl_v6,
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
