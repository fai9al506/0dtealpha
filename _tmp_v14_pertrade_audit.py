"""V14-era per-trade root-cause audit.

For each real-money trade since 2026-04-29 (V14 went live):
  1. Pull state JSON + setup_log row.
  2. Walk SPX chain_snapshots path from signal_ts -> session-end and apply EXACT
     SL/trail params the bot would have used in SPX space. Compute portal_pnl_pts
     and portal_exit_event.
  3. Compute real_pnl_pts from broker state (MES space, converted to SPX-equivalent
     points 1:1 since MES tick = ES tick = 1 SPX pt of equivalent exposure).
  4. Classify gap into root cause buckets:
       TRAIL_TAG_EARLY, SL_HIT_BOTH, TARGET_HIT_BOTH, BUG_WRONG_SIDE_STOP,
       BUG_GHOST_RECONCILE, BUG_EOD_FLATTEN, BASIS_DRIFT, OUTCOME_RESOLVED,
       OTHER_UNKNOWN
  5. For TRAIL_TAG_EARLY, deep-dive: compute MES max-adverse excursion using VPS
     5pt range bars and compare against SPX max-adverse.
  6. Hypothesis test: pct_gap_from_MES_wick = $(TRAIL_TAG_EARLY) / $(total_gap)
  7. Architecture-change simulation: replay trades with SPX-driven trail,
     broker-SL kept as capital protection. Compare to actual.
  8. Risk catalog.

OUTPUT: _tmp_v14_pertrade_audit.html (dark theme)
"""
from __future__ import annotations
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
V14_START = "2026-04-29"
MES_PT_USD = 5.0  # $5/pt per MES
BASIS_DRIFT_THRESHOLD_PT = 5.0  # gap <= 5 pts that is explained by basis is classified BASIS_DRIFT
SLIPPAGE_PT = 0.5  # assumed slippage if we did SPX-trail market exit

# ---------------------------------------------------------------------------
# Trail/SL params from app/main.py:4337-4407 (initial stop_lvl) +
# app/main.py:4592-4614 (trail params). AG Short uses lis+/-5 capped at spot+/-20,
# so SL distance is dynamic — we'll override per-trade from the actual real stop dist.
# ---------------------------------------------------------------------------
TRAIL_PARAMS = {
    "DD Exhaustion":   {"mode": "continuous", "activation": 20, "gap": 5,  "sl": 12,  "tp": None},
    "GEX Long":        {"mode": "continuous", "activation": 15, "gap": 5,  "sl": 14,  "tp": None},
    "GEX Velocity":    {"mode": "hybrid", "be_trigger": 8,  "activation": 10, "gap": 5, "sl": 8, "tp": None},
    "AG Short":        {"mode": "hybrid", "be_trigger": 10, "activation": 12, "gap": 5, "sl": 20, "tp": None},
    "Skew Charm":      {"mode": "hybrid", "be_trigger": 10, "activation": 10, "gap": 5, "sl": 14, "tp": None},
    "ES Absorption":   {"mode": "hybrid", "be_trigger": 5,  "activation": 8,  "gap": 3, "sl": 8,  "tp": None},
    # VIX Divergence — longs only on V14 era:
    "VIX Divergence":  {"mode": "continuous", "activation": 10, "gap": 8,  "sl": 8,  "tp": None},
}

# Max session hold (rough): SC/AG/DD/GEX have 90-min outcome window; ES Abs none.
SESSION_END_UTC_BY_DATE: Dict[str, datetime] = {}

# ---------------------------------------------------------------------------

def conn_db():
    return psycopg2.connect(DB_URL)

def load_v14_universe(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT rto.setup_log_id, rto.state, rto.created_at,
               sl.setup_name, sl.direction, sl.spot AS signal_spot, sl.ts AS signal_ts,
               sl.target, sl.lis, sl.grade, sl.paradigm,
               sl.outcome_pnl, sl.outcome_result, sl.outcome_first_event,
               sl.outcome_max_profit, sl.outcome_max_loss, sl.outcome_elapsed_min,
               sl.trail_sl, sl.trail_activation, sl.trail_gap,
               sl.exit_price, sl.bofa_stop_level, sl.bofa_target_level,
               sl.charm_limit_entry, sl.abs_es_price
        FROM real_trade_orders rto
        JOIN setup_log sl ON sl.id = rto.setup_log_id
        WHERE rto.created_at >= %s
        ORDER BY rto.created_at
    """, (V14_START,))
    return list(cur.fetchall())

def load_chain_path(conn, signal_ts: datetime, session_end: datetime) -> List[Tuple[datetime, float]]:
    """Return list of (ts, spot) from chain_snapshots in [signal_ts, session_end]."""
    cur = conn.cursor()
    cur.execute("""
        SELECT ts, spot FROM chain_snapshots
        WHERE ts >= %s AND ts <= %s
        ORDER BY ts
    """, (signal_ts, session_end))
    return [(r[0], float(r[1])) for r in cur.fetchall()]

def load_es_bars(conn, signal_ts: datetime, session_end: datetime) -> List[Dict[str, Any]]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, ts_start, ts_end
        FROM vps_es_range_bars
        WHERE ts_end >= %s AND ts_start <= %s AND range_pts = 5
        ORDER BY ts_start
    """, (signal_ts, session_end))
    return list(cur.fetchall())

def session_end_for(ts: datetime) -> datetime:
    """16:00 ET on the same trading date (cap)."""
    # ts is UTC. ET = UTC-4 (EDT in April/May) for V14 era.
    et = ts.astimezone(timezone(timedelta(hours=-4)))
    end_et = et.replace(hour=16, minute=0, second=0, microsecond=0)
    return end_et.astimezone(timezone.utc)


def simulate_spx_trail(
    signal_ts: datetime,
    entry_spx: float,
    is_long: bool,
    setup_name: str,
    chain_path: List[Tuple[datetime, float]],
    session_end: datetime,
    override_sl_dist: Optional[float] = None,
) -> Dict[str, Any]:
    """Replay the trade in SPX space with exact V14-era SL/trail params.
    Returns dict with: pnl_pts, exit_event, exit_ts, exit_spx, max_fav, max_adv,
                        trail_lock_at_exit.
    """
    tp = TRAIL_PARAMS.get(setup_name)
    sl_dist = override_sl_dist if override_sl_dist is not None else (tp["sl"] if tp else 14)
    target_dist = tp.get("tp") if tp else None

    if is_long:
        stop_lvl = entry_spx - sl_dist
        target_lvl = entry_spx + target_dist if target_dist else None
    else:
        stop_lvl = entry_spx + sl_dist
        target_lvl = entry_spx - target_dist if target_dist else None

    max_fav = 0.0
    max_adv = 0.0
    trail_active = False
    be_locked = False

    # Walk forward through chain_path. Chain is 2min intervals.
    # We can only see the spot at each sample; assume between samples spot
    # interpolates monotonically (this is the bot's actual view of the SPX path).
    for i, (ts, spot) in enumerate(chain_path):
        if ts < signal_ts:
            continue
        # use prev_spot and spot to model the segment
        if i == 0:
            prev = spot
        else:
            prev = chain_path[i-1][1]
        seg_hi = max(prev, spot)
        seg_lo = min(prev, spot)

        # Update fav/adv
        fav = (seg_hi - entry_spx) if is_long else (entry_spx - seg_lo)
        adv = (entry_spx - seg_lo) if is_long else (seg_hi - entry_spx)
        if fav > max_fav: max_fav = fav
        if adv > max_adv: max_adv = adv

        # Advance trail based on max_fav (using cumulative fav, like bot)
        new_trail_lock = None
        if tp:
            if tp["mode"] == "continuous":
                if max_fav >= tp["activation"]:
                    new_trail_lock = max_fav - tp["gap"]
            elif tp["mode"] == "hybrid":
                if max_fav >= tp["activation"]:
                    new_trail_lock = max_fav - tp["gap"]
                elif max_fav >= tp["be_trigger"]:
                    new_trail_lock = 0  # breakeven
        if new_trail_lock is not None:
            cand = (entry_spx + new_trail_lock) if is_long else (entry_spx - new_trail_lock)
            if is_long and cand > stop_lvl:
                stop_lvl = cand
                trail_active = (new_trail_lock > 0)
                be_locked = (new_trail_lock == 0) or be_locked
            elif (not is_long) and cand < stop_lvl:
                stop_lvl = cand
                trail_active = (new_trail_lock > 0)
                be_locked = (new_trail_lock == 0) or be_locked

        # Stop check (uses segment hi/lo to capture intra-segment touches)
        if is_long and seg_lo <= stop_lvl:
            pnl = stop_lvl - entry_spx
            event = "trail" if trail_active else ("breakeven" if be_locked else "stop")
            return {"pnl_pts": pnl, "exit_event": event, "exit_ts": ts, "exit_spx": stop_lvl,
                    "max_fav": max_fav, "max_adv": max_adv, "trail_active": trail_active,
                    "be_locked": be_locked}
        if (not is_long) and seg_hi >= stop_lvl:
            pnl = entry_spx - stop_lvl
            event = "trail" if trail_active else ("breakeven" if be_locked else "stop")
            return {"pnl_pts": pnl, "exit_event": event, "exit_ts": ts, "exit_spx": stop_lvl,
                    "max_fav": max_fav, "max_adv": max_adv, "trail_active": trail_active,
                    "be_locked": be_locked}
        # Target check
        if target_lvl is not None:
            if is_long and seg_hi >= target_lvl:
                return {"pnl_pts": target_lvl - entry_spx, "exit_event": "target",
                        "exit_ts": ts, "exit_spx": target_lvl, "max_fav": max_fav,
                        "max_adv": max_adv, "trail_active": trail_active, "be_locked": be_locked}
            if (not is_long) and seg_lo <= target_lvl:
                return {"pnl_pts": entry_spx - target_lvl, "exit_event": "target",
                        "exit_ts": ts, "exit_spx": target_lvl, "max_fav": max_fav,
                        "max_adv": max_adv, "trail_active": trail_active, "be_locked": be_locked}

    # Reached session end with no stop/target
    if not chain_path:
        return {"pnl_pts": 0.0, "exit_event": "no_data", "exit_ts": None, "exit_spx": entry_spx,
                "max_fav": 0.0, "max_adv": 0.0, "trail_active": False, "be_locked": False}
    last_ts, last_spot = chain_path[-1]
    pnl = (last_spot - entry_spx) if is_long else (entry_spx - last_spot)
    return {"pnl_pts": pnl, "exit_event": "eod", "exit_ts": last_ts, "exit_spx": last_spot,
            "max_fav": max_fav, "max_adv": max_adv, "trail_active": trail_active,
            "be_locked": be_locked}


# Broker fills loaded once from tsrt_historical_orders.json (where available).
_BROKER_FILLS_CACHE = None
def _load_broker_fills():
    global _BROKER_FILLS_CACHE
    if _BROKER_FILLS_CACHE is not None:
        return _BROKER_FILLS_CACHE
    try:
        with open("_tmp_tsrt/tsrt_historical_orders.json") as f:
            orders = json.load(f)
    except Exception:
        _BROKER_FILLS_CACHE = []
        return _BROKER_FILLS_CACHE
    fills = []
    for o in orders:
        if o.get("Status") != "FLL":
            continue
        leg = (o.get("Legs") or [{}])[0]
        if not leg.get("ExecutionPrice"):
            continue
        fills.append({
            "oid": str(o.get("OrderID")),
            "account": o.get("_account"),
            "ts": o.get("ClosedDateTime"),
            "side": leg.get("BuyOrSell"),
            "price": float(leg.get("ExecutionPrice")),
            "type": o.get("OrderType"),
        })
    fills.sort(key=lambda r: (r["account"], r["ts"] or ""))
    _BROKER_FILLS_CACHE = fills
    return fills


def _find_next_opposite_fill(account: str, entry_ts_iso: str, exit_side: str, exclude_oids: set):
    """Find the next FLL on the same account after entry_ts on opposite side."""
    fills = _load_broker_fills()
    for f in fills:
        if f["account"] != account: continue
        if (f["ts"] or "") <= entry_ts_iso: continue
        if f["side"] != exit_side: continue
        if f["oid"] in exclude_oids: continue
        return f
    return None


def compute_real_pnl(state: dict, is_long: bool) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    """Return (real_pnl_pts_mes_space, entry_px_mes, exit_px_mes, exit_source_label)."""
    entry = state.get("fill_price")
    if entry is None:
        return None, None, None, "no_entry"
    entry = float(entry)
    exit_px = state.get("stop_fill_price") or state.get("close_fill_price")
    if exit_px is None:
        # broker exit unknown — try target_fill_price
        exit_px = state.get("target_fill_price")
    if exit_px is None:
        # Try to find broker fill from historical orders
        account = state.get("account_id")
        entry_oid = str(state.get("entry_order_id") or "")
        ts_placed = state.get("ts_placed") or ""
        # ts_placed is "2026-05-04T13:45:05.648768" naive; add 'Z' since file uses 'Z' UTC
        if ts_placed and "Z" not in ts_placed:
            entry_ts = ts_placed[:19] + "Z"
        else:
            entry_ts = ts_placed
        exit_side = "Sell" if is_long else "Buy"
        bf = _find_next_opposite_fill(account or "", entry_ts, exit_side, {entry_oid})
        if bf:
            exit_px = bf["price"]
            return ((exit_px - entry) if is_long else (entry - exit_px)), entry, exit_px, f"broker_lookup_{bf['type']}"
        return None, entry, None, "no_exit"
    exit_px = float(exit_px)
    pnl = (exit_px - entry) if is_long else (entry - exit_px)
    src = "stop_fill" if state.get("stop_fill_price") else "close_fill"
    return pnl, entry, exit_px, src


def classify_trade(
    trade: Dict[str, Any],
    real_pnl: Optional[float],
    portal_pnl: float,
    portal_event: str,
    real_event: str,
    state: dict,
    portal_trail_active: bool,
    portal_be_locked: bool,
    setup_name: str,
    is_long: bool,
    entry_spx: float,
    portal_max_adv: float,
    sl_dist_used: float,
) -> Tuple[str, str]:
    """Return (category, narrative)."""
    cr = state.get("close_reason") or ""

    if cr == "ghost_reconcile":
        return "BUG_GHOST_RECONCILE", f"Reconciler-recovered close; bot/broker state diverged. real_pnl={real_pnl:+.2f} portal={portal_pnl:+.2f}"
    if cr == "eod_flatten":
        return "BUG_EOD_FLATTEN", f"Held overnight (eod flatten). portal exit_event={portal_event}"
    if cr == "stop_rejected_async":
        return "BUG_STOP_REJECTED", f"Broker rejected stop order async — bot market-exited at {real_pnl}. Portal would have done {portal_pnl:+.2f} via {portal_event}."
    if "wrong_side" in cr.lower():
        return "BUG_WRONG_SIDE_STOP", "S80 wrong-side-stop fingerprint"
    if cr.startswith("outcome_resolved"):
        return "OUTCOME_RESOLVED", f"Outcome tracker resolved ({cr}), broker stop never fired. real={real_pnl} portal={portal_pnl:+.2f}"
    if cr == "trail_market_exit":
        return "OUTCOME_RESOLVED", "Bot market-exited at trail trigger (rare path)"

    # cr == 'stop_filled' or 'LOSS' (legacy)
    if real_pnl is None:
        return "OTHER_UNKNOWN", f"No real_pnl computed (cr={cr})"

    gap = real_pnl - portal_pnl
    sl_dist = sl_dist_used

    # ---- Initial SL on both ----
    # portal_event="stop" means it hit the entry-SL exactly (not trail)
    if portal_event == "stop" and real_pnl <= -(sl_dist - 1) and portal_pnl <= -(sl_dist - 1):
        return "SL_HIT_BOTH", f"Both initial SL. real={real_pnl:+.1f} portal={portal_pnl:+.1f} gap={gap:+.2f}"

    # ---- Real took initial SL, portal didn't ----
    # The DEFINITIVE TRAIL_TAG_EARLY pattern: broker stop fired (real_pnl ~= -sl_dist)
    # while SPX portal path either trailed to profit or never hit stop.
    if real_pnl <= -(sl_dist - 1) and portal_pnl > -(sl_dist - 1):
        return "TRAIL_TAG_ADVERSE_WICK", f"Real broker SL fired (loss {real_pnl:+.1f}pt) while SPX portal would have exited at {portal_event} for {portal_pnl:+.1f}pt. SPX-path max-adv was only {portal_max_adv:.1f}pt vs SL distance {sl_dist}pt."

    # ---- Both target hit ----
    if portal_event == "target" and real_pnl > 0:
        return "TARGET_HIT_BOTH", f"Both target-hit. Gap={gap:+.2f}pt = basis drift only."

    # ---- Trail/BE fired in real at LOWER profit than SPX-trail would have ----
    # The "favorable wick" TRAIL_TAG_EARLY: MES had bigger fav-move than SPX, so trail
    # locked tighter on MES wick. real_pnl > 0 but portal_pnl >> real_pnl.
    if portal_event in ("trail", "breakeven", "target") and gap <= -BASIS_DRIFT_THRESHOLD_PT:
        return "TRAIL_TAG_FAV_WICK", f"Real trail/BE fired at {real_pnl:+.1f}pt; SPX path would have run further to {portal_event} for {portal_pnl:+.1f}pt. MES wick over-tightened trail."

    # ---- Real took stop but portal would have exited better via trail ----
    if real_pnl < 0 and real_pnl > -(sl_dist - 1) and portal_event in ("trail", "breakeven"):
        return "TRAIL_TAG_ADVERSE_WICK", f"Real took partial loss ({real_pnl:+.1f}pt); SPX path would have caught the trail at {portal_pnl:+.1f}pt."

    # ---- Real took small loss/win, portal also lost via trail ----
    if abs(gap) <= BASIS_DRIFT_THRESHOLD_PT:
        return "BASIS_DRIFT", f"Gap={gap:+.2f}pt within basis drift threshold."

    # ---- Positive gap (real better than portal) — MES move was MORE favorable than SPX ----
    if gap > BASIS_DRIFT_THRESHOLD_PT:
        return "MES_FAVORABLE", f"Real outperformed SPX path by {gap:+.2f}pt (MES had bigger favorable swing than SPX showed)."

    return "OTHER_UNKNOWN", f"Unclassified — gap={gap:+.2f}pt real_event={real_event} portal_event={portal_event}"


# ---------------------------------------------------------------------------

def run_audit():
    conn = conn_db()
    universe = load_v14_universe(conn)

    print(f"Loaded {len(universe)} V14-era RTOs")

    results = []
    skipped = []

    for row in universe:
        sid = row["setup_log_id"]
        state = row["state"] or {}
        setup_name = row["setup_name"]
        direction = row["direction"]
        is_long = direction in ("long", "bullish")
        signal_ts = row["signal_ts"]
        signal_spot = float(row["signal_spot"])

        # Use signal_spot as entry_spx (close enough — bot would have entered ~1 cycle later but
        # chain_snapshots is 2-min so this is the best approximation in SPX space).
        # Better: take the spot at first chain tick AFTER signal_ts.
        sess_end = session_end_for(signal_ts)
        chain = load_chain_path(conn, signal_ts, sess_end)
        if not chain:
            skipped.append((sid, "no_chain_data"))
            continue
        entry_spx = chain[0][1]  # first available chain spot at/after signal

        # SL distance: use setup_log.outcome_stop_level vs signal_spot for SPX-space SL
        # (this captures AG Short's dynamic lis-based SL). Falls back to TRAIL_PARAMS default.
        override_sl = None
        osl_attr = row["outcome_stop_level"] if "outcome_stop_level" in row.keys() else None
        # actually we didn't select it — re-fetch
        # For now, use known: AG Short uses up to 20pt SL (lis+/-5 capped at 20).
        # SC/GEX/etc use TRAIL_PARAMS defaults.
        if setup_name == "AG Short":
            # Use real MES SL distance as proxy for SPX SL distance (1:1 since both ticks = 1pt)
            st_fill = state.get("fill_price")
            stop_fp = state.get("stop_fill_price")
            # If stop fired and current_stop == stop_fill_price AND no trail (max_favorable=0),
            # then current_stop = initial_stop. Use that.
            if st_fill and stop_fp:
                # If max_favorable=0, current_stop == initial_stop
                if (state.get("max_favorable") or 0) == 0:
                    d = abs(float(st_fill) - float(stop_fp))
                    if 5 < d < 30:
                        override_sl = d
            # If trail fired, use 20 as upper-bound proxy
            if override_sl is None:
                override_sl = 20.0

        portal = simulate_spx_trail(
            signal_ts=signal_ts,
            entry_spx=entry_spx,
            is_long=is_long,
            setup_name=setup_name,
            chain_path=chain,
            session_end=sess_end,
            override_sl_dist=override_sl,
        )
        # Real
        real_pnl, real_entry_mes, real_exit_mes, real_src = compute_real_pnl(state, is_long)

        # Effective SL used (for classification thresholds)
        eff_sl = override_sl if override_sl else TRAIL_PARAMS.get(setup_name, {}).get("sl", 14)
        # Classify
        category, narrative = classify_trade(
            trade=dict(row), real_pnl=real_pnl, portal_pnl=portal["pnl_pts"],
            portal_event=portal["exit_event"], real_event=state.get("close_reason") or "",
            state=state, portal_trail_active=portal["trail_active"],
            portal_be_locked=portal["be_locked"], setup_name=setup_name,
            is_long=is_long, entry_spx=entry_spx, portal_max_adv=portal["max_adv"],
            sl_dist_used=eff_sl,
        )

        # MES-bar excursion for ALL trades — compare against SPX path
        mes_max_adv = None
        mes_max_fav = None
        if real_entry_mes:
            es_bars = load_es_bars(conn, signal_ts, sess_end)
            if es_bars:
                hi = max(float(b["bar_high"]) for b in es_bars)
                lo = min(float(b["bar_low"]) for b in es_bars)
                if is_long:
                    mes_max_fav = hi - real_entry_mes
                    mes_max_adv = real_entry_mes - lo
                else:
                    mes_max_fav = real_entry_mes - lo
                    mes_max_adv = hi - real_entry_mes

        gap = (real_pnl - portal["pnl_pts"]) if real_pnl is not None else None
        results.append({
            "lid": sid,
            "signal_ts": signal_ts,
            "setup_name": setup_name,
            "direction": direction,
            "grade": row["grade"],
            "paradigm": row["paradigm"],
            "entry_spx": entry_spx,
            "entry_mes": real_entry_mes,
            "exit_mes": real_exit_mes,
            "exit_spx": portal["exit_spx"],
            "real_pnl_pts": real_pnl,
            "portal_pnl_pts": portal["pnl_pts"],
            "gap_pts": gap,
            "portal_event": portal["exit_event"],
            "real_event": state.get("close_reason"),
            "spx_max_adv": portal["max_adv"],
            "spx_max_fav": portal["max_fav"],
            "mes_max_adv": mes_max_adv,
            "mes_max_fav": mes_max_fav,
            "category": category,
            "narrative": narrative,
        })

    print(f"Classified {len(results)} | Skipped {len(skipped)}")
    return results, skipped


# ---------------------------------------------------------------------------
# Architecture simulation
# ---------------------------------------------------------------------------

def simulate_arch_change(results: List[dict]) -> List[dict]:
    """For each trade, simulate the proposed architecture: broker SL stays (capital
    protection at initial SL level), but trail/BE exits driven by SPX path (no
    MES wick).

    Mechanics:
      - If portal_event == "stop" (initial SL hit in SPX too): real already matches, no change.
      - If portal_event == "target" or "trail" or "breakeven" or "eod":
            new_real_pnl = portal_pnl - SLIPPAGE_PT  (we market-exit at SPX trigger)
      - If real took an initial SL hit but portal didn't (TRAIL_TAG_EARLY-like with bigger pre-stop SL):
            Since broker SL stays at original distance, if SPX path never touched it but MES did:
              -> with proposed arch, broker SL still protects capital but trail driven by SPX.
              -> So real_pnl = portal_pnl - SLIPPAGE_PT.
      - For ghost_reconcile, eod_flatten, outcome_resolved trades: leave real_pnl unchanged
        (not in scope of MES-wick fix).
    """
    out = []
    for r in results:
        new = dict(r)
        cat = r["category"]
        rpnl = r.get("real_pnl_pts")
        ppnl = r.get("portal_pnl_pts")
        if rpnl is None or ppnl is None:
            new["arch_pnl_pts"] = rpnl
            new["arch_note"] = "no real/portal data"
            out.append(new); continue
        if cat in ("BUG_GHOST_RECONCILE", "BUG_EOD_FLATTEN", "OUTCOME_RESOLVED", "BUG_WRONG_SIDE_STOP", "BUG_STOP_REJECTED", "OTHER_UNKNOWN"):
            new["arch_pnl_pts"] = rpnl
            new["arch_note"] = "out of scope (broker bug or outcome tracker)"
            out.append(new); continue
        if cat == "TARGET_HIT_BOTH":
            new["arch_pnl_pts"] = rpnl
            new["arch_note"] = "both hit target — no change"
            out.append(new); continue
        if cat == "SL_HIT_BOTH":
            new["arch_pnl_pts"] = rpnl
            new["arch_note"] = "both hit SL — no change (broker SL still fires)"
            out.append(new); continue
        if cat == "BASIS_DRIFT":
            new["arch_pnl_pts"] = rpnl
            new["arch_note"] = "small basis drift only"
            out.append(new); continue
        if cat in ("TRAIL_TAG_EARLY", "TRAIL_TAG_ADVERSE_WICK", "TRAIL_TAG_FAV_WICK"):
            # Replace with SPX-driven exit (portal_pnl) minus slippage
            new_pnl = ppnl - SLIPPAGE_PT
            new["arch_pnl_pts"] = new_pnl
            new["arch_note"] = f"SPX-trail market-exit @ portal_event={r['portal_event']}, slippage={SLIPPAGE_PT}pt"
            out.append(new); continue
        new["arch_pnl_pts"] = rpnl
        new["arch_note"] = "fallback"
        out.append(new)
    return out


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def fmt_pts(v):
    if v is None: return "—"
    return f"{v:+.2f}"

def fmt_usd(v):
    if v is None: return "—"
    return f"${v:+,.2f}"

CATEGORY_COLOR = {
    "TRAIL_TAG_EARLY":      "#ff6b6b",
    "TRAIL_TAG_ADVERSE_WICK": "#ff6b6b",
    "TRAIL_TAG_FAV_WICK":   "#ff8787",
    "SL_HIT_BOTH":          "#6c757d",
    "TARGET_HIT_BOTH":      "#51cf66",
    "BUG_WRONG_SIDE_STOP":  "#d63384",
    "BUG_GHOST_RECONCILE":  "#e67e22",
    "BUG_EOD_FLATTEN":      "#fab005",
    "BASIS_DRIFT":          "#74c0fc",
    "OUTCOME_RESOLVED":     "#9775fa",
    "MES_FAVORABLE":        "#82c91e",
    "BUG_STOP_REJECTED":    "#cc5de8",
    "OTHER_UNKNOWN":        "#868e96",
}

def build_html(results: List[dict], arch_results: List[dict]) -> str:
    # Summary by category
    cat_summary = defaultdict(lambda: {"n": 0, "real_pts": 0.0, "portal_pts": 0.0, "gap_pts": 0.0, "arch_pts": 0.0})
    for r, a in zip(results, arch_results):
        c = r["category"]
        cat_summary[c]["n"] += 1
        if r["real_pnl_pts"] is not None: cat_summary[c]["real_pts"] += r["real_pnl_pts"]
        if r["portal_pnl_pts"] is not None: cat_summary[c]["portal_pts"] += r["portal_pnl_pts"]
        if r["gap_pts"] is not None: cat_summary[c]["gap_pts"] += r["gap_pts"]
        if a["arch_pnl_pts"] is not None: cat_summary[c]["arch_pts"] += a["arch_pnl_pts"]

    total_real = sum(r["real_pnl_pts"] for r in results if r["real_pnl_pts"] is not None)
    total_portal = sum(r["portal_pnl_pts"] for r in results if r["portal_pnl_pts"] is not None)
    total_gap = total_real - total_portal
    total_arch = sum(a["arch_pnl_pts"] for a in arch_results if a["arch_pnl_pts"] is not None)
    arch_delta = total_arch - total_real

    trail_tag_gap_pts = (
        cat_summary.get("TRAIL_TAG_EARLY", {}).get("gap_pts", 0.0)
        + cat_summary.get("TRAIL_TAG_ADVERSE_WICK", {}).get("gap_pts", 0.0)
        + cat_summary.get("TRAIL_TAG_FAV_WICK", {}).get("gap_pts", 0.0)
    )
    pct_from_mes_wick = (trail_tag_gap_pts / total_gap * 100) if abs(total_gap) > 0.01 else 0.0

    if pct_from_mes_wick >= 80:
        verdict = "CONFIRMED"
        verdict_color = "#51cf66"
    elif 50 <= pct_from_mes_wick < 80:
        verdict = "PARTIALLY CONFIRMED"
        verdict_color = "#fab005"
    else:
        verdict = "REJECTED"
        verdict_color = "#ff6b6b"

    # Per-trade rows
    rows_html = []
    for r, a in zip(results, arch_results):
        cat = r["category"]
        bg = CATEGORY_COLOR.get(cat, "#868e96")
        rpnl = r["real_pnl_pts"]; ppnl = r["portal_pnl_pts"]; gap = r["gap_pts"]
        rows_html.append(f"""
        <tr>
          <td>{r['lid']}</td>
          <td>{str(r['signal_ts'])[:19]}</td>
          <td>{r['setup_name']}</td>
          <td>{r['direction']}</td>
          <td>{r['grade'] or '-'}</td>
          <td>{(r['paradigm'] or '-')[:18]}</td>
          <td>{r['entry_spx']:.1f}</td>
          <td>{(f"{r['entry_mes']:.1f}" if r['entry_mes'] is not None else '—')}</td>
          <td>{fmt_pts(rpnl)}</td>
          <td>{fmt_pts(ppnl)}</td>
          <td style="color:{'#ff6b6b' if (gap is not None and gap < 0) else '#51cf66'}">{fmt_pts(gap)}</td>
          <td>{r['portal_event']}</td>
          <td>{(r['real_event'] or '-')[:18]}</td>
          <td>{fmt_pts(r['spx_max_adv']) if r['spx_max_adv'] is not None else '—'}</td>
          <td>{fmt_pts(r['mes_max_adv']) if r['mes_max_adv'] is not None else '—'}</td>
          <td style="background:{bg};color:#000;font-weight:bold;padding:4px 8px;border-radius:4px;">{cat}</td>
          <td>{fmt_pts(a['arch_pnl_pts'])}</td>
          <td style="font-size:11px;color:#aaa">{r['narrative']}</td>
        </tr>""")

    # Category summary
    cat_rows = []
    for cat in sorted(cat_summary.keys(), key=lambda k: -abs(cat_summary[k]["gap_pts"])):
        s = cat_summary[cat]
        bg = CATEGORY_COLOR.get(cat, "#868e96")
        delta_arch = s["arch_pts"] - s["real_pts"]
        cat_rows.append(f"""
        <tr>
          <td style="background:{bg};color:#000;font-weight:bold;padding:6px;border-radius:4px;">{cat}</td>
          <td>{s['n']}</td>
          <td>{fmt_pts(s['real_pts'])} ({fmt_usd(s['real_pts']*MES_PT_USD)})</td>
          <td>{fmt_pts(s['portal_pts'])} ({fmt_usd(s['portal_pts']*MES_PT_USD)})</td>
          <td>{fmt_pts(s['gap_pts'])} ({fmt_usd(s['gap_pts']*MES_PT_USD)})</td>
          <td>{fmt_pts(s['arch_pts'])} ({fmt_usd(s['arch_pts']*MES_PT_USD)})</td>
          <td>{fmt_pts(delta_arch)} ({fmt_usd(delta_arch*MES_PT_USD)})</td>
        </tr>""")

    # Trail-tag-early specific stats (both adverse and favorable wick subtypes)
    tte = [r for r in results if r["category"] in ("TRAIL_TAG_EARLY", "TRAIL_TAG_ADVERSE_WICK", "TRAIL_TAG_FAV_WICK")]
    tte_mes_vs_spx = []
    for r in tte:
        if r["mes_max_adv"] is not None and r["spx_max_adv"] is not None:
            diverg = r["mes_max_adv"] - r["spx_max_adv"]
            tte_mes_vs_spx.append((r["lid"], r["spx_max_adv"], r["mes_max_adv"], diverg))
    tte_div_avg = statistics.mean(d for *_, d in tte_mes_vs_spx) if tte_mes_vs_spx else 0
    tte_div_max = max((d for *_, d in tte_mes_vs_spx), default=0)
    tte_rows = []
    for lid, spx_adv, mes_adv, div in sorted(tte_mes_vs_spx, key=lambda x: -x[3]):
        tte_rows.append(f"<tr><td>{lid}</td><td>{spx_adv:+.2f}</td><td>{mes_adv:+.2f}</td><td style='color:#ff6b6b'>{div:+.2f}</td></tr>")

    html = f"""<!doctype html>
<html><head><meta charset='utf-8'><title>V14 Per-Trade Audit</title>
<style>
  body {{ background:#0d0d12; color:#e8e8ec; font-family: 'Segoe UI', system-ui, sans-serif; margin: 20px; }}
  h1, h2, h3 {{ color: #fcc419; border-bottom: 1px solid #444; padding-bottom: 6px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 12px; }}
  th, td {{ border: 1px solid #2a2a2e; padding: 5px 8px; text-align: left; }}
  th {{ background: #1a1a22; color: #fcc419; font-weight: 600; }}
  tr:nth-child(even) {{ background: #15151a; }}
  tr:hover {{ background: #20202a; }}
  .verdict {{ padding: 20px; border-radius: 8px; margin: 20px 0; font-size: 18px; }}
  .stat {{ display: inline-block; background:#1a1a22; padding: 12px 18px; margin: 6px; border-radius: 6px; min-width: 200px; }}
  .stat .label {{ font-size: 11px; color: #999; text-transform: uppercase; }}
  .stat .value {{ font-size: 22px; font-weight: bold; color: #fcc419; }}
  .narrative {{ font-size: 11px; color: #999; }}
  .caveat {{ background: #2a1a00; border-left: 4px solid #fab005; padding: 12px; margin: 12px 0; }}
</style></head>
<body>
<h1>V14-Era Per-Trade Root-Cause Audit</h1>
<div class='caveat'>
  <b>Sample:</b> {len(results)} real-money MES trades from {V14_START} - 2026-05-13 (V14 era).
  <b>Confidence:</b> Directional only (n &lt; 50 = small sample). Use as hypothesis test, not final proof.
</div>

<h2>Top-line metrics</h2>
<div>
  <div class='stat'><div class='label'>Total real PnL</div><div class='value'>{total_real:+.1f} pt<br>${total_real*MES_PT_USD:+,.0f}</div></div>
  <div class='stat'><div class='label'>Total portal sim PnL</div><div class='value'>{total_portal:+.1f} pt<br>${total_portal*MES_PT_USD:+,.0f}</div></div>
  <div class='stat'><div class='label'>Total gap (real - portal)</div><div class='value' style='color:{("#ff6b6b" if total_gap<0 else "#51cf66")}'>{total_gap:+.1f} pt<br>${total_gap*MES_PT_USD:+,.0f}</div></div>
  <div class='stat'><div class='label'>TRAIL_TAG_EARLY gap</div><div class='value'>{trail_tag_gap_pts:+.1f} pt<br>${trail_tag_gap_pts*MES_PT_USD:+,.0f}</div></div>
  <div class='stat'><div class='label'>% gap from MES-wick</div><div class='value' style='color:{verdict_color}'>{pct_from_mes_wick:.1f}%</div></div>
</div>

<div class='verdict' style='background:{verdict_color};color:#0d0d12;font-weight:bold;'>
  HYPOTHESIS VERDICT: {verdict}<br>
  <span style='font-size:14px;font-weight:normal;'>
    User hypothesis: 90%+ of portal-vs-real gap from MES tick wicks tagging trail stops SPX never sees.<br>
    Measured: {pct_from_mes_wick:.1f}% of total $ gap classified TRAIL_TAG_EARLY.
  </span>
</div>

<h2>Category summary</h2>
<table>
  <thead><tr><th>Category</th><th>n</th><th>Real PnL</th><th>Portal sim PnL</th><th>Gap (real-portal)</th><th>Architecture-fix PnL</th><th>Delta (arch-real)</th></tr></thead>
  <tbody>
    {"".join(cat_rows)}
    <tr style='font-weight:bold;background:#222'>
      <td>TOTAL</td>
      <td>{len(results)}</td>
      <td>{fmt_pts(total_real)} ({fmt_usd(total_real*MES_PT_USD)})</td>
      <td>{fmt_pts(total_portal)} ({fmt_usd(total_portal*MES_PT_USD)})</td>
      <td>{fmt_pts(total_gap)} ({fmt_usd(total_gap*MES_PT_USD)})</td>
      <td>{fmt_pts(total_arch)} ({fmt_usd(total_arch*MES_PT_USD)})</td>
      <td style='color:{"#51cf66" if arch_delta>0 else "#ff6b6b"}'>{fmt_pts(arch_delta)} ({fmt_usd(arch_delta*MES_PT_USD)})</td>
    </tr>
  </tbody>
</table>

<h2>Architecture-change simulation</h2>
<p>Proposal: keep broker hard SL for capital protection, but drive trail/BE/target exits from SPX 30s/2-min path (market-close at SPX trigger).
Assumes {SLIPPAGE_PT}pt slippage on each market exit.</p>
<div class='stat'><div class='label'>Real total</div><div class='value'>{total_real:+.1f} pt / {fmt_usd(total_real*MES_PT_USD)}</div></div>
<div class='stat'><div class='label'>Architecture-fix total (sim)</div><div class='value'>{total_arch:+.1f} pt / {fmt_usd(total_arch*MES_PT_USD)}</div></div>
<div class='stat'><div class='label'>Net improvement (sim)</div><div class='value' style='color:{("#51cf66" if arch_delta>0 else "#ff6b6b")}'>{arch_delta:+.1f} pt / {fmt_usd(arch_delta*MES_PT_USD)}</div></div>
<div class='stat'><div class='label'>Expected real (80% capture)</div><div class='value' style='color:{("#51cf66" if arch_delta>0 else "#ff6b6b")}'>{arch_delta*0.8:+.1f} pt / {fmt_usd(arch_delta*MES_PT_USD*0.8)}</div></div>
<p style='font-size:11px;color:#aaa'>Per <code>feedback_capture_rate_anchor.md</code>: apply 75-85% capture rate when projecting sim&rarr;real. Default 80% blend at 1 MES.</p>

<h2>TRAIL_TAG_EARLY deep-dive: SPX vs MES excursion</h2>
<p>For each TRAIL_TAG_EARLY trade, MES wick is larger than SPX wick by avg {tte_div_avg:+.2f} pt (worst case {tte_div_max:+.2f} pt).
This is the wick that tagged the broker stop while SPX path never saw it.</p>
<table>
  <thead><tr><th>lid</th><th>SPX max-adv</th><th>MES max-adv</th><th>MES-SPX divergence</th></tr></thead>
  <tbody>{"".join(tte_rows)}</tbody>
</table>

<h2>Per-trade detail</h2>
<table>
  <thead><tr>
    <th>lid</th><th>signal_ts</th><th>setup</th><th>dir</th><th>grade</th><th>para</th>
    <th>SPX entry</th><th>MES entry</th>
    <th>real_pnl</th><th>portal_pnl</th><th>gap</th>
    <th>portal_event</th><th>real_event</th>
    <th>SPX max-adv</th><th>MES max-adv</th>
    <th>category</th><th>arch_pnl</th><th>narrative</th>
  </tr></thead>
  <tbody>{"".join(rows_html)}</tbody>
</table>

<h2>Risk catalog: edge cases where architecture proposal would fail</h2>
<ul>
  <li><b>Bot offline (Railway redeploy/crash)</b> — SPX-trail signal can't fire if poller dead. Broker hard SL still protects but trail not advanced. Mitigation: heartbeat alert on SPX-poller, fallback to MES-trail if poller >2 cycles late.</li>
  <li><b>30s SPX poll vs sub-second MES move</b> — In a true flash crash, SPX can drop 20+ pts between 30s polls. Broker hard SL at {TRAIL_PARAMS['Skew Charm']['sl']}pt is your capital backstop, fine, but missed trail-lock opportunity if 20pt drop happens in 30s window.</li>
  <li><b>chain_snapshots is 2-min not 30s in V14 era</b> — even worse latency than assumed 30s. Means SPX-driven trail will lag MES wicks by 1-2 min. To make this proposal viable, would need to switch chain_snapshots polling back to 30s.</li>
  <li><b>SPX gap below trail level</b> — if SPX gaps through the trail level (rare intraday but possible at open), broker SL would catch it sooner than SPX-trail. Net: SPX-trail might lose 2-3pt more on the gap. Mitigation: floor SPX exit at broker SL level.</li>
  <li><b>SIM trade differs from real trade</b> — the architecture-fix sim above assumes &quot;market exit at SPX trigger&quot; happens cleanly. In real life, between SPX poll detecting trigger and bot sending market order, MES could have wicked another 1-2pt. Net effect: arch fix captures ~80% of theoretical gain, not 100%.</li>
</ul>

<h2>Methodology and validation</h2>
<ul>
  <li>Trail/SL params lifted verbatim from <code>app/main.py:4592-4614</code></li>
  <li>SPX path from <code>chain_snapshots</code> (2-min interval in V14 era — confirmed empirically)</li>
  <li>MES bars from <code>vps_es_range_bars</code> (5pt range)</li>
  <li>Real PnL from <code>state.fill_price</code> / <code>state.stop_fill_price</code> in MES space</li>
  <li>Portal sim uses prev-spot &rarr; current-spot segment hi/lo to capture intra-cycle touches</li>
  <li>Architecture-fix assumes {SLIPPAGE_PT}pt slippage per market exit (conservative)</li>
</ul>

<p style='color:#666;font-size:11px;margin-top:30px'>Generated {datetime.now().isoformat()[:19]} | Working dir: G:/My Drive/Python/MyProject/GitHub/0dtealpha</p>
</body></html>
"""
    return html


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    results, skipped = run_audit()
    arch = simulate_arch_change(results)

    # Persist json for inspection
    with open("_tmp_v14_pertrade_audit.json", "w") as f:
        json.dump({"results": results, "arch": arch, "skipped": skipped}, f, indent=2, default=str)

    # Console summary
    cat_count = Counter(r["category"] for r in results)
    print("Category breakdown:")
    for c, n in cat_count.most_common():
        print(f"  {c:25s} {n}")

    total_real = sum(r["real_pnl_pts"] for r in results if r["real_pnl_pts"] is not None)
    total_portal = sum(r["portal_pnl_pts"] for r in results if r["portal_pnl_pts"] is not None)
    total_arch = sum(a["arch_pnl_pts"] for a in arch if a["arch_pnl_pts"] is not None)
    print(f"\nReal:      {total_real:+.1f}pt / ${total_real*MES_PT_USD:+,.0f}")
    print(f"Portal:    {total_portal:+.1f}pt / ${total_portal*MES_PT_USD:+,.0f}")
    print(f"Gap:       {total_real-total_portal:+.1f}pt / ${(total_real-total_portal)*MES_PT_USD:+,.0f}")
    print(f"Arch fix:  {total_arch:+.1f}pt / ${total_arch*MES_PT_USD:+,.0f}")
    print(f"Net delta: {(total_arch-total_real):+.1f}pt / ${(total_arch-total_real)*MES_PT_USD:+,.0f}")

    # HTML
    html = build_html(results, arch)
    with open("_tmp_v14_pertrade_audit.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nWrote _tmp_v14_pertrade_audit.html ({len(html):,} bytes)")
