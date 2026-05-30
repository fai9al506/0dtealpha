"""
Dip-Buy Long detector — PORTAL/LOG-ONLY (NOT TSRT, NOT eval).

Discord-pro-inspired momentum dip-buy (May 2026 study). Self-contained module:
detects trigger from live SPX spot, logs to setup_log with a grade, tracks the
outcome itself, and updates setup_log. Zero coupling to real_trader / eval_trader /
auto_trader. No Telegram.

Trigger (one trade/day, 9:30-11:30 ET entry window):
  1. Track session high from the 9:30 open.
  2. Dip: spot falls >= DIP_PTS (8) below the session high.
  3. Confirm: spot bounces >= CONFIRM_PTS (4) off the dip low ("bottom wick that holds").
  4. Enter long at the confirmation spot. Exit +TARGET (10) / -STOP (8) / EOD.

Grading (hypothesis to validate forward — NOT yet proven robust, see SESSION_LOG
2026-05-30): factors logged per trade so we can later check which actually predicts.
  - prior_close_ok : entry >= prior-day close - 2pt  (in-sample edge, Mar-May)
  - vx_diverge_ok  : VX made no new high during the dip (tick VX, best-effort)
  A+ = both, A = one, B = neither.
"""
import json
import traceback
from datetime import time as dtime, datetime
from zoneinfo import ZoneInfo
from sqlalchemy import text

ET = ZoneInfo("America/New_York")
SETUP_NAME = "Dip-Buy Long"

# params (from 2026-05-30 backtest)
WIN_START = dtime(9, 30)
WIN_END = dtime(11, 30)
EXIT_CUTOFF = dtime(16, 0)
DIP_PTS = 8.0
CONFIRM_PTS = 4.0
TARGET = 10.0
STOP = 8.0

_engine = None
# per-day intraday state
_state = {
    "date": None,
    "sess_high": None,
    "sess_high_ts": None,
    "in_dip": False,
    "local_low": None,
    "local_low_ts": None,
    "fired": False,
}
_open_trades = []   # list of dicts for outcome tracking
_prev_close_cache = {}   # et_date -> prior-day close


def init(engine):
    global _engine
    _engine = engine
    try:
        _hydrate_open_trades()
    except Exception:
        print(f"[dipbuy] hydrate failed: {traceback.format_exc()}", flush=True)
    print("[dipbuy] initialized (portal/log-only)", flush=True)


def _today_et():
    return datetime.now(ET).date()


def _reset_day(d):
    _state.update(date=d, sess_high=None, sess_high_ts=None,
                  in_dip=False, local_low=None, local_low_ts=None, fired=False)


def _prev_close(d):
    if d in _prev_close_cache:
        return _prev_close_cache[d]
    if not _engine:
        return None
    try:
        with _engine.begin() as conn:
            row = conn.execute(text("""
                SELECT spot FROM chain_snapshots
                WHERE ts::date < :d AND spot IS NOT NULL
                ORDER BY ts DESC LIMIT 1
            """), {"d": d.isoformat()}).fetchone()
        pc = float(row[0]) if row else None
    except Exception:
        pc = None
    _prev_close_cache[d] = pc
    return pc


def _vx_no_new_high(hi_ts, entry_ts):
    """Best-effort VX divergence: did tick VX make NO meaningful new high during the dip?
    Returns True (diverge), False (confirmed), or None (no data)."""
    if not _engine or hi_ts is None:
        return None
    try:
        with _engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT price FROM vps_vix_ticks
                WHERE ts BETWEEN :t0 AND :t1 ORDER BY ts
            """), {"t0": hi_ts, "t1": entry_ts}).fetchall()
        if not rows or len(rows) < 3:
            return None
        prices = [float(r[0]) for r in rows]
        return (max(prices) - prices[0]) <= 0.10
    except Exception:
        return None


def _hydrate_open_trades():
    """On restart: reload today's unresolved Dip-Buy rows + mark fired if any today."""
    if not _engine:
        return
    d = _today_et()
    with _engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, ts, spot, outcome_target_level, outcome_stop_level
            FROM setup_log
            WHERE setup_name = :n AND ts::date = :d
            ORDER BY ts
        """), {"n": SETUP_NAME, "d": d.isoformat()}).fetchall()
        for r in rows:
            _state["fired"] = True   # already fired today, don't re-enter
            unresolved = conn.execute(text(
                "SELECT outcome_result FROM setup_log WHERE id=:i"), {"i": r[0]}).fetchone()
            if unresolved and unresolved[0]:
                continue
            entry = float(r[2]) if r[2] is not None else None
            if entry is None:
                continue
            _open_trades.append({
                "id": r[0], "entry": entry, "entry_ts": r[1],
                "target": entry + TARGET, "stop": entry - STOP,
                "max_fav": 0.0, "max_adv": 0.0,
            })
    if _state["fired"]:
        _state["date"] = d
        print(f"[dipbuy] hydrated: fired today + {len(_open_trades)} open trade(s)", flush=True)


def _grade(prior_close_ok, vx_div_ok):
    n = sum(1 for x in (prior_close_ok, vx_div_ok) if x)
    if prior_close_ok and vx_div_ok:
        return "A+", 90.0
    if n == 1:
        return "A", 70.0
    return "B", 50.0


def on_cycle(ts_utc, spot, vix=None):
    """Called every SPX cycle with current spot. Detects trigger + tracks outcomes.
    Fully wrapped — never raises into the caller."""
    if _engine is None or not spot:
        return
    try:
        et = ts_utc.astimezone(ET) if ts_utc.tzinfo else ts_utc.replace(tzinfo=ET)
        d = et.date()
        if _state["date"] != d:
            _reset_day(d)
        _track_outcomes(et, float(spot))
        _detect(et, float(spot), vix)
    except Exception:
        print(f"[dipbuy] on_cycle error: {traceback.format_exc()}", flush=True)


def _detect(et, spot, vix):
    t = et.time()
    if t < WIN_START or t > WIN_END or _state["fired"]:
        return
    # track session high
    if _state["sess_high"] is None or spot > _state["sess_high"]:
        _state["sess_high"] = spot
        _state["sess_high_ts"] = et
    if not _state["in_dip"]:
        if spot <= _state["sess_high"] - DIP_PTS:
            _state["in_dip"] = True
            _state["local_low"] = spot
            _state["local_low_ts"] = et
    else:
        if spot < _state["local_low"]:
            _state["local_low"] = spot
            _state["local_low_ts"] = et
        elif spot >= _state["local_low"] + CONFIRM_PTS:
            _fire(et, spot, vix)


def _fire(et, entry, vix):
    _state["fired"] = True
    pc = _prev_close(et.date())
    prior_close_ok = (pc is not None) and (entry >= pc - 2.0)
    # VX divergence over the dip window [session-high time -> entry time]
    hi_ts = _state["sess_high_ts"]
    vx_div_ok = _vx_no_new_high(hi_ts.astimezone(ET) if hi_ts else None, et)
    grade, score = _grade(prior_close_ok, bool(vx_div_ok))
    dip_depth = (_state["sess_high"] - _state["local_low"]) if _state["local_low"] else None
    mins = (et.hour * 60 + et.minute) - (9 * 60 + 30)
    factors = {
        "prior_close": round(pc, 1) if pc is not None else None,
        "vs_prior_close": round(entry - pc, 1) if pc is not None else None,
        "prior_close_ok": prior_close_ok,
        "vx_diverge_ok": vx_div_ok,
        "dip_depth": round(dip_depth, 1) if dip_depth else None,
        "mins_from_open": mins,
        "dip_pts": DIP_PTS, "confirm_pts": CONFIRM_PTS,
    }
    comments = (f"Dip-buy: entry {entry:.1f}, dip {dip_depth:.0f}pt, "
                f"{'>' if prior_close_ok else '<'}prevclose, "
                f"VXdiv={vx_div_ok}, grade {grade}")
    try:
        with _engine.begin() as conn:
            res = conn.execute(text("""
                INSERT INTO setup_log
                    (setup_name, direction, grade, score, spot, target,
                     vix, first_hour, notified, comments, abs_details,
                     outcome_target_level, outcome_stop_level)
                VALUES
                    (:n, 'long', :g, :s, :spot, :tgt,
                     :vix, :fh, FALSE, :c, :ad,
                     :tl, :sl)
                RETURNING id
            """), {
                "n": SETUP_NAME, "g": grade, "s": score, "spot": entry,
                "tgt": entry + TARGET, "vix": vix, "fh": (mins < 30),
                "c": comments, "ad": json.dumps(factors),
                "tl": entry + TARGET, "sl": entry - STOP,
            })
            log_id = res.fetchone()[0]
        _open_trades.append({
            "id": log_id, "entry": entry, "entry_ts": et,
            "target": entry + TARGET, "stop": entry - STOP,
            "max_fav": 0.0, "max_adv": 0.0,
        })
        print(f"[dipbuy] FIRED id={log_id} entry={entry:.1f} grade={grade} "
              f"prevclose_ok={prior_close_ok} vxdiv={vx_div_ok}", flush=True)
    except Exception:
        print(f"[dipbuy] insert failed: {traceback.format_exc()}", flush=True)


def _track_outcomes(et, spot):
    if not _open_trades:
        return
    still_open = []
    for tr in _open_trades:
        fav = spot - tr["entry"]
        tr["max_fav"] = max(tr["max_fav"], fav)
        tr["max_adv"] = min(tr["max_adv"], fav)
        result = None
        pnl = None
        if spot <= tr["stop"]:
            result, pnl, first_event = "LOSS", -STOP, "stop"
        elif spot >= tr["target"]:
            result, pnl, first_event = "WIN", TARGET, "target"
        elif et.time() >= EXIT_CUTOFF:
            result, pnl, first_event = ("EXPIRED", round(spot - tr["entry"], 2), "eod")
        if result is None:
            still_open.append(tr)
            continue
        elapsed = int((et - (tr["entry_ts"].astimezone(ET) if tr["entry_ts"].tzinfo else tr["entry_ts"])).total_seconds() / 60)
        try:
            with _engine.begin() as conn:
                conn.execute(text("""
                    UPDATE setup_log SET
                        outcome_result = :r, outcome_pnl = :p,
                        outcome_max_profit = :mp, outcome_max_loss = :ml,
                        outcome_first_event = :fe, outcome_elapsed_min = :em
                    WHERE id = :i
                """), {"r": result, "p": pnl, "mp": round(tr["max_fav"], 2),
                       "ml": round(tr["max_adv"], 2), "fe": first_event,
                       "em": elapsed, "i": tr["id"]})
            print(f"[dipbuy] RESOLVED id={tr['id']} {result} {pnl:+.1f}pt", flush=True)
        except Exception:
            print(f"[dipbuy] outcome update failed: {traceback.format_exc()}", flush=True)
            still_open.append(tr)
    _open_trades[:] = still_open


def status():
    return {"date": str(_state["date"]), "fired_today": _state["fired"],
            "session_high": _state["sess_high"], "in_dip": _state["in_dip"],
            "open_trades": len(_open_trades)}
