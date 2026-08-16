"""S217 Basket gate (Scheme B) — added 2026-06-13.

Skip real TSRT trades that the tech basket (NVDA/AMD/AVGO/META/MSFT/GOOGL)
CONTRADICTS. Validated Mar–Jun 2026: trades the basket confirms win 72%, ones it
contradicts win 54%. "0/0/1" policy = take only basket-CONFIRMED trades (plus
fail-open when basket data is missing/stale). Neutral + contradicted are skipped.

Self-contained and FAIL-SOFT: any error / missing data / stale capture →
state="no_data" → block=False (the trade is TAKEN). The gate can never *create*
a loss — its only effect is to skip trades, and on any uncertainty it defers to
current behavior (place the trade).

Env switch:  BASKET_GATE_ENABLED  (default "false" — must be flipped true to arm)

Reads the latest row of `semi_basket` (et PK = ET-naive wall time, basket_pct =
equal-weight %-from-session-open, n_names = how many of the 6 captured).
"""
from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

# NOTE: do NOT import psycopg2 at module level — this project ships psycopg v3
# (postgresql+psycopg), NOT psycopg2. A hard top-level `import psycopg2` threw
# ModuleNotFoundError in-container, so `from app import basket_gate` failed and
# place_trade's try/except fail-opened EVERY trade (gate never ran, Jun 13–15).
# The engine path below is what runs in production; psycopg2 is a lazy, optional
# last-resort fallback only.

NY = ZoneInfo("America/New_York")

DEADBAND_PCT = 0.15   # |basket %-from-open| below this = neutral (not a clear signal)
FRESH_MINUTES = 10    # latest semi_basket row must be <= this old, else stale -> no_data
MIN_NAMES = 4         # need >= this many of the 6 basket names captured to trust it


def _enabled() -> bool:
    return os.getenv("BASKET_GATE_ENABLED", "false").lower() == "true"


def classify(basket_pct: float, direction: str) -> str:
    """Pure classifier: confirm / neutral / contradict. (No I/O — unit-testable.)"""
    is_long = direction.lower() in ("long", "bullish")
    if abs(basket_pct) < DEADBAND_PCT:
        return "neutral"
    basket_up = basket_pct > 0
    return "confirm" if (basket_up == is_long) else "contradict"


def _latest_basket(engine=None):
    """Return (et_naive, basket_pct, n_names) of the newest semi_basket row, or None.
    Prefers the app's shared SQLAlchemy engine (proven to work in-container); falls back
    to a raw psycopg2 connection. Raises on DB error (caller treats as fail-open)."""
    sql = "SELECT et, basket_pct, n_names FROM semi_basket ORDER BY et DESC LIMIT 1"
    if engine is not None:
        from sqlalchemy import text
        with engine.connect() as conn:
            row = conn.execute(text(sql)).fetchone()
            return tuple(row) if row is not None else None
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return None
    try:
        import psycopg2  # lazy: not installed in prod (psycopg v3 only) — engine path is primary
    except ModuleNotFoundError:
        import psycopg as psycopg2  # fall back to psycopg v3, same DB-API
    conn = psycopg2.connect(dsn, connect_timeout=4)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchone()
    finally:
        conn.close()


def _sizing_mode() -> str:
    """'012' (default 0/1/2) re-admits neutral; '001' = legacy 0/0/1 skip-neutral.
    Lockstep with main.py _passes_live_filter + live_filter.basket_blocks (see
    feedback_filter_three_copies_lockstep)."""
    return os.getenv("BASKET_SIZING_MODE", "012").lower()


def evaluate(direction: str, engine=None) -> dict:
    """Decide whether the basket gate should BLOCK this real trade.

    Returns {state, basket_pct, n_names, block, enabled, reason}.
      state  ∈ confirm | neutral | contradict | no_data
      block  = True ONLY when enabled AND state would skip under the active sizing mode:
        - mode "001" (legacy 0/0/1): block neutral AND contradict
        - mode "012" (default 0/1/2): block ONLY contradict (neutral re-admitted, sized 1x)
    Fail-open on EVERY uncertainty (no env / no data / stale / few names / error).
    NOTE: as of 2026-06-16 this gate is RETIRED from the real_trader trade path (the basket
    decision lives in _passes_live_filter via the stamped setup_log.basket_pct). Kept for the
    /api ad-hoc view + classify() reuse by real_trader._effective_qty.
    Pass the app's shared SQLAlchemy engine to avoid a raw psycopg2 connect (which
    fail-opened silently in-container — S218, 2026-06-15). Logs every fail-open.
    """
    enabled = _enabled()
    out = {"state": "no_data", "basket_pct": None, "n_names": None,
           "block": False, "enabled": enabled, "reason": ""}
    try:
        row = _latest_basket(engine)
        if not row or row[1] is None:
            out["reason"] = "no_row"
            print(f"[basket_gate] fail-open ({direction}): no_row", flush=True)
            return out
        et, basket_pct, n_names = row[0], float(row[1]), (int(row[2]) if row[2] is not None else 0)
        out["basket_pct"] = basket_pct
        out["n_names"] = n_names

        # staleness: et is ET-naive wall time
        now_naive = datetime.now(NY).replace(tzinfo=None)
        age_min = (now_naive - et).total_seconds() / 60.0
        if age_min > FRESH_MINUTES:
            out["reason"] = f"stale_{age_min:.0f}m"
            print(f"[basket_gate] fail-open ({direction}): stale {age_min:.0f}m", flush=True)
            return out  # fail-open
        if n_names < MIN_NAMES:
            out["reason"] = f"few_names_{n_names}"
            print(f"[basket_gate] fail-open ({direction}): few_names {n_names}", flush=True)
            return out  # fail-open

        state = classify(basket_pct, direction)
        out["state"] = state
        out["reason"] = "ok"
        # Block set depends on sizing mode: '001' skips neutral+contradict, '012' skips only contradict.
        _block_states = ("contradict",) if _sizing_mode() == "012" else ("neutral", "contradict")
        if enabled and state in _block_states:
            out["block"] = True
        print(f"[basket_gate] {direction} basket={basket_pct:+.2f}% -> {state} "
              f"block={out['block']} (enabled={enabled})", flush=True)
        return out
    except Exception as e:  # ANY failure → fail-open (take the trade)
        out["reason"] = f"error:{type(e).__name__}"
        print(f"[basket_gate] FAIL-OPEN ({direction}): {type(e).__name__}: {e}", flush=True)
        return out
