# Auto-Trader: MES Futures SIM execution module
# Self-contained — receives engine, token fn, and telegram fn via init()
# Hardcoded to SIM API — cannot hit live.
#
# Default: 1 MES (SIM overnight margin ~$2,735/contract, $5K account).
# Scale via env vars MES_TOTAL_QTY, MES_T1_QTY, MES_T2_QTY.
# Uses individual orders (not bracket groups — TS v3 BRK doesn't work for futures exits).
#   Flow A (BofA/Absorption/Paradigm): entry + stop + single limit @ +10pts
#   Flow B (GEX/AG/DD): entry + stop + T1 @ +10pts + T2 @ full target (DD: trail-only)

import os, json, math, time, calendar, requests
from datetime import datetime, date, timedelta
from threading import Lock

# ====== MES CONTRACT AUTO-ROLLOVER ======
_MES_MONTHS = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]

def _third_friday(year: int, month: int) -> date:
    c = calendar.Calendar(firstweekday=calendar.MONDAY)
    fridays = [d for d in c.itermonthdates(year, month) if d.month == month and d.weekday() == 4]
    return fridays[2]

def _auto_mes_symbol() -> str:
    """Return front-month MES symbol for TradeStation (e.g. MESH26), rolling ~8 days before expiry."""
    today = date.today()
    for month_num, code in _MES_MONTHS:
        expiry = _third_friday(today.year, month_num)
        if today <= expiry - timedelta(days=8):
            return f"MES{code}{today.year % 100}"
    return f"MESH{(today.year + 1) % 100}"

# ====== CONFIG ======
SIM_BASE = "https://sim-api.tradestation.com/v3"
SIM_ACCOUNT_ID = "SIM2609239F"
_es_env = os.getenv("ES_TRADE_SYMBOL", "auto")
MES_SYMBOL = _auto_mes_symbol() if _es_env.lower() == "auto" else _es_env
AUTO_TRADE_ENABLED = os.getenv("AUTO_TRADE_ENABLED", "false").lower() == "true"

# SIM: $2,735 overnight margin per MES (no API intraday discount)
# With $5K account: max 1 MES. Adjust via env vars for larger accounts.
TOTAL_QTY = int(os.getenv("MES_TOTAL_QTY", "1"))
T1_QTY = int(os.getenv("MES_T1_QTY", "1"))
T2_QTY = int(os.getenv("MES_T2_QTY", "0"))   # 0 = no T2 with 1 contract
FIRST_TARGET_PTS = 10.0  # T1 target for all setups
MES_TICK_SIZE = 0.25     # MES minimum price increment
MES_POINT_VALUE = 5.0    # $5 per point per MES contract
COMMISSION_PER_SIDE = 0.50  # $0.50 per contract per side (TS standard micro)


def _round_mes(price: float) -> float:
    """Round price to nearest MES tick (0.25)."""
    return round(round(price / MES_TICK_SIZE) * MES_TICK_SIZE, 2)


def _order_ok(resp: dict | None) -> tuple[bool, str | None]:
    """Check if an order response succeeded. Returns (ok, order_id).
    TS returns HTTP 200 even for FAILED orders — must check order-level Error."""
    if not resp:
        return False, None
    orders = resp.get("Orders", [])
    if not orders:
        return False, None
    first = orders[0]
    if first.get("Error") == "FAILED":
        msg = first.get("Message", "unknown error")
        print(f"[auto-trader] order FAILED: {msg}", flush=True)
        return False, first.get("OrderID")
    oid = first.get("OrderID")
    return bool(oid), oid


# ====== STATE ======
_engine = None
_get_token = None       # callable -> str (access token)
_send_telegram = None   # callable(msg) -> bool
_lock = Lock()
_active_orders: dict[int, dict] = {}  # keyed by setup_log_id

# Per-setup toggles — all default ON for SIM testing
_toggles: dict[str, bool] = {
    "GEX Long": True,
    "AG Short": True,
    "BofA Scalp": True,
    "ES Absorption": True,
    "Paradigm Reversal": True,
    "DD Exhaustion": True,
    "Skew Charm": True,
}

# Setup → order flow mapping
_SINGLE_TARGET_SETUPS = {"BofA Scalp", "Paradigm Reversal"}
_SPLIT_TARGET_SETUPS = {"GEX Long", "AG Short", "DD Exhaustion", "Skew Charm"}


def init(engine, get_token_fn, send_telegram_fn):
    """Initialize auto-trader. Called once at startup."""
    global _engine, _get_token, _send_telegram
    _engine = engine
    _get_token = get_token_fn
    _send_telegram = send_telegram_fn
    _load_active_orders()
    n = len(_active_orders)
    print(f"[auto-trader] init: enabled={AUTO_TRADE_ENABLED} symbol={MES_SYMBOL} "
          f"account={SIM_ACCOUNT_ID} active_orders={n}", flush=True)
    for name, on in _toggles.items():
        if on:
            print(f"[auto-trader]   {name}: ON", flush=True)
    # Diagnostic: verify SIM account access
    if AUTO_TRADE_ENABLED and _get_token:
        try:
            acct = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}", None)
            if acct:
                print(f"[auto-trader] account info: {json.dumps(acct, default=str)[:300]}", flush=True)
            else:
                print("[auto-trader] WARNING: cannot access SIM account", flush=True)
        except Exception as e:
            print(f"[auto-trader] account check error: {e}", flush=True)

        # Startup orphan check: detect and close any positions from previous session
        try:
            _close_broker_orphans(source="STARTUP")
        except Exception as e:
            print(f"[auto-trader] startup orphan check error: {e}", flush=True)


# ====== MAIN ENTRY POINT ======

def place_trade(setup_log_id: int, setup_name: str, direction: str,
                es_price: float, target_pts: float | None, stop_pts: float,
                full_target_pts: float | None = None,
                limit_entry_price: float | None = None):
    """Place MES SIM trade when a setup fires.

    Args:
        setup_log_id: DB id from setup_log table
        setup_name: e.g. "BofA Scalp", "GEX Long"
        direction: "Long"/"Bullish" or "Short"/"Bearish"
        es_price: current ES/MES price from quote stream
        target_pts: distance in points to first target (None for trailing setups)
        stop_pts: distance in points to stop
        full_target_pts: distance to Volland full target for T2. None = same as target_pts.
        limit_entry_price: MES limit entry price (charm S/R). None = market order.
    """
    if not AUTO_TRADE_ENABLED:
        print(f"[auto-trader] skip {setup_name}: master switch OFF", flush=True)
        return
    if not _toggles.get(setup_name, False):
        print(f"[auto-trader] skip {setup_name}: toggle OFF", flush=True)
        return
    if not setup_log_id:
        print(f"[auto-trader] skip {setup_name}: no setup_log_id", flush=True)
        return

    with _lock:
        if setup_log_id in _active_orders:
            print(f"[auto-trader] skip {setup_name} id={setup_log_id}: already active", flush=True)
            return
        # DEDUP: block if same setup_name+direction placed within last 90s (deploy overlap guard)
        from datetime import timezone as _utc
        _now = datetime.now(_utc.utc)
        for _lid, _o in _active_orders.items():
            if (_o.get("setup_name") == setup_name and
                _o.get("direction", "").lower() == direction.lower()):
                _placed = _o.get("ts_placed", "")
                if _placed:
                    try:
                        _placed_dt = datetime.fromisoformat(_placed)
                        if _placed_dt.tzinfo is None:
                            _placed_dt = _placed_dt.replace(tzinfo=_utc.utc)
                        if (_now - _placed_dt).total_seconds() < 90:
                            print(f"[auto-trader] DEDUP {setup_name} id={setup_log_id}: "
                                  f"same setup placed {(_now - _placed_dt).total_seconds():.0f}s ago "
                                  f"(id={_lid})", flush=True)
                            return
                    except (ValueError, TypeError):
                        pass

    is_long = direction.lower() in ("long", "bullish")

    # Same-direction stacking: allow multiple positions in same direction, block opposite
    with _lock:
        active_filled = [(lid, o) for lid, o in _active_orders.items()
                         if o["status"] in ("pending_entry", "pending_limit", "filled")]
    if active_filled:
        has_opposite = any(
            (is_long and o["direction"].lower() not in ("long", "bullish")) or
            (not is_long and o["direction"].lower() in ("long", "bullish"))
            for _, o in active_filled
        )
        if has_opposite:
            active_names = ", ".join(f"{o['setup_name']}#{lid}" for lid, o in active_filled)
            print(f"[auto-trader] skip {setup_name}: opposite direction active, "
                  f"existing: {active_names}", flush=True)
            return
        # Same direction — stack it
        active_names = ", ".join(f"{o['setup_name']}#{lid}" for lid, o in active_filled)
        print(f"[auto-trader] STACKING {setup_name} (same dir), "
              f"active: {active_names}", flush=True)

    # ── Margin/buying-power pre-check ──
    # Prevents spam rejections when account can't afford new positions
    bp = _get_buying_power()
    if bp is not None:
        # MES margin requirement: ~$2,737/contract, TOTAL_QTY contracts
        margin_needed = TOTAL_QTY * 2737
        if bp < margin_needed:
            print(f"[auto-trader] skip {setup_name}: insufficient buying power "
                  f"(${bp:,.0f} < ${margin_needed:,.0f} needed for {TOTAL_QTY} MES)",
                  flush=True)
            return

    # Charm S/R limit entry: use deferred two-phase flow
    if limit_entry_price is not None:
        _place_limit_entry(setup_log_id, setup_name, direction, is_long,
                           es_price, stop_pts, target_pts, full_target_pts,
                           limit_entry_price)
        return

    # Determine order flow
    if setup_name in _SINGLE_TARGET_SETUPS:
        # Flow A: single target, all 10 contracts
        _place_single_target(setup_log_id, setup_name, direction, is_long,
                             es_price, stop_pts)
    else:
        # Flow B: split target (T1 @ +10, T2 @ full target or trail-only)
        _place_split_target(setup_log_id, setup_name, direction, is_long,
                            es_price, stop_pts, full_target_pts)


# ====== ORDER PLACEMENT ======

_LIMIT_ENTRY_TIMEOUT_S = 1800  # 30 min timeout for limit entries


def _place_limit_entry(setup_log_id, setup_name, direction, is_long,
                       es_price, stop_pts, target_pts, full_target_pts,
                       limit_entry_price):
    """Charm S/R: place LIMIT entry only. Stop/target placed after fill (Phase 2)."""
    side = "Buy" if is_long else "Sell"
    limit_price = _round_mes(limit_entry_price)

    entry_payload = {
        "AccountID": SIM_ACCOUNT_ID, "Symbol": MES_SYMBOL,
        "Quantity": str(TOTAL_QTY), "OrderType": "Limit",
        "LimitPrice": str(limit_price),
        "TradeAction": side, "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    resp = _sim_api("POST", "/orderexecution/orders", entry_payload)
    ok, entry_oid = _order_ok(resp)
    if not ok:
        _alert(f"[AUTO-TRADE] FAILED limit entry for {setup_name}\n"
               f"Side: {side} {TOTAL_QTY} {MES_SYMBOL} LIMIT @ {limit_price:.2f}")
        return

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "entry_order_id": entry_oid,
        "t1_order_id": None,
        "t2_order_id": None,
        "stop_order_id": None,
        "stop_qty": TOTAL_QTY,
        "t1_qty": 0,
        "t2_qty": 0,
        "current_stop": None,
        "first_target_price": None,
        "full_target_price": None,
        "status": "pending_limit",
        "t1_filled": False,
        "t2_filled": False,
        "fill_price": None,
        "ts_placed": datetime.utcnow().isoformat(),
        "limit_entry_price": limit_price,
        "limit_placed_at": datetime.utcnow().isoformat(),
        "deferred_stop_pts": stop_pts,
        "deferred_target_pts": target_pts,
        "deferred_full_target_pts": full_target_pts,
        "deferred_es_price": es_price,
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    print(f"[auto-trader] LIMIT placed: {setup_name} {side} {TOTAL_QTY} {MES_SYMBOL} "
          f"LIMIT @ {limit_price:.2f} (market was {es_price:.2f}) "
          f"id={entry_oid}", flush=True)
    _alert(f"[AUTO-TRADE] {setup_name} LIMIT entry\n"
           f"Side: {side} | {TOTAL_QTY} {MES_SYMBOL} LIMIT @ {limit_price:.2f}\n"
           f"[CHARM S/R] Waiting for fill (market @ {es_price:.2f})")


def _place_deferred_protective_orders(lid, order, fill_price):
    """Phase 2: place stop + target orders after limit entry fills."""
    is_long = order["direction"].lower() in ("long", "bullish")
    exit_side = "Sell" if is_long else "Buy"
    stop_pts = order["deferred_stop_pts"]
    target_pts = order.get("deferred_target_pts")
    full_target_pts = order.get("deferred_full_target_pts")
    setup_name = order["setup_name"]

    if is_long:
        es_stop = _round_mes(fill_price - stop_pts)
        t1_price = _round_mes(fill_price + FIRST_TARGET_PTS)
        t2_price = _round_mes(fill_price + full_target_pts) if full_target_pts else None
    else:
        es_stop = _round_mes(fill_price + stop_pts)
        t1_price = _round_mes(fill_price - FIRST_TARGET_PTS)
        t2_price = _round_mes(fill_price - full_target_pts) if full_target_pts else None

    # DD Exhaustion / AG Short: trail-only T2
    if setup_name in ("DD Exhaustion", "AG Short"):
        t2_price = None

    # Determine flow: single target (BofA, Paradigm) vs split target
    is_single = setup_name in _SINGLE_TARGET_SETUPS

    # 1. Stop order
    stop_payload = {
        "AccountID": SIM_ACCOUNT_ID, "Symbol": MES_SYMBOL,
        "Quantity": str(TOTAL_QTY), "OrderType": "StopMarket",
        "StopPrice": str(es_stop), "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
    }
    stop_resp = _sim_api("POST", "/orderexecution/orders", stop_payload)
    stop_ok, stop_oid = _order_ok(stop_resp)
    if not stop_ok:
        stop_oid = None
        _alert(f"[AUTO-TRADE] MANUAL INTERVENTION: {setup_name} limit FILLED "
               f"@ {fill_price:.2f} but STOP FAILED!")

    # 2. T1 limit order
    t1_oid = None
    t1_qty = TOTAL_QTY if is_single else T1_QTY
    if t1_qty > 0:
        t1_payload = {
            "AccountID": SIM_ACCOUNT_ID, "Symbol": MES_SYMBOL,
            "Quantity": str(t1_qty), "OrderType": "Limit",
            "LimitPrice": str(t1_price), "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
        }
        t1_resp = _sim_api("POST", "/orderexecution/orders", t1_payload)
        t1_ok, t1_oid = _order_ok(t1_resp)
        if not t1_ok:
            t1_oid = None

    # 3. T2 limit order (split target only)
    t2_oid = None
    if not is_single and t2_price is not None and T2_QTY > 0:
        t2_payload = {
            "AccountID": SIM_ACCOUNT_ID, "Symbol": MES_SYMBOL,
            "Quantity": str(T2_QTY), "OrderType": "Limit",
            "LimitPrice": str(t2_price), "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"}, "Route": "Intelligent",
        }
        t2_resp = _sim_api("POST", "/orderexecution/orders", t2_payload)
        t2_ok, t2_oid = _order_ok(t2_resp)
        if not t2_ok:
            t2_oid = None

    # Update order state
    with _lock:
        order["stop_order_id"] = stop_oid
        order["t1_order_id"] = t1_oid
        order["t2_order_id"] = t2_oid
        order["current_stop"] = es_stop
        order["first_target_price"] = t1_price
        order["full_target_price"] = t2_price
        order["t1_qty"] = t1_qty
        order["t2_qty"] = 0 if is_single else T2_QTY
    _persist_order(lid)

    imp_pts = abs(fill_price - order.get("deferred_es_price", fill_price))
    t2_str = f"T2={t2_price:.2f}" if t2_price else "T2=trail"
    print(f"[auto-trader] DEFERRED orders placed: {setup_name} "
          f"stop={es_stop:.2f} T1={t1_price:.2f} {t2_str} "
          f"(entry improved {imp_pts:.1f}pts from market)", flush=True)
    _alert(f"[AUTO-TRADE] {setup_name} LIMIT FILLED @ {fill_price:.2f}\n"
           f"[CHARM S/R] Improved {imp_pts:+.1f}pts from market "
           f"({order.get('deferred_es_price', 0):.2f})\n"
           f"Stop: {es_stop:.2f} | T1: {t1_price:.2f} | {t2_str}")


def _place_single_target(setup_log_id, setup_name, direction, is_long,
                          es_price, stop_pts):
    """Flow A: TOTAL_QTY MES with single limit target @ +10pts + stop.
    Uses individual orders (entry, then stop, then target) because TS v3
    bracket groups don't work correctly for futures exit legs."""
    side = "Buy" if is_long else "Sell"
    exit_side = "Sell" if is_long else "Buy"

    if is_long:
        es_stop = _round_mes(es_price - stop_pts)
        es_target = _round_mes(es_price + FIRST_TARGET_PTS)
    else:
        es_stop = _round_mes(es_price + stop_pts)
        es_target = _round_mes(es_price - FIRST_TARGET_PTS)

    qty = str(TOTAL_QTY)

    # 1. Market entry
    entry_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": MES_SYMBOL,
        "Quantity": qty,
        "OrderType": "Market",
        "TradeAction": side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    resp = _sim_api("POST", "/orderexecution/orders", entry_payload)
    ok, entry_oid = _order_ok(resp)
    if not ok:
        _alert(f"[AUTO-TRADE] FAILED entry for {setup_name}\n"
               f"Side: {side} {TOTAL_QTY} {MES_SYMBOL} @ {es_price:.2f}")
        return

    # 2. Stop order (exit side)
    stop_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": MES_SYMBOL,
        "Quantity": qty,
        "OrderType": "StopMarket",
        "StopPrice": str(es_stop),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    stop_resp = _sim_api("POST", "/orderexecution/orders", stop_payload)
    stop_ok, stop_oid = _order_ok(stop_resp)
    if not stop_ok:
        _alert(f"[AUTO-TRADE] MANUAL INTERVENTION: {setup_name} entry placed "
               f"(id={entry_oid}) but STOP FAILED!\n"
               f"Side: {side} MES: {es_price:.2f} Stop: {es_stop:.2f}")

    # 3. Target limit (exit side) — best-effort: may be rejected if insufficient
    #    margin (TS treats limit exit orders as new positions requiring margin).
    #    If rejected, outcome tracking handles target exits via market close.
    t1_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": MES_SYMBOL,
        "Quantity": qty,
        "OrderType": "Limit",
        "LimitPrice": str(es_target),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    t1_resp = _sim_api("POST", "/orderexecution/orders", t1_payload)
    t1_ok, t1_oid = _order_ok(t1_resp)
    if not t1_ok:
        t1_oid = None
        print(f"[auto-trader] target limit skipped (margin): {setup_name} "
              f"target={es_target:.2f} — outcome tracking will handle exit", flush=True)

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "entry_order_id": entry_oid,
        "t1_order_id": t1_oid,
        "t2_order_id": None,
        "stop_order_id": stop_oid,
        "stop_qty": TOTAL_QTY,
        "t1_qty": TOTAL_QTY if t1_oid else 0,
        "t2_qty": 0,
        "current_stop": es_stop,
        "first_target_price": es_target,
        "full_target_price": None,
        "status": "pending_entry",
        "t1_filled": False,
        "t2_filled": False,
        "fill_price": None,
        "ts_placed": datetime.utcnow().isoformat(),
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    print(f"[auto-trader] placed: {setup_name} {side} {TOTAL_QTY} {MES_SYMBOL} "
          f"@ {es_price:.2f} target={es_target:.2f} stop={es_stop:.2f} "
          f"ids=entry:{entry_oid}/stop:{stop_oid}/t1:{t1_oid}", flush=True)
    _alert(f"[AUTO-TRADE] {setup_name} placed\n"
           f"Side: {side} | {TOTAL_QTY} {MES_SYMBOL} @ {es_price:.2f}\n"
           f"Target: {es_target:.2f} | Stop: {es_stop:.2f}")


def _place_split_target(setup_log_id, setup_name, direction, is_long,
                         es_price, stop_pts, full_target_pts):
    """Flow B: TOTAL_QTY MES entry, T1@+10pts, T2@full_target (or trail-only for DD).
    Uses individual orders because TS v3 bracket groups don't work for futures exit legs."""
    side = "Buy" if is_long else "Sell"
    exit_side = "Sell" if is_long else "Buy"

    if is_long:
        es_stop = _round_mes(es_price - stop_pts)
        t1_price = _round_mes(es_price + FIRST_TARGET_PTS)
        t2_price = _round_mes(es_price + full_target_pts) if full_target_pts else None
    else:
        es_stop = _round_mes(es_price + stop_pts)
        t1_price = _round_mes(es_price - FIRST_TARGET_PTS)
        t2_price = _round_mes(es_price - full_target_pts) if full_target_pts else None

    # DD Exhaustion / AG Short: trail-only T2 (no limit order)
    is_trail_only_t2 = setup_name in ("DD Exhaustion", "AG Short")
    if is_trail_only_t2:
        t2_price = None

    # 1. Market entry
    entry_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": MES_SYMBOL,
        "Quantity": str(TOTAL_QTY),
        "OrderType": "Market",
        "TradeAction": side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    resp = _sim_api("POST", "/orderexecution/orders", entry_payload)
    ok, entry_oid = _order_ok(resp)
    if not ok:
        _alert(f"[AUTO-TRADE] FAILED entry for {setup_name}\n"
               f"Side: {side} {TOTAL_QTY} {MES_SYMBOL} @ {es_price:.2f}")
        return

    # 2. Stop order (exit side — covers full position)
    stop_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": MES_SYMBOL,
        "Quantity": str(TOTAL_QTY),
        "OrderType": "StopMarket",
        "StopPrice": str(es_stop),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    stop_resp = _sim_api("POST", "/orderexecution/orders", stop_payload)
    stop_ok, stop_oid = _order_ok(stop_resp)
    if not stop_ok:
        stop_oid = None
        _alert(f"[AUTO-TRADE] MANUAL INTERVENTION: {setup_name} entry placed "
               f"(id={entry_oid}) but STOP FAILED!\n"
               f"Side: {side} MES: {es_price:.2f} Stop: {es_stop:.2f}")

    # 3. T1 limit order — best-effort (may be rejected if insufficient margin)
    t1_oid = None
    if T1_QTY > 0:
        t1_payload = {
            "AccountID": SIM_ACCOUNT_ID,
            "Symbol": MES_SYMBOL,
            "Quantity": str(T1_QTY),
            "OrderType": "Limit",
            "LimitPrice": str(t1_price),
            "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        t1_resp = _sim_api("POST", "/orderexecution/orders", t1_payload)
        t1_ok, t1_oid = _order_ok(t1_resp)
        if not t1_ok:
            t1_oid = None
            print(f"[auto-trader] T1 limit skipped (margin): {setup_name} "
                  f"t1={t1_price:.2f} — outcome tracking will handle exit", flush=True)

    # 4. T2 limit order @ full target — skip for DD trail-only or if T2_QTY=0
    t2_oid = None
    if t2_price is not None and T2_QTY > 0:
        t2_payload = {
            "AccountID": SIM_ACCOUNT_ID,
            "Symbol": MES_SYMBOL,
            "Quantity": str(T2_QTY),
            "OrderType": "Limit",
            "LimitPrice": str(t2_price),
            "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        t2_resp = _sim_api("POST", "/orderexecution/orders", t2_payload)
        t2_ok, t2_oid = _order_ok(t2_resp)
        if not t2_ok:
            t2_oid = None

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "entry_order_id": entry_oid,
        "t1_order_id": t1_oid,
        "t2_order_id": t2_oid,
        "stop_order_id": stop_oid,
        "stop_qty": TOTAL_QTY,
        "t1_qty": T1_QTY,
        "t2_qty": T2_QTY,
        "current_stop": es_stop,
        "first_target_price": t1_price,
        "full_target_price": t2_price,
        "status": "pending_entry",
        "t1_filled": False,
        "t2_filled": False,
        "fill_price": None,
        "ts_placed": datetime.utcnow().isoformat(),
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    t2_str = f"T2={t2_price:.2f}" if t2_price else "T2=trail"
    print(f"[auto-trader] SPLIT placed: {setup_name} {side} {TOTAL_QTY} {MES_SYMBOL} "
          f"@ {es_price:.2f} T1={t1_price:.2f} {t2_str} stop={es_stop:.2f} "
          f"ids=entry:{entry_oid}/stop:{stop_oid}/t1:{t1_oid}/t2:{t2_oid}", flush=True)
    _alert(f"[AUTO-TRADE] {setup_name} SPLIT placed\n"
           f"Side: {side} | {TOTAL_QTY} {MES_SYMBOL} @ {es_price:.2f}\n"
           f"T1: {T1_QTY} @ {t1_price:.2f} | {t2_str}\n"
           f"Stop: {es_stop:.2f}")


# ====== TRAIL & CLOSE ======

def update_stop(setup_log_id: int, new_stop_price: float):
    """Update the stop order price (called when trail advances)."""
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] != "filled":
            return
        old_stop = order["current_stop"]
        # Skip trivial changes (< 0.25 pts = 1 MES tick)
        if abs(new_stop_price - old_stop) < 0.25:
            return
        stop_oid = order["stop_order_id"]
        stop_qty = order["stop_qty"]

    if not stop_oid or stop_qty <= 0:
        return

    new_stop_price = _round_mes(new_stop_price)

    # Replace the stop order via PUT (with current remaining qty)
    replace_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": MES_SYMBOL,
        "Quantity": str(stop_qty),
        "OrderType": "StopMarket",
        "StopPrice": str(new_stop_price),
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }

    resp = _sim_api("PUT", f"/orderexecution/orders/{stop_oid}", replace_payload)
    if resp:
        with _lock:
            order["current_stop"] = new_stop_price
            new_orders = resp.get("Orders", [])
            if new_orders and new_orders[0].get("OrderID"):
                order["stop_order_id"] = new_orders[0]["OrderID"]
        _persist_order(setup_log_id)
        print(f"[auto-trader] stop updated: id={setup_log_id} "
              f"{old_stop:.2f} -> {new_stop_price:.2f} qty={stop_qty}", flush=True)
    else:
        _alert(f"[AUTO-TRADE] MANUAL INTERVENTION: stop update FAILED\n"
               f"id={setup_log_id} old={old_stop:.2f} new={new_stop_price:.2f}")


def close_trade(setup_log_id: int, result_type: str):
    """Close a trade on outcome resolution.
    Always flattens because individual orders are not linked — a WIN doesn't
    auto-cancel the stop, and a LOSS doesn't auto-cancel the target."""
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] == "closed":
            return

    setup_name = order["setup_name"]

    # Always flatten: cancel remaining orders + market close if needed
    _flatten_position(order)

    with _lock:
        order["status"] = "closed"
    _persist_order(setup_log_id)
    print(f"[auto-trader] closed: {setup_name} id={setup_log_id} "
          f"result={result_type}", flush=True)

    # Stacking integrity check: verify broker position matches remaining active trades
    _verify_stacking_integrity()


def _get_order_fill_price(order_id: str) -> float | None:
    """Get fill price for a specific order by polling broker."""
    try:
        data = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}/orders", None)
        if data:
            for o in data.get("Orders", []):
                if o.get("OrderID") == order_id and o.get("Status") == "FLL":
                    return _extract_fill_price(o)
    except Exception:
        pass
    return None


def _get_buying_power() -> float | None:
    """Query account buying power (purchasing power) from broker.
    Returns dollar amount or None on error."""
    try:
        data = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}/balances", None)
        if not data:
            return None
        balances = data.get("Balances", [])
        if isinstance(balances, list) and balances:
            b = balances[0]
        elif isinstance(balances, dict):
            b = balances
        else:
            return None
        bp = b.get("BuyingPower") or b.get("CashBalance")
        return float(bp) if bp else None
    except Exception as e:
        print(f"[auto-trader] buying power query error: {e}", flush=True)
        return None


def _get_broker_position() -> dict | None:
    """Query broker for actual MES position on SIM account.
    Returns {'qty': int, 'long_short': str, 'symbol': str} or None if flat/error."""
    try:
        pos_data = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}/positions", None)
        for pos in (pos_data or {}).get("Positions", []):
            symbol = pos.get("Symbol", "")
            qty = int(pos.get("Quantity", "0"))
            if qty > 0 and "MES" in symbol.upper():
                return {
                    "qty": qty,
                    "long_short": pos.get("LongShort", ""),
                    "symbol": symbol,
                }
    except Exception as e:
        print(f"[auto-trader] broker position query error: {e}", flush=True)
    return None


def _flatten_position(order):
    """Market close remaining contracts + cancel all pending orders.
    Checks broker position FIRST to prevent ghost positions from race conditions."""
    is_long = order["direction"].lower() in ("long", "bullish")
    close_side = "Sell" if is_long else "Buy"  # Futures: Buy/Sell only

    # Cancel pending stop/t1/t2 orders FIRST (before checking position)
    for oid_key in ("stop_order_id", "t1_order_id", "t2_order_id"):
        oid = order.get(oid_key)
        if oid:
            _sim_api("DELETE", f"/orderexecution/orders/{oid}", None)

    # Cancel pending limit entry order if not yet filled
    if order.get("status") == "pending_limit" and order.get("entry_order_id"):
        _sim_api("DELETE", f"/orderexecution/orders/{order['entry_order_id']}", None)
        print(f"[auto-trader] cancelled pending limit entry: {order['setup_name']}", flush=True)
        return

    # Wait briefly for any in-flight fills to settle after cancellations
    time.sleep(0.5)

    # Market close remaining contracts — but CHECK BROKER POSITION FIRST
    if order["status"] == "filled":
        remaining = order.get("stop_qty", 0)
        if remaining <= 0:
            return

        # === SAFETY: verify broker actually has a position before sending close ===
        broker_pos = _get_broker_position()
        if not broker_pos:
            # Broker shows FLAT — stop/target already closed the position.
            # Do NOT send a market order (would open a new ghost position).
            print(f"[auto-trader] flatten SKIPPED: broker already flat "
                  f"(stop/target filled). {order['setup_name']}", flush=True)
            order["stop_qty"] = 0
            return

        # Verify the position matches our expected direction
        broker_is_long = broker_pos["long_short"] == "Long"
        if broker_is_long != is_long:
            # Broker has opposite position — something is wrong, don't compound it
            print(f"[auto-trader] flatten SKIPPED: broker position direction mismatch! "
                  f"Expected={'Long' if is_long else 'Short'} "
                  f"Actual={broker_pos['long_short']} qty={broker_pos['qty']}. "
                  f"Needs manual review.", flush=True)
            _alert_critical(f"[AUTO-TRADE] POSITION MISMATCH\n"
                           f"Expected: {'Long' if is_long else 'Short'}\n"
                           f"Broker: {broker_pos['long_short']} {broker_pos['qty']}\n"
                           f"MANUAL REVIEW NEEDED")
            order["stop_qty"] = 0
            return

        # With stacking, broker position is the NET of all stacked trades.
        # Close only THIS trade's qty, NOT the entire broker position.
        # Cap at broker qty to avoid over-selling if broker has less than expected.
        actual_broker_qty = broker_pos["qty"]
        close_qty = min(remaining, actual_broker_qty)
        if actual_broker_qty != remaining:
            print(f"[auto-trader] flatten qty note: trade_qty={remaining} "
                  f"broker_total={actual_broker_qty} closing={close_qty}", flush=True)

        close_payload = {
            "AccountID": SIM_ACCOUNT_ID,
            "Symbol": MES_SYMBOL,
            "Quantity": str(close_qty),
            "OrderType": "Market",
            "TradeAction": close_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        resp = _sim_api("POST", "/orderexecution/orders", close_payload)
        if resp:
            order["close_qty"] = close_qty
            # Capture flatten fill price (SIM fills near-instantly)
            close_oid = None
            orders_list = resp.get("Orders", [])
            if orders_list:
                close_oid = orders_list[0].get("OrderID")
            if close_oid:
                time.sleep(1)
                close_fp = _get_order_fill_price(close_oid)
                if close_fp:
                    order["close_fill_price"] = close_fp

            print(f"[auto-trader] flattened: {order['setup_name']} "
                  f"qty={close_qty} fill={order.get('close_fill_price')}", flush=True)
        else:
            _alert_critical(f"[AUTO-TRADE] FLATTEN FAILED\n"
                           f"{order['setup_name']} id={order['setup_log_id']} qty={close_qty}\n"
                           f"MANUAL INTERVENTION NEEDED")


# ====== POLL ORDER STATUS ======

def poll_order_status():
    """Check order fills via TS API. Called each ~30s cycle."""
    if not AUTO_TRADE_ENABLED:
        return
    with _lock:
        if not _active_orders:
            return
        pending = [(lid, o) for lid, o in _active_orders.items()
                   if o["status"] in ("pending_entry", "pending_limit", "filled")]
    if not pending:
        return

    # Fetch recent orders from broker
    try:
        orders_data = _sim_api("GET",
            f"/brokerage/accounts/{SIM_ACCOUNT_ID}/orders", None)
    except Exception as e:
        print(f"[auto-trader] poll error: {e}", flush=True)
        return
    if not orders_data:
        return

    broker_orders = {}
    for o in orders_data.get("Orders", []):
        oid = o.get("OrderID")
        if oid:
            broker_orders[oid] = o

    for lid, order in pending:
        _check_order_fills(lid, order, broker_orders)


def _check_order_fills(lid, order, broker_orders):
    """Check individual order fills and update state."""
    changed = False

    # Check pending limit entry (Phase 2: deferred stop/target)
    if order["status"] == "pending_limit" and order.get("entry_order_id"):
        entry = broker_orders.get(order["entry_order_id"], {})
        entry_status = entry.get("Status", "")
        if entry_status == "FLL":
            fill_price = _extract_fill_price(entry)
            with _lock:
                order["status"] = "filled"
                order["fill_price"] = fill_price
            changed = True
            # Place deferred stop + target orders using actual fill price
            _place_deferred_protective_orders(lid, order, fill_price)
        elif entry_status in ("REJ", "CAN", "EXP"):
            with _lock:
                order["status"] = "closed"
            changed = True
            print(f"[auto-trader] limit entry {entry_status}: {order['setup_name']}", flush=True)
            _alert(f"[AUTO-TRADE] {order['setup_name']} LIMIT {entry_status}\n"
                   f"[CHARM S/R] Entry not filled — trade skipped")
        else:
            # Check timeout (30 min)
            placed_at = order.get("limit_placed_at")
            if placed_at:
                try:
                    placed_dt = datetime.fromisoformat(placed_at)
                    if placed_dt.tzinfo is None:
                        placed_dt = placed_dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
                    elapsed = (datetime.utcnow() - placed_dt.replace(tzinfo=None)).total_seconds()
                    if elapsed > _LIMIT_ENTRY_TIMEOUT_S:
                        _sim_api("DELETE", f"/orderexecution/orders/{order['entry_order_id']}", None)
                        with _lock:
                            order["status"] = "closed"
                        changed = True
                        print(f"[auto-trader] LIMIT TIMEOUT: {order['setup_name']} cancelled "
                              f"after {elapsed/60:.0f} min", flush=True)
                        _alert(f"[AUTO-TRADE] {order['setup_name']} LIMIT EXPIRED\n"
                               f"[CHARM S/R] {order.get('limit_entry_price', 0):.2f} not reached "
                               f"in {elapsed/60:.0f} min — trade skipped")
                except (ValueError, TypeError):
                    pass

    # Check entry fill
    if order["status"] == "pending_entry" and order.get("entry_order_id"):
        entry = broker_orders.get(order["entry_order_id"], {})
        entry_status = entry.get("Status", "")
        if entry_status == "FLL":  # Filled
            fill_price = _extract_fill_price(entry)
            with _lock:
                order["status"] = "filled"
                order["fill_price"] = fill_price
            changed = True
            print(f"[auto-trader] FILLED: {order['setup_name']} "
                  f"10 {MES_SYMBOL} @ {fill_price}", flush=True)

            t2_str = ""
            if order.get("full_target_price"):
                t2_str = f" | T2: {order['full_target_price']:.2f}"
            elif order.get("t2_qty", 0) > 0:
                t2_str = " | T2: trail"

            _alert(f"[AUTO-TRADE] {order['setup_name']} FILLED\n"
                   f"10 {MES_SYMBOL} @ {fill_price}\n"
                   f"T1: {order.get('first_target_price', 0):.2f}"
                   f"{t2_str}\n"
                   f"Stop: {order['current_stop']:.2f}")
        elif entry_status in ("REJ", "CAN", "EXP"):
            with _lock:
                order["status"] = "closed"
            changed = True
            rej_reason = entry.get("RejectReason") or entry.get("StatusDescription") or entry.get("Message", "")
            print(f"[auto-trader] entry {entry_status}: {order['setup_name']} reason={rej_reason} order={json.dumps(entry, default=str)[:500]}", flush=True)

    # Check T1/T2/stop fills (for filled orders)
    if order["status"] == "filled":
        # Check T1 fill
        if not order.get("t1_filled") and order.get("t1_order_id"):
            t1 = broker_orders.get(order["t1_order_id"], {})
            if t1.get("Status") == "FLL":
                t1_qty = order.get("t1_qty", T1_QTY)
                t1_fp = _extract_fill_price(t1)
                with _lock:
                    order["t1_filled"] = True
                    order["t1_fill_price"] = t1_fp
                    order["stop_qty"] -= t1_qty
                changed = True
                # Move stop to breakeven + commissions for remaining contracts
                # Covers round-trip commissions on ALL contracts (open + close)
                be_price = order.get("fill_price")
                if be_price and order["stop_qty"] > 0:
                    total_commission = TOTAL_QTY * COMMISSION_PER_SIDE * 2
                    be_offset = total_commission / (order["stop_qty"] * MES_POINT_VALUE)
                    # Round UP to next tick so we fully cover commissions
                    be_offset = math.ceil(be_offset / MES_TICK_SIZE) * MES_TICK_SIZE
                    is_long = order["direction"].lower() in ("long", "bullish")
                    if is_long:
                        order["current_stop"] = _round_mes(be_price + be_offset)
                    else:
                        order["current_stop"] = _round_mes(be_price - be_offset)
                print(f"[auto-trader] T1 filled: {order['setup_name']} "
                      f"qty={t1_qty} stop_qty={order['stop_qty']} "
                      f"stop->BE={order.get('current_stop')}", flush=True)
                _alert(f"[AUTO-TRADE] {order['setup_name']} T1 FILLED\n"
                       f"{t1_qty} {MES_SYMBOL} @ {order.get('first_target_price', 0):.2f}\n"
                       f"Remaining: {order['stop_qty']} contracts\n"
                       f"Stop moved to breakeven: {order.get('current_stop', 0):.2f}")
                # Reduce stop qty + update price to breakeven
                _adjust_stop_qty(lid, order)

        # Check T2 fill
        if not order.get("t2_filled") and order.get("t2_order_id"):
            t2 = broker_orders.get(order["t2_order_id"], {})
            if t2.get("Status") == "FLL":
                t2_qty = order.get("t2_qty", T2_QTY)
                t2_fp = _extract_fill_price(t2)
                with _lock:
                    order["t2_filled"] = True
                    order["t2_fill_price"] = t2_fp
                    order["stop_qty"] -= t2_qty
                changed = True
                print(f"[auto-trader] T2 filled: {order['setup_name']} "
                      f"qty={t2_qty} stop_qty={order['stop_qty']}", flush=True)
                _alert(f"[AUTO-TRADE] {order['setup_name']} T2 FILLED\n"
                       f"{t2_qty} {MES_SYMBOL} @ {order.get('full_target_price', 0):.2f}\n"
                       f"Remaining: {order['stop_qty']} contracts")
                _adjust_stop_qty(lid, order)

        # Check stop fill (closes remaining position)
        if order.get("stop_order_id"):
            stop_order = broker_orders.get(order["stop_order_id"], {})
            if stop_order.get("Status") == "FLL":
                stop_fp = _extract_fill_price(stop_order)
                stop_filled_qty = order.get("stop_qty", 0)
                with _lock:
                    order["stop_fill_price"] = stop_fp
                    order["stop_filled_qty"] = stop_filled_qty
                    order["status"] = "closed"
                    order["stop_qty"] = 0
                changed = True
                print(f"[auto-trader] STOP filled: {order['setup_name']} "
                      f"@ {stop_fp} qty={stop_filled_qty}", flush=True)
                # Cancel remaining limit orders
                for oid_key in ("t1_order_id", "t2_order_id"):
                    oid = order.get(oid_key)
                    filled_key = oid_key.replace("_order_id", "_filled")
                    if oid and not order.get(filled_key):
                        _sim_api("DELETE", f"/orderexecution/orders/{oid}", None)

        # All targets filled and no remaining position → close
        if order["stop_qty"] <= 0 and order["status"] == "filled":
            with _lock:
                order["status"] = "closed"
            # Cancel the stop if still open
            if order.get("stop_order_id"):
                _sim_api("DELETE", f"/orderexecution/orders/{order['stop_order_id']}", None)
            changed = True
            print(f"[auto-trader] all targets filled: {order['setup_name']}", flush=True)

    if changed:
        _persist_order(lid)
        # If order just closed (stop fill or all targets filled), run integrity check
        if order["status"] == "closed":
            _verify_stacking_integrity()


def _adjust_stop_qty(lid, order):
    """After a partial fill (T1 or T2), adjust the stop order quantity."""
    stop_oid = order.get("stop_order_id")
    new_qty = order["stop_qty"]

    if new_qty <= 0:
        # All contracts covered by targets — cancel stop
        if stop_oid:
            _sim_api("DELETE", f"/orderexecution/orders/{stop_oid}", None)
        return

    if not stop_oid:
        return

    # Replace stop with reduced quantity
    replace_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": MES_SYMBOL,
        "Quantity": str(new_qty),
        "OrderType": "StopMarket",
        "StopPrice": str(order["current_stop"]),
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }

    resp = _sim_api("PUT", f"/orderexecution/orders/{stop_oid}", replace_payload)
    if resp:
        new_orders = resp.get("Orders", [])
        if new_orders and new_orders[0].get("OrderID"):
            with _lock:
                order["stop_order_id"] = new_orders[0]["OrderID"]
        _persist_order(lid)
        print(f"[auto-trader] stop qty adjusted: id={lid} qty={new_qty}", flush=True)
    else:
        _alert(f"[AUTO-TRADE] MANUAL INTERVENTION: stop qty adjust FAILED\n"
               f"id={lid} target_qty={new_qty}")


def _extract_fill_price(entry_order: dict) -> float | None:
    """Extract fill price from a broker order response."""
    # Try FilledPrice first (top-level, most reliable)
    try:
        fp = float(entry_order.get("FilledPrice", 0))
        if fp > 0:
            return fp
    except (ValueError, TypeError):
        pass
    # Fallback: Legs[0].ExecPrice
    fills = entry_order.get("Legs", [{}])
    if fills:
        try:
            ep = float(fills[0].get("ExecPrice", 0))
            if ep > 0:
                return ep
        except (ValueError, TypeError):
            pass
    return None


# ====== STATUS & TOGGLES ======

def get_status() -> dict:
    """Return status dict for health endpoint."""
    with _lock:
        active = {lid: {
            "setup_name": o["setup_name"],
            "direction": o["direction"],
            "status": o["status"],
            "fill_price": o["fill_price"],
            "current_stop": o["current_stop"],
            "first_target_price": o.get("first_target_price"),
            "full_target_price": o.get("full_target_price"),
            "stop_qty": o.get("stop_qty", 0),
            "t1_filled": o.get("t1_filled", False),
            "t2_filled": o.get("t2_filled", False),
        } for lid, o in _active_orders.items() if o["status"] != "closed"}

    return {
        "enabled": AUTO_TRADE_ENABLED,
        "symbol": MES_SYMBOL,
        "total_qty": TOTAL_QTY,
        "active_count": len(active),
        "active_orders": active,
        "toggles": dict(_toggles),
    }


def test_order() -> dict:
    """Place a 1 MES test order (entry + stop) on SIM. Returns full response."""
    if not _get_token:
        return {"error": "no token function"}

    token = _get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    results = {}

    # 1. Check balance
    try:
        r = requests.get(f"{SIM_BASE}/brokerage/accounts/{SIM_ACCOUNT_ID}/balances",
                         headers=headers, timeout=10)
        if r.status_code == 200:
            bal = r.json().get("Balances", [{}])[0]
            results["balance"] = {
                "cash": bal.get("CashBalance"),
                "buying_power": bal.get("BuyingPower"),
                "init_margin": bal.get("BalanceDetail", {}).get("InitialMargin"),
            }
        else:
            results["balance"] = {"status": r.status_code, "body": r.text[:200]}
    except Exception as e:
        results["balance"] = {"error": str(e)}

    # 2. Place 1 MES Buy at Market
    entry_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": MES_SYMBOL,
        "Quantity": "1",
        "OrderType": "Market",
        "TradeAction": "Buy",
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    try:
        r = requests.post(f"{SIM_BASE}/orderexecution/orders",
                          headers=headers, json=entry_payload, timeout=10)
        results["entry"] = {"status": r.status_code, "body": r.json() if r.text else {}}
        entry_oid = None
        if r.status_code == 200:
            entry_oid = r.json().get("Orders", [{}])[0].get("OrderID")

        # 3. Place protective stop (Sell at -20 pts)
        if entry_oid:
            stop_payload = {
                "AccountID": SIM_ACCOUNT_ID,
                "Symbol": MES_SYMBOL,
                "Quantity": "1",
                "OrderType": "StopMarket",
                "StopPrice": "6850.00",
                "TradeAction": "Sell",
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            }
            sr = requests.post(f"{SIM_BASE}/orderexecution/orders",
                               headers=headers, json=stop_payload, timeout=10)
            results["stop"] = {"status": sr.status_code, "body": sr.json() if sr.text else {}}
            stop_oid = None
            if sr.status_code == 200:
                stop_oid = sr.json().get("Orders", [{}])[0].get("OrderID")

            import time as _time
            _time.sleep(3)

            # 4. Check order status + margin
            or_ = requests.get(f"{SIM_BASE}/brokerage/accounts/{SIM_ACCOUNT_ID}/orders",
                               headers=headers, timeout=10)
            if or_.status_code == 200:
                for o in or_.json().get("Orders", []):
                    if o.get("OrderID") == entry_oid:
                        results["entry_detail"] = {
                            "status": o.get("Status"),
                            "filled_price": o.get("FilledPrice"),
                        }

            br = requests.get(f"{SIM_BASE}/brokerage/accounts/{SIM_ACCOUNT_ID}/balances",
                              headers=headers, timeout=10)
            if br.status_code == 200:
                bal = br.json().get("Balances", [{}])[0]
                results["margin_with_position"] = {
                    "buying_power": bal.get("BuyingPower"),
                    "init_margin": bal.get("BalanceDetail", {}).get("InitialMargin"),
                    "day_trade_margin": bal.get("BalanceDetail", {}).get("DayTradeMargin"),
                }

            # 5. Clean up: cancel stop, close position
            if stop_oid:
                requests.delete(f"{SIM_BASE}/orderexecution/orders/{stop_oid}",
                                headers=headers, timeout=5)
            _time.sleep(1)
            requests.post(f"{SIM_BASE}/orderexecution/orders", headers=headers,
                          json={**entry_payload, "TradeAction": "Sell"}, timeout=10)
            results["cleaned_up"] = True

    except Exception as e:
        results["entry"] = {"error": str(e)}

    return results


def set_toggle(setup_name: str, enabled: bool) -> bool:
    """Toggle a specific setup on/off. Returns True if valid setup name."""
    if setup_name not in _toggles:
        return False
    _toggles[setup_name] = enabled
    print(f"[auto-trader] toggle {setup_name} = {enabled}", flush=True)
    return True


def get_toggles() -> dict:
    return dict(_toggles)


# ====== SIM API HELPER ======

def _sim_api(method: str, path: str, json_body: dict | None) -> dict | None:
    """Authenticated request to TradeStation SIM API."""
    if not _get_token:
        print("[auto-trader] no token function", flush=True)
        return None

    for attempt in range(2):
        try:
            token = _get_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            url = f"{SIM_BASE}{path}"

            if method == "GET":
                r = requests.get(url, headers=headers, timeout=10)
            elif method == "POST":
                r = requests.post(url, headers=headers, json=json_body, timeout=10)
            elif method == "PUT":
                r = requests.put(url, headers=headers, json=json_body, timeout=10)
            elif method == "DELETE":
                r = requests.delete(url, headers=headers, timeout=10)
            else:
                return None

            if r.status_code == 401 and attempt == 0:
                continue

            if r.status_code >= 400:
                print(f"[auto-trader] API {method} {path} [{r.status_code}]: "
                      f"{r.text[:300]}", flush=True)
                if method == "POST" and json_body:
                    print(f"[auto-trader] payload: {json.dumps(json_body, default=str)[:300]}", flush=True)
                return None

            result = r.json() if r.text else {}
            if method == "POST" and "order" in path.lower():
                print(f"[auto-trader] API {method} {path} [{r.status_code}]: "
                      f"{json.dumps(result, default=str)[:300]}", flush=True)
            return result

        except Exception as e:
            print(f"[auto-trader] API error {method} {path}: {e}", flush=True)
            return None

    return None


# ====== PERSISTENCE ======

def _persist_order(setup_log_id: int):
    """Save order state to DB for crash recovery."""
    if not _engine:
        return
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        state = json.dumps(order)

    try:
        from sqlalchemy import text
        with _engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO auto_trade_orders (setup_log_id, state, updated_at)
                VALUES (:id, :s, NOW())
                ON CONFLICT (setup_log_id) DO UPDATE SET state = :s, updated_at = NOW()
            """), {"id": setup_log_id, "s": state})
    except Exception as e:
        print(f"[auto-trader] persist error: {e}", flush=True)


def _load_active_orders():
    """Load non-closed orders from DB on startup.
    Only loads orders from today — stale overnight orders are auto-closed to prevent
    blocking new trades or masking orphaned broker positions."""
    global _active_orders
    if not _engine:
        return
    try:
        from sqlalchemy import text
        with _engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT setup_log_id, state FROM auto_trade_orders
                WHERE state->>'status' != 'closed'
            """)).mappings().all()

        today_str = date.today().isoformat()
        loaded = 0
        stale = 0
        for row in rows:
            lid = row["setup_log_id"]
            state = row["state"]
            if isinstance(state, str):
                state = json.loads(state)
            # Check if order is from today — skip stale overnight orders
            ts_placed = state.get("ts_placed", "")
            order_date = ts_placed[:10] if len(ts_placed) >= 10 else ""
            if order_date and order_date < today_str:
                # Stale order from previous day — mark closed in DB
                stale += 1
                state["status"] = "closed"
                state["close_reason"] = "stale_overnight"
                _active_orders[lid] = state  # temporarily load to persist
                _persist_order(lid)
                del _active_orders[lid]
                print(f"[auto-trader] STALE order auto-closed: {state.get('setup_name', '?')} "
                      f"id={lid} from {order_date}", flush=True)
                continue
            _active_orders[lid] = state
            loaded += 1

        if loaded:
            print(f"[auto-trader] restored {loaded} active orders", flush=True)
        if stale:
            print(f"[auto-trader] auto-closed {stale} stale overnight order(s)", flush=True)
    except Exception as e:
        print(f"[auto-trader] load error (non-fatal): {e}", flush=True)


# ====== TELEGRAM HELPER ======

def flatten_all_eod():
    """Force-close all open SIM positions at end of day.
    Called by scheduler at 15:55 ET before market close.
    With stacking: cancel ALL orders first, then close broker position once."""
    with _lock:
        open_orders = [(lid, o) for lid, o in _active_orders.items()
                       if o["status"] in ("pending_entry", "pending_limit", "filled")]
    if not open_orders:
        print("[auto-trader] EOD flatten: no tracked positions", flush=True)
    else:
        print(f"[auto-trader] EOD flatten: closing {len(open_orders)} tracked position(s)", flush=True)

        # Phase 1a: Cancel ALL orders across ALL tracked trades first
        # This prevents orphaned stop/target orders from filling during flatten
        cancelled = 0
        for lid, order in open_orders:
            for oid_key in ("entry_order_id", "stop_order_id", "t1_order_id", "t2_order_id"):
                oid = order.get(oid_key)
                # Cancel entry order only for pending_limit (not yet filled)
                if oid_key == "entry_order_id" and order.get("status") != "pending_limit":
                    continue
                if oid:
                    try:
                        _sim_api("DELETE", f"/orderexecution/orders/{oid}", None)
                        cancelled += 1
                    except Exception:
                        pass
        print(f"[auto-trader] EOD: cancelled {cancelled} orders", flush=True)

        # Phase 1b: Wait for cancellations to settle (margin freed)
        # TS SIM needs time to release margin from cancelled orders
        time.sleep(3)

        # Phase 1c: Close actual broker position with retry on rejection
        # Margin may not be fully released yet — retry with increasing waits
        broker_pos = _get_broker_position()
        if broker_pos:
            close_side = "Sell" if broker_pos["long_short"] == "Long" else "Buy"
            closed = False
            for attempt, wait in enumerate([0, 3, 5, 10], start=1):
                if attempt > 1:
                    print(f"[auto-trader] EOD close retry #{attempt} after {wait}s wait...",
                          flush=True)
                    time.sleep(wait)
                    # Re-check position (may have been closed by a delayed fill)
                    broker_pos = _get_broker_position()
                    if not broker_pos:
                        print(f"[auto-trader] EOD: position closed during wait", flush=True)
                        closed = True
                        break
                    close_side = "Sell" if broker_pos["long_short"] == "Long" else "Buy"

                close_payload = {
                    "AccountID": SIM_ACCOUNT_ID,
                    "Symbol": broker_pos["symbol"],
                    "Quantity": str(broker_pos["qty"]),
                    "OrderType": "Market",
                    "TradeAction": close_side,
                    "TimeInForce": {"Duration": "DAY"},
                    "Route": "Intelligent",
                }
                resp = _sim_api("POST", "/orderexecution/orders", close_payload)
                if resp:
                    # Check if order was accepted (not rejected)
                    orders = resp.get("Orders", [])
                    if orders and orders[0].get("Error") == "FAILED":
                        msg = orders[0].get("Message", "")
                        print(f"[auto-trader] EOD close rejected (attempt {attempt}): {msg}",
                              flush=True)
                        continue  # retry
                    print(f"[auto-trader] EOD: closed {broker_pos['long_short']} "
                          f"{broker_pos['qty']} MES (attempt {attempt})", flush=True)
                    closed = True
                    break
                else:
                    print(f"[auto-trader] EOD close API error (attempt {attempt})", flush=True)

            if not closed:
                _alert_critical(f"[AUTO-TRADE] EOD CLOSE FAILED after 4 attempts\n"
                               f"{broker_pos['long_short']} {broker_pos['qty']} MES\n"
                               f"MANUAL CLOSE REQUIRED")
        else:
            print("[auto-trader] EOD: broker already flat", flush=True)

        # Phase 1d: Mark all tracked trades as closed
        for lid, order in open_orders:
            with _lock:
                order["status"] = "closed"
            _persist_order(lid)
            print(f"[auto-trader] EOD marked closed: {order['setup_name']} id={lid}", flush=True)

    # ── Phase 2: cancel ALL open orders on account (nuclear — catches orphans) ──
    try:
        ord_data = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}/orders", None)
        for o in (ord_data or {}).get("Orders", []):
            status = o.get("Status", "")
            if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
                continue
            oid = o.get("OrderID")
            if oid:
                _sim_api("DELETE", f"/orderexecution/orders/{oid}", None)
                print(f"[auto-trader] EOD: cancelled remaining order {oid}", flush=True)
    except Exception as e:
        print(f"[auto-trader] EOD order cancel sweep error: {e}", flush=True)

    # ── Phase 3: close any orphaned positions ──
    time.sleep(1)
    _close_broker_orphans(source="EOD")

    # ── Phase 4: final verification — confirm we are actually flat ──
    time.sleep(1)
    final_pos = _get_broker_position()
    if final_pos:
        print(f"[auto-trader] EOD CRITICAL: still have position after flatten! "
              f"{final_pos['long_short']} {final_pos['qty']} {final_pos['symbol']}. "
              f"Retrying...", flush=True)
        # One more attempt via flatten_account_positions (nuclear option)
        try:
            flatten_account_positions()
        except Exception as e:
            print(f"[auto-trader] EOD retry flatten error: {e}", flush=True)
        # Final check
        time.sleep(1)
        still_open = _get_broker_position()
        if still_open:
            _alert_critical(f"[AUTO-TRADE] EOD FLATTEN FAILED\n"
                           f"STILL OPEN: {still_open['long_short']} {still_open['qty']}\n"
                           f"MANUAL INTERVENTION REQUIRED")
            print(f"[auto-trader] EOD FLATTEN FAILED: {still_open}", flush=True)
        else:
            print(f"[auto-trader] EOD retry flatten succeeded", flush=True)
    else:
        print(f"[auto-trader] EOD flatten complete (verified flat)", flush=True)


def flatten_account_positions() -> dict:
    """Query TS SIM account for real positions and orders, close everything.
    Returns summary dict. Used by admin flatten-now endpoint and EOD orphan cleanup."""
    result = {"positions_closed": [], "orders_cancelled": [], "errors": []}

    # 1. Get actual positions from broker
    try:
        pos_data = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}/positions", None)
        positions = (pos_data or {}).get("Positions", [])
    except Exception as e:
        result["errors"].append(f"positions fetch: {e}")
        positions = []

    for pos in positions:
        symbol = pos.get("Symbol", "")
        qty = int(pos.get("Quantity", "0"))
        long_short = pos.get("LongShort", "")
        if qty <= 0:
            continue

        close_side = "Sell" if long_short == "Long" else "Buy"
        close_payload = {
            "AccountID": SIM_ACCOUNT_ID,
            "Symbol": symbol,
            "Quantity": str(qty),
            "OrderType": "Market",
            "TradeAction": close_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        try:
            resp = _sim_api("POST", "/orderexecution/orders", close_payload)
            fill_price = None
            rejected = False
            if resp:
                orders_list = resp.get("Orders", [])
                if orders_list and orders_list[0].get("Error") == "FAILED":
                    msg = orders_list[0].get("Message", "")
                    result["errors"].append(f"close {symbol} rejected: {msg}")
                    print(f"[auto-trader] flatten-account: REJECTED {long_short} "
                          f"{qty} {symbol}: {msg}", flush=True)
                    rejected = True
                else:
                    oid = None
                    for o in orders_list:
                        oid = o.get("OrderID")
                    if oid:
                        time.sleep(1)
                        fill_price = _get_order_fill_price(oid)
            if not rejected:
                result["positions_closed"].append({
                    "symbol": symbol, "qty": qty, "side": close_side,
                    "long_short": long_short, "fill_price": fill_price,
                })
                print(f"[auto-trader] flatten-account: closed {long_short} {qty} {symbol} "
                      f"fill={fill_price}", flush=True)
        except Exception as e:
            result["errors"].append(f"close {symbol}: {e}")

    # 2. Cancel all open orders on the account
    try:
        ord_data = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}/orders", None)
        orders = (ord_data or {}).get("Orders", [])
    except Exception as e:
        result["errors"].append(f"orders fetch: {e}")
        orders = []

    for o in orders:
        status = o.get("Status", "")
        if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
            continue  # already terminal
        oid = o.get("OrderID")
        if not oid:
            continue
        try:
            _sim_api("DELETE", f"/orderexecution/orders/{oid}", None)
            result["orders_cancelled"].append(oid)
            print(f"[auto-trader] flatten-account: cancelled order {oid}", flush=True)
        except Exception as e:
            result["errors"].append(f"cancel {oid}: {e}")

    print(f"[auto-trader] flatten-account: {len(result['positions_closed'])} positions closed, "
          f"{len(result['orders_cancelled'])} orders cancelled", flush=True)
    return result


def _close_broker_orphans(source: str = "EOD"):
    """Check broker for positions not tracked in _active_orders. Close any orphans."""
    try:
        pos_data = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}/positions", None)
        positions = (pos_data or {}).get("Positions", [])
    except Exception as e:
        print(f"[auto-trader] {source} orphan check failed: {e}", flush=True)
        return

    if not positions:
        print(f"[auto-trader] {source} orphan check: no broker positions — clean", flush=True)
        return

    # Check which positions are tracked vs orphaned
    with _lock:
        tracked_directions = set()
        for o in _active_orders.values():
            if o["status"] in ("pending_entry", "filled"):
                d = o["direction"].lower()
                tracked_directions.add("Long" if d in ("long", "bullish") else "Short")

    for pos in positions:
        symbol = pos.get("Symbol", "")
        qty = int(pos.get("Quantity", "0"))
        long_short = pos.get("LongShort", "")
        if qty <= 0:
            continue

        if long_short in tracked_directions:
            print(f"[auto-trader] {source} orphan check: {long_short} {qty} {symbol} "
                  f"— matches tracked position, OK", flush=True)
            continue

        print(f"[auto-trader] WARNING: {source} orphan detected — {long_short} {qty} {symbol}. "
              f"Closing...", flush=True)
        _alert_critical(f"[AUTO-TRADE] {source} ORPHAN DETECTED\n"
                        f"{long_short} {qty} {symbol}\nAuto-closing...")
        close_side = "Sell" if long_short == "Long" else "Buy"
        close_payload = {
            "AccountID": SIM_ACCOUNT_ID,
            "Symbol": symbol,
            "Quantity": str(qty),
            "OrderType": "Market",
            "TradeAction": close_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        try:
            _sim_api("POST", "/orderexecution/orders", close_payload)
            print(f"[auto-trader] {source} orphan closed: {long_short} {qty} {symbol}", flush=True)
        except Exception as e:
            print(f"[auto-trader] {source} orphan close FAILED {symbol}: {e}", flush=True)

    # Also cancel any remaining open orders not tracked by us
    try:
        ord_data = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}/orders", None)
        tracked_oids = set()
        with _lock:
            for o in _active_orders.values():
                for k in ("entry_order_id", "stop_order_id", "t1_order_id", "t2_order_id"):
                    oid = o.get(k)
                    if oid:
                        tracked_oids.add(str(oid))
        for o in (ord_data or {}).get("Orders", []):
            status = o.get("Status", "")
            if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
                continue
            oid = o.get("OrderID")
            if oid and str(oid) not in tracked_oids:
                _sim_api("DELETE", f"/orderexecution/orders/{oid}", None)
                print(f"[auto-trader] {source} orphan cleanup: cancelled untracked order {oid}", flush=True)
    except Exception as e:
        print(f"[auto-trader] {source} orphan order cancel failed: {e}", flush=True)


def periodic_orphan_check():
    """Periodic safety check — detect orphaned broker positions during market hours.
    Called by scheduler every 5 minutes.
    With stacking: also detects direction mismatches (ghost positions from orphaned orders).
    Also cleans up stale overnight orders that survived startup check."""
    if not AUTO_TRADE_ENABLED or not _get_token:
        return

    # Daily cleanup: expire any stale orders from previous days
    today_str = date.today().isoformat()
    with _lock:
        stale_ids = []
        for lid, o in _active_orders.items():
            if o["status"] in ("pending_entry", "pending_limit", "filled"):
                ts_placed = o.get("ts_placed", "")
                order_date = ts_placed[:10] if len(ts_placed) >= 10 else ""
                if order_date and order_date < today_str:
                    stale_ids.append(lid)
        for lid in stale_ids:
            o = _active_orders[lid]
            print(f"[auto-trader] PERIODIC: expiring stale order {o.get('setup_name', '?')} "
                  f"id={lid} from {o.get('ts_placed', '?')[:10]}", flush=True)
            o["status"] = "closed"
            o["close_reason"] = "stale_overnight_periodic"
            _persist_order(lid)

    broker_pos = _get_broker_position()
    if not broker_pos:
        return  # flat, nothing to do

    with _lock:
        active_filled = [(lid, o) for lid, o in _active_orders.items()
                         if o["status"] in ("pending_entry", "filled")]

    if not active_filled:
        # No tracked trades but broker has position — orphan
        print(f"[auto-trader] PERIODIC: orphan position detected — "
              f"{broker_pos['long_short']} {broker_pos['qty']} {broker_pos['symbol']}. "
              f"Closing...", flush=True)
        _alert_critical(f"[AUTO-TRADE] PERIODIC ORPHAN DETECTED\n"
                        f"{broker_pos['long_short']} {broker_pos['qty']} "
                        f"{broker_pos['symbol']}\nAuto-closing...")
        _close_broker_orphans(source="PERIODIC")
        return

    # With stacking: check direction matches tracked trades
    tracked_long = any(o["direction"].lower() in ("long", "bullish") for _, o in active_filled)
    tracked_short = any(o["direction"].lower() not in ("long", "bullish") for _, o in active_filled)
    broker_is_long = broker_pos["long_short"] == "Long"

    # Direction mismatch = ghost position (e.g., tracking longs but broker is short)
    if broker_is_long and not tracked_long:
        print(f"[auto-trader] PERIODIC: direction mismatch — broker Long but "
              f"no tracked longs. Ghost position. Closing...", flush=True)
        _alert_critical(f"[AUTO-TRADE] PERIODIC GHOST DETECTED\n"
                        f"Broker: {broker_pos['long_short']} {broker_pos['qty']}\n"
                        f"Tracked: {'Short only' if tracked_short else 'none'}\n"
                        f"Auto-closing...")
        _close_broker_orphans(source="PERIODIC")
    elif not broker_is_long and not tracked_short:
        print(f"[auto-trader] PERIODIC: direction mismatch — broker Short but "
              f"no tracked shorts. Ghost position. Closing...", flush=True)
        _alert_critical(f"[AUTO-TRADE] PERIODIC GHOST DETECTED\n"
                        f"Broker: {broker_pos['long_short']} {broker_pos['qty']}\n"
                        f"Tracked: {'Long only' if tracked_long else 'none'}\n"
                        f"Auto-closing...")
        _close_broker_orphans(source="PERIODIC")


def _verify_stacking_integrity():
    """After closing a stacked trade, verify broker position matches remaining trades.
    Detects ghost positions from orphaned stop/target orders and closes them."""
    try:
        time.sleep(1)  # let any in-flight fills settle
        broker_pos = _get_broker_position()

        with _lock:
            active_filled = [(lid, o) for lid, o in _active_orders.items()
                             if o["status"] in ("pending_entry", "filled")]

        if not active_filled:
            # No remaining tracked trades — broker should be flat
            if broker_pos:
                print(f"[auto-trader] INTEGRITY: ghost position detected! "
                      f"No tracked trades but broker has {broker_pos['long_short']} "
                      f"{broker_pos['qty']} {broker_pos['symbol']}. Closing...", flush=True)
                _alert_critical(f"[AUTO-TRADE] GHOST POSITION DETECTED\n"
                               f"{broker_pos['long_short']} {broker_pos['qty']}\n"
                               f"Auto-closing...")
                _close_broker_orphans(source="INTEGRITY")
            return

        # Calculate expected position from remaining active trades
        expected_qty = 0
        expected_dir = None
        for lid, o in active_filled:
            d = o["direction"].lower()
            is_long = d in ("long", "bullish")
            qty = o.get("stop_qty", 0)
            if expected_dir is None:
                expected_dir = "Long" if is_long else "Short"
            if is_long:
                expected_qty += qty
            else:
                expected_qty -= qty

        if broker_pos:
            broker_is_long = broker_pos["long_short"] == "Long"
            expected_is_long = expected_qty > 0
            # Direction mismatch = ghost position
            if broker_is_long != expected_is_long:
                print(f"[auto-trader] INTEGRITY: direction mismatch! "
                      f"Expected={'Long' if expected_is_long else 'Short'} "
                      f"Broker={broker_pos['long_short']} {broker_pos['qty']}. "
                      f"Closing ghost...", flush=True)
                _alert_critical(f"[AUTO-TRADE] INTEGRITY: DIRECTION MISMATCH\n"
                               f"Expected: {'Long' if expected_is_long else 'Short'} "
                               f"{abs(expected_qty)}\n"
                               f"Broker: {broker_pos['long_short']} {broker_pos['qty']}\n"
                               f"Auto-closing excess...")
                _close_broker_orphans(source="INTEGRITY")
    except Exception as e:
        print(f"[auto-trader] integrity check error: {e}", flush=True)


def _alert(msg: str):
    """Non-critical auto-trade alerts — log only (Telegram suppressed for SIM)."""
    pass


def _alert_critical(msg: str):
    """Critical safety alerts — always send Telegram. For orphans, position mismatches, etc."""
    if _send_telegram:
        try:
            _send_telegram(msg)
        except Exception as e:
            print(f"[auto-trader] critical alert send failed: {e}", flush=True)
    print(f"[auto-trader] CRITICAL: {msg}", flush=True)
