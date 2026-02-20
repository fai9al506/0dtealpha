# Auto-Trader: MES Futures SIM execution module
# Self-contained — receives engine, token fn, and telegram fn via init()
# Hardcoded to SIM API — cannot hit live.
#
# Default: 1 MES (SIM overnight margin ~$2,735/contract, $5K account).
# Scale via env vars MES_TOTAL_QTY, MES_T1_QTY, MES_T2_QTY.
# Uses individual orders (not bracket groups — TS v3 BRK doesn't work for futures exits).
#   Flow A (BofA/Absorption/Paradigm): entry + stop + single limit @ +10pts
#   Flow B (GEX/AG/DD): entry + stop + T1 @ +10pts + T2 @ full target (DD: trail-only)

import os, json, time, requests
from datetime import datetime
from threading import Lock

# ====== CONFIG ======
SIM_BASE = "https://sim-api.tradestation.com/v3"
SIM_ACCOUNT_ID = "SIM2609239F"
MES_SYMBOL = os.getenv("ES_TRADE_SYMBOL", "MESH26")
AUTO_TRADE_ENABLED = os.getenv("AUTO_TRADE_ENABLED", "false").lower() == "true"

# SIM: $2,735 overnight margin per MES (no API intraday discount)
# With $5K account: max 1 MES. Adjust via env vars for larger accounts.
TOTAL_QTY = int(os.getenv("MES_TOTAL_QTY", "1"))
T1_QTY = int(os.getenv("MES_T1_QTY", "1"))
T2_QTY = int(os.getenv("MES_T2_QTY", "0"))   # 0 = no T2 with 1 contract
FIRST_TARGET_PTS = 10.0  # T1 target for all setups

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
}

# Setup → order flow mapping
_SINGLE_TARGET_SETUPS = {"BofA Scalp", "ES Absorption", "Paradigm Reversal"}
_SPLIT_TARGET_SETUPS = {"GEX Long", "AG Short", "DD Exhaustion"}


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


# ====== MAIN ENTRY POINT ======

def place_trade(setup_log_id: int, setup_name: str, direction: str,
                es_price: float, target_pts: float | None, stop_pts: float,
                full_target_pts: float | None = None):
    """Place MES SIM trade when a setup fires.

    Args:
        setup_log_id: DB id from setup_log table
        setup_name: e.g. "BofA Scalp", "GEX Long"
        direction: "Long"/"Bullish" or "Short"/"Bearish"
        es_price: current ES/MES price from quote stream
        target_pts: distance in points to first target (None for trailing setups)
        stop_pts: distance in points to stop
        full_target_pts: distance to Volland full target for T2. None = same as target_pts.
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

    is_long = direction.lower() in ("long", "bullish")

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

def _place_single_target(setup_log_id, setup_name, direction, is_long,
                          es_price, stop_pts):
    """Flow A: TOTAL_QTY MES with single limit target @ +10pts + stop.
    Uses individual orders (entry, then stop, then target) because TS v3
    bracket groups don't work correctly for futures exit legs."""
    side = "Buy" if is_long else "Sell"
    exit_side = "Sell" if is_long else "Buy"

    if is_long:
        es_stop = round(es_price - stop_pts, 2)
        es_target = round(es_price + FIRST_TARGET_PTS, 2)
    else:
        es_stop = round(es_price + stop_pts, 2)
        es_target = round(es_price - FIRST_TARGET_PTS, 2)

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
    if not resp:
        _alert(f"[AUTO-TRADE] FAILED entry for {setup_name}\n"
               f"Side: {side} {TOTAL_QTY} {MES_SYMBOL} @ {es_price:.2f}")
        return

    entry_oid = resp.get("Orders", [{}])[0].get("OrderID")

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
    stop_oid = None
    if stop_resp:
        stop_oid = stop_resp.get("Orders", [{}])[0].get("OrderID")
    if not stop_oid:
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
    t1_oid = None
    if t1_resp:
        t1_oid = t1_resp.get("Orders", [{}])[0].get("OrderID")
    if not t1_oid:
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
        es_stop = round(es_price - stop_pts, 2)
        t1_price = round(es_price + FIRST_TARGET_PTS, 2)
        t2_price = round(es_price + full_target_pts, 2) if full_target_pts else None
    else:
        es_stop = round(es_price + stop_pts, 2)
        t1_price = round(es_price - FIRST_TARGET_PTS, 2)
        t2_price = round(es_price - full_target_pts, 2) if full_target_pts else None

    # DD Exhaustion: trail-only T2 (no limit order)
    is_trail_only_t2 = (setup_name == "DD Exhaustion")
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
    if not resp:
        _alert(f"[AUTO-TRADE] FAILED entry for {setup_name}\n"
               f"Side: {side} {TOTAL_QTY} {MES_SYMBOL} @ {es_price:.2f}")
        return

    entry_oid = resp.get("Orders", [{}])[0].get("OrderID")

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
    stop_oid = None
    if stop_resp:
        stop_oid = stop_resp.get("Orders", [{}])[0].get("OrderID")
    if not stop_oid:
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
        if t1_resp:
            t1o = t1_resp.get("Orders", [])
            t1_oid = t1o[0].get("OrderID") if t1o else None
        if not t1_oid:
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
        if t2_resp:
            t2o = t2_resp.get("Orders", [])
            t2_oid = t2o[0].get("OrderID") if t2o else None

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

    new_stop_price = round(new_stop_price, 2)

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


def _flatten_position(order):
    """Market close remaining contracts + cancel all pending orders."""
    is_long = order["direction"].lower() in ("long", "bullish")
    close_side = "Sell" if is_long else "Buy"  # Futures: Buy/Sell only

    # Cancel pending stop/t1/t2 orders
    for oid_key in ("stop_order_id", "t1_order_id", "t2_order_id"):
        oid = order.get(oid_key)
        if oid:
            _sim_api("DELETE", f"/orderexecution/orders/{oid}", None)

    # Market close remaining contracts
    if order["status"] == "filled":
        remaining = order.get("stop_qty", 0)
        if remaining <= 0:
            return
        close_payload = {
            "AccountID": SIM_ACCOUNT_ID,
            "Symbol": MES_SYMBOL,
            "Quantity": str(remaining),
            "OrderType": "Market",
            "TradeAction": close_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        resp = _sim_api("POST", "/orderexecution/orders", close_payload)
        if resp:
            print(f"[auto-trader] flattened: {order['setup_name']} "
                  f"qty={remaining}", flush=True)
        else:
            _alert(f"[AUTO-TRADE] MANUAL INTERVENTION: flatten FAILED\n"
                   f"{order['setup_name']} id={order['setup_log_id']} qty={remaining}")


# ====== POLL ORDER STATUS ======

def poll_order_status():
    """Check order fills via TS API. Called each ~30s cycle."""
    if not AUTO_TRADE_ENABLED:
        return
    with _lock:
        if not _active_orders:
            return
        pending = [(lid, o) for lid, o in _active_orders.items()
                   if o["status"] in ("pending_entry", "filled")]
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
                with _lock:
                    order["t1_filled"] = True
                    order["stop_qty"] -= t1_qty
                changed = True
                print(f"[auto-trader] T1 filled: {order['setup_name']} "
                      f"qty={t1_qty} stop_qty={order['stop_qty']}", flush=True)
                _alert(f"[AUTO-TRADE] {order['setup_name']} T1 FILLED\n"
                       f"{t1_qty} {MES_SYMBOL} @ {order.get('first_target_price', 0):.2f}\n"
                       f"Remaining: {order['stop_qty']} contracts")
                # Reduce stop qty or close if all filled
                _adjust_stop_qty(lid, order)

        # Check T2 fill
        if not order.get("t2_filled") and order.get("t2_order_id"):
            t2 = broker_orders.get(order["t2_order_id"], {})
            if t2.get("Status") == "FLL":
                t2_qty = order.get("t2_qty", T2_QTY)
                with _lock:
                    order["t2_filled"] = True
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
                with _lock:
                    order["status"] = "closed"
                    order["stop_qty"] = 0
                changed = True
                print(f"[auto-trader] STOP filled: {order['setup_name']}", flush=True)
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
    """Load non-closed orders from DB on startup."""
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
        for row in rows:
            lid = row["setup_log_id"]
            state = row["state"]
            if isinstance(state, str):
                state = json.loads(state)
            _active_orders[lid] = state
        if _active_orders:
            print(f"[auto-trader] restored {len(_active_orders)} active orders", flush=True)
    except Exception as e:
        print(f"[auto-trader] load error (non-fatal): {e}", flush=True)


# ====== TELEGRAM HELPER ======

def _alert(msg: str):
    """Send auto-trade alert via setups Telegram."""
    if _send_telegram:
        try:
            _send_telegram(msg)
        except Exception:
            pass
