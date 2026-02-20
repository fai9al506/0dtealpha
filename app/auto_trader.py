# Auto-Trader: ES Futures SIM execution module
# Self-contained — receives engine, token fn, and telegram fn via init()
# Hardcoded to SIM API — cannot hit live.

import os, json, time, requests
from datetime import datetime
from threading import Lock

# ====== CONFIG ======
SIM_BASE = "https://sim-api.tradestation.com/v3"
SIM_ACCOUNT_ID = "SIM2609239F"
QUANTITY = "1"
ES_SYMBOL = os.getenv("ES_TRADE_SYMBOL", "@ES")
AUTO_TRADE_ENABLED = os.getenv("AUTO_TRADE_ENABLED", "false").lower() == "true"

# ====== STATE ======
_engine = None
_get_token = None       # callable -> str (access token)
_send_telegram = None   # callable(msg) -> bool
_lock = Lock()
_active_orders: dict[int, dict] = {}  # keyed by setup_log_id

# Per-setup toggles — all default OFF
_toggles: dict[str, bool] = {
    "GEX Long": False,
    "AG Short": False,
    "BofA Scalp": False,
    "ES Absorption": False,
    "Paradigm Reversal": False,
    "DD Exhaustion": False,
}

# Setup → order strategy mapping
_BRACKET_SETUPS = {"BofA Scalp", "ES Absorption", "Paradigm Reversal"}
_TRAILING_SETUPS = {"GEX Long", "AG Short", "DD Exhaustion"}


def init(engine, get_token_fn, send_telegram_fn):
    """Initialize auto-trader. Called once at startup."""
    global _engine, _get_token, _send_telegram
    _engine = engine
    _get_token = get_token_fn
    _send_telegram = send_telegram_fn
    _load_active_orders()
    n = len(_active_orders)
    print(f"[auto-trader] init: enabled={AUTO_TRADE_ENABLED} symbol={ES_SYMBOL} "
          f"active_orders={n}", flush=True)
    for name, on in _toggles.items():
        if on:
            print(f"[auto-trader]   {name}: ON", flush=True)


# ====== MAIN ENTRY POINT ======

def place_trade(setup_log_id: int, setup_name: str, direction: str,
                es_price: float, target_pts: float | None, stop_pts: float):
    """Place an ES SIM trade when a setup fires.

    Args:
        setup_log_id: DB id from setup_log table
        setup_name: e.g. "BofA Scalp", "GEX Long"
        direction: "Long"/"Bullish" or "Short"/"Bearish"
        es_price: current ES price from quote stream
        target_pts: distance in points to target (None for trailing setups)
        stop_pts: distance in points to stop
    """
    if not AUTO_TRADE_ENABLED:
        return
    if not _toggles.get(setup_name, False):
        return
    if not setup_log_id:
        print(f"[auto-trader] skip {setup_name}: no setup_log_id", flush=True)
        return

    with _lock:
        if setup_log_id in _active_orders:
            print(f"[auto-trader] skip {setup_name} id={setup_log_id}: already active", flush=True)
            return

    is_long = direction.lower() in ("long", "bullish")

    # Calculate ES target/stop prices from point distances
    if is_long:
        es_stop = round(es_price - stop_pts, 2)
        es_target = round(es_price + target_pts, 2) if target_pts else None
    else:
        es_stop = round(es_price + stop_pts, 2)
        es_target = round(es_price - target_pts, 2) if target_pts else None

    side = "Buy" if is_long else "SellShort"

    if setup_name in _BRACKET_SETUPS and es_target is not None:
        _place_bracket_order(setup_log_id, setup_name, side, es_price,
                             es_target, es_stop, direction)
    else:
        _place_entry_with_stop(setup_log_id, setup_name, side, es_price,
                               es_stop, direction)


# ====== ORDER PLACEMENT ======

def _place_bracket_order(setup_log_id, setup_name, side, es_price,
                         es_target, es_stop, direction):
    """Place bracket order (market entry + stop + limit target) as a group."""
    exit_side = "Sell" if side == "Buy" else "BuyToCover"

    payload = {
        "Type": "BRK",
        "Orders": [
            {
                "AccountID": SIM_ACCOUNT_ID,
                "Symbol": ES_SYMBOL,
                "Quantity": QUANTITY,
                "OrderType": "Market",
                "TradeAction": side,
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            },
            {
                "AccountID": SIM_ACCOUNT_ID,
                "Symbol": ES_SYMBOL,
                "Quantity": QUANTITY,
                "OrderType": "StopMarket",
                "StopPrice": str(es_stop),
                "TradeAction": exit_side,
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            },
            {
                "AccountID": SIM_ACCOUNT_ID,
                "Symbol": ES_SYMBOL,
                "Quantity": QUANTITY,
                "OrderType": "Limit",
                "LimitPrice": str(es_target),
                "TradeAction": exit_side,
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            },
        ],
    }

    resp = _sim_api("POST", "/ordergroups", payload)
    if not resp:
        _alert(f"[AUTO-TRADE] FAILED to place bracket for {setup_name}\n"
               f"Side: {side} ES: {es_price:.2f}")
        return

    # Extract order IDs from response
    orders = resp.get("Orders", [])
    entry_oid = orders[0].get("OrderID") if len(orders) > 0 else None
    stop_oid = orders[1].get("OrderID") if len(orders) > 1 else None
    target_oid = orders[2].get("OrderID") if len(orders) > 2 else None

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "entry_order_id": entry_oid,
        "stop_order_id": stop_oid,
        "target_order_id": target_oid,
        "current_stop": es_stop,
        "current_target": es_target,
        "status": "pending_entry",
        "fill_price": None,
        "ts_placed": datetime.utcnow().isoformat(),
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    print(f"[auto-trader] BRACKET placed: {setup_name} {side} "
          f"ES={es_price:.2f} target={es_target:.2f} stop={es_stop:.2f} "
          f"ids={entry_oid}/{stop_oid}/{target_oid}", flush=True)
    _alert(f"[AUTO-TRADE] {setup_name} BRACKET placed\n"
           f"Side: {side} | ES: {es_price:.2f}\n"
           f"Target: {es_target:.2f} | Stop: {es_stop:.2f}")


def _place_entry_with_stop(setup_log_id, setup_name, side, es_price,
                            es_stop, direction):
    """Place market entry + separate stop order (for trailing setups)."""
    # 1. Market entry
    entry_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": ES_SYMBOL,
        "Quantity": QUANTITY,
        "OrderType": "Market",
        "TradeAction": side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }

    resp = _sim_api("POST", "/orders", entry_payload)
    if not resp:
        _alert(f"[AUTO-TRADE] FAILED entry for {setup_name}\n"
               f"Side: {side} ES: {es_price:.2f}")
        return

    orders = resp.get("Orders", [])
    entry_oid = orders[0].get("OrderID") if orders else None

    # 2. Stop order
    exit_side = "Sell" if side == "Buy" else "BuyToCover"
    stop_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": ES_SYMBOL,
        "Quantity": QUANTITY,
        "OrderType": "StopMarket",
        "StopPrice": str(es_stop),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }

    stop_resp = _sim_api("POST", "/orders", stop_payload)
    stop_oid = None
    if stop_resp:
        stop_orders = stop_resp.get("Orders", [])
        stop_oid = stop_orders[0].get("OrderID") if stop_orders else None

    if not stop_oid:
        _alert(f"[AUTO-TRADE] MANUAL INTERVENTION: {setup_name} entry placed "
               f"(id={entry_oid}) but STOP FAILED!\n"
               f"Side: {side} ES: {es_price:.2f} Stop: {es_stop:.2f}")

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "entry_order_id": entry_oid,
        "stop_order_id": stop_oid,
        "target_order_id": None,
        "current_stop": es_stop,
        "current_target": None,
        "status": "pending_entry",
        "fill_price": None,
        "ts_placed": datetime.utcnow().isoformat(),
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    print(f"[auto-trader] ENTRY+STOP placed: {setup_name} {side} "
          f"ES={es_price:.2f} stop={es_stop:.2f} "
          f"ids={entry_oid}/{stop_oid}", flush=True)
    _alert(f"[AUTO-TRADE] {setup_name} ENTRY placed\n"
           f"Side: {side} | ES: {es_price:.2f}\n"
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
        # Skip trivial changes (< 0.25 pts = 1 ES tick)
        if abs(new_stop_price - old_stop) < 0.25:
            return
        stop_oid = order["stop_order_id"]

    if not stop_oid:
        return

    new_stop_price = round(new_stop_price, 2)

    # Replace the stop order via PUT
    replace_payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": ES_SYMBOL,
        "Quantity": QUANTITY,
        "OrderType": "StopMarket",
        "StopPrice": str(new_stop_price),
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }

    resp = _sim_api("PUT", f"/orders/{stop_oid}", replace_payload)
    if resp:
        with _lock:
            order["current_stop"] = new_stop_price
            # Update stop_order_id if replaced order gets new ID
            new_orders = resp.get("Orders", [])
            if new_orders and new_orders[0].get("OrderID"):
                order["stop_order_id"] = new_orders[0]["OrderID"]
        _persist_order(setup_log_id)
        print(f"[auto-trader] stop updated: id={setup_log_id} "
              f"{old_stop:.2f} -> {new_stop_price:.2f}", flush=True)
    else:
        _alert(f"[AUTO-TRADE] MANUAL INTERVENTION: stop update FAILED\n"
               f"id={setup_log_id} old={old_stop:.2f} new={new_stop_price:.2f}")


def close_trade(setup_log_id: int, result_type: str):
    """Close a trade on outcome resolution."""
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] == "closed":
            return

    setup_name = order["setup_name"]

    if result_type == "EXPIRED":
        # Flatten position + cancel pending orders
        _flatten_position(order)
    # For WIN/LOSS on bracket orders, the broker auto-handles the fill
    # We just update state

    with _lock:
        order["status"] = "closed"
    _persist_order(setup_log_id)
    print(f"[auto-trader] closed: {setup_name} id={setup_log_id} "
          f"result={result_type}", flush=True)


def _flatten_position(order):
    """Market close + cancel all pending orders for a trade."""
    is_long = order["direction"].lower() in ("long", "bullish")
    close_side = "Sell" if is_long else "BuyToCover"

    # Cancel pending stop/target orders
    for oid_key in ("stop_order_id", "target_order_id"):
        oid = order.get(oid_key)
        if oid:
            _sim_api("DELETE", f"/orders/{oid}", None)

    # Market close
    if order["status"] == "filled":
        close_payload = {
            "AccountID": SIM_ACCOUNT_ID,
            "Symbol": ES_SYMBOL,
            "Quantity": QUANTITY,
            "OrderType": "Market",
            "TradeAction": close_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        resp = _sim_api("POST", "/orders", close_payload)
        if resp:
            print(f"[auto-trader] flattened: {order['setup_name']}", flush=True)
        else:
            _alert(f"[AUTO-TRADE] MANUAL INTERVENTION: flatten FAILED\n"
                   f"{order['setup_name']} id={order['setup_log_id']}")


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
            fill_price = None
            fills = entry.get("Legs", [{}])
            if fills:
                try:
                    fill_price = float(fills[0].get("ExecPrice", 0))
                except (ValueError, TypeError):
                    pass
            if not fill_price:
                try:
                    fill_price = float(entry.get("FilledPrice", 0))
                except (ValueError, TypeError):
                    pass
            with _lock:
                order["status"] = "filled"
                order["fill_price"] = fill_price
            changed = True
            print(f"[auto-trader] FILLED: {order['setup_name']} "
                  f"@ {fill_price}", flush=True)
            _alert(f"[AUTO-TRADE] {order['setup_name']} FILLED @ {fill_price}\n"
                   f"Stop: {order['current_stop']:.2f}"
                   + (f" | Target: {order['current_target']:.2f}"
                      if order.get('current_target') else ""))
        elif entry_status in ("REJ", "CAN", "EXP"):  # Rejected/Cancelled/Expired
            with _lock:
                order["status"] = "closed"
            changed = True
            print(f"[auto-trader] entry {entry_status}: {order['setup_name']}", flush=True)

    # Check stop/target fills (for filled orders)
    if order["status"] == "filled":
        for oid_key in ("stop_order_id", "target_order_id"):
            oid = order.get(oid_key)
            if not oid:
                continue
            exit_order = broker_orders.get(oid, {})
            exit_status = exit_order.get("Status", "")
            if exit_status == "FLL":
                with _lock:
                    order["status"] = "closed"
                changed = True
                label = "STOP" if oid_key == "stop_order_id" else "TARGET"
                print(f"[auto-trader] {label} filled: {order['setup_name']}", flush=True)
                break

    if changed:
        _persist_order(lid)


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
            "current_target": o["current_target"],
        } for lid, o in _active_orders.items() if o["status"] != "closed"}

    return {
        "enabled": AUTO_TRADE_ENABLED,
        "symbol": ES_SYMBOL,
        "active_count": len(active),
        "active_orders": active,
        "toggles": dict(_toggles),
    }


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
                # Force token refresh and retry
                continue

            if r.status_code >= 400:
                print(f"[auto-trader] API {method} {path} [{r.status_code}]: "
                      f"{r.text[:200]}", flush=True)
                return None

            return r.json() if r.text else {}

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
