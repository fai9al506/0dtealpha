# Options Trader: 0DTE options SIM execution module
# Self-contained — receives engine, token fn, and telegram fn via init()
# Hardcoded to SIM API — cannot hit live.
#
# Buys SPY (or SPXW) 0DTE options at ~0.30 delta on ALL setups (behind Greek filter).
# Single-leg only, no splits, no trailing — exit on outcome resolution.
# No stop-loss needed — max risk = premium paid.
#
# Tracks BOTH:
#   - SIM P&L (actual broker fills — may be unreliable for index options)
#   - Theoretical P&L (live API bid/ask at entry/exit — accurate market prices)

import os, json, time, requests
from datetime import datetime, date
from threading import Lock

# ====== CONFIG ======
SIM_BASE = "https://sim-api.tradestation.com/v3"
SIM_ACCOUNT_ID = os.getenv("OPTIONS_SIM_ACCOUNT", "SIM2609238M")
OPTIONS_TRADE_ENABLED = os.getenv("OPTIONS_TRADE_ENABLED", "false").lower() == "true"
OPTIONS_QTY = int(os.getenv("OPTIONS_QTY", "1"))
TARGET_DELTA = float(os.getenv("OPTIONS_TARGET_DELTA", "0.30"))
MAX_HOLD_MINUTES = int(os.getenv("OPTIONS_MAX_HOLD_MIN", "90"))     # close after 90 min
OPTIONS_UNDERLYING = os.getenv("OPTIONS_UNDERLYING", "SPY")         # "SPY" or "SPXW"

# Chain column indices (CANONICAL_COLS from main.py)
# C_Volume(0), C_OI(1), C_IV(2), C_Gamma(3), C_Delta(4), C_Bid(5), C_BidSize(6), C_Ask(7), C_AskSize(8), C_Last(9),
# Strike(10),
# P_Last(11), P_Ask(12), P_AskSize(13), P_Bid(14), P_BidSize(15), P_Delta(16), P_Gamma(17), P_IV(18), P_OI(19), P_Volume(20)
IDX_C_DELTA = 4
IDX_C_BID = 5
IDX_C_ASK = 7
IDX_STRIKE = 10
IDX_P_DELTA = 16
IDX_P_BID = 14
IDX_P_ASK = 12


# ====== STATE ======
_engine = None
_get_token = None       # callable -> str (access token)
_send_telegram = None   # callable(msg) -> bool
_lock = Lock()
_active_orders: dict[int, dict] = {}  # keyed by setup_log_id


def init(engine, get_token_fn, send_telegram_fn):
    """Initialize options trader. Called once at startup."""
    global _engine, _get_token, _send_telegram
    _engine = engine
    _get_token = get_token_fn
    _send_telegram = send_telegram_fn
    _load_active_orders()
    n = len(_active_orders)
    print(f"[options] init: enabled={OPTIONS_TRADE_ENABLED} account={SIM_ACCOUNT_ID} "
          f"underlying={OPTIONS_UNDERLYING} qty={OPTIONS_QTY} delta={TARGET_DELTA} active={n}", flush=True)


# ====== MAIN ENTRY POINT ======

def place_trade(setup_log_id: int, setup_name: str, direction: str, spot: float):
    """Buy 0DTE option when a setup fires.

    Args:
        setup_log_id: DB id from setup_log table
        setup_name: e.g. "Skew Charm"
        direction: "Long"/"Bullish" or "Short"/"Bearish"
        spot: current SPX spot price
    """
    if not OPTIONS_TRADE_ENABLED:
        print(f"[options] skip {setup_name}: master switch OFF", flush=True)
        return
    if not setup_log_id:
        print(f"[options] skip {setup_name}: no setup_log_id", flush=True)
        return

    with _lock:
        if setup_log_id in _active_orders:
            print(f"[options] skip {setup_name} id={setup_log_id}: already active", flush=True)
            return

    is_long = direction.lower() in ("long", "bullish")

    # Find the best strike from chain data
    strike_info = _find_strike(is_long)
    if not strike_info:
        print(f"[options] skip {setup_name}: no suitable strike found", flush=True)
        return

    strike = strike_info["strike"]
    delta = strike_info["delta"]
    snap_bid = strike_info["bid"]
    snap_ask = strike_info["ask"]

    # Build option symbol: "SPY 260313C670" or "SPXW 260305C5880"
    today = date.today()
    cp = "C" if is_long else "P"
    symbol = f"{OPTIONS_UNDERLYING} {today.strftime('%y%m%d')}{cp}{strike}"

    # Fetch LIVE quote for accurate entry price (snapshot can be up to 2 min stale)
    live_q = _get_option_quote(symbol)
    if live_q and live_q.get("ask") and live_q["ask"] > 0:
        ask = live_q["ask"]
        bid = live_q["bid"] or snap_bid
        print(f"[options] live quote {symbol}: bid=${bid:.2f} ask=${ask:.2f} "
              f"(snap bid=${snap_bid:.2f} ask=${snap_ask:.2f})", flush=True)
    else:
        # Fallback to snapshot if live quote fails
        ask = snap_ask
        bid = snap_bid
        print(f"[options] live quote failed for {symbol}, using snapshot: "
              f"bid=${bid:.2f} ask=${ask:.2f}", flush=True)

    # Place limit buy at ask price
    limit_price = round(ask, 2) if ask > 0 else None
    if not limit_price:
        print(f"[options] skip {setup_name}: ask price is 0", flush=True)
        return

    payload = {
        "AccountID": SIM_ACCOUNT_ID,
        "Symbol": symbol,
        "Quantity": str(OPTIONS_QTY),
        "OrderType": "Limit",
        "LimitPrice": str(limit_price),
        "TradeAction": "BUYTOOPEN",
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    resp = _sim_api("POST", "/orderexecution/orders", payload)
    ok, entry_oid = _order_ok(resp)
    if not ok:
        _alert(f"[OPTIONS] FAILED entry for {setup_name}\n"
               f"Symbol: {symbol} | Delta: {delta:.3f} | Ask: ${ask:.2f}")
        return

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "symbol": symbol,
        "strike": strike,
        "delta_at_entry": delta,
        "entry_order_id": entry_oid,
        "close_order_id": None,
        "qty": OPTIONS_QTY,
        "entry_price": None,        # SIM fill price (may be unreliable)
        "close_price": None,        # SIM fill price (may be unreliable)
        "theo_entry_price": ask,    # theoretical: buy at ask (accurate)
        "theo_close_price": None,   # theoretical: sell at bid (set on close)
        "status": "pending_entry",
        "spot_at_entry": spot,
        "bid_at_entry": bid,
        "ask_at_entry": ask,
        "ts_placed": datetime.utcnow().isoformat(),
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    print(f"[options] placed: {setup_name} BUYTOOPEN {OPTIONS_QTY} {symbol} "
          f"delta={delta:.3f} limit=${limit_price:.2f} spot={spot:.0f}", flush=True)
    _alert(f"[OPTIONS] {setup_name} placed\n"
           f"BUYTOOPEN {OPTIONS_QTY} {symbol}\n"
           f"Delta: {delta:.3f} | Limit: ${limit_price:.2f} | SPX: {spot:.0f}")


def close_trade(setup_log_id: int, result_type: str = ""):
    """Close option position on outcome resolution."""
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] == "closed":
            return

    setup_name = order["setup_name"]
    symbol = order["symbol"]

    # Sell to close at bid price (avoid terrible SIM market fills)
    if order["status"] == "filled":
        # Get current bid for limit exit — this IS the theoretical close price
        bid_price = _get_option_bid(symbol)

        # Save theoretical close price (live API bid — accurate market price)
        if bid_price and bid_price > 0:
            with _lock:
                order["theo_close_price"] = bid_price
            theo_entry = order.get("theo_entry_price") or order.get("ask_at_entry") or 0
            theo_pnl = (bid_price - theo_entry) * 100 * order["qty"]
            print(f"[options] theo close: {setup_name} theo_bid=${bid_price:.2f} "
                  f"theo_pnl=${theo_pnl:+.0f}", flush=True)

        close_order_type = "Limit" if bid_price and bid_price > 0 else "Market"
        close_payload = {
            "AccountID": SIM_ACCOUNT_ID,
            "Symbol": symbol,
            "Quantity": str(order["qty"]),
            "OrderType": close_order_type,
            "TradeAction": "SELLTOCLOSE",
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        if close_order_type == "Limit":
            close_payload["LimitPrice"] = str(round(bid_price, 2))
        resp = _sim_api("POST", "/orderexecution/orders", close_payload)
        close_ok, close_oid = _order_ok(resp)
        if close_ok:
            with _lock:
                order["close_order_id"] = close_oid
                order["ts_closed"] = datetime.utcnow().isoformat()
            # Try to capture fill price
            if close_oid:
                time.sleep(1)
                fp = _get_order_fill_price(close_oid)
                if fp:
                    with _lock:
                        order["close_price"] = fp
            print(f"[options] closed: {setup_name} {symbol} "
                  f"result={result_type} sim_close=${order.get('close_price')} "
                  f"theo_close=${order.get('theo_close_price')}", flush=True)
        else:
            print(f"[options] close FAILED: {setup_name} {symbol} — "
                  f"will expire cash-settled", flush=True)
    elif order["status"] == "pending_entry":
        # Entry not filled yet — cancel it
        if order.get("entry_order_id"):
            _sim_api("DELETE", f"/orderexecution/orders/{order['entry_order_id']}", None)
        print(f"[options] cancelled unfilled entry: {setup_name} {symbol}", flush=True)

    with _lock:
        order["status"] = "closed"
    _persist_order(setup_log_id)


def poll_order_status():
    """Check order fills, stop-loss, and time exit via TS API. Called each ~30s cycle."""
    if not OPTIONS_TRADE_ENABLED:
        return
    with _lock:
        if not _active_orders:
            return
        pending = [(lid, o) for lid, o in _active_orders.items()
                   if o["status"] in ("pending_entry", "filled")]
    if not pending:
        return

    try:
        orders_data = _sim_api("GET",
            f"/brokerage/accounts/{SIM_ACCOUNT_ID}/orders", None)
    except Exception as e:
        print(f"[options] poll error: {e}", flush=True)
        return
    if not orders_data:
        return

    broker_orders = {}
    for o in orders_data.get("Orders", []):
        oid = o.get("OrderID")
        if oid:
            broker_orders[oid] = o

    for lid, order in pending:
        _check_fills(lid, order, broker_orders)

    # ── Time-based exit for filled positions ──
    with _lock:
        filled = [(lid, o) for lid, o in _active_orders.items()
                  if o["status"] == "filled" and o.get("entry_price")]
    if not filled:
        return

    # Get current positions to check mark price
    try:
        pos_data = _sim_api("GET",
            f"/brokerage/accounts/{SIM_ACCOUNT_ID}/positions", None)
    except Exception:
        pos_data = None

    positions_by_symbol = {}
    if pos_data:
        for p in pos_data.get("Positions", []):
            sym = p.get("Symbol", "")
            positions_by_symbol[sym] = p

    now = datetime.utcnow()
    for lid, order in filled:
        symbol = order["symbol"]
        entry_price = order["entry_price"]
        placed_ts = order.get("ts_placed")

        # Get current mark/last price from position
        pos = positions_by_symbol.get(symbol)
        current_price = None
        if pos:
            last = pos.get("Last") or pos.get("MarketValue")
            if last:
                try:
                    current_price = float(str(last).replace(",", ""))
                except (ValueError, TypeError):
                    pass

        should_close = False
        reason = ""

        # No stop-loss: max risk is just the option premium, which is tiny.

        # Time exit: close if held > MAX_HOLD_MINUTES
        if not should_close and placed_ts:
            try:
                placed = datetime.fromisoformat(placed_ts)
                held_min = (now - placed).total_seconds() / 60
                if held_min >= MAX_HOLD_MINUTES:
                    should_close = True
                    reason = f"time-exit ({held_min:.0f} min > {MAX_HOLD_MINUTES} min)"
            except (ValueError, TypeError):
                pass

        if should_close:
            print(f"[options] auto-close: {order['setup_name']} {symbol} — {reason}",
                  flush=True)
            _alert(f"[OPTIONS] AUTO-CLOSE: {order['setup_name']}\n"
                   f"{symbol} | {reason}\n"
                   f"Entry: ${entry_price:.2f} | Current: ${current_price or '?'}")
            close_trade(lid, result_type=reason)


# ====== STRIKE SELECTION ======

def _find_strike(is_long: bool) -> dict | None:
    """Find option strike nearest to TARGET_DELTA from latest chain snapshot."""
    if not _engine:
        return None

    # Use SPY chain for SPY, SPX chain for SPXW
    table = "spy_chain_snapshots" if OPTIONS_UNDERLYING == "SPY" else "chain_snapshots"

    try:
        from sqlalchemy import text
        with _engine.begin() as conn:
            row = conn.execute(text(
                f"SELECT columns, rows FROM {table} ORDER BY ts DESC LIMIT 1"
            )).mappings().first()
        if not row:
            print(f"[options] no {table} snapshot available", flush=True)
            return None

        cols = json.loads(row["columns"]) if isinstance(row["columns"], str) else row["columns"]
        rows = json.loads(row["rows"]) if isinstance(row["rows"], str) else row["rows"]
    except Exception as e:
        print(f"[options] chain query error: {e}", flush=True)
        return None

    if not rows:
        print(f"[options] {table} snapshot has no rows", flush=True)
        return None

    # For LONG: buy call → scan C_Delta (idx 4), target ~0.30
    # For SHORT: buy put → scan P_Delta (idx 16), target ~-0.30 (abs 0.30)
    best = None
    best_gap = float("inf")

    for r in rows:
        try:
            strike = float(r[IDX_STRIKE]) if r[IDX_STRIKE] else None
            if not strike:
                continue

            if is_long:
                delta = float(r[IDX_C_DELTA]) if r[IDX_C_DELTA] else None
                bid = float(r[IDX_C_BID]) if r[IDX_C_BID] else 0
                ask = float(r[IDX_C_ASK]) if r[IDX_C_ASK] else 0
            else:
                delta = float(r[IDX_P_DELTA]) if r[IDX_P_DELTA] else None
                bid = float(r[IDX_P_BID]) if r[IDX_P_BID] else 0
                ask = float(r[IDX_P_ASK]) if r[IDX_P_ASK] else 0

            if delta is None:
                continue

            # Put delta is negative; compare absolute value
            gap = abs(abs(delta) - TARGET_DELTA)
            if gap < best_gap:
                best_gap = gap
                best = {
                    "strike": int(strike),
                    "delta": delta,
                    "bid": bid,
                    "ask": ask,
                }
        except (ValueError, TypeError, IndexError):
            continue

    if best:
        print(f"[options] strike selected: {OPTIONS_UNDERLYING} {best['strike']} "
              f"delta={best['delta']:.3f} bid={best['bid']:.2f} ask={best['ask']:.2f}",
              flush=True)
    return best


# ====== ORDER FILL CHECKING ======

def _check_fills(lid, order, broker_orders):
    """Check individual order fills and update state."""
    changed = False

    # Check entry fill
    if order["status"] == "pending_entry" and order.get("entry_order_id"):
        entry = broker_orders.get(order["entry_order_id"], {})
        status = entry.get("Status", "")
        if status == "FLL":
            fp = _extract_fill_price(entry)
            with _lock:
                order["status"] = "filled"
                order["entry_price"] = fp
            changed = True
            # Log both SIM fill and theoretical entry
            theo = order.get("theo_entry_price") or order.get("ask_at_entry")
            print(f"[options] FILLED: {order['setup_name']} {order['symbol']} "
                  f"sim=${fp} theo=${theo}", flush=True)
            _alert(f"[OPTIONS] {order['setup_name']} FILLED\n"
                   f"{order['symbol']} sim=${fp} theo=${theo}\n"
                   f"Delta: {order['delta_at_entry']:.3f}")
        elif status in ("REJ", "CAN", "EXP"):
            with _lock:
                order["status"] = "closed"
            changed = True
            reason = entry.get("RejectReason") or entry.get("StatusDescription") or ""
            print(f"[options] entry {status}: {order['setup_name']} "
                  f"reason={reason}", flush=True)

    # Check close fill
    if order["status"] == "filled" and order.get("close_order_id"):
        close = broker_orders.get(order["close_order_id"], {})
        if close.get("Status") == "FLL":
            fp = _extract_fill_price(close)
            with _lock:
                order["close_price"] = fp
                order["status"] = "closed"
            changed = True
            sim_pnl = (fp - (order.get("entry_price") or 0)) * 100 * order["qty"]
            theo_entry = order.get("theo_entry_price") or order.get("ask_at_entry") or 0
            theo_close = order.get("theo_close_price") or 0
            theo_pnl = (theo_close - theo_entry) * 100 * order["qty"] if theo_close else None
            theo_str = f" theo=${theo_pnl:+.0f}" if theo_pnl is not None else ""
            print(f"[options] CLOSE FILLED: {order['setup_name']} {order['symbol']} "
                  f"sim=${fp} sim_pnl=${sim_pnl:.0f}{theo_str}", flush=True)

    if changed:
        _persist_order(lid)


# ====== API HELPERS ======

def _sim_api(method: str, path: str, json_body: dict | None) -> dict | None:
    """Authenticated request to TradeStation SIM API."""
    if not _get_token:
        print("[options] no token function", flush=True)
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
            elif method == "DELETE":
                r = requests.delete(url, headers=headers, timeout=10)
            else:
                return None

            if r.status_code == 401 and attempt == 0:
                continue

            if r.status_code >= 400:
                print(f"[options] API {method} {path} [{r.status_code}]: "
                      f"{r.text[:300]}", flush=True)
                if method == "POST" and json_body:
                    print(f"[options] payload: {json.dumps(json_body, default=str)[:300]}", flush=True)
                return None

            result = r.json() if r.text else {}
            if method == "POST" and "order" in path.lower():
                print(f"[options] API {method} {path} [{r.status_code}]: "
                      f"{json.dumps(result, default=str)[:300]}", flush=True)
            return result

        except Exception as e:
            print(f"[options] API error {method} {path}: {e}", flush=True)
            return None

    return None


def _order_ok(resp: dict | None) -> tuple[bool, str | None]:
    """Check if an order response succeeded."""
    if not resp:
        return False, None
    orders = resp.get("Orders", [])
    if not orders:
        return False, None
    first = orders[0]
    if first.get("Error") == "FAILED":
        msg = first.get("Message", "unknown error")
        print(f"[options] order FAILED: {msg}", flush=True)
        return False, first.get("OrderID")
    oid = first.get("OrderID")
    return bool(oid), oid


def _extract_fill_price(order_data: dict) -> float | None:
    """Extract fill price from TS order response."""
    fp = order_data.get("FilledPrice") or order_data.get("AvgFillPrice")
    if fp:
        try:
            return float(str(fp).replace(",", ""))
        except (ValueError, TypeError):
            pass
    return None


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


def _get_option_quote(symbol: str) -> dict | None:
    """Get live bid/ask/last for an option symbol from TS quote API.
    Uses live API (not SIM) since marketdata endpoints are read-only."""
    try:
        if not _get_token:
            return None
        token = _get_token()
        headers = {"Authorization": f"Bearer {token}"}
        import urllib.parse
        encoded = urllib.parse.quote(symbol)
        r = requests.get(
            f"https://api.tradestation.com/v3/marketdata/quotes/{encoded}",
            headers=headers, timeout=10)
        if r.status_code == 200:
            quotes = r.json().get("Quotes", [])
            if quotes:
                q = quotes[0]
                def _pf(v):
                    if v is None: return None
                    try: return float(str(v).replace(",", ""))
                    except (ValueError, TypeError): return None
                return {
                    "bid": _pf(q.get("Bid")),
                    "ask": _pf(q.get("Ask")),
                    "last": _pf(q.get("Last")),
                    "delta": _pf(q.get("Delta")),
                }
    except Exception as e:
        print(f"[options] quote error for {symbol}: {e}", flush=True)
    return None


def _get_option_bid(symbol: str) -> float | None:
    """Get current bid price (convenience wrapper)."""
    q = _get_option_quote(symbol)
    return q["bid"] if q and q.get("bid") else None


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
                INSERT INTO options_trade_orders (setup_log_id, state, updated_at)
                VALUES (:id, :s, NOW())
                ON CONFLICT (setup_log_id) DO UPDATE SET state = :s, updated_at = NOW()
            """), {"id": setup_log_id, "s": state})
    except Exception as e:
        print(f"[options] persist error: {e}", flush=True)


def _load_active_orders():
    """Load non-closed orders from DB on startup."""
    global _active_orders
    if not _engine:
        return
    try:
        from sqlalchemy import text
        with _engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT setup_log_id, state FROM options_trade_orders
                WHERE state->>'status' != 'closed'
            """)).mappings().all()
        for row in rows:
            lid = row["setup_log_id"]
            state = row["state"]
            if isinstance(state, str):
                state = json.loads(state)
            _active_orders[lid] = state
        if _active_orders:
            print(f"[options] restored {len(_active_orders)} active orders", flush=True)
    except Exception as e:
        print(f"[options] load error (non-fatal): {e}", flush=True)


# ====== RECONCILIATION ======

_last_reconcile = 0.0  # epoch timestamp

def reconcile_with_broker():
    """Backfill missing entry/close prices from TS API. Called every ~60s.

    Purely additive — only READS from broker and UPDATES our DB state
    where data is missing. Does NOT change any trading logic or status flow.
    """
    global _last_reconcile
    if not OPTIONS_TRADE_ENABLED or not _get_token:
        return

    now = time.time()
    if now - _last_reconcile < 55:  # run at most every ~60s
        return
    _last_reconcile = now

    # Collect orders that need backfill
    with _lock:
        needs_backfill = [
            (lid, o) for lid, o in _active_orders.items()
            if (o.get("status") == "closed" and o.get("close_price") is None
                and o.get("close_order_id"))
            or (o.get("status") == "closed" and o.get("entry_price") is None
                and o.get("entry_order_id"))
        ]
    if not needs_backfill:
        return

    # Pull all orders from broker
    try:
        data = _sim_api("GET", f"/brokerage/accounts/{SIM_ACCOUNT_ID}/orders", None)
    except Exception:
        return
    if not data:
        return

    broker_orders = {}
    for o in data.get("Orders", []):
        oid = o.get("OrderID")
        if oid:
            broker_orders[oid] = o

    updated = 0
    for lid, order in needs_backfill:
        changed = False

        # Backfill missing entry_price
        if order.get("entry_price") is None and order.get("entry_order_id"):
            entry = broker_orders.get(order["entry_order_id"], {})
            if entry.get("Status") == "FLL":
                fp = _extract_fill_price(entry)
                if fp:
                    with _lock:
                        order["entry_price"] = fp
                    changed = True

        # Backfill missing close_price
        if order.get("close_price") is None and order.get("close_order_id"):
            close = broker_orders.get(order["close_order_id"], {})
            if close.get("Status") == "FLL":
                fp = _extract_fill_price(close)
                if fp:
                    with _lock:
                        order["close_price"] = fp
                    changed = True
                    entry_p = order.get("entry_price") or 0
                    pnl = (fp - entry_p) * 100 * order.get("qty", 1)
                    print(f"[options] reconcile: #{lid} {order.get('setup_name')} "
                          f"close_price=${fp:.2f} backfilled (P&L=${pnl:+.0f})",
                          flush=True)

        if changed:
            _persist_order(lid)
            updated += 1

    if updated:
        print(f"[options] reconcile: backfilled {updated} orders", flush=True)


# ====== TELEGRAM ======

def _alert(msg: str):
    """Send Telegram alert for option trades."""
    if _send_telegram:
        try:
            _send_telegram(msg)
        except Exception as e:
            print(f"[options] telegram error: {e}", flush=True)
    print(f"[options] {msg}", flush=True)
