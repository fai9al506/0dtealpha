# Real Trader: MES Futures LIVE execution module
# Self-contained -- receives engine, get_token_fn, and send_telegram_fn via init()
# Uses REAL TradeStation API (api.tradestation.com) -- THIS IS REAL MONEY.
#
# Two accounts, direction-routed:
#   210VYX65 -> longs only
#   210VYX91 -> shorts only
#
# 1 MES per trade. No split target -- entry + stop + target.
# Cap: 2 concurrent per direction.
# Trail: SC trail (BE trigger=10, activation=10, gap=5).

import os, json, math, time, calendar, requests, zoneinfo
from datetime import datetime, date, timedelta
from threading import Lock

NY = zoneinfo.ZoneInfo("US/Eastern")

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
REAL_BASE = "https://api.tradestation.com/v3"

# Account whitelist -- ONLY these accounts can receive orders
ACCOUNT_WHITELIST = frozenset({"210VYX65", "210VYX91"})

# Direction binding -- each account is locked to one direction
_LONGS_ACCOUNT = os.getenv("REAL_TRADE_LONGS_ACCOUNT", "210VYX65")
_SHORTS_ACCOUNT = os.getenv("REAL_TRADE_SHORTS_ACCOUNT", "210VYX91")
ACCOUNT_DIRECTION_BINDING = {
    _LONGS_ACCOUNT: "long",
    _SHORTS_ACCOUNT: "short",
}

# Master switches -- both default OFF for safety
LONGS_ENABLED = os.getenv("REAL_TRADE_LONGS_ENABLED", "false").lower() == "true"
SHORTS_ENABLED = os.getenv("REAL_TRADE_SHORTS_ENABLED", "false").lower() == "true"

# Symbol
_es_env = os.getenv("REAL_TRADE_MES_SYMBOL", "auto")
MES_SYMBOL = _auto_mes_symbol() if _es_env.lower() == "auto" else _es_env

# Position sizing -- 1 MES per trade, per-direction concurrent caps
# Apr 18 2026: raised SHORTS cap 1→2 after slot study (TSRT-live 1-cap missed
# 12 shorts worth +244 pts / 91% WR over 17 days, ~$1,200 at 1 MES).
# LONGS stay at 1 (slot cap was neutral on longs — 5 skipped = +0.5 pts).
# Margin required per active MES: ~$687 intraday. Accounts funded +$1k each
# (pre-Monday) to support 2 concurrent on shorts.
QTY = 1
MAX_CONCURRENT_LONG = int(os.getenv("REAL_TRADE_MAX_CONCURRENT_LONG", "1"))
MAX_CONCURRENT_SHORT = int(os.getenv("REAL_TRADE_MAX_CONCURRENT_SHORT", "2"))
# Backward-compat alias — any legacy code reading MAX_CONCURRENT_PER_DIR gets
# the conservative LONG value. New code should use the per-direction constants.
MAX_CONCURRENT_PER_DIR = MAX_CONCURRENT_LONG

# Risk management
FIRST_TARGET_PTS = 10.0
MES_TICK_SIZE = 0.25
MES_POINT_VALUE = 5.0
MARGIN_PER_MES = float(os.getenv("REAL_TRADE_MARGIN_PER_MES", "700"))  # TS intraday margin $686.75/MES (Jan 2026)
DAILY_LOSS_LIMIT = float(os.getenv("REAL_TRADE_DAILY_LOSS_LIMIT", "300"))  # max daily loss in $

# SC Trail parameters
BE_TRIGGER_PTS = 10.0    # move stop to breakeven after 10pts profit
TRAIL_ACTIVATION_PTS = 10.0  # trail activates at 10pts
TRAIL_GAP_PTS = 5.0      # trail gap = max_fav - 5
BE_BUFFER_PTS = 0.25     # breakeven + 1 tick buffer

# Charm S/R limit entry timeout
_LIMIT_ENTRY_TIMEOUT_S = 1800  # 30 min

# DB table name
DB_TABLE = "real_trade_orders"


def _round_mes(price: float) -> float:
    """Round price to nearest MES tick (0.25)."""
    return round(round(price / MES_TICK_SIZE) * MES_TICK_SIZE, 2)


def _get_current_mes_price() -> float | None:
    """Fetch current MES Last price via REST for side-of-market validation.
    Used by update_stop() to avoid submitting a stop that market has already
    crossed (TS rejects such modifies and may wipe the original stop).
    ~100-200ms network call. Returns None on any failure."""
    if not _get_token:
        return None
    try:
        token = _get_token()
        sym = MES_SYMBOL.replace("@", "%40")
        r = requests.get(
            f"{REAL_BASE}/marketdata/quotes/{sym}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
        if r.status_code != 200:
            print(f"[real-trader] mes price fetch [{r.status_code}]", flush=True)
            return None
        js = r.json()
        for q in js.get("Quotes", []):
            last = q.get("Last")
            if last:
                return float(last)
    except Exception as e:
        print(f"[real-trader] mes price fetch failed: {e}", flush=True)
    return None


def _order_ok(resp: dict | None) -> tuple[bool, str | None]:
    """Check if an order response succeeded. Returns (ok, order_id).
    TS returns HTTP 200 even for FAILED orders -- must check order-level Error."""
    if not resp:
        return False, None
    orders = resp.get("Orders", [])
    if not orders:
        return False, None
    first = orders[0]
    if first.get("Error") == "FAILED":
        msg = first.get("Message", "unknown error")
        print(f"[real-trader] order FAILED: {msg}", flush=True)
        return False, first.get("OrderID")
    oid = first.get("OrderID")
    return bool(oid), oid


# ====== STATE ======
_engine = None
_get_token = None       # callable -> str (access token)
_send_telegram = None   # callable(msg) -> bool
_lock = Lock()
_active_orders: dict[int, dict] = {}  # keyed by setup_log_id
# Position reconciliation is now driven by a 30s scheduler job calling
# reconcile_positions() — no throttle variable needed here.


def init(engine, get_token_fn, send_telegram_fn):
    """Initialize real trader. Called once at startup."""
    global _engine, _get_token, _send_telegram
    _engine = engine
    _get_token = get_token_fn
    _send_telegram = send_telegram_fn
    _load_active_orders()
    n = len(_active_orders)
    print(f"[real-trader] init: longs={LONGS_ENABLED} (acct={_LONGS_ACCOUNT}) "
          f"shorts={SHORTS_ENABLED} (acct={_SHORTS_ACCOUNT}) "
          f"symbol={MES_SYMBOL} qty={QTY} "
          f"max_long={MAX_CONCURRENT_LONG} max_short={MAX_CONCURRENT_SHORT} "
          f"active_orders={n}", flush=True)

    # Validate account configuration
    if _LONGS_ACCOUNT not in ACCOUNT_WHITELIST:
        print(f"[real-trader] FATAL: longs account {_LONGS_ACCOUNT} not in whitelist!", flush=True)
    if _SHORTS_ACCOUNT not in ACCOUNT_WHITELIST:
        print(f"[real-trader] FATAL: shorts account {_SHORTS_ACCOUNT} not in whitelist!", flush=True)
    if _LONGS_ACCOUNT == _SHORTS_ACCOUNT:
        print(f"[real-trader] WARNING: longs and shorts using same account {_LONGS_ACCOUNT}", flush=True)

    # Verify account access on startup (if either direction enabled)
    if (LONGS_ENABLED or SHORTS_ENABLED) and _get_token:
        for acct_id in set(filter(None, [
            _LONGS_ACCOUNT if LONGS_ENABLED else None,
            _SHORTS_ACCOUNT if SHORTS_ENABLED else None,
        ])):
            try:
                # Use /balances endpoint (live TS API returns 404 on bare /accounts/{id})
                bal = _ts_api("GET", f"/brokerage/accounts/{acct_id}/balances", None, acct_id)
                if bal:
                    print(f"[real-trader] account {acct_id} OK: "
                          f"{json.dumps(bal, default=str)[:300]}", flush=True)
                else:
                    print(f"[real-trader] WARNING: cannot access account {acct_id}", flush=True)
                    _alert(f"⚠️ WARNING: Cannot access account {acct_id} on startup")
            except Exception as e:
                print(f"[real-trader] account {acct_id} check error: {e}", flush=True)

    # Pre-market startup cleanup
    if LONGS_ENABLED or SHORTS_ENABLED:
        try:
            from datetime import timezone as _tz
            import zoneinfo
            _et = zoneinfo.ZoneInfo("US/Eastern")
            _now_et = datetime.now(_tz.utc).astimezone(_et)
            _market_open = _now_et.replace(hour=9, minute=20, second=0, microsecond=0)
            _market_close = _now_et.replace(hour=16, minute=10, second=0, microsecond=0)
            if _now_et < _market_open or _now_et > _market_close:
                # Outside market hours -- flatten everything
                for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
                    if acct_id not in ACCOUNT_WHITELIST:
                        continue
                    broker_pos = _get_broker_position(acct_id)
                    if broker_pos:
                        print(f"[real-trader] PRE-MARKET CLEANUP: {acct_id} has "
                              f"{broker_pos['long_short']} {broker_pos['qty']} {broker_pos['symbol']}",
                              flush=True)
                        _alert(f"⚠️ PRE-MARKET: Found position on {acct_id}\n"
                               f"{broker_pos['long_short']} {broker_pos['qty']} {broker_pos['symbol']}\n"
                               f"Auto-closing...")
                        _flatten_account(acct_id)
                # Mark all tracked orders as closed
                _to_persist = []
                with _lock:
                    for lid, o in _active_orders.items():
                        if o["status"] not in ("closed",):
                            o["status"] = "closed"
                            o["close_reason"] = "pre_market_cleanup"
                            _to_persist.append((lid, o.get('setup_name', '?')))
                # Persist OUTSIDE lock (avoid deadlock — _persist_order also acquires _lock)
                for lid, name in _to_persist:
                    _persist_order(lid)
                    print(f"[real-trader] PRE-MARKET: closed order {name} id={lid}", flush=True)
            else:
                # During market hours -- orphan check
                for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
                    if acct_id in ACCOUNT_WHITELIST:
                        _close_broker_orphans(acct_id, source="STARTUP")
        except Exception as e:
            print(f"[real-trader] startup cleanup error: {e}", flush=True)


# ====== HELPERS ======

def _get_account_for_direction(is_long: bool) -> str | None:
    """Return the account ID for a given direction, or None if disabled."""
    if is_long:
        if not LONGS_ENABLED:
            return None
        return _LONGS_ACCOUNT
    else:
        if not SHORTS_ENABLED:
            return None
        return _SHORTS_ACCOUNT


def _validate_account_direction(account_id: str, is_long: bool) -> bool:
    """Validate that the account is allowed for this direction. CRITICAL SAFETY CHECK."""
    if account_id not in ACCOUNT_WHITELIST:
        print(f"[real-trader] BLOCKED: account {account_id} not in whitelist!", flush=True)
        _alert(f"🚨 SECURITY: Blocked order to non-whitelisted account {account_id}")
        return False
    expected_dir = ACCOUNT_DIRECTION_BINDING.get(account_id)
    if expected_dir is None:
        print(f"[real-trader] BLOCKED: account {account_id} has no direction binding!", flush=True)
        _alert(f"🚨 SECURITY: No direction binding for account {account_id}")
        return False
    actual_dir = "long" if is_long else "short"
    if expected_dir != actual_dir:
        print(f"[real-trader] BLOCKED: account {account_id} bound to {expected_dir}, "
              f"got {actual_dir}!", flush=True)
        _alert(f"🚨 SECURITY: Direction mismatch!\n"
               f"Account {account_id} bound to {expected_dir}, attempted {actual_dir}")
        return False
    return True


def _count_active_for_direction(is_long: bool) -> int:
    """Count currently active orders (pending or filled) for a direction."""
    count = 0
    with _lock:
        for o in _active_orders.values():
            if o["status"] in ("pending_entry", "pending_limit", "filled"):
                o_is_long = o["direction"].lower() in ("long", "bullish")
                if o_is_long == is_long:
                    count += 1
    return count


# ====== MAIN ENTRY POINT ======

def place_trade(setup_log_id: int, setup_name: str, direction: str,
                es_price: float, target_pts: float | None, stop_pts: float,
                charm_limit_price: float | None = None):
    """Place 1 MES REAL trade when a setup fires.

    Args:
        setup_log_id: DB id from setup_log table
        setup_name: e.g. "Skew Charm", "AG Short"
        direction: "Long"/"Bullish" or "Short"/"Bearish"
        es_price: current ES/MES price from quote stream
        target_pts: distance in points to target (None for trailing setups)
        stop_pts: distance in points to stop
        charm_limit_price: MES limit entry price (charm S/R shorts). None = market order.
    """
    is_long = direction.lower() in ("long", "bullish")

    # Setup filter: only trade Skew Charm + AG Short (defense-in-depth, main.py also filters)
    # AG Short added 2026-04-08 — SHORT account only (AG hardcoded direction="short")
    if setup_name not in ("Skew Charm", "AG Short"):
        print(f"[real-trader] skip {setup_name}: only Skew Charm/AG Short allowed on real accounts", flush=True)
        return

    # Check master switch for this direction
    account_id = _get_account_for_direction(is_long)
    if not account_id:
        dir_str = "longs" if is_long else "shorts"
        print(f"[real-trader] skip {setup_name}: {dir_str} master switch OFF", flush=True)
        return

    # Validate account-direction binding (CRITICAL SAFETY)
    if not _validate_account_direction(account_id, is_long):
        return

    if not setup_log_id:
        print(f"[real-trader] skip {setup_name}: no setup_log_id", flush=True)
        return

    # Dedup: already tracking this setup_log_id
    with _lock:
        if setup_log_id in _active_orders:
            print(f"[real-trader] skip {setup_name} id={setup_log_id}: already active", flush=True)
            return
        # DEDUP: block if same setup_name+direction placed within last 90s (deploy overlap)
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
                            print(f"[real-trader] DEDUP {setup_name} id={setup_log_id}: "
                                  f"same setup placed {(_now - _placed_dt).total_seconds():.0f}s ago "
                                  f"(id={_lid})", flush=True)
                            return
                    except (ValueError, TypeError):
                        pass

    # Cap check: max concurrent per direction (asymmetric — longs=1, shorts=2)
    active_count = _count_active_for_direction(is_long)
    cap = MAX_CONCURRENT_LONG if is_long else MAX_CONCURRENT_SHORT
    if active_count >= cap:
        dir_str = "long" if is_long else "short"
        # Log which orders are blocking (for debugging stale-order issues)
        blocking = [(lid, o.get("setup_name"), o.get("ts_placed", "")[:10], o.get("status"))
                    for lid, o in _active_orders.items()
                    if o.get("status") in ("pending_entry", "pending_limit", "filled")
                    and (o.get("direction", "").lower() in ("long", "bullish")) == is_long]
        print(f"[real-trader] skip {setup_name}: {dir_str} cap reached "
              f"({active_count}/{cap}) blocking={blocking}", flush=True)
        return

    # Margin/buying power pre-check
    bp = _get_buying_power(account_id)
    if bp is not None:
        margin_needed = QTY * MARGIN_PER_MES
        if bp < margin_needed:
            print(f"[real-trader] skip {setup_name}: insufficient buying power on {account_id} "
                  f"(${bp:,.0f} < ${margin_needed:,.0f})", flush=True)
            _alert(f"⚠️ SKIPPED {setup_name}: insufficient margin\n"
                   f"Account: {account_id} | BP: ${bp:,.0f} < ${margin_needed:,.0f}")
            return

    # Daily loss circuit breaker
    daily_loss = _get_daily_realized_loss()
    if daily_loss >= DAILY_LOSS_LIMIT:
        print(f"[real-trader] CIRCUIT BREAKER: daily loss ${daily_loss:,.0f} >= limit ${DAILY_LOSS_LIMIT:,.0f}", flush=True)
        _alert(f"🚨 CIRCUIT BREAKER HIT\n"
               f"Daily loss: ${daily_loss:,.0f} >= ${DAILY_LOSS_LIMIT:,.0f}\n"
               f"No more trades today.")
        return

    # Charm S/R limit entry for shorts ONLY (safety: ignore for longs)
    if charm_limit_price is not None and not is_long:
        _place_limit_entry(setup_log_id, setup_name, direction, is_long,
                           account_id, es_price, stop_pts, target_pts,
                           charm_limit_price)
        return

    # Standard market entry
    _place_market_entry(setup_log_id, setup_name, direction, is_long,
                        account_id, es_price, stop_pts, target_pts)


# ====== ORDER PLACEMENT ======

def _place_market_entry(setup_log_id, setup_name, direction, is_long,
                        account_id, es_price, stop_pts, target_pts):
    """Place market entry + stop (+ optional target) for 1 MES.
    When target_pts is None: trail-only mode (Opt2) — no target limit order placed.
    """
    # Final safety check before placing order
    if not _validate_account_direction(account_id, is_long):
        return

    side = "Buy" if is_long else "Sell"
    exit_side = "Sell" if is_long else "Buy"
    trail_only = target_pts is None  # Opt2: no target, trail stop only

    if is_long:
        es_stop = _round_mes(es_price - stop_pts)
        es_target = None if trail_only else _round_mes(es_price + target_pts)
    else:
        es_stop = _round_mes(es_price + stop_pts)
        es_target = None if trail_only else _round_mes(es_price - target_pts)

    # 1. Market entry
    entry_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(QTY),
        "OrderType": "Market",
        "TradeAction": side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    print(f"[real-trader] PLACING: {setup_name} {side} {QTY} {MES_SYMBOL} "
          f"@ ~{es_price:.2f} on {account_id}", flush=True)
    resp = _ts_api("POST", "/orderexecution/orders", entry_payload, account_id)
    ok, entry_oid = _order_ok(resp)
    if not ok:
        _alert(f"🚨 FAILED entry for {setup_name}\n"
               f"Account: {account_id}\n"
               f"Side: {side} {QTY} {MES_SYMBOL} @ ~{es_price:.2f}")
        return

    # 2. Stop order
    stop_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(QTY),
        "OrderType": "StopMarket",
        "StopPrice": str(es_stop),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    stop_resp = _ts_api("POST", "/orderexecution/orders", stop_payload, account_id)
    stop_ok, stop_oid = _order_ok(stop_resp)
    if not stop_ok:
        stop_oid = None
        _alert(f"🚨 MANUAL INTERVENTION: {setup_name} entry placed "
               f"(id={entry_oid}) but STOP FAILED!\n"
               f"Account: {account_id}\n"
               f"Side: {side} {QTY} {MES_SYMBOL} @ ~{es_price:.2f} Stop: {es_stop:.2f}")

    # 3. Target limit (skip for trail-only / Opt2 — saves margin, lets runners run)
    t1_oid = None
    if not trail_only:
        t1_payload = {
            "AccountID": account_id,
            "Symbol": MES_SYMBOL,
            "Quantity": str(QTY),
            "OrderType": "Limit",
            "LimitPrice": str(es_target),
            "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        t1_resp = _ts_api("POST", "/orderexecution/orders", t1_payload, account_id)
        t1_ok, t1_oid = _order_ok(t1_resp)
        if not t1_ok:
            t1_oid = None
            print(f"[real-trader] target limit skipped (margin?): {setup_name} "
                  f"target={es_target:.2f}", flush=True)

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "account_id": account_id,
        "entry_order_id": entry_oid,
        "target_order_id": t1_oid,
        "stop_order_id": stop_oid,
        "current_stop": es_stop,
        "target_price": es_target,
        "trail_only": trail_only,
        "status": "pending_entry",
        "fill_price": None,
        "max_favorable": 0.0,
        "be_triggered": False,
        "trail_active": False,
        "ts_placed": datetime.utcnow().isoformat(),
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    dir_str = "LONG" if is_long else "SHORT"
    tgt_str = "TRAIL-ONLY" if trail_only else f"{es_target:.2f}"
    print(f"[real-trader] PLACED: {setup_name} {dir_str} {QTY} {MES_SYMBOL} "
          f"@ ~{es_price:.2f} target={tgt_str} stop={es_stop:.2f} "
          f"acct={account_id} ids=entry:{entry_oid}/stop:{stop_oid}/tgt:{t1_oid}",
          flush=True)
    dir_label = "Long" if is_long else "Short"
    _alert(f"🟢 {setup_name} PLACED\n"
           f"{dir_label} {QTY} MES @ ~{es_price:.2f}\n"
           f"Target: {tgt_str} | Stop: {es_stop:.2f}")


def _place_limit_entry(setup_log_id, setup_name, direction, is_long,
                       account_id, es_price, stop_pts, target_pts,
                       limit_entry_price):
    """Charm S/R: place LIMIT entry only. Stop/target placed after fill (Phase 2)."""
    # Final safety check
    if not _validate_account_direction(account_id, is_long):
        return

    side = "Buy" if is_long else "Sell"
    limit_price = _round_mes(limit_entry_price)

    entry_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(QTY),
        "OrderType": "Limit",
        "LimitPrice": str(limit_price),
        "TradeAction": side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    print(f"[real-trader] PLACING LIMIT: {setup_name} {side} {QTY} {MES_SYMBOL} "
          f"LIMIT @ {limit_price:.2f} on {account_id}", flush=True)
    resp = _ts_api("POST", "/orderexecution/orders", entry_payload, account_id)
    ok, entry_oid = _order_ok(resp)
    if not ok:
        _alert(f"🚨 FAILED limit entry for {setup_name}\n"
               f"Account: {account_id}\n"
               f"Side: {side} {QTY} {MES_SYMBOL} LIMIT @ {limit_price:.2f}")
        return

    order = {
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "account_id": account_id,
        "entry_order_id": entry_oid,
        "target_order_id": None,
        "stop_order_id": None,
        "current_stop": None,
        "target_price": None,
        "status": "pending_limit",
        "fill_price": None,
        "max_favorable": 0.0,
        "be_triggered": False,
        "trail_active": False,
        "ts_placed": datetime.utcnow().isoformat(),
        "limit_entry_price": limit_price,
        "limit_placed_at": datetime.utcnow().isoformat(),
        "deferred_stop_pts": stop_pts,
        "deferred_target_pts": target_pts,
        "deferred_es_price": es_price,
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    dir_str = "LONG" if is_long else "SHORT"
    print(f"[real-trader] LIMIT placed: {setup_name} {dir_str} {QTY} {MES_SYMBOL} "
          f"LIMIT @ {limit_price:.2f} (market was {es_price:.2f}) "
          f"acct={account_id} id={entry_oid}", flush=True)
    dir_label = "Long" if is_long else "Short"
    _alert(f"🟢 {setup_name} LIMIT entry\n"
           f"{dir_label} {QTY} MES LIMIT @ {limit_price:.2f}\n"
           f"[CHARM S/R] Waiting for fill (market @ {es_price:.2f})")


def _place_deferred_protective_orders(lid, order, fill_price):
    """Phase 2: place stop + target orders after limit entry fills."""
    is_long = order["direction"].lower() in ("long", "bullish")
    account_id = order["account_id"]
    exit_side = "Sell" if is_long else "Buy"
    stop_pts = order["deferred_stop_pts"]
    target_pts = order.get("deferred_target_pts")
    setup_name = order["setup_name"]

    if is_long:
        es_stop = _round_mes(fill_price - stop_pts)
        es_target = _round_mes(fill_price + (target_pts if target_pts else FIRST_TARGET_PTS))
    else:
        es_stop = _round_mes(fill_price + stop_pts)
        es_target = _round_mes(fill_price - (target_pts if target_pts else FIRST_TARGET_PTS))

    # Final safety check
    if not _validate_account_direction(account_id, is_long):
        _alert(f"🚨 MANUAL INTERVENTION: {setup_name} limit FILLED "
               f"@ {fill_price:.2f} but direction validation FAILED!\n"
               f"Account: {account_id}")
        return

    # 1. Stop order
    stop_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(QTY),
        "OrderType": "StopMarket",
        "StopPrice": str(es_stop),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    stop_resp = _ts_api("POST", "/orderexecution/orders", stop_payload, account_id)
    stop_ok, stop_oid = _order_ok(stop_resp)
    if not stop_ok:
        stop_oid = None
        _alert(f"🚨 MANUAL INTERVENTION: {setup_name} limit FILLED "
               f"@ {fill_price:.2f} but STOP FAILED!\n"
               f"Account: {account_id}")

    # 2. Target limit
    t1_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(QTY),
        "OrderType": "Limit",
        "LimitPrice": str(es_target),
        "TradeAction": exit_side,
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }
    t1_resp = _ts_api("POST", "/orderexecution/orders", t1_payload, account_id)
    t1_ok, t1_oid = _order_ok(t1_resp)
    if not t1_ok:
        t1_oid = None

    # Update order state
    with _lock:
        order["stop_order_id"] = stop_oid
        order["target_order_id"] = t1_oid
        order["current_stop"] = es_stop
        order["target_price"] = es_target
    _persist_order(lid)

    imp_pts = abs(fill_price - order.get("deferred_es_price", fill_price))
    print(f"[real-trader] DEFERRED orders placed: {setup_name} "
          f"stop={es_stop:.2f} target={es_target:.2f} "
          f"(entry improved {imp_pts:.1f}pts from market) acct={account_id}", flush=True)
    _alert(f"🟢 {setup_name} LIMIT FILLED @ {fill_price:.2f}\n"
           f"[CHARM S/R] Improved {imp_pts:+.1f}pts from market "
           f"({order.get('deferred_es_price', 0):.2f})\n"
           f"Stop: {es_stop:.2f} | Target: {es_target:.2f}")


# ====== TRAIL & CLOSE ======

def update_stop(setup_log_id: int, new_stop_price: float):
    """Update the stop order price (called when trail advances).

    Safety layers:
      L1 Pre-check side-of-market: if market has already crossed the trail level,
         the exit signal has fired — execute immediate market close instead of
         submitting a doomed modify that TS would reject (wiping the stop and
         leaving the position bare).
      L2 Post-check PUT response: if TS returns Error=FAILED on the replace,
         emergency close. TS replace-or-reject semantics mean the original
         stop is gone once the modify is rejected.
    """
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] != "filled":
            return
        old_stop = order["current_stop"]
        if old_stop is None:
            return
        # Skip trivial changes (< 1 MES tick)
        if abs(new_stop_price - old_stop) < MES_TICK_SIZE:
            return
        stop_oid = order["stop_order_id"]
        account_id = order["account_id"]
        setup_name = order["setup_name"]
        direction = order["direction"]

    if not stop_oid:
        return

    new_stop_price = _round_mes(new_stop_price)
    is_long = direction.lower() in ("long", "bullish")

    # Validate account before modifying order
    if not _validate_account_direction(account_id, is_long):
        return

    # LAYER 1: side-of-market validation.
    # For short: stop is a BUY STOP — must live ABOVE current market.
    # For long: stop is a SELL STOP — must live BELOW current market.
    # If market has bounced past the trail's target, the trail's intended exit
    # was already crossed. Execute that exit at current market price instead of
    # trying to modify to a now-invalid price.
    current_mes = _get_current_mes_price()
    if current_mes is not None:
        SIDE_BUFFER = 0.5  # tolerate 2-tick race noise; real violations are >0.5pt
        wrong_side = False
        if is_long and new_stop_price >= current_mes - SIDE_BUFFER:
            wrong_side = True
            side_reason = (f"long trail {new_stop_price:.2f} >= market "
                           f"{current_mes:.2f} (exit already crossed)")
        elif not is_long and new_stop_price <= current_mes + SIDE_BUFFER:
            wrong_side = True
            side_reason = (f"short trail {new_stop_price:.2f} <= market "
                           f"{current_mes:.2f} (exit already crossed)")
        if wrong_side:
            print(f"[real-trader] WRONG-SIDE TRAIL id={setup_log_id}: "
                  f"{side_reason}", flush=True)
            _alert(f"⚠️ {setup_name} TRAIL-EXIT via market\n"
                   f"{side_reason}\n"
                   f"Closing at ~{current_mes:.2f}")
            close_trade(setup_log_id, "trail_market_exit")
            return

    replace_payload = {
        "AccountID": account_id,
        "Symbol": MES_SYMBOL,
        "Quantity": str(QTY),
        "OrderType": "StopMarket",
        "StopPrice": str(new_stop_price),
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    }

    resp = _ts_api("PUT", f"/orderexecution/orders/{stop_oid}", replace_payload, account_id)

    # LAYER 2: verify PUT was accepted. TS may return 200 with Error=FAILED at
    # order-level. A rejected replace wipes the original stop → position bare.
    if resp:
        resp_orders = resp.get("Orders", [])
        if resp_orders and resp_orders[0].get("Error") == "FAILED":
            err_msg = resp_orders[0].get("Message", "unknown")[:120]
            print(f"[real-trader] stop modify REJECTED id={setup_log_id} "
                  f"new={new_stop_price:.2f}: {err_msg}", flush=True)
            _alert(f"🚨 {setup_name} STOP MODIFY REJECTED\n"
                   f"{err_msg}\n"
                   f"Closing at market to avoid bare position...")
            close_trade(setup_log_id, "modify_rejected")
            return

        with _lock:
            order["current_stop"] = new_stop_price
            new_orders = resp.get("Orders", [])
            if new_orders and new_orders[0].get("OrderID"):
                order["stop_order_id"] = new_orders[0]["OrderID"]
            # Mark first post-entry realign consumed; all later updates alert normally
            first_realign = not order.get("initial_realign_done")
            order["initial_realign_done"] = True
        _persist_order(setup_log_id)
        print(f"[real-trader] stop updated: id={setup_log_id} "
              f"{old_stop:.2f} -> {new_stop_price:.2f} acct={account_id}"
              f"{' [initial-realign]' if first_realign else ''}", flush=True)
        # Suppress Telegram only for the first update after entry IF delta is small
        # (pure entry-slippage realign). A big first-cycle move (fast trail activation)
        # still alerts. Every subsequent update alerts regardless.
        small_first_realign = first_realign and abs(new_stop_price - old_stop) < 3.0
        if not small_first_realign:
            _alert(f"🔄 {setup_name} stop updated\n"
                   f"{old_stop:.2f} → {new_stop_price:.2f}")
    else:
        # Network/401/timeout. Existing stop may still be live — do NOT
        # emergency close. Just alert for manual review.
        _alert(f"🚨 MANUAL INTERVENTION: stop update FAILED (network)\n"
               f"Account: {account_id}\n"
               f"id={setup_log_id} old={old_stop:.2f} new={new_stop_price:.2f}")


def close_trade(setup_log_id: int, result_type: str):
    """Close a trade on outcome resolution.
    Cancel remaining orders + market close if position still open.
    NOTE: force_release() may have already set status='closed' to free the
    concurrent slot. We still need to do broker cleanup (cancel stop/target),
    so do NOT early-return on status=='closed'."""
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        already_closed = order["status"] == "closed"

    setup_name = order["setup_name"]
    account_id = order["account_id"]

    # Flatten: cancel pending orders + market close (idempotent — safe to re-run)
    _flatten_position(order)

    if not already_closed:
        with _lock:
            order["status"] = "closed"
            order["close_reason"] = result_type
        _persist_order(setup_log_id)
        print(f"[real-trader] closed: {setup_name} id={setup_log_id} "
              f"result={result_type} acct={account_id}", flush=True)
        _alert(f"🏁 {setup_name} CLOSED: {result_type}")
    else:
        print(f"[real-trader] broker cleanup done (slot already released): "
              f"{setup_name} id={setup_log_id} acct={account_id}", flush=True)


def force_release(setup_log_id: int, result_type: str):
    """Immediately free the concurrent slot for a resolved trade.

    Called DIRECTLY (not via _broker_submit) by outcome tracker to guarantee
    the slot is freed. close_trade() is still called in background for broker
    cleanup, but this ensures MAX_CONCURRENT_PER_DIR is not blocked if
    close_trade fails or is delayed.

    Bug 2026-04-06: #1559 close_trade via _broker_submit silently failed.
    _active_orders kept #1559 as 'filled', blocking 7 subsequent SC shorts
    (+45 pts missed).
    """
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] == "closed":
            return
        old_status = order["status"]
        order["status"] = "closed"
        order["close_reason"] = result_type
    try:
        _persist_order(setup_log_id)
    except Exception as e:
        print(f"[real-trader] force_release persist error: {e}", flush=True)
    print(f"[real-trader] force_release: id={setup_log_id} {old_status}->{result_type}", flush=True)


def _flatten_position(order):
    """Market close remaining position + cancel all pending orders."""
    account_id = order["account_id"]
    is_long = order["direction"].lower() in ("long", "bullish")
    close_side = "Sell" if is_long else "Buy"

    # Validate before any order modification
    if not _validate_account_direction(account_id, is_long):
        _alert(f"🚨 CRITICAL: Cannot flatten -- direction validation failed!\n"
               f"Account: {account_id} | {order.get('setup_name')}")
        return

    # Cancel pending stop and target orders FIRST
    for oid_key in ("stop_order_id", "target_order_id"):
        oid = order.get(oid_key)
        if oid:
            _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, account_id)

    # Cancel pending limit entry if not yet filled
    if order.get("status") == "pending_limit" and order.get("entry_order_id"):
        _ts_api("DELETE", f"/orderexecution/orders/{order['entry_order_id']}", None, account_id)
        print(f"[real-trader] cancelled pending limit entry: {order['setup_name']} "
              f"acct={account_id}", flush=True)
        _alert(f"🏁 {order['setup_name']} limit entry cancelled")
        return

    # Wait for cancellations to settle
    time.sleep(0.5)

    # Market close if position exists
    if order["status"] == "filled":
        # Check broker position first -- don't create ghost positions
        broker_pos = _get_broker_position(account_id)
        if not broker_pos:
            print(f"[real-trader] flatten SKIPPED: broker already flat on {account_id} "
                  f"(stop/target filled). {order['setup_name']}", flush=True)
            return

        # Verify direction matches
        broker_is_long = broker_pos["long_short"] == "Long"
        if broker_is_long != is_long:
            print(f"[real-trader] flatten SKIPPED: direction mismatch on {account_id}! "
                  f"Expected={'Long' if is_long else 'Short'} "
                  f"Actual={broker_pos['long_short']} qty={broker_pos['qty']}", flush=True)
            _alert(f"⚠️ POSITION MISMATCH on {account_id}\n"
                   f"Expected: {'Long' if is_long else 'Short'}\n"
                   f"Broker: {broker_pos['long_short']} {broker_pos['qty']}\n"
                   f"MANUAL REVIEW NEEDED")
            return

        close_qty = min(QTY, broker_pos["qty"])
        close_payload = {
            "AccountID": account_id,
            "Symbol": MES_SYMBOL,
            "Quantity": str(close_qty),
            "OrderType": "Market",
            "TradeAction": close_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        resp = _ts_api("POST", "/orderexecution/orders", close_payload, account_id)
        if resp:
            # Check for rejection
            orders_list = resp.get("Orders", [])
            if orders_list and orders_list[0].get("Error") == "FAILED":
                msg = orders_list[0].get("Message", "")
                _alert(f"🚨 FLATTEN REJECTED on {account_id}\n"
                       f"{order['setup_name']} qty={close_qty}\n"
                       f"Error: {msg}\nMANUAL CLOSE REQUIRED")
                return
            # Capture fill price
            close_oid = None
            if orders_list:
                close_oid = orders_list[0].get("OrderID")
            if close_oid:
                time.sleep(1)
                close_fp = _get_order_fill_price(close_oid, account_id)
                if close_fp:
                    order["close_fill_price"] = close_fp
            print(f"[real-trader] flattened: {order['setup_name']} qty={close_qty} "
                  f"fill={order.get('close_fill_price')} acct={account_id}", flush=True)
        else:
            _alert(f"🚨 FLATTEN FAILED on {account_id}\n"
                   f"{order['setup_name']} id={order['setup_log_id']} qty={close_qty}\n"
                   f"MANUAL CLOSE REQUIRED")


# ====== POLL ORDER STATUS ======

def poll_order_status():
    """Check order fills via TS API. Called each ~30s cycle."""
    if not (LONGS_ENABLED or SHORTS_ENABLED):
        return
    with _lock:
        if not _active_orders:
            return
        pending = [(lid, o) for lid, o in _active_orders.items()
                   if o["status"] in ("pending_entry", "pending_limit", "filled")]
    if not pending:
        return

    # Group by account to minimize API calls
    by_account: dict[str, list] = {}
    for lid, o in pending:
        acct = o.get("account_id", "")
        if acct:
            by_account.setdefault(acct, []).append((lid, o))

    for account_id, order_list in by_account.items():
        if account_id not in ACCOUNT_WHITELIST:
            continue
        try:
            orders_data = _ts_api("GET",
                f"/brokerage/accounts/{account_id}/orders", None, account_id)
        except Exception as e:
            print(f"[real-trader] poll error for {account_id}: {e}", flush=True)
            continue
        if not orders_data:
            continue

        broker_orders = {}
        for o in orders_data.get("Orders", []):
            oid = o.get("OrderID")
            if oid:
                broker_orders[oid] = o

        for lid, order in order_list:
            _check_order_fills(lid, order, broker_orders)

    # Note: position reconciliation was previously throttled inside this
    # function but early-exited above when bot state was empty (leaving a
    # 5-min exposure window covered only by periodic_orphan_check). It is
    # now driven by a dedicated 30s scheduler job calling reconcile_positions()
    # which runs regardless of tracked-order state.


def reconcile_positions():
    """Public entry point for the 30s reconcile scheduler.

    Runs regardless of whether bot state is empty — catches the case where
    force_release has marked a trade closed but broker still holds position
    (e.g., when a stop modify was rejected async and the original stop wiped).
    See #2018, #2031 on 2026-04-21."""
    if not (LONGS_ENABLED or SHORTS_ENABLED) or not _get_token:
        return
    try:
        _reconcile_positions()
    except Exception as e:
        print(f"[real-trader] reconcile_positions error: {e}", flush=True)


def _reconcile_positions():
    """Check broker positions match tracked orders. Alert on mismatch."""
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id not in ACCOUNT_WHITELIST:
            continue
        # Count expected qty from tracked orders
        with _lock:
            expected_qty = sum(
                QTY for o in _active_orders.values()
                if o.get("account_id") == acct_id and o["status"] == "filled"
            )
        # Query broker
        broker_pos = _get_broker_position(acct_id)
        broker_qty = broker_pos["qty"] if broker_pos else 0
        if broker_qty != expected_qty:
            print(f"[real-trader] RECONCILE MISMATCH on {acct_id}: "
                  f"expected={expected_qty} broker={broker_qty}", flush=True)
            if broker_qty > 0 and expected_qty == 0:
                # Orphan: broker has position we don't track
                _alert(f"⚠️ POSITION MISMATCH on {acct_id}\n"
                       f"Expected: {expected_qty} MES\n"
                       f"Broker: {broker_qty} MES\n"
                       f"ORPHAN detected -- auto-closing")
                _close_broker_orphans(acct_id, source="RECONCILE")
            elif broker_qty == 0 and expected_qty > 0:
                # Ghost: we think we have position but broker doesn't
                # Before marking closed, try to recover the actual fill price
                # from broker order history so P&L accounting stays accurate.
                _ghost_ids = []
                _backfilled = []
                _lost_price = []
                with _lock:
                    _to_process = [
                        (lid, o) for lid, o in _active_orders.items()
                        if o.get("account_id") == acct_id and o["status"] == "filled"
                    ]
                # Release lock while querying broker history (network call)
                for lid, o in _to_process:
                    backfill = _backfill_ghost_fill(o)
                    with _lock:
                        if backfill:
                            field, price = backfill
                            o[field] = price
                            _backfilled.append((lid, field, price))
                        else:
                            _lost_price.append(lid)
                        o["status"] = "closed"
                        o["close_reason"] = "ghost_reconcile"
                        _ghost_ids.append(o["setup_log_id"])
                for _gid in _ghost_ids:
                    _persist_order(_gid)
                # Alert with backfill detail — tells user if accounting is accurate or not
                _msg = (f"⚠️ GHOST POSITION on {acct_id}\n"
                        f"Expected: {expected_qty} MES · Broker: FLAT\n"
                        f"Marked {len(_ghost_ids)} closed")
                if _backfilled:
                    _msg += f"\n✅ Recovered {len(_backfilled)} fill price(s): "
                    _msg += ", ".join(f"lid={l} {f}={p}" for l, f, p in _backfilled)
                if _lost_price:
                    _msg += f"\n🚨 Could not recover fill for {len(_lost_price)} trade(s): {_lost_price}"
                _alert(_msg)
                print(f"[real-trader] ghost_reconcile {acct_id}: backfilled={_backfilled} lost={_lost_price}", flush=True)
            elif broker_qty != expected_qty:
                # Qty mismatch
                _alert(f"⚠️ QTY MISMATCH on {acct_id}\n"
                       f"Expected: {expected_qty} MES\n"
                       f"Broker: {broker_qty} MES\n"
                       f"Check manually")


def _check_order_fills(lid, order, broker_orders):
    """Check individual order fills and update state."""
    changed = False
    account_id = order.get("account_id", "")

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
            _place_deferred_protective_orders(lid, order, fill_price)
        elif entry_status in ("REJ", "CAN", "EXP"):
            with _lock:
                order["status"] = "closed"
                order["close_reason"] = f"limit_{entry_status}"
            changed = True
            print(f"[real-trader] limit entry {entry_status}: {order['setup_name']} "
                  f"acct={account_id}", flush=True)
            _alert(f"⚠️ {order['setup_name']} LIMIT {entry_status}\n"
                   f"[CHARM S/R] Entry not filled -- trade skipped")
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
                        _ts_api("DELETE",
                                f"/orderexecution/orders/{order['entry_order_id']}",
                                None, account_id)
                        with _lock:
                            order["status"] = "closed"
                            order["close_reason"] = "limit_timeout"
                        changed = True
                        print(f"[real-trader] LIMIT TIMEOUT: {order['setup_name']} cancelled "
                              f"after {elapsed/60:.0f} min acct={account_id}", flush=True)
                        _alert(f"🏁 {order['setup_name']} LIMIT EXPIRED\n"
                               f"[CHARM S/R] {order.get('limit_entry_price', 0):.2f} not reached "
                               f"in {elapsed/60:.0f} min -- trade skipped")
                except (ValueError, TypeError):
                    pass

    # Check market entry fill
    if order["status"] == "pending_entry" and order.get("entry_order_id"):
        entry = broker_orders.get(order["entry_order_id"], {})
        entry_status = entry.get("Status", "")
        if entry_status == "FLL":
            fill_price = _extract_fill_price(entry)
            with _lock:
                order["status"] = "filled"
                order["fill_price"] = fill_price
            changed = True
            print(f"[real-trader] FILLED: {order['setup_name']} "
                  f"{QTY} {MES_SYMBOL} @ {fill_price} acct={account_id}", flush=True)
            dir_label = "Long" if order["direction"].lower() in ("long", "bullish") else "Short"
            _alert(f"🟢 {order['setup_name']} FILLED\n"
                   f"{dir_label} {QTY} MES @ {fill_price}\n"
                   f"Target: {order.get('target_price', 0):.2f} | "
                   f"Stop: {order['current_stop']:.2f}")
        elif entry_status in ("REJ", "CAN", "EXP"):
            with _lock:
                order["status"] = "closed"
                order["close_reason"] = f"entry_{entry_status}"
            changed = True
            rej_reason = (entry.get("RejectReason") or entry.get("StatusDescription")
                          or entry.get("Message", ""))
            print(f"[real-trader] entry {entry_status}: {order['setup_name']} "
                  f"reason={rej_reason} acct={account_id}", flush=True)
            _alert(f"⚠️ {order['setup_name']} entry {entry_status}\n"
                   f"Reason: {rej_reason}")

    # Check target/stop fills for active positions
    if order["status"] == "filled":
        # Check target fill
        if order.get("target_order_id"):
            tgt = broker_orders.get(order["target_order_id"], {})
            if tgt.get("Status") == "FLL":
                tgt_fp = _extract_fill_price(tgt)
                with _lock:
                    order["target_fill_price"] = tgt_fp
                    order["status"] = "closed"
                    order["close_reason"] = "target_filled"
                changed = True
                # Cancel stop since target filled (verify + retry)
                if order.get("stop_order_id"):
                    _cancel_order_verified(order["stop_order_id"], account_id,
                                           f"stop after target fill ({order['setup_name']})")
                pnl = None
                if tgt_fp and order.get("fill_price"):
                    is_long = order["direction"].lower() in ("long", "bullish")
                    if is_long:
                        pnl = (tgt_fp - order["fill_price"]) * MES_POINT_VALUE * QTY
                    else:
                        pnl = (order["fill_price"] - tgt_fp) * MES_POINT_VALUE * QTY
                pnl_str = f"${pnl:.2f}" if pnl is not None else "n/a"
                dir_label = "Long" if order["direction"].lower() in ("long", "bullish") else "Short"
                print(f"[real-trader] TARGET filled: {order['setup_name']} "
                      f"@ {tgt_fp} pnl={pnl_str} acct={account_id}", flush=True)
                _alert(f"🏁 {order['setup_name']} TARGET FILLED\n"
                       f"{dir_label} {QTY} MES @ {tgt_fp}\n"
                       f"P&L: {pnl_str}"
                       f"{_day_line(account_id)}")

        # Check stop REJ — TS can reject a stop-modify asynchronously even after
        # PUT returned 200. Replace-or-reject semantics mean the original stop
        # is wiped. Detect + emergency close before position stays bare.
        if order.get("stop_order_id") and order["status"] == "filled":
            stop_order = broker_orders.get(order["stop_order_id"], {})
            if stop_order.get("Status") == "REJ":
                rej_reason = (stop_order.get("StatusDescription")
                              or stop_order.get("RejectReason")
                              or "rejected async by exchange")
                print(f"[real-trader] stop REJECTED async id={lid}: {rej_reason} "
                      f"acct={account_id}", flush=True)
                _alert(f"🚨 {order['setup_name']} STOP REJECTED BY EXCHANGE\n"
                       f"{rej_reason[:120]}\n"
                       f"Closing at market to avoid bare position...")
                close_trade(lid, "stop_rejected_async")
                return  # stop processing this order; state is now closed

        # Check stop fill
        if order.get("stop_order_id") and order["status"] == "filled":
            stop_order = broker_orders.get(order["stop_order_id"], {})
            if stop_order.get("Status") == "FLL":
                stop_fp = _extract_fill_price(stop_order)
                with _lock:
                    order["stop_fill_price"] = stop_fp
                    order["status"] = "closed"
                    order["close_reason"] = "stop_filled"
                changed = True
                # Cancel target since stop filled (verify + retry)
                if order.get("target_order_id"):
                    _cancel_order_verified(order["target_order_id"], account_id,
                                           f"target after stop fill ({order['setup_name']})")
                pnl = None
                if stop_fp and order.get("fill_price"):
                    is_long = order["direction"].lower() in ("long", "bullish")
                    if is_long:
                        pnl = (stop_fp - order["fill_price"]) * MES_POINT_VALUE * QTY
                    else:
                        pnl = (order["fill_price"] - stop_fp) * MES_POINT_VALUE * QTY
                pnl_str = f"${pnl:.2f}" if pnl is not None else "n/a"
                dir_label = "Long" if order["direction"].lower() in ("long", "bullish") else "Short"
                print(f"[real-trader] STOP filled: {order['setup_name']} "
                      f"@ {stop_fp} pnl={pnl_str} acct={account_id}", flush=True)
                _alert(f"🏁 {order['setup_name']} STOP FILLED\n"
                       f"{dir_label} {QTY} MES @ {stop_fp}\n"
                       f"P&L: {pnl_str}"
                       f"{_day_line(account_id)}")

    if changed:
        _persist_order(lid)


def _cancel_order_verified(order_id: str, account_id: str, label: str, retries: int = 3):
    """Cancel an order and verify it was actually cancelled. Retry + alert on failure."""
    for attempt in range(retries):
        _ts_api("DELETE", f"/orderexecution/orders/{order_id}", None, account_id)
        time.sleep(1)  # give TS time to process
        # Verify: query the order status
        resp = _ts_api("GET", f"/brokerage/accounts/{account_id}/orders", None, account_id)
        if resp:
            orders = resp.get("Orders", [])
            still_active = any(
                o.get("OrderID") == str(order_id) and o.get("Status") in ("OPN", "ACK", "UCN", "DON")
                for o in orders
            )
            if not still_active:
                print(f"[real-trader] cancel verified: {label} order_id={order_id} (attempt {attempt+1})", flush=True)
                return
            print(f"[real-trader] cancel NOT confirmed: {label} order_id={order_id} "
                  f"(attempt {attempt+1}/{retries}), retrying...", flush=True)
        else:
            print(f"[real-trader] cancel verify failed (no response): {label} order_id={order_id}", flush=True)
    # All retries exhausted
    _alert(f"🚨 CRITICAL: Failed to cancel {label}\n"
           f"Order ID: {order_id}\nAccount: {account_id}\n"
           f"MANUAL CANCEL REQUIRED!")


def _extract_fill_price(entry_order: dict) -> float | None:
    """Extract fill price from a broker order response."""
    try:
        fp = float(entry_order.get("FilledPrice", 0))
        if fp > 0:
            return fp
    except (ValueError, TypeError):
        pass
    fills = entry_order.get("Legs", [{}])
    if fills:
        try:
            ep = float(fills[0].get("ExecPrice", 0))
            if ep > 0:
                return ep
        except (ValueError, TypeError):
            pass
    return None


def _get_order_fill_price(order_id: str, account_id: str) -> float | None:
    """Get fill price for a specific order by polling broker.
    Checks BOTH live /orders AND /historicalorders (filled orders may have moved
    to history between our polls). Returns fill price or None if not found/not filled."""
    if account_id not in ACCOUNT_WHITELIST:
        return None
    # 1) Try live orders first (faster, always current)
    try:
        data = _ts_api("GET", f"/brokerage/accounts/{account_id}/orders", None, account_id)
        if data:
            for o in data.get("Orders", []):
                if o.get("OrderID") == order_id and o.get("Status") == "FLL":
                    return _extract_fill_price(o)
    except Exception:
        pass
    # 2) Fall back to historical orders (stop fills often move to history quickly)
    try:
        from datetime import datetime as _dt, timezone as _tz
        # Use today's date in UTC — covers intraday stop fills
        today = _dt.now(_tz.utc).strftime("%m-%d-%Y")
        data = _ts_api("GET",
                       f"/brokerage/accounts/{account_id}/historicalorders?since={today}&pageSize=600",
                       None, account_id)
        if data:
            for o in data.get("Orders", []):
                if o.get("OrderID") == order_id and o.get("Status") == "FLL":
                    return _extract_fill_price(o)
    except Exception as e:
        print(f"[real-trader] historicalorders lookup failed for {order_id}: {e}", flush=True)
    return None


def _backfill_ghost_fill(order: dict) -> tuple[str, float] | None:
    """Try to recover the real fill price of a position that the bot missed.
    Checks stop_order_id → target_order_id → entry_order_id (reverse chronological).
    Returns (field_name, fill_price) or None if nothing found. Safe to call — all
    exceptions swallowed to keep reconciliation working under any broker hiccup."""
    acct = order.get("account_id", "")
    if acct not in ACCOUNT_WHITELIST:
        return None
    try:
        # Check the closing orders first — if broker says flat, one of these must have filled
        for field, oid_key in (("stop_fill_price", "stop_order_id"),
                                ("target_fill_price", "target_order_id")):
            oid = order.get(oid_key)
            if not oid:
                continue
            fp = _get_order_fill_price(oid, acct)
            if fp is not None and fp > 0:
                return (field, float(fp))
    except Exception as e:
        print(f"[real-trader] ghost backfill error for lid={order.get('setup_log_id')}: {e}", flush=True)
    return None


# ====== SC TRAIL LOGIC ======

def update_trail(setup_log_id: int, current_es_price: float):
    """Update trailing stop based on current ES price.
    SC Trail: BE trigger=10, activation=10, gap=5.

    Called externally (from main.py's outcome tracking loop) with the current ES price.
    This function tracks max favorable excursion and advances the stop accordingly.
    """
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] != "filled":
            return
        fill_price = order.get("fill_price")
        if not fill_price:
            return

    is_long = order["direction"].lower() in ("long", "bullish")

    # Calculate current profit
    if is_long:
        profit = current_es_price - fill_price
    else:
        profit = fill_price - current_es_price

    # Update max favorable excursion
    with _lock:
        if profit > order.get("max_favorable", 0):
            order["max_favorable"] = profit

        max_fav = order["max_favorable"]
        current_stop = order.get("current_stop")
        if current_stop is None:
            return

        new_stop = current_stop  # default: no change

        # Phase 1: Breakeven trigger
        if not order.get("be_triggered") and max_fav >= BE_TRIGGER_PTS:
            order["be_triggered"] = True
            if is_long:
                be_stop = _round_mes(fill_price + BE_BUFFER_PTS)
                if be_stop > current_stop:
                    new_stop = be_stop
            else:
                be_stop = _round_mes(fill_price - BE_BUFFER_PTS)
                if be_stop < current_stop:
                    new_stop = be_stop

        # Phase 2: Trail activation
        if max_fav >= TRAIL_ACTIVATION_PTS:
            order["trail_active"] = True
            trail_stop = _round_mes(fill_price + (max_fav - TRAIL_GAP_PTS)) if is_long else \
                         _round_mes(fill_price - (max_fav - TRAIL_GAP_PTS))
            # Trail only moves forward (tighter)
            if is_long and trail_stop > new_stop:
                new_stop = trail_stop
            elif not is_long and trail_stop < new_stop:
                new_stop = trail_stop

    # Apply stop update if changed
    if new_stop != current_stop:
        # Trail only moves in protective direction
        if is_long and new_stop > current_stop:
            update_stop(setup_log_id, new_stop)
        elif not is_long and new_stop < current_stop:
            update_stop(setup_log_id, new_stop)


# ====== EOD FLATTEN ======

def flatten_all_eod():
    """Force-close all open REAL positions at end of day.
    Called by scheduler at 15:55 ET before market close."""
    # Track the flatten close order ID per account so we can look up actual fill price
    # for per-trade P&L reporting in Phase 1d
    _close_oids: dict[str, str] = {}
    with _lock:
        open_orders = [(lid, o) for lid, o in _active_orders.items()
                       if o["status"] in ("pending_entry", "pending_limit", "filled")]
    if not open_orders:
        print("[real-trader] EOD flatten: no tracked positions", flush=True)
    else:
        print(f"[real-trader] EOD flatten: closing {len(open_orders)} tracked position(s)",
              flush=True)
        _alert(f"⚠️ EOD FLATTEN: closing {len(open_orders)} position(s)")

        # Phase 1a: Cancel ALL orders across ALL tracked trades first
        cancelled = 0
        for lid, order in open_orders:
            account_id = order.get("account_id", "")
            if account_id not in ACCOUNT_WHITELIST:
                continue
            for oid_key in ("entry_order_id", "stop_order_id", "target_order_id"):
                # Cancel entry only for pending_limit
                if oid_key == "entry_order_id" and order.get("status") != "pending_limit":
                    continue
                oid = order.get(oid_key)
                if oid:
                    try:
                        _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, account_id)
                        cancelled += 1
                    except Exception:
                        pass
        print(f"[real-trader] EOD: cancelled {cancelled} orders", flush=True)

        # Phase 1b: Wait for cancellations to settle
        time.sleep(3)

        # Phase 1c: Close actual broker positions with retry
        for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
            if acct_id not in ACCOUNT_WHITELIST:
                continue
            broker_pos = _get_broker_position(acct_id)
            if not broker_pos:
                print(f"[real-trader] EOD: {acct_id} already flat", flush=True)
                continue

            close_side = "Sell" if broker_pos["long_short"] == "Long" else "Buy"
            closed = False
            close_oid_for_acct = None
            for attempt, wait in enumerate([0, 3, 5, 10], start=1):
                if attempt > 1:
                    print(f"[real-trader] EOD close retry #{attempt} after {wait}s wait "
                          f"on {acct_id}...", flush=True)
                    time.sleep(wait)
                    broker_pos = _get_broker_position(acct_id)
                    if not broker_pos:
                        print(f"[real-trader] EOD: {acct_id} closed during wait", flush=True)
                        closed = True
                        break
                    close_side = "Sell" if broker_pos["long_short"] == "Long" else "Buy"

                close_payload = {
                    "AccountID": acct_id,
                    "Symbol": broker_pos["symbol"],
                    "Quantity": str(broker_pos["qty"]),
                    "OrderType": "Market",
                    "TradeAction": close_side,
                    "TimeInForce": {"Duration": "DAY"},
                    "Route": "Intelligent",
                }
                resp = _ts_api("POST", "/orderexecution/orders", close_payload, acct_id)
                if resp:
                    orders_list = resp.get("Orders", [])
                    if orders_list and orders_list[0].get("Error") == "FAILED":
                        msg = orders_list[0].get("Message", "")
                        print(f"[real-trader] EOD close rejected on {acct_id} "
                              f"(attempt {attempt}): {msg}", flush=True)
                        continue
                    # Capture close order ID so we can look up the actual fill price for P&L
                    if orders_list:
                        close_oid_for_acct = orders_list[0].get("OrderID")
                    print(f"[real-trader] EOD: closed {broker_pos['long_short']} "
                          f"{broker_pos['qty']} MES on {acct_id} (attempt {attempt}) "
                          f"close_oid={close_oid_for_acct}", flush=True)
                    closed = True
                    break
                else:
                    print(f"[real-trader] EOD close API error on {acct_id} "
                          f"(attempt {attempt})", flush=True)

            if closed and close_oid_for_acct:
                _close_oids[acct_id] = close_oid_for_acct

            if not closed:
                _alert(f"🚨 EOD CLOSE FAILED after 4 attempts\n"
                       f"Account: {acct_id}\n"
                       f"{broker_pos['long_short']} {broker_pos['qty']} MES\n"
                       f"MANUAL CLOSE REQUIRED IMMEDIATELY")

        # Let close orders settle on broker so fill price is retrievable
        time.sleep(3)

        # Phase 1d: Mark all tracked trades as closed and send per-trade P&L Telegram
        for lid, order in open_orders:
            acct_id = order.get("account_id", "")
            close_fp = None
            close_oid = _close_oids.get(acct_id)
            if close_oid:
                try:
                    close_fp = _get_order_fill_price(close_oid, acct_id)
                except Exception as e:
                    print(f"[real-trader] EOD fill lookup failed for {close_oid}: {e}",
                          flush=True)
            with _lock:
                order["status"] = "closed"
                order["close_reason"] = "eod_flatten"
                if close_fp is not None:
                    order["stop_fill_price"] = close_fp
            _persist_order(lid)

            # Compute P&L and send Telegram per trade
            entry_fp = order.get("fill_price")
            pnl = None
            if close_fp is not None and entry_fp is not None:
                is_long = order["direction"].lower() in ("long", "bullish")
                if is_long:
                    pnl = (close_fp - entry_fp) * MES_POINT_VALUE * QTY
                else:
                    pnl = (entry_fp - close_fp) * MES_POINT_VALUE * QTY
            pnl_str = f"${pnl:.2f}" if pnl is not None else "n/a"
            dir_label = "Long" if order["direction"].lower() in ("long", "bullish") else "Short"
            if close_fp is not None:
                _alert(f"🏁 {order['setup_name']} EOD FLATTEN\n"
                       f"{dir_label} {QTY} MES @ {close_fp}\n"
                       f"P&L: {pnl_str}"
                       f"{_day_line(acct_id)}")
            else:
                # Fallback if we couldn't retrieve fill price
                _alert(f"🏁 {order['setup_name']} EOD FLATTEN\n"
                       f"{dir_label} {QTY} MES\n"
                       f"P&L: n/a (fill price unavailable)"
                       f"{_day_line(acct_id)}")
            print(f"[real-trader] EOD marked closed: {order['setup_name']} id={lid} "
                  f"acct={acct_id} close_fp={close_fp} pnl={pnl_str}", flush=True)

    # Phase 2: Cancel ALL remaining open orders on both accounts
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id not in ACCOUNT_WHITELIST:
            continue
        try:
            ord_data = _ts_api("GET", f"/brokerage/accounts/{acct_id}/orders", None, acct_id)
            for o in (ord_data or {}).get("Orders", []):
                status = o.get("Status", "")
                if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
                    continue
                oid = o.get("OrderID")
                if oid:
                    _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, acct_id)
                    print(f"[real-trader] EOD: cancelled remaining order {oid} "
                          f"on {acct_id}", flush=True)
        except Exception as e:
            print(f"[real-trader] EOD order sweep error on {acct_id}: {e}", flush=True)

    # Phase 3: Close any orphaned positions
    time.sleep(1)
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id in ACCOUNT_WHITELIST:
            _close_broker_orphans(acct_id, source="EOD")

    # Phase 4: Final verification -- confirm we are flat on both accounts
    time.sleep(1)
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id not in ACCOUNT_WHITELIST:
            continue
        final_pos = _get_broker_position(acct_id)
        if final_pos:
            print(f"[real-trader] EOD CRITICAL: still have position on {acct_id}! "
                  f"{final_pos['long_short']} {final_pos['qty']} {final_pos['symbol']}",
                  flush=True)
            _alert(f"🚨 EOD FLATTEN FAILED on {acct_id}\n"
                   f"STILL OPEN: {final_pos['long_short']} {final_pos['qty']}\n"
                   f"MANUAL INTERVENTION REQUIRED IMMEDIATELY")
            # Last-resort retry
            try:
                _flatten_account(acct_id)
                time.sleep(1)
                still_open = _get_broker_position(acct_id)
                if still_open:
                    print(f"[real-trader] EOD FLATTEN FAILED FINAL on {acct_id}: "
                          f"{still_open}", flush=True)
                else:
                    print(f"[real-trader] EOD retry flatten succeeded on {acct_id}", flush=True)
            except Exception as e:
                print(f"[real-trader] EOD retry flatten error on {acct_id}: {e}", flush=True)
        else:
            print(f"[real-trader] EOD flatten verified flat on {acct_id}", flush=True)


def _flatten_account(account_id: str):
    """Close all positions + cancel all orders on a specific account."""
    if account_id not in ACCOUNT_WHITELIST:
        print(f"[real-trader] BLOCKED: _flatten_account on non-whitelisted {account_id}",
              flush=True)
        return

    # Cancel all open orders
    try:
        ord_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/orders", None, account_id)
        for o in (ord_data or {}).get("Orders", []):
            status = o.get("Status", "")
            if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
                continue
            oid = o.get("OrderID")
            if oid:
                _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, account_id)
    except Exception as e:
        print(f"[real-trader] flatten-account order cancel error on {account_id}: {e}",
              flush=True)

    time.sleep(1)

    # Close all positions
    try:
        pos_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/positions", None, account_id)
        for pos in (pos_data or {}).get("Positions", []):
            symbol = pos.get("Symbol", "")
            # TS futures positions return signed Quantity (e.g. "-1" for shorts) — use abs
            qty = abs(int(pos.get("Quantity", "0")))
            long_short = pos.get("LongShort", "")
            if qty == 0:
                continue
            close_side = "Sell" if long_short == "Long" else "Buy"
            close_payload = {
                "AccountID": account_id,
                "Symbol": symbol,
                "Quantity": str(qty),
                "OrderType": "Market",
                "TradeAction": close_side,
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            }
            resp = _ts_api("POST", "/orderexecution/orders", close_payload, account_id)
            if resp:
                orders_list = resp.get("Orders", [])
                if orders_list and orders_list[0].get("Error") == "FAILED":
                    msg = orders_list[0].get("Message", "")
                    print(f"[real-trader] flatten-account REJECTED on {account_id}: {msg}",
                          flush=True)
                    _alert(f"🚨 FLATTEN REJECTED on {account_id}\n"
                           f"{long_short} {qty} {symbol}: {msg}")
                else:
                    print(f"[real-trader] flatten-account: closed {long_short} {qty} {symbol} "
                          f"on {account_id}", flush=True)
    except Exception as e:
        print(f"[real-trader] flatten-account position close error on {account_id}: {e}",
              flush=True)


# ====== ORPHAN DETECTION ======

def _close_broker_orphans(account_id: str, source: str = "EOD"):
    """Check broker for positions not tracked in _active_orders. Close any orphans."""
    if account_id not in ACCOUNT_WHITELIST:
        return

    try:
        pos_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/positions", None, account_id)
        positions = (pos_data or {}).get("Positions", [])
    except Exception as e:
        print(f"[real-trader] {source} orphan check failed on {account_id}: {e}", flush=True)
        return

    if not positions:
        return

    # Check which positions are tracked
    with _lock:
        tracked_acct_directions = set()
        for o in _active_orders.values():
            if o["status"] in ("pending_entry", "filled") and o.get("account_id") == account_id:
                d = o["direction"].lower()
                tracked_acct_directions.add("Long" if d in ("long", "bullish") else "Short")

    for pos in positions:
        symbol = pos.get("Symbol", "")
        # TS futures positions return signed Quantity (e.g. "-1" for shorts) — use abs
        qty = abs(int(pos.get("Quantity", "0")))
        long_short = pos.get("LongShort", "")
        if qty == 0:
            continue
        if long_short in tracked_acct_directions:
            print(f"[real-trader] {source} orphan check on {account_id}: "
                  f"{long_short} {qty} {symbol} -- matches tracked, OK", flush=True)
            continue

        print(f"[real-trader] WARNING: {source} orphan on {account_id} -- "
              f"{long_short} {qty} {symbol}. Closing...", flush=True)
        _alert(f"⚠️ {source} ORPHAN on {account_id}\n"
               f"{long_short} {qty} {symbol}\nAuto-closing...")
        close_side = "Sell" if long_short == "Long" else "Buy"
        close_payload = {
            "AccountID": account_id,
            "Symbol": symbol,
            "Quantity": str(qty),
            "OrderType": "Market",
            "TradeAction": close_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        try:
            _ts_api("POST", "/orderexecution/orders", close_payload, account_id)
            print(f"[real-trader] {source} orphan closed on {account_id}: "
                  f"{long_short} {qty} {symbol}", flush=True)
        except Exception as e:
            print(f"[real-trader] {source} orphan close FAILED on {account_id}: {e}",
                  flush=True)

    # Cancel untracked orders
    try:
        ord_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/orders", None, account_id)
        tracked_oids = set()
        with _lock:
            for o in _active_orders.values():
                if o.get("account_id") == account_id:
                    for k in ("entry_order_id", "stop_order_id", "target_order_id"):
                        oid = o.get(k)
                        if oid:
                            tracked_oids.add(str(oid))
        for o in (ord_data or {}).get("Orders", []):
            status = o.get("Status", "")
            if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
                continue
            oid = o.get("OrderID")
            if oid and str(oid) not in tracked_oids:
                _ts_api("DELETE", f"/orderexecution/orders/{oid}", None, account_id)
                print(f"[real-trader] {source} orphan cleanup: cancelled untracked order "
                      f"{oid} on {account_id}", flush=True)
    except Exception as e:
        print(f"[real-trader] {source} orphan order cancel failed on {account_id}: {e}",
              flush=True)


def periodic_orphan_check():
    """Periodic safety check -- detect orphaned broker positions during market hours.
    Called by scheduler every 5 minutes."""
    if not (LONGS_ENABLED or SHORTS_ENABLED) or not _get_token:
        return

    # Daily cleanup: expire stale orders from previous days
    today_str = date.today().isoformat()
    with _lock:
        stale_ids = []
        for lid, o in _active_orders.items():
            if o["status"] in ("pending_entry", "pending_limit", "filled"):
                ts_placed = o.get("ts_placed", "")
                order_date = ts_placed[:10] if len(ts_placed) >= 10 else ""
                if order_date and order_date < today_str:
                    stale_ids.append(lid)
    # Update + persist OUTSIDE lock (avoid deadlock — _persist_order acquires _lock)
    for lid in stale_ids:
        with _lock:
            o = _active_orders.get(lid)
            if not o:
                continue
            print(f"[real-trader] PERIODIC: expiring stale order {o.get('setup_name', '?')} "
                  f"id={lid} from {o.get('ts_placed', '?')[:10]} "
                  f"acct={o.get('account_id')}", flush=True)
            o["status"] = "closed"
            o["close_reason"] = "stale_overnight_periodic"
        _persist_order(lid)

    # Check each account
    for acct_id in (_LONGS_ACCOUNT, _SHORTS_ACCOUNT):
        if acct_id not in ACCOUNT_WHITELIST:
            continue
        broker_pos = _get_broker_position(acct_id)
        if not broker_pos:
            continue

        with _lock:
            has_tracked = any(
                o["status"] in ("pending_entry", "filled") and o.get("account_id") == acct_id
                for o in _active_orders.values()
            )

        if not has_tracked:
            print(f"[real-trader] PERIODIC: orphan on {acct_id} -- "
                  f"{broker_pos['long_short']} {broker_pos['qty']} {broker_pos['symbol']}. "
                  f"Closing...", flush=True)
            _alert(f"⚠️ PERIODIC ORPHAN on {acct_id}\n"
                   f"{broker_pos['long_short']} {broker_pos['qty']} "
                   f"{broker_pos['symbol']}\nAuto-closing...")
            _close_broker_orphans(acct_id, source="PERIODIC")


# ====== STATUS ======

def get_status() -> dict:
    """Return status dict for health endpoint."""
    with _lock:
        active = {lid: {
            "setup_name": o["setup_name"],
            "direction": o["direction"],
            "account_id": o.get("account_id"),
            "status": o["status"],
            "fill_price": o["fill_price"],
            "current_stop": o["current_stop"],
            "target_price": o.get("target_price"),
            "max_favorable": o.get("max_favorable", 0),
            "be_triggered": o.get("be_triggered", False),
            "trail_active": o.get("trail_active", False),
        } for lid, o in _active_orders.items() if o["status"] != "closed"}

    return {
        "longs_enabled": LONGS_ENABLED,
        "shorts_enabled": SHORTS_ENABLED,
        "longs_account": _LONGS_ACCOUNT,
        "shorts_account": _SHORTS_ACCOUNT,
        "symbol": MES_SYMBOL,
        "qty": QTY,
        "max_concurrent_per_dir": MAX_CONCURRENT_PER_DIR,  # legacy alias = MAX_CONCURRENT_LONG
        "max_concurrent_long": MAX_CONCURRENT_LONG,
        "max_concurrent_short": MAX_CONCURRENT_SHORT,
        "active_count": len(active),
        "active_orders": active,
    }


def get_full_status() -> dict:
    """Full monitoring: balances, positions, orders, daily P&L for both accounts."""
    result = get_status()
    if not _get_token:
        result["error"] = "no token"
        return result

    for acct_id, label in [(_LONGS_ACCOUNT, "longs"), (_SHORTS_ACCOUNT, "shorts")]:
        acct = {"account_id": acct_id}
        # Balances
        try:
            bal_data = _ts_api("GET", f"/brokerage/accounts/{acct_id}/balances", None, acct_id)
            if bal_data:
                b = bal_data.get("Balances", [{}])
                b = b[0] if isinstance(b, list) and b else b
                detail = b.get("BalanceDetail", {})
                acct["cash"] = float(b.get("CashBalance", 0))
                acct["equity"] = float(b.get("Equity", 0))
                acct["buying_power"] = float(b.get("BuyingPower", 0))
                acct["today_pnl"] = float(b.get("TodaysProfitLoss", 0))
                acct["realized_pnl"] = float(detail.get("RealizedProfitLoss", 0))
                acct["unrealized_pnl"] = float(detail.get("UnrealizedProfitLoss", 0))
                acct["day_trade_excess"] = float(detail.get("DayTradeExcess", 0))
        except Exception as e:
            acct["balance_error"] = str(e)
        # Positions
        try:
            pos = _get_broker_position(acct_id)
            acct["position"] = pos if pos else "flat"
        except Exception as e:
            acct["position_error"] = str(e)
        # Open orders
        try:
            ord_data = _ts_api("GET", f"/brokerage/accounts/{acct_id}/orders", None, acct_id)
            open_orders = []
            for o in (ord_data or {}).get("Orders", []):
                if o.get("Status") in ("OPN", "ACK", "DON"):
                    open_orders.append({
                        "id": o.get("OrderID"),
                        "type": o.get("Type"),
                        "side": o.get("Legs", [{}])[0].get("BuyOrSell"),
                        "qty": o.get("Quantity"),
                        "price": o.get("LimitPrice") or o.get("StopPrice"),
                        "status": o.get("Status"),
                    })
            acct["open_orders"] = open_orders
        except Exception as e:
            acct["orders_error"] = str(e)
        # Margin warning
        if acct.get("buying_power") is not None:
            margin_needed = MARGIN_PER_MES * QTY
            acct["can_trade"] = acct["buying_power"] >= margin_needed
            acct["margin_buffer"] = round(acct["buying_power"] - margin_needed, 2)
            losses_until_margin = int(acct.get("margin_buffer", 0) / (14 * MES_POINT_VALUE * QTY)) if acct.get("margin_buffer", 0) > 0 else 0
            acct["losses_until_margin_block"] = losses_until_margin
        result[f"account_{label}"] = acct

    # Daily loss check
    try:
        result["daily_loss"] = _get_daily_loss()
        result["daily_loss_limit"] = DAILY_LOSS_LIMIT
        result["circuit_breaker_triggered"] = result["daily_loss"] >= DAILY_LOSS_LIMIT
    except Exception:
        pass

    return result


# ====== BROKER QUERIES ======

def _get_broker_position(account_id: str) -> dict | None:
    """Query broker for actual MES position on a specific account.
    Returns {'qty': int, 'long_short': str, 'symbol': str} or None if flat.

    NOTE: TS API returns Quantity as a SIGNED string for futures positions
    (e.g. "-1" for shorts). Use abs() so the filter doesn't drop shorts.
    """
    if account_id not in ACCOUNT_WHITELIST:
        return None
    try:
        pos_data = _ts_api("GET", f"/brokerage/accounts/{account_id}/positions", None, account_id)
        for pos in (pos_data or {}).get("Positions", []):
            symbol = pos.get("Symbol", "")
            qty = abs(int(pos.get("Quantity", "0")))
            if qty > 0 and "MES" in symbol.upper():
                return {
                    "qty": qty,
                    "long_short": pos.get("LongShort", ""),
                    "symbol": symbol,
                }
    except Exception as e:
        print(f"[real-trader] broker position query error on {account_id}: {e}", flush=True)
    return None


def _get_daily_realized_loss() -> float:
    """Sum realized losses for today from setup_log outcomes for real trades.
    Uses setup_log.outcome_pnl × MES_POINT_VALUE for closed real trades.
    Returns positive $ amount of losses only."""
    if not _engine:
        return 0.0
    try:
        from sqlalchemy import text
        today = datetime.now(NY).strftime("%Y-%m-%d")
        with _engine.begin() as conn:
            # Find today's real trades via real_trade_orders table
            rows = conn.execute(text(
                f"SELECT setup_log_id, state FROM {DB_TABLE} "
                f"WHERE state->>'status' = 'closed'"
            )).fetchall()
            total_loss = 0.0
            for row in rows:
                log_id = row[0]
                state = row[1] if isinstance(row[1], dict) else {}
                # Check setup_log for outcome_pnl
                pnl_row = conn.execute(text(
                    "SELECT outcome_pnl FROM setup_log "
                    "WHERE id = :lid AND ts::date = :today AND outcome_pnl < 0"
                ), {"lid": log_id, "today": today}).fetchone()
                if pnl_row:
                    loss_pts = abs(float(pnl_row[0]))
                    total_loss += loss_pts * MES_POINT_VALUE * QTY
            return total_loss
    except Exception as e:
        print(f"[real-trader] daily loss query error: {e}", flush=True)
        return 0.0


def _get_buying_power(account_id: str) -> float | None:
    """Query account buying power from broker."""
    if account_id not in ACCOUNT_WHITELIST:
        return None
    try:
        data = _ts_api("GET", f"/brokerage/accounts/{account_id}/balances", None, account_id)
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
        print(f"[real-trader] buying power query error on {account_id}: {e}", flush=True)
        return None


def _get_daily_realized_pnl(account_id: str) -> float | None:
    """Fetch today's realized P&L from broker for Telegram 'Day:' line.
    Uses BalanceDetail.RealizedProfitLoss (authoritative, includes fees)."""
    if account_id not in ACCOUNT_WHITELIST:
        return None
    try:
        data = _ts_api("GET", f"/brokerage/accounts/{account_id}/balances", None, account_id)
        if not data:
            return None
        balances = data.get("Balances", [])
        if isinstance(balances, list) and balances:
            b = balances[0]
        elif isinstance(balances, dict):
            b = balances
        else:
            return None
        detail = b.get("BalanceDetail", {}) or {}
        val = detail.get("RealizedProfitLoss")
        if val is None:
            return None
        return float(val)
    except Exception as e:
        print(f"[real-trader] daily P&L query error on {account_id}: {e}", flush=True)
        return None


def _day_line(account_id: str) -> str:
    """Format 'Day: +$XX.XX' line from broker's daily realized P&L.
    Returns empty string on failure so alerts still send."""
    pnl = _get_daily_realized_pnl(account_id)
    if pnl is None:
        return ""
    sign = "+" if pnl >= 0 else "-"
    return f"\nDay: {sign}${abs(pnl):.2f}"


# ====== TS API HELPER ======

def _ts_api(method: str, path: str, json_body: dict | None,
            account_id: str) -> dict | None:
    """Authenticated request to TradeStation REAL API.
    Every call validates account_id against whitelist before proceeding."""
    # CRITICAL: validate account on every API call
    if account_id not in ACCOUNT_WHITELIST:
        print(f"[real-trader] BLOCKED API call: account {account_id} not in whitelist! "
              f"method={method} path={path}", flush=True)
        _alert(f"🚨 SECURITY BLOCK: API call to non-whitelisted account {account_id}\n"
               f"{method} {path}")
        return None

    if not _get_token:
        print("[real-trader] no token function", flush=True)
        return None

    for attempt in range(2):
        try:
            token = _get_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            url = f"{REAL_BASE}{path}"

            # Log every request (this is real money)
            if json_body:
                print(f"[real-trader] API {method} {path} acct={account_id} "
                      f"payload={json.dumps(json_body, default=str)[:400]}", flush=True)
            else:
                print(f"[real-trader] API {method} {path} acct={account_id}", flush=True)

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

            # Log response
            print(f"[real-trader] API {method} {path} [{r.status_code}] "
                  f"acct={account_id}", flush=True)

            if r.status_code == 401 and attempt == 0:
                print(f"[real-trader] API 401 on {account_id}, retrying with fresh token...",
                      flush=True)
                continue

            if r.status_code >= 400:
                print(f"[real-trader] API ERROR {method} {path} [{r.status_code}]: "
                      f"{r.text[:500]}", flush=True)
                return None

            result = r.json() if r.text else {}

            # Log order responses fully (this is real money)
            if method in ("POST", "PUT") and "order" in path.lower():
                print(f"[real-trader] API RESPONSE {method} {path}: "
                      f"{json.dumps(result, default=str)[:500]}", flush=True)

            return result

        except Exception as e:
            print(f"[real-trader] API error {method} {path} acct={account_id}: {e}", flush=True)
            if attempt == 0:
                continue
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
            conn.execute(text(f"""
                INSERT INTO {DB_TABLE} (setup_log_id, state, updated_at)
                VALUES (:id, :s, NOW())
                ON CONFLICT (setup_log_id) DO UPDATE SET state = :s, updated_at = NOW()
            """), {"id": setup_log_id, "s": state})
    except Exception as e:
        print(f"[real-trader] persist error: {e}", flush=True)


def _load_active_orders():
    """Load non-closed orders from DB on startup.
    Only loads orders from today -- stale overnight orders are auto-closed."""
    global _active_orders
    if not _engine:
        return
    try:
        from sqlalchemy import text
        with _engine.begin() as conn:
            rows = conn.execute(text(f"""
                SELECT setup_log_id, state, updated_at FROM {DB_TABLE}
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
            ts_placed = state.get("ts_placed", "")
            order_date = ts_placed[:10] if len(ts_placed) >= 10 else ""
            if not order_date and row.get("updated_at"):
                order_date = str(row["updated_at"])[:10]
            is_stale = (order_date < today_str) if order_date else True
            if is_stale:
                stale += 1
                state["status"] = "closed"
                state["close_reason"] = "stale_overnight"
                _active_orders[lid] = state
                _persist_order(lid)
                del _active_orders[lid]
                print(f"[real-trader] STALE order auto-closed: {state.get('setup_name', '?')} "
                      f"id={lid} from {order_date or 'unknown'} "
                      f"acct={state.get('account_id')}", flush=True)
                continue
            _active_orders[lid] = state
            loaded += 1

        if loaded:
            print(f"[real-trader] restored {loaded} active orders", flush=True)
        if stale:
            print(f"[real-trader] auto-closed {stale} stale overnight order(s)", flush=True)
    except Exception as e:
        print(f"[real-trader] load error (non-fatal): {e}", flush=True)


def cleanup_stale_orders():
    """Clear any active orders from previous days. Called daily at market open.

    Defensive layer: if EOD flatten fails to persist closed state, this catches
    stale orders before they block new trades via MAX_CONCURRENT_PER_DIR.
    Bug found 2026-04-06: #1540 from Apr 2 blocked #1544/#1551 shorts all morning.
    """
    today_str = date.today().isoformat()
    cleaned = 0
    # Collect stale IDs first, then update+persist+delete outside iteration
    with _lock:
        stale_ids = [lid for lid, order in _active_orders.items()
                     if order.get("status") != "closed"
                     and (order.get("ts_placed", "")[:10] or "") < today_str
                     and len(order.get("ts_placed", "")) >= 10]
    for lid in stale_ids:
        with _lock:
            order = _active_orders.get(lid)
            if not order or order.get("status") == "closed":
                continue
            order["status"] = "closed"
            order["close_reason"] = "stale_daily_cleanup"
        _persist_order(lid)
        with _lock:
            _active_orders.pop(lid, None)
        cleaned += 1
        print(f"[real-trader] DAILY CLEANUP: stale order closed: "
                  f"{order.get('setup_name', '?')} id={lid} from {order.get('ts_placed', '?')[:10]} "
                  f"acct={order.get('account_id')}", flush=True)
    if cleaned:
        _alert(f"⚠️ Daily cleanup: {cleaned} stale order(s) from previous day(s) removed.\n"
               f"These were blocking new trades via concurrent cap.")
    else:
        print(f"[real-trader] daily cleanup: no stale orders", flush=True)


# ====== TELEGRAM HELPER ======

def _alert(msg: str):
    """Send Telegram alert for EVERY action -- this is real money."""
    if _send_telegram:
        try:
            _send_telegram(msg)
        except Exception as e:
            print(f"[real-trader] alert send failed: {e}", flush=True)
    print(f"[real-trader] ALERT: {msg}", flush=True)
