# Options Trader: 0DTE options SIM execution module
# Self-contained — receives engine, token fn, and telegram fn via init()
# Hardcoded to SIM API — cannot hit live.
#
# Supports two strategies:
#   - "credit_spread" (default): sell ATM credit spread, theta works for you
#     Bullish = bull put spread (sell put + buy lower put)
#     Bearish = bear call spread (sell call + buy higher call)
#   - "single_leg": buy call/put (original behavior)
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
OPTIONS_LOG_ONLY = os.getenv("OPTIONS_LOG_ONLY", "true").lower() == "true"  # portal log only — no SIM orders, no Telegram
OPTIONS_QTY = int(os.getenv("OPTIONS_QTY", "1"))
TARGET_DELTA = float(os.getenv("OPTIONS_TARGET_DELTA", "0.50"))
MAX_HOLD_MINUTES = int(os.getenv("OPTIONS_MAX_HOLD_MIN", "90"))     # single-leg only
OPTIONS_UNDERLYING = os.getenv("OPTIONS_UNDERLYING", "SPY")         # "SPY" or "SPXW"
OPTIONS_STRATEGY = os.getenv("OPTIONS_STRATEGY", "credit_spread")   # "credit_spread" or "single_leg"
SPREAD_WIDTH = int(os.getenv("OPTIONS_SPREAD_WIDTH", "2"))          # $1 or $2 for SPY

# Chain column indices (CANONICAL_COLS from main.py)
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
    strat_info = f"strategy={OPTIONS_STRATEGY}"
    if OPTIONS_STRATEGY == "credit_spread":
        strat_info += f" width=${SPREAD_WIDTH}"
    mode = "LOG-ONLY" if OPTIONS_LOG_ONLY else ("LIVE" if OPTIONS_TRADE_ENABLED else "OFF")
    print(f"[options] init: mode={mode} account={SIM_ACCOUNT_ID} "
          f"underlying={OPTIONS_UNDERLYING} qty={OPTIONS_QTY} delta={TARGET_DELTA} "
          f"{strat_info} active={n}", flush=True)


# ====== MAIN ENTRY POINT ======

def place_trade(setup_log_id: int, setup_name: str, direction: str, spot: float):
    """Place 0DTE option trade when a setup fires."""
    if not OPTIONS_TRADE_ENABLED and not OPTIONS_LOG_ONLY:
        print(f"[options] skip {setup_name}: master switch OFF", flush=True)
        return
    if not setup_log_id:
        print(f"[options] skip {setup_name}: no setup_log_id", flush=True)
        return

    with _lock:
        if setup_log_id in _active_orders:
            print(f"[options] skip {setup_name} id={setup_log_id}: already active", flush=True)
            return

    if OPTIONS_STRATEGY == "credit_spread":
        _place_credit_spread(setup_log_id, setup_name, direction, spot)
    else:
        _place_single_leg(setup_log_id, setup_name, direction, spot)


# ====== CREDIT SPREAD ENTRY ======

def _place_credit_spread(setup_log_id: int, setup_name: str, direction: str, spot: float):
    """Sell ATM credit spread: bull put spread for bullish, bear call spread for bearish."""
    is_long = direction.lower() in ("long", "bullish")

    # For credit spread, short leg option type is OPPOSITE of single-leg:
    #   Bullish → sell PUT spread (sell put near ATM, buy lower put)
    #   Bearish → sell CALL spread (sell call near ATM, buy higher call)
    scan_puts = is_long

    chain_rows = _get_chain_rows()
    if not chain_rows:
        print(f"[options] skip {setup_name}: no chain data", flush=True)
        return

    # Find short leg near TARGET_DELTA
    short_info = _find_strike_in_rows(chain_rows, scan_puts, TARGET_DELTA)
    if not short_info:
        print(f"[options] skip {setup_name}: no short strike found", flush=True)
        return

    short_strike = short_info["strike"]
    cp = "P" if scan_puts else "C"

    # Long leg: further OTM by SPREAD_WIDTH
    if scan_puts:
        long_strike = short_strike - SPREAD_WIDTH   # lower put = more OTM
    else:
        long_strike = short_strike + SPREAD_WIDTH   # higher call = more OTM

    # Get long leg data from chain
    long_info = _find_exact_strike(chain_rows, scan_puts, long_strike)
    if not long_info:
        print(f"[options] skip {setup_name}: no chain data for {cp}{long_strike}", flush=True)
        return

    # Build option symbols
    today = date.today()
    ds = today.strftime('%y%m%d')
    short_sym = f"{OPTIONS_UNDERLYING} {ds}{cp}{short_strike}"
    long_sym = f"{OPTIONS_UNDERLYING} {ds}{cp}{long_strike}"

    # Get live quotes for both legs
    short_q = _get_option_quote(short_sym)
    long_q = _get_option_quote(long_sym)

    short_bid = (short_q["bid"] if short_q and short_q.get("bid") else short_info["bid"]) or 0
    short_ask = (short_q["ask"] if short_q and short_q.get("ask") else short_info["ask"]) or 0
    long_bid = (long_q["bid"] if long_q and long_q.get("bid") else long_info["bid"]) or 0
    long_ask = (long_q["ask"] if long_q and long_q.get("ask") else long_info["ask"]) or 0

    print(f"[options] spread quotes: {short_sym} bid=${short_bid:.2f} ask=${short_ask:.2f} | "
          f"{long_sym} bid=${long_bid:.2f} ask=${long_ask:.2f}", flush=True)

    # Net credit = what we receive (short bid) - what we pay (long ask)
    net_credit = round(short_bid - long_ask, 2)
    if net_credit <= 0.01:
        print(f"[options] skip {setup_name}: no positive credit "
              f"(short_bid=${short_bid:.2f} - long_ask=${long_ask:.2f} = ${net_credit:.2f})", flush=True)
        return

    max_loss = round(SPREAD_WIDTH - net_credit, 2)

    # ── Log-only mode: skip SIM API, record theoretical trade ──
    if OPTIONS_LOG_ONLY:
        entry_oid = None
        entry_status = "filled"  # treat as instant fill at theo prices
        entry_price = net_credit
    else:
        # Place as single atomic multi-leg order using TS API Legs array.
        payload = {
            "AccountID": SIM_ACCOUNT_ID,
            "Symbol": short_sym,
            "Quantity": str(OPTIONS_QTY),
            "OrderType": "Limit",
            "LimitPrice": str(round(net_credit, 2)),
            "TradeAction": "SELLTOOPEN",
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
            "Legs": [
                {
                    "Symbol": short_sym,
                    "Quantity": str(OPTIONS_QTY),
                    "TradeAction": "SELLTOOPEN",
                },
                {
                    "Symbol": long_sym,
                    "Quantity": str(OPTIONS_QTY),
                    "TradeAction": "BUYTOOPEN",
                },
            ],
        }
        resp = _sim_api("POST", "/orderexecution/orders", payload)
        ok, entry_oid = _order_ok(resp)

        if not ok:
            _alert(f"[OPTIONS] FAILED credit spread for {setup_name}\n"
                   f"{short_sym} / {long_sym}")
            return
        entry_status = "pending_entry"
        entry_price = None  # SIM fill comes later

    order = {
        "strategy": "credit_spread",
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "symbol": short_sym,                # primary symbol for display/compat
        "short_symbol": short_sym,
        "long_symbol": long_sym,
        "short_strike": short_strike,
        "long_strike": long_strike,
        "spread_width": SPREAD_WIDTH,
        "cp": cp,
        "delta_at_entry": short_info["delta"],
        "entry_order_id": entry_oid,        # single atomic multi-leg order
        "close_order_id": None,             # single atomic close order
        "qty": OPTIONS_QTY,
        "entry_price": entry_price,         # SIM fill or theo credit (log-only)
        "close_price": None,                # SIM fill: net debit
        "theo_credit": net_credit,          # live bid/ask net credit
        "theo_debit": None,                 # live bid/ask net debit at close
        "theo_entry_price": net_credit,     # compat: stored for log endpoint
        "theo_close_price": None,           # compat: set at close
        "theo_pnl": None,                   # pre-computed at close
        "status": entry_status,
        "spot_at_entry": spot,
        "max_loss": max_loss,
        "ts_placed": datetime.utcnow().isoformat(),
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    _log_only_tag = " [LOG-ONLY]" if OPTIONS_LOG_ONLY else ""
    print(f"[options] CREDIT SPREAD placed{_log_only_tag}: {setup_name} "
          f"SELL {short_sym} @ ${short_bid:.2f} / BUY {long_sym} @ ${long_ask:.2f} "
          f"credit=${net_credit:.2f} maxloss=${max_loss:.2f}", flush=True)
    _alert(f"[OPTIONS] CREDIT SPREAD {setup_name}\n"
           f"SELL {short_sym} @ ${short_bid:.2f}\n"
           f"BUY {long_sym} @ ${long_ask:.2f}\n"
           f"Credit: ${net_credit:.2f} | Max loss: ${max_loss:.2f} | SPX: {spot:.0f}")


# ====== SINGLE LEG ENTRY (original) ======

def _place_single_leg(setup_log_id: int, setup_name: str, direction: str, spot: float):
    """Buy 0DTE option (original single-leg strategy)."""
    is_long = direction.lower() in ("long", "bullish")

    strike_info = _find_strike(is_long)
    if not strike_info:
        print(f"[options] skip {setup_name}: no suitable strike found", flush=True)
        return

    strike = strike_info["strike"]
    delta = strike_info["delta"]
    snap_bid = strike_info["bid"]
    snap_ask = strike_info["ask"]

    today = date.today()
    cp = "C" if is_long else "P"
    symbol = f"{OPTIONS_UNDERLYING} {today.strftime('%y%m%d')}{cp}{strike}"

    live_q = _get_option_quote(symbol)
    if live_q and live_q.get("ask") and live_q["ask"] > 0:
        ask = live_q["ask"]
        bid = live_q["bid"] or snap_bid
        print(f"[options] live quote {symbol}: bid=${bid:.2f} ask=${ask:.2f} "
              f"(snap bid=${snap_bid:.2f} ask=${snap_ask:.2f})", flush=True)
    else:
        ask = snap_ask
        bid = snap_bid
        print(f"[options] live quote failed for {symbol}, using snapshot: "
              f"bid=${bid:.2f} ask=${ask:.2f}", flush=True)

    limit_price = round(ask, 2) if ask > 0 else None
    if not limit_price:
        print(f"[options] skip {setup_name}: ask price is 0", flush=True)
        return

    # ── Log-only mode: skip SIM API, record theoretical trade ──
    if OPTIONS_LOG_ONLY:
        entry_oid = None
        entry_status = "filled"
        entry_price = ask
    else:
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
        entry_status = "pending_entry"
        entry_price = None

    order = {
        "strategy": "single_leg",
        "setup_log_id": setup_log_id,
        "setup_name": setup_name,
        "direction": direction,
        "symbol": symbol,
        "strike": strike,
        "delta_at_entry": delta,
        "entry_order_id": entry_oid,
        "close_order_id": None,
        "qty": OPTIONS_QTY,
        "entry_price": entry_price,
        "close_price": None,
        "theo_entry_price": ask,
        "theo_close_price": None,
        "theo_pnl": None,
        "status": entry_status,
        "spot_at_entry": spot,
        "bid_at_entry": bid,
        "ask_at_entry": ask,
        "ts_placed": datetime.utcnow().isoformat(),
    }

    with _lock:
        _active_orders[setup_log_id] = order
    _persist_order(setup_log_id)

    _log_only_tag = " [LOG-ONLY]" if OPTIONS_LOG_ONLY else ""
    print(f"[options] placed{_log_only_tag}: {setup_name} BUYTOOPEN {OPTIONS_QTY} {symbol} "
          f"delta={delta:.3f} limit=${limit_price:.2f} spot={spot:.0f}", flush=True)
    _alert(f"[OPTIONS] {setup_name} placed\n"
           f"BUYTOOPEN {OPTIONS_QTY} {symbol}\n"
           f"Delta: {delta:.3f} | Limit: ${limit_price:.2f} | SPX: {spot:.0f}")


# ====== CLOSE TRADE ======

def close_trade(setup_log_id: int, result_type: str = ""):
    """Close option position on outcome resolution."""
    with _lock:
        order = _active_orders.get(setup_log_id)
        if not order:
            return
        if order["status"] == "closed":
            return

    if order.get("strategy") == "credit_spread":
        _close_credit_spread(setup_log_id, order, result_type)
    else:
        _close_single_leg(setup_log_id, order, result_type)


def _close_credit_spread(setup_log_id: int, order: dict, result_type: str):
    """Close credit spread as single atomic multi-leg order."""
    setup_name = order["setup_name"]
    short_sym = order["short_symbol"]
    long_sym = order["long_symbol"]

    if order["status"] == "filled":
        # Get live quotes for both legs
        short_q = _get_option_quote(short_sym)
        long_q = _get_option_quote(long_sym)

        short_ask = (short_q["ask"] if short_q and short_q.get("ask") else None) or 0
        long_bid = (long_q["bid"] if long_q and long_q.get("bid") else None) or 0

        # Compute theo debit (cost to close the spread)
        theo_debit = round(max(short_ask - long_bid, 0), 2)
        theo_credit = order.get("theo_credit", 0)
        theo_pnl = round((theo_credit - theo_debit) * 100 * order["qty"], 0)

        with _lock:
            order["theo_debit"] = theo_debit
            order["theo_close_price"] = theo_debit
            order["theo_pnl"] = theo_pnl

        print(f"[options] credit spread close: {setup_name} credit=${theo_credit:.2f} "
              f"debit=${theo_debit:.2f} pnl=${theo_pnl:+.0f} ({result_type})", flush=True)

        # ── Log-only: skip SIM close order ──
        if not OPTIONS_LOG_ONLY:
            # Close as single atomic multi-leg order
            if theo_debit > 0.01:
                payload = {
                    "AccountID": SIM_ACCOUNT_ID,
                    "Symbol": short_sym,
                    "Quantity": str(order["qty"]),
                    "OrderType": "Limit",
                    "LimitPrice": str(round(theo_debit, 2)),
                    "TradeAction": "BUYTOCLOSE",
                    "TimeInForce": {"Duration": "DAY"},
                    "Route": "Intelligent",
                    "Legs": [
                        {
                            "Symbol": short_sym,
                            "Quantity": str(order["qty"]),
                            "TradeAction": "BUYTOCLOSE",
                        },
                        {
                            "Symbol": long_sym,
                            "Quantity": str(order["qty"]),
                            "TradeAction": "SELLTOCLOSE",
                        },
                    ],
                }
                resp = _sim_api("POST", "/orderexecution/orders", payload)
                close_ok, close_oid = _order_ok(resp)
                if close_ok:
                    with _lock:
                        order["close_order_id"] = close_oid
                else:
                    print(f"[options] spread close order failed: {setup_name} — "
                          f"will expire cash-settled", flush=True)
            else:
                print(f"[options] spread near $0, letting expire: {setup_name}", flush=True)

        with _lock:
            order["ts_closed"] = datetime.utcnow().isoformat()
            order["close_price"] = theo_debit  # log-only: use theo as SIM price too

        _alert(f"[OPTIONS] SPREAD CLOSED: {setup_name} ({result_type})\n"
               f"Credit: ${theo_credit:.2f} | Debit: ${theo_debit:.2f}\n"
               f"Theo P&L: ${theo_pnl:+.0f}")

    elif order["status"] == "pending_entry":
        # Cancel the single entry order (covers both legs)
        oid = order.get("entry_order_id")
        if oid and not OPTIONS_LOG_ONLY:
            _sim_api("DELETE", f"/orderexecution/orders/{oid}", None)
        print(f"[options] cancelled unfilled spread: {setup_name}", flush=True)

    with _lock:
        order["status"] = "closed"
    _persist_order(setup_log_id)


def _close_single_leg(setup_log_id: int, order: dict, result_type: str):
    """Close single-leg option (original behavior)."""
    setup_name = order["setup_name"]
    symbol = order["symbol"]

    if order["status"] == "filled":
        bid_price = _get_option_bid(symbol)

        if bid_price and bid_price > 0:
            with _lock:
                order["theo_close_price"] = bid_price
            theo_entry = order.get("theo_entry_price") or order.get("ask_at_entry") or 0
            theo_pnl = (bid_price - theo_entry) * 100 * order["qty"]
            with _lock:
                order["theo_pnl"] = round(theo_pnl, 0)
                order["close_price"] = bid_price  # log-only: use theo as close price
            print(f"[options] theo close: {setup_name} theo_bid=${bid_price:.2f} "
                  f"theo_pnl=${theo_pnl:+.0f}", flush=True)

        # ── Log-only: skip SIM close order ──
        if not OPTIONS_LOG_ONLY:
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
        else:
            with _lock:
                order["ts_closed"] = datetime.utcnow().isoformat()
            print(f"[options] closed [LOG-ONLY]: {setup_name} {symbol} "
                  f"result={result_type} theo_close=${order.get('theo_close_price')}", flush=True)

    elif order["status"] == "pending_entry":
        if order.get("entry_order_id") and not OPTIONS_LOG_ONLY:
            _sim_api("DELETE", f"/orderexecution/orders/{order['entry_order_id']}", None)
        print(f"[options] cancelled unfilled entry: {setup_name} {symbol}", flush=True)

    with _lock:
        order["status"] = "closed"
    _persist_order(setup_log_id)


# ====== POLL ORDER STATUS ======

def poll_order_status():
    """Check order fills and time exit. Called each ~30s cycle."""
    if not OPTIONS_TRADE_ENABLED and not OPTIONS_LOG_ONLY:
        return
    if OPTIONS_LOG_ONLY:
        return  # no SIM orders to poll in log-only mode
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
        if order.get("strategy") == "credit_spread":
            _check_spread_fills(lid, order, broker_orders)
        else:
            _check_fills(lid, order, broker_orders)

    # ── Time-based exit for SINGLE-LEG filled positions only ──
    # Credit spreads: NO time exit (theta works for us, hold to resolution)
    with _lock:
        filled = [(lid, o) for lid, o in _active_orders.items()
                  if o["status"] == "filled" and o.get("entry_price")
                  and o.get("strategy") != "credit_spread"]
    if not filled:
        return

    now = datetime.utcnow()
    for lid, order in filled:
        symbol = order["symbol"]
        entry_price = order["entry_price"]
        placed_ts = order.get("ts_placed")

        should_close = False
        reason = ""

        # Time exit: close if held > MAX_HOLD_MINUTES
        if placed_ts:
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
                   f"Entry: ${entry_price:.2f}")
            close_trade(lid, result_type=reason)


# ====== FILL CHECKING ======

def _check_spread_fills(lid, order, broker_orders):
    """Check credit spread fills. Single atomic order — both legs fill together."""
    changed = False

    # Check entry fill (single multi-leg order)
    if order["status"] == "pending_entry" and order.get("entry_order_id"):
        bo = broker_orders.get(order["entry_order_id"], {})
        status = bo.get("Status", "")
        if status == "FLL":
            fp = _extract_fill_price(bo)
            with _lock:
                order["status"] = "filled"
                order["entry_price"] = fp  # net credit fill price
            changed = True
            theo = order.get("theo_credit", 0)
            print(f"[options] SPREAD FILLED: {order['setup_name']} "
                  f"sim_credit=${fp} theo_credit=${theo:.2f}", flush=True)
            _alert(f"[OPTIONS] SPREAD FILLED: {order['setup_name']}\n"
                   f"SELL {order['short_symbol']}\n"
                   f"BUY {order['long_symbol']}\n"
                   f"Credit: ${fp} (theo: ${theo:.2f})")
        elif status in ("REJ", "CAN", "EXP"):
            with _lock:
                order["status"] = "closed"
            changed = True
            reason = bo.get("RejectReason") or bo.get("StatusDescription") or ""
            print(f"[options] spread entry {status}: {order['setup_name']} "
                  f"reason={reason}", flush=True)

    # Check close fill (single multi-leg order)
    if order["status"] == "filled" and order.get("close_order_id"):
        bo = broker_orders.get(order["close_order_id"], {})
        if bo.get("Status") == "FLL":
            fp = _extract_fill_price(bo)
            with _lock:
                order["close_price"] = fp  # net debit fill price
                order["status"] = "closed"
            changed = True
            theo_credit = order.get("theo_credit", 0)
            sim_pnl = ((order.get("entry_price") or 0) - (fp or 0)) * 100 * order["qty"]
            print(f"[options] SPREAD CLOSE FILLED: {order['setup_name']} "
                  f"sim_debit=${fp} sim_pnl=${sim_pnl:+.0f}", flush=True)

    if changed:
        _persist_order(lid)


def _check_fills(lid, order, broker_orders):
    """Check single-leg order fills."""
    changed = False

    if order["status"] == "pending_entry" and order.get("entry_order_id"):
        entry = broker_orders.get(order["entry_order_id"], {})
        status = entry.get("Status", "")
        if status == "FLL":
            fp = _extract_fill_price(entry)
            with _lock:
                order["status"] = "filled"
                order["entry_price"] = fp
            changed = True
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


# ====== STRIKE SELECTION ======

def _get_chain_rows():
    """Get latest chain snapshot rows."""
    if not _engine:
        return None
    table = "spy_chain_snapshots" if OPTIONS_UNDERLYING == "SPY" else "chain_snapshots"
    try:
        from sqlalchemy import text
        with _engine.begin() as conn:
            row = conn.execute(text(
                f"SELECT rows FROM {table} ORDER BY ts DESC LIMIT 1"
            )).mappings().first()
        if not row:
            print(f"[options] no {table} snapshot available", flush=True)
            return None
        rows = json.loads(row["rows"]) if isinstance(row["rows"], str) else row["rows"]
        return rows
    except Exception as e:
        print(f"[options] chain query error: {e}", flush=True)
        return None


def _find_strike_in_rows(rows, scan_puts: bool, target_delta: float) -> dict | None:
    """Find strike nearest to target_delta. scan_puts=True scans put side."""
    best = None
    best_gap = float("inf")
    for r in rows:
        try:
            strike = float(r[IDX_STRIKE]) if r[IDX_STRIKE] else None
            if not strike:
                continue
            if scan_puts:
                delta = float(r[IDX_P_DELTA]) if r[IDX_P_DELTA] else None
                bid = float(r[IDX_P_BID]) if r[IDX_P_BID] else 0
                ask = float(r[IDX_P_ASK]) if r[IDX_P_ASK] else 0
            else:
                delta = float(r[IDX_C_DELTA]) if r[IDX_C_DELTA] else None
                bid = float(r[IDX_C_BID]) if r[IDX_C_BID] else 0
                ask = float(r[IDX_C_ASK]) if r[IDX_C_ASK] else 0
            if delta is None:
                continue
            gap = abs(abs(delta) - target_delta)
            if gap < best_gap:
                best_gap = gap
                best = {"strike": int(strike), "delta": delta, "bid": bid, "ask": ask}
        except (ValueError, TypeError, IndexError):
            continue
    if best:
        side = "P" if scan_puts else "C"
        print(f"[options] short strike: {OPTIONS_UNDERLYING} {side}{best['strike']} "
              f"delta={best['delta']:.3f} bid={best['bid']:.2f} ask={best['ask']:.2f}", flush=True)
    return best


def _find_exact_strike(rows, scan_puts: bool, target_strike: int) -> dict | None:
    """Find exact strike in chain rows."""
    for r in rows:
        try:
            strike = float(r[IDX_STRIKE]) if r[IDX_STRIKE] else None
            if not strike or int(strike) != target_strike:
                continue
            if scan_puts:
                delta = float(r[IDX_P_DELTA]) if r[IDX_P_DELTA] else None
                bid = float(r[IDX_P_BID]) if r[IDX_P_BID] else 0
                ask = float(r[IDX_P_ASK]) if r[IDX_P_ASK] else 0
            else:
                delta = float(r[IDX_C_DELTA]) if r[IDX_C_DELTA] else None
                bid = float(r[IDX_C_BID]) if r[IDX_C_BID] else 0
                ask = float(r[IDX_C_ASK]) if r[IDX_C_ASK] else 0
            return {"strike": int(strike), "delta": delta, "bid": bid, "ask": ask}
        except (ValueError, TypeError, IndexError):
            continue
    return None


def _find_strike(is_long: bool) -> dict | None:
    """Find option strike nearest to TARGET_DELTA (single-leg mode)."""
    chain_rows = _get_chain_rows()
    if not chain_rows:
        return None
    # Single-leg: bullish = call, bearish = put
    scan_puts = not is_long
    return _find_strike_in_rows(chain_rows, scan_puts, TARGET_DELTA)


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
    """Backfill missing entry/close prices from TS API. Called every ~60s."""
    global _last_reconcile
    if not OPTIONS_TRADE_ENABLED or not _get_token:
        return

    now = time.time()
    if now - _last_reconcile < 55:
        return
    _last_reconcile = now

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

        if order.get("entry_price") is None and order.get("entry_order_id"):
            entry = broker_orders.get(order["entry_order_id"], {})
            if entry.get("Status") == "FLL":
                fp = _extract_fill_price(entry)
                if fp:
                    with _lock:
                        order["entry_price"] = fp
                    changed = True

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
    """Send Telegram alert for option trades. Suppressed in log-only mode."""
    if OPTIONS_LOG_ONLY:
        return  # portal-only: no Telegram
    if _send_telegram:
        try:
            _send_telegram(msg)
        except Exception as e:
            print(f"[options] telegram error: {e}", flush=True)
    print(f"[options] {msg}", flush=True)
