"""Sierra Chart DTC Protocol bridge for MES trading.

Drop-in replacement for NT8Bridge. Connects to Sierra Chart's DTC server
via WebSocket (JSON encoding). Same public interface as NT8Bridge — including
_oid(), _write() (OIF parser), .account, .symbol attributes — so
PositionTracker code works unchanged.

Sierra Chart DTC Server setup:
  1. Global Settings >> SC Server Settings >> DTC Protocol Server
  2. Enable DTC Protocol Server
  3. Set Listening Port (e.g. 11099)
  4. Allow Trading = YES
  5. Allowed IPs = Local Computer Only

Usage:
  bridge = SierraBridge("127.0.0.1", 11099, "malde6558-sim", "MESM26.CME")
  bridge.connect()
  bridge.place_entry_and_stop("short", 8, 6740.25)
"""

import json
import threading
import time
import logging

try:
    import websocket
except ImportError:
    raise ImportError("pip install websocket-client")

log = logging.getLogger("eval_trader")

MES_TICK_SIZE = 0.25


def _round_tick(price: float) -> str:
    """Round to MES tick size (0.25) and format as string."""
    rounded = round(round(price / MES_TICK_SIZE) * MES_TICK_SIZE, 2)
    return f"{rounded:.2f}"


# ─── DTC Message Types ────────────────────────────────────────────────────────
LOGON_REQUEST = 1
LOGON_RESPONSE = 2
HEARTBEAT = 3
SUBMIT_ORDER = 208
CANCEL_ORDER = 203
CANCEL_REPLACE = 204
OPEN_ORDERS_REQUEST = 300
ORDER_UPDATE = 301
POSITIONS_REQUEST = 305
POSITION_UPDATE = 306
FLATTEN_POSITION = 209
ACCOUNTS_REQUEST = 400
ACCOUNT_RESPONSE = 401
ACCOUNT_BALANCE_REQUEST = 601
ACCOUNT_BALANCE_UPDATE = 600

# ─── DTC Enums ────────────────────────────────────────────────────────────────
ORDER_TYPE_MARKET = 1
ORDER_TYPE_LIMIT = 2
ORDER_TYPE_STOP = 3
BUY = 1
SELL = 2
TIF_DAY = 1

# Order status codes
_STATUS_MAP = {
    0: "UNKNOWN", 1: "SENT", 2: "PENDING_OPEN", 3: "PENDING_CHILD",
    4: "OPEN", 5: "PENDING_CANCEL_REPLACE", 6: "PENDING_CANCEL",
    7: "FILLED", 8: "CANCELED", 9: "REJECTED", 10: "PARTIAL",
}


class SierraBridge:
    """DTC WebSocket bridge to Sierra Chart. Drop-in replacement for NT8Bridge."""

    # Sierra DTC returns prices multiplied by this factor (FloatToIntPriceMultiplier).
    # MES/ES use 100 (6639.75 → 663975). Must multiply when sending, divide when reading.
    PRICE_MULT = 100

    def __init__(self, host: str, port: int, account_id: str, symbol: str,
                 exchange: str = ""):
        self.host = host
        self.port = port
        self.account = account_id
        self.symbol = symbol
        self.exchange = exchange  # Sierra ignores this — symbol has exchange suffix
        self._ws = None
        self._connected = False
        self._counter = int(time.time()) % 100000
        self._lock = threading.Lock()

        # State caches (updated by receiver thread)
        self._order_states: dict[str, dict] = {}
        self._server_oid_map: dict[str, str] = {}  # client_oid → server_oid
        self._positions: dict[str, dict] = {}  # symbol → {qty, avg_price}
        self._recv_thread = None
        self._running = False
        self._trade_accounts: list[str] = []

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self):
        """Connect to Sierra Chart DTC server and start receiver thread."""
        url = f"ws://{self.host}:{self.port}"
        log.info(f"Connecting to Sierra DTC: {url}")

        self._ws = websocket.create_connection(url, timeout=15)

        # WebSocket connections use JSON automatically — NO encoding request needed.
        # Send LOGON_REQUEST as first message.
        self._send({
            "Type": LOGON_REQUEST,
            "ProtocolVersion": 8,
            "Username": "",
            "Password": "",
            "HeartbeatIntervalInSeconds": 30,
            "ClientName": "EvalTrader",
            "TradeMode": 2,  # SIMULATED (E2T runs on Rithmic Paper Trading)
        })

        # Read logon response
        logon = self._recv_one()
        if logon.get("Type") != LOGON_RESPONSE:
            raise ConnectionError(f"Expected LOGON_RESPONSE (type 2), got: {logon}")
        if not logon.get("TradingIsSupported"):
            raise ConnectionError(
                "Trading not supported — enable 'Allow Trading' in Sierra Chart "
                "DTC Server settings (Global Settings >> SC Server Settings)")
        log.info(f"Sierra DTC logon OK: {logon.get('ServerName', 'unknown')} "
                 f"(trading={'YES' if logon.get('TradingIsSupported') else 'NO'})")

        self._connected = True
        self._running = True

        # Start background receiver
        self._recv_thread = threading.Thread(target=self._receiver_loop, daemon=True)
        self._recv_thread.start()

        # Discover trade accounts (gets exact account name string)
        self._send({"Type": ACCOUNTS_REQUEST, "RequestID": 1})
        time.sleep(1.0)

        if self._trade_accounts:
            log.info(f"Trade accounts: {self._trade_accounts}")
            # Auto-match account if user's account ID is found
            if self.account not in self._trade_accounts:
                log.warning(f"Account '{self.account}' not found in Sierra accounts: "
                            f"{self._trade_accounts}")
                # Try fuzzy match (case-insensitive, dash↔underscore)
                norm = self.account.lower().replace("-", "_")
                for ta in self._trade_accounts:
                    if ta.lower().replace("-", "_") == norm:
                        log.info(f"  Auto-matched: '{self.account}' → '{ta}'")
                        self.account = ta
                        break

        # Query initial positions and balance
        self._balances = {}
        self._send({
            "Type": POSITIONS_REQUEST,
            "RequestID": 2,
            "TradeAccount": self.account,
        })
        self._send({
            "Type": ACCOUNT_BALANCE_REQUEST,
            "RequestID": 3,
            "TradeAccount": self.account,
        })
        time.sleep(0.5)

        # Show balance at startup
        bal = self.get_balance()
        if bal:
            log.info(f"Account balance: ${bal['cash']:,.2f} "
                     f"(margin=${bal['margin_used']:,.2f} open_pnl=${bal['open_pnl']:,.2f})")

    def disconnect(self):
        """Close WebSocket connection."""
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        log.info("Sierra DTC disconnected")

    # ── Internal messaging ────────────────────────────────────────────────────

    def _send(self, msg: dict):
        """Send JSON DTC message with null terminator."""
        raw = json.dumps(msg) + '\x00'
        self._ws.send(raw)

    def _recv_one(self) -> dict:
        """Receive and parse one DTC JSON message (blocking)."""
        data = self._ws.recv()
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        parts = [p for p in data.split('\x00') if p.strip()]
        if parts:
            return json.loads(parts[0])
        return {}

    def _receiver_loop(self):
        """Background thread: receive DTC messages and update state caches."""
        while self._running:
            try:
                data = self._ws.recv()
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                for part in data.split('\x00'):
                    if not part.strip():
                        continue
                    try:
                        msg = json.loads(part)
                    except json.JSONDecodeError:
                        continue
                    self._handle_message(msg)
            except websocket.WebSocketConnectionClosedException:
                if self._running:
                    log.error("Sierra DTC WebSocket closed unexpectedly")
                break
            except Exception as e:
                if self._running:
                    log.error(f"Sierra DTC receiver error: {e}")
                    time.sleep(1)

    def _handle_message(self, msg: dict):
        """Route incoming DTC messages to state caches."""
        t = msg.get("Type")

        if t == HEARTBEAT:
            self._send({"Type": HEARTBEAT})

        elif t == ORDER_UPDATE:
            oid = msg.get("ClientOrderID", "")
            status_code = msg.get("OrderStatus", 0)
            status_name = _STATUS_MAP.get(status_code, "UNKNOWN")
            reason = msg.get("OrderUpdateReason", 0)
            server_oid = msg.get("ServerOrderID", "")

            raw_price = float(msg.get("AverageFillPrice", 0)
                              or msg.get("LastFillPrice", 0))
            fill_price = raw_price / self.PRICE_MULT if raw_price else 0.0

            with self._lock:
                self._order_states[oid] = {
                    "status": status_name,
                    "qty": int(msg.get("FilledQuantity", 0)),
                    "price": fill_price,
                    "server_oid": server_oid,
                    "status_code": status_code,
                    "reason": reason,
                    "info": msg.get("InfoText", ""),
                }
                if server_oid:
                    self._server_oid_map[oid] = server_oid

            if status_name in ("FILLED", "REJECTED", "CANCELED"):
                extra = f" — {msg.get('InfoText', '')}" if msg.get("InfoText") else ""
                log.info(f"DTC [{oid}]: {status_name} "
                         f"filled={msg.get('FilledQuantity', 0)} "
                         f"price={msg.get('AverageFillPrice', 0)}{extra}")

        elif t == POSITION_UPDATE:
            sym = msg.get("Symbol", "")
            with self._lock:
                if msg.get("NoPositions"):
                    self._positions.pop(sym, None)
                else:
                    raw_avg = float(msg.get("AveragePrice", 0))
                    self._positions[sym] = {
                        "qty": float(msg.get("Quantity", 0)),
                        "avg_price": raw_avg / self.PRICE_MULT if raw_avg else 0.0,
                    }

        elif t == ACCOUNT_RESPONSE:
            acct = msg.get("TradeAccount", "")
            if acct and acct not in self._trade_accounts:
                self._trade_accounts.append(acct)

        elif t == ACCOUNT_BALANCE_UPDATE:
            acct = msg.get("TradeAccount", "")
            # Balances are in dollars — NOT scaled by PRICE_MULT
            balance = float(msg.get("CashBalance", 0))
            with self._lock:
                self._balances = getattr(self, '_balances', {})
                self._balances[acct] = {
                    "cash": balance,
                    "margin_used": float(msg.get("MarginRequirement", 0)),
                    "open_pnl": float(msg.get("SecuritiesValue", 0)),
                }
            log.debug(f"Account balance [{acct}]: cash=${balance:,.2f}")

    # ── Order ID and OIF compatibility ────────────────────────────────────────

    def _oid(self, prefix: str) -> str:
        """Generate unique client order ID. Same interface as NT8Bridge."""
        self._counter += 1
        return f"{prefix}{self._counter}"

    def _submit_order(self, side: str, qty: int, order_type: int,
                      price: float | None = None,
                      client_oid: str | None = None) -> str:
        """Submit a single DTC order. Returns client_order_id."""
        if not client_oid:
            client_oid = self._oid("o")

        msg = {
            "Type": SUBMIT_ORDER,
            "Symbol": self.symbol,
            "Exchange": self.exchange,
            "TradeAccount": self.account,
            "ClientOrderID": client_oid,
            "OrderType": order_type,
            "BuySell": BUY if side.upper() == "BUY" else SELL,
            "Quantity": float(qty),
            "TimeInForce": TIF_DAY,
            "IsAutomatedOrder": 1,
        }

        if price is not None and order_type in (ORDER_TYPE_LIMIT, ORDER_TYPE_STOP):
            tick_price = round(round(price / MES_TICK_SIZE) * MES_TICK_SIZE, 2)
            msg["Price1"] = tick_price * self.PRICE_MULT
        msg["Price2"] = 0.0  # explicit zero for non-stop-limit orders
        msg["OpenOrClose"] = 0  # unset — let exchange decide

        self._send(msg)
        log.info(f"DTC >> {json.dumps(msg)}")
        return client_oid

    def _write(self, cmd: str):
        """Parse NT8 OIF command string and execute via DTC.

        This provides backward compatibility with PositionTracker code that
        builds OIF strings directly (reverse(), flatten(), crash recovery).

        OIF format:
          PLACE;account;symbol;BUY/SELL;qty;MARKET/LIMIT/STOPMARKET;[limit];[stop];TIF;;orderId;;
          CANCEL;;;;;;;;;;orderId;;
          CHANGE;account;symbol;BUY/SELL;qty;STOPMARKET;;stop_price;TIF;;orderId;;
          CANCELALLORDERS;account;symbol
        """
        parts = cmd.strip().split(';')
        action = parts[0].upper()

        if action == "PLACE":
            side = parts[3]       # BUY or SELL
            qty = int(parts[4])
            otype = parts[5].upper()
            limit_px = parts[6] if len(parts) > 6 and parts[6] else None
            stop_px = parts[7] if len(parts) > 7 and parts[7] else None
            oid = parts[10] if len(parts) > 10 and parts[10] else None

            if otype == "MARKET":
                self._submit_order(side, qty, ORDER_TYPE_MARKET, client_oid=oid)
            elif otype == "LIMIT":
                self._submit_order(side, qty, ORDER_TYPE_LIMIT,
                                   price=float(limit_px), client_oid=oid)
            elif otype == "STOPMARKET":
                self._submit_order(side, qty, ORDER_TYPE_STOP,
                                   price=float(stop_px), client_oid=oid)
            else:
                log.warning(f"Unknown OIF order type: {otype}")

        elif action == "CANCEL":
            oid = parts[10] if len(parts) > 10 and parts[10] else None
            if oid:
                self.cancel(oid)

        elif action == "CHANGE":
            # CHANGE;acct;sym;side;qty;STOPMARKET;;new_price;TIF;;orderId;;
            stop_px = parts[7] if len(parts) > 7 and parts[7] else None
            oid = parts[10] if len(parts) > 10 and parts[10] else None
            qty = int(parts[4]) if len(parts) > 4 and parts[4] else 0
            if oid and stop_px:
                self.change_stop(oid, float(stop_px), qty)

        elif action == "CANCELALLORDERS":
            self.cancel_all()

        else:
            log.warning(f"Unknown OIF action: {action}")

    # ── Public API (matches NT8Bridge interface) ──────────────────────────────

    def place_bracket(self, direction: str, qty: int,
                      stop_price: float, target_price: float) -> dict:
        """Market entry + stop + target. Returns {entry_oid, stop_oid, target_oid}."""
        is_long = direction in ("long", "bullish")
        entry_side = "BUY" if is_long else "SELL"
        exit_side = "SELL" if is_long else "BUY"

        entry_oid = self._oid("e")
        stop_oid = self._oid("s")
        target_oid = self._oid("t")

        self._submit_order(entry_side, qty, ORDER_TYPE_MARKET, client_oid=entry_oid)
        time.sleep(0.5)
        self._submit_order(exit_side, qty, ORDER_TYPE_STOP,
                           price=stop_price, client_oid=stop_oid)
        self._submit_order(exit_side, qty, ORDER_TYPE_LIMIT,
                           price=target_price, client_oid=target_oid)

        log.info(f"Sierra bracket: {entry_side} {qty} {self.symbol} "
                 f"stop={_round_tick(stop_price)} target={_round_tick(target_price)}")
        return {"entry_oid": entry_oid, "stop_oid": stop_oid, "target_oid": target_oid}

    def place_entry_and_stop(self, direction: str, qty: int,
                             stop_price: float) -> dict:
        """Market entry + stop only (trail-only, no target). Returns oids dict."""
        is_long = direction in ("long", "bullish")
        entry_side = "BUY" if is_long else "SELL"
        exit_side = "SELL" if is_long else "BUY"

        entry_oid = self._oid("e")
        stop_oid = self._oid("s")

        self._submit_order(entry_side, qty, ORDER_TYPE_MARKET, client_oid=entry_oid)
        time.sleep(0.5)
        self._submit_order(exit_side, qty, ORDER_TYPE_STOP,
                           price=stop_price, client_oid=stop_oid)

        log.info(f"Sierra entry+stop: {entry_side} {qty} {self.symbol} "
                 f"stop={_round_tick(stop_price)} (trail-only)")
        return {"entry_oid": entry_oid, "stop_oid": stop_oid, "target_oid": None}

    def place_limit_entry_only(self, direction: str, qty: int,
                               limit_price: float) -> dict:
        """Limit entry only (charm S/R deferred). Returns oids dict."""
        is_long = direction in ("long", "bullish")
        entry_side = "BUY" if is_long else "SELL"
        entry_oid = self._oid("L")

        self._submit_order(entry_side, qty, ORDER_TYPE_LIMIT,
                           price=limit_price, client_oid=entry_oid)

        log.info(f"Sierra LIMIT entry: {entry_side} {qty} {self.symbol} "
                 f"@ {_round_tick(limit_price)}")
        return {"entry_oid": entry_oid, "stop_oid": None, "target_oid": None}

    def place_deferred_exits(self, direction: str, qty: int,
                             stop_price: float,
                             target_price: float | None = None) -> dict:
        """Place stop + optional target after limit entry fills. Returns oids dict."""
        is_long = direction in ("long", "bullish")
        exit_side = "SELL" if is_long else "BUY"

        stop_oid = self._oid("s")
        self._submit_order(exit_side, qty, ORDER_TYPE_STOP,
                           price=stop_price, client_oid=stop_oid)

        target_oid = None
        if target_price is not None:
            time.sleep(0.3)
            target_oid = self._oid("t")
            self._submit_order(exit_side, qty, ORDER_TYPE_LIMIT,
                               price=target_price, client_oid=target_oid)

        log.info(f"Sierra deferred exits: stop={_round_tick(stop_price)} "
                 f"target={_round_tick(target_price) if target_price else 'trail-only'}")
        return {"stop_oid": stop_oid, "target_oid": target_oid}

    def change_stop(self, order_id: str, new_stop_price: float, qty: int,
                    direction: str = "long"):
        """Modify stop order price via DTC CANCEL_REPLACE.

        If ServerOrderID is unknown (e.g., after restart), cancels the old
        stop and places a new one to get a fresh ServerOrderID.
        Returns the (possibly new) order_id.
        """
        server_oid = self._get_server_oid(order_id)
        if not server_oid:
            # No ServerOrderID — cancel old + place new stop
            log.warning(f"No ServerOrderID for {order_id} — replacing with new stop")
            self.cancel(order_id)
            time.sleep(0.3)
            is_long = direction in ("long", "bullish")
            exit_side = "SELL" if is_long else "BUY"
            # Reuse the SAME client order_id so PositionTracker's stop_oid stays valid
            self._submit_order(exit_side, qty, ORDER_TYPE_STOP,
                               price=new_stop_price, client_oid=order_id)
            log.info(f"Sierra NEW stop: {order_id} @ {_round_tick(new_stop_price)} "
                     f"(replaced old — new ServerOrderID will arrive)")
            # Wait briefly for ORDER_UPDATE with new ServerOrderID
            time.sleep(0.5)
            return order_id

        tick_price = round(round(new_stop_price / MES_TICK_SIZE) * MES_TICK_SIZE, 2)
        self._send({
            "Type": CANCEL_REPLACE,
            "ServerOrderID": server_oid,
            "ClientOrderID": order_id,
            "Price1": tick_price * self.PRICE_MULT,
            "Price1IsSet": 1,
            "Price2IsSet": 0,
            "Quantity": float(qty),
            "TimeInForce": TIF_DAY,
        })
        log.info(f"Sierra CHANGE stop: {order_id} → {_round_tick(new_stop_price)}")
        return order_id

    def cancel(self, order_id: str):
        """Cancel order by client order ID."""
        server_oid = self._get_server_oid(order_id)
        msg = {"Type": CANCEL_ORDER, "ClientOrderID": order_id}
        if server_oid:
            msg["ServerOrderID"] = server_oid
        self._send(msg)
        log.info(f"Sierra cancel: {order_id}")

    def close_position(self, direction: str = "long", qty: int = 0):
        """Flatten via counter market order (same as NT8Bridge approach)."""
        if not qty:
            log.warning("close_position called with qty=0, skipping")
            return
        close_side = "SELL" if direction in ("long", "bullish") else "BUY"
        close_oid = self._oid("x")
        self._submit_order(close_side, qty, ORDER_TYPE_MARKET, client_oid=close_oid)
        log.info(f"Sierra close: {close_side} {qty} {self.symbol} (oid={close_oid})")

    def cancel_all(self):
        """Cancel all working orders + flatten position for this symbol."""
        # DTC FLATTEN cancels position only — need to cancel orders first
        # Query open orders, then cancel each
        self._send({
            "Type": OPEN_ORDERS_REQUEST,
            "RequestID": int(time.time()) % 10000,
            "RequestAllOrders": 1,
            "TradeAccount": self.account,
        })
        # Give time for ORDER_UPDATE responses to come back
        time.sleep(1.0)

        # Cancel all OPEN orders we know about
        with self._lock:
            open_oids = [
                oid for oid, state in self._order_states.items()
                if state["status"] in ("OPEN", "PENDING_OPEN", "PARTIAL")
            ]
        for oid in open_oids:
            self.cancel(oid)
            time.sleep(0.1)

        log.info(f"Sierra cancel_all: cancelled {len(open_oids)} working orders")

    def check_order_state(self, order_id: str) -> dict | None:
        """Check order status from internal cache. Same return as NT8Bridge.

        Returns {status: str, qty: int, price: float} or None.
        """
        with self._lock:
            state = self._order_states.get(order_id)
            if not state:
                return None
            return {
                "status": state["status"],
                "qty": state["qty"],
                "price": state["price"],
            }

    # ── Sierra-only methods ───────────────────────────────────────────────────

    def get_position(self, symbol: str | None = None) -> dict | None:
        """Query current position. Returns {qty, avg_price} or None if flat.

        Positive qty = long, negative qty = short.
        """
        sym = symbol or self.symbol
        # Request fresh position data
        req_id = int(time.time()) % 10000
        self._send({
            "Type": POSITIONS_REQUEST,
            "RequestID": req_id,
            "TradeAccount": self.account,
        })
        time.sleep(0.5)  # wait for response

        with self._lock:
            pos = self._positions.get(sym)
            if pos and pos["qty"] != 0:
                return pos
        return None

    def get_balance(self, account: str | None = None) -> dict | None:
        """Get account balance. Returns {cash, margin_used, open_pnl} or None."""
        acct = account or self.account
        # Request fresh balance
        self._send({
            "Type": ACCOUNT_BALANCE_REQUEST,
            "RequestID": int(time.time()) % 10000,
            "TradeAccount": acct,
        })
        time.sleep(0.3)
        with self._lock:
            return self._balances.get(acct)

    def _get_server_oid(self, client_oid: str) -> str | None:
        """Get DTC ServerOrderID for cancel/modify operations."""
        with self._lock:
            return self._server_oid_map.get(client_oid)
