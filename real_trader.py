"""
real_trader.py -- MES Futures REAL MONEY Auto-Trader via TradeStation API

Standalone local script that:
  1. Polls Railway API for Skew Charm signals
  2. Filters by configured direction (long-only or short-only per account)
  3. Places MES bracket orders on TradeStation REAL accounts
  4. Manages trailing stops, crash recovery, EOD flatten
  5. Sends Telegram on EVERY action

SAFETY:
  - Hardcoded account whitelist: ONLY 210VYX65 and 210VYX91
  - Account-direction binding: 210VYX65=long, 210VYX91=short
  - 1 MES per trade, max 2 concurrent
  - Daily loss limit $300 default
  - Master switch "enabled": false by default

Usage:
  python real_trader.py --config real_trader_config_longs.json
  python real_trader.py --config real_trader_config_shorts.json

Requirements: Python 3.10+, requests
"""

import os, sys, json, time, logging, calendar, argparse, atexit
from datetime import datetime, timedelta, time as dtime, date
from pathlib import Path
from threading import Lock

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests

# ============================================================================
#  HARDCODED SAFETY CONSTANTS -- DO NOT CHANGE
# ============================================================================

# Only these two accounts are allowed. Period.
ACCOUNT_WHITELIST = {"210VYX65", "210VYX91"}

# Each account is bound to exactly one direction.
ACCOUNT_DIRECTION_BINDING = {
    "210VYX65": "long",    # Account A: longs only
    "210VYX91": "short",   # Account B: shorts only
}

# Real TS API base -- NOT sim-api
TS_API_BASE = "https://api.tradestation.com/v3"

# ============================================================================
#  Timezone & Logging
# ============================================================================

CT = ZoneInfo("US/Central")
ET = ZoneInfo("US/Eastern")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("real_trader")

# ============================================================================
#  MES Contract Auto-Rollover (same as eval_trader.py)
# ============================================================================

MES_POINT_VALUE = 5.0
MES_TICK_SIZE = 0.25
_MES_MONTHS = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]


def _third_friday(year: int, month: int) -> date:
    c = calendar.Calendar(firstweekday=calendar.MONDAY)
    fridays = [d for d in c.itermonthdates(year, month)
               if d.month == month and d.weekday() == 4]
    return fridays[2]


def _auto_mes_symbol() -> str:
    """Return front-month MES symbol for TradeStation (e.g. MESH26),
    rolling ~8 days before 3rd Friday expiry."""
    today = date.today()
    for month_num, code in _MES_MONTHS:
        expiry = _third_friday(today.year, month_num)
        if today <= expiry - timedelta(days=8):
            return f"MES{code}{today.year % 100}"
    return f"MESH{(today.year + 1) % 100}"


def _round_mes(price: float) -> float:
    """Round price to nearest MES tick (0.25)."""
    return round(round(price / MES_TICK_SIZE) * MES_TICK_SIZE, 2)


# ============================================================================
#  File paths (derived from config filename)
# ============================================================================

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "real_trader_config.json"
STATE_FILE = SCRIPT_DIR / "real_trader_state.json"
POSITION_FILE = SCRIPT_DIR / "real_trader_positions.json"
API_STATE_FILE = SCRIPT_DIR / "real_trader_api_state.json"
LOG_FILE = "real_trader.log"
LOCK_FILE = SCRIPT_DIR / "real_trader.lock"


def _init_file_paths(config_path: str):
    """Derive state/position/api/log paths from the config filename."""
    global CONFIG_FILE, STATE_FILE, POSITION_FILE, API_STATE_FILE, LOG_FILE, LOCK_FILE
    p = Path(config_path)
    config_name = p.stem  # e.g. "real_trader_config_longs"
    prefix = "real_trader_config"
    suffix = config_name[len(prefix):] if config_name.startswith(prefix) else ""
    config_dir = p.parent if p.is_absolute() else SCRIPT_DIR

    CONFIG_FILE = config_dir / f"real_trader_config{suffix}.json"
    STATE_FILE = config_dir / f"real_trader_state{suffix}.json"
    POSITION_FILE = config_dir / f"real_trader_positions{suffix}.json"
    API_STATE_FILE = config_dir / f"real_trader_api_state{suffix}.json"
    LOG_FILE = f"real_trader{suffix}.log"
    LOCK_FILE = config_dir / f"real_trader{suffix}.lock"


# ============================================================================
#  Singleton Lock (same pattern as eval_trader.py)
# ============================================================================

def _acquire_singleton_lock():
    if LOCK_FILE.exists():
        try:
            old_pid = int(LOCK_FILE.read_text().strip())
            if sys.platform == "win32":
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x1000, False, old_pid)
                if handle:
                    kernel32.CloseHandle(handle)
                    print(f"\n{'='*60}")
                    print(f"  BLOCKED: Another real_trader instance already running!")
                    print(f"  PID: {old_pid}  |  Lock: {LOCK_FILE.name}")
                    print(f"  Kill it first, then retry.")
                    print(f"{'='*60}\n")
                    sys.exit(1)
            else:
                os.kill(old_pid, 0)
                print(f"\n{'='*60}")
                print(f"  BLOCKED: Another real_trader instance already running!")
                print(f"  PID: {old_pid}  |  Lock: {LOCK_FILE.name}")
                print(f"{'='*60}\n")
                sys.exit(1)
        except (OSError, ValueError):
            pass  # stale lock, safe to take over

    LOCK_FILE.write_text(str(os.getpid()))
    atexit.register(_release_singleton_lock)


def _release_singleton_lock():
    try:
        if LOCK_FILE.exists() and LOCK_FILE.read_text().strip() == str(os.getpid()):
            LOCK_FILE.unlink()
    except Exception:
        pass


# ============================================================================
#  Default Configuration
# ============================================================================

DEFAULT_CONFIG = {
    # -- Master switch (MUST be explicitly set to true) --
    "enabled": False,

    # -- Account --
    "account_id": "",           # Must be in ACCOUNT_WHITELIST
    "direction": "",            # "long" or "short" -- must match ACCOUNT_DIRECTION_BINDING

    # -- TS API credentials (can also come from env vars) --
    "ts_client_id": "",
    "ts_client_secret": "",
    "ts_refresh_token": "",

    # -- Signal source --
    "railway_api_url": "",
    "eval_api_key": "",
    "poll_interval_s": 2,

    # -- Telegram --
    "telegram_bot_token": "",
    "telegram_chat_id": "",

    # -- MES symbol --
    "mes_symbol": "auto",       # "auto" or e.g. "MESM26"

    # -- Sizing --
    "qty_per_trade": 1,
    "max_concurrent": 2,

    # -- Risk --
    "stop_pts": 14,
    "target_pts": 10,           # First target (bracket limit)
    "trail_be_trigger": 10,     # Move stop to breakeven at +10 pts
    "trail_activation": 10,     # Continuous trail starts at +10 pts
    "trail_gap": 8,             # Trail gap: lock at max_profit - 8

    # -- Daily loss limit --
    "daily_loss_limit": 300,    # Stop trading when daily P&L <= -$300

    # -- Time rules (Central Time) --
    "market_open_ct": "08:30",
    "no_new_trades_after_ct": "15:20",
    "flatten_time_ct": "15:50",

    # -- Max signal age --
    "max_signal_age_s": 120,
}


# ============================================================================
#  Config Management
# ============================================================================

def load_config() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            saved = json.load(f)
        merged = {**DEFAULT_CONFIG, **saved}
        return merged
    else:
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()


def save_config(cfg: dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)


# ============================================================================
#  TradeStation OAuth2
# ============================================================================

class TSAuth:
    """Manages OAuth2 token refresh for TradeStation REAL API."""

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = None
        self.token_expiry = 0.0

    def get_token(self) -> str | None:
        """Returns a valid access token, refreshing if needed."""
        if self.access_token and time.time() < self.token_expiry:
            return self.access_token
        return self._refresh()

    def _refresh(self) -> str | None:
        try:
            resp = requests.post(
                "https://signin.tradestation.com/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token,
                },
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                self.access_token = data["access_token"]
                self.token_expiry = time.time() + data.get("expires_in", 1200) - 60
                log.info("TS API token refreshed successfully")
                return self.access_token
            else:
                log.error(f"TS token refresh failed: HTTP {resp.status_code} - {resp.text[:200]}")
                return None
        except Exception as e:
            log.error(f"TS token refresh error: {e}")
            return None


# ============================================================================
#  TS API Helper
# ============================================================================

def _ts_api(auth: TSAuth, account_id: str, method: str, path: str,
            json_body: dict | None = None) -> dict | None:
    """Authenticated request to TradeStation REAL API.

    SAFETY: Validates account_id against whitelist EVERY call.
    """
    # CRITICAL SAFETY: re-validate account on every API call
    if account_id not in ACCOUNT_WHITELIST:
        log.error(f"SAFETY BLOCK: account {account_id} not in whitelist! Refusing API call.")
        return None

    for attempt in range(2):
        token = auth.get_token()
        if not token:
            log.error("No valid token -- cannot make API call")
            return None
        try:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            url = f"{TS_API_BASE}{path}"

            log.debug(f"API {method} {url}")
            if json_body:
                log.debug(f"  payload: {json.dumps(json_body, default=str)[:400]}")

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

            log.debug(f"  response: [{r.status_code}] {r.text[:300]}")

            if r.status_code == 401 and attempt == 0:
                log.warning("TS API 401 -- refreshing token and retrying")
                auth.access_token = None  # force refresh
                continue

            if r.status_code >= 400:
                log.error(f"TS API {method} {path} [{r.status_code}]: {r.text[:300]}")
                if method == "POST" and json_body:
                    log.error(f"  payload: {json.dumps(json_body, default=str)[:300]}")
                return None

            result = r.json() if r.text else {}

            # Log order-related responses fully
            if method in ("POST", "PUT") and "order" in path.lower():
                log.info(f"TS API {method} {path} [{r.status_code}]: "
                         f"{json.dumps(result, default=str)[:400]}")
            return result

        except Exception as e:
            log.error(f"TS API error {method} {path}: {e}")
            return None

    return None


def _order_ok(resp: dict | None) -> tuple[bool, str | None]:
    """Check if order response succeeded. Returns (ok, order_id).
    TS returns HTTP 200 even for FAILED orders -- must check order-level Error."""
    if not resp:
        return False, None
    orders = resp.get("Orders", [])
    if not orders:
        return False, None
    first = orders[0]
    if first.get("Error") == "FAILED":
        msg = first.get("Message", "unknown error")
        log.error(f"Order FAILED: {msg}")
        return False, first.get("OrderID")
    oid = first.get("OrderID")
    return bool(oid), oid


def _extract_fill_price(order: dict) -> float | None:
    """Extract fill price from broker order response."""
    try:
        fp = float(order.get("FilledPrice", 0))
        if fp > 0:
            return fp
    except (ValueError, TypeError):
        pass
    fills = order.get("Legs", [{}])
    if fills:
        try:
            ep = float(fills[0].get("ExecPrice", 0))
            if ep > 0:
                return ep
        except (ValueError, TypeError):
            pass
    return None


# ============================================================================
#  Telegram
# ============================================================================

def _send_telegram(cfg: dict, msg: str):
    """Send Telegram message. Logs errors but never raises."""
    token = cfg.get("telegram_bot_token", "")
    chat_id = cfg.get("telegram_chat_id", "")
    if not token or not chat_id:
        log.warning("Telegram not configured -- message not sent")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            log.debug("Telegram sent OK")
        else:
            log.warning(f"Telegram send failed: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        log.warning(f"Telegram send error: {e}")


def _tg(cfg: dict, msg: str):
    """Shorthand: send Telegram + log."""
    log.info(f"[TG] {msg[:200]}")
    _send_telegram(cfg, msg)


# ============================================================================
#  API Poller (same pattern as eval_trader APIPoller, simplified)
# ============================================================================

MAX_SIGNAL_AGE_S = 120
TRADE_DEDUP_WINDOW = 120

class APIPoller:
    """Polls Railway /api/eval/signals for Skew Charm signals."""

    def __init__(self, api_url: str, api_key: str):
        self.url = api_url.rstrip("/") + "/api/eval/signals"
        self.api_key = api_key
        self.last_id = 0
        self._seen_signals: set[int] = set()
        self._state_date: str = ""
        self._load_state()

    def _load_state(self):
        if API_STATE_FILE.exists():
            try:
                data = json.loads(API_STATE_FILE.read_text())
                saved_date = data.get("date", "")
                today = date.today().isoformat()
                if saved_date == today:
                    self.last_id = data.get("last_id", 0)
                    self._seen_signals = set(data.get("seen_signals", []))
                    self._state_date = today
                    log.info(f"API poller state restored: last_id={self.last_id}, "
                             f"seen={len(self._seen_signals)}")
                else:
                    log.info(f"API poller: new day (was {saved_date}), resetting state")
                    self._state_date = today
            except Exception:
                pass

    def _save_state(self):
        API_STATE_FILE.write_text(json.dumps({
            "date": date.today().isoformat(),
            "last_id": self.last_id,
            "seen_signals": list(self._seen_signals),
        }))

    def poll(self) -> tuple[list[dict], float | None]:
        """Poll the API. Returns (new_signals, es_price).

        Each signal has: setup_name, direction, spot, signal_ts, grade,
        msg_target_pts, msg_stop_pts, es_price, charm_limit_entry
        """
        try:
            resp = requests.get(
                self.url,
                params={"since_id": self.last_id},
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=8,
            )
            if resp.status_code == 401:
                log.error("API auth failed -- check eval_api_key")
                return [], None
            if resp.status_code != 200:
                log.warning(f"API poll error: HTTP {resp.status_code}")
                return [], None
            data = resp.json()
        except requests.ConnectionError:
            log.debug("API unreachable -- will retry")
            return [], None
        except Exception as e:
            log.error(f"API poll error: {e}")
            return [], None

        raw_signals = data.get("signals", [])
        es_price = data.get("es_price")
        if es_price is not None:
            es_price = float(es_price)

        if raw_signals:
            max_id = max(s["id"] for s in raw_signals)
            if max_id > self.last_id:
                self.last_id = max_id
                self._save_state()

        new_signals = []
        for s in raw_signals:
            sid = s["id"]
            if sid in self._seen_signals:
                continue
            self._seen_signals.add(sid)
            sig = self._convert(s, es_price)
            if sig:
                new_signals.append(sig)
        if new_signals:
            self._save_state()

        return new_signals, es_price

    def _convert(self, s: dict, es_price: float | None) -> dict | None:
        setup = s.get("setup_name")
        direction = s.get("direction", "long")
        spot = s.get("spot")
        if not spot:
            return None

        target_level = s.get("target_level")
        stop_level = s.get("stop_level")
        msg_target_pts = None
        msg_stop_pts = None
        if target_level is not None:
            msg_target_pts = round(abs(target_level - spot), 1)
        if stop_level is not None:
            msg_stop_pts = round(abs(spot - stop_level), 1)

        return {
            "setup_name": setup,
            "direction": direction,
            "spot": spot,
            "signal_ts": s.get("ts"),
            "grade": s.get("grade", "?"),
            "msg_target_pts": msg_target_pts,
            "msg_stop_pts": msg_stop_pts,
            "es_price": es_price,
            "charm_limit_entry": s.get("charm_limit_entry"),
            "paradigm": s.get("paradigm"),
            "greek_alignment": s.get("greek_alignment"),
        }


# ============================================================================
#  Position Manager
# ============================================================================

class PositionManager:
    """Manages open positions with crash recovery via JSON file.

    Each position is keyed by a unique trade_id (timestamp-based).
    Max concurrent positions enforced.
    """

    def __init__(self, cfg: dict, auth: TSAuth):
        self.cfg = cfg
        self.auth = auth
        self.account_id = cfg["account_id"]
        self.mes_symbol = cfg["_resolved_symbol"]
        self._lock = Lock()
        self.positions: dict[str, dict] = {}   # trade_id -> position state
        self._load()

    def _load(self):
        if POSITION_FILE.exists():
            try:
                data = json.loads(POSITION_FILE.read_text())
                today_str = date.today().isoformat()
                loaded = 0
                stale = 0
                for tid, pos in data.items():
                    pos_date = pos.get("ts", "")[:10]
                    if pos_date < today_str:
                        stale += 1
                        log.warning(f"STALE position {tid} from {pos_date} -- will flatten on startup")
                        pos["_stale"] = True
                    self.positions[tid] = pos
                    loaded += 1
                if loaded:
                    log.info(f"Loaded {loaded} positions ({stale} stale)")
            except Exception as e:
                log.warning(f"Position load error: {e}")

    def _save(self):
        with self._lock:
            POSITION_FILE.write_text(json.dumps(self.positions, indent=2))

    @property
    def active_count(self) -> int:
        with self._lock:
            return sum(1 for p in self.positions.values()
                       if p.get("status") in ("pending_entry", "pending_limit", "filled"))

    @property
    def active_positions(self) -> list[tuple[str, dict]]:
        with self._lock:
            return [(tid, p) for tid, p in self.positions.items()
                    if p.get("status") in ("pending_entry", "pending_limit", "filled")]

    def has_stale(self) -> list[str]:
        with self._lock:
            return [tid for tid, p in self.positions.items() if p.get("_stale")]

    def new_trade_id(self) -> str:
        return f"rt_{int(time.time() * 1000)}"

    # ---- Order Placement ----

    def place_market_bracket(self, trade_id: str, signal: dict) -> bool:
        """Place market entry + stop + target bracket for a new trade.
        Returns True on success."""
        cfg = self.cfg
        direction = cfg["direction"]
        is_long = direction == "long"
        side = "Buy" if is_long else "Sell"
        exit_side = "Sell" if is_long else "Buy"
        qty = cfg["qty_per_trade"]
        stop_pts = cfg["stop_pts"]
        target_pts = cfg["target_pts"]

        # Use ES price for stop/target calculation
        es_price = signal.get("es_price")
        spot = signal["spot"]
        if es_price:
            es_price = float(es_price)
            if abs(es_price - spot) > 75:
                log.warning(f"Stale ES price {es_price} vs SPX {spot} -- using SPX")
                es_price = None
        order_ref = es_price if es_price else spot

        if is_long:
            stop_price = _round_mes(order_ref - stop_pts)
            target_price = _round_mes(order_ref + target_pts)
        else:
            stop_price = _round_mes(order_ref + stop_pts)
            target_price = _round_mes(order_ref - target_pts)

        # ---- SAFETY: validate before every order ----
        if not self._validate_order_safety(direction):
            return False

        # 1. Market entry
        entry_payload = {
            "AccountID": self.account_id,
            "Symbol": self.mes_symbol,
            "Quantity": str(qty),
            "OrderType": "Market",
            "TradeAction": side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        resp = _ts_api(self.auth, self.account_id, "POST",
                       "/orderexecution/orders", entry_payload)
        ok, entry_oid = _order_ok(resp)
        if not ok:
            _tg(cfg, f"[REAL-TRADE] FAILED entry for Skew Charm\n"
                     f"Side: {side} {qty} {self.mes_symbol} @ ~{order_ref:.2f}\n"
                     f"Account: {self.account_id}")
            return False

        # 2. Stop order
        stop_payload = {
            "AccountID": self.account_id,
            "Symbol": self.mes_symbol,
            "Quantity": str(qty),
            "OrderType": "StopMarket",
            "StopPrice": str(stop_price),
            "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        stop_resp = _ts_api(self.auth, self.account_id, "POST",
                            "/orderexecution/orders", stop_payload)
        stop_ok, stop_oid = _order_ok(stop_resp)
        if not stop_ok:
            stop_oid = None
            _tg(cfg, f"[REAL-TRADE] CRITICAL: Entry placed but STOP FAILED!\n"
                     f"Account: {self.account_id}\n"
                     f"MANUAL INTERVENTION NEEDED")

        # 3. Target limit
        target_payload = {
            "AccountID": self.account_id,
            "Symbol": self.mes_symbol,
            "Quantity": str(qty),
            "OrderType": "Limit",
            "LimitPrice": str(target_price),
            "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        target_resp = _ts_api(self.auth, self.account_id, "POST",
                              "/orderexecution/orders", target_payload)
        target_ok, target_oid = _order_ok(target_resp)
        if not target_ok:
            target_oid = None
            log.warning("Target limit order failed -- trail/outcome will handle exit")

        # Record position
        pos = {
            "trade_id": trade_id,
            "setup_name": "Skew Charm",
            "direction": direction,
            "grade": signal.get("grade", "?"),
            "entry_order_id": entry_oid,
            "stop_order_id": stop_oid,
            "target_order_id": target_oid,
            "entry_ref_price": order_ref,
            "es_entry_price": es_price or order_ref,
            "spx_spot": spot,
            "stop_price": stop_price,
            "target_price": target_price,
            "stop_pts": stop_pts,
            "qty": qty,
            "status": "pending_entry",
            "fill_price": None,
            "be_triggered": False,
            "max_fav": 0.0,
            "ts": datetime.now(CT).isoformat(),
            "account_id": self.account_id,
        }

        with self._lock:
            self.positions[trade_id] = pos
        self._save()

        log.info(f"TRADE PLACED: SC {direction.upper()} {qty} {self.mes_symbol} "
                 f"@ ~{order_ref:.2f} stop={stop_price:.2f} target={target_price:.2f}")
        _tg(cfg, f"[REAL-TRADE] Skew Charm {direction.upper()} [{signal.get('grade', '?')}]\n"
                 f"Account: {self.account_id}\n"
                 f"{side} {qty} {self.mes_symbol} @ ~{order_ref:.2f}\n"
                 f"Stop: {stop_price:.2f} | Target: {target_price:.2f}")
        return True

    def place_limit_entry(self, trade_id: str, signal: dict) -> bool:
        """Place charm S/R limit entry for short trades. Stop+target placed after fill."""
        cfg = self.cfg
        direction = cfg["direction"]
        is_long = direction == "long"
        side = "Buy" if is_long else "Sell"
        qty = cfg["qty_per_trade"]
        stop_pts = cfg["stop_pts"]
        target_pts = cfg["target_pts"]

        es_price = signal.get("es_price")
        spot = signal["spot"]
        charm_limit_spx = signal["charm_limit_entry"]
        if es_price:
            es_price = float(es_price)
            if abs(es_price - spot) > 75:
                es_price = None
        order_ref = es_price if es_price else spot

        # Convert SPX charm limit to MES space
        spx_to_mes = order_ref - spot
        limit_price = _round_mes(charm_limit_spx + spx_to_mes)

        if not self._validate_order_safety(direction):
            return False

        entry_payload = {
            "AccountID": self.account_id,
            "Symbol": self.mes_symbol,
            "Quantity": str(qty),
            "OrderType": "Limit",
            "LimitPrice": str(limit_price),
            "TradeAction": side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        resp = _ts_api(self.auth, self.account_id, "POST",
                       "/orderexecution/orders", entry_payload)
        ok, entry_oid = _order_ok(resp)
        if not ok:
            _tg(cfg, f"[REAL-TRADE] FAILED limit entry for Skew Charm\n"
                     f"Account: {self.account_id}\n"
                     f"{side} {qty} {self.mes_symbol} LIMIT @ {limit_price:.2f}")
            return False

        if is_long:
            deferred_stop = _round_mes(order_ref - stop_pts)
            deferred_target = _round_mes(order_ref + target_pts)
        else:
            deferred_stop = _round_mes(order_ref + stop_pts)
            deferred_target = _round_mes(order_ref - target_pts)

        pos = {
            "trade_id": trade_id,
            "setup_name": "Skew Charm",
            "direction": direction,
            "grade": signal.get("grade", "?"),
            "entry_order_id": entry_oid,
            "stop_order_id": None,
            "target_order_id": None,
            "entry_ref_price": order_ref,
            "es_entry_price": es_price or order_ref,
            "spx_spot": spot,
            "stop_price": deferred_stop,
            "target_price": deferred_target,
            "stop_pts": stop_pts,
            "qty": qty,
            "status": "pending_limit",
            "fill_price": None,
            "be_triggered": False,
            "max_fav": 0.0,
            "ts": datetime.now(CT).isoformat(),
            "limit_entry_price": limit_price,
            "limit_placed_at": datetime.now(CT).isoformat(),
            "account_id": self.account_id,
        }

        with self._lock:
            self.positions[trade_id] = pos
        self._save()

        log.info(f"LIMIT PLACED: SC {direction.upper()} {qty} {self.mes_symbol} "
                 f"LIMIT @ {limit_price:.2f} (market @ {order_ref:.2f})")
        _tg(cfg, f"[REAL-TRADE] Skew Charm LIMIT entry [{signal.get('grade', '?')}]\n"
                 f"Account: {self.account_id}\n"
                 f"{side} {qty} {self.mes_symbol} LIMIT @ {limit_price:.2f}\n"
                 f"[CHARM S/R] Market @ {order_ref:.2f} -- waiting for fill")
        return True

    def _place_deferred_protective_orders(self, trade_id: str, fill_price: float):
        """Place stop + target after limit entry fills."""
        with self._lock:
            pos = self.positions.get(trade_id)
            if not pos:
                return

        cfg = self.cfg
        is_long = pos["direction"] == "long"
        exit_side = "Sell" if is_long else "Buy"
        qty = pos["qty"]
        stop_pts = pos["stop_pts"]
        target_pts = cfg["target_pts"]

        if is_long:
            stop_price = _round_mes(fill_price - stop_pts)
            target_price = _round_mes(fill_price + target_pts)
        else:
            stop_price = _round_mes(fill_price + stop_pts)
            target_price = _round_mes(fill_price - target_pts)

        # Stop order
        stop_payload = {
            "AccountID": self.account_id,
            "Symbol": self.mes_symbol,
            "Quantity": str(qty),
            "OrderType": "StopMarket",
            "StopPrice": str(stop_price),
            "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        stop_resp = _ts_api(self.auth, self.account_id, "POST",
                            "/orderexecution/orders", stop_payload)
        stop_ok, stop_oid = _order_ok(stop_resp)
        if not stop_ok:
            stop_oid = None
            _tg(cfg, f"[REAL-TRADE] CRITICAL: Limit FILLED but STOP FAILED!\n"
                     f"Account: {self.account_id}\n"
                     f"MANUAL INTERVENTION NEEDED")

        # Target limit
        target_payload = {
            "AccountID": self.account_id,
            "Symbol": self.mes_symbol,
            "Quantity": str(qty),
            "OrderType": "Limit",
            "LimitPrice": str(target_price),
            "TradeAction": exit_side,
            "TimeInForce": {"Duration": "DAY"},
            "Route": "Intelligent",
        }
        target_resp = _ts_api(self.auth, self.account_id, "POST",
                              "/orderexecution/orders", target_payload)
        target_ok, target_oid = _order_ok(target_resp)
        if not target_ok:
            target_oid = None

        with self._lock:
            pos["stop_order_id"] = stop_oid
            pos["target_order_id"] = target_oid
            pos["stop_price"] = stop_price
            pos["target_price"] = target_price
        self._save()

        imp = abs(fill_price - pos.get("entry_ref_price", fill_price))
        log.info(f"DEFERRED orders placed: stop={stop_price:.2f} target={target_price:.2f} "
                 f"(entry improved {imp:.1f}pts)")
        _tg(cfg, f"[REAL-TRADE] SC LIMIT FILLED @ {fill_price:.2f}\n"
                 f"Account: {self.account_id}\n"
                 f"[CHARM S/R] Improved {imp:+.1f}pts from market\n"
                 f"Stop: {stop_price:.2f} | Target: {target_price:.2f}")

    # ---- Order Polling ----

    def poll_orders(self, es_price: float | None):
        """Poll TS for order fills. Check entry fills, stop fills, target fills, limit timeouts."""
        active = self.active_positions
        if not active:
            return

        # Fetch all orders from broker
        try:
            orders_data = _ts_api(self.auth, self.account_id, "GET",
                                  f"/brokerage/accounts/{self.account_id}/orders")
        except Exception as e:
            log.error(f"Order poll error: {e}")
            return
        if not orders_data:
            return

        broker_orders = {}
        for o in orders_data.get("Orders", []):
            oid = o.get("OrderID")
            if oid:
                broker_orders[oid] = o

        for tid, pos in active:
            self._check_fills(tid, pos, broker_orders, es_price)

    def _check_fills(self, tid: str, pos: dict, broker_orders: dict,
                     es_price: float | None):
        changed = False
        cfg = self.cfg

        # ---- Pending limit entry ----
        if pos["status"] == "pending_limit" and pos.get("entry_order_id"):
            entry = broker_orders.get(pos["entry_order_id"], {})
            entry_status = entry.get("Status", "")

            if entry_status == "FLL":
                fill_price = _extract_fill_price(entry)
                with self._lock:
                    pos["status"] = "filled"
                    pos["fill_price"] = fill_price
                    pos["es_entry_price"] = fill_price
                changed = True
                self._place_deferred_protective_orders(tid, fill_price)

            elif entry_status in ("REJ", "CAN", "EXP"):
                with self._lock:
                    pos["status"] = "closed"
                    pos["close_reason"] = f"limit_entry_{entry_status}"
                changed = True
                log.info(f"Limit entry {entry_status}: {tid}")
                _tg(cfg, f"[REAL-TRADE] SC LIMIT {entry_status}\n"
                         f"Account: {self.account_id}\n"
                         f"Entry not filled -- trade skipped")

            else:
                # Check 30-min timeout
                placed_at = pos.get("limit_placed_at")
                if placed_at:
                    try:
                        placed_dt = datetime.fromisoformat(placed_at)
                        elapsed = (datetime.now(CT) - placed_dt).total_seconds()
                        if elapsed > 1800:
                            _ts_api(self.auth, self.account_id, "DELETE",
                                    f"/orderexecution/orders/{pos['entry_order_id']}")
                            with self._lock:
                                pos["status"] = "closed"
                                pos["close_reason"] = "limit_timeout"
                            changed = True
                            log.info(f"LIMIT TIMEOUT: {tid} cancelled after {elapsed/60:.0f}min")
                            _tg(cfg, f"[REAL-TRADE] SC LIMIT EXPIRED\n"
                                     f"Account: {self.account_id}\n"
                                     f"{pos.get('limit_entry_price', 0):.2f} not reached "
                                     f"in {elapsed/60:.0f}min")
                    except (ValueError, TypeError):
                        pass

        # ---- Pending entry (market) ----
        if pos["status"] == "pending_entry" and pos.get("entry_order_id"):
            entry = broker_orders.get(pos["entry_order_id"], {})
            entry_status = entry.get("Status", "")
            if entry_status == "FLL":
                fill_price = _extract_fill_price(entry)
                with self._lock:
                    pos["status"] = "filled"
                    pos["fill_price"] = fill_price
                    pos["es_entry_price"] = fill_price or pos.get("es_entry_price")
                changed = True
                log.info(f"FILLED: {tid} @ {fill_price}")
                _tg(cfg, f"[REAL-TRADE] SC FILLED @ {fill_price}\n"
                         f"Account: {self.account_id}\n"
                         f"Stop: {pos['stop_price']:.2f} | Target: {pos['target_price']:.2f}")
            elif entry_status in ("REJ", "CAN", "EXP"):
                with self._lock:
                    pos["status"] = "closed"
                    pos["close_reason"] = f"entry_{entry_status}"
                changed = True
                rej = entry.get("RejectReason") or entry.get("StatusDescription") or ""
                log.info(f"Entry {entry_status}: {tid} reason={rej}")
                _tg(cfg, f"[REAL-TRADE] Entry {entry_status}\n"
                         f"Account: {self.account_id}\nReason: {rej}")

        # ---- Filled position: check stop/target fills ----
        if pos["status"] == "filled":
            # Check stop fill
            if pos.get("stop_order_id"):
                stop_o = broker_orders.get(pos["stop_order_id"], {})
                if stop_o.get("Status") == "FLL":
                    stop_fp = _extract_fill_price(stop_o)
                    entry_fp = pos.get("fill_price") or pos.get("es_entry_price", 0)
                    is_long = pos["direction"] == "long"
                    pnl = (stop_fp - entry_fp) if is_long else (entry_fp - stop_fp) if stop_fp and entry_fp else 0
                    with self._lock:
                        pos["status"] = "closed"
                        pos["close_reason"] = "stop_filled"
                        pos["close_price"] = stop_fp
                        pos["pnl_pts"] = round(pnl, 2)
                    changed = True
                    # Cancel target
                    if pos.get("target_order_id"):
                        _ts_api(self.auth, self.account_id, "DELETE",
                                f"/orderexecution/orders/{pos['target_order_id']}")
                    log.info(f"STOP FILLED: {tid} @ {stop_fp} PnL={pnl:+.1f}pts")
                    _tg(cfg, f"[REAL-TRADE] STOP FILLED\n"
                             f"Account: {self.account_id}\n"
                             f"@ {stop_fp:.2f} | PnL: {pnl:+.1f}pts (${pnl * pos['qty'] * MES_POINT_VALUE:+.0f})")

            # Check target fill
            if pos.get("target_order_id") and pos["status"] == "filled":
                target_o = broker_orders.get(pos["target_order_id"], {})
                if target_o.get("Status") == "FLL":
                    target_fp = _extract_fill_price(target_o)
                    entry_fp = pos.get("fill_price") or pos.get("es_entry_price", 0)
                    is_long = pos["direction"] == "long"
                    pnl = (target_fp - entry_fp) if is_long else (entry_fp - target_fp) if target_fp and entry_fp else 0
                    with self._lock:
                        pos["status"] = "closed"
                        pos["close_reason"] = "target_filled"
                        pos["close_price"] = target_fp
                        pos["pnl_pts"] = round(pnl, 2)
                    changed = True
                    # Cancel stop
                    if pos.get("stop_order_id"):
                        _ts_api(self.auth, self.account_id, "DELETE",
                                f"/orderexecution/orders/{pos['stop_order_id']}")
                    log.info(f"TARGET FILLED: {tid} @ {target_fp} PnL={pnl:+.1f}pts")
                    _tg(cfg, f"[REAL-TRADE] TARGET FILLED\n"
                             f"Account: {self.account_id}\n"
                             f"@ {target_fp:.2f} | PnL: {pnl:+.1f}pts (${pnl * pos['qty'] * MES_POINT_VALUE:+.0f})")

        if changed:
            self._save()

    # ---- Trail Logic (SC params: BE@10, activation=10, gap=8) ----

    def check_trails(self, es_price: float | None):
        """Check trailing stop for all filled positions."""
        if not es_price:
            return
        for tid, pos in self.active_positions:
            if pos["status"] != "filled":
                continue
            if pos.get("pending_limit"):
                continue
            self._trail_position(tid, pos, es_price)

    def _trail_position(self, tid: str, pos: dict, es_price: float):
        cfg = self.cfg
        es_entry = pos.get("es_entry_price") or pos.get("fill_price")
        if not es_entry:
            return

        is_long = pos["direction"] == "long"
        profit = (es_price - es_entry) if is_long else (es_entry - es_price)

        # Track max favorable excursion
        max_fav = pos.get("max_fav", 0.0)
        if profit > max_fav:
            max_fav = profit
            pos["max_fav"] = max_fav

        be_trigger = cfg.get("trail_be_trigger", 10)
        activation = cfg.get("trail_activation", 10)
        gap = cfg.get("trail_gap", 8)
        new_stop = None

        # 1. Breakeven trigger
        if not pos.get("be_triggered") and profit >= be_trigger:
            new_stop = es_entry
            pos["be_triggered"] = True

        # 2. Continuous trail (overrides BE once activated)
        if max_fav >= activation:
            lock = max_fav - gap
            trail_stop = (es_entry + lock) if is_long else (es_entry - lock)
            if new_stop is None or (is_long and trail_stop > new_stop) or (not is_long and trail_stop < new_stop):
                new_stop = trail_stop

        if new_stop is not None:
            new_stop = _round_mes(new_stop)
            current_stop = pos["stop_price"]
            tighter = (new_stop > current_stop) if is_long else (new_stop < current_stop)
            # Never moves backward -- only tighter
            if tighter and pos.get("stop_order_id"):
                # Replace stop via PUT
                replace_payload = {
                    "AccountID": self.account_id,
                    "Symbol": self.mes_symbol,
                    "Quantity": str(pos["qty"]),
                    "OrderType": "StopMarket",
                    "StopPrice": str(new_stop),
                    "TimeInForce": {"Duration": "DAY"},
                    "Route": "Intelligent",
                }
                resp = _ts_api(self.auth, self.account_id, "PUT",
                               f"/orderexecution/orders/{pos['stop_order_id']}",
                               replace_payload)
                if resp:
                    new_orders = resp.get("Orders", [])
                    if new_orders and new_orders[0].get("OrderID"):
                        pos["stop_order_id"] = new_orders[0]["OrderID"]
                    pos["stop_price"] = new_stop
                    self._save()
                    trail_type = "TRAIL" if max_fav >= activation else "BREAKEVEN"
                    log.info(f"{trail_type}: {tid} stop {current_stop:.2f} -> {new_stop:.2f} "
                             f"(profit={profit:+.1f} max={max_fav:+.1f})")
                    _tg(cfg, f"[REAL-TRADE] {trail_type}\n"
                             f"Account: {self.account_id}\n"
                             f"Stop: {current_stop:.2f} -> {new_stop:.2f}\n"
                             f"Profit: {profit:+.1f}pts | Max: {max_fav:+.1f}pts")
                else:
                    log.error(f"TRAIL FAILED: {tid} stop update to {new_stop:.2f}")
                    _tg(cfg, f"[REAL-TRADE] TRAIL FAILED!\n"
                             f"Account: {self.account_id}\n"
                             f"Could not update stop to {new_stop:.2f}\n"
                             f"MANUAL INTERVENTION MAY BE NEEDED")

    # ---- Close / Flatten ----

    def close_position(self, tid: str, reason: str = "manual"):
        """Close a specific position: cancel orders + market close."""
        with self._lock:
            pos = self.positions.get(tid)
            if not pos or pos["status"] == "closed":
                return

        cfg = self.cfg
        is_long = pos["direction"] == "long"
        close_side = "Sell" if is_long else "Buy"

        # Cancel all exit orders
        for key in ("stop_order_id", "target_order_id"):
            oid = pos.get(key)
            if oid:
                _ts_api(self.auth, self.account_id, "DELETE",
                        f"/orderexecution/orders/{oid}")

        # Cancel pending limit entry if not filled
        if pos["status"] == "pending_limit" and pos.get("entry_order_id"):
            _ts_api(self.auth, self.account_id, "DELETE",
                    f"/orderexecution/orders/{pos['entry_order_id']}")
            with self._lock:
                pos["status"] = "closed"
                pos["close_reason"] = reason
                pos["pnl_pts"] = 0
            self._save()
            log.info(f"Cancelled pending limit: {tid} ({reason})")
            _tg(cfg, f"[REAL-TRADE] Pending limit cancelled\n"
                     f"Account: {self.account_id}\nReason: {reason}")
            return

        # Market close if filled
        if pos["status"] == "filled":
            time.sleep(0.5)

            # Verify broker has a position first
            broker_pos = self._get_broker_position()
            if not broker_pos:
                log.info(f"Flatten skipped: broker already flat ({reason})")
                with self._lock:
                    pos["status"] = "closed"
                    pos["close_reason"] = reason
                self._save()
                return

            close_payload = {
                "AccountID": self.account_id,
                "Symbol": self.mes_symbol,
                "Quantity": str(pos["qty"]),
                "OrderType": "Market",
                "TradeAction": close_side,
                "TimeInForce": {"Duration": "DAY"},
                "Route": "Intelligent",
            }
            resp = _ts_api(self.auth, self.account_id, "POST",
                           "/orderexecution/orders", close_payload)
            if resp:
                close_ok, close_oid = _order_ok(resp)
                if close_ok:
                    time.sleep(1)
                    close_fp = self._get_order_fill_price(close_oid)
                    entry_fp = pos.get("fill_price") or pos.get("es_entry_price", 0)
                    pnl = 0.0
                    if close_fp and entry_fp:
                        pnl = (close_fp - entry_fp) if is_long else (entry_fp - close_fp)
                    with self._lock:
                        pos["status"] = "closed"
                        pos["close_reason"] = reason
                        pos["close_price"] = close_fp
                        pos["pnl_pts"] = round(pnl, 2)
                    log.info(f"CLOSED: {tid} @ {close_fp} PnL={pnl:+.1f}pts ({reason})")
                    _tg(cfg, f"[REAL-TRADE] CLOSED ({reason})\n"
                             f"Account: {self.account_id}\n"
                             f"@ {close_fp:.2f} | PnL: {pnl:+.1f}pts "
                             f"(${pnl * pos['qty'] * MES_POINT_VALUE:+.0f})")
                else:
                    _tg(cfg, f"[REAL-TRADE] CLOSE FAILED ({reason})\n"
                             f"Account: {self.account_id}\n"
                             f"MANUAL INTERVENTION NEEDED")
            else:
                _tg(cfg, f"[REAL-TRADE] CLOSE API ERROR ({reason})\n"
                         f"Account: {self.account_id}\n"
                         f"MANUAL INTERVENTION NEEDED")
        else:
            with self._lock:
                pos["status"] = "closed"
                pos["close_reason"] = reason
        self._save()

    def flatten_all(self, reason: str = "EOD"):
        """Flatten all open positions."""
        active = self.active_positions
        if not active:
            log.info(f"Flatten ({reason}): no active positions")
            return

        log.info(f"Flatten ({reason}): closing {len(active)} position(s)")
        _tg(self.cfg, f"[REAL-TRADE] FLATTEN ALL ({reason})\n"
                      f"Account: {self.account_id}\n"
                      f"Closing {len(active)} position(s)")

        # Phase 1: Cancel all orders across all trades
        cancelled = 0
        for tid, pos in active:
            for key in ("entry_order_id", "stop_order_id", "target_order_id"):
                oid = pos.get(key)
                if key == "entry_order_id" and pos.get("status") != "pending_limit":
                    continue
                if oid:
                    try:
                        _ts_api(self.auth, self.account_id, "DELETE",
                                f"/orderexecution/orders/{oid}")
                        cancelled += 1
                    except Exception:
                        pass
        log.info(f"Flatten: cancelled {cancelled} orders")

        time.sleep(2)

        # Phase 2: Close broker position
        broker_pos = self._get_broker_position()
        if broker_pos:
            close_side = "Sell" if broker_pos["long_short"] == "Long" else "Buy"
            for attempt in range(3):
                if attempt > 0:
                    time.sleep(3)
                    broker_pos = self._get_broker_position()
                    if not broker_pos:
                        log.info("Flatten: position closed during wait")
                        break
                    close_side = "Sell" if broker_pos["long_short"] == "Long" else "Buy"

                close_payload = {
                    "AccountID": self.account_id,
                    "Symbol": broker_pos["symbol"],
                    "Quantity": str(broker_pos["qty"]),
                    "OrderType": "Market",
                    "TradeAction": close_side,
                    "TimeInForce": {"Duration": "DAY"},
                    "Route": "Intelligent",
                }
                resp = _ts_api(self.auth, self.account_id, "POST",
                               "/orderexecution/orders", close_payload)
                if resp:
                    orders = resp.get("Orders", [])
                    if orders and orders[0].get("Error") == "FAILED":
                        log.warning(f"Flatten close rejected (attempt {attempt+1}): "
                                    f"{orders[0].get('Message', '')}")
                        continue
                    log.info(f"Flatten: closed {broker_pos['long_short']} "
                             f"{broker_pos['qty']} MES (attempt {attempt+1})")
                    break
            else:
                _tg(self.cfg, f"[REAL-TRADE] FLATTEN FAILED after 3 attempts!\n"
                              f"Account: {self.account_id}\n"
                              f"MANUAL CLOSE REQUIRED")
        else:
            log.info("Flatten: broker already flat")

        # Phase 3: Mark all positions as closed
        for tid, pos in active:
            with self._lock:
                if pos["status"] != "closed":
                    pos["status"] = "closed"
                    pos["close_reason"] = reason
        self._save()

        # Phase 4: Cancel any remaining open orders on account
        self._cancel_all_open_orders()

        # Phase 5: Verify flat
        time.sleep(1)
        final = self._get_broker_position()
        if final:
            _tg(self.cfg, f"[REAL-TRADE] STILL HAVE POSITION AFTER FLATTEN!\n"
                          f"Account: {self.account_id}\n"
                          f"{final['long_short']} {final['qty']} {final['symbol']}\n"
                          f"MANUAL INTERVENTION REQUIRED")
            log.error(f"STILL OPEN after flatten: {final}")
        else:
            log.info("Flatten complete (verified flat)")

    # ---- Safety Validation ----

    def _validate_order_safety(self, direction: str) -> bool:
        """Validate account + direction before EVERY order placement."""
        if self.account_id not in ACCOUNT_WHITELIST:
            log.error(f"SAFETY: account {self.account_id} NOT in whitelist! BLOCKING order.")
            return False
        expected_dir = ACCOUNT_DIRECTION_BINDING.get(self.account_id)
        if expected_dir != direction:
            log.error(f"SAFETY: account {self.account_id} bound to {expected_dir}, "
                      f"but trying to trade {direction}! BLOCKING order.")
            return False
        return True

    # ---- Broker Queries ----

    def _get_broker_position(self) -> dict | None:
        """Query broker for actual MES position."""
        try:
            pos_data = _ts_api(self.auth, self.account_id, "GET",
                               f"/brokerage/accounts/{self.account_id}/positions")
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
            log.error(f"Broker position query error: {e}")
        return None

    def _get_order_fill_price(self, order_id: str) -> float | None:
        try:
            data = _ts_api(self.auth, self.account_id, "GET",
                           f"/brokerage/accounts/{self.account_id}/orders")
            if data:
                for o in data.get("Orders", []):
                    if o.get("OrderID") == order_id and o.get("Status") == "FLL":
                        return _extract_fill_price(o)
        except Exception:
            pass
        return None

    def _cancel_all_open_orders(self):
        """Cancel ALL open orders on account (nuclear safety net)."""
        try:
            ord_data = _ts_api(self.auth, self.account_id, "GET",
                               f"/brokerage/accounts/{self.account_id}/orders")
            for o in (ord_data or {}).get("Orders", []):
                status = o.get("Status", "")
                if status in ("FLL", "CAN", "REJ", "EXP", "BRO", "OUT", "TSC"):
                    continue
                oid = o.get("OrderID")
                if oid:
                    _ts_api(self.auth, self.account_id, "DELETE",
                            f"/orderexecution/orders/{oid}")
                    log.info(f"Cancelled remaining order: {oid}")
        except Exception as e:
            log.error(f"Cancel-all error: {e}")


# ============================================================================
#  Daily State Tracker
# ============================================================================

class DailyState:
    """Tracks daily P&L and trade count for loss limit enforcement."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.daily_pnl = 0.0
        self.trades_today = 0
        self.last_reset_date = None
        self._load()

    def _load(self):
        if STATE_FILE.exists():
            try:
                s = json.loads(STATE_FILE.read_text())
                saved_date = s.get("last_reset_date", "")
                today = datetime.now(CT).strftime("%Y-%m-%d")
                if saved_date == today:
                    self.daily_pnl = s.get("daily_pnl", 0.0)
                    self.trades_today = s.get("trades_today", 0)
                    self.last_reset_date = today
                    log.info(f"Daily state loaded: PnL=${self.daily_pnl:+.0f}, "
                             f"trades={self.trades_today}")
                else:
                    self.last_reset_date = today
                    log.info(f"New trading day (was {saved_date})")
            except Exception:
                pass

    def save(self):
        STATE_FILE.write_text(json.dumps({
            "daily_pnl": self.daily_pnl,
            "trades_today": self.trades_today,
            "last_reset_date": self.last_reset_date or datetime.now(CT).strftime("%Y-%m-%d"),
        }, indent=2))

    def daily_reset(self):
        today = datetime.now(CT).strftime("%Y-%m-%d")
        if self.last_reset_date == today:
            return
        if self.last_reset_date and self.daily_pnl != 0:
            log.info(f"Previous day PnL: ${self.daily_pnl:+.0f}")
        self.daily_pnl = 0.0
        self.trades_today = 0
        self.last_reset_date = today
        self.save()
        log.info(f"Daily reset: {today}")

    def record_trade(self, pnl_pts: float, qty: int):
        """Record a completed trade."""
        pnl_dollars = pnl_pts * qty * MES_POINT_VALUE
        self.daily_pnl += pnl_dollars
        self.trades_today += 1
        self.save()
        log.info(f"Trade recorded: {pnl_pts:+.1f}pts x {qty} = ${pnl_dollars:+.0f} "
                 f"(daily total: ${self.daily_pnl:+.0f})")

    def can_trade(self) -> tuple[bool, str]:
        """Check if daily loss limit allows another trade."""
        limit = self.cfg.get("daily_loss_limit", 300)
        if self.daily_pnl <= -limit:
            return False, f"daily loss limit hit (${self.daily_pnl:+.0f} <= -${limit})"
        return True, "ok"


# ============================================================================
#  Startup Reconciliation
# ============================================================================

def _reconcile_on_startup(pm: PositionManager, daily: DailyState, cfg: dict):
    """Handle stale positions and verify broker state on startup."""
    # Flatten stale overnight positions
    stale_ids = pm.has_stale()
    if stale_ids:
        log.warning(f"Found {len(stale_ids)} stale position(s) -- flattening")
        _tg(cfg, f"[REAL-TRADE] STALE POSITIONS ON STARTUP\n"
                 f"Account: {cfg['account_id']}\n"
                 f"Flattening {len(stale_ids)} stale position(s)")
        for tid in stale_ids:
            pm.close_position(tid, reason="stale_overnight")
        # Also check broker for orphan positions
        broker_pos = pm._get_broker_position()
        if broker_pos:
            log.warning(f"Broker still has position after stale cleanup: "
                        f"{broker_pos['long_short']} {broker_pos['qty']}")
            pm.flatten_all("stale_cleanup")

    # Check for orphan broker positions (we're flat but broker isn't)
    if pm.active_count == 0:
        broker_pos = pm._get_broker_position()
        if broker_pos:
            log.warning(f"ORPHAN: broker has {broker_pos['long_short']} {broker_pos['qty']} "
                        f"but no tracked positions!")
            _tg(cfg, f"[REAL-TRADE] ORPHAN POSITION ON STARTUP\n"
                     f"Account: {cfg['account_id']}\n"
                     f"{broker_pos['long_short']} {broker_pos['qty']} {broker_pos['symbol']}\n"
                     f"Closing...")
            pm.flatten_all("orphan_cleanup")

    # Scan closed positions from today for daily P&L
    today_str = date.today().isoformat()
    for tid, pos in pm.positions.items():
        if pos.get("status") == "closed" and pos.get("ts", "")[:10] == today_str:
            pnl = pos.get("pnl_pts", 0)
            if pnl != 0 and pos.get("_counted") is not True:
                daily.record_trade(pnl, pos.get("qty", 1))
                pos["_counted"] = True
    pm._save()


# ============================================================================
#  Banner
# ============================================================================

def _banner(cfg: dict):
    acct = cfg["account_id"]
    direction = cfg["direction"]
    symbol = cfg["_resolved_symbol"]
    log.info("=" * 60)
    log.info("  REAL MONEY MES TRADER")
    log.info("  *** THIS IS LIVE TRADING ***")
    log.info("=" * 60)
    log.info(f"  Config:     {CONFIG_FILE.name}")
    log.info(f"  Account:    {acct}")
    log.info(f"  Direction:  {direction.upper()} ONLY")
    log.info(f"  Symbol:     {symbol}")
    log.info(f"  Qty/trade:  {cfg['qty_per_trade']} MES")
    log.info(f"  Max conc:   {cfg['max_concurrent']}")
    log.info(f"  Stop:       {cfg['stop_pts']}pts")
    log.info(f"  Target:     {cfg['target_pts']}pts")
    log.info(f"  Trail:      BE@{cfg['trail_be_trigger']}pts, "
             f"activation={cfg['trail_activation']}pts, gap={cfg['trail_gap']}pts")
    log.info(f"  Loss limit: ${cfg['daily_loss_limit']}")
    log.info(f"  Cutoff:     {cfg['no_new_trades_after_ct']} CT | "
             f"Flatten: {cfg['flatten_time_ct']} CT")
    log.info(f"  Enabled:    {cfg.get('enabled', False)}")
    log.info("=" * 60)


# ============================================================================
#  Main Loop
# ============================================================================

def main():
    cfg = load_config()

    # ---- CRITICAL VALIDATION ----
    account_id = cfg.get("account_id", "")
    direction = cfg.get("direction", "")

    if not account_id:
        log.error("account_id is empty. Set it in the config file.")
        save_config(cfg)
        sys.exit(1)

    if account_id not in ACCOUNT_WHITELIST:
        log.error(f"SAFETY: account_id '{account_id}' is NOT in the whitelist "
                  f"{ACCOUNT_WHITELIST}. Refusing to start.")
        sys.exit(1)

    expected_dir = ACCOUNT_DIRECTION_BINDING.get(account_id)
    if direction != expected_dir:
        log.error(f"SAFETY: account {account_id} is bound to '{expected_dir}' direction, "
                  f"but config says '{direction}'. Refusing to start.")
        log.error(f"Fix 'direction' in {CONFIG_FILE} to match the account binding.")
        sys.exit(1)

    if direction not in ("long", "short"):
        log.error(f"direction must be 'long' or 'short', got '{direction}'")
        sys.exit(1)

    # Validate required fields
    required = {
        "railway_api_url": cfg.get("railway_api_url", ""),
        "eval_api_key": cfg.get("eval_api_key", ""),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        log.error(f"Missing required config: {missing}")
        save_config(cfg)
        sys.exit(1)

    # TS API credentials: config > env vars
    ts_client_id = cfg.get("ts_client_id") or os.environ.get("TS_CLIENT_ID", "")
    ts_client_secret = cfg.get("ts_client_secret") or os.environ.get("TS_CLIENT_SECRET", "")
    ts_refresh_token = cfg.get("ts_refresh_token") or os.environ.get("TS_REFRESH_TOKEN", "")
    if not all([ts_client_id, ts_client_secret, ts_refresh_token]):
        log.error("TS API credentials missing. Set ts_client_id, ts_client_secret, "
                  "ts_refresh_token in config or env vars.")
        sys.exit(1)

    # Resolve MES symbol
    mes_raw = cfg.get("mes_symbol", "auto")
    mes_symbol = _auto_mes_symbol() if mes_raw.lower() == "auto" else mes_raw
    cfg["_resolved_symbol"] = mes_symbol

    _banner(cfg)

    if not cfg.get("enabled", False):
        log.error("Master switch is OFF. Set 'enabled': true in config to start trading.")
        log.error("This is a safety feature. You must explicitly enable real trading.")
        sys.exit(1)

    # Telegram startup notification
    _tg(cfg, f"[REAL-TRADE] STARTING\n"
             f"Account: {account_id}\n"
             f"Direction: {direction.upper()}\n"
             f"Symbol: {mes_symbol}\n"
             f"Qty: {cfg['qty_per_trade']} MES | Max: {cfg['max_concurrent']} concurrent")

    # Initialize components
    auth = TSAuth(ts_client_id, ts_client_secret, ts_refresh_token)

    # Verify token works
    token = auth.get_token()
    if not token:
        log.error("Cannot obtain TS API token. Check credentials.")
        _tg(cfg, f"[REAL-TRADE] FAILED TO START\n"
                 f"Account: {account_id}\nCannot obtain TS API token")
        sys.exit(1)

    # Verify account access
    acct_info = _ts_api(auth, account_id, "GET",
                        f"/brokerage/accounts/{account_id}")
    if acct_info:
        log.info(f"Account verified: {json.dumps(acct_info, default=str)[:300]}")
    else:
        log.error(f"Cannot access account {account_id}. Check permissions.")
        _tg(cfg, f"[REAL-TRADE] FAILED TO START\n"
                 f"Cannot access account {account_id}")
        sys.exit(1)

    api_poller = APIPoller(cfg["railway_api_url"], cfg["eval_api_key"])
    pm = PositionManager(cfg, auth)
    daily = DailyState(cfg)

    # Startup reconciliation
    _reconcile_on_startup(pm, daily, cfg)

    poll_interval = cfg.get("poll_interval_s", 2)
    last_trail_check = 0.0
    TRAIL_CHECK_INTERVAL = 5.0
    latest_es_price: float | None = None
    _trade_dedup: dict[str, float] = {}  # "setup_dir" -> timestamp

    log.info(f"Polling every {poll_interval}s -- waiting for Skew Charm signals...")

    try:
        while True:
            now_ct = datetime.now(CT)

            # Daily reset
            daily.daily_reset()

            # ---- EOD Flatten ----
            flatten_time = datetime.strptime(cfg["flatten_time_ct"], "%H:%M").time()
            if now_ct.time() >= flatten_time and pm.active_count > 0:
                pm.flatten_all("EOD")

            # ---- Trail + fill polling (every 5s) ----
            if pm.active_count > 0 and time.time() - last_trail_check >= TRAIL_CHECK_INTERVAL:
                pm.poll_orders(latest_es_price)
                pm.check_trails(latest_es_price)
                last_trail_check = time.time()

                # Record closed positions' PnL
                _record_closed_pnl(pm, daily)

            # ---- Poll for signals ----
            new_signals, poll_es = api_poller.poll()
            if poll_es:
                latest_es_price = poll_es

            for signal in new_signals:
                setup_name = signal.get("setup_name", "")
                sig_direction = signal.get("direction", "").lower()

                # Only Skew Charm
                if setup_name != "Skew Charm":
                    continue

                # Only matching direction
                if direction == "long":
                    dir_match = sig_direction in ("long", "bullish")
                else:
                    dir_match = sig_direction in ("short", "bearish")
                if not dir_match:
                    log.debug(f"Skipping SC {sig_direction} (we only trade {direction})")
                    continue

                log.info(f"Signal: SC {sig_direction.upper()} [{signal.get('grade', '?')}] "
                         f"@ SPX {signal['spot']:.2f}")

                # Staleness check
                sig_ts = signal.get("signal_ts")
                if sig_ts:
                    try:
                        sig_dt = datetime.fromisoformat(sig_ts)
                        if sig_dt.tzinfo is None:
                            sig_dt = sig_dt.replace(tzinfo=ET)
                        age_s = (datetime.now(ET) - sig_dt).total_seconds()
                        if age_s > cfg.get("max_signal_age_s", MAX_SIGNAL_AGE_S):
                            log.info(f"  SKIPPED: signal too old ({age_s:.0f}s)")
                            continue
                    except Exception:
                        pass

                # Dedup check
                dedup_key = f"SC_{sig_direction}"
                now_ts = time.time()
                if dedup_key in _trade_dedup and (now_ts - _trade_dedup[dedup_key]) < TRADE_DEDUP_WINDOW:
                    log.info(f"  DEDUP: SC {sig_direction} already traded "
                             f"{now_ts - _trade_dedup[dedup_key]:.0f}s ago")
                    continue

                # Master switch
                if not cfg.get("enabled", False):
                    log.info("  BLOCKED: master switch OFF")
                    continue

                # Market hours
                market_open = datetime.strptime(cfg["market_open_ct"], "%H:%M").time()
                if now_ct.time() < market_open:
                    log.info(f"  BLOCKED: before market open ({cfg['market_open_ct']} CT)")
                    continue

                cutoff = datetime.strptime(cfg["no_new_trades_after_ct"], "%H:%M").time()
                if now_ct.time() >= cutoff:
                    log.info(f"  BLOCKED: past cutoff ({cfg['no_new_trades_after_ct']} CT)")
                    continue

                # Daily loss limit
                can, reason = daily.can_trade()
                if not can:
                    log.info(f"  BLOCKED: {reason}")
                    continue

                # Concurrent cap
                if pm.active_count >= cfg.get("max_concurrent", 2):
                    log.info(f"  BLOCKED: max concurrent ({pm.active_count}/{cfg['max_concurrent']})")
                    continue

                # ---- PLACE TRADE ----
                trade_id = pm.new_trade_id()
                charm_limit = signal.get("charm_limit_entry")
                use_limit = charm_limit is not None and direction == "short"

                if use_limit:
                    ok = pm.place_limit_entry(trade_id, signal)
                else:
                    ok = pm.place_market_bracket(trade_id, signal)

                if ok:
                    _trade_dedup[dedup_key] = now_ts
                    daily.trades_today += 1
                    daily.save()

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        log.info("")
        log.info("Shutting down...")
        active = pm.active_positions
        if active:
            log.warning(f"POSITIONS STILL OPEN ({len(active)}):")
            for tid, pos in active:
                log.warning(f"  {pos['direction']} @ {pos.get('fill_price') or pos.get('entry_ref_price', '?')}")
            log.warning("Manage manually in TradeStation!")
        _tg(cfg, f"[REAL-TRADE] SHUTDOWN\n"
                 f"Account: {cfg['account_id']}\n"
                 f"Active positions: {len(active)}\n"
                 f"Daily PnL: ${daily.daily_pnl:+.0f}")
        daily.save()
        log.info("State saved. Goodbye.")


def _record_closed_pnl(pm: PositionManager, daily: DailyState):
    """Check for newly closed positions and record their PnL."""
    for tid, pos in list(pm.positions.items()):
        if pos.get("status") == "closed" and pos.get("pnl_pts") is not None and not pos.get("_pnl_recorded"):
            daily.record_trade(pos["pnl_pts"], pos.get("qty", 1))
            pos["_pnl_recorded"] = True
    pm._save()


# ============================================================================
#  Entry Point
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MES Real Money Trader")
    parser.add_argument("--config", required=True,
                        help="Config file (e.g. real_trader_config_longs.json)")
    args = parser.parse_args()

    _init_file_paths(args.config)
    _acquire_singleton_lock()

    # Add file handler
    log.addHandler(logging.FileHandler(LOG_FILE, encoding="utf-8"))

    main()
