"""
eval_trader.py — E2T Evaluation Auto-Trader for NinjaTrader 8

Standalone local script that:
  1. Polls Railway API (or Telegram as fallback) for setup signals from 0DTE Alpha
  2. Enforces E2T 50K TCP compliance rules
  3. Places orders through NinjaTrader 8 OIF (Order Instruction Files)
  4. Tracks position state and daily P&L for compliance

Requirements: Python 3.10+, requests
Usage: python eval_trader.py
Config: eval_trader_config.json (created on first run — fill in required fields)

Architecture:
  Railway (setup fires) → /api/eval/signals → this script → OIF file → NT8 → Rithmic → E2T
  (Legacy: Railway → Telegram → this script, when signal_source="telegram")

Stop/target orders execute at exchange level via NT8. Even if this script
crashes, your stops and targets remain live. The script's job is signal
reception, compliance gating, order placement, and P&L tracking.
"""

import os, sys, json, re, time, logging, calendar, argparse, atexit
from datetime import datetime, timedelta, time as dtime, date
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests

# ─── Timezone ─────────────────────────────────────────────────────────────────
CT = ZoneInfo("US/Central")   # E2T / CME use Central Time
ET = ZoneInfo("US/Eastern")

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("eval_trader")


def _init_log_file():
    """Add file handler after LOG_FILE is resolved (deferred for --config support)."""
    log.addHandler(logging.FileHandler(LOG_FILE, encoding="utf-8"))

# ─── Constants ────────────────────────────────────────────────────────────────
MES_POINT_VALUE = 5.0   # $5 per point per MES contract
MES_TICK_SIZE = 0.25     # MES minimum price increment
MAX_SIGNAL_AGE_S = 120   # Skip signals older than 2 min (prevents stale entries after restart)
TRADE_DEDUP_WINDOW = 120  # Block duplicate setup+direction within 2 min (deploy overlap guard)
_trade_dedup: dict[tuple[str, str], float] = {}  # (setup_name, direction) → timestamp
_TIGHTEN_GAP_PTS = 5.0   # Low-conviction opposing signal: tighten SL to this many pts from spot
_ENV_OVERRIDE_THRESHOLD = 2  # Conviction score needed to close/reverse against current position
TICK_TRADE_TIME_ET = dtime(15, 30)  # Place tick trade at 15:30 ET if no trades today
TICK_TRADE_TICKS = 2     # 2 ticks (0.50 pts) TP and SL — avoids race condition rejections
SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "eval_trader_config.json"
STATE_FILE = SCRIPT_DIR / "eval_trader_state.json"
POSITION_FILE = SCRIPT_DIR / "eval_trader_position.json"
API_STATE_FILE = SCRIPT_DIR / "eval_trader_api_state.json"
LOG_FILE = "eval_trader.log"
LOCK_FILE = SCRIPT_DIR / "eval_trader.lock"


def _acquire_singleton_lock():
    """Ensure only one instance runs per config suffix. Kills the process if another is alive."""
    global LOCK_FILE
    suffix = CONFIG_FILE.stem.replace("eval_trader_config", "")
    LOCK_FILE = SCRIPT_DIR / f"eval_trader{suffix}.lock"

    if LOCK_FILE.exists():
        try:
            old_pid = int(LOCK_FILE.read_text().strip())
            # Check if the old process is still alive
            if sys.platform == "win32":
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x1000, False, old_pid)  # PROCESS_QUERY_LIMITED_INFORMATION
                if handle:
                    kernel32.CloseHandle(handle)
                    print(f"\n{'='*60}")
                    print(f"  BLOCKED: Another eval_trader instance is already running!")
                    print(f"  PID: {old_pid}  |  Lock: {LOCK_FILE.name}")
                    print(f"  Kill it first, or stop it in PyCharm, then retry.")
                    print(f"{'='*60}\n")
                    sys.exit(1)
            else:
                os.kill(old_pid, 0)  # signal 0 = check if alive (Unix)
                print(f"\n{'='*60}")
                print(f"  BLOCKED: Another eval_trader instance is already running!")
                print(f"  PID: {old_pid}  |  Lock: {LOCK_FILE.name}")
                print(f"  Kill it first, then retry.")
                print(f"{'='*60}\n")
                sys.exit(1)
        except (OSError, ValueError):
            pass  # Old process is dead — stale lock file, safe to take over

    # Write our PID
    LOCK_FILE.write_text(str(os.getpid()))
    atexit.register(_release_singleton_lock)


def _release_singleton_lock():
    """Remove lock file on clean exit."""
    try:
        if LOCK_FILE.exists() and LOCK_FILE.read_text().strip() == str(os.getpid()):
            LOCK_FILE.unlink()
    except Exception:
        pass


def _init_file_paths(config_path: str):
    """Derive all state/position/api_state/log file paths from the config filename.

    eval_trader_config.json       → suffix ""      (backward compatible)
    eval_trader_config_real.json  → suffix "_real"
    """
    global CONFIG_FILE, STATE_FILE, POSITION_FILE, API_STATE_FILE, LOG_FILE
    config_name = Path(config_path).stem  # e.g. "eval_trader_config_real"
    # Extract suffix: strip "eval_trader_config" prefix
    prefix = "eval_trader_config"
    suffix = config_name[len(prefix):] if config_name.startswith(prefix) else ""
    config_dir = Path(config_path).parent if Path(config_path).is_absolute() else SCRIPT_DIR

    CONFIG_FILE = config_dir / f"eval_trader_config{suffix}.json"
    STATE_FILE = config_dir / f"eval_trader_state{suffix}.json"
    POSITION_FILE = config_dir / f"eval_trader_position{suffix}.json"
    API_STATE_FILE = config_dir / f"eval_trader_api_state{suffix}.json"
    LOG_FILE = f"eval_trader{suffix}.log"

# ─── MES Contract Auto-Rollover ───────────────────────────────────────────────
# MES quarterly cycle: H(Mar), M(Jun), U(Sep), Z(Dec)
# Rollover = 2nd Thursday before 3rd Friday of expiry month (≈8 days before expiry)

_MES_MONTHS = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]


def _third_friday(year: int, month: int) -> date:
    """3rd Friday of a given month."""
    c = calendar.Calendar(firstweekday=calendar.MONDAY)
    fridays = [d for d in c.itermonthdates(year, month) if d.month == month and d.weekday() == 4]
    return fridays[2]


def current_mes_symbol(fmt: str = "nt8") -> str:
    """Return front-month MES symbol, rolling to next contract the day after expiry.

    fmt="nt8"  → "MES 03-26"
    fmt="ts"   → "MESH26"
    """
    today = date.today()
    for month_num, code in _MES_MONTHS:
        year = today.year
        expiry = _third_friday(year, month_num)
        rollover = expiry - timedelta(days=8)  # 2nd Thursday before expiry (CME convention)
        if today <= rollover:
            if fmt == "ts":
                return f"MES{code}{year % 100}"
            return f"MES {month_num:02d}-{year % 100}"
    # Past December rollover → next year March
    year = today.year + 1
    if fmt == "ts":
        return f"MESH{year % 100}"
    return f"MES 03-{year % 100}"


# ─── Default Configuration ────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    # ── Signal source: "api" (Railway endpoint) or "telegram" (legacy) ──
    "signal_source": "api",
    "railway_api_url": "",         # e.g. "https://0dtealpha-production.up.railway.app"
    "eval_api_key": "",            # Must match EVAL_API_KEY env var on Railway

    # ── Telegram (legacy fallback — used when signal_source="telegram") ──
    "telegram_bot_token": "",
    "telegram_chat_id": "",        # TELEGRAM_CHAT_ID_SETUPS value
    "telegram_poll_interval_s": 2,

    # ── NinjaTrader 8 ──
    "nt8_incoming_folder": "",     # e.g. "C:\\Users\\Faisa\\Documents\\NinjaTrader 8\\incoming"
    "nt8_account_id": "",          # Rithmic eval account (shown in NT8 Accounts tab)
    "nt8_mes_symbol": "auto",      # "auto" = quarterly rollover, or manual e.g. "MES 03-26"

    # ── Position sizing ──
    "qty": 10,
    "max_trade_risk": 300,       # $300 max risk per trade (dynamic sizing)
    "dynamic_sizing": True,      # True = calc qty from max_trade_risk / (stop × $5)

    # ── Survival mode ──
    "be_trigger_pts": 5.0,       # Move stop to breakeven when ES moves +5 pts
    "max_stop_loss_pts": 12,     # Cap ALL stops at 12 pts max (survival mode)

    # ── E2T 50K TCP Rules ──
    "e2t_daily_loss_limit": 1100,       # $1,100 hard limit
    "e2t_daily_loss_buffer": 100,       # stop trading at -$1,000 (safety margin)
    "e2t_eod_trailing_drawdown": 2000,  # $2,000 trailing from peak EOD balance
    "e2t_max_contracts_es_equiv": 6,    # 6 ES = 60 MES
    "e2t_starting_balance": 50000,
    "e2t_peak_balance": 50000,          # updated automatically at EOD

    # ── Time rules (Central Time) ──
    "market_open_ct": "08:30",          # CME futures open
    "no_new_trades_after_ct": "15:30",  # no new entries after this
    "flatten_time_ct": "15:50",         # flatten all positions

    # ── Daily P&L cap (E2T consistency rule: no single day > 30% of target) ──
    "e2t_daily_pnl_cap": 900,      # $900 = 30% of $3,000 target; stop taking new trades

    # ── Per-setup stop/target (points) ──
    # target = fixed take-profit distance; null/"msg" = use Volland target from Telegram
    # Mirrors production system: same stops, same targets, smaller size
    "setup_rules": {
        "GEX Long":          {"enabled": True,  "stop": 8,  "target": None},
        "GEX Velocity":      {"enabled": True,  "stop": 8,  "target": None},
        "AG Short":          {"enabled": True,  "stop": 12, "target": None},
        "BofA Scalp":        {"enabled": False, "stop": 12, "target": "msg", "max_hold_min": 30},
        "ES Absorption":    {"enabled": True,  "stop": 8,  "target": 10},
        "Paradigm Reversal": {"enabled": True,  "stop": 12, "target": 10},
        "DD Exhaustion":     {"enabled": True,  "stop": 12, "target": None},
        "Skew Charm":        {"enabled": False, "stop": 12, "target": None},
    },

    # ── Master switch ──
    "enabled": True,
}


# ═════════════════════════════════════════════════════════════════════════════
#  CONFIG MANAGEMENT
# ═════════════════════════════════════════════════════════════════════════════

def load_config() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            saved = json.load(f)
        # Merge defaults (adds new keys from DEFAULT_CONFIG)
        merged = {**DEFAULT_CONFIG, **saved}
        # Deep-merge setup_rules
        default_rules = DEFAULT_CONFIG["setup_rules"]
        saved_rules = saved.get("setup_rules", {})
        merged["setup_rules"] = {k: {**default_rules.get(k, {}), **saved_rules.get(k, {})}
                                  for k in set(list(default_rules) + list(saved_rules))}
        return merged
    else:
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()


def save_config(cfg: dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)


# ═════════════════════════════════════════════════════════════════════════════
#  TELEGRAM POLLING
# ═════════════════════════════════════════════════════════════════════════════

class TelegramPoller:
    """Polls Telegram Bot API for new setup messages."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = str(chat_id)
        self.base = f"https://api.telegram.org/bot{token}"
        self.offset = 0
        self._seen = set()

    def poll(self) -> list[dict]:
        """Returns list of new messages as {message_id, text} dicts."""
        try:
            resp = requests.get(
                f"{self.base}/getUpdates",
                params={"offset": self.offset, "timeout": 3, "allowed_updates": '["message"]'},
                timeout=8,
            )
            data = resp.json()
            if not data.get("ok"):
                return []

            messages = []
            for upd in data.get("result", []):
                self.offset = upd["update_id"] + 1
                msg = upd.get("message") or upd.get("channel_post") or {}
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != self.chat_id:
                    continue
                mid = msg.get("message_id")
                if mid in self._seen:
                    continue
                self._seen.add(mid)
                # Telegram sends HTML in 'text' field
                text = msg.get("text", "")
                messages.append({"message_id": mid, "text": text})
            return messages
        except Exception as e:
            log.error(f"Telegram poll error: {e}")
            return []


# ═════════════════════════════════════════════════════════════════════════════
#  RAILWAY API POLLING
# ═════════════════════════════════════════════════════════════════════════════

class APIPoller:
    """Polls Railway /api/eval/signals for setup signals and outcomes."""

    def __init__(self, api_url: str, api_key: str):
        self.url = api_url.rstrip("/") + "/api/eval/signals"
        self.api_key = api_key
        self.last_id = 0
        self._seen_signals: set[int] = set()   # track signal IDs already emitted
        self._seen_outcomes: set[int] = set()   # track outcome IDs already processed
        self._state_date: str = ""  # date string for daily reset
        self._load_state()

    def _state_file(self) -> Path:
        return API_STATE_FILE

    def _load_state(self):
        global _trade_dedup
        sf = self._state_file()
        if sf.exists():
            try:
                data = json.loads(sf.read_text())
                saved_date = data.get("date", "")
                today = date.today().isoformat()
                if saved_date == today:
                    self.last_id = data.get("last_id", 0)
                    self._seen_signals = set(data.get("seen_signals", []))
                    self._seen_outcomes = set(data.get("seen_outcomes", []))
                    self._state_date = today
                    # Restore trade dedup dict (survives restart)
                    saved_dedup = data.get("trade_dedup", {})
                    now = time.time()
                    for key_str, ts in saved_dedup.items():
                        if (now - ts) < TRADE_DEDUP_WINDOW:
                            parts = key_str.split("|", 1)
                            if len(parts) == 2:
                                _trade_dedup[(parts[0], parts[1])] = ts
                    log.info(f"API poller state restored: last_id={self.last_id}")
                else:
                    log.info(f"API poller: new day (was {saved_date}), resetting state")
                    self._state_date = today
            except Exception:
                pass

    def _save_state(self):
        # Serialize _trade_dedup: tuple keys → "setup|direction" string keys
        dedup_serialized = {}
        now = time.time()
        for (setup, direction), ts in _trade_dedup.items():
            if (now - ts) < TRADE_DEDUP_WINDOW:  # Only save entries still within window
                dedup_serialized[f"{setup}|{direction}"] = ts
        self._state_file().write_text(json.dumps({
            "date": date.today().isoformat(),
            "last_id": self.last_id,
            "seen_signals": list(self._seen_signals),
            "seen_outcomes": list(self._seen_outcomes),
            "trade_dedup": dedup_serialized,
        }))

    def poll(self) -> tuple[list[dict], list[dict], float | None]:
        """Poll the API. Returns (new_signals, new_outcomes, es_price).

        Each signal dict matches the format expected by open_trade():
          {setup_name, direction, spot, grade, msg_target_pts, msg_stop_pts}
        Each outcome dict matches close_on_outcome():
          {setup_name, result, pnl_pts}
        es_price: current ES/MES price from Railway quote stream (for trailing stop)
        """
        try:
            resp = requests.get(
                self.url,
                params={"since_id": self.last_id},
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=8,
            )
            if resp.status_code == 401:
                log.error("API auth failed — check eval_api_key in config")
                return [], [], None
            if resp.status_code != 200:
                log.warning(f"API poll error: HTTP {resp.status_code}")
                return [], [], None

            data = resp.json()
        except requests.ConnectionError:
            log.debug("API unreachable — will retry")
            return [], [], None
        except Exception as e:
            log.error(f"API poll error: {e}")
            return [], [], None

        raw_signals = data.get("signals", [])
        raw_outcomes = data.get("outcomes", [])
        es_price = data.get("es_price")  # current MES/ES price from Railway
        if es_price is not None:
            es_price = float(es_price)

        # Update last_id to highest seen
        if raw_signals:
            max_id = max(s["id"] for s in raw_signals)
            if max_id > self.last_id:
                self.last_id = max_id
                self._save_state()

        # Convert API signals to the format open_trade() expects
        # Use _seen_signals to track which signal IDs we've already processed.
        # On first sight, always emit the signal even if outcome_result is set
        # (e.g. DD fired and got REVERSED by AG in the same Railway cycle —
        # the eval trader should still see the DD signal and let compliance decide).
        new_signals = []
        for s in raw_signals:
            sid = s["id"]
            if sid in self._seen_signals:
                continue
            self._seen_signals.add(sid)
            sig = self._api_to_signal(s)
            if sig:
                sig["es_price"] = es_price
                new_signals.append(sig)
        if new_signals:
            self._save_state()

        # Convert outcomes (only unseen ones)
        new_outcomes = []
        for o in raw_outcomes:
            oid = o["id"]
            if oid in self._seen_outcomes:
                continue
            self._seen_outcomes.add(oid)
            new_outcomes.append({
                "setup_name": o["setup_name"],
                "result": o["outcome_result"],
                "pnl_pts": o.get("outcome_pnl", 0) or 0,
            })
        if new_outcomes:
            self._save_state()

        return new_signals, new_outcomes, es_price

    def _api_to_signal(self, s: dict) -> dict | None:
        """Convert API signal entry to the dict format expected by open_trade()."""
        setup = s.get("setup_name")
        direction = s.get("direction", "long")
        is_long = direction.lower() in ("long", "bullish")

        # ES Absorption uses ES price as spot
        if setup == "ES Absorption":
            spot = s.get("abs_es_price") or s.get("spot")
        else:
            spot = s.get("spot")

        if not spot:
            return None

        # Compute msg_target_pts from target_level / stop_level
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
            "paradigm": s.get("paradigm"),
            "greek_alignment": s.get("greek_alignment"),
            "spot_vol_beta": s.get("spot_vol_beta"),
            "vanna_all": s.get("vanna_all"),
            "charm_limit_entry": s.get("charm_limit_entry"),
        }


# ═════════════════════════════════════════════════════════════════════════════
#  TRADESTATION QUOTE POLLER (for breakeven stop)
# ═════════════════════════════════════════════════════════════════════════════

class TSQuotePoller:
    """Polls TradeStation API for ES quotes (used for breakeven stop logic)."""

    def __init__(self):
        self.client_id = os.environ.get("TS_CLIENT_ID", "")
        self.client_secret = os.environ.get("TS_CLIENT_SECRET", "")
        self.refresh_token = os.environ.get("TS_REFRESH_TOKEN", "")
        self.access_token = None
        self.token_expiry = 0
        self.available = bool(self.client_id and self.client_secret and self.refresh_token)
        if not self.available:
            log.info("TS API credentials not set — breakeven stop disabled")

    def _refresh_access_token(self) -> bool:
        """OAuth2 refresh_token → access_token."""
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
                # Refresh 1 min before expiry
                self.token_expiry = time.time() + data.get("expires_in", 1200) - 60
                log.info("TS API token refreshed")
                return True
            else:
                log.warning(f"TS token refresh failed: {resp.status_code}")
                return False
        except Exception as e:
            log.warning(f"TS token refresh error: {e}")
            return False

    def get_es_price(self) -> float | None:
        """Get current ES price. Returns None on failure."""
        if not self.available:
            return None
        if time.time() >= self.token_expiry:
            if not self._refresh_access_token():
                return None
        try:
            resp = requests.get(
                "https://api.tradestation.com/v3/marketdata/quotes/@ES",
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=5,
            )
            if resp.status_code == 200:
                quotes = resp.json().get("Quotes", [])
                if quotes:
                    return float(quotes[0].get("Last", 0))
            return None
        except Exception as e:
            log.debug(f"ES quote error: {e}")
            return None


# ═════════════════════════════════════════════════════════════════════════════
#  SIGNAL PARSER
# ═════════════════════════════════════════════════════════════════════════════

def _xf(text: str, pattern: str) -> float | None:
    """Extract first float match."""
    m = re.search(pattern, text)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    return None


def _xs(text: str, pattern: str) -> str | None:
    """Extract first string match."""
    m = re.search(pattern, text)
    return m.group(1).strip() if m else None


def parse_signal(text: str) -> dict | None:
    """Parse Telegram setup message → signal dict or None.

    Returns: {setup_name, direction, spot, grade, msg_target, msg_stop} or None.
    msg_target/msg_stop are the actual values from the Telegram message (if present).
    """
    if not text:
        return None

    # ── GEX Long ──
    if "GEX Long Setup" in text:
        spot = _xf(text, r"SPX:\s*([\d,.]+)")
        grade = _xs(text, r"GEX Long Setup\s*[—–-]\s*(\S+)")
        target_lvl = _xf(text, r"Target:\s*([\d,.]+)")
        if spot:
            target_pts = round(target_lvl - spot, 1) if target_lvl and target_lvl > spot else None
            return {"setup_name": "GEX Long", "direction": "long",
                    "spot": spot, "grade": grade or "?",
                    "msg_target_pts": target_pts}

    # ── AG Short ──
    if "AG Short Setup" in text:
        spot = _xf(text, r"SPX:\s*([\d,.]+)")
        grade = _xs(text, r"AG Short Setup\s*[—–-]\s*(\S+)")
        target_lvl = _xf(text, r"Target:\s*([\d,.]+)")
        if spot:
            target_pts = round(spot - target_lvl, 1) if target_lvl and target_lvl < spot else None
            return {"setup_name": "AG Short", "direction": "short",
                    "spot": spot, "grade": grade or "?",
                    "msg_target_pts": target_pts}

    # ── BofA Scalp ──
    if "BofA Scalp" in text:
        direction = "long" if "LONG" in text else "short"
        spot = _xf(text, r"Spot:\s*([\d,.]+)")
        grade = _xs(text, r"(?:Grade:.*?)(A\+|A-Entry|A)\b")
        target_lvl = _xf(text, r"Target:\s*([\d,.]+)")
        stop_lvl = _xf(text, r"Stop:\s*([\d,.]+)")
        if spot:
            if direction == "long":
                msg_target = round(target_lvl - spot, 1) if target_lvl else None
                msg_stop = round(spot - stop_lvl, 1) if stop_lvl else None
            else:
                msg_target = round(spot - target_lvl, 1) if target_lvl else None
                msg_stop = round(stop_lvl - spot, 1) if stop_lvl else None
            return {"setup_name": "BofA Scalp", "direction": direction,
                    "spot": spot, "grade": grade or "?",
                    "msg_target_pts": msg_target, "msg_stop_pts": msg_stop}

    # ── ES Absorption ──
    if "ES ABSORPTION" in text:
        direction = "long" if "BUY" in text else "short"
        # ES Absorption uses ES price, not SPX
        price = _xf(text, r"Price:\s*([\d,.]+)")
        grade = _xs(text, r"\[(\S+?)\]")
        if price:
            return {"setup_name": "ES Absorption", "direction": direction,
                    "spot": price, "grade": grade or "?"}

    # ── DD Exhaustion ──
    if "DD EXHAUSTION" in text:
        direction = "long" if "LONG" in text else "short"
        spot = _xf(text, r"Entry:\s*\$?([\d,.]+)")
        grade = _xs(text, r"DD EXHAUSTION.*?\((\S+)\s*/")
        if spot:
            return {"setup_name": "DD Exhaustion", "direction": direction,
                    "spot": spot, "grade": grade or "?"}

    # ── Paradigm Reversal ──
    if "Paradigm Reversal" in text:
        direction = "long" if "LONG" in text else "short"
        spot = _xf(text, r"Spot:\s*([\d,.]+)")
        grade = _xs(text, r"(?:Grade:.*?)(A\+|A-Entry|A)\b")
        if spot:
            return {"setup_name": "Paradigm Reversal", "direction": direction,
                    "spot": spot, "grade": grade or "?"}

    return None


def parse_outcome(text: str) -> dict | None:
    """Parse Telegram outcome message → {setup_name, result, pnl_pts} or None."""
    if not text:
        return None
    # Outcome messages contain WIN/LOSS/EXPIRED with points
    for result_type in ("WIN", "LOSS", "EXPIRED"):
        if result_type not in text:
            continue
        # Try to extract setup name and P&L
        setup = _xs(text, r"(GEX Long|AG Short|BofA Scalp|ES Absorption|Paradigm Reversal|DD Exhaustion)")
        pnl = _xf(text, r"([+-]?[\d.]+)\s*pts")
        if setup and pnl is not None:
            return {"setup_name": setup, "result": result_type, "pnl_pts": pnl}
    return None


# ═════════════════════════════════════════════════════════════════════════════
#  E2T COMPLIANCE GATE
# ═════════════════════════════════════════════════════════════════════════════

class ComplianceGate:
    """Enforces E2T 50K TCP rules. Rejects any trade that could violate eval rules."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.daily_pnl = 0.0
        self.total_pnl = 0.0
        self.trades_today = 0
        self.losses_today = 0
        self.daily_commissions = 0.0
        self.trade_days = set()
        self.has_open_position = False
        self.last_reset_date = None
        self._first_spot_today = None  # Track first spot for momentum filter
        self.tick_trade_done = False    # E2T day-count tick trade placed today
        self._load()

    def _load(self):
        if STATE_FILE.exists():
            try:
                s = json.loads(STATE_FILE.read_text())
                self.daily_pnl = s.get("daily_pnl", 0.0)
                self.total_pnl = s.get("total_pnl", 0.0)
                self.trades_today = s.get("trades_today", 0)
                self.losses_today = s.get("losses_today", 0)
                self.daily_commissions = s.get("daily_commissions", 0.0)
                self.trade_days = set(s.get("trade_days", []))
                self.last_reset_date = s.get("last_reset_date")
                self.cfg["e2t_peak_balance"] = s.get("peak_balance", self.cfg["e2t_peak_balance"])
                comm_rate = self.cfg.get("commission_per_contract", 0)
                log.info(f"State loaded: daily=${self.daily_pnl:+.0f} (comm=${self.daily_commissions:.0f}) "
                         f"total=${self.total_pnl:+.0f} "
                         f"peak=${self.cfg['e2t_peak_balance']:,.0f} losses={self.losses_today} "
                         f"days={len(self.trade_days)} comm/ct=${comm_rate:.2f}")
            except Exception as e:
                log.warning(f"State load error: {e}")

    def save(self):
        s = {
            "daily_pnl": self.daily_pnl,
            "total_pnl": self.total_pnl,
            "trades_today": self.trades_today,
            "losses_today": self.losses_today,
            "daily_commissions": self.daily_commissions,
            "trade_days": list(self.trade_days),
            "last_reset_date": self.last_reset_date,
            "peak_balance": self.cfg["e2t_peak_balance"],
        }
        STATE_FILE.write_text(json.dumps(s, indent=2))

    def daily_reset(self):
        """Reset daily counters when the date changes."""
        today = datetime.now(CT).strftime("%Y-%m-%d")
        if self.last_reset_date == today:
            return
        if self.last_reset_date is not None:
            # End of previous day: update peak balance
            current_bal = self.cfg["e2t_starting_balance"] + self.total_pnl
            if current_bal > self.cfg["e2t_peak_balance"]:
                self.cfg["e2t_peak_balance"] = current_bal
                log.info(f"Peak balance updated: ${current_bal:,.0f}")
            if self.daily_pnl != 0:
                log.info(f"Previous day P&L: ${self.daily_pnl:+.0f}")
        self.daily_pnl = 0.0
        self.trades_today = 0
        self.losses_today = 0
        self.daily_commissions = 0.0
        self._first_spot_today = None
        self.tick_trade_done = False
        self.last_reset_date = today
        self.save()
        log.info(f"Daily reset: {today}")

    def check(self, signal: dict) -> tuple[bool, str]:
        """Check all E2T rules. Returns (allowed, reason)."""
        cfg = self.cfg
        now = datetime.now(CT)

        # Master switch
        if not cfg["enabled"]:
            return False, "master switch OFF"

        # Setup enabled?
        rules = cfg["setup_rules"].get(signal["setup_name"], {})
        if not rules.get("enabled", True):
            return False, f"{signal['setup_name']} disabled"

        # DD Exhaustion filters (enabled per-config)
        if signal["setup_name"] == "DD Exhaustion":
            if cfg.get("dd_block_after_14et"):
                now_et = datetime.now(ET)
                if now_et.time() >= dtime(14, 0):
                    return False, "DD blocked after 14:00 ET (0% WR)"
            if cfg.get("dd_block_bofa_pure"):
                paradigm = signal.get("paradigm") or ""
                if "BOFA" in paradigm and "PURE" in paradigm:
                    return False, "DD blocked on BOFA-PURE paradigm (18% WR)"

        # V10 filter (V9-SC + GEX-LIS block)
        # Longs: alignment >= +2, VIX gate at 22 (SC exempt), overvix override
        # Shorts: whitelist SC + DD(align!=0) + AG, block GEX-LIS paradigm on SC/DD
        if cfg.get("greek_filter_enabled"):
            alignment = signal.get("greek_alignment", 0)
            _is_long = signal.get("direction", "") in ("long", "bullish")
            sname = signal.get("setup_name", "")
            if _is_long:
                if alignment < 2:
                    return False, f"Greek filter: long alignment {alignment:+d} < +2"
                # V10: Skew Charm exempt from VIX gate (82% WR at VIX 22-26)
                if sname != "Skew Charm":
                    _sig_vix = signal.get("vix")
                    _sig_ov = signal.get("overvix")
                    if _sig_vix is not None and _sig_vix > 22:
                        _ov = _sig_ov if _sig_ov is not None else -99
                        if _ov < 2:
                            return False, f"V9 VIX gate: VIX={_sig_vix:.1f}>22, overvix={_ov:+.1f}<+2"
            else:
                # V10 short whitelist
                _short_allowed = False
                if sname == "Skew Charm":
                    _short_allowed = True
                elif sname == "AG Short":
                    _short_allowed = True
                elif sname == "DD Exhaustion" and alignment != 0:
                    _short_allowed = True
                if not _short_allowed:
                    return False, f"Greek filter: {sname} short not in V10 whitelist (align={alignment:+d})"
                # Block GEX-LIS paradigm on SC/DD shorts (43% WR, -57.6 pts — LIS = support floor)
                _paradigm = signal.get("paradigm") or ""
                if _paradigm == "GEX-LIS" and sname in ("Skew Charm", "DD Exhaustion"):
                    return False, f"GEX-LIS paradigm blocked for {sname} short"

        # Already in position?
        # Opposite-direction signals return "reverse" so main loop can close + reopen
        if self.has_open_position:
            return False, "already in position"

        # Daily loss floor — stop trading when net P&L drops below threshold
        daily_loss_floor = cfg.get("daily_loss_floor", -800)
        if self.daily_pnl <= daily_loss_floor:
            return False, f"daily loss floor reached (${self.daily_pnl:+.0f} <= ${daily_loss_floor})"

        # Daily P&L cap (E2T consistency rule: no single day > 30% of target)
        daily_cap = cfg.get("e2t_daily_pnl_cap", 0)
        if daily_cap > 0 and self.daily_pnl >= daily_cap:
            return False, f"daily P&L cap reached (${self.daily_pnl:+.0f} >= ${daily_cap:.0f})"

        # Market hours
        market_open = datetime.strptime(cfg["market_open_ct"], "%H:%M").time()
        if now.time() < market_open:
            return False, f"before market open ({cfg['market_open_ct']} CT)"

        # No new trades cutoff
        cutoff = datetime.strptime(cfg["no_new_trades_after_ct"], "%H:%M").time()
        if now.time() >= cutoff:
            return False, f"past entry cutoff ({cfg['no_new_trades_after_ct']} CT)"

        # Dynamic position sizing
        stop_pts = rules.get("stop", 15)
        qty = _calc_qty(cfg, stop_pts)
        potential_loss = stop_pts * qty * MES_POINT_VALUE
        effective_limit = cfg["e2t_daily_loss_limit"] - cfg["e2t_daily_loss_buffer"]

        if self.daily_pnl <= -effective_limit:
            return False, f"daily P&L at buffer (${self.daily_pnl:+.0f}, limit: -${effective_limit:.0f})"

        if self.daily_pnl - potential_loss < -cfg["e2t_daily_loss_limit"]:
            return False, (f"potential loss ${potential_loss:.0f} would breach daily limit "
                           f"(P&L: ${self.daily_pnl:+.0f})")

        # Trailing drawdown (floor caps at starting balance — E2T rule)
        current_bal = cfg["e2t_starting_balance"] + self.total_pnl
        drawdown_floor = min(
            cfg["e2t_peak_balance"] - cfg["e2t_eod_trailing_drawdown"],
            cfg["e2t_starting_balance"],
        )
        if current_bal - potential_loss <= drawdown_floor:
            return False, (f"potential loss would breach drawdown floor "
                           f"(bal: ${current_bal:,.0f}, floor: ${drawdown_floor:,.0f})")

        # Max contracts
        max_mes = cfg["e2t_max_contracts_es_equiv"] * 10
        if qty > max_mes:
            return False, f"qty {qty} exceeds max {max_mes} MES"

        return True, "ok"

    def record_trade(self, pnl_pts: float, setup_name: str, qty: int = 0):
        """Record completed trade P&L. Uses actual trade qty for dollar calculation.
        Deducts round-trip commission per contract so daily_pnl tracks NET P&L."""
        trade_qty = qty or self.cfg["qty"]
        gross_dollars = pnl_pts * trade_qty * MES_POINT_VALUE
        commission = trade_qty * self.cfg.get("commission_per_contract", 0)
        pnl_dollars = gross_dollars - commission
        self.daily_pnl += pnl_dollars
        self.total_pnl += pnl_dollars
        self.daily_commissions += commission
        self.trades_today += 1
        if pnl_dollars < 0:
            self.losses_today += 1
        today = datetime.now(CT).strftime("%Y-%m-%d")
        self.trade_days.add(today)
        self.has_open_position = False

        current_bal = self.cfg["e2t_starting_balance"] + self.total_pnl
        log.info(f"Trade recorded: {setup_name} {pnl_pts:+.1f} pts x {trade_qty} "
                 f"(gross=${gross_dollars:+.0f} comm=${commission:.0f} net=${pnl_dollars:+.0f})")
        log.info(f"  Daily: ${self.daily_pnl:+.0f} (comm=${self.daily_commissions:.0f}) | "
                 f"Total: ${self.total_pnl:+.0f} | Balance: ${current_bal:,.0f} | "
                 f"Losses today: {self.losses_today}")
        self.save()


# ═════════════════════════════════════════════════════════════════════════════
#  NINJATRADER 8 OIF BRIDGE
# ═════════════════════════════════════════════════════════════════════════════

def _round_tick(price: float) -> str:
    """Round to MES tick size (0.25) and format as string."""
    rounded = round(round(price / MES_TICK_SIZE) * MES_TICK_SIZE, 2)
    return f"{rounded:.2f}"


def _calc_qty(cfg: dict, stop_pts: float) -> int:
    """Calculate dynamic position size: max_trade_risk / (stop × $5/pt).

    Caps at E2T max (60 MES). Falls back to cfg["qty"] if dynamic sizing disabled.
    """
    if not cfg.get("dynamic_sizing", False):
        return cfg["qty"]
    max_risk = cfg.get("max_trade_risk", 300)
    qty = max(1, int(max_risk / (stop_pts * MES_POINT_VALUE)))
    max_mes = cfg["e2t_max_contracts_es_equiv"] * 10  # 60 MES
    return min(qty, max_mes)


class NT8Bridge:
    """Writes OIF files to NinjaTrader 8's incoming folder.

    OIF format: PLACE;ACCOUNT;INSTRUMENT;ACTION;QTY;ORDER_TYPE;[LIMIT];[STOP];TIF;;[ORDID];;
    Files are consumed instantly by NT8. Stop/target orders execute at exchange level.
    """

    def __init__(self, incoming_folder: str, account_id: str, symbol: str):
        self.incoming = Path(incoming_folder)
        self.account = account_id
        self.symbol = symbol
        self._counter = int(time.time()) % 100000
        self._write_seq = 0

        if not self.incoming.exists():
            log.error(f"NT8 incoming folder NOT FOUND: {self.incoming}")
            log.error("Create the folder or fix nt8_incoming_folder in config.")

    def _oid(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}{self._counter}"

    def _write(self, cmd: str):
        for attempt in range(2):
            try:
                self._write_seq += 1
                f = self.incoming / f"oif{int(time.time() * 1000)}_{self._write_seq}.txt"
                f.write_text(cmd)
                log.debug(f"OIF: {cmd.strip()}")
                return
            except Exception as e:
                if attempt == 0:
                    log.warning(f"OIF write failed (retry in 0.5s): {e}")
                    time.sleep(0.5)
                else:
                    log.error(f"OIF write FAILED after retry: {e}")
                    raise

    def place_bracket(self, direction: str, qty: int,
                      stop_price: float, target_price: float) -> dict:
        """Place market entry + stop + target as 3 separate OIF orders.

        Returns dict with order IDs: {entry_oid, stop_oid, target_oid}.
        All 3 orders are exchange-level — they persist even if this script dies.
        """
        is_long = direction in ("long", "bullish")
        entry_side = "BUY" if is_long else "SELL"
        exit_side = "SELL" if is_long else "BUY"

        entry_oid = self._oid("e")
        stop_oid = self._oid("s")
        target_oid = self._oid("t")

        # 1. Market entry
        self._write(
            f"PLACE;{self.account};{self.symbol};{entry_side};{qty};"
            f"MARKET;;;DAY;;{entry_oid};;\n"
        )

        # Small delay so NT8 processes entry before exit orders
        time.sleep(0.3)

        # 2. Stop-market (exit side, full qty)
        self._write(
            f"PLACE;{self.account};{self.symbol};{exit_side};{qty};"
            f"STOPMARKET;;{_round_tick(stop_price)};DAY;;{stop_oid};;\n"
        )

        # 3. Limit target (exit side, full qty)
        self._write(
            f"PLACE;{self.account};{self.symbol};{exit_side};{qty};"
            f"LIMIT;{_round_tick(target_price)};;DAY;;{target_oid};;\n"
        )

        log.info(f"NT8 bracket placed: {entry_side} {qty} {self.symbol} "
                 f"stop={_round_tick(stop_price)} target={_round_tick(target_price)}")

        return {"entry_oid": entry_oid, "stop_oid": stop_oid, "target_oid": target_oid}

    def place_entry_and_stop(self, direction: str, qty: int,
                             stop_price: float) -> dict:
        """Place market entry + stop only (no target limit order).

        Used for trail-only setups like DD Exhaustion where exit comes from
        Railway outcome tracking (via Telegram) or EOD flatten.
        Returns dict with order IDs: {entry_oid, stop_oid, target_oid}.
        target_oid is None for trail-only.
        """
        is_long = direction in ("long", "bullish")
        entry_side = "BUY" if is_long else "SELL"
        exit_side = "SELL" if is_long else "BUY"

        entry_oid = self._oid("e")
        stop_oid = self._oid("s")

        # 1. Market entry
        self._write(
            f"PLACE;{self.account};{self.symbol};{entry_side};{qty};"
            f"MARKET;;;DAY;;{entry_oid};;\n"
        )

        time.sleep(0.3)

        # 2. Stop-market (exit side, full qty)
        self._write(
            f"PLACE;{self.account};{self.symbol};{exit_side};{qty};"
            f"STOPMARKET;;{_round_tick(stop_price)};DAY;;{stop_oid};;\n"
        )

        log.info(f"NT8 entry+stop placed: {entry_side} {qty} {self.symbol} "
                 f"stop={_round_tick(stop_price)} (trail-only, no target)")

        return {"entry_oid": entry_oid, "stop_oid": stop_oid, "target_oid": None}

    def place_limit_entry_only(self, direction: str, qty: int,
                               limit_price: float) -> dict:
        """Place LIMIT entry only (no stop/target). Used for charm S/R deferred entry.

        Stop and target are placed AFTER the entry fills to prevent
        creating unintended positions from orphan exit orders.
        """
        is_long = direction in ("long", "bullish")
        entry_side = "BUY" if is_long else "SELL"
        entry_oid = self._oid("L")  # L for limit

        self._write(
            f"PLACE;{self.account};{self.symbol};{entry_side};{qty};"
            f"LIMIT;{_round_tick(limit_price)};;DAY;;{entry_oid};;\n"
        )

        log.info(f"NT8 LIMIT entry placed: {entry_side} {qty} {self.symbol} "
                 f"LIMIT @ {_round_tick(limit_price)}")

        return {"entry_oid": entry_oid, "stop_oid": None, "target_oid": None}

    def place_deferred_exits(self, direction: str, qty: int,
                             stop_price: float, target_price: float | None = None) -> dict:
        """Place stop + optional target after a limit entry fills.

        Returns dict with {stop_oid, target_oid}.
        """
        is_long = direction in ("long", "bullish")
        exit_side = "SELL" if is_long else "BUY"

        stop_oid = self._oid("s")
        target_oid = None

        # 1. Stop-market
        self._write(
            f"PLACE;{self.account};{self.symbol};{exit_side};{qty};"
            f"STOPMARKET;;{_round_tick(stop_price)};DAY;;{stop_oid};;\n"
        )

        # 2. Limit target (optional)
        if target_price is not None:
            target_oid = self._oid("t")
            time.sleep(0.3)
            self._write(
                f"PLACE;{self.account};{self.symbol};{exit_side};{qty};"
                f"LIMIT;{_round_tick(target_price)};;DAY;;{target_oid};;\n"
            )

        log.info(f"NT8 deferred exits placed: stop={_round_tick(stop_price)} "
                 f"target={_round_tick(target_price) if target_price else 'trail-only'}")

        return {"stop_oid": stop_oid, "target_oid": target_oid}

    def change_stop(self, order_id: str, new_stop_price: float, qty: int,
                    direction: str = "long"):
        """Modify an existing stop order price via OIF CHANGE command.

        direction is the POSITION direction (long/short). The stop action is
        the opposite: SELL for longs, BUY for shorts.
        """
        exit_side = "SELL" if direction in ("long", "bullish") else "BUY"
        self._write(
            f"CHANGE;{self.account};{self.symbol};{exit_side};{qty};"
            f"STOPMARKET;;{_round_tick(new_stop_price)};DAY;;{order_id};;\n"
        )
        log.info(f"NT8 CHANGE stop: {order_id} → {_round_tick(new_stop_price)}")

    def cancel(self, order_id: str):
        """Cancel an order by ID.

        OIF CANCEL uses same 12-field format as PLACE — order ID in position 11:
        CANCEL;;;;;;;;;;orderId;;
        """
        self._write(f"CANCEL;;;;;;;;;;{order_id};;\n")
        log.info(f"NT8 cancel: {order_id}")

    def close_position(self, direction: str = "long", qty: int = 0):
        """Flatten position by placing a counter market order.

        CLOSEPOSITION OIF command is unreliable in NT8 — use explicit
        market order in opposite direction instead.
        """
        if not qty:
            log.warning("close_position called with qty=0, skipping")
            return
        close_side = "SELL" if direction in ("long", "bullish") else "BUY"
        close_oid = self._oid("x")
        self._write(
            f"PLACE;{self.account};{self.symbol};{close_side};{qty};"
            f"MARKET;;;DAY;;{close_oid};;\n"
        )
        log.info(f"NT8 close: {close_side} {qty} {self.symbol} (oid={close_oid})")

    def cancel_all(self):
        """Cancel ALL working orders for this symbol (safety net)."""
        self._write(f"CANCELALLORDERS;{self.account};{self.symbol}\n")
        log.info(f"NT8 CANCELALLORDERS: {self.symbol}")

    def check_order_state(self, order_id: str) -> dict | None:
        """Check NT8 outgoing folder for order fill/reject status.

        NT8 writes '{account}_{orderID}.txt' with content like:
          FILLED;qty;price   or   REJECTED;0;0
        Returns {status, qty, price} or None if no file found.
        """
        outgoing = self.incoming.parent / "outgoing"
        if not outgoing.exists():
            return None
        f = outgoing / f"{self.account}_{order_id}.txt"
        if not f.exists():
            return None
        try:
            content = f.read_text().strip()
            parts = content.split(";")
            status = parts[0]
            qty = int(parts[1]) if len(parts) > 1 else 0
            price = float(parts[2]) if len(parts) > 2 else 0.0
            return {"status": status, "qty": qty, "price": price}
        except Exception:
            return None


# ═════════════════════════════════════════════════════════════════════════════
#  NT8 POSITION STATE READER
# ═════════════════════════════════════════════════════════════════════════════

NT8_POSITION_STALE_SEC = 10  # consider NT8 data stale if older than this

def read_nt8_position(nt8_incoming_folder: str) -> dict | None:
    """Read position_state.json written by NT8 PositionReporter strategy.

    Returns dict with {status, account, instrument, position, quantity,
    avg_price, orders, timestamp} or None if file missing/unreadable/stale.
    """
    nt8_base = Path(nt8_incoming_folder).parent
    pos_file = nt8_base / "position_state.json"
    if not pos_file.exists():
        return None
    try:
        data = json.loads(pos_file.read_text())
        if data.get("status") != "online":
            return None
        # Check staleness
        ts_str = data.get("timestamp", "")
        if ts_str:
            ts = datetime.fromisoformat(ts_str)
            age = (datetime.now(ts.tzinfo) - ts).total_seconds()
            if age > NT8_POSITION_STALE_SEC:
                return None  # NT8 data too old
        return data
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════════════════
#  POSITION TRACKER
# ═════════════════════════════════════════════════════════════════════════════

class PositionTracker:
    """Tracks open position state. Persists to disk for crash recovery."""

    def __init__(self, nt8: NT8Bridge, compliance: ComplianceGate, cfg: dict,
                 quote_poller: TSQuotePoller | None = None):
        self.nt8 = nt8
        self.compliance = compliance
        self.cfg = cfg
        self.quote_poller = quote_poller
        self.position = None
        self._load()

    def _load(self):
        if POSITION_FILE.exists():
            try:
                pos = json.loads(POSITION_FILE.read_text())
                if pos:
                    # Check if position is from a previous day → auto-flatten
                    pos_ts = pos.get("ts", "")
                    if pos_ts:
                        try:
                            pos_dt = datetime.fromisoformat(pos_ts)
                            today_ct = datetime.now(CT).date()
                            if pos_dt.date() < today_ct:
                                log.warning(f"STALE POSITION from {pos_dt.date()}: "
                                            f"{pos['setup_name']} {pos['direction']} "
                                            f"@ {pos['entry_price']:.2f}")
                                log.warning(f"  Auto-flattening — should have been closed EOD")
                                self.position = pos  # set temporarily so flatten() works
                                self.compliance.has_open_position = True
                                # Don't call flatten() yet — nt8 not initialized
                                # Just mark for flatten on first loop iteration
                                self._stale_flatten = True
                                return
                        except Exception:
                            pass

                    # Check if pending_limit entry is stale (>30 min old → expired)
                    # Can't cancel broker order here (bridge not connected yet),
                    # so save the entry_oid for cancellation after bridge.connect()
                    if pos.get("pending_limit"):
                        is_stale_limit = False
                        placed_at = pos.get("limit_placed_at", "")
                        if placed_at:
                            try:
                                placed_dt = datetime.fromisoformat(placed_at)
                                elapsed = (datetime.now(CT) - placed_dt).total_seconds()
                                if elapsed > self._LIMIT_ENTRY_TIMEOUT_S:
                                    log.warning(f"STALE PENDING LIMIT from {placed_at}: "
                                                f"{pos['setup_name']} {pos['direction']} "
                                                f"limit={pos.get('limit_entry_price', '?')}")
                                    log.warning(f"  Elapsed {elapsed/60:.0f}min > "
                                                f"{self._LIMIT_ENTRY_TIMEOUT_S/60:.0f}min — expired")
                                    is_stale_limit = True
                            except Exception as e:
                                log.warning(f"  pending_limit date parse failed: {e}")
                                is_stale_limit = True
                        else:
                            log.warning(f"  pending_limit has no limit_placed_at — treating as stale")
                            is_stale_limit = True
                        if is_stale_limit:
                            # Save entry_oid so startup code can cancel it after bridge connects
                            self._stale_pending_cancel_oid = pos.get("entry_oid")
                            log.warning(f"  Will cancel broker order {self._stale_pending_cancel_oid} "
                                        f"after bridge connects")
                            POSITION_FILE.write_text("{}")
                            self._stale_flatten = False
                            return

                    self.position = pos
                    self.compliance.has_open_position = True
                    tgt = pos.get('target_price')
                    tgt_str = f"{tgt:.2f}" if tgt else "trail-only"
                    log.info(f"Position restored: {pos['setup_name']} {pos['direction']} "
                             f"@ {pos['entry_price']:.2f} (stop={pos['stop_price']:.2f} "
                             f"target={tgt_str})")
            except Exception:
                pass
        self._stale_flatten = False
        if not hasattr(self, '_stale_pending_cancel_oid'):
            self._stale_pending_cancel_oid = None

    def _save(self):
        if self.position:
            POSITION_FILE.write_text(json.dumps(self.position, indent=2))
        elif POSITION_FILE.exists():
            POSITION_FILE.unlink()

    def open_trade(self, signal: dict):
        """Place bracket order and track position.

        Target resolution order:
          1. If setup target is "msg" → use msg_target_pts from Telegram (Volland target)
          2. If setup target is None → trail-only, no target limit order (DD Exhaustion)
          3. If setup target is a number → use that fixed value
        Falls back to 10 pts if "msg" but no target found in message.
        """
        name = signal["setup_name"]
        direction = signal["direction"]
        spot = signal["spot"]
        if name not in self.cfg["setup_rules"]:
            log.error(f"  ABORT OPEN: unknown setup '{name}' (not in setup_rules)")
            return
        rules = self.cfg["setup_rules"][name]
        is_long = direction in ("long", "bullish")

        stop_pts = rules["stop"]
        max_sl = self.cfg.get("max_stop_loss_pts")
        if max_sl and stop_pts > max_sl:
            log.info(f"  Stop {stop_pts}pts capped to {max_sl}pts (survival mode)")
            stop_pts = max_sl
        qty = _calc_qty(self.cfg, stop_pts)

        # Resolve target
        cfg_target = rules.get("target")
        trail_only = False
        if cfg_target == "msg":
            # Use full Volland target from Telegram message
            target_pts = signal.get("msg_target_pts")
            if not target_pts or target_pts <= 0:
                target_pts = 10  # fallback if not parsed
                log.warning(f"  No target in Telegram message — fallback to {target_pts}pts")
            else:
                log.info(f"  Using Volland target from message: {target_pts:.1f}pts")
        elif cfg_target is None:
            # Trail-only: no target limit order (rely on breakeven + Railway outcome)
            trail_only = True
            target_pts = 0
            log.info(f"  Trail-only mode: no target limit order")
        else:
            target_pts = float(cfg_target)

        # Use ES/MES price for stop/target calculation (SPX and MES differ by ~15-20 pts)
        # ES price comes from Railway API's quote stream; fall back to SPX spot if unavailable
        _MAX_ES_SPX_SPREAD = 75  # reject stale ES price if spread > 25 pts
        es_price = signal.get("es_price")
        if es_price is not None:
            es_price = float(es_price)
        if es_price:
            spread = abs(es_price - spot)
            if spread > _MAX_ES_SPX_SPREAD:
                log.warning(f"  STALE ES PRICE: {es_price:.2f} vs SPX {spot:.2f} "
                            f"(spread={spread:.1f} > {_MAX_ES_SPX_SPREAD}) — using SPX spot")
                es_price = None
                order_ref = spot
            else:
                order_ref = es_price
                log.info(f"  Using ES price for orders: {es_price:.2f} (SPX spot: {spot:.2f})")
        else:
            order_ref = spot
            log.warning(f"  ES price unavailable — using SPX spot {spot:.2f} for orders")

        stop_price = (order_ref - stop_pts) if is_long else (order_ref + stop_pts)
        target_price = (order_ref + target_pts) if is_long else (order_ref - target_pts)

        # ES entry price for breakeven tracking
        es_entry = es_price
        if not es_entry and self.quote_poller and self.quote_poller.available:
            es_entry = self.quote_poller.get_es_price()
        if es_entry:
            log.info(f"  ES entry price: {es_entry:.2f} (for breakeven tracking)")

        # Charm S/R limit entry for shorts
        charm_limit = signal.get("charm_limit_entry")
        use_limit_entry = charm_limit is not None and not is_long

        if use_limit_entry:
            # Convert SPX charm limit to MES space
            spx_to_mes = order_ref - spot
            mes_limit = charm_limit + spx_to_mes
            oids = self.nt8.place_limit_entry_only(direction, qty, mes_limit)

            self.position = {
                "setup_name": name,
                "direction": direction,
                "grade": signal.get("grade", "?"),
                "entry_price": order_ref,
                "spx_spot": spot,
                "stop_price": stop_price,
                "target_price": target_price if not trail_only else None,
                "stop_pts": stop_pts,
                "target_pts": target_pts if not trail_only else None,
                "trail_only": trail_only,
                "qty": qty,
                "ts": datetime.now(CT).isoformat(),
                "max_hold_min": rules.get("max_hold_min"),
                "es_entry_price": es_entry,
                "be_triggered": False,
                "pending_limit": True,
                "limit_entry_price": _round_tick(mes_limit),
                "limit_placed_at": datetime.now(CT).isoformat(),
                "deferred_stop": stop_price,
                "deferred_target": target_price if not trail_only else None,
                **oids,
            }
            self.compliance.has_open_position = True
            self._save()

            log.info(f"TRADE OPENED (LIMIT): {name} {direction.upper()} [{signal.get('grade', '?')}]")
            log.info(f"  MES LIMIT Entry: {_round_tick(mes_limit)} (market @ {order_ref:.2f})")
            log.info(f"  [CHARM S/R] Deferred stop={stop_price:.2f} target={target_price:.2f}")
            log.info(f"  Waiting for limit fill (30 min timeout)")
        else:
            # Standard market entry
            if trail_only:
                oids = self.nt8.place_entry_and_stop(direction, qty, stop_price)
            else:
                oids = self.nt8.place_bracket(direction, qty, stop_price, target_price)

            self.position = {
                "setup_name": name,
                "direction": direction,
                "grade": signal.get("grade", "?"),
                "entry_price": order_ref,
                "spx_spot": spot,
                "stop_price": stop_price,
                "target_price": target_price if not trail_only else None,
                "stop_pts": stop_pts,
                "target_pts": target_pts if not trail_only else None,
                "trail_only": trail_only,
                "qty": qty,
                "ts": datetime.now(CT).isoformat(),
                "max_hold_min": rules.get("max_hold_min"),
                "es_entry_price": es_entry,
                "be_triggered": False,
                **oids,
            }
            self.compliance.has_open_position = True
            self._save()

            pnl_risk = stop_pts * qty * MES_POINT_VALUE
            if trail_only:
                log.info(f"TRADE OPENED: {name} {direction.upper()} [{signal.get('grade', '?')}]")
                log.info(f"  MES Entry: {order_ref:.2f} | Stop: {stop_price:.2f} (-{stop_pts}pts / -${pnl_risk:.0f}) | "
                         f"Target: TRAIL-ONLY (breakeven @ +{self.cfg.get('be_trigger_pts', 5)}pts) | Qty: {qty}")
            else:
                pnl_reward = target_pts * qty * MES_POINT_VALUE
                log.info(f"TRADE OPENED: {name} {direction.upper()} [{signal.get('grade', '?')}]")
                log.info(f"  MES Entry: {order_ref:.2f} | Stop: {stop_price:.2f} (-{stop_pts}pts / -${pnl_risk:.0f}) | "
                         f"Target: {target_price:.2f} (+{target_pts:.1f}pts / +${pnl_reward:.0f}) | Qty: {qty}")

    def close_on_outcome(self, outcome: dict):
        """Close position when Railway sends outcome via Telegram."""
        if not self.position:
            return
        if outcome["setup_name"] != self.position["setup_name"]:
            return

        pnl_pts = outcome["pnl_pts"]
        result = outcome["result"]
        trade_qty = self.position.get("qty", self.cfg["qty"])

        # Cancel ALL remaining orders — always cancel both stop and target
        # (reconciler may resolve before NT8 fills, leaving orphaned orders)
        if self.position.get("stop_oid"):
            self.nt8.cancel(self.position["stop_oid"])
        if self.position.get("target_oid"):
            self.nt8.cancel(self.position["target_oid"])
        if result == "EXPIRED" or self.position.get("trail_only"):
            # Force close any remaining position (always close for trail-only)
            self.nt8.close_position(self.position["direction"], trade_qty)

        self.compliance.record_trade(pnl_pts, self.position["setup_name"], trade_qty)

        emoji = {"WIN": "V", "LOSS": "X", "EXPIRED": "~"}.get(result, "?")
        log.info(f"[{emoji}] CLOSED: {self.position['setup_name']} | {result} | "
                 f"{pnl_pts:+.1f} pts x {trade_qty}")

        self.position = None
        self._save()

    def is_opposite(self, signal: dict) -> bool:
        """Check if signal is in opposite direction to current position."""
        if not self.position:
            return False
        pos_long = self.position["direction"] in ("long", "bullish")
        sig_long = signal["direction"] in ("long", "bullish")
        return pos_long != sig_long

    def tighten_stop(self, es_price: float, gap_pts: float = _TIGHTEN_GAP_PTS):
        """Tighten SL to gap_pts from current ES price.

        Used on low-conviction opposing signals — keep position open but
        move stop closer as a compromise.
        """
        if not self.position:
            return
        is_long = self.position["direction"] in ("long", "bullish")
        raw_stop = (es_price - gap_pts) if is_long else (es_price + gap_pts)
        # Round to tick size as a float (change_stop calls _round_tick internally)
        new_stop = round(round(raw_stop / MES_TICK_SIZE) * MES_TICK_SIZE, 2)
        old_stop = self.position["stop_price"]

        self.nt8.change_stop(self.position["stop_oid"], new_stop,
                             self.position.get("qty", self.cfg["qty"]),
                             self.position["direction"])
        self.position["stop_price"] = new_stop
        self._save()

        log.info(f"  Tightened SL: {old_stop} → {new_stop:.2f} "
                 f"({gap_pts}pts from ES {es_price:.2f}), keeping {self.position['setup_name']} open")

    def reverse(self, signal: dict, es_price: float | None):
        """Close current position and open new one in opposite direction.

        Avoids unreliable CLOSEPOSITION. Instead:
        1. Cancel old stop/target by ID
        2. Single market order: old_qty + new_qty in new direction
           (e.g., LONG 3 → SHORT 2 = SELL 5 at market — atomic net-off)
        3. Place new stop and target
        """
        if not self.position:
            return

        old_name = self.position["setup_name"]
        old_dir = self.position["direction"].upper()
        new_name = signal["setup_name"]
        new_dir = signal["direction"].upper()
        old_qty = self.position.get("qty", self.cfg["qty"])

        # Step 0: Validate new setup rules BEFORE touching any orders
        # (prevents orphan positions if new setup is unknown/misconfigured)
        if new_name not in self.cfg["setup_rules"]:
            log.error(f"  ABORT REVERSE: unknown setup '{new_name}' — keeping current position")
            return
        new_rules = self.cfg["setup_rules"][new_name]

        log.info(f"REVERSING: closing {old_name} {old_dir} for new {new_name} {new_dir}")

        # If current position is a pending limit, just cancel entry and open new trade
        if self.position.get("pending_limit"):
            entry_oid = self.position.get("entry_oid")
            if entry_oid:
                self.nt8.cancel(entry_oid)
            log.info(f"  Cancelled pending limit entry for {old_name}")
            self.compliance.record_trade(0.0, old_name, old_qty)
            self.position = None
            self.compliance.has_open_position = False
            self._save()
            self.open_trade(signal)
            return

        # Step 1: Cancel old exit orders individually (known-working CANCEL command)
        if self.position.get("stop_oid"):
            self.nt8.cancel(self.position["stop_oid"])
        if self.position.get("target_oid"):
            time.sleep(0.3)
            self.nt8.cancel(self.position["target_oid"])

        # Estimate P&L from ES price
        pnl_pts = 0.0
        if es_price and self.position.get("es_entry_price"):
            is_long = self.position["direction"] in ("long", "bullish")
            pnl_pts = (es_price - self.position["es_entry_price"]) if is_long else (self.position["es_entry_price"] - es_price)

        self.compliance.record_trade(pnl_pts, old_name, old_qty)
        log.info(f"  Closed {old_name}: ~{pnl_pts:+.1f} pts (estimated from ES price)")

        # Check if PNL cap reached after closing — if so, just close, don't reverse
        daily_cap = self.cfg.get("e2t_daily_pnl_cap", 0)
        if daily_cap > 0 and self.compliance.daily_pnl >= daily_cap:
            log.info(f"  PNL cap reached (${self.compliance.daily_pnl:+.0f} >= ${daily_cap:.0f}) — closing only, no reversal")
            # Just close the old position
            close_side = "SELL" if self.position["direction"] in ("long", "bullish") else "BUY"
            close_oid = self.nt8._oid("c")
            time.sleep(0.5)
            self.nt8._write(
                f"PLACE;{self.nt8.account};{self.nt8.symbol};{close_side};{old_qty};"
                f"MARKET;;;DAY;;{close_oid};;\n"
            )
            self.position = None
            self.compliance.has_open_position = False
            self._save()
            self.compliance.save()
            return

        # NOTE: do NOT clear position file here — keep old position on disk
        # until new position is saved. Prevents orphan positions on crash.
        new_stop_pts = new_rules["stop"]
        new_qty = _calc_qty(self.cfg, new_stop_pts)
        new_is_long = signal["direction"] in ("long", "bullish")

        # Step 3: Atomic net-off — single market order covers close + new entry
        # e.g., was LONG 3, want SHORT 2 → SELL 5 at market
        net_qty = old_qty + new_qty
        net_side = "BUY" if new_is_long else "SELL"
        exit_side = "SELL" if new_is_long else "BUY"
        net_oid = self.nt8._oid("r")

        time.sleep(0.5)
        self.nt8._write(
            f"PLACE;{self.nt8.account};{self.nt8.symbol};{net_side};{net_qty};"
            f"MARKET;;;DAY;;{net_oid};;\n"
        )
        log.info(f"  Net-off order: {net_side} {net_qty} (close {old_qty} + open {new_qty})")

        # Step 4: Compute new stop/target prices
        # Apply same stale ES price guard as open_trade()
        _MAX_ES_SPX_SPREAD = 75
        spot = float(signal["spot"])
        _es = signal.get("es_price") or es_price
        if _es is not None:
            _es = float(_es)
            if abs(_es - spot) > _MAX_ES_SPX_SPREAD:
                log.warning(f"  STALE ES PRICE: {_es:.2f} vs SPX {spot:.2f} "
                            f"(spread={abs(_es - spot):.1f} > {_MAX_ES_SPX_SPREAD}) — using SPX spot")
                _es = None
        es_ref = _es if _es is not None else spot
        raw_stop = (es_ref - new_stop_pts) if new_is_long else (es_ref + new_stop_pts)
        new_stop_price = round(round(raw_stop / MES_TICK_SIZE) * MES_TICK_SIZE, 2)

        cfg_target = new_rules.get("target")
        trail_only = cfg_target is None
        if cfg_target == "msg":
            target_pts = signal.get("msg_target_pts") or 10
        elif cfg_target is None:
            target_pts = 0
        else:
            target_pts = float(cfg_target)
        raw_target = (es_ref + target_pts) if new_is_long else (es_ref - target_pts)
        new_target_price = round(round(raw_target / MES_TICK_SIZE) * MES_TICK_SIZE, 2)

        es_entry = _es  # use validated es_price (None if stale)

        # Step 5: SAVE position immediately after net-off (crash resilience)
        # If we crash before placing stop/target, startup recovery will place them.
        # stop_oid/target_oid are None until placed — startup detects this.
        self.position = {
            "setup_name": new_name,
            "direction": signal["direction"],
            "grade": signal.get("grade", "?"),
            "entry_price": es_ref,
            "spx_spot": signal["spot"],
            "stop_price": new_stop_price,
            "target_price": new_target_price if not trail_only else None,
            "stop_pts": new_stop_pts,
            "target_pts": target_pts if not trail_only else None,
            "trail_only": trail_only,
            "qty": new_qty,
            "ts": datetime.now(CT).isoformat(),
            "max_hold_min": new_rules.get("max_hold_min"),
            "es_entry_price": float(es_entry) if es_entry else None,
            "be_triggered": False,
            "entry_oid": net_oid,
            "stop_oid": None,   # not placed yet
            "target_oid": None, # not placed yet
        }
        self.compliance.has_open_position = True  # record_trade() cleared this — restore for new position
        self._save()

        # Step 6: Place stop and target orders
        time.sleep(0.5)
        stop_oid = self.nt8._oid("s")
        self.nt8._write(
            f"PLACE;{self.nt8.account};{self.nt8.symbol};{exit_side};{new_qty};"
            f"STOPMARKET;;{_round_tick(new_stop_price)};DAY;;{stop_oid};;\n"
        )
        self.position["stop_oid"] = stop_oid
        self._save()

        target_oid = None
        if not trail_only:
            time.sleep(0.3)
            target_oid = self.nt8._oid("t")
            self.nt8._write(
                f"PLACE;{self.nt8.account};{self.nt8.symbol};{exit_side};{new_qty};"
                f"LIMIT;{_round_tick(new_target_price)};;DAY;;{target_oid};;\n"
            )
            self.position["target_oid"] = target_oid
            self._save()

        log.info(f"NT8 reverse placed: {net_side} {net_qty} (net) + "
                 f"stop={_round_tick(new_stop_price)}"
                 f"{' target=' + str(_round_tick(new_target_price)) if not trail_only else ' (trail-only)'}")

        log.info(f"TRADE OPENED: {new_name} {new_dir} [{signal.get('grade', '?')}]")
        log.info(f"  MES Entry: {es_ref:.2f} | Stop: {new_stop_price:.2f} "
                 f"(-{new_stop_pts}pts / -${new_stop_pts * new_qty * MES_POINT_VALUE:.0f}) | "
                 f"{'Target: ' + str(_round_tick(new_target_price)) + ' (+' + str(target_pts) + 'pts)' if not trail_only else 'Target: TRAIL-ONLY'}"
                 f" | Qty: {new_qty}")

    def flatten(self, reason: str = "EOD", es_price: float = None):
        """Force-close position (e.g., EOD flatten).

        Uses explicit cancel + market exit order (not CLOSEPOSITION which is unreliable).
        If es_price is provided, estimates P&L for accurate compliance tracking.
        """
        if not self.position:
            return

        # If pending limit entry, just cancel the entry order
        if self.position.get("pending_limit"):
            entry_oid = self.position.get("entry_oid")
            if entry_oid:
                self.nt8.cancel(entry_oid)
            log.info(f"FLATTEN: cancelled pending limit entry ({reason})")
            self.compliance.record_trade(0.0, self.position["setup_name"],
                                         self.position.get("qty", self.cfg["qty"]))
            self.position = None
            self.compliance.has_open_position = False
            self._save()
            return

        trade_qty = self.position.get("qty", self.cfg["qty"])
        is_long = self.position["direction"] in ("long", "bullish")
        exit_side = "SELL" if is_long else "BUY"

        # Cancel old exit orders
        if self.position.get("stop_oid"):
            self.nt8.cancel(self.position["stop_oid"])
        if self.position.get("target_oid"):
            time.sleep(0.3)
            self.nt8.cancel(self.position["target_oid"])

        # Market exit order to flatten
        time.sleep(0.5)
        flat_oid = self.nt8._oid("f")
        self.nt8._write(
            f"PLACE;{self.nt8.account};{self.nt8.symbol};{exit_side};{trade_qty};"
            f"MARKET;;;DAY;;{flat_oid};;\n"
        )

        # Estimate P&L from ES price (if available) for accurate compliance tracking
        pnl_pts = 0.0
        if es_price and self.position.get("entry_price"):
            entry = self.position["entry_price"]
            if is_long:
                pnl_pts = es_price - entry
            else:
                pnl_pts = entry - es_price
            log.info(f"FLATTENED ({reason}): {self.position['setup_name']} — "
                     f"{exit_side} {trade_qty} at market ~{es_price:.2f} "
                     f"(est PnL: {pnl_pts:+.1f} pts)")
        else:
            log.info(f"FLATTENED ({reason}): {self.position['setup_name']} — "
                     f"{exit_side} {trade_qty} at market (PnL unknown, recording 0)")

        self.compliance.record_trade(pnl_pts, self.position["setup_name"], trade_qty)
        self.position = None
        self._save()

    # Trail params — mirrors Railway's _trail_params in main.py
    # DD Exhaustion: continuous trail (activation=20, gap=5)
    # GEX Long: hybrid trail (BE at +8, continuous trail activation=10 gap=5)
    # AG Short: hybrid trail (BE at +10, continuous trail activation=12 gap=5)
    # ES Absorption: fixed target (SL=8/T=10), no trailing
    _TRAIL_PARAMS = {
        "DD Exhaustion":  {"mode": "continuous", "activation": 20, "gap": 5},
        "GEX Long":       {"mode": "hybrid", "be_trigger": 8, "activation": 10, "gap": 5},
        "GEX Velocity":   {"mode": "hybrid", "be_trigger": 8, "activation": 10, "gap": 5},
        "AG Short":       {"mode": "hybrid", "be_trigger": 10, "activation": 12, "gap": 5},
        "Skew Charm":     {"mode": "hybrid", "be_trigger": 10, "activation": 10, "gap": 5},
    }

    def check_trail(self, es_price: float | None):
        """Trailing stop + breakeven using live ES price from Railway API.

        Trail logic (same as Railway):
          - DD Exhaustion: continuous — once profit >= 20, lock at max_profit - 5
          - GEX Long: rung-based — +12→lock+10, +17→lock+15, +22→lock+20, ...
          - All others: breakeven only — move stop to entry at +be_trigger_pts
        """
        if not self.position or not es_price:
            return
        if self.position.get("pending_limit"):
            return  # No trail until limit entry fills
        if not self.position.get("es_entry_price"):
            return

        es_entry = self.position["es_entry_price"]
        is_long = self.position["direction"] in ("long", "bullish")
        profit = (es_price - es_entry) if is_long else (es_entry - es_price)
        setup_name = self.position["setup_name"]
        qty = self.position.get("qty", self.cfg["qty"])

        # Track max favorable excursion
        max_fav = self.position.get("_max_fav", 0.0)
        if profit > max_fav:
            max_fav = profit
            self.position["_max_fav"] = max_fav

        tp = self._TRAIL_PARAMS.get(setup_name)
        new_stop = None

        # ── Breakeven first (applies to ALL setups) ──
        be_pts = self.cfg.get("be_trigger_pts", 5.0)
        if not self.position.get("be_triggered") and profit >= be_pts:
            new_stop = es_entry
            self.position["be_triggered"] = True

        if tp:
            # ── Trailing stop setups (overrides BE once trail activates) ──
            if tp["mode"] == "continuous":
                if max_fav >= tp["activation"]:
                    lock = max_fav - tp["gap"]
                    trail_stop = (es_entry + lock) if is_long else (es_entry - lock)
                    if new_stop is None or (is_long and trail_stop > new_stop) or (not is_long and trail_stop < new_stop):
                        new_stop = trail_stop
            elif tp["mode"] == "hybrid":
                # Hybrid: breakeven at be_trigger, then continuous trail
                if max_fav >= tp["activation"]:
                    lock = max_fav - tp["gap"]
                    trail_stop = (es_entry + lock) if is_long else (es_entry - lock)
                    if new_stop is None or (is_long and trail_stop > new_stop) or (not is_long and trail_stop < new_stop):
                        new_stop = trail_stop
                elif max_fav >= tp["be_trigger"]:
                    # Lock at breakeven (entry price)
                    if new_stop is None or (is_long and es_entry > new_stop) or (not is_long and es_entry < new_stop):
                        new_stop = es_entry
            else:
                rung_start = tp["rung_start"]
                step = tp["step"]
                lock_offset = tp["lock_offset"]
                if max_fav >= rung_start:
                    rungs_hit = int((max_fav - rung_start) / step)
                    lock = rung_start + (rungs_hit * step) - lock_offset
                    trail_stop = (es_entry + lock) if is_long else (es_entry - lock)
                    if new_stop is None or (is_long and trail_stop > new_stop) or (not is_long and trail_stop < new_stop):
                        new_stop = trail_stop

        # Move stop if new level is tighter than current
        if new_stop is not None:
            current_stop = self.position["stop_price"]
            tighter = (new_stop > current_stop) if is_long else (new_stop < current_stop)
            if tighter:
                self.nt8.change_stop(self.position["stop_oid"], new_stop, qty,
                                     self.position["direction"])
                old_stop = self.position["stop_price"]
                self.position["stop_price"] = new_stop
                self._save()
                trail_type = "TRAIL" if tp else "BREAKEVEN"
                log.info(f"  {trail_type}: stop {old_stop:.2f} → {new_stop:.2f} "
                         f"(profit={profit:+.1f} max={max_fav:+.1f})")

    _LIMIT_ENTRY_TIMEOUT_S = 1800  # 30 min timeout for limit entries

    def check_nt8_fills(self):
        """Poll NT8 outgoing folder to detect stop/target fills or rejections.

        If the stop or target filled in NT8, close the position in the tracker
        so we don't block new signals with a phantom position.
        """
        if not self.position:
            return

        stop_oid = self.position.get("stop_oid")
        target_oid = self.position.get("target_oid")
        entry_oid = self.position.get("entry_oid")
        trade_qty = self.position.get("qty", self.cfg["qty"])
        is_long = self.position["direction"] in ("long", "bullish")

        # Check pending limit entry (charm S/R deferred flow)
        if self.position.get("pending_limit") and entry_oid:
            entry_state = self.nt8.check_order_state(entry_oid)
            if entry_state and entry_state["status"] == "FILLED":
                fill_price = entry_state["price"]
                log.info(f"NT8: LIMIT entry FILLED @ {fill_price:.2f}")
                imp = abs(fill_price - self.position.get("entry_price", fill_price))
                log.info(f"  [CHARM S/R] Improved {imp:.1f}pts from market")

                # Update entry price to actual fill for breakeven tracking
                self.position["entry_price"] = fill_price
                self.position["es_entry_price"] = fill_price
                self.position["pending_limit"] = False

                # Place deferred stop + target
                deferred_stop = self.position.get("deferred_stop")
                deferred_target = self.position.get("deferred_target")
                # Recalculate stop/target relative to actual fill price
                stop_pts = self.position["stop_pts"]
                if is_long:
                    new_stop = fill_price - stop_pts
                else:
                    new_stop = fill_price + stop_pts
                new_target = None
                if deferred_target is not None:
                    target_pts = self.position.get("target_pts")
                    if target_pts:
                        new_target = (fill_price + target_pts) if is_long else (fill_price - target_pts)

                self.position["stop_price"] = new_stop
                if new_target is not None:
                    self.position["target_price"] = new_target

                if self.position.get("trail_only"):
                    exit_oids = self.nt8.place_deferred_exits(
                        self.position["direction"], trade_qty, new_stop)
                else:
                    exit_oids = self.nt8.place_deferred_exits(
                        self.position["direction"], trade_qty, new_stop, new_target)

                self.position["stop_oid"] = exit_oids["stop_oid"]
                self.position["target_oid"] = exit_oids["target_oid"]
                self._save()
                return

            elif entry_state and entry_state["status"] == "REJECTED":
                log.warning(f"NT8: LIMIT entry REJECTED — clearing position")
                log.info(f"  [CHARM S/R] Limit entry not filled — trade skipped")
                self.position = None
                self.compliance.has_open_position = False
                self._save()
                return

            # Check timeout
            placed_at = self.position.get("limit_placed_at")
            if placed_at:
                try:
                    placed_dt = datetime.fromisoformat(placed_at)
                    elapsed = (datetime.now(CT) - placed_dt).total_seconds()
                    if elapsed > self._LIMIT_ENTRY_TIMEOUT_S:
                        self.nt8.cancel(entry_oid)
                        limit_px = self.position.get('limit_entry_price', '?')
                        log.info(f"NT8: LIMIT TIMEOUT — cancelled {entry_oid} after {elapsed/60:.0f}min")
                        log.info(f"  [CHARM S/R] {limit_px} not reached — trade skipped")
                        self.position = None
                        self.compliance.has_open_position = False
                        self._save()
                        return
                except (ValueError, TypeError) as e:
                    log.warning(f"  pending_limit timeout check failed: {e}")
            return  # Still waiting for fill, skip normal fill checks

        # Check if entry was rejected
        if entry_oid:
            entry_state = self.nt8.check_order_state(entry_oid)
            if entry_state and entry_state["status"] == "REJECTED":
                log.warning(f"NT8: entry REJECTED — cancelling orphan orders and clearing position")
                # Cancel stop and target orders that were placed after the entry.
                # These are now orphans — if left alive they could fill and create
                # an untracked position in NT8.
                if stop_oid:
                    self.nt8.cancel(stop_oid)
                    log.info(f"  Cancelled orphan stop: {stop_oid}")
                if target_oid:
                    self.nt8.cancel(target_oid)
                    log.info(f"  Cancelled orphan target: {target_oid}")
                self.position = None
                self.compliance.has_open_position = False
                self._save()
                return

        # Check if stop filled
        if stop_oid:
            stop_state = self.nt8.check_order_state(stop_oid)
            if stop_state and stop_state["status"] == "FILLED":
                fill_price = stop_state["price"]
                entry_price = self.position["entry_price"]
                pnl_pts = (fill_price - entry_price) if is_long else (entry_price - fill_price)
                log.info(f"NT8: stop FILLED @ {fill_price:.2f} → {pnl_pts:+.1f} pts")
                # Cancel target if it exists
                if target_oid:
                    self.nt8.cancel(target_oid)
                self.compliance.record_trade(pnl_pts, self.position["setup_name"], trade_qty)
                self.position = None
                self._save()
                return

        # Check if target filled
        if target_oid:
            target_state = self.nt8.check_order_state(target_oid)
            if target_state and target_state["status"] == "FILLED":
                fill_price = target_state["price"]
                entry_price = self.position["entry_price"]
                pnl_pts = (fill_price - entry_price) if is_long else (entry_price - fill_price)
                log.info(f"NT8: target FILLED @ {fill_price:.2f} → {pnl_pts:+.1f} pts")
                # Cancel stop
                if stop_oid:
                    self.nt8.cancel(stop_oid)
                self.compliance.record_trade(pnl_pts, self.position["setup_name"], trade_qty)
                self.position = None
                self._save()
                return

    _RECONCILE_GRACE_S = 30  # don't reconcile within 30s of opening a trade

    def reconcile_with_api(self, api_url: str, api_key: str):
        """Periodic safety net: re-check Railway API for resolved outcomes.

        Bypasses the normal _seen_outcomes filter. If Railway shows this setup
        as resolved (WIN/LOSS/EXPIRED), clear the phantom position.
        Only matches outcomes that were resolved AFTER the position was opened
        (prevents stale outcomes from closing fresh trades).
        """
        if not self.position:
            return
        # Grace period: don't reconcile too soon after opening
        pos_ts = self.position.get("ts")
        if pos_ts:
            try:
                pos_dt = datetime.fromisoformat(pos_ts)
                age_s = (datetime.now(CT) - pos_dt).total_seconds()
                if age_s < self._RECONCILE_GRACE_S:
                    return
            except Exception:
                pass
        try:
            resp = requests.get(
                api_url.rstrip("/") + "/api/eval/signals",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=8,
            )
            if resp.status_code != 200:
                return
            outcomes = resp.json().get("outcomes", [])
            pos_name = self.position["setup_name"]
            for o in outcomes:
                if (o.get("setup_name") == pos_name
                        and o.get("outcome_result") in ("WIN", "LOSS", "EXPIRED")):
                    # Only match outcomes resolved AFTER position opened
                    outcome_ts = o.get("outcome_ts") or o.get("updated_at")
                    if outcome_ts and pos_ts:
                        try:
                            o_dt = datetime.fromisoformat(outcome_ts)
                            p_dt = datetime.fromisoformat(pos_ts)
                            # Normalize: make both offset-aware for comparison
                            if o_dt.tzinfo is None:
                                o_dt = o_dt.replace(tzinfo=ET)
                            if p_dt.tzinfo is None:
                                p_dt = p_dt.replace(tzinfo=CT)
                            if o_dt < p_dt:
                                continue  # stale outcome from before this position
                        except Exception:
                            pass
                    pnl = o.get("outcome_pnl", 0) or 0
                    log.info(f"[RECONCILE] Railway shows {pos_name} resolved: "
                             f"{o['outcome_result']} ({pnl:+.1f} pts) — clearing phantom position")
                    self.close_on_outcome({
                        "setup_name": pos_name,
                        "result": o["outcome_result"],
                        "pnl_pts": pnl,
                    })
                    return
        except Exception as e:
            log.debug(f"Reconcile API check failed: {e}")

    def reconcile_with_nt8(self):
        """Check NT8 position_state.json for phantom position detection.

        If NT8 shows Flat but eval_trader thinks we're in a position,
        the position was closed without our knowledge → clear phantom.
        """
        nt8_data = read_nt8_position(self.cfg["nt8_incoming_folder"])
        if nt8_data is None:
            return  # file missing, stale, or offline — can't reconcile

        nt8_flat = nt8_data.get("position", "Flat") == "Flat"

        if self.position and nt8_flat:
            # Grace period: don't reconcile too soon after opening
            # (NT8 may not have processed the entry order yet)
            pos_ts = self.position.get("ts")
            if pos_ts:
                try:
                    pos_dt = datetime.fromisoformat(pos_ts)
                    age_s = (datetime.now(CT) - pos_dt).total_seconds()
                    if age_s < self._RECONCILE_GRACE_S:
                        return
                except Exception:
                    pass
            # NT8 is flat but we think we have a position → phantom
            log.warning(f"[NT8 RECONCILE] NT8 is FLAT but eval_trader has "
                        f"{self.position['setup_name']} {self.position['direction']} — "
                        f"clearing phantom position")
            # Cancel ALL remaining orders to prevent orphans
            if self.position.get("stop_oid"):
                self.nt8.cancel(self.position["stop_oid"])
            if self.position.get("target_oid"):
                self.nt8.cancel(self.position["target_oid"])
            trade_qty = self.position.get("qty", self.cfg["qty"])
            self.compliance.record_trade(0, self.position["setup_name"], trade_qty)
            self.position = None
            self.compliance.has_open_position = False
            self._save()

        elif not self.position and not nt8_flat:
            # NT8 has a position but we don't track it — warn user
            nt8_dir = nt8_data.get("position", "?")
            nt8_qty = nt8_data.get("quantity", 0)
            log.warning(f"[NT8 RECONCILE] NT8 has {nt8_dir} {nt8_qty} but eval_trader "
                        f"is not tracking it — manage manually or restart with position file")

    @property
    def is_open(self) -> bool:
        return self.position is not None


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN LOOP
# ═════════════════════════════════════════════════════════════════════════════

def _banner(cfg: dict):
    """Print startup banner with current config summary."""
    dynamic = cfg.get("dynamic_sizing", False)
    max_risk = cfg.get("max_trade_risk", 300)
    dd_floor_raw = cfg["e2t_peak_balance"] - cfg["e2t_eod_trailing_drawdown"]
    dd_floor = min(dd_floor_raw, cfg["e2t_starting_balance"])
    capped = dd_floor < dd_floor_raw

    log.info("=" * 60)
    log.info("  E2T EVALUATION AUTO-TRADER")
    log.info(f"  Config:    {CONFIG_FILE.name}")
    log.info("=" * 60)
    sym = cfg['nt8_mes_symbol']
    if sym.lower() == "auto":
        sym = f"{current_mes_symbol('nt8')} (auto)"
    log.info(f"  Symbol:    {sym}")
    log.info(f"  Account:   {cfg['nt8_account_id']}")
    log.info(f"  NT8 dir:   {cfg['nt8_incoming_folder']}")
    log.info(f"  Sizing:    {'DYNAMIC' if dynamic else 'FIXED'} "
             f"(max risk: ${max_risk}/trade)")
    log.info(f"  Balance:   ${cfg['e2t_starting_balance']:,.0f} (peak: ${cfg['e2t_peak_balance']:,.0f})")
    log.info(f"  DD floor:  ${dd_floor:,.0f}{' (capped)' if capped else ''}")
    log.info(f"  Daily lim: ${cfg['e2t_daily_loss_limit']:,.0f} (buffer: ${cfg['e2t_daily_loss_buffer']:.0f})")
    loss_floor = cfg.get("daily_loss_floor", -800)
    log.info(f"  Loss floor: ${loss_floor}/day (stop trading below this)")
    log.info(f"  BE trigger: +{cfg.get('be_trigger_pts', 5.0)} pts")
    log.info(f"  Cutoff:    {cfg['no_new_trades_after_ct']} CT | Flatten: {cfg['flatten_time_ct']} CT")
    log.info("-" * 60)

    daily_cap = cfg.get("e2t_daily_pnl_cap", 0)
    if daily_cap > 0:
        log.info(f"  P&L cap:   ${daily_cap:.0f}/day (E2T consistency rule)")

    # DD filters
    dd_filters = []
    if cfg.get("dd_block_after_14et"):
        dd_filters.append("block after 14:00 ET")
    if cfg.get("dd_block_bofa_pure"):
        dd_filters.append("block BOFA-PURE")
    if dd_filters:
        log.info(f"  DD filters: {', '.join(dd_filters)}")

    # Per-setup sizing breakdown
    log.info("  %-20s %5s %8s %4s %6s  %s" % ("Setup", "Stop", "Target", "Qty", "Risk", "Status"))
    log.info("  %-20s %5s %8s %4s %6s  %s" % ("-" * 20, "----", "------", "---", "-----", "------"))
    for name, rules in cfg["setup_rules"].items():
        enabled = rules.get("enabled", True)
        stop = rules.get("stop", 15)
        target = rules.get("target")
        qty = _calc_qty(cfg, stop) if dynamic else cfg["qty"]
        risk = stop * qty * MES_POINT_VALUE
        status = "ON" if enabled else "OFF"
        tgt_str = "Volland" if target == "msg" else ("trail" if target is None else f"{target}pt")
        log.info("  %-20s %5d %8s %4d $%5.0f  %s" % (name, stop, tgt_str, qty, risk, status))

    log.info("=" * 60)


def main():
    cfg = load_config()
    use_api = cfg.get("signal_source", "api") == "api"

    # Validate required fields based on signal source
    if use_api:
        required = {
            "railway_api_url": cfg.get("railway_api_url", ""),
            "eval_api_key": cfg.get("eval_api_key", ""),
            "nt8_incoming_folder": cfg["nt8_incoming_folder"],
            "nt8_account_id": cfg["nt8_account_id"],
        }
    else:
        required = {
            "telegram_bot_token": cfg["telegram_bot_token"],
            "telegram_chat_id": cfg["telegram_chat_id"],
            "nt8_incoming_folder": cfg["nt8_incoming_folder"],
            "nt8_account_id": cfg["nt8_account_id"],
        }
    missing = [k for k, v in required.items() if not v]
    if missing:
        log.error(f"Missing required config in {CONFIG_FILE}:")
        for m in missing:
            log.error(f"  - {m}")
        log.error("Fill in these fields and restart.")
        save_config(cfg)
        sys.exit(1)

    _banner(cfg)

    # Resolve MES symbol (auto-rollover or manual)
    mes_symbol = cfg["nt8_mes_symbol"]
    if mes_symbol.lower() == "auto":
        mes_symbol = current_mes_symbol("nt8")
        log.info(f"Auto-rollover: MES symbol resolved to {mes_symbol}")

    # Initialize components
    if use_api:
        api_poller = APIPoller(cfg["railway_api_url"], cfg["eval_api_key"])
        log.info(f"Signal source: Railway API ({cfg['railway_api_url']})")
    else:
        telegram_poller = TelegramPoller(cfg["telegram_bot_token"], cfg["telegram_chat_id"])
        log.info(f"Signal source: Telegram (legacy)")

    quote_poller = TSQuotePoller()
    compliance = ComplianceGate(cfg)
    nt8 = NT8Bridge(cfg["nt8_incoming_folder"], cfg["nt8_account_id"], mes_symbol)
    tracker = PositionTracker(nt8, compliance, cfg, quote_poller)

    # Auto-flatten stale positions from previous day
    if getattr(tracker, '_stale_flatten', False):
        tracker.flatten("STALE_OVERNIGHT")
        tracker._stale_flatten = False

    # Cancel orphaned pending limit order from previous session
    stale_oid = getattr(tracker, '_stale_pending_cancel_oid', None)
    if stale_oid:
        log.warning(f"Cancelling orphaned limit order: {stale_oid}")
        nt8.cancel(stale_oid)
        tracker._stale_pending_cancel_oid = None

    # ── Startup reconciliation: verify restored position is still open ──
    if tracker.is_open:
        # Layer 0: recover from mid-reverse crash (position saved but stop never placed)
        if tracker.position.get("stop_oid") is None:
            pos = tracker.position
            is_long = pos["direction"] in ("long", "bullish")
            exit_side = "SELL" if is_long else "BUY"
            stop_oid = nt8._oid("s")
            log.warning(f"CRASH RECOVERY: position has no stop order — placing now")
            nt8._write(
                f"PLACE;{nt8.account};{nt8.symbol};{exit_side};{pos['qty']};"
                f"STOPMARKET;;{_round_tick(pos['stop_price'])};DAY;;{stop_oid};;\n"
            )
            tracker.position["stop_oid"] = stop_oid
            tracker._save()
            log.info(f"  Stop placed: {stop_oid} @ {pos['stop_price']:.2f}")

            # Also place target if needed and missing
            if not pos.get("trail_only") and pos.get("target_price") and pos.get("target_oid") is None:
                time.sleep(0.3)
                target_oid = nt8._oid("t")
                nt8._write(
                    f"PLACE;{nt8.account};{nt8.symbol};{exit_side};{pos['qty']};"
                    f"LIMIT;{_round_tick(pos['target_price'])};;DAY;;{target_oid};;\n"
                )
                tracker.position["target_oid"] = target_oid
                tracker._save()
                log.info(f"  Target placed: {target_oid} @ {pos['target_price']:.2f}")

        log.info("Reconciling restored position against NT8 fills...")
        # Layer 1: check NT8 outgoing folder for fill files
        tracker.check_nt8_fills()
        # Layer 2: Railway API reconciliation DISABLED — SPX-based outcomes
        # don't match our MES position's independent trailing stop.
        # NT8 fills are the truth source for position state.
        # Layer 3: check NT8 PositionReporter for flat/position mismatch
        if tracker.is_open:
            tracker.reconcile_with_nt8()
        if tracker.is_open:
            log.info(f"Position confirmed open: {tracker.position['setup_name']} "
                     f"{tracker.position['direction']}")

    # Also check NT8 for untracked positions (we're flat but NT8 isn't)
    if not tracker.is_open:
        nt8_state = read_nt8_position(cfg["nt8_incoming_folder"])
        if nt8_state and nt8_state.get("position", "Flat") != "Flat":
            log.warning(f"[NT8] NT8 has {nt8_state['position']} {nt8_state.get('quantity', '?')} "
                        f"but eval_trader is flat — manage in NT8 or create position file")

    poll_interval = cfg.get("telegram_poll_interval_s", 2)
    log.info(f"Polling every {poll_interval}s...")
    last_trail_check = 0.0
    last_reconcile = time.time()  # don't reconcile immediately on startup
    TRAIL_CHECK_INTERVAL = 5.0  # seconds between trail/fill checks
    RECONCILE_INTERVAL = 60.0   # seconds between Railway reconciliation checks
    latest_es_price = None  # updated from each API poll

    try:
        while True:
            now_ct = datetime.now(CT)

            # Daily reset
            compliance.daily_reset()

            # ── E2T Tick Trade: count trading day even with no signals ──
            # At 15:30 ET, if no trades today and no position open, place 1 MES
            # with 2-tick TP/SL just to register the day for E2T's 10-day minimum.
            now_et = datetime.now(ET)
            if (cfg.get("tick_trade_enabled", True)
                    and now_et.time() >= TICK_TRADE_TIME_ET
                    and not compliance.tick_trade_done
                    and compliance.trades_today == 0
                    and not tracker.is_open
                    and latest_es_price):
                tick_pts = TICK_TRADE_TICKS * MES_TICK_SIZE  # 0.50 pts
                stop_px = latest_es_price - tick_pts
                target_px = latest_es_price + tick_pts
                log.info(f"TICK TRADE: No trades today, placing 1 MES BUY "
                         f"@ ~{latest_es_price:.2f} TP={target_px:.2f} SL={stop_px:.2f}")
                oids = tracker.nt8.place_bracket("long", 1, stop_px, target_px)
                # Track as position so flatten/fill detection works
                tracker.position = {
                    "setup_name": "TickTrade",
                    "direction": "long",
                    "grade": "TICK",
                    "entry_price": latest_es_price,
                    "spx_spot": latest_es_price,
                    "stop_price": stop_px,
                    "target_price": target_px,
                    "stop_pts": tick_pts,
                    "target_pts": tick_pts,
                    "trail_only": False,
                    "qty": 1,
                    "ts": datetime.now(CT).isoformat(),
                    "max_hold_min": 5,  # auto-close quickly
                    "es_entry_price": latest_es_price,
                    "be_triggered": False,
                    **oids,
                }
                tracker.compliance.has_open_position = True
                tracker._save()
                compliance.tick_trade_done = True
                compliance.save()

            # EOD flatten check
            flatten_time = datetime.strptime(cfg["flatten_time_ct"], "%H:%M").time()
            if now_ct.time() >= flatten_time and tracker.is_open:
                tracker.flatten("EOD_FLATTEN", es_price=latest_es_price)

            # Check NT8 fills + trailing stop (every 5s when position open)
            if tracker.is_open and time.time() - last_trail_check >= TRAIL_CHECK_INTERVAL:
                tracker.check_nt8_fills()
                tracker.check_trail(latest_es_price)
                last_trail_check = time.time()

            # Periodic reconciliation (every 60s when position open)
            # NT8 fill detection only — Railway outcomes disabled (SPX-based,
            # doesn't match our MES position's independent trailing stop)
            if (tracker.is_open
                    and time.time() - last_reconcile >= RECONCILE_INTERVAL):
                # NT8 position file check (fastest, no network)
                tracker.reconcile_with_nt8()
                last_reconcile = time.time()

            # ── Poll for signals and outcomes ──
            if use_api:
                new_signals, new_outcomes, poll_es_price = api_poller.poll()
                if poll_es_price:
                    latest_es_price = poll_es_price

                # Railway outcomes disabled — SPX-based outcomes don't match
                # our MES position's independent trailing stop. NT8 fills are
                # the truth source (check_nt8_fills + check_trail handle exits).
                # for outcome in new_outcomes:
                #     if tracker.is_open:
                #         tracker.close_on_outcome(outcome)

                # Process signals
                for signal in new_signals:
                    log.info(f"Signal received: {signal['setup_name']} "
                             f"{signal['direction'].upper()} [{signal.get('grade', '?')}] "
                             f"@ {signal['spot']:.2f}")
                    # Unknown setup guard: skip signals for setups not in config
                    if signal["setup_name"] not in cfg["setup_rules"]:
                        log.info(f"  SKIPPED: unknown setup '{signal['setup_name']}' (not in setup_rules)")
                        continue
                    # Staleness check: skip signals older than 2 minutes
                    sig_ts = signal.get("signal_ts")
                    if sig_ts:
                        try:
                            sig_dt = datetime.fromisoformat(sig_ts)
                            if sig_dt.tzinfo is None:
                                sig_dt = sig_dt.replace(tzinfo=ET)
                            age_s = (datetime.now(ET) - sig_dt).total_seconds()
                            if age_s > MAX_SIGNAL_AGE_S:
                                log.info(f"  SKIPPED: signal too old ({age_s:.0f}s > {MAX_SIGNAL_AGE_S}s)")
                                continue
                        except Exception:
                            pass
                    # Check for reversal: opposite-direction signal while in position
                    if tracker.is_open and tracker.is_opposite(signal):
                        # Environment override: score conviction from multiple factors
                        conviction = 1  # the opposing signal itself
                        reasons = [f"{signal['setup_name']} {signal['direction'].upper()}"]
                        pos_dir = tracker.position["direction"].lower()

                        # +1 if Greek alignment opposes current position
                        # Signal alignment is relative to signal direction (opposite to position)
                        # So alignment > 0 = Greeks support opposing signal = Greeks oppose our position
                        alignment = signal.get("greek_alignment")
                        if alignment is not None and alignment > 0:
                            conviction += 1
                            reasons.append(f"greeks={alignment:+d} vs {pos_dir.upper()}")

                        # +1 if paradigm/regime opposes current position
                        paradigm = (signal.get("paradigm") or "").upper()
                        if pos_dir in ("short", "bearish") and "GEX" in paradigm:
                            conviction += 1
                            reasons.append(f"regime=GEX vs SHORT")
                        elif pos_dir in ("long", "bullish") and "AG" in paradigm:
                            conviction += 1
                            reasons.append(f"regime=AG vs LONG")

                        log.info(f"  ENV CHECK: conviction={conviction}/{_ENV_OVERRIDE_THRESHOLD} "
                                 f"[{', '.join(reasons)}]")

                        if conviction >= _ENV_OVERRIDE_THRESHOLD:
                            # Strong environment opposition — attempt reversal
                            compliance.has_open_position = False
                            allowed, reason = compliance.check(signal)
                            compliance.has_open_position = True
                            if allowed:
                                log.info(f"  REVERSING: conviction {conviction}/3 — "
                                         f"environment opposes {pos_dir.upper()} position")
                                tracker.reverse(signal, latest_es_price)
                            else:
                                # Can't reverse (setup disabled etc.) but environment is against us
                                log.info(f"  CLOSING FLAT: conviction {conviction}/3 "
                                         f"but reverse blocked ({reason}) — flattening")
                                tracker.flatten(reason=f"env_override conviction={conviction}",
                                               es_price=latest_es_price)
                        else:
                            # Low conviction — tighten stop only
                            if latest_es_price:
                                tracker.tighten_stop(latest_es_price)
                                log.info(f"  LOW CONVICTION ({conviction}/3): tightened SL, holding position")
                            else:
                                log.warning(f"  LOW CONVICTION ({conviction}/3): no ES price, cannot tighten")
                        continue

                    # DEDUP: block if same setup+direction traded recently (deploy overlap guard)
                    dedup_key = (signal["setup_name"], signal["direction"].lower())
                    now_ts = time.time()
                    if dedup_key in _trade_dedup and (now_ts - _trade_dedup[dedup_key]) < TRADE_DEDUP_WINDOW:
                        log.info(f"  DEDUP: {signal['setup_name']} {signal['direction']} already traded "
                                 f"{now_ts - _trade_dedup[dedup_key]:.0f}s ago, skipping")
                        continue

                    allowed, reason = compliance.check(signal)
                    if not allowed:
                        log.info(f"  BLOCKED: {reason}")
                        continue
                    _trade_dedup[dedup_key] = now_ts
                    tracker.open_trade(signal)
            else:
                messages = telegram_poller.poll()
                for msg in messages:
                    text = msg["text"]
                    outcome = parse_outcome(text)
                    if outcome and tracker.is_open:
                        tracker.close_on_outcome(outcome)
                        continue
                    signal = parse_signal(text)
                    if not signal:
                        continue
                    log.info(f"Signal received: {signal['setup_name']} "
                             f"{signal['direction'].upper()} [{signal.get('grade', '?')}] "
                             f"@ {signal['spot']:.2f}")
                    allowed, reason = compliance.check(signal)
                    if not allowed:
                        log.info(f"  BLOCKED: {reason}")
                        continue
                    tracker.open_trade(signal)

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        log.info("")
        log.info("Shutting down...")
        if tracker.is_open:
            log.warning("POSITION STILL OPEN — manage manually in NinjaTrader!")
            log.warning(f"  {tracker.position['setup_name']} {tracker.position['direction']} "
                        f"@ {tracker.position['entry_price']:.2f}")
        compliance.save()
        save_config(cfg)
        log.info("State saved. Goodbye.")


def test_mode(test_dir: str = "buy"):
    """Test the full OIF pipeline with a fake signal.

    Usage: python eval_trader.py --test [buy|sell]

    Places a small test order (1 MES), monitors NT8 outgoing for fills,
    then auto-flattens after 10 seconds. Tests the entire chain:
      signal → compliance → OIF write → NT8 fill detection → position close
    """
    direction = "short" if test_dir.lower() in ("sell", "short") else "long"

    cfg = load_config()
    mes_symbol = cfg["nt8_mes_symbol"]
    if mes_symbol.lower() == "auto":
        mes_symbol = current_mes_symbol("nt8")

    nt8 = NT8Bridge(cfg["nt8_incoming_folder"], cfg["nt8_account_id"], mes_symbol)

    log.info("=" * 50)
    log.info("  TEST MODE — fake signal pipeline test")
    log.info("=" * 50)
    log.info(f"  Symbol: {mes_symbol}")
    log.info(f"  Direction: {direction.upper()}")
    log.info(f"  Qty: 1 (test)")
    log.info("")

    # Step 1: Place market entry + stop
    is_long = direction == "long"
    # Use a wide stop so it won't fill during test (50 pts away)
    fake_price = 6850.0  # doesn't matter — market order fills at current price
    stop_price = (fake_price - 50) if is_long else (fake_price + 50)

    log.info("[1/4] Placing market entry + stop via OIF...")
    oids = nt8.place_entry_and_stop(direction, 1, stop_price)
    log.info(f"  entry_oid: {oids['entry_oid']}")
    log.info(f"  stop_oid:  {oids['stop_oid']}")

    # Step 2: Wait for NT8 to process and check outgoing
    log.info("[2/4] Waiting for NT8 fill (checking outgoing folder)...")
    for i in range(20):  # wait up to 10 seconds
        time.sleep(0.5)
        entry_state = nt8.check_order_state(oids["entry_oid"])
        if entry_state:
            log.info(f"  Entry: {entry_state['status']} qty={entry_state['qty']} "
                     f"price={entry_state['price']}")
            break
    else:
        log.warning("  No entry fill detected in 10s — check NT8 manually")

    stop_state = nt8.check_order_state(oids["stop_oid"])
    if stop_state:
        log.info(f"  Stop: {stop_state['status']} "
                 f"{'@ ' + str(stop_state['price']) if stop_state['price'] else ''}")
    else:
        log.info("  Stop: pending (not yet filled — good)")

    # Step 3: Flatten
    log.info("[3/4] Flattening test position...")
    time.sleep(1)
    nt8.close_position(direction, 1)
    nt8.cancel(oids["stop_oid"])

    # Step 4: Verify close
    log.info("[4/4] Waiting for close confirmation...")
    time.sleep(2)
    stop_state2 = nt8.check_order_state(oids["stop_oid"])
    if stop_state2:
        log.info(f"  Stop after cancel: {stop_state2['status']}")

    log.info("")
    log.info("TEST COMPLETE. Check NT8 to confirm position is flat.")
    log.info("If everything worked: entry filled, stop cancelled, position closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E2T Evaluation Auto-Trader")
    parser.add_argument("--config", default="eval_trader_config.json",
                        help="Config file path (default: eval_trader_config.json)")
    parser.add_argument("--test", nargs="?", const="buy",
                        help="Test mode: place 1 MES, flatten. Optional: buy/sell (default: buy)")
    args = parser.parse_args()

    _init_file_paths(args.config)
    _acquire_singleton_lock()
    _init_log_file()

    if args.test:
        test_mode(args.test)
    else:
        main()
