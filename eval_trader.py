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

import os, sys, json, re, time, logging, calendar
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
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("eval_trader.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("eval_trader")

# ─── Constants ────────────────────────────────────────────────────────────────
MES_POINT_VALUE = 5.0   # $5 per point per MES contract
MES_TICK_SIZE = 0.25     # MES minimum price increment
MAX_SIGNAL_AGE_S = 120   # Skip signals older than 2 min (prevents stale entries after restart)
_STRONG_SETUPS = {"AG Short", "GEX Long", "Paradigm Reversal", "ES Absorption"}
_TIGHTEN_GAP_PTS = 5.0   # When DD fires vs strong: tighten SL to this many pts from spot
SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "eval_trader_config.json"
STATE_FILE = SCRIPT_DIR / "eval_trader_state.json"
POSITION_FILE = SCRIPT_DIR / "eval_trader_position.json"

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
    """Return front-month MES symbol, auto-rolling ~8 days before expiry.

    fmt="nt8"  → "MES 03-26"
    fmt="ts"   → "MESH26"
    """
    today = date.today()
    for month_num, code in _MES_MONTHS:
        year = today.year
        expiry = _third_friday(year, month_num)
        rollover = expiry - timedelta(days=8)
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

    # ── Breakeven stop ──
    "be_trigger_pts": 5.0,       # Move stop to breakeven when ES moves +5 pts

    # ── Daily loss limit (trade count) ──
    "max_losses_per_day": 3,     # Stop trading after 3 losses

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
        "GEX Long":          {"enabled": False, "stop": 8,  "target": "msg"},
        "AG Short":          {"enabled": True,  "stop": 20, "target": "msg"},
        "BofA Scalp":        {"enabled": False, "stop": 12, "target": "msg", "max_hold_min": 30},
        "ES Absorption":     {"enabled": True,  "stop": 12, "target": 10},
        "Paradigm Reversal": {"enabled": True,  "stop": 15, "target": 10},
        "DD Exhaustion":     {"enabled": True,  "stop": 12, "target": None},
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
        return SCRIPT_DIR / "eval_trader_api_state.json"

    def _load_state(self):
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
                    log.info(f"API poller state restored: last_id={self.last_id}")
                else:
                    log.info(f"API poller: new day (was {saved_date}), resetting state")
                    self._state_date = today
            except Exception:
                pass

    def _save_state(self):
        self._state_file().write_text(json.dumps({
            "date": date.today().isoformat(),
            "last_id": self.last_id,
            "seen_signals": list(self._seen_signals),
            "seen_outcomes": list(self._seen_outcomes),
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
        self.trade_days = set()
        self.has_open_position = False
        self.last_reset_date = None
        self._load()

    def _load(self):
        if STATE_FILE.exists():
            try:
                s = json.loads(STATE_FILE.read_text())
                self.daily_pnl = s.get("daily_pnl", 0.0)
                self.total_pnl = s.get("total_pnl", 0.0)
                self.trades_today = s.get("trades_today", 0)
                self.losses_today = s.get("losses_today", 0)
                self.trade_days = set(s.get("trade_days", []))
                self.last_reset_date = s.get("last_reset_date")
                self.cfg["e2t_peak_balance"] = s.get("peak_balance", self.cfg["e2t_peak_balance"])
                log.info(f"State loaded: daily=${self.daily_pnl:+.0f} total=${self.total_pnl:+.0f} "
                         f"peak=${self.cfg['e2t_peak_balance']:,.0f} losses={self.losses_today} "
                         f"days={len(self.trade_days)}")
            except Exception as e:
                log.warning(f"State load error: {e}")

    def save(self):
        s = {
            "daily_pnl": self.daily_pnl,
            "total_pnl": self.total_pnl,
            "trades_today": self.trades_today,
            "losses_today": self.losses_today,
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

        # Already in position?
        # Opposite-direction signals return "reverse" so main loop can close + reopen
        if self.has_open_position:
            return False, "already in position"

        # 3-loss daily shutoff
        max_losses = cfg.get("max_losses_per_day", 99)
        if self.losses_today >= max_losses:
            return False, f"daily loss limit reached ({self.losses_today}/{max_losses} losses)"

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

        # Trailing drawdown
        current_bal = cfg["e2t_starting_balance"] + self.total_pnl
        drawdown_floor = cfg["e2t_peak_balance"] - cfg["e2t_eod_trailing_drawdown"]
        if current_bal - potential_loss <= drawdown_floor:
            return False, (f"potential loss would breach drawdown floor "
                           f"(bal: ${current_bal:,.0f}, floor: ${drawdown_floor:,.0f})")

        # Max contracts
        max_mes = cfg["e2t_max_contracts_es_equiv"] * 10
        if qty > max_mes:
            return False, f"qty {qty} exceeds max {max_mes} MES"

        return True, "ok"

    def record_trade(self, pnl_pts: float, setup_name: str, qty: int = 0):
        """Record completed trade P&L. Uses actual trade qty for dollar calculation."""
        trade_qty = qty or self.cfg["qty"]
        pnl_dollars = pnl_pts * trade_qty * MES_POINT_VALUE
        self.daily_pnl += pnl_dollars
        self.total_pnl += pnl_dollars
        self.trades_today += 1
        if pnl_dollars < 0:
            self.losses_today += 1
        today = datetime.now(CT).strftime("%Y-%m-%d")
        self.trade_days.add(today)
        self.has_open_position = False

        current_bal = self.cfg["e2t_starting_balance"] + self.total_pnl
        log.info(f"Trade recorded: {setup_name} {pnl_pts:+.1f} pts x {trade_qty} (${pnl_dollars:+.0f})")
        log.info(f"  Daily: ${self.daily_pnl:+.0f} | Total: ${self.total_pnl:+.0f} | "
                 f"Balance: ${current_bal:,.0f} | Losses today: {self.losses_today}")
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
        try:
            self._write_seq += 1
            f = self.incoming / f"oif{int(time.time() * 1000)}_{self._write_seq}.txt"
            f.write_text(cmd)
            log.debug(f"OIF: {cmd.strip()}")
        except Exception as e:
            log.error(f"OIF write FAILED: {e}")
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

    def change_stop(self, order_id: str, new_stop_price: float, qty: int):
        """Modify an existing stop order price via OIF CHANGE command."""
        self._write(
            f"CHANGE;{self.account};{self.symbol};;{qty};"
            f"STOPMARKET;;{_round_tick(new_stop_price)};DAY;;{order_id};;\n"
        )
        log.info(f"NT8 CHANGE stop: {order_id} → {_round_tick(new_stop_price)}")

    def cancel(self, order_id: str):
        """Cancel an order by ID."""
        self._write(f"CANCEL;{self.account};{self.symbol};{order_id}\n")
        log.info(f"NT8 cancel: {order_id}")

    def close_position(self):
        """Flatten all positions for this symbol."""
        self._write(f"CLOSEPOSITION;{self.account};{self.symbol}\n")
        log.info(f"NT8 CLOSEPOSITION: {self.symbol}")

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
        rules = self.cfg["setup_rules"][name]
        is_long = direction in ("long", "bullish")

        stop_pts = rules["stop"]
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
        es_price = signal.get("es_price")
        if es_price:
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

        # Place orders in NT8
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

        # Cancel remaining orders (stop or target — whichever didn't trigger)
        if result in ("WIN", "EXPIRED"):
            # Target hit or expired — cancel stop
            self.nt8.cancel(self.position["stop_oid"])
        if result in ("LOSS", "EXPIRED"):
            # Stop hit or expired — cancel target (if exists)
            if self.position.get("target_oid"):
                self.nt8.cancel(self.position["target_oid"])
        if result == "EXPIRED" or self.position.get("trail_only"):
            # Force close any remaining position (always close for trail-only)
            self.nt8.close_position()

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
        """Tighten SL to gap_pts from current ES price instead of reversing.

        Used when DD Exhaustion fires against a strong setup — keep the strong
        setup open but move stop closer. Mirrors Railway auto_trader logic.
        """
        if not self.position:
            return
        is_long = self.position["direction"] in ("long", "bullish")
        new_stop = _round_tick(es_price - gap_pts) if is_long else _round_tick(es_price + gap_pts)
        old_stop = self.position["stop_price"]

        self.nt8.change_stop(self.position["stop_oid"], new_stop, self.position.get("qty", self.cfg["qty"]))
        self.position["stop_price"] = new_stop
        self._save()

        log.info(f"  Tightened SL: {old_stop:.2f} → {new_stop:.2f} "
                 f"({gap_pts}pts from ES {es_price:.2f}), keeping {self.position['setup_name']} open")

    def reverse(self, signal: dict, es_price: float | None):
        """Close current position and open new one in opposite direction.

        Uses CLOSEPOSITION to flatten, then cancels ALL working orders for the
        symbol before placing new ones. Sleeps between each OIF command to
        prevent file collisions and give NT8 time to process.
        """
        if not self.position:
            return

        old_name = self.position["setup_name"]
        old_dir = self.position["direction"].upper()
        new_name = signal["setup_name"]
        new_dir = signal["direction"].upper()
        trade_qty = self.position.get("qty", self.cfg["qty"])

        log.info(f"REVERSING: closing {old_name} {old_dir} for new {new_name} {new_dir}")

        # Step 1: Cancel ALL working orders for symbol (safety net — catches any orphans)
        self.nt8.cancel_all()

        # Step 2: Flatten position
        time.sleep(0.5)
        self.nt8.close_position()

        # Estimate P&L from ES price (best we can do without fill data)
        pnl_pts = 0.0
        if es_price and self.position.get("es_entry_price"):
            is_long = self.position["direction"] in ("long", "bullish")
            pnl_pts = (es_price - self.position["es_entry_price"]) if is_long else (self.position["es_entry_price"] - es_price)

        self.compliance.record_trade(pnl_pts, old_name, trade_qty)
        log.info(f"  Closed {old_name}: ~{pnl_pts:+.1f} pts (estimated from ES price)")

        self.position = None
        self._save()

        # Step 3: Wait for NT8 to finish processing close + cancels
        time.sleep(1.0)

        # Step 4: Open new position
        self.open_trade(signal)

    def flatten(self, reason: str = "EOD"):
        """Force-close position (e.g., EOD flatten)."""
        if not self.position:
            return

        trade_qty = self.position.get("qty", self.cfg["qty"])
        # Cancel all working orders, then flatten
        self.nt8.cancel_all()
        time.sleep(0.5)
        self.nt8.close_position()

        log.info(f"FLATTENED ({reason}): {self.position['setup_name']} — "
                 f"recording 0 P&L (actual may differ)")

        # Record 0 P&L for flatten (conservative — actual filled at market)
        self.compliance.record_trade(0, self.position["setup_name"], trade_qty)
        self.position = None
        self._save()

    # Trail params — mirrors Railway's _trail_params in main.py
    # DD Exhaustion: continuous trail (activation=20, gap=5)
    # GEX Long: rung-based trail (rung_start=12, step=5, lock_offset=2)
    _TRAIL_PARAMS = {
        "DD Exhaustion": {"mode": "continuous", "activation": 20, "gap": 5},
        "GEX Long":      {"mode": "rung", "rung_start": 12, "step": 5, "lock_offset": 2},
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

        if tp:
            # ── Trailing stop setups ──
            if tp["mode"] == "continuous":
                # Continuous: once max_fav >= activation, lock at max_fav - gap
                if max_fav >= tp["activation"]:
                    lock = max_fav - tp["gap"]
                    new_stop = (es_entry + lock) if is_long else (es_entry - lock)
            else:
                # Rung-based: step every N pts with lock offset
                rung_start = tp["rung_start"]
                step = tp["step"]
                lock_offset = tp["lock_offset"]
                if max_fav >= rung_start:
                    rungs_hit = int((max_fav - rung_start) / step)
                    lock = rung_start + (rungs_hit * step) - lock_offset
                    new_stop = (es_entry + lock) if is_long else (es_entry - lock)
        else:
            # ── Non-trailing setups: breakeven only ──
            be_pts = self.cfg.get("be_trigger_pts", 5.0)
            if not self.position.get("be_triggered") and profit >= be_pts:
                new_stop = es_entry
                self.position["be_triggered"] = True

        # Move stop if new level is tighter than current
        if new_stop is not None:
            current_stop = self.position["stop_price"]
            tighter = (new_stop > current_stop) if is_long else (new_stop < current_stop)
            if tighter:
                self.nt8.change_stop(self.position["stop_oid"], new_stop, qty)
                old_stop = self.position["stop_price"]
                self.position["stop_price"] = new_stop
                self._save()
                trail_type = "TRAIL" if tp else "BREAKEVEN"
                log.info(f"  {trail_type}: stop {old_stop:.2f} → {new_stop:.2f} "
                         f"(profit={profit:+.1f} max={max_fav:+.1f})")

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

        # Check if entry was rejected
        if entry_oid:
            entry_state = self.nt8.check_order_state(entry_oid)
            if entry_state and entry_state["status"] == "REJECTED":
                log.warning(f"NT8: entry REJECTED — clearing position")
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
    dd_floor = cfg["e2t_peak_balance"] - cfg["e2t_eod_trailing_drawdown"]

    log.info("=" * 60)
    log.info("  E2T EVALUATION AUTO-TRADER")
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
    log.info(f"  DD floor:  ${dd_floor:,.0f}")
    log.info(f"  Daily lim: ${cfg['e2t_daily_loss_limit']:,.0f} (buffer: ${cfg['e2t_daily_loss_buffer']:.0f})")
    log.info(f"  Max losses: {cfg.get('max_losses_per_day', 99)}/day")
    log.info(f"  BE trigger: +{cfg.get('be_trigger_pts', 5.0)} pts")
    log.info(f"  Cutoff:    {cfg['no_new_trades_after_ct']} CT | Flatten: {cfg['flatten_time_ct']} CT")
    log.info("-" * 60)

    daily_cap = cfg.get("e2t_daily_pnl_cap", 0)
    if daily_cap > 0:
        log.info(f"  P&L cap:   ${daily_cap:.0f}/day (E2T consistency rule)")

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

    poll_interval = cfg.get("telegram_poll_interval_s", 2)
    log.info(f"Polling every {poll_interval}s...")
    last_trail_check = 0.0
    TRAIL_CHECK_INTERVAL = 5.0  # seconds between trail/fill checks
    latest_es_price = None  # updated from each API poll

    try:
        while True:
            now_ct = datetime.now(CT)

            # Daily reset
            compliance.daily_reset()

            # EOD flatten check
            flatten_time = datetime.strptime(cfg["flatten_time_ct"], "%H:%M").time()
            if now_ct.time() >= flatten_time and tracker.is_open:
                tracker.flatten("EOD_FLATTEN")

            # Check NT8 fills + trailing stop (every 5s when position open)
            if tracker.is_open and time.time() - last_trail_check >= TRAIL_CHECK_INTERVAL:
                tracker.check_nt8_fills()
                tracker.check_trail(latest_es_price)
                last_trail_check = time.time()

            # ── Poll for signals and outcomes ──
            if use_api:
                new_signals, new_outcomes, poll_es_price = api_poller.poll()
                if poll_es_price:
                    latest_es_price = poll_es_price

                # Process outcomes first
                for outcome in new_outcomes:
                    if tracker.is_open:
                        tracker.close_on_outcome(outcome)

                # Process signals
                for signal in new_signals:
                    log.info(f"Signal received: {signal['setup_name']} "
                             f"{signal['direction'].upper()} [{signal.get('grade', '?')}] "
                             f"@ {signal['spot']:.2f}")
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
                        # DD vs strong setup: tighten SL instead of reversing
                        # (mirrors Railway auto_trader logic)
                        existing_name = tracker.position["setup_name"]
                        new_name = signal["setup_name"]
                        if new_name == "DD Exhaustion" and existing_name in _STRONG_SETUPS:
                            if latest_es_price:
                                tracker.tighten_stop(latest_es_price)
                                log.info(f"  DD vs {existing_name}: tightened SL, skipping DD trade")
                            else:
                                log.warning(f"  DD vs {existing_name}: no ES price, cannot tighten — skipping")
                            continue
                        # Run compliance on the new signal (skip "already in position")
                        compliance.has_open_position = False
                        allowed, reason = compliance.check(signal)
                        compliance.has_open_position = True
                        if not allowed:
                            log.info(f"  BLOCKED (reverse): {reason}")
                            continue
                        tracker.reverse(signal, latest_es_price)
                        continue

                    allowed, reason = compliance.check(signal)
                    if not allowed:
                        log.info(f"  BLOCKED: {reason}")
                        continue
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


def test_mode():
    """Test the full OIF pipeline with a fake signal.

    Usage: python eval_trader.py --test [buy|sell]

    Places a small test order (1 MES), monitors NT8 outgoing for fills,
    then auto-flattens after 10 seconds. Tests the entire chain:
      signal → compliance → OIF write → NT8 fill detection → position close
    """
    direction = "long"
    if len(sys.argv) > 2 and sys.argv[2].lower() in ("sell", "short"):
        direction = "short"

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
    nt8.close_position()
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
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_mode()
    else:
        main()
