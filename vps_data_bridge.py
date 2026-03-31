"""
vps_data_bridge.py — Sierra Chart DTC → Railway Data Bridge (Self-Healing)

Connects to Sierra Chart's DTC Protocol Server (localhost:11099) via WebSocket,
subscribes to @ES and /VX market data, builds 5-pt ES range bars with CVD,
and POSTs completed bars + VX ticks to Railway API endpoints.

Self-healing features:
  - On startup: queries Railway for last stored bar, backfills any gap from .scid files
  - During operation: detects stale data (no ticks for 5 min during market hours)
  - On reconnect: always checks for gaps before resuming live stream
  - Reads Sierra .scid binary files directly for backfill (no manual export needed)

Fully independent data pipeline — does NOT touch existing Rithmic tables.
Data goes to: vps_es_range_bars, vps_vix_ticks, vps_heartbeats.

Requirements: Python 3.10+, websocket-client, requests, pytz
Usage: python vps_data_bridge.py [--config vps_bridge_config.json]
"""

import json
import struct
import time
import threading
import logging
import argparse
import sys
import os
from datetime import datetime, timedelta, time as dtime, date
from pathlib import Path
from collections import defaultdict

import pytz
import requests

try:
    import websocket
except ImportError:
    sys.exit("pip install websocket-client")

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("vps_bridge")

ET = pytz.timezone("US/Eastern")
CT = pytz.timezone("US/Central")

# ─── DTC Message Types (Market Data) ─────────────────────────────────────────
LOGON_REQUEST = 1
LOGON_RESPONSE = 2
HEARTBEAT = 3
MARKET_DATA_REQUEST = 101
MARKET_DATA_REJECT = 103
MARKET_DATA_SNAPSHOT = 104
MARKET_DATA_UPDATE_TRADE = 107
MARKET_DATA_UPDATE_BID_ASK = 108
MARKET_DATA_UPDATE_SESSION_VOLUME = 113
MARKET_DATA_UPDATE_TRADE_WITH_UNBUNDLED_INDICATOR = 132
MARKET_DATA_UPDATE_TRADE_NO_TIMESTAMP = 142

# ─── DTC AtBidOrAsk Enum ─────────────────────────────────────────────────────
AT_BID = 1
AT_ASK = 2

# ─── Sierra SCID File Constants ──────────────────────────────────────────────
SCID_HEADER_SIZE = 56
SCID_RECORD_SIZE = 40
SC_EPOCH = datetime(1899, 12, 30)

# ─── Config Defaults ─────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "sierra_host": "127.0.0.1",
    "sierra_port": 11099,
    "railway_api_url": "https://0dtealpha.com",
    "vps_api_key": "",
    "es_symbol": "ESM26.CME",
    "vx_symbol": "VXM26.CFE",
    "es_scid_file": "C:/SierraChart/Data/ESM26-CME.scid",
    "vx_scid_file": "C:/SierraChart/Data/VXM26_FUT_CFE.scid",
    "range_pts": 5.0,
    "vx_batch_seconds": 10,
    "heartbeat_seconds": 60,
    "post_timeout": 10,
    "stale_timeout_minutes": 5,     # Reconnect if no ticks for this long during market hours
}

# ─── ES Symbol Helper ────────────────────────────────────────────────────────
_ES_MONTHS = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]

def _third_friday(year, month):
    import calendar
    cal = calendar.monthcalendar(year, month)
    fridays = [w[calendar.FRIDAY] for w in cal if w[calendar.FRIDAY] != 0]
    return date(year, month, fridays[2])

def current_es_symbol() -> str:
    from datetime import timedelta
    today = date.today()
    for month_num, code in _ES_MONTHS:
        year = today.year
        expiry = _third_friday(year, month_num)
        rollover = expiry - timedelta(days=8)
        if today <= rollover:
            return f"ES{code}{year % 10}.CME"
    year = today.year + 1
    return f"ESH{year % 10}.CME"

def current_vx_symbol() -> str:
    from datetime import timedelta
    today = date.today()
    for month_num, code in _ES_MONTHS:
        year = today.year
        expiry = _third_friday(year, month_num)
        rollover = expiry - timedelta(days=14)
        if today <= rollover:
            return f"VX{code}{year % 10}.CFE"
    year = today.year + 1
    return f"VXH{year % 10}.CFE"


# ─── Session Helpers ─────────────────────────────────────────────────────────

def _now_et():
    return datetime.now(ET)

def _es_session_date():
    t = _now_et()
    if t.hour >= 18:
        return (t + timedelta(days=1)).strftime("%Y-%m-%d")
    return t.strftime("%Y-%m-%d")

def _session_date_from_ts(ts):
    """Session date from a naive UTC timestamp. Converts to ET first."""
    ts_et = pytz.utc.localize(ts).astimezone(ET).replace(tzinfo=None)
    if ts_et.hour >= 18:
        return (ts_et + timedelta(days=1)).date()
    return ts_et.date()

def _market_open():
    t = _now_et()
    wd = t.weekday()
    hour = t.hour
    if wd == 5:
        return False
    if wd == 6:
        return hour >= 18
    if wd == 4:
        return hour < 17
    return not (hour == 17)


# ─── Trade Classification ────────────────────────────────────────────────────

def _classify_trade(at_bid_or_ask, price, bid, ask, volume):
    if at_bid_or_ask == AT_ASK:
        return volume, 0, volume
    if at_bid_or_ask == AT_BID:
        return 0, volume, -volume
    if bid and ask and bid < ask:
        if price >= ask:
            return volume, 0, volume
        if price <= bid:
            return 0, volume, -volume
        mid = (bid + ask) / 2.0
        if price >= mid:
            return volume, 0, volume
        return 0, volume, -volume
    return 0, 0, 0

def _classify_scid(bid_vol, ask_vol, volume):
    """Classify from SCID BidVolume/AskVolume fields."""
    if ask_vol > 0 and bid_vol == 0:
        return volume, 0, volume
    if bid_vol > 0 and ask_vol == 0:
        return 0, volume, -volume
    return ask_vol, bid_vol, ask_vol - bid_vol


# ─── Range Bar Builder ───────────────────────────────────────────────────────

class RangeBarBuilder:
    def __init__(self, range_pts=5.0):
        self.range_pts = range_pts
        self.cvd = 0
        self.bar_idx = 0
        self.forming_bar = None
        self.completed_bars = []

    def reset_session(self, start_idx=0, start_cvd=0):
        self.cvd = start_cvd
        self.bar_idx = start_idx
        self.forming_bar = None
        self.completed_bars = []

    def _new_bar(self, price, ts):
        return {
            "open": price, "high": price, "low": price, "close": price,
            "volume": 0, "buy": 0, "sell": 0, "delta": 0,
            "ts_start": ts, "ts_end": ts,
            "cvd_open": self.cvd, "cvd_high": self.cvd, "cvd_low": self.cvd,
        }

    def process_tick(self, price, volume, buy_vol, sell_vol, delta, ts):
        """Process one trade tick. Returns completed bar dict if bar closed, else None."""
        if self.forming_bar is None:
            self.forming_bar = self._new_bar(price, ts)

        bar = self.forming_bar
        bar["close"] = price
        bar["high"] = max(bar["high"], price)
        bar["low"] = min(bar["low"], price)
        bar["volume"] += volume
        bar["buy"] += buy_vol
        bar["sell"] += sell_vol
        bar["delta"] += delta
        bar["ts_end"] = ts

        self.cvd += delta
        bar["cvd_high"] = max(bar["cvd_high"], self.cvd)
        bar["cvd_low"] = min(bar["cvd_low"], self.cvd)

        if bar["high"] - bar["low"] >= self.range_pts - 0.001:
            completed = {
                "idx": self.bar_idx,
                "open": bar["open"], "high": bar["high"],
                "low": bar["low"], "close": bar["close"],
                "volume": bar["volume"], "delta": bar["delta"],
                "buy_volume": bar["buy"], "sell_volume": bar["sell"],
                "cvd": self.cvd,
                "cvd_open": bar["cvd_open"],
                "cvd_high": bar["cvd_high"],
                "cvd_low": bar["cvd_low"],
                "cvd_close": self.cvd,
                "ts_start": bar["ts_start"], "ts_end": bar["ts_end"],
                "status": "closed",
            }
            self.completed_bars.append(completed)
            self.bar_idx += 1
            self.forming_bar = self._new_bar(price, ts)
            return completed
        return None

    def force_close(self):
        """Force close forming bar (session end)."""
        bar = self.forming_bar
        if bar and bar["volume"] > 0:
            completed = {
                "idx": self.bar_idx,
                "open": bar["open"], "high": bar["high"],
                "low": bar["low"], "close": bar["close"],
                "volume": bar["volume"], "delta": bar["delta"],
                "buy_volume": bar["buy"], "sell_volume": bar["sell"],
                "cvd": self.cvd,
                "cvd_open": bar["cvd_open"],
                "cvd_high": bar["cvd_high"],
                "cvd_low": bar["cvd_low"],
                "cvd_close": self.cvd,
                "ts_start": bar["ts_start"], "ts_end": bar["ts_end"],
                "status": "closed",
            }
            self.completed_bars.append(completed)
            self.bar_idx += 1
            self.forming_bar = None
            return completed
        return None


# ─── SCID File Reader ────────────────────────────────────────────────────────

def read_scid_ticks(filepath, since_ts=None):
    """Generator yielding ticks from a Sierra .scid file.

    Args:
        filepath: Path to .scid file
        since_ts: Only yield ticks after this datetime (naive, exchange time)

    Yields: dict with {ts, price, volume, bid_vol, ask_vol}
    """
    filepath = Path(filepath)
    if not filepath.exists():
        log.warning(f"SCID file not found: {filepath}")
        return

    file_size = os.path.getsize(filepath)
    num_records = (file_size - SCID_HEADER_SIZE) // SCID_RECORD_SIZE
    log.info(f"Reading {filepath.name}: {num_records:,} records")

    with open(filepath, 'rb') as f:
        f.seek(SCID_HEADER_SIZE)
        count = 0

        for i in range(num_records):
            data = f.read(SCID_RECORD_SIZE)
            if len(data) < SCID_RECORD_SIZE:
                break

            dt_int = struct.unpack_from('<q', data, 0)[0]
            if dt_int <= 0:
                continue

            # Sierra .scid timestamps are in exchange local time (ET), NOT UTC.
            # Convert to UTC for consistency with Rithmic data.
            ts_naive = SC_EPOCH + timedelta(microseconds=dt_int)
            ts_et = ET.localize(ts_naive)
            ts = ts_et.astimezone(pytz.utc).replace(tzinfo=None)  # naive UTC

            if since_ts and ts <= since_ts:
                continue

            _, h, l, c = struct.unpack_from('<ffff', data, 8)
            _, vol, bid_vol, ask_vol = struct.unpack_from('<IIII', data, 24)

            price = h if h > 0 else c
            if price <= 0 or vol <= 0:
                continue

            count += 1
            if count % 2_000_000 == 0:
                log.info(f"  SCID read: {count:,} ticks @ {ts}")

            yield {"ts": ts, "price": price, "volume": vol,
                   "bid_vol": bid_vol, "ask_vol": ask_vol}

    log.info(f"  SCID done: {count:,} ticks read")


# ─── Railway Poster ──────────────────────────────────────────────────────────

class RailwayPoster:
    def __init__(self, base_url, api_key, timeout=10):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._session = requests.Session()
        if api_key:
            self._session.headers["Authorization"] = f"Bearer {api_key}"
        self._session.headers["Content-Type"] = "application/json"

    def post_es_bar(self, bar: dict, trade_date: str):
        payload = {**bar, "trade_date": trade_date}
        return self._post("/api/vps/es/bar", payload)

    def post_vx_ticks(self, ticks: list):
        if not ticks:
            return True
        return self._post("/api/vps/vix/ticks", {"ticks": ticks})

    def post_heartbeat(self, status: dict):
        return self._post("/api/vps/heartbeat", status)

    def get_last_es_bar(self):
        """Query Railway for the last stored ES bar timestamp and bar_idx."""
        url = f"{self.base_url}/api/vps/es/last"
        try:
            r = self._session.get(url, timeout=self.timeout)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log.warning(f"GET /api/vps/es/last failed: {e}")
        return None

    def get_last_vx_tick(self):
        """Query Railway for the last stored VX tick timestamp."""
        url = f"{self.base_url}/api/vps/vix/last"
        try:
            r = self._session.get(url, timeout=self.timeout)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log.warning(f"GET /api/vps/vix/last failed: {e}")
        return None

    def _post(self, path, payload):
        url = f"{self.base_url}{path}"
        for attempt in range(3):
            try:
                r = self._session.post(url, json=payload, timeout=self.timeout)
                if r.status_code == 200:
                    return True
                log.warning(f"POST {path} -> {r.status_code}: {r.text[:200]}")
            except Exception as e:
                log.warning(f"POST {path} attempt {attempt+1} failed: {e}")
                time.sleep(1)
        return False


# ─── Gap Detector & Backfiller ───────────────────────────────────────────────

class GapBackfiller:
    """Detects gaps in Railway data and backfills from Sierra .scid files."""

    def __init__(self, poster: RailwayPoster, cfg: dict):
        self.poster = poster
        self.cfg = cfg
        self.range_pts = cfg.get("range_pts", 5.0)

    def check_and_fill_es(self):
        """Check for ES bar gaps and backfill from .scid if needed."""
        scid_path = self.cfg.get("es_scid_file", "")
        if not scid_path or not Path(scid_path).exists():
            log.warning(f"ES SCID file not found: {scid_path}")
            return 0

        # Get last bar from Railway
        last = self.poster.get_last_es_bar()
        if last and last.get("ts_end"):
            last_ts_str = last["ts_end"]
            # Parse ISO timestamp
            last_ts = datetime.fromisoformat(last_ts_str.replace("Z", "+00:00"))
            if last_ts.tzinfo:
                last_ts = last_ts.replace(tzinfo=None)
            log.info(f"Last ES bar in Railway: idx={last.get('bar_idx')} ts_end={last_ts}")
        else:
            last_ts = datetime(2026, 1, 1)  # No data — backfill from Jan 1
            log.info("No ES bars in Railway — backfilling from 2026-01-01")

        # Read .scid ticks after last_ts and build bars
        builder = RangeBarBuilder(self.range_pts)
        bars_to_upload = []  # (session_date_str, bar_dict)
        current_session = None

        for tick in read_scid_ticks(scid_path, since_ts=last_ts):
            session = _session_date_from_ts(tick["ts"])

            # Session rollover
            if session != current_session:
                if current_session is not None:
                    bar = builder.force_close()
                    if bar:
                        bars_to_upload.append((str(current_session), bar))
                current_session = session
                builder.reset_session()

            buy_vol, sell_vol, delta = _classify_scid(
                tick["bid_vol"], tick["ask_vol"], tick["volume"]
            )
            completed = builder.process_tick(
                tick["price"], tick["volume"],
                buy_vol, sell_vol, delta,
                tick["ts"].isoformat(),
            )
            if completed:
                bars_to_upload.append((str(current_session), completed))

        # Force close last forming bar
        bar = builder.force_close()
        if bar and current_session:
            bars_to_upload.append((str(current_session), bar))

        if not bars_to_upload:
            log.info("ES: No gap detected — data is up to date")
            return 0

        log.info(f"ES: Backfilling {len(bars_to_upload)} bars...")

        uploaded = 0
        for trade_date, bar in bars_to_upload:
            ok = self.poster.post_es_bar(bar, trade_date)
            if ok:
                uploaded += 1
            if uploaded % 100 == 0 and uploaded > 0:
                log.info(f"  ES backfill: {uploaded}/{len(bars_to_upload)} bars...")

        log.info(f"ES: Backfilled {uploaded} bars")
        return uploaded

    def check_and_fill_vx(self):
        """Check for VX tick gaps and backfill from .scid if needed."""
        scid_path = self.cfg.get("vx_scid_file", "")
        if not scid_path or not Path(scid_path).exists():
            log.warning(f"VX SCID file not found: {scid_path}")
            return 0

        # Get last VX tick from Railway
        last = self.poster.get_last_vx_tick()
        if last and last.get("ts"):
            last_ts_str = last["ts"]
            last_ts = datetime.fromisoformat(last_ts_str.replace("Z", "+00:00"))
            if last_ts.tzinfo:
                last_ts = last_ts.replace(tzinfo=None)
            log.info(f"Last VX tick in Railway: ts={last_ts}")
        else:
            last_ts = datetime(2026, 1, 1)
            log.info("No VX ticks in Railway — backfilling from 2026-01-01")

        # Read .scid ticks after last_ts
        batch = []
        total = 0

        for tick in read_scid_ticks(scid_path, since_ts=last_ts):
            buy_vol, sell_vol, delta = _classify_scid(
                tick["bid_vol"], tick["ask_vol"], tick["volume"]
            )
            batch.append({
                "price": round(tick["price"], 2),
                "volume": tick["volume"],
                "delta": delta,
                "bid": None,
                "ask": None,
                "ts": tick["ts"].isoformat(),
            })

            if len(batch) >= 500:
                self.poster.post_vx_ticks(batch)
                total += len(batch)
                batch = []
                if total % 5000 == 0:
                    log.info(f"  VX backfill: {total} ticks...")

        if batch:
            self.poster.post_vx_ticks(batch)
            total += len(batch)

        if total == 0:
            log.info("VX: No gap detected — data is up to date")
        else:
            log.info(f"VX: Backfilled {total} ticks")
        return total


# ─── DTC Bridge ──────────────────────────────────────────────────────────────

class VPSDataBridge:
    """Main bridge: Sierra DTC → Range Bars + VX Ticks → Railway.
    Self-healing: detects gaps, backfills from .scid, monitors staleness.
    """

    PRICE_MULT = 100

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.host = cfg["sierra_host"]
        self.port = cfg["sierra_port"]
        self.es_symbol = cfg.get("es_symbol") or current_es_symbol()
        self.vx_symbol = cfg.get("vx_symbol") or current_vx_symbol()

        self._ws = None
        self._connected = False
        self._running = False
        self._lock = threading.Lock()

        self._es_symbol_id = 1
        self._vx_symbol_id = 2

        self._es_bid = 0.0
        self._es_ask = 0.0
        self._vx_bid = 0.0
        self._vx_ask = 0.0

        self.bar_builder = RangeBarBuilder(cfg.get("range_pts", 5.0))

        self._vx_ticks = []
        self._vx_last_flush = time.time()

        self.poster = RailwayPoster(
            cfg["railway_api_url"],
            cfg.get("vps_api_key", ""),
            cfg.get("post_timeout", 10),
        )

        self.backfiller = GapBackfiller(self.poster, cfg)

        # Stats
        self._es_tick_count = 0
        self._vx_tick_count = 0
        self._bars_posted = 0
        self._vx_batches_posted = 0
        self._last_heartbeat = 0
        self._session_date = None
        self._last_es_tick_time = time.time()
        self._last_vx_tick_time = time.time()
        self._stale_timeout = cfg.get("stale_timeout_minutes", 5) * 60
        self._backfill_count = 0

    # ── Gap Detection & Backfill ──────────────────────────────────────────────

    def backfill_gaps(self):
        """Check for gaps and backfill from .scid files. Called on startup and reconnect."""
        log.info("=" * 40)
        log.info("  CHECKING FOR DATA GAPS")
        log.info("=" * 40)

        es_filled = self.backfiller.check_and_fill_es()
        vx_filled = self.backfiller.check_and_fill_vx()

        self._backfill_count += 1

        if es_filled or vx_filled:
            log.info(f"Backfill complete: {es_filled} ES bars, {vx_filled} VX ticks")
        else:
            log.info("No gaps found — all data up to date")

        return es_filled, vx_filled

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self):
        url = f"ws://{self.host}:{self.port}"
        log.info(f"Connecting to Sierra DTC: {url}")

        self._ws = websocket.create_connection(url, timeout=15)

        self._send({
            "Type": LOGON_REQUEST,
            "ProtocolVersion": 8,
            "Username": "",
            "Password": "",
            "HeartbeatIntervalInSeconds": 30,
            "ClientName": "VPSDataBridge",
            "TradeMode": 0,
        })

        logon = self._recv_one()
        if logon.get("Type") != LOGON_RESPONSE:
            raise ConnectionError(f"Expected LOGON_RESPONSE, got: {logon}")
        log.info(f"DTC logon OK: {logon.get('ServerName', 'unknown')}")

        self._connected = True
        self._running = True

        # Subscribe ES
        self._send({
            "Type": MARKET_DATA_REQUEST,
            "RequestAction": 1,
            "SymbolID": self._es_symbol_id,
            "Symbol": self.es_symbol,
            "Exchange": "",
        })
        log.info(f"Subscribed to ES: {self.es_symbol}")

        # Subscribe VX
        self._send({
            "Type": MARKET_DATA_REQUEST,
            "RequestAction": 1,
            "SymbolID": self._vx_symbol_id,
            "Symbol": self.vx_symbol,
            "Exchange": "",
        })
        log.info(f"Subscribed to VX: {self.vx_symbol}")

        # Start receiver
        self._recv_thread = threading.Thread(target=self._receiver_loop, daemon=True)
        self._recv_thread.start()

        self._session_date = _es_session_date()
        self._last_es_tick_time = time.time()
        self._last_vx_tick_time = time.time()
        log.info(f"Session date: {self._session_date}")

    def disconnect(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        log.info("Disconnected from Sierra DTC")

    # ── DTC Messaging ─────────────────────────────────────────────────────────

    def _send(self, msg: dict):
        raw = json.dumps(msg) + '\x00'
        self._ws.send(raw)

    def _recv_one(self) -> dict:
        data = self._ws.recv()
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        for part in data.split('\x00'):
            if part.strip():
                return json.loads(part)
        return {}

    def _receiver_loop(self):
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
                    log.error("DTC WebSocket closed unexpectedly")
                break
            except Exception as e:
                if self._running:
                    log.error(f"DTC receiver error: {e}")
                    time.sleep(1)

    # ── Message Handling ──────────────────────────────────────────────────────

    def _handle_message(self, msg: dict):
        t = msg.get("Type")

        if t == HEARTBEAT:
            self._send({"Type": HEARTBEAT})
        elif t == MARKET_DATA_SNAPSHOT:
            self._handle_snapshot(msg)
        elif t in (MARKET_DATA_UPDATE_TRADE,
                   MARKET_DATA_UPDATE_TRADE_WITH_UNBUNDLED_INDICATOR,
                   MARKET_DATA_UPDATE_TRADE_NO_TIMESTAMP):
            self._handle_trade(msg)
        elif t == MARKET_DATA_UPDATE_BID_ASK:
            self._handle_bid_ask(msg)
        elif t == MARKET_DATA_REJECT:
            log.error(f"Market data rejected: {msg.get('RejectText', 'unknown')}")

    def _handle_snapshot(self, msg: dict):
        sid = msg.get("SymbolID")
        if sid == self._es_symbol_id:
            price = msg.get("LastTradePrice", 0) / self.PRICE_MULT
            self._es_bid = msg.get("BidPrice", 0) / self.PRICE_MULT
            self._es_ask = msg.get("AskPrice", 0) / self.PRICE_MULT
            log.info(f"ES snapshot: last={price:.2f} bid={self._es_bid:.2f} ask={self._es_ask:.2f}")
        elif sid == self._vx_symbol_id:
            price = msg.get("LastTradePrice", 0) / self.PRICE_MULT
            self._vx_bid = msg.get("BidPrice", 0) / self.PRICE_MULT
            self._vx_ask = msg.get("AskPrice", 0) / self.PRICE_MULT
            log.info(f"VX snapshot: last={price:.2f} bid={self._vx_bid:.2f} ask={self._vx_ask:.2f}")

    def _handle_trade(self, msg: dict):
        sid = msg.get("SymbolID")
        price_raw = msg.get("Price", 0)
        volume = msg.get("Volume", 0)
        at_bid_or_ask = msg.get("AtBidOrAsk", 0)
        raw_ts = msg.get("DateTime", "")

        if not volume or not price_raw:
            return

        # Sierra sends DateTime in ET (exchange time). Convert to UTC for DB consistency.
        if raw_ts:
            try:
                naive = datetime.fromisoformat(raw_ts) if isinstance(raw_ts, str) else raw_ts
                if not hasattr(naive, 'tzinfo') or naive.tzinfo is None:
                    ts = ET.localize(naive).astimezone(pytz.utc).isoformat()
                else:
                    ts = naive.astimezone(pytz.utc).isoformat()
            except Exception:
                ts = datetime.now(pytz.utc).isoformat()
        else:
            ts = datetime.now(pytz.utc).isoformat()

        price = price_raw / self.PRICE_MULT

        if sid == self._es_symbol_id:
            self._handle_es_trade(price, volume, at_bid_or_ask, ts)
        elif sid == self._vx_symbol_id:
            self._handle_vx_trade(price, volume, at_bid_or_ask, ts)

    def _handle_bid_ask(self, msg: dict):
        sid = msg.get("SymbolID")
        bid = msg.get("BidPrice", 0) / self.PRICE_MULT
        ask = msg.get("AskPrice", 0) / self.PRICE_MULT
        if sid == self._es_symbol_id:
            if bid > 0: self._es_bid = bid
            if ask > 0: self._es_ask = ask
        elif sid == self._vx_symbol_id:
            if bid > 0: self._vx_bid = bid
            if ask > 0: self._vx_ask = ask

    def _handle_es_trade(self, price, volume, at_bid_or_ask, ts):
        self._es_tick_count += 1
        self._last_es_tick_time = time.time()

        new_date = _es_session_date()
        if new_date != self._session_date:
            log.info(f"Session rollover: {self._session_date} -> {new_date}")
            self.bar_builder.reset_session()
            self._session_date = new_date

        buy_vol, sell_vol, delta = _classify_trade(
            at_bid_or_ask, price, self._es_bid, self._es_ask, volume
        )
        completed = self.bar_builder.process_tick(
            price, volume, buy_vol, sell_vol, delta, ts
        )

        if completed:
            log.info(
                f"ES bar #{completed['idx']}: "
                f"O={completed['open']:.2f} H={completed['high']:.2f} "
                f"L={completed['low']:.2f} C={completed['close']:.2f} "
                f"vol={completed['volume']} delta={completed['delta']:+d} "
                f"cvd={completed['cvd']:+d}"
            )
            threading.Thread(
                target=self.poster.post_es_bar,
                args=(completed, self._session_date),
                daemon=True,
            ).start()
            self._bars_posted += 1

    def _handle_vx_trade(self, price, volume, at_bid_or_ask, ts):
        self._vx_tick_count += 1
        self._last_vx_tick_time = time.time()

        buy_vol, sell_vol, delta = _classify_trade(
            at_bid_or_ask, price, self._vx_bid, self._vx_ask, volume
        )

        with self._lock:
            self._vx_ticks.append({
                "price": price,
                "volume": volume,
                "delta": delta,
                "bid": self._vx_bid,
                "ask": self._vx_ask,
                "ts": ts,
            })

    # ── Stale Data Detection ─────────────────────────────────────────────────

    def _check_stale(self):
        """Returns True if data is stale (no ticks during market hours)."""
        if not _market_open():
            return False
        now = time.time()
        es_stale = (now - self._last_es_tick_time) > self._stale_timeout
        if es_stale:
            log.warning(
                f"STALE: No ES ticks for {(now - self._last_es_tick_time)/60:.1f} min"
            )
        return es_stale

    # ── Main Loop ─────────────────────────────────────────────────────────────

    def run_forever(self):
        log.info("Bridge running 24/7. Ctrl+C to stop.")
        vx_interval = self.cfg.get("vx_batch_seconds", 10)
        hb_interval = self.cfg.get("heartbeat_seconds", 60)

        while self._running:
            try:
                now = time.time()

                # Flush VX ticks
                if now - self._vx_last_flush >= vx_interval:
                    self._flush_vx_ticks()
                    self._vx_last_flush = now

                # Heartbeat
                if now - self._last_heartbeat >= hb_interval:
                    self._send_heartbeat()
                    self._last_heartbeat = now

                # Stale detection — triggers reconnect
                if self._check_stale():
                    log.warning("Stale data detected — triggering reconnect + backfill")
                    self._running = False
                    raise ConnectionError("Stale data — reconnecting")

                time.sleep(1)

            except KeyboardInterrupt:
                log.info("Shutting down...")
                self._running = False
                break

    def _flush_vx_ticks(self):
        with self._lock:
            if not self._vx_ticks:
                return
            batch = self._vx_ticks.copy()
            self._vx_ticks.clear()

        ok = self.poster.post_vx_ticks(batch)
        if ok:
            self._vx_batches_posted += 1

    def _send_heartbeat(self):
        forming = self.bar_builder.forming_bar
        status = {
            "component": "vps_data_bridge",
            "ts": datetime.now(ET).isoformat(),
            "session_date": self._session_date,
            "es_symbol": self.es_symbol,
            "vx_symbol": self.vx_symbol,
            "es_ticks": self._es_tick_count,
            "vx_ticks": self._vx_tick_count,
            "bars_completed": self.bar_builder.bar_idx,
            "bars_posted": self._bars_posted,
            "vx_batches_posted": self._vx_batches_posted,
            "market_open": _market_open(),
            "backfill_count": self._backfill_count,
            "forming_bar": {
                "open": forming["open"],
                "high": forming["high"],
                "low": forming["low"],
                "close": forming["close"],
                "volume": forming["volume"],
                "range": round(forming["high"] - forming["low"], 2),
            } if forming else None,
        }
        self.poster.post_heartbeat(status)
        log.info(
            f"Heartbeat: ES ticks={self._es_tick_count} "
            f"bars={self.bar_builder.bar_idx} "
            f"VX ticks={self._vx_tick_count} "
            f"market={'OPEN' if _market_open() else 'CLOSED'}"
        )


# ─── Main ────────────────────────────────────────────────────────────────────

def load_config(path: str = None) -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if path and Path(path).exists():
        with open(path) as f:
            user_cfg = json.load(f)
        cfg.update(user_cfg)
    return cfg


def main():
    parser = argparse.ArgumentParser(description="VPS Data Bridge: Sierra DTC -> Railway (Self-Healing)")
    parser.add_argument("--config", default="vps_bridge_config.json",
                        help="Config file path")
    parser.add_argument("--test", action="store_true",
                        help="Connect, print first 10 ticks, then exit")
    parser.add_argument("--backfill-only", action="store_true",
                        help="Only run backfill (no live streaming)")
    args = parser.parse_args()

    cfg = load_config(args.config)

    if not cfg.get("es_symbol"):
        cfg["es_symbol"] = current_es_symbol()
    if not cfg.get("vx_symbol"):
        cfg["vx_symbol"] = current_vx_symbol()

    log.info("=" * 50)
    log.info("  VPS DATA BRIDGE (Self-Healing)")
    log.info("=" * 50)
    log.info(f"  DTC:       {cfg['sierra_host']}:{cfg['sierra_port']}")
    log.info(f"  ES:        {cfg['es_symbol']}")
    log.info(f"  VX:        {cfg['vx_symbol']}")
    log.info(f"  ES SCID:   {cfg.get('es_scid_file', 'N/A')}")
    log.info(f"  VX SCID:   {cfg.get('vx_scid_file', 'N/A')}")
    log.info(f"  Railway:   {cfg['railway_api_url']}")
    log.info(f"  Range:     {cfg['range_pts']}pt bars")
    log.info(f"  Stale:     {cfg.get('stale_timeout_minutes', 5)} min")
    log.info("=" * 50)

    # ── Reconnect loop with backfill ──
    while True:
        bridge = VPSDataBridge(cfg)

        try:
            # Step 1: Always backfill gaps first
            bridge.backfill_gaps()

            if args.backfill_only:
                log.info("Backfill-only mode — exiting")
                return

            # Step 2: Connect to DTC for live streaming
            bridge.connect()

            if args.test:
                log.info("Test mode: waiting 30s for ticks...")
                time.sleep(30)
                log.info(f"ES ticks: {bridge._es_tick_count}, "
                         f"VX ticks: {bridge._vx_tick_count}, "
                         f"Bars: {bridge.bar_builder.bar_idx}")
                bridge.disconnect()
                return

            # Step 3: Stream live data
            bridge.run_forever()

        except KeyboardInterrupt:
            bridge.disconnect()
            break
        except Exception as e:
            log.error(f"Bridge error: {e}")
            try:
                bridge.disconnect()
            except Exception:
                pass

            # Step 4: On any disconnect/error — wait, then loop back to Step 1
            # (which will backfill any gap before resuming live)
            log.info("Reconnecting in 10s (will backfill gaps first)...")
            time.sleep(10)


if __name__ == "__main__":
    main()
