"""
vps_data_bridge.py — Sierra Chart SCID → Railway Data Bridge (Self-Healing)

Tails Sierra Chart's .scid binary data files in real-time, builds 5-pt ES
range bars with CVD, and POSTs completed bars + VX ticks to Railway API.

Sierra's DTC Protocol Server blocks market data (exchange rules, since v2351).
Trading via DTC still works (sierra_bridge.py). This bridge reads .scid files
directly — Sierra writes them every ~5s, shared file access is allowed.

Self-healing features:
  - On startup: queries Railway for last stored bar, backfills any gap from .scid
  - During operation: tails .scid files every 2s for new ticks
  - Detects stale data (file not growing for 5 min during market hours)
  - Telegram alerts on stale (3 cycles) and reconnect (5 cycles)

Fully independent data pipeline — does NOT touch existing Rithmic tables.
Data goes to: vps_es_range_bars, vps_vix_ticks, vps_heartbeats.

Requirements: Python 3.10+, requests, pytz
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

import pytz
import requests

# ─── Logging ─────────────────────────────────────────────────────────────────
from logging.handlers import RotatingFileHandler

_LOG_DIR = Path(__file__).parent / "logs"
_LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("vps_bridge")

# File handler — 5 MB rotating, 3 backups
_file_handler = RotatingFileHandler(
    _LOG_DIR / "vps_bridge.log", maxBytes=5 * 1024 * 1024, backupCount=3,
    encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
log.addHandler(_file_handler)

ET = pytz.timezone("US/Eastern")
CT = pytz.timezone("US/Central")

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
    "es_symbol": "ESM26-CME",
    "vx_symbol": "VXM26_FUT_CFE",
    "es_scid_file": "C:/SierraChart/Data/ESM26-CME.scid",
    "vx_scid_file": "C:/SierraChart/Data/VXM26_FUT_CFE.scid",
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "range_pts": 5.0,
    "range_pts_10": 10.0,           # Phase 1: parallel 10-pt builder for SB10 Absorption parity
    "vx_batch_seconds": 10,
    "heartbeat_seconds": 60,
    "post_timeout": 10,
    "stale_timeout_minutes": 5,     # Reconnect if no ticks for this long during market hours
    "vol_signal_file": "C:/SierraChart/Data/vol_signal.txt",
    "vol_signal_poll_seconds": 2,
}

# ─── ES Symbol Helper ────────────────────────────────────────────────────────
_ES_MONTHS = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]

def _third_friday(year, month):
    import calendar
    cal = calendar.monthcalendar(year, month)
    fridays = [w[calendar.FRIDAY] for w in cal if w[calendar.FRIDAY] != 0]
    return date(year, month, fridays[2])

def current_es_symbol() -> str:
    """Sierra DTC symbol format: ESM26-CME (2-digit year, dash separator)."""
    from datetime import timedelta
    today = date.today()
    for month_num, code in _ES_MONTHS:
        year = today.year
        expiry = _third_friday(year, month_num)
        rollover = expiry - timedelta(days=8)
        if today <= rollover:
            return f"ES{code}{year % 100}-CME"
    year = today.year + 1
    return f"ESH{year % 100}-CME"

def current_vx_symbol() -> str:
    """Sierra DTC symbol format: VXM26_FUT_CFE (2-digit year, underscore, FUT suffix)."""
    from datetime import timedelta
    today = date.today()
    for month_num, code in _ES_MONTHS:
        year = today.year
        expiry = _third_friday(year, month_num)
        rollover = expiry - timedelta(days=14)
        if today <= rollover:
            return f"VX{code}{year % 100}_FUT_CFE"
    year = today.year + 1
    return f"VXH{year % 100}_FUT_CFE"


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

def _send_telegram(cfg, message):
    """Send a Telegram alert. Silent fail if not configured."""
    token = cfg.get("telegram_bot_token", "")
    chat_id = cfg.get("telegram_chat_id", "")
    if not token or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": f"🖥 VPS Bridge: {message}"},
                      timeout=5)
    except Exception as e:
        log.warning(f"Telegram alert failed: {e}")


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
        # Binary search to skip ahead if since_ts is set (avoids scanning millions of records)
        start_record = 0
        if since_ts and num_records > 1000:
            lo, hi = 0, num_records - 1
            # Convert since_ts (naive UTC) back to ET microseconds for comparison
            since_et = pytz.utc.localize(since_ts).astimezone(ET).replace(tzinfo=None)
            target_us = int((since_et - SC_EPOCH).total_seconds() * 1_000_000)
            while lo < hi:
                mid = (lo + hi) // 2
                f.seek(SCID_HEADER_SIZE + mid * SCID_RECORD_SIZE)
                data = f.read(SCID_RECORD_SIZE)
                dt_int = struct.unpack_from('<q', data, 0)[0]
                if dt_int <= target_us:
                    lo = mid + 1
                else:
                    hi = mid
            start_record = max(0, lo - 10)  # small buffer
            log.info(f"  Binary search: skipping to record {start_record:,} / {num_records:,}")

        f.seek(SCID_HEADER_SIZE + start_record * SCID_RECORD_SIZE)
        count = 0

        for i in range(start_record, num_records):
            data = f.read(SCID_RECORD_SIZE)
            if len(data) < SCID_RECORD_SIZE:
                break

            dt_int = struct.unpack_from('<q', data, 0)[0]
            if dt_int <= 0:
                continue

            # Sierra .scid timestamps are in UTC (after Sierra timezone fix).
            ts = SC_EPOCH + timedelta(microseconds=dt_int)

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

    def post_es_bar(self, bar: dict, trade_date: str, range_pts: float = 5.0):
        payload = {**bar, "trade_date": trade_date, "range_pts": range_pts}
        return self._post("/api/vps/es/bar", payload)

    def post_es_bar_10(self, bar: dict, trade_date: str):
        """Phase 1: route 10-pt bars to same endpoint with range_pts=10.0
        (UNIQUE constraint includes range_pts so 5pt/10pt bars don't collide)."""
        return self.post_es_bar(bar, trade_date, range_pts=10.0)

    def post_vx_ticks(self, ticks: list):
        if not ticks:
            return True
        return self._post("/api/vps/vix/ticks", {"ticks": ticks})

    def post_heartbeat(self, status: dict):
        return self._post("/api/vps/heartbeat", status)

    def post_vol_signal(self, signal: dict):
        return self._post("/api/vps/vol/signal", signal)

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
            last_ts = datetime(2026, 3, 23)  # ESM26 (June) contract — backfill from Mar 23
            log.info("No ES bars in Railway — backfilling from 2026-03-23")

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
            last_ts = datetime(2026, 3, 23)
            log.info("No VX ticks in Railway — backfilling from 2026-03-23")

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


# ─── SCID File Tailer ────────────────────────────────────────────────────────

class SCIDTailer:
    """Tails a Sierra .scid file in real-time by tracking byte offset.

    Sierra flushes new ticks to .scid every ~5 seconds. We poll the file
    every 2 seconds, read any new 40-byte records, and yield them.
    """

    def __init__(self, filepath: str):
        self.filepath = Path(filepath)
        self._offset = 0  # byte offset into file (past header)

    def seek_to_end(self):
        """Set offset to current end of file (skip existing data)."""
        if self.filepath.exists():
            self._offset = os.path.getsize(self.filepath)
            log.info(f"SCID tailer: {self.filepath.name} → offset {self._offset:,} bytes "
                     f"(skipping existing)")

    def seek_to_time(self, since_ts: datetime):
        """Set offset via binary search to start reading from a specific time."""
        if not self.filepath.exists():
            return
        file_size = os.path.getsize(self.filepath)
        num_records = (file_size - SCID_HEADER_SIZE) // SCID_RECORD_SIZE
        if num_records <= 0:
            self._offset = SCID_HEADER_SIZE
            return

        # Binary search for the record closest to since_ts
        since_et = pytz.utc.localize(since_ts).astimezone(ET).replace(tzinfo=None)
        target_us = int((since_et - SC_EPOCH).total_seconds() * 1_000_000)

        with open(self.filepath, 'rb') as f:
            lo, hi = 0, num_records - 1
            while lo < hi:
                mid = (lo + hi) // 2
                f.seek(SCID_HEADER_SIZE + mid * SCID_RECORD_SIZE)
                data = f.read(SCID_RECORD_SIZE)
                dt_int = struct.unpack_from('<q', data, 0)[0]
                if dt_int <= target_us:
                    lo = mid + 1
                else:
                    hi = mid
            self._offset = SCID_HEADER_SIZE + max(0, lo - 5) * SCID_RECORD_SIZE

        log.info(f"SCID tailer: {self.filepath.name} → seeking to record ~{lo} "
                 f"(offset {self._offset:,})")

    def read_new_ticks(self):
        """Read any new records appended since last read. Returns list of tick dicts."""
        if not self.filepath.exists():
            return []

        file_size = os.path.getsize(self.filepath)
        if file_size <= self._offset:
            return []  # No new data

        ticks = []
        with open(self.filepath, 'rb') as f:
            f.seek(self._offset)
            bytes_available = file_size - self._offset
            records_available = bytes_available // SCID_RECORD_SIZE

            for _ in range(records_available):
                data = f.read(SCID_RECORD_SIZE)
                if len(data) < SCID_RECORD_SIZE:
                    break

                dt_int = struct.unpack_from('<q', data, 0)[0]
                if dt_int <= 0:
                    continue

                ts = SC_EPOCH + timedelta(microseconds=dt_int)
                _, h, l, c = struct.unpack_from('<ffff', data, 8)
                _, vol, bid_vol, ask_vol = struct.unpack_from('<IIII', data, 24)

                price = h if h > 0 else c
                if price <= 0 or vol <= 0:
                    continue

                ticks.append({
                    "ts": ts,
                    "price": price,
                    "volume": vol,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol,
                })

            # Update offset to where we stopped reading
            self._offset = SCID_HEADER_SIZE + (
                (self._offset - SCID_HEADER_SIZE) // SCID_RECORD_SIZE + records_available
            ) * SCID_RECORD_SIZE

        return ticks

    @property
    def file_size(self):
        if self.filepath.exists():
            return os.path.getsize(self.filepath)
        return 0


# ─── SCID-Based Data Bridge ─────────────────────────────────────────────────

class VPSDataBridge:
    """Main bridge: Sierra .scid files → Range Bars + VX Ticks → Railway.
    Tails .scid files every 2s for new ticks. Self-healing with gap backfill.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.es_scid = cfg.get("es_scid_file", "")
        self.vx_scid = cfg.get("vx_scid_file", "")

        self._running = False
        self._lock = threading.Lock()

        self.bar_builder = RangeBarBuilder(cfg.get("range_pts", 5.0))
        # Phase 1: parallel 10-pt builder for SB10 Absorption parity. Same tick stream,
        # independent bar boundaries. Bars POSTed to /api/vps/es/bar10 on completion.
        self.bar_builder_10 = RangeBarBuilder(cfg.get("range_pts_10", 10.0))
        self._bars_posted_10 = 0

        self._vx_ticks = []
        self._vx_last_flush = time.time()

        self.poster = RailwayPoster(
            cfg["railway_api_url"],
            cfg.get("vps_api_key", ""),
            cfg.get("post_timeout", 10),
        )

        self.backfiller = GapBackfiller(self.poster, cfg)

        # SCID file tailers
        self._es_tailer = SCIDTailer(self.es_scid) if self.es_scid else None
        self._vx_tailer = SCIDTailer(self.vx_scid) if self.vx_scid else None

        # Vol signal file watcher
        self._vol_signal_file = cfg.get("vol_signal_file", "C:/SierraChart/Data/vol_signal.txt")
        self._vol_signal_poll = cfg.get("vol_signal_poll_seconds", 2)
        self._vol_signal_last_mtime = 0.0
        self._vol_signal_last_content = ""
        self._vol_signals_posted = 0

        # Stats
        self._es_tick_count = 0
        self._vx_tick_count = 0
        self._bars_posted = 0
        self._vx_batches_posted = 0
        self._last_heartbeat = 0
        self._session_date = None
        self._last_es_tick_time = time.time()
        self._stale_timeout = cfg.get("stale_timeout_minutes", 5) * 60
        self._backfill_count = 0

        # Stale cycle tracking (3 cycles alert, 5 cycles Telegram warning)
        self._stale_cycles = 0
        self._STALE_ALERT_CYCLES = 3
        self._STALE_WARN_CYCLES = 5

    # ── Gap Detection & Backfill ──────────────────────────────────────────────

    def backfill_gaps(self):
        """Check for gaps and backfill from .scid files. Called on startup."""
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

    # ── Start Tailing ────────────────────────────────────────────────────────

    def start(self):
        """Initialize tailers to current end of .scid files and start tailing."""
        # Seek tailers to end of file (we already backfilled historical data)
        if self._es_tailer:
            self._es_tailer.seek_to_end()
        if self._vx_tailer:
            self._vx_tailer.seek_to_end()

        self._running = True
        self._session_date = _es_session_date()
        self._last_es_tick_time = time.time()

        log.info(f"SCID tailing started — session: {self._session_date}")
        log.info(f"  ES: {self.es_scid}")
        log.info(f"  VX: {self.vx_scid}")
        log.info(f"  Poll interval: 2s")

    def stop(self):
        self._running = False
        log.info("SCID tailing stopped")

    # ── Tick Processing ──────────────────────────────────────────────────────

    def _poll_es_ticks(self):
        """Read new ES ticks from .scid and feed into range bar builder."""
        if not self._es_tailer:
            return

        ticks = self._es_tailer.read_new_ticks()
        if not ticks:
            return

        self._last_es_tick_time = time.time()

        for tick in ticks:
            self._es_tick_count += 1

            # S50 fix (2026-05-04): drive session date from the TICK timestamp,
            # not wall-clock. Catch-up ticks (after Sierra reconnect) carry
            # their real ts but were previously tagged with today's session,
            # which on Apr 20 produced 1,014 mis-dated bars that had to be
            # manually deleted + re-backfilled. Mirror what backfill does
            # at line 451: session = _session_date_from_ts(tick["ts"]).
            # _session_date_from_ts returns date; str() to match _es_session_date format.
            tick_session = str(_session_date_from_ts(tick["ts"]))
            if tick_session != self._session_date:
                log.info(f"Session rollover: {self._session_date} -> {tick_session} "
                         f"(driven by tick ts={tick['ts'].isoformat()})")
                self.bar_builder.reset_session()
                self.bar_builder_10.reset_session()
                self._session_date = tick_session

            buy_vol, sell_vol, delta = _classify_scid(
                tick["bid_vol"], tick["ask_vol"], tick["volume"]
            )
            ts_iso = tick["ts"].isoformat()

            completed = self.bar_builder.process_tick(
                tick["price"], tick["volume"],
                buy_vol, sell_vol, delta,
                ts_iso,
            )
            # Phase 1: feed same tick into 10-pt builder. Independent state, same tick stream.
            completed_10 = self.bar_builder_10.process_tick(
                tick["price"], tick["volume"],
                buy_vol, sell_vol, delta,
                ts_iso,
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
                    args=(completed, tick_session),
                    daemon=True,
                ).start()
                self._bars_posted += 1

            if completed_10:
                log.info(
                    f"ES10 bar #{completed_10['idx']}: "
                    f"O={completed_10['open']:.2f} H={completed_10['high']:.2f} "
                    f"L={completed_10['low']:.2f} C={completed_10['close']:.2f} "
                    f"vol={completed_10['volume']} delta={completed_10['delta']:+d}"
                )
                threading.Thread(
                    target=self.poster.post_es_bar_10,
                    args=(completed_10, tick_session),
                    daemon=True,
                ).start()
                self._bars_posted_10 += 1

    def _poll_vx_ticks(self):
        """Read new VX ticks from .scid and buffer for batch posting."""
        if not self._vx_tailer:
            return

        ticks = self._vx_tailer.read_new_ticks()
        if not ticks:
            return

        for tick in ticks:
            self._vx_tick_count += 1
            buy_vol, sell_vol, delta = _classify_scid(
                tick["bid_vol"], tick["ask_vol"], tick["volume"]
            )
            with self._lock:
                self._vx_ticks.append({
                    "price": round(tick["price"], 2),
                    "volume": tick["volume"],
                    "delta": delta,
                    "bid": None,
                    "ask": None,
                    "ts": tick["ts"].isoformat(),
                })

    # ── Stale Detection ──────────────────────────────────────────────────────

    def _check_stale(self):
        """Check if .scid file stopped growing during market hours."""
        if not _market_open():
            self._stale_cycles = 0
            return

        now = time.time()
        es_age = now - self._last_es_tick_time

        if es_age <= self._stale_timeout:
            if self._stale_cycles > 0:
                log.info(f"ES data recovered (was stale for {self._stale_cycles} cycles)")
                self._stale_cycles = 0
            return

        self._stale_cycles += 1
        mins = es_age / 60
        log.warning(f"STALE cycle {self._stale_cycles}: No ES ticks for {mins:.1f} min "
                     f"(SCID file not growing)")

        if self._stale_cycles == self._STALE_ALERT_CYCLES:
            _send_telegram(self.cfg,
                f"⚠️ SCID stale — no ES ticks for {mins:.0f} min. "
                f"Sierra Chart data feed may be down.")

        if self._stale_cycles == self._STALE_WARN_CYCLES:
            _send_telegram(self.cfg,
                f"🔴 SCID stale for {mins:.0f} min — check Sierra Chart! "
                f"(file: {self.es_scid})")

    # ── Main Loop ─────────────────────────────────────────────────────────────

    def run_forever(self):
        log.info("SCID bridge running 24/7. Ctrl+C to stop.")
        poll_interval = 2  # seconds
        vx_flush_interval = self.cfg.get("vx_batch_seconds", 10)
        hb_interval = self.cfg.get("heartbeat_seconds", 60)
        vol_interval = self._vol_signal_poll
        last_vol_check = 0.0

        while self._running:
            try:
                now = time.time()

                # Poll SCID files for new ticks (every 2s)
                self._poll_es_ticks()
                self._poll_vx_ticks()

                # Flush VX ticks to Railway
                if now - self._vx_last_flush >= vx_flush_interval:
                    self._flush_vx_ticks()
                    self._vx_last_flush = now

                # Check vol signal file
                if now - last_vol_check >= vol_interval:
                    self._check_vol_signal()
                    last_vol_check = now

                # Heartbeat + stale detection
                if now - self._last_heartbeat >= hb_interval:
                    self._send_heartbeat()
                    self._last_heartbeat = now
                    self._check_stale()

                time.sleep(poll_interval)

            except KeyboardInterrupt:
                log.info("Shutting down...")
                self._running = False
                break
            except Exception as e:
                log.error(f"Bridge loop error: {e}", exc_info=True)
                time.sleep(5)

    def _flush_vx_ticks(self):
        with self._lock:
            if not self._vx_ticks:
                return
            batch = self._vx_ticks.copy()
            self._vx_ticks.clear()

        # Chunk to avoid Railway timeouts on large catch-up batches
        CHUNK_SIZE = 500
        MAX_BUFFER = 50000  # ~1 hr of VX; drop oldest if exceeded during long outage
        failed = []
        for i in range(0, len(batch), CHUNK_SIZE):
            chunk = batch[i:i + CHUNK_SIZE]
            if self.poster.post_vx_ticks(chunk):
                self._vx_batches_posted += 1
            else:
                failed.extend(chunk)

        if failed:
            with self._lock:
                combined = failed + self._vx_ticks
                if len(combined) > MAX_BUFFER:
                    dropped = len(combined) - MAX_BUFFER
                    combined = combined[-MAX_BUFFER:]
                    log.warning(f"VX buffer overflow — dropped {dropped} oldest ticks")
                self._vx_ticks = combined
            log.warning(f"VX flush: {len(failed)} ticks requeued after POST failure")

    def _check_vol_signal(self):
        """Read Sierra VolDetector signal file. POST to Railway if new signal."""
        try:
            fpath = Path(self._vol_signal_file)
            if not fpath.exists():
                return

            mtime = fpath.stat().st_mtime
            if mtime <= self._vol_signal_last_mtime:
                return

            content = fpath.read_text().strip()
            if not content or content == self._vol_signal_last_content:
                self._vol_signal_last_mtime = mtime
                return

            self._vol_signal_last_mtime = mtime
            self._vol_signal_last_content = content

            parts = content.split(",")
            if len(parts) < 8:
                log.warning(f"Vol signal malformed: {content}")
                return

            signal = {
                "direction": int(parts[0]),
                "vx_price": float(parts[1]),
                "delta": float(parts[2]),
                "ask_vol": float(parts[3]),
                "bid_vol": float(parts[4]),
                "avg_delta": float(parts[5]),
                "ratio": float(parts[6]),
                "bar_ts": parts[7],
            }

            label = "VOL_SELLERS" if signal["direction"] < 0 else "VOL_BUYERS"
            log.info(
                f"Vol signal: {label} VX={signal['vx_price']:.2f} "
                f"delta={signal['delta']:+.0f} ratio={signal['ratio']:.1f}x "
                f"bar={signal['bar_ts']}"
            )

            ok = self.poster.post_vol_signal(signal)
            if ok:
                self._vol_signals_posted += 1

        except Exception as e:
            log.warning(f"Vol signal check error: {e}")

    def _send_heartbeat(self):
        forming = self.bar_builder.forming_bar
        es_size = self._es_tailer.file_size if self._es_tailer else 0
        status = {
            "component": "vps_data_bridge",
            "mode": "scid_tail",
            "ts": datetime.now(ET).isoformat(),
            "session_date": self._session_date,
            "es_scid": str(self.es_scid),
            "vx_scid": str(self.vx_scid),
            "es_ticks": self._es_tick_count,
            "vx_ticks": self._vx_tick_count,
            "bars_completed": self.bar_builder.bar_idx,
            "bars_posted": self._bars_posted,
            "bars_completed_10": self.bar_builder_10.bar_idx,
            "bars_posted_10": self._bars_posted_10,
            "vx_batches_posted": self._vx_batches_posted,
            "market_open": _market_open(),
            "backfill_count": self._backfill_count,
            "stale_cycles": self._stale_cycles,
            "es_scid_size_mb": round(es_size / 1024 / 1024, 1),
            "vol_signals_posted": self._vol_signals_posted,
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
        stale_tag = f" STALE({self._stale_cycles})" if self._stale_cycles > 0 else ""
        log.info(
            f"Heartbeat: ES ticks={self._es_tick_count} "
            f"bars={self.bar_builder.bar_idx} bars10={self.bar_builder_10.bar_idx} "
            f"VX ticks={self._vx_tick_count} "
            f"market={'OPEN' if _market_open() else 'CLOSED'}{stale_tag}"
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
    parser = argparse.ArgumentParser(description="VPS Data Bridge: Sierra SCID -> Railway")
    parser.add_argument("--config", default="vps_bridge_config.json",
                        help="Config file path")
    parser.add_argument("--test", action="store_true",
                        help="Tail SCID for 30s, print tick counts, then exit")
    parser.add_argument("--backfill-only", action="store_true",
                        help="Only run backfill (no live tailing)")
    args = parser.parse_args()

    cfg = load_config(args.config)

    log.info("=" * 50)
    log.info("  VPS DATA BRIDGE (SCID Tail Mode)")
    log.info("=" * 50)
    log.info(f"  ES SCID:   {cfg.get('es_scid_file', 'N/A')}")
    log.info(f"  VX SCID:   {cfg.get('vx_scid_file', 'N/A')}")
    log.info(f"  Railway:   {cfg['railway_api_url']}")
    log.info(f"  Range:     {cfg['range_pts']}pt bars")
    log.info(f"  Poll:      2s")
    log.info(f"  Stale:     {cfg.get('stale_timeout_minutes', 5)} min")
    log.info("=" * 50)

    # Verify .scid files exist
    es_scid = cfg.get("es_scid_file", "")
    vx_scid = cfg.get("vx_scid_file", "")
    if es_scid and not Path(es_scid).exists():
        log.error(f"ES SCID file not found: {es_scid}")
        _send_telegram(cfg, f"🔴 ES SCID file not found: {es_scid}")
    if vx_scid and not Path(vx_scid).exists():
        log.error(f"VX SCID file not found: {vx_scid}")

    bridge = VPSDataBridge(cfg)

    try:
        # Step 1: Backfill any gaps from .scid files
        bridge.backfill_gaps()

        if args.backfill_only:
            log.info("Backfill-only mode — exiting")
            return

        # Step 2: Start tailing .scid files for live data
        bridge.start()

        if args.test:
            log.info("Test mode: tailing SCID for 30s...")
            for _ in range(15):
                bridge._poll_es_ticks()
                bridge._poll_vx_ticks()
                time.sleep(2)
            log.info(f"ES ticks: {bridge._es_tick_count}, "
                     f"VX ticks: {bridge._vx_tick_count}, "
                     f"Bars: {bridge.bar_builder.bar_idx}")
            return

        # Step 3: Run forever — poll SCID every 2s
        _send_telegram(cfg,
            f"🟢 VPS Bridge started (SCID tail mode)\n"
            f"ES: {es_scid}\nVX: {vx_scid}")
        bridge.run_forever()

    except KeyboardInterrupt:
        bridge.stop()
    except Exception as e:
        log.error(f"Bridge fatal error: {e}", exc_info=True)
        _send_telegram(cfg, f"🔴 VPS Bridge crashed: {e}")
        bridge.stop()


if __name__ == "__main__":
    main()
