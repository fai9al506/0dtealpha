"""
vps_historical_upload.py — Build range bars from Sierra .scid tick files and upload to Railway.

Reads ESM26-CME.scid and VXM26_FUT_CFE.scid, builds 5pt ES range bars with CVD,
batches VX ticks, and bulk POSTs to Railway vps_* tables.

Usage:
    python vps_historical_upload.py                          # Upload both ES + VX
    python vps_historical_upload.py --es-only                # ES range bars only
    python vps_historical_upload.py --vx-only                # VX ticks only
    python vps_historical_upload.py --since 2026-01-01       # Only data from Jan 1+
    python vps_historical_upload.py --dry-run                # Parse + count, no upload
"""

import struct
import os
import sys
import json
import argparse
import time
from datetime import datetime, timedelta, date
from collections import defaultdict
from pathlib import Path

import pytz
import requests

ET = pytz.timezone("US/Eastern")
SC_EPOCH = datetime(1899, 12, 30)
HEADER_SIZE = 56
RECORD_SIZE = 40

# ─── Config ──────────────────────────────────────────────────────────────────

SIERRA_DATA = Path("C:/SierraChart/Data")
ES_FILE = SIERRA_DATA / "ESM26-CME.scid"
VX_FILE = SIERRA_DATA / "VXM26_FUT_CFE.scid"

RAILWAY_URL = "https://0dtealpha.com"
VPS_API_KEY = ""  # Set if auth is configured

RANGE_PTS = 5.0
VX_BATCH_SIZE = 500  # Ticks per POST batch


def _update_config(key, value):
    global RAILWAY_URL, VPS_API_KEY
    if key == "url":
        RAILWAY_URL = value
    elif key == "key":
        VPS_API_KEY = value


# ─── SCID Reader ─────────────────────────────────────────────────────────────

def read_scid_ticks(filepath, since_date=None, progress_every=1_000_000):
    """Generator yielding tick dicts from a Sierra .scid file.

    Each tick: {ts, high, low, close, volume, bid_vol, ask_vol}
    For tick data: high = trade price (or high of tick range),
                   bid_vol/ask_vol = aggressor classification.
    """
    file_size = os.path.getsize(filepath)
    num_records = (file_size - HEADER_SIZE) // RECORD_SIZE
    print(f"Reading {filepath.name}: {num_records:,} records ({file_size/1e6:.1f} MB)")

    with open(filepath, 'rb') as f:
        f.seek(HEADER_SIZE)
        count = 0
        skipped = 0

        for i in range(num_records):
            data = f.read(RECORD_SIZE)
            if len(data) < RECORD_SIZE:
                break

            dt_int = struct.unpack_from('<q', data, 0)[0]
            if dt_int <= 0:
                skipped += 1
                continue

            ts = SC_EPOCH + timedelta(microseconds=dt_int)

            # Filter by date
            if since_date and ts.date() < since_date:
                skipped += 1
                continue

            o, h, l, c = struct.unpack_from('<ffff', data, 8)
            trades, vol, bid_vol, ask_vol = struct.unpack_from('<IIII', data, 24)

            # Sierra tick records: Open=0, High=price (or high), Low=price (or low)
            # Use High as the trade price for tick data
            price = h if h > 0 else c

            if price <= 0 or vol <= 0:
                skipped += 1
                continue

            count += 1
            if count % progress_every == 0:
                pct = (i / num_records) * 100
                print(f"  {pct:.0f}% ({count:,} ticks, {skipped:,} skipped) @ {ts}")

            yield {
                "ts": ts,
                "price": price,
                "low": l,
                "volume": vol,
                "bid_vol": bid_vol,
                "ask_vol": ask_vol,
            }

    print(f"  Done: {count:,} ticks yielded, {skipped:,} skipped")


# ─── ES Session Date ─────────────────────────────────────────────────────────

def _session_date(ts):
    """ES session date: 6 PM ET boundary."""
    # Convert naive UTC-like timestamp to ET
    ts_et = ts  # Sierra stores in exchange time (CT/ET depending on symbol)
    if ts_et.hour >= 18:
        return (ts_et + timedelta(days=1)).date()
    return ts_et.date()


# ─── Range Bar Builder ───────────────────────────────────────────────────────

class HistoricalBarBuilder:
    """Builds 5pt range bars from historical tick data, grouped by session date."""

    def __init__(self, range_pts=5.0):
        self.range_pts = range_pts
        self.cvd = 0
        self.bar_idx = 0
        self.forming_bar = None
        self.current_date = None
        self.all_bars = []  # list of (session_date, bar_dict)

    def _classify(self, bid_vol, ask_vol, volume):
        """Classify trade: ask_vol > 0 = buyer, bid_vol > 0 = seller."""
        if ask_vol > 0 and bid_vol == 0:
            return volume, 0, volume
        if bid_vol > 0 and ask_vol == 0:
            return 0, volume, -volume
        # Split — rare
        return ask_vol, bid_vol, ask_vol - bid_vol

    def _new_bar(self, price, ts):
        return {
            "open": price, "high": price, "low": price, "close": price,
            "volume": 0, "buy": 0, "sell": 0, "delta": 0,
            "ts_start": ts.isoformat(), "ts_end": ts.isoformat(),
            "cvd_open": self.cvd, "cvd_high": self.cvd, "cvd_low": self.cvd,
        }

    def process_tick(self, tick):
        """Process one tick. Returns completed bar or None."""
        price = tick["price"]
        volume = tick["volume"]
        ts = tick["ts"]
        session = _session_date(ts)

        # Session rollover
        if session != self.current_date:
            # Close any forming bar from previous session
            if self.forming_bar and self.current_date:
                self._force_close(ts)
            self.current_date = session
            self.cvd = 0
            self.bar_idx = 0
            self.forming_bar = None

        buy_vol, sell_vol, delta = self._classify(
            tick["bid_vol"], tick["ask_vol"], volume
        )

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
        bar["ts_end"] = ts.isoformat()

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
            self.all_bars.append((session, completed))
            self.bar_idx += 1
            self.forming_bar = self._new_bar(price, ts)
            return completed

        return None

    def _force_close(self, ts):
        """Force close forming bar at session end."""
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
            self.all_bars.append((self.current_date, completed))
            self.bar_idx += 1
        self.forming_bar = None


# ─── Railway Upload ──────────────────────────────────────────────────────────

def _make_session():
    s = requests.Session()
    if VPS_API_KEY:
        s.headers["Authorization"] = f"Bearer {VPS_API_KEY}"
    s.headers["Content-Type"] = "application/json"
    return s


def upload_es_bars(bars, dry_run=False):
    """Upload ES range bars to Railway in batches."""
    if not bars:
        print("No ES bars to upload.")
        return

    # Group by date
    by_date = defaultdict(list)
    for session_date, bar in bars:
        by_date[str(session_date)].append(bar)

    total_dates = len(by_date)
    total_bars = len(bars)
    print(f"\nES Upload: {total_bars:,} bars across {total_dates} dates")

    if dry_run:
        for d in sorted(by_date.keys()):
            print(f"  {d}: {len(by_date[d])} bars")
        return

    session = _make_session()
    uploaded = 0
    errors = 0

    for d in sorted(by_date.keys()):
        date_bars = by_date[d]
        for bar in date_bars:
            payload = {**bar, "trade_date": d}
            try:
                r = session.post(
                    f"{RAILWAY_URL}/api/vps/es/bar",
                    json=payload, timeout=10,
                )
                if r.status_code == 200:
                    uploaded += 1
                else:
                    errors += 1
                    if errors <= 5:
                        print(f"  ERROR {r.status_code}: {r.text[:100]}")
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  ERROR: {e}")

            if uploaded % 100 == 0 and uploaded > 0:
                print(f"  Uploaded {uploaded:,}/{total_bars:,} bars...", end="\r")

        print(f"  {d}: {len(date_bars)} bars uploaded")

    print(f"\nES Upload complete: {uploaded:,} uploaded, {errors:,} errors")


def upload_vx_ticks(ticks, dry_run=False):
    """Upload VX ticks to Railway in batches."""
    if not ticks:
        print("No VX ticks to upload.")
        return

    total = len(ticks)
    print(f"\nVX Upload: {total:,} ticks in batches of {VX_BATCH_SIZE}")

    if dry_run:
        first = ticks[0]
        last = ticks[-1]
        print(f"  First: {first['ts']} price={first['price']:.2f}")
        print(f"  Last:  {last['ts']} price={last['price']:.2f}")
        return

    session = _make_session()
    uploaded = 0
    errors = 0

    for i in range(0, total, VX_BATCH_SIZE):
        batch = ticks[i:i + VX_BATCH_SIZE]
        payload = {"ticks": batch}
        try:
            r = session.post(
                f"{RAILWAY_URL}/api/vps/vix/ticks",
                json=payload, timeout=15,
            )
            if r.status_code == 200:
                uploaded += len(batch)
            else:
                errors += len(batch)
                if errors <= VX_BATCH_SIZE * 3:
                    print(f"  ERROR {r.status_code}: {r.text[:100]}")
        except Exception as e:
            errors += len(batch)
            if errors <= VX_BATCH_SIZE * 3:
                print(f"  ERROR: {e}")

        if (i // VX_BATCH_SIZE) % 10 == 0:
            print(f"  Uploaded {uploaded:,}/{total:,} ticks...", end="\r")

    print(f"\nVX Upload complete: {uploaded:,} uploaded, {errors:,} errors")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Upload Sierra historical data to Railway")
    parser.add_argument("--es-only", action="store_true", help="Only upload ES range bars")
    parser.add_argument("--vx-only", action="store_true", help="Only upload VX ticks")
    parser.add_argument("--since", type=str, help="Only data from this date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Parse and count, no upload")
    parser.add_argument("--railway-url", type=str, default=None)
    parser.add_argument("--api-key", type=str, default=None)
    args = parser.parse_args()

    if args.railway_url:
        _update_config("url", args.railway_url)
    if args.api_key:
        _update_config("key", args.api_key)

    since_date = None
    if args.since:
        since_date = date.fromisoformat(args.since)
        print(f"Filtering: only data since {since_date}")

    do_es = not args.vx_only
    do_vx = not args.es_only

    # ── ES Range Bars ──
    if do_es:
        if not ES_FILE.exists():
            print(f"ES file not found: {ES_FILE}")
        else:
            print("\n" + "=" * 50)
            print("  BUILDING ES 5-PT RANGE BARS")
            print("=" * 50)

            builder = HistoricalBarBuilder(RANGE_PTS)
            t0 = time.time()

            for tick in read_scid_ticks(ES_FILE, since_date):
                builder.process_tick(tick)

            # Force close last forming bar
            if builder.forming_bar:
                builder._force_close(datetime.now())

            elapsed = time.time() - t0
            print(f"Built {len(builder.all_bars):,} range bars in {elapsed:.1f}s")

            # Summary by date
            by_date = defaultdict(int)
            for d, _ in builder.all_bars:
                by_date[str(d)] += 1
            for d in sorted(by_date.keys())[-10:]:
                print(f"  {d}: {by_date[d]} bars")
            if len(by_date) > 10:
                print(f"  ... ({len(by_date)} total dates)")

            upload_es_bars(builder.all_bars, dry_run=args.dry_run)

    # ── VX Ticks ──
    if do_vx:
        if not VX_FILE.exists():
            print(f"VX file not found: {VX_FILE}")
        else:
            print("\n" + "=" * 50)
            print("  READING VX TICKS")
            print("=" * 50)

            vx_ticks = []
            for tick in read_scid_ticks(VX_FILE, since_date, progress_every=50_000):
                buy_vol, sell_vol = tick["ask_vol"], tick["bid_vol"]
                delta = buy_vol - sell_vol
                vx_ticks.append({
                    "price": round(tick["price"], 2),
                    "volume": tick["volume"],
                    "delta": delta,
                    "bid": round(tick["low"], 2) if tick["low"] > 0 else None,
                    "ask": None,
                    "ts": tick["ts"].isoformat(),
                })

            print(f"Total VX ticks: {len(vx_ticks):,}")
            upload_vx_ticks(vx_ticks, dry_run=args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
