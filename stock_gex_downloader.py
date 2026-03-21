"""
Stock GEX Historical Data Downloader — ThetaData API

Downloads EOD options data (OI + Greeks per strike) for backtesting
the stock GEX support/magnet strategy.

Prerequisites:
  1. ThetaData account (Value plan, $40/mo)
  2. Java 17+ installed
  3. ThetaData Terminal running:
     java -jar ThetaTerminal.jar your@email.com your_password
  4. pip install requests pandas

Usage:
  python stock_gex_downloader.py                  # Download last 12 months
  python stock_gex_downloader.py --months 6       # Download last 6 months
  python stock_gex_downloader.py --test            # Test with 1 stock, 1 date
  python stock_gex_downloader.py --test-format     # Show raw response format
"""

import requests
import json
import os
import sys
import time
import argparse
from datetime import datetime, date, timedelta

# ── Config ──────────────────────────────────────────────────────────

THETA_URL = "http://127.0.0.1:25510"
DATA_DIR = "data/stock_gex_historical"

STOCKS = [
    "AAPL", "MSFT", "GOOGL", "GOOG", "META", "NVDA", "AMZN", "NFLX", "TSLA",
    "AMD", "INTC", "MU", "QCOM", "AVGO", "SMCI",
    "PYPL", "SQ", "SOFI", "COIN", "AFRM", "UPST",
    "SNAP", "SHOP", "ROKU", "DKNG", "RBLX", "LULU", "SNOW",
    "AMC", "GME", "PLTR", "MARA", "RIOT", "NKLA", "LCID", "PLUG",
    "RIVN", "NIO",
    "BAC", "JPM", "WFC", "C",
    "BA", "DIS", "F", "GM", "T", "AAL", "CCL",
    "XOM", "OXY",
    "PFE", "JNJ",
    "UBER", "CVNA", "AI", "BABA", "COST", "ENPH",
]

# Rate limit: 10 calls/sec on paid plan, be safe with 8
CALLS_PER_SEC = 8
CALL_DELAY = 1.0 / CALLS_PER_SEC


# ── Helpers ─────────────────────────────────────────────────────────

def fmt_date(d):
    """date -> YYYYMMDD integer for ThetaData API."""
    return int(d.strftime("%Y%m%d"))


def get_mondays(months_back=12):
    """Generate all Mondays from N months ago to today."""
    end = date.today()
    start = end - timedelta(days=months_back * 30)
    mondays = []
    d = start
    while d <= end:
        if d.weekday() == 0:
            mondays.append(d)
        d += timedelta(days=1)
    return mondays


def get_target_expirations(trade_date):
    """Calculate weekly (this Friday) and opex (nearest 3rd Friday) expirations.

    Returns list of (exp_date, label) tuples.
    """
    targets = []

    # Weekly: nearest Friday >= trade_date
    days_to_fri = (4 - trade_date.weekday()) % 7
    if days_to_fri == 0:
        days_to_fri = 7  # if Monday IS Friday somehow (shouldn't happen)
    weekly = trade_date + timedelta(days=days_to_fri)
    targets.append((weekly, "weekly"))

    # OpEx: nearest 3rd Friday of month, at least 3 days out
    for m_offset in range(3):
        y, m = trade_date.year, trade_date.month + m_offset
        if m > 12:
            m -= 12
            y += 1
        first_day = date(y, m, 1)
        first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
        third_friday = first_friday + timedelta(days=14)
        if third_friday >= trade_date + timedelta(days=3):
            if third_friday == weekly:
                # OpEx week — relabel the weekly as opex
                targets[0] = (weekly, "opex")
            else:
                targets.append((third_friday, "opex"))
            break

    return targets


def check_terminal():
    """Check if ThetaData Terminal is running."""
    try:
        r = requests.get(f"{THETA_URL}/v2/hist/stock/ohlc",
                         params={"root": "AAPL", "start_date": 20250101,
                                 "end_date": 20250102, "ivl": 86400000},
                         timeout=5)
        return r.status_code == 200
    except requests.ConnectionError:
        return False


# ── ThetaData API ───────────────────────────────────────────────────

def fetch_options_eod(root, exp_date, trade_date):
    """Fetch bulk EOD options data for a stock+expiration on a date.

    Returns list of dicts with: strike, right, open_interest, gamma, delta, ...
    """
    url = f"{THETA_URL}/v2/bulk_hist/option/eod"
    params = {
        "root": root,
        "exp": fmt_date(exp_date),
        "start_date": fmt_date(trade_date),
        "end_date": fmt_date(trade_date),
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            return None
        js = r.json()

        if js.get("header", {}).get("error_type"):
            return None

        response = js.get("response", [])
        if not response:
            return None

        # Parse response: each item has "contract" + "ticks"
        records = []
        for item in response:
            contract = item.get("contract", {})
            ticks = item.get("ticks", [])
            if not ticks:
                continue

            # Use last tick (most recent EOD data for that date)
            tick = ticks[-1] if isinstance(ticks[0], list) else ticks

            record = {
                "root": contract.get("root", root),
                "strike": contract.get("strike", 0),  # in 1/10th cent
                "expiration": contract.get("expiration", 0),
                "right": contract.get("right", "?"),
            }

            # Map tick fields by position
            # ThetaData bulk EOD ticks format (positions may vary):
            # We extract what we can and store the full tick for debugging
            if isinstance(tick, list) and len(tick) >= 17:
                record["open"] = tick[2]
                record["high"] = tick[3]
                record["low"] = tick[4]
                record["close"] = tick[5]
                record["volume"] = tick[6]
                record["date"] = tick[16] if len(tick) > 16 else None

                # OI and Greeks typically after the base OHLCV+bid/ask fields
                if len(tick) > 17:
                    record["open_interest"] = tick[17]
                if len(tick) > 18:
                    # Greeks positions vary — store extras
                    extras = tick[17:]
                    record["_extras"] = extras

            record["_tick_len"] = len(tick) if isinstance(tick, list) else 0
            records.append(record)

        return records

    except Exception as e:
        print(f"    Error fetching {root} exp={exp_date}: {e}")
        return None


def fetch_options_eod_raw(root, exp_date, trade_date):
    """Fetch raw JSON response for format inspection."""
    url = f"{THETA_URL}/v2/bulk_hist/option/eod"
    params = {
        "root": root,
        "exp": fmt_date(exp_date),
        "start_date": fmt_date(trade_date),
        "end_date": fmt_date(trade_date),
    }
    r = requests.get(url, params=params, timeout=30)
    return r.json()


def fetch_stock_prices(root, start_date, end_date):
    """Fetch daily OHLC for a stock over a date range.

    Returns list of dicts: {date, open, high, low, close, volume}
    """
    url = f"{THETA_URL}/v2/hist/stock/ohlc"
    params = {
        "root": root,
        "start_date": fmt_date(start_date),
        "end_date": fmt_date(end_date),
        "ivl": 86400000,  # daily bars
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            return None
        js = r.json()

        if js.get("header", {}).get("error_type"):
            return None

        bars = []
        for tick in js.get("response", []):
            if isinstance(tick, list) and len(tick) >= 7:
                bars.append({
                    "date": tick[6] if len(tick) > 6 else tick[-1],
                    "open": tick[1],
                    "high": tick[2],
                    "low": tick[3],
                    "close": tick[4],
                    "volume": tick[5],
                })
        return bars

    except Exception as e:
        print(f"    Error fetching prices for {root}: {e}")
        return None


# ── Download ────────────────────────────────────────────────────────

def download(months=12, stocks=None):
    """Main download loop."""
    stocks = stocks or STOCKS
    mondays = get_mondays(months)

    print(f"=== Stock GEX Downloader ===")
    print(f"Stocks: {len(stocks)}")
    print(f"Mondays: {len(mondays)} ({mondays[0]} to {mondays[-1]})")
    print(f"Data dir: {DATA_DIR}")
    print()

    os.makedirs(f"{DATA_DIR}/prices", exist_ok=True)

    # Phase 1: Stock prices (one file per stock, entire date range)
    print("--- Phase 1: Daily stock prices ---")
    start_dt = mondays[0] - timedelta(days=7)  # include week before first Monday
    end_dt = date.today()
    prices_done = 0

    for i, stock in enumerate(stocks):
        price_file = f"{DATA_DIR}/prices/{stock}.json"
        if os.path.exists(price_file):
            prices_done += 1
            continue

        bars = fetch_stock_prices(stock, start_dt, end_dt)
        time.sleep(CALL_DELAY)

        if bars:
            with open(price_file, "w") as f:
                json.dump(bars, f)
            print(f"  [{i+1}/{len(stocks)}] {stock}: {len(bars)} days")
        else:
            print(f"  [{i+1}/{len(stocks)}] {stock}: no data")
        prices_done += 1

    print(f"  Prices: {prices_done}/{len(stocks)} done\n")

    # Phase 2: Options data (per stock per Monday per expiration)
    print("--- Phase 2: Options chain data ---")
    total_calls = 0
    total_skipped = 0
    total_saved = 0

    for mi, monday in enumerate(mondays):
        targets = get_target_expirations(monday)
        saved_this_monday = 0

        for stock in stocks:
            stock_dir = f"{DATA_DIR}/options/{stock}"
            os.makedirs(stock_dir, exist_ok=True)

            for exp_date, label in targets:
                out_file = f"{stock_dir}/{monday}_{label}.json"

                # Skip if already downloaded
                if os.path.exists(out_file):
                    total_skipped += 1
                    continue

                records = fetch_options_eod(stock, exp_date, monday)
                total_calls += 1
                time.sleep(CALL_DELAY)

                if records and len(records) > 0:
                    with open(out_file, "w") as f:
                        json.dump(records, f)
                    saved_this_monday += 1
                    total_saved += 1
                else:
                    # Save empty marker so we don't retry
                    with open(out_file, "w") as f:
                        json.dump([], f)

        # Progress update per Monday
        pct = (mi + 1) / len(mondays) * 100
        print(f"  [{mi+1}/{len(mondays)}] {monday}: {saved_this_monday} files saved "
              f"({pct:.0f}% done, {total_calls} API calls)")

    print(f"\n=== Download Complete ===")
    print(f"  API calls: {total_calls}")
    print(f"  Files saved: {total_saved}")
    print(f"  Files skipped (existing): {total_skipped}")
    print(f"  Data directory: {os.path.abspath(DATA_DIR)}")


# ── Test / Format Detection ────────────────────────────────────────

def test_format():
    """Fetch one response and show the raw format for debugging."""
    print("=== Testing ThetaData Response Format ===\n")

    # Use a recent Monday and AAPL
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    if monday == today:
        monday -= timedelta(days=7)
    exp_date = monday + timedelta(days=(4 - monday.weekday()) % 7)

    print(f"Stock: AAPL")
    print(f"Trade date: {monday}")
    print(f"Expiration: {exp_date}")
    print()

    raw = fetch_options_eod_raw("AAPL", exp_date, monday)

    # Show header
    print(f"Header: {raw.get('header')}")
    print(f"Response items: {len(raw.get('response', []))}")
    print()

    # Show first few items
    for i, item in enumerate(raw.get("response", [])[:3]):
        print(f"--- Item {i} ---")
        print(f"  Contract: {item.get('contract')}")
        ticks = item.get("ticks", [])
        if ticks:
            tick = ticks[0] if isinstance(ticks[0], list) else ticks
            print(f"  Tick length: {len(tick)}")
            print(f"  Tick values: {tick}")
        print()

    # Show strike format
    contracts = raw.get("response", [])
    if contracts:
        c = contracts[0].get("contract", {})
        strike_raw = c.get("strike", 0)
        print(f"Strike raw value: {strike_raw}")
        print(f"Strike in dollars: ${strike_raw / 1000:.2f}")
        print(f"Right: {c.get('right')}")

    print(f"\nFull first item JSON:")
    if raw.get("response"):
        print(json.dumps(raw["response"][0], indent=2))


def test_download():
    """Test download with 1 stock, 1 Monday."""
    print("=== Test Download (1 stock, 1 Monday) ===\n")
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    if monday == today:
        monday -= timedelta(days=7)

    targets = get_target_expirations(monday)
    print(f"Monday: {monday}")
    print(f"Expirations: {[(str(e), l) for e, l in targets]}")
    print()

    # Fetch AAPL
    for exp_date, label in targets:
        print(f"Fetching AAPL {label} (exp={exp_date})...")
        records = fetch_options_eod("AAPL", exp_date, monday)

        if records:
            print(f"  Got {len(records)} contracts")
            # Show sample
            calls = [r for r in records if r["right"] == "C"]
            puts = [r for r in records if r["right"] == "P"]
            print(f"  Calls: {len(calls)}, Puts: {len(puts)}")

            # Show tick field count
            if records:
                print(f"  Tick fields: {records[0].get('_tick_len', '?')}")
                if records[0].get("_extras"):
                    print(f"  Extra fields (after base 17): {records[0]['_extras']}")

            # Show a sample record
            sample = records[len(records) // 2]
            strike_dollars = sample["strike"] / 1000
            print(f"\n  Sample contract:")
            print(f"    Strike: ${strike_dollars:.2f}")
            print(f"    Right: {sample['right']}")
            print(f"    Close: {sample.get('close')}")
            print(f"    Volume: {sample.get('volume')}")
            print(f"    OI: {sample.get('open_interest', 'NOT IN BASE FIELDS')}")
            if sample.get("_extras"):
                print(f"    Extras[0] (likely OI): {sample['_extras'][0]}")
                if len(sample["_extras"]) > 1:
                    print(f"    Extras[1-5] (likely Greeks): {sample['_extras'][1:6]}")
        else:
            print(f"  No data returned")
        print()


# ── Main ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download stock GEX data from ThetaData")
    parser.add_argument("--months", type=int, default=12, help="Months of history (default: 12)")
    parser.add_argument("--test", action="store_true", help="Test with 1 stock, 1 date")
    parser.add_argument("--test-format", action="store_true", help="Show raw response format")
    parser.add_argument("--stocks", nargs="+", help="Override stock list")
    args = parser.parse_args()

    # Check terminal
    print("Checking ThetaData Terminal connection...")
    if not check_terminal():
        print("\nERROR: ThetaData Terminal not running!")
        print("Start it with: java -jar ThetaTerminal.jar your@email.com your_password")
        print("Download from: https://download-stable.thetadata.us/")
        sys.exit(1)
    print("Connected!\n")

    if args.test_format:
        test_format()
    elif args.test:
        test_download()
    else:
        download(months=args.months, stocks=args.stocks)
