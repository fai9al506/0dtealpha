"""
SPX 0DTE GEX Historical Data Downloader — ThetaData API (Value Plan)

Downloads 10:00 AM options chain snapshots (bid/ask + OI per strike) for SPXW
0DTE expirations and computes gamma via Black-Scholes. Also downloads SPX
intraday 30-min bars for price action analysis.

Key differences from stock_gex_downloader.py:
  - Root: SPXW (PM-settled 0DTE, every weekday since May 2022)
  - Every trading day = its own expiration (exp == trade_date)
  - 10:00 AM snapshot via bulk_hist/option/quote (not EOD — 0DTE expired by EOD)
  - 40 strikes: ±100 pts from spot at $5 intervals
  - Gamma T = 6 hours (10:00->16:00 expiry)

Prerequisites:
  1. ThetaData account (Value plan, $40/mo)
  2. Java 11+ installed
  3. ThetaData Terminal running:
     java -jar ThetaTerminal.jar your@email.com your_password
  4. pip install requests

Usage:
  python spx_gex_downloader.py                  # Download last 12 months
  python spx_gex_downloader.py --months 6       # Download last 6 months
  python spx_gex_downloader.py --test           # Test 1 day, show GEX preview
  python spx_gex_downloader.py --test-format    # Show raw response format
  python spx_gex_downloader.py --resume         # Resume interrupted download
  python spx_gex_downloader.py --start 2025-06-01  # Start from specific date
"""

import requests
import json
import os
import sys
import time
import math
import argparse
from datetime import datetime, date, timedelta

# ── Config ──────────────────────────────────────────────────────────

THETA_URL = "http://127.0.0.1:25510"
DATA_DIR = os.environ.get("GEX_DATA_DIR", r"C:\Users\Faisa\stock_gex_data")
SYM_DIR = None  # set in main()

# Rate limit: 10 calls/sec on paid plan, be safe
CALL_DELAY = 0.15
RISK_FREE_RATE = 0.045  # ~4.5% Fed Funds rate

SNAPSHOT_TIME_MS = 36000000  # 10:00 AM ET in ms since midnight
GAMMA_T_HOURS = 6.0      # 10:00 AM -> 16:00 expiry = 6 hours

# Per-symbol config: option_root, strike_interval, strike_range, price_type
SYMBOLS = {
    "SPX":  {"option_root": "SPXW", "strike_interval": 5,  "strike_range": 100, "price_type": "index"},
    "SPY":  {"option_root": "SPY",  "strike_interval": 1,  "strike_range": 20,  "price_type": "stock"},
    "QQQ":  {"option_root": "QQQ",  "strike_interval": 1,  "strike_range": 20,  "price_type": "stock"},
    "IWM":  {"option_root": "IWM",  "strike_interval": 1,  "strike_range": 15,  "price_type": "stock"},
}


# ── Black-Scholes Gamma (reused from stock_gex_downloader.py) ─────

def _norm_cdf(x):
    """Standard normal CDF approximation (Abramowitz & Stegun)."""
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x / 2.0)
    return 0.5 * (1.0 + sign * y)


def _norm_pdf(x):
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _bs_price(S, K, T, r, sigma, right):
    """Black-Scholes option price."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if right == "C":
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def implied_vol(S, K, T, r, market_price, right, tol=1e-6, max_iter=50):
    """Newton-Raphson implied vol solver."""
    if market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return None
    sigma = 0.3  # initial guess
    for _ in range(max_iter):
        price = _bs_price(S, K, T, r, sigma, right)
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
        vega = S * _norm_pdf(d1) * math.sqrt(T)
        if vega < 1e-12:
            return None
        diff = price - market_price
        sigma -= diff / vega
        if sigma <= 0.001:
            sigma = 0.001
        if abs(diff) < tol:
            return sigma
    return sigma if abs(diff) < 0.05 else None


def bs_gamma(S, K, T, r, sigma):
    """Black-Scholes gamma (same for calls and puts)."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    return _norm_pdf(d1) / (S * sigma * math.sqrt(T))


# ── Helpers ─────────────────────────────────────────────────────────

def fmt_date(d):
    """date -> YYYYMMDD integer for ThetaData API."""
    return int(d.strftime("%Y%m%d"))


def parse_theta_date(d_int):
    """YYYYMMDD int -> date object."""
    s = str(d_int)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def check_terminal():
    """Check if ThetaData Terminal is running."""
    for attempt in range(3):
        try:
            r = requests.get(f"{THETA_URL}/v2/list/expirations",
                             params={"root": "SPXW"}, timeout=15)
            return r.status_code == 200
        except (requests.ConnectionError, requests.ReadTimeout):
            if attempt < 2:
                time.sleep(5)
    return False


# ── ThetaData API ───────────────────────────────────────────────────

def fetch_expirations(option_root):
    """Fetch all available expirations for an option root.
    Returns list of YYYYMMDD integers."""
    url = f"{THETA_URL}/v2/list/expirations"
    try:
        r = requests.get(url, params={"root": option_root}, timeout=15)
        if r.status_code != 200:
            print(f"  Expirations API error: {r.status_code}")
            return []
        js = r.json()
        return js.get("response", [])
    except Exception as e:
        print(f"  Error fetching {option_root} expirations: {e}")
        return []


def fetch_prices(symbol, start_date, end_date, price_type="index"):
    """Fetch daily prices. Uses index endpoint for SPX, stock endpoint for ETFs."""
    if price_type == "index":
        url = f"{THETA_URL}/v2/hist/index/eod"
    else:
        url = f"{THETA_URL}/v2/hist/stock/eod"

    params = {
        "root": symbol,
        "start_date": fmt_date(start_date),
        "end_date": fmt_date(end_date),
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            print(f"  {symbol} price API error: {r.status_code}")
            return None
        js = r.json()
        if js.get("header", {}).get("error_type"):
            print(f"  {symbol} price error: {js['header']['error_type']}")
            return None

        # EOD format: [ms_of_day, ms_of_day2, open, high, low, close,
        #   volume, count, bid_size, bid_exchange, bid, bid_condition,
        #   ask_size, ask_exchange, ask, ask_condition, date]
        bars = []
        for tick in js.get("response", []):
            if isinstance(tick, list) and len(tick) >= 17:
                bars.append({
                    "date": tick[16],
                    "open": tick[2],
                    "high": tick[3],
                    "low": tick[4],
                    "close": tick[5],
                    "volume": tick[6],
                })
        return bars if bars else None
    except Exception as e:
        print(f"  Error fetching {symbol} prices: {e}")
        return None


def fetch_spx_intraday(start_date, end_date):
    """Fetch SPX 30-min intraday bars.
    Uses /v2/hist/stock/trade with ivl=1800000 (30 min in ms)."""
    for root in ["SPX", "$SPX"]:
        url = f"{THETA_URL}/v2/hist/stock/trade"
        params = {
            "root": root,
            "start_date": fmt_date(start_date),
            "end_date": fmt_date(end_date),
            "ivl": 1800000,  # 30 minutes in ms
        }
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code != 200:
                continue
            js = r.json()
            if js.get("header", {}).get("error_type"):
                continue

            # Trade format: [ms_of_day, ..., open, high, low, close, volume, ..., date]
            # The exact indices depend on the interval format
            fmt = js.get("header", {}).get("format", [])
            bars = []
            for tick in js.get("response", []):
                if isinstance(tick, list) and len(tick) >= 7:
                    # For interval data, format is typically:
                    # [ms_of_day, open, high, low, close, volume, count, date]
                    bar = {
                        "ms_of_day": tick[0],
                        "open": tick[1] if len(tick) > 1 else 0,
                        "high": tick[2] if len(tick) > 2 else 0,
                        "low": tick[3] if len(tick) > 3 else 0,
                        "close": tick[4] if len(tick) > 4 else 0,
                        "volume": tick[5] if len(tick) > 5 else 0,
                        "date": tick[-1] if len(tick) > 6 else 0,
                    }
                    # Convert ms_of_day to HH:MM
                    ms = tick[0]
                    hours = ms // 3600000
                    minutes = (ms % 3600000) // 60000
                    bar["time"] = f"{hours:02d}:{minutes:02d}"
                    bars.append(bar)

            if bars:
                return bars
        except Exception:
            pass
        time.sleep(CALL_DELAY)

    return None


def fetch_10am_chain(exp_date, spot, option_root="SPXW", strike_interval=5, strike_range=100):
    """Fetch 10:00 AM bid/ask + OI for all strikes on a 0DTE expiration.

    Makes 2 API calls:
    1. /v2/bulk_hist/option/quote (all 1-min ticks, extract 10AM) -> bid/ask per strike
    2. /v2/bulk_hist/option/open_interest -> OI per strike

    Then computes gamma via BS model with T = 6 hours.
    Filters to ±strike_range pts from spot.
    """
    exp_int = fmt_date(exp_date)

    # ── 1. Fetch quotes (bulk, all 1-min ticks) then extract 10AM locally ──
    quote_items = None
    r = None
    for attempt in range(3):
        try:
            r = requests.get(f"{THETA_URL}/v2/bulk_hist/option/quote",
                             params={
                                 "root": option_root,
                                 "exp": exp_int,
                                 "start_date": exp_int,
                                 "end_date": exp_int,
                                 "ivl": 60000,  # 1-minute intervals
                             },
                             timeout=90)
            break
        except Exception as e:
            if attempt < 2:
                print(f"    Quote timeout for {exp_date}, retry {attempt+1}...")
                time.sleep(5)
            else:
                print(f"    Error fetching quotes for {exp_date}: {e}")
                return None

    if r.status_code != 200:
        print(f"    Quote API error {r.status_code} for {exp_date}")
        return None

    try:
        js_quote = r.json()
    except Exception:
        print(f"    Quote API invalid JSON for {exp_date}")
        return None

    quote_items = js_quote.get("response", [])

    if not quote_items:
        print(f"    No quotes for {exp_date}")
        return None

    # Parse quote data: extract 10:00 AM tick from each contract's tick list
    # Quote format: [ms_of_day, bid_size, bid_exchange, bid, bid_condition,
    #                ask_size, ask_exchange, ask, ask_condition, date]
    # Indices: ms_of_day=0, bid=3, ask=7
    quote_map = {}
    for item in quote_items:
        contract = item.get("contract", {})
        ticks = item.get("ticks", [])
        if not ticks:
            continue

        strike_raw = contract.get("strike", 0)
        right = contract.get("right", "?")
        strike_dollars = strike_raw / 1000.0

        # Filter to ±range from spot
        if abs(strike_dollars - spot) > strike_range:
            continue
        # Filter to correct strike interval
        if strike_interval > 1 and strike_dollars % strike_interval != 0:
            continue

        # Find the 10:00 AM tick (ms_of_day == 36000000)
        tick_10am = None
        for t in ticks:
            if isinstance(t, list) and len(t) >= 10 and t[0] == SNAPSHOT_TIME_MS:
                tick_10am = t
                break

        if not tick_10am:
            # Fallback: find closest tick to 10:00 AM (within 5 min)
            best = None
            best_dist = 300001  # 5 min in ms
            for t in ticks:
                if isinstance(t, list) and len(t) >= 10:
                    dist = abs(t[0] - SNAPSHOT_TIME_MS)
                    if dist < best_dist:
                        best_dist = dist
                        best = t
            tick_10am = best

        if not tick_10am:
            continue

        bid = tick_10am[3] if tick_10am[3] is not None else 0
        ask = tick_10am[7] if tick_10am[7] is not None else 0

        quote_map[(strike_raw, right)] = {"bid": bid, "ask": ask}

    time.sleep(CALL_DELAY)

    # ── 2. Fetch OI ──
    r2 = None
    for attempt in range(3):
        try:
            r2 = requests.get(f"{THETA_URL}/v2/bulk_hist/option/open_interest",
                              params={
                                  "root": option_root,
                                  "exp": exp_int,
                                  "start_date": exp_int,
                                  "end_date": exp_int,
                              },
                              timeout=60)
            break
        except Exception as e:
            if attempt < 2:
                print(f"    OI timeout for {exp_date}, retry {attempt+1}...")
                time.sleep(5)
            else:
                print(f"    Error fetching OI for {exp_date}: {e}")
                return None

    if r2.status_code != 200:
        print(f"    OI API error {r2.status_code} for {exp_date}")
        return None

    try:
        js_oi = r2.json()
    except Exception:
        print(f"    OI API invalid JSON for {exp_date}")
        return None

    oi_items = js_oi.get("response", [])
    # OI format: [ms_of_day, open_interest, date] -> tick[1] = OI

    oi_map = {}
    for item in oi_items:
        contract = item.get("contract", {})
        ticks = item.get("ticks", [])
        if not ticks:
            continue
        tick = ticks[-1] if isinstance(ticks[0], list) else ticks
        if not isinstance(tick, list) or len(tick) < 2:
            continue

        strike_raw = contract.get("strike", 0)
        right = contract.get("right", "?")
        strike_dollars = strike_raw / 1000.0

        # Same filter
        if abs(strike_dollars - spot) > strike_range:
            continue
        if strike_interval > 1 and strike_dollars % strike_interval != 0:
            continue

        oi_map[(strike_raw, right)] = tick[1]

    # ── 3. Merge + compute gamma ──
    T = GAMMA_T_HOURS / (365.0 * 24.0)  # 6 hours in year-fraction

    records = []
    all_keys = set(quote_map.keys()) | set(oi_map.keys())

    for (strike_raw, right) in all_keys:
        if right not in ("C", "P"):
            continue

        quote_info = quote_map.get((strike_raw, right), {})
        oi = oi_map.get((strike_raw, right), 0)

        bid = quote_info.get("bid", 0) or 0
        ask = quote_info.get("ask", 0) or 0
        strike_dollars = strike_raw / 1000.0

        # Mid-price for IV/gamma computation
        mid = 0
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
        elif ask > 0:
            mid = ask
        elif bid > 0:
            mid = bid

        # Compute IV and gamma
        iv = None
        gamma = 0.0
        if mid > 0 and spot > 0 and strike_dollars > 0:
            iv = implied_vol(spot, strike_dollars, T, RISK_FREE_RATE, mid, right)
            if iv and iv > 0.01:
                gamma = bs_gamma(spot, strike_dollars, T, RISK_FREE_RATE, iv)

        record = {
            "strike": strike_raw,
            "strike_dollars": strike_dollars,
            "right": right,
            "bid": bid,
            "ask": ask,
            "mid": round(mid, 2),
            "open_interest": oi,
            "gamma": gamma,
            "iv": iv,
        }
        records.append(record)

    # Sort by strike then right
    records.sort(key=lambda r: (r["strike_dollars"], r["right"]))

    return records if records else None


# ── Download ────────────────────────────────────────────────────────

def download(symbol="SPX", months=12, start_date_str=None):
    """Main download loop. Downloads every trading day in the date range."""
    global SYM_DIR

    cfg = SYMBOLS[symbol]
    option_root = cfg["option_root"]
    strike_interval = cfg["strike_interval"]
    strike_range = cfg["strike_range"]
    price_type = cfg["price_type"]
    n_strikes = strike_range * 2 // strike_interval

    print(f"=== {symbol} 0DTE GEX Downloader (ThetaData Value Plan) ===")
    print(f"Option root: {option_root}")
    print(f"Snapshot time: 10:00 AM ET")
    print(f"Strike range: +/-{strike_range} pts ({n_strikes} strikes at ${strike_interval})")
    print(f"Gamma T: {GAMMA_T_HOURS} hours (10:00->16:00)")
    print(f"Data dir: {SYM_DIR}")
    print()

    # Phase 0: Get all expirations (= trading days with 0DTE)
    print(f"Fetching {option_root} expirations list...")
    all_exps = fetch_expirations(option_root)
    time.sleep(CALL_DELAY)

    if not all_exps:
        print(f"ERROR: No {option_root} expirations returned. Check Terminal connection.")
        return

    # Filter to our date range
    end_dt = date.today()
    if start_date_str:
        start_dt = date.fromisoformat(start_date_str)
    else:
        start_dt = end_dt - timedelta(days=months * 30)

    start_int = fmt_date(start_dt)
    end_int = fmt_date(end_dt)

    trading_days = sorted([
        parse_theta_date(e) for e in all_exps
        if start_int <= e <= end_int
    ])

    if not trading_days:
        print(f"No {option_root} expirations between {start_dt} and {end_dt}")
        return

    print(f"Trading days: {len(trading_days)} ({trading_days[0]} to {trading_days[-1]})")

    # Save expirations cache
    exp_cache_file = os.path.join(SYM_DIR, "expirations.json")
    with open(exp_cache_file, "w") as f:
        json.dump(all_exps, f)
    print(f"Expirations cached: {len(all_exps)} total\n")

    # Phase 1: Daily prices
    print(f"--- Phase 1: {symbol} daily prices ---")
    price_file = os.path.join(SYM_DIR, "prices", f"{symbol}.json")
    os.makedirs(os.path.join(SYM_DIR, "prices"), exist_ok=True)

    if os.path.exists(price_file):
        print(f"  Already exists, loading...")
        with open(price_file) as f:
            price_bars = json.load(f)
    else:
        price_bars = fetch_prices(symbol, start_dt - timedelta(days=7), end_dt, price_type)
        time.sleep(CALL_DELAY)
        if price_bars:
            with open(price_file, "w") as f:
                json.dump(price_bars, f)
            print(f"  Saved {len(price_bars)} daily bars")
        else:
            print(f"  WARNING: Could not fetch {symbol} prices. Will skip spot lookup.")
            price_bars = []

    # Build price lookup
    price_map = {int(b["date"]): b for b in price_bars}
    print(f"  Price map: {len(price_map)} days\n")

    # Phase 2: 10:00 AM option chains for each trading day
    print("--- Phase 2: 10:00 AM option chains (bid/ask + OI + BS gamma) ---")
    os.makedirs(os.path.join(SYM_DIR, "options"), exist_ok=True)

    total_api_calls = 0
    total_saved = 0
    total_skipped = 0
    total_empty = 0

    for di, trade_date in enumerate(trading_days):
        out_file = os.path.join(SYM_DIR, "options", f"{trade_date}_0dte.json")

        if os.path.exists(out_file):
            total_skipped += 1
            continue

        # Get spot price for this date (use open for 10 AM approximation)
        trade_int = fmt_date(trade_date)
        bar = price_map.get(trade_int)
        if not bar:
            # Try adjacent days
            for offset in [-1, 1, -2, 2]:
                alt = trade_date + timedelta(days=offset)
                bar = price_map.get(fmt_date(alt))
                if bar:
                    break

        if not bar:
            # No price data -- write empty and skip
            with open(out_file, "w") as f:
                json.dump([], f)
            total_empty += 1
            pct = (di + 1) / len(trading_days) * 100
            print(f"  [{di+1}/{len(trading_days)}] {trade_date}: no spot price ({pct:.0f}%)")
            continue

        # Use open price as closer to 10 AM than close
        spot = bar["open"]

        records = fetch_10am_chain(trade_date, spot, option_root, strike_interval, strike_range)
        total_api_calls += 2
        time.sleep(1.0)  # breathing room between days to avoid terminal overload

        if records and len(records) > 0:
            with open(out_file, "w") as f:
                json.dump(records, f)
            total_saved += 1

            n_str = len(set(r["strike_dollars"] for r in records))
            n_oi = len([r for r in records if r["open_interest"] > 0])

            pct = (di + 1) / len(trading_days) * 100
            print(f"  [{di+1}/{len(trading_days)}] {trade_date}: {n_str} strikes, "
                  f"{n_oi} with OI, spot={spot:.1f} ({pct:.0f}%)")
        else:
            with open(out_file, "w") as f:
                json.dump([], f)
            total_empty += 1
            pct = (di + 1) / len(trading_days) * 100
            print(f"  [{di+1}/{len(trading_days)}] {trade_date}: no data ({pct:.0f}%)")

    print(f"\n=== {symbol} Download Complete ===")
    print(f"  Trading days: {len(trading_days)}")
    print(f"  API calls: {total_api_calls}")
    print(f"  Files saved (with data): {total_saved}")
    print(f"  Files empty (no data): {total_empty}")
    print(f"  Files skipped (existing): {total_skipped}")
    print(f"  Data directory: {os.path.abspath(SYM_DIR)}")


# ── Test / Format Detection ────────────────────────────────────────

def test_format():
    """Show raw API response formats for SPXW to verify endpoints work."""
    print("=== Testing ThetaData SPXW Response Formats (Value Plan) ===\n")

    # Find a recent valid expiration
    exps = fetch_spxw_expirations()
    time.sleep(CALL_DELAY)

    if not exps:
        print("ERROR: No SPXW expirations returned")
        return

    # Use the most recent past expiration
    today_int = fmt_date(date.today())
    past_exps = [e for e in exps if e < today_int]
    if not past_exps:
        print("No past SPXW expirations found")
        return

    exp_int = past_exps[-1]
    exp_date = parse_theta_date(exp_int)
    print(f"Test date: {exp_date} (most recent past 0DTE)\n")

    # 1. Bulk Quote at 10:00 AM
    print("--- Bulk Quote (10:00 AM) ---")
    try:
        r = requests.get(f"{THETA_URL}/v2/bulk_hist/option/quote",
                         params={
                             "root": "SPXW",
                             "exp": exp_int,
                             "start_date": exp_int,
                             "end_date": exp_int,
                             "ivl": 60000,
                             "start_time": SNAPSHOT_TIME_MS,
                             "end_time": SNAPSHOT_TIME_MS,
                         },
                         timeout=30)
        js = r.json()
        print(f"Status: {r.status_code}")
        print(f"Format: {js.get('header', {}).get('format')}")
        items = js.get("response", [])
        print(f"Items: {len(items)}")
        if items:
            print(f"Contract sample: {items[0].get('contract')}")
            ticks = items[0].get("ticks", [])
            if ticks:
                tick = ticks[0] if isinstance(ticks[0], list) else ticks
                print(f"Tick ({len(tick)} fields): {tick}")
    except Exception as e:
        print(f"Error: {e}")
    print()
    time.sleep(CALL_DELAY)

    # 2. Bulk OI
    print("--- Bulk OI ---")
    try:
        r = requests.get(f"{THETA_URL}/v2/bulk_hist/option/open_interest",
                         params={
                             "root": "SPXW",
                             "exp": exp_int,
                             "start_date": exp_int,
                             "end_date": exp_int,
                         },
                         timeout=30)
        js = r.json()
        print(f"Status: {r.status_code}")
        print(f"Format: {js.get('header', {}).get('format')}")
        items = js.get("response", [])
        print(f"Items: {len(items)}")
        if items:
            print(f"Contract sample: {items[0].get('contract')}")
            ticks = items[0].get("ticks", [])
            if ticks:
                tick = ticks[0] if isinstance(ticks[0], list) else ticks
                print(f"Tick ({len(tick)} fields): {tick}")
    except Exception as e:
        print(f"Error: {e}")
    print()
    time.sleep(CALL_DELAY)

    # 3. SPX daily price
    print("--- SPX Daily Price ---")
    for root in ["SPX", "$SPX"]:
        try:
            r = requests.get(f"{THETA_URL}/v2/hist/stock/eod",
                             params={
                                 "root": root,
                                 "start_date": exp_int,
                                 "end_date": exp_int,
                             },
                             timeout=15)
            js = r.json()
            err = js.get("header", {}).get("error_type")
            print(f"root={root}: status={r.status_code}, error={err}")
            resp = js.get("response", [])
            if resp:
                tick = resp[0] if isinstance(resp[0], list) else resp
                print(f"  Format: {js.get('header', {}).get('format')}")
                print(f"  Tick: {tick}")
                break
        except Exception as e:
            print(f"  root={root}: {e}")
        time.sleep(CALL_DELAY)
    print()

    # 4. Try Greeks endpoint (might work on Value)
    print("--- Greeks 1st Order (testing Value plan access) ---")
    try:
        r = requests.get(f"{THETA_URL}/v2/bulk_hist/option/greeks",
                         params={
                             "root": "SPXW",
                             "exp": exp_int,
                             "start_date": exp_int,
                             "end_date": exp_int,
                             "ivl": 60000,
                             "start_time": SNAPSHOT_TIME_MS,
                             "end_time": SNAPSHOT_TIME_MS,
                         },
                         timeout=30)
        js = r.json()
        print(f"Status: {r.status_code}")
        err = js.get("header", {}).get("error_type")
        print(f"Error: {err}")
        items = js.get("response", [])
        print(f"Items: {len(items)}")
        if items:
            print(f"Contract: {items[0].get('contract')}")
            ticks = items[0].get("ticks", [])
            if ticks:
                tick = ticks[0] if isinstance(ticks[0], list) else ticks
                print(f"Tick ({len(tick)} fields): {tick}")
            print(">>> Greeks endpoint WORKS on Value plan! Can skip BS computation.")
        else:
            print(">>> Greeks endpoint returned empty. Using BS computation.")
    except Exception as e:
        print(f"Error: {e}")
        print(">>> Greeks endpoint not available. Using BS computation.")
    print()

    # 5. SPXW expirations sample
    print("--- SPXW Expirations ---")
    print(f"Total: {len(exps)}")
    print(f"First 5: {exps[:5]}")
    print(f"Last 5: {exps[-5:]}")


def test_download(target_date=None):
    """Test with 1 date — full pipeline with GEX preview.
    If target_date given (date object), uses that. Otherwise uses most recent."""
    global SPX_DIR
    print("=== Test Download (SPXW 0DTE, 1 day, with BS Gamma) ===\n")

    if target_date:
        exp_date = target_date
        exp_int = fmt_date(exp_date)
        print(f"Test date: {exp_date} (user-specified)")
    else:
        # Find most recent past expiration
        exps = fetch_spxw_expirations()
        time.sleep(CALL_DELAY)

        if not exps:
            print("ERROR: No SPXW expirations")
            return

        today_int = fmt_date(date.today())
        past_exps = [e for e in exps if e < today_int]
        if not past_exps:
            print("No past expirations found")
            return

        exp_int = past_exps[-1]
        exp_date = parse_theta_date(exp_int)
        print(f"Test date: {exp_date} (most recent)")

    # Get spot
    bars = fetch_spx_prices(exp_date - timedelta(days=3), exp_date + timedelta(days=1))
    time.sleep(CALL_DELAY)

    if not bars:
        print("No SPX price data!")
        return

    # Find the matching day's bar
    day_bar = None
    for b in bars:
        if b["date"] == exp_int:
            day_bar = b
            break
    if not day_bar:
        day_bar = bars[-1]

    spot = day_bar["open"]
    print(f"Spot (open): {spot:.2f}")
    print(f"Day range: {day_bar['low']:.2f} - {day_bar['high']:.2f}")
    print(f"Close: {day_bar['close']:.2f}\n")

    # Fetch 10 AM chain
    print(f"Fetching 10:00 AM chain for SPXW {exp_date}...")
    records = fetch_10am_chain(exp_date, spot)

    if not records:
        print("No data returned!")
        return

    calls = [r for r in records if r["right"] == "C"]
    puts = [r for r in records if r["right"] == "P"]
    with_oi = [r for r in records if r["open_interest"] > 0]
    with_gamma = [r for r in records if r["gamma"] > 0]

    print(f"Contracts: {len(records)} ({len(calls)} C, {len(puts)} P)")
    print(f"With OI > 0: {len(with_oi)}")
    print(f"With gamma: {len(with_gamma)}")

    # Show ATM sample
    atm_records = [r for r in records if r["right"] == "C"]
    if atm_records:
        atm = min(atm_records, key=lambda r: abs(r["strike_dollars"] - spot))
        print(f"\nATM Call (strike={atm['strike_dollars']:.0f}):")
        print(f"  Bid: ${atm['bid']:.2f}  Ask: ${atm['ask']:.2f}  Mid: ${atm['mid']:.2f}")
        print(f"  OI: {atm['open_interest']:,}")
        print(f"  IV: {atm['iv']:.2%}" if atm['iv'] else "  IV: N/A")
        print(f"  Gamma: {atm['gamma']:.6f}")

    # GEX computation and preview
    gex_by_strike = {}
    for r in records:
        k = r["strike_dollars"]
        g = r["gamma"] * r["open_interest"] * 100
        if r["right"] == "P":
            g = -g
        gex_by_strike[k] = gex_by_strike.get(k, 0) + g

    if gex_by_strike:
        sorted_gex = sorted(gex_by_strike.items(), key=lambda x: x[0])

        print(f"\n{'='*60}")
        print(f"  GEX LEVELS (spot={spot:.0f})")
        print(f"{'='*60}")

        # Find key levels
        neg_levels = [(k, v) for k, v in sorted_gex if v < 0]
        pos_levels = [(k, v) for k, v in sorted_gex if v > 0]
        neg_levels.sort(key=lambda x: x[1])  # most negative first
        pos_levels.sort(key=lambda x: x[1], reverse=True)  # most positive first

        print(f"\n  Top 3 -GEX (support/magnets below):")
        for k, v in neg_levels[:3]:
            dist = k - spot
            label = "BELOW" if dist < 0 else "ABOVE"
            print(f"    ${k:.0f}  GEX={v:>12,.0f}  ({dist:+.0f} pts {label} spot)")

        print(f"\n  Top 3 +GEX (magnets above):")
        for k, v in pos_levels[:3]:
            dist = k - spot
            label = "BELOW" if dist < 0 else "ABOVE"
            print(f"    ${k:.0f}  GEX={v:>12,.0f}  ({dist:+.0f} pts {label} spot)")

        # Zero-gamma line (where net GEX crosses from neg to pos)
        for i in range(len(sorted_gex) - 1):
            k1, v1 = sorted_gex[i]
            k2, v2 = sorted_gex[i + 1]
            if v1 < 0 and v2 > 0 and k1 <= spot + 50:
                flip = (k1 + k2) / 2
                print(f"\n  Zero-gamma line: ~${flip:.0f} (GEX flips neg->pos)")
                break

        # Total GEX
        total = sum(v for _, v in sorted_gex)
        regime = "POSITIVE (mean-reverting)" if total > 0 else "NEGATIVE (trending)"
        print(f"\n  Total GEX: {total:,.0f} -> {regime}")

        # Full table (near spot)
        print(f"\n  Strike-by-strike GEX (near spot):")
        print(f"  {'Strike':>8} {'Net GEX':>12} {'Bar':>30}")
        for k, v in sorted_gex:
            if abs(k - spot) <= 50:
                bar_len = int(abs(v) / max(abs(v2) for _, v2 in sorted_gex) * 25)
                bar_char = "+" if v > 0 else "-"
                gex_bar = bar_char * bar_len
                marker = " << SPOT" if abs(k - spot) < 3 else ""
                print(f"  ${k:>7.0f} {v:>12,.0f}  {gex_bar}{marker}")

        # Check against day's price action
        print(f"\n  Day's price action:")
        print(f"    Open:  {day_bar['open']:.2f}")
        print(f"    High:  {day_bar['high']:.2f}")
        print(f"    Low:   {day_bar['low']:.2f}")
        print(f"    Close: {day_bar['close']:.2f}")

        if neg_levels:
            strongest_neg = neg_levels[0][0]
            if day_bar["low"] <= strongest_neg:
                recovered = day_bar["close"] > strongest_neg
                print(f"\n    >> Price hit strongest -GEX at ${strongest_neg:.0f}")
                print(f"    >> {'BOUNCED back above' if recovered else 'Stayed below'} (close={day_bar['close']:.2f})")

    # Save test file
    out_file = os.path.join(SPX_DIR, "options", f"{exp_date}_0dte.json")
    os.makedirs(os.path.join(SPX_DIR, "options"), exist_ok=True)
    with open(out_file, "w") as f:
        json.dump(records, f)
    print(f"\n  Saved to: {out_file}")


# ── Main ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download SPX 0DTE GEX data from ThetaData")
    parser.add_argument("--months", type=int, default=12, help="Months of history (default: 12)")
    parser.add_argument("--test", action="store_true", help="Test with 1 day, show GEX preview")
    parser.add_argument("--test-format", action="store_true", help="Show raw response format")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--resume", action="store_true", help="Resume (skip existing files)")
    parser.add_argument("--test-date", type=str, help="Test specific date (YYYY-MM-DD)")
    parser.add_argument("--symbol", type=str, default="SPX",
                        choices=list(SYMBOLS.keys()),
                        help="Symbol to download (default: SPX)")
    args = parser.parse_args()

    symbol = args.symbol.upper()
    if symbol not in SYMBOLS:
        print(f"ERROR: Unknown symbol {symbol}. Available: {list(SYMBOLS.keys())}")
        sys.exit(1)

    SYM_DIR = os.path.join(DATA_DIR, symbol.lower())
    os.makedirs(SYM_DIR, exist_ok=True)

    print("Checking ThetaData Terminal connection...")
    if not check_terminal():
        print("\nERROR: ThetaData Terminal not running!")
        print("Start it with: java -jar ThetaTerminal.jar your@email.com your_password")
        sys.exit(1)
    print("Connected!\n")

    if args.test_format:
        test_format()
    elif args.test_date:
        target = date.fromisoformat(args.test_date)
        test_download(target_date=target)
    elif args.test:
        test_download()
    else:
        download(symbol=symbol, months=args.months, start_date_str=args.start)
