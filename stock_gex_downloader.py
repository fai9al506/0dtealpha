"""
Stock GEX Historical Data Downloader — ThetaData API (Value Plan)

Downloads EOD options data (OI + prices per strike) and computes gamma
via Black-Scholes for backtesting the stock GEX support/magnet strategy.

Value plan provides: bulk_hist/option/eod (OHLCV), bulk_hist/option/open_interest,
hist/stock/eod. Greeks endpoint requires Standard+ so we compute gamma ourselves.

Prerequisites:
  1. ThetaData account (Value plan, $40/mo)
  2. Java 11+ installed
  3. ThetaData Terminal running:
     java -jar ThetaTerminal.jar your@email.com your_password
  4. pip install requests

Usage:
  python stock_gex_downloader.py                  # Download last 12 months
  python stock_gex_downloader.py --months 6       # Download last 6 months
  python stock_gex_downloader.py --test           # Test with 1 stock, 1 date
  python stock_gex_downloader.py --test-format    # Show raw response format
  python stock_gex_downloader.py --resume         # Resume interrupted download
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

# Rate limit: 10 calls/sec on paid plan, be safe
CALL_DELAY = 0.15
RISK_FREE_RATE = 0.045  # ~4.5% Fed Funds rate


# ── Black-Scholes Gamma ────────────────────────────────────────────

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


def get_trade_dates(months_back=12):
    """Generate (Monday, Tuesday) pairs for each week.

    Tuesday OI reflects positions opened on Monday — critical for backtesting.
    Returns list of (date, day_label) tuples.
    """
    dates = []
    for monday in get_mondays(months_back):
        dates.append((monday, "mon"))
        dates.append((monday + timedelta(days=1), "tue"))
    return dates


def get_target_expirations(trade_date):
    """Calculate weekly (this Friday) and opex (nearest 3rd Friday) expirations."""
    targets = []

    # Weekly: nearest Friday >= trade_date
    days_to_fri = (4 - trade_date.weekday()) % 7
    if days_to_fri == 0:
        days_to_fri = 7
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
                targets[0] = (weekly, "opex")
            else:
                targets.append((third_friday, "opex"))
            break

    return targets


def check_terminal():
    """Check if ThetaData Terminal is running (Value plan compatible)."""
    try:
        r = requests.get(f"{THETA_URL}/v2/list/expirations",
                         params={"root": "AAPL"}, timeout=5)
        return r.status_code == 200
    except requests.ConnectionError:
        return False


# ── ThetaData API ───────────────────────────────────────────────────

def fetch_expirations(root):
    """Fetch available expirations for a stock."""
    url = f"{THETA_URL}/v2/list/expirations"
    try:
        r = requests.get(url, params={"root": root}, timeout=15)
        if r.status_code != 200:
            return []
        js = r.json()
        return js.get("response", [])
    except Exception:
        return []


def fetch_options_eod_and_oi(root, exp_date, trade_date, spot):
    """Fetch EOD prices + OI for all strikes, compute gamma via BS.

    Makes 2 API calls: bulk_hist/option/eod + bulk_hist/option/open_interest.
    Tries trade_date, then +1 day, +2 days (handles holiday Mondays).
    Returns list of dicts: {strike, right, close, open_interest, gamma, iv, ...}
    """
    exp_int = fmt_date(exp_date)

    # ── Fetch EOD prices (try trade_date, then +1, +2 for holidays) ──
    eod_items = None
    actual_trade_date = trade_date
    for day_offset in range(3):
        try_date = trade_date + timedelta(days=day_offset)
        try_int = fmt_date(try_date)
        try:
            r = requests.get(f"{THETA_URL}/v2/bulk_hist/option/eod",
                             params={"root": root, "exp": exp_int,
                                     "start_date": try_int, "end_date": try_int},
                             timeout=30)
        except Exception as e:
            print(f"    Error fetching EOD {root} exp={exp_date}: {e}")
            time.sleep(CALL_DELAY)
            continue

        if r.status_code == 200:
            try:
                js_eod = r.json()
            except Exception:
                time.sleep(CALL_DELAY)
                continue
            items = js_eod.get("response", [])
            if items:
                eod_items = items
                actual_trade_date = try_date
                break
        time.sleep(CALL_DELAY)

    if not eod_items:
        return None

    # Parse header format: ["ms_of_day","ms_of_day2","open","high","low","close","volume",
    #   "count","bid_size","bid_exchange","bid","bid_condition",
    #   "ask_size","ask_exchange","ask","ask_condition","date"]
    # Indices: open=2, high=3, low=4, close=5, volume=6, bid=10, ask=14, date=16

    # Build price lookup: (strike, right) -> {close, bid, ask, volume}
    price_map = {}
    for item in eod_items:
        contract = item.get("contract", {})
        ticks = item.get("ticks", [])
        if not ticks:
            continue
        tick = ticks[-1] if isinstance(ticks[0], list) else ticks
        if not isinstance(tick, list) or len(tick) < 17:
            continue

        key = (contract.get("strike", 0), contract.get("right", "?"))
        price_map[key] = {
            "close": tick[5],
            "bid": tick[10],
            "ask": tick[14],
            "volume": tick[6],
        }

    time.sleep(CALL_DELAY)

    # ── Fetch OI (use same actual_trade_date that EOD succeeded on) ──
    trade_int = fmt_date(actual_trade_date)
    try:
        r2 = requests.get(f"{THETA_URL}/v2/bulk_hist/option/open_interest",
                          params={"root": root, "exp": exp_int,
                                  "start_date": trade_int, "end_date": trade_int},
                          timeout=30)
    except Exception as e:
        print(f"    Error fetching OI {root} exp={exp_date}: {e}")
        return None

    if r2.status_code != 200:
        return None
    try:
        js_oi = r2.json()
    except Exception:
        return None

    oi_items = js_oi.get("response", [])
    # OI format: ["ms_of_day","open_interest","date"] → tick[1] = OI

    # Build OI lookup: (strike, right) -> OI
    oi_map = {}
    for item in oi_items:
        contract = item.get("contract", {})
        ticks = item.get("ticks", [])
        if not ticks:
            continue
        tick = ticks[-1] if isinstance(ticks[0], list) else ticks
        if not isinstance(tick, list) or len(tick) < 2:
            continue
        key = (contract.get("strike", 0), contract.get("right", "?"))
        oi_map[key] = tick[1]

    # ── Merge + compute gamma ──
    dte = (exp_date - trade_date).days
    T = max(dte / 365.0, 1 / 365.0)  # min 1 day

    records = []
    all_keys = set(price_map.keys()) | set(oi_map.keys())

    for (strike_raw, right) in all_keys:
        if right not in ("C", "P"):
            continue

        price_info = price_map.get((strike_raw, right), {})
        oi = oi_map.get((strike_raw, right), 0)

        opt_close = price_info.get("close", 0)
        strike_dollars = strike_raw / 1000.0

        # Use midpoint if close is 0
        mid = None
        bid = price_info.get("bid", 0)
        ask = price_info.get("ask", 0)
        if opt_close <= 0 and bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            opt_close = mid

        # Compute IV and gamma
        iv = None
        gamma = 0.0
        if opt_close > 0 and spot > 0 and strike_dollars > 0:
            iv = implied_vol(spot, strike_dollars, T, RISK_FREE_RATE, opt_close, right)
            if iv and iv > 0.01:
                gamma = bs_gamma(spot, strike_dollars, T, RISK_FREE_RATE, iv)

        record = {
            "strike": strike_raw,
            "strike_dollars": strike_dollars,
            "right": right,
            "close": price_info.get("close", 0),
            "bid": bid,
            "ask": ask,
            "volume": price_info.get("volume", 0),
            "open_interest": oi,
            "gamma": gamma,
            "iv": iv,
        }
        records.append(record)

    return records if records else None


def fetch_stock_prices(root, start_date, end_date):
    """Fetch daily EOD for a stock (Value plan: hist/stock/eod)."""
    url = f"{THETA_URL}/v2/hist/stock/eod"
    params = {
        "root": root,
        "start_date": fmt_date(start_date),
        "end_date": fmt_date(end_date),
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            return None
        js = r.json()

        if js.get("header", {}).get("error_type"):
            return None

        # Format: [ms_of_day, ms_of_day2, open, high, low, close, volume, count,
        #          bid_size, bid_exchange, bid, bid_condition,
        #          ask_size, ask_exchange, ask, ask_condition, date]
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
        return bars

    except Exception as e:
        print(f"    Error fetching prices for {root}: {e}")
        return None


# ── Download ────────────────────────────────────────────────────────

def download(months=12, stocks=None):
    """Main download loop. Downloads Monday + Tuesday data for each week."""
    stocks = stocks or STOCKS
    trade_dates = get_trade_dates(months)
    mondays = get_mondays(months)

    print(f"=== Stock GEX Downloader (ThetaData Value Plan) ===")
    print(f"Stocks: {len(stocks)}")
    print(f"Weeks: {len(mondays)} ({mondays[0]} to {mondays[-1]})")
    print(f"Trade dates: {len(trade_dates)} (Mon + Tue per week)")
    print(f"Data dir: {DATA_DIR}")
    print(f"Gamma: computed via Black-Scholes (no Greeks endpoint on Value)")
    print()

    os.makedirs(f"{DATA_DIR}/prices", exist_ok=True)

    # Phase 1: Stock prices (one file per stock, entire date range)
    print("--- Phase 1: Daily stock prices ---")
    start_dt = mondays[0] - timedelta(days=7)
    end_dt = date.today()
    prices_done = 0
    prices_new = 0

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
            prices_new += 1
        else:
            print(f"  [{i+1}/{len(stocks)}] {stock}: no data")
        prices_done += 1

    print(f"  Prices: {prices_done}/{len(stocks)} done ({prices_new} new)\n")

    # Phase 2: Options data (per stock per trade_date per expiration)
    print("--- Phase 2: Options chain data (EOD + OI + BS gamma) ---")
    print("  Downloading Monday + Tuesday per week")
    total_api_calls = 0
    total_skipped = 0
    total_saved = 0
    total_empty = 0

    # Load price data for spot lookup
    price_cache = {}
    for stock in stocks:
        price_file = f"{DATA_DIR}/prices/{stock}.json"
        if os.path.exists(price_file):
            with open(price_file) as f:
                bars = json.load(f)
            price_cache[stock] = {int(b["date"]): b for b in bars}

    for di, (trade_date, day_label) in enumerate(trade_dates):
        # Use the Monday of this week for expiration targets
        monday = trade_date if day_label == "mon" else trade_date - timedelta(days=1)
        targets = get_target_expirations(monday)
        saved_this_date = 0
        trade_int = fmt_date(trade_date)

        for stock in stocks:
            stock_dir = f"{DATA_DIR}/options/{stock}"
            os.makedirs(stock_dir, exist_ok=True)

            # Get spot price for this trade date
            prices = price_cache.get(stock, {})
            bar = prices.get(trade_int)
            if not bar:
                for offset in range(1, 4):
                    alt = trade_date + timedelta(days=offset)
                    bar = prices.get(fmt_date(alt))
                    if bar:
                        break
            spot = bar["close"] if bar else 0

            for exp_date, label in targets:
                # File naming: YYYY-MM-DD_day_label.json (e.g. 2026-03-09_mon_weekly.json)
                out_file = f"{stock_dir}/{trade_date}_{day_label}_{label}.json"

                if os.path.exists(out_file):
                    total_skipped += 1
                    continue

                if spot <= 0:
                    with open(out_file, "w") as f:
                        json.dump([], f)
                    total_empty += 1
                    continue

                records = fetch_options_eod_and_oi(stock, exp_date, trade_date, spot)
                total_api_calls += 2
                time.sleep(CALL_DELAY)

                if records and len(records) > 0:
                    with open(out_file, "w") as f:
                        json.dump(records, f)
                    saved_this_date += 1
                    total_saved += 1
                else:
                    with open(out_file, "w") as f:
                        json.dump([], f)
                    total_empty += 1

        pct = (di + 1) / len(trade_dates) * 100
        print(f"  [{di+1}/{len(trade_dates)}] {trade_date} ({day_label}): {saved_this_date} files "
              f"({pct:.0f}% done, {total_api_calls} API calls, {total_saved} saved)")

    print(f"\n=== Download Complete ===")
    print(f"  API calls: {total_api_calls}")
    print(f"  Files saved (with data): {total_saved}")
    print(f"  Files empty (no data): {total_empty}")
    print(f"  Files skipped (existing): {total_skipped}")
    print(f"  Data directory: {os.path.abspath(DATA_DIR)}")


# ── Test / Format Detection ────────────────────────────────────────

def test_format():
    """Show raw API response formats for debugging."""
    print("=== Testing ThetaData Response Formats (Value Plan) ===\n")

    monday = date.today() - timedelta(days=date.today().weekday())
    if monday == date.today():
        monday -= timedelta(days=7)
    exp_date = monday + timedelta(days=(4 - monday.weekday()) % 7)

    print(f"Stock: AAPL | Trade date: {monday} | Expiration: {exp_date}\n")

    # Stock EOD
    print("--- Stock EOD ---")
    r = requests.get(f"{THETA_URL}/v2/hist/stock/eod",
                     params={"root": "AAPL", "start_date": fmt_date(monday),
                             "end_date": fmt_date(monday)}, timeout=15)
    js = r.json()
    print(f"Format: {js.get('header', {}).get('format')}")
    if js.get("response"):
        print(f"Sample: {js['response'][0]}")
    print()

    # Options EOD
    print("--- Options Bulk EOD ---")
    r = requests.get(f"{THETA_URL}/v2/bulk_hist/option/eod",
                     params={"root": "AAPL", "exp": fmt_date(exp_date),
                             "start_date": fmt_date(monday), "end_date": fmt_date(monday)},
                     timeout=30)
    js = r.json()
    print(f"Format: {js.get('header', {}).get('format')}")
    items = js.get("response", [])
    print(f"Items: {len(items)}")
    if items:
        print(f"Contract: {items[0].get('contract')}")
        tick = items[0].get("ticks", [[]])[0]
        print(f"Tick ({len(tick)} fields): {tick}")
    print()

    # Options OI
    print("--- Options Bulk OI ---")
    r = requests.get(f"{THETA_URL}/v2/bulk_hist/option/open_interest",
                     params={"root": "AAPL", "exp": fmt_date(exp_date),
                             "start_date": fmt_date(monday), "end_date": fmt_date(monday)},
                     timeout=30)
    js = r.json()
    print(f"Format: {js.get('header', {}).get('format')}")
    items = js.get("response", [])
    print(f"Items: {len(items)}")
    if items:
        print(f"Contract: {items[0].get('contract')}")
        tick = items[0].get("ticks", [[]])[0]
        print(f"Tick ({len(tick)} fields): {tick}")
    print()

    # Expirations
    print("--- Expirations ---")
    exps = fetch_expirations("AAPL")
    print(f"Count: {len(exps)}")
    if exps:
        print(f"Sample (first 5): {exps[:5]}")


def test_download():
    """Test with 1 stock, 1 Monday — full pipeline with gamma."""
    print("=== Test Download (AAPL, 1 Monday, with BS Gamma) ===\n")

    monday = date.today() - timedelta(days=date.today().weekday())
    if monday == date.today():
        monday -= timedelta(days=7)

    targets = get_target_expirations(monday)
    print(f"Monday: {monday}")
    print(f"Expirations: {[(str(e), l) for e, l in targets]}")

    # Get spot
    bars = fetch_stock_prices("AAPL", monday, monday + timedelta(days=4))
    if not bars:
        print("No price data!")
        return

    spot = bars[0]["close"]
    print(f"Spot: ${spot:.2f}\n")

    for exp_date, label in targets:
        print(f"Fetching AAPL {label} (exp={exp_date})...")
        records = fetch_options_eod_and_oi("AAPL", exp_date, monday, spot)

        if records:
            calls = [r for r in records if r["right"] == "C"]
            puts = [r for r in records if r["right"] == "P"]
            with_oi = [r for r in records if r["open_interest"] > 0]
            with_gamma = [r for r in records if r["gamma"] > 0]

            print(f"  Contracts: {len(records)} ({len(calls)} C, {len(puts)} P)")
            print(f"  With OI > 0: {len(with_oi)}")
            print(f"  With gamma computed: {len(with_gamma)}")

            # Show ATM sample
            atm = min(records, key=lambda r: abs(r["strike_dollars"] - spot))
            print(f"\n  ATM sample (strike=${atm['strike_dollars']:.0f}):")
            print(f"    Right: {atm['right']}")
            print(f"    Close: ${atm['close']:.2f}")
            print(f"    OI: {atm['open_interest']}")
            print(f"    IV: {atm['iv']:.2%}" if atm['iv'] else "    IV: N/A")
            print(f"    Gamma: {atm['gamma']:.6f}")

            # GEX preview
            gex_by_strike = {}
            for r in records:
                k = r["strike_dollars"]
                g = r["gamma"] * r["open_interest"] * 100
                if r["right"] == "P":
                    g = -g
                gex_by_strike[k] = gex_by_strike.get(k, 0) + g

            if gex_by_strike:
                sorted_gex = sorted(gex_by_strike.items(), key=lambda x: x[1])
                print(f"\n  GEX preview:")
                print(f"    Strongest -GEX: ${sorted_gex[0][0]:.0f} ({sorted_gex[0][1]:,.0f})")
                print(f"    Strongest +GEX: ${sorted_gex[-1][0]:.0f} ({sorted_gex[-1][1]:,.0f})")

                # Near-spot GEX
                near = [(k, v) for k, v in gex_by_strike.items()
                        if abs(k - spot) < spot * 0.1]
                if near:
                    near.sort(key=lambda x: x[1])
                    print(f"    Near-spot -GEX: ${near[0][0]:.0f} ({near[0][1]:,.0f})")
                    print(f"    Near-spot +GEX: ${near[-1][0]:.0f} ({near[-1][1]:,.0f})")
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
    parser.add_argument("--resume", action="store_true", help="Resume (skip existing files)")
    args = parser.parse_args()

    print("Checking ThetaData Terminal connection...")
    if not check_terminal():
        print("\nERROR: ThetaData Terminal not running!")
        print("Start it with: java -jar ThetaTerminal.jar your@email.com your_password")
        sys.exit(1)
    print("Connected!\n")

    if args.test_format:
        test_format()
    elif args.test:
        test_download()
    else:
        download(months=args.months, stocks=args.stocks)
