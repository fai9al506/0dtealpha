"""
Stock GEX Live Scanner — Support Bounce Strategy

Scans 52 stocks for GEX support bounce setups.
Two-layer: 30-min GEX scan + 1-2 min spot monitoring.

Strategy: Buy OTM weekly call at -GEX strike when stock dips 1% below -GEX.
Filters: ratio>3, support below, magnet above, spot above -GEX, skip 09:30.
Targets: T1 = -GEX recovery, T2 = +GEX magnet.

Self-contained module. Receives engine, api_get, send_telegram via init().
"""

import json
import math
import os
import time
import traceback
from datetime import datetime, date, timedelta, time as dtime
from threading import Lock
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

# ── Config ──────────────────────────────────────────────────────────

ET = ZoneInfo("US/Eastern")

STOCKS = [
    # Tier 1 - Mega options volume
    "AAPL", "MSFT", "GOOGL", "GOOG", "META", "NVDA", "AMZN", "NFLX", "TSLA",
    "AMD", "PLTR",
    # Tier 2 - High options volume
    "INTC", "MU", "QCOM", "AVGO", "SMCI", "MSTR", "ORCL", "GS",
    "PYPL", "SOFI", "COIN", "AFRM", "UPST", "SQ",
    "SNAP", "SHOP", "ROKU", "RBLX", "LULU", "SNOW",
    "AMC", "GME", "MARA", "LCID", "RIVN", "RKLB",
    # Tier 3 - Active options volume
    "BAC", "JPM", "WFC", "C", "GE",
    "BA", "DIS", "GM", "T", "AAL", "CCL", "F",
    "IBM", "ABNB", "PINS", "U",
    "OXY", "PFE", "JNJ",
    "UBER", "CVNA", "AI", "BABA", "COST", "ENPH",
    # Tier 4 - Semi/Tech volume
    "TSM", "AMAT", "LRCX", "PDD", "WBD",
]

TIER_A = {
    "AFRM", "AI", "AMD", "AVGO", "BAC", "CCL", "CVNA", "GOOGL", "INTC",
    "LCID", "MARA", "MU", "PLTR", "PYPL", "QCOM", "ROKU", "SHOP", "SOFI", "TSLA",
}

# Filters
MIN_GEX_RATIO = 2.0
MIN_SUPPORT_BELOW = 1
MIN_MAGNETS_ABOVE = 1
ENTRY_OFFSET_PCT = 1.0  # entry at -GEX minus this %
SKIP_0930 = True
GEX_SIGNIFICANCE_THRESHOLD = 0.10  # level must be >= 10% of max

# ── Module State ────────────────────────────────────────────────────

_engine = None
_api_get = None
_send_telegram = None
_initialized = False
_lock = Lock()

# GEX levels per stock (refreshed every 30 min)
_gex_levels = {}  # {symbol: {highest_neg, lowest_pos, neg_strikes, pos_strikes, ratio, ...}}

# Watchlist: stocks passing all filters
_watchlist = {}  # {symbol: {levels + trigger_price}}

# Active trades being monitored
_active_trades = []  # [{symbol, entry_time, entry_price, strike, ...}]

# Completed trade log
_trade_log = []

# Status tracking
_last_scan_at = None
_last_monitor_at = None
_scan_count = 0
_today_trades = 0
_today_pnl = 0.0


# ── Black-Scholes helpers ──────────────────────────────────────────

def _norm_cdf(x):
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x / 2.0)
    return 0.5 * (1.0 + sign * y)


def _norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _bs_call_price(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)


def _implied_vol(S, K, T, r, market_price, tol=1e-6, max_iter=50):
    if market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return None
    sigma = 0.3
    for _ in range(max_iter):
        price = _bs_call_price(S, K, T, r, sigma)
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


def _bs_gamma(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    return _norm_pdf(d1) / (S * sigma * math.sqrt(T))


def _bs_delta(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.5
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    return _norm_cdf(d1)


# ── GEX Computation ────────────────────────────────────────────────

def _get_weekly_expiration():
    """Get this week's Friday expiration string (YYYY-MM-DD)."""
    now = datetime.now(ET)
    today = now.date()
    days_to_fri = (4 - today.weekday()) % 7
    if days_to_fri == 0 and now.hour >= 16:
        days_to_fri = 7  # after Friday close, use next week
    friday = today + timedelta(days=days_to_fri)
    return str(friday)


def _compute_stock_gex(symbol, chain_rows, spot):
    """Compute GEX levels for a stock from chain data.

    chain_rows: list of dicts with {Strike, Type (C/P), Gamma, OpenInterest, ...}
    Returns dict with levels or None if insufficient data.
    """
    # GEX per strike
    gex_by_strike = {}
    for r in chain_rows:
        k = r["Strike"]
        g = r.get("Gamma", 0) * r.get("OpenInterest", 0) * 100
        if r["Type"] == "P":
            g = -g
        gex_by_strike[k] = gex_by_strike.get(k, 0) + g

    if not gex_by_strike:
        return None

    pos = [(k, v) for k, v in gex_by_strike.items() if v > 0]
    neg = [(k, v) for k, v in gex_by_strike.items() if v < 0]
    if not pos or not neg:
        return None

    pos.sort(key=lambda x: x[1], reverse=True)
    neg.sort(key=lambda x: x[1])

    top_pos = pos[:3]
    top_neg = neg[:3]

    # Significance filter
    if top_pos:
        mx = top_pos[0][1]
        top_pos = [(k, v) for k, v in top_pos if v >= mx * GEX_SIGNIFICANCE_THRESHOLD]
    if top_neg:
        mx = abs(top_neg[0][1])
        top_neg = [(k, v) for k, v in top_neg if abs(v) >= mx * GEX_SIGNIFICANCE_THRESHOLD]

    if not top_pos or not top_neg:
        return None

    neg_strikes = sorted([k for k, v in top_neg])
    pos_strikes = sorted([k for k, v in top_pos])
    highest_neg = max(neg_strikes)
    lowest_pos = min(pos_strikes)

    if highest_neg >= lowest_pos:
        return None

    total_pos = sum(v for k, v in top_pos)
    total_neg = sum(abs(v) for k, v in top_neg)
    ratio = total_pos / total_neg if total_neg > 0 else 0

    zone_width = (lowest_pos - highest_neg) / highest_neg * 100
    support_below = [s for s in neg_strikes if s < highest_neg]
    magnets_above = [k for k, v in top_pos if k > highest_neg]

    return {
        "symbol": symbol,
        "spot": round(spot, 2),
        "neg_strikes": neg_strikes,
        "pos_strikes": pos_strikes,
        "neg_levels": [{"strike": k, "gex": round(v)} for k, v in top_neg],
        "pos_levels": [{"strike": k, "gex": round(v)} for k, v in top_pos],
        "highest_neg": highest_neg,
        "lowest_pos": lowest_pos,
        "ratio": round(ratio, 2),
        "zone_width": round(zone_width, 2),
        "n_support_below": len(support_below),
        "n_magnets_above": len(magnets_above),
        "total_pos_gex": round(total_pos),
        "total_neg_gex": round(total_neg),
    }


def _grade_setup(levels, day_name):
    """Grade the setup A+/A/B/C based on day + GEX structure.

    Grading (calibrated with real option prices from optionstrat):
      A+: Wed + ratio>3     96% WR, +243% real winner, EV +$459/trade (BEST)
      A:  Mon-Wed + ratio>2  93% WR, +57-243% real winner, reliable
      B:  Thu + ratio>2      20% WR, +370% real winner, but EV negative (-$12)
      C:  Fri + ratio>2      36% WR, +275% real winner, EV marginal (+$70)

    All grades fire signals. Grade determines:
      A+/A = TRADE (94% WR, proven profitable)
      B/C  = ALERT ONLY (low WR, optional lottery ticket)
    """
    ratio = levels["ratio"]
    if day_name == "Wed" and ratio >= 3:
        return "A+"
    elif day_name in ("Mon", "Tue", "Wed"):
        return "A"
    elif day_name == "Thu":
        return "B"
    else:  # Fri
        return "C"


def _passes_filters(levels):
    """Check if a stock's GEX levels pass all filters."""
    if levels["ratio"] < MIN_GEX_RATIO:
        return False, f"ratio {levels['ratio']} < {MIN_GEX_RATIO}"
    if levels["n_support_below"] < MIN_SUPPORT_BELOW:
        return False, f"no support below"
    if levels["n_magnets_above"] < MIN_MAGNETS_ABOVE:
        return False, f"no magnets above"
    if levels["spot"] < levels["highest_neg"]:
        return False, f"spot {levels['spot']} below -GEX {levels['highest_neg']}"
    return True, "pass"


# ── Chain Fetching (via TS API) ─────────────────────────────────────

def _fetch_chain(symbol, expiration, spot):
    """Fetch options chain for a stock via TS API.

    Uses same endpoint/format as main.py get_chain_rows().
    TS API returns: Legs[0].StrikePrice, OptionType, Gamma, OpenInterest, Delta, IV, etc.
    Returns list of {Strike, Type, Gamma, OpenInterest, Delta, IV, Bid, Ask, ...} or None.
    """
    if not _api_get:
        return None

    try:
        exp_str = expiration.replace("-", "")

        # Strike interval based on stock price
        if spot > 500:
            interval = 5
        elif spot > 100:
            interval = 2.5
        elif spot > 50:
            interval = 1
        else:
            interval = 0.5

        proximity = int(interval * 15)  # +/- 15 strikes

        # Try streaming endpoint first (same as SPX chain)
        params = {
            "spreadType": "Single",
            "enableGreeks": "true",
            "priceCenter": f"{spot:.2f}",
            "strikeProximity": proximity,
            "optionType": "All",
            "strikeInterval": interval,
            "expiration": exp_str,
        }

        rows = []

        # Try snapshot endpoint (more reliable for stocks)
        try:
            resp = _api_get(f"/marketdata/options/chains", params={
                "symbol": symbol,
                "enableGreeks": "true",
                "optionType": "All",
                "priceCenter": f"{spot:.2f}",
                "strikeProximity": proximity,
                "strikeInterval": interval,
                "spreadType": "Single",
                "expiration": exp_str,
            }, timeout=12)

            if resp and hasattr(resp, 'json'):
                js = resp.json()
                for it in js.get("Options", []):
                    legs = it.get("Legs") or []
                    leg0 = legs[0] if legs else {}
                    side = (leg0.get("OptionType") or it.get("OptionType") or "").lower()
                    side = "C" if side.startswith("c") else "P" if side.startswith("p") else "?"
                    if side == "?":
                        continue

                    strike = _safe_float(leg0.get("StrikePrice"))
                    gamma = _safe_float(it.get("Gamma") or it.get("TheoGamma"))
                    oi = _safe_float(it.get("OpenInterest") or it.get("DailyOpenInterest"))
                    delta = _safe_float(it.get("Delta") or it.get("TheoDelta"))
                    iv = _safe_float(it.get("ImpliedVolatility") or it.get("TheoIV"))
                    bid = _safe_float(it.get("Bid"))
                    ask = _safe_float(it.get("Ask"))
                    last = _safe_float(it.get("Last"))

                    if strike and oi:
                        rows.append({
                            "Strike": strike,
                            "Type": side,
                            "Gamma": gamma or 0,
                            "OpenInterest": oi,
                            "Delta": delta,
                            "IV": iv,
                            "Bid": bid,
                            "Ask": ask,
                            "Last": last,
                        })
            elif isinstance(resp, list):
                # Direct list response (streaming format)
                for it in resp:
                    if not isinstance(it, dict):
                        continue
                    legs = it.get("Legs") or []
                    leg0 = legs[0] if legs else {}
                    side = (leg0.get("OptionType") or it.get("OptionType") or "").lower()
                    side = "C" if side.startswith("c") else "P" if side.startswith("p") else "?"
                    if side == "?":
                        continue

                    strike = _safe_float(leg0.get("StrikePrice"))
                    gamma = _safe_float(it.get("Gamma") or it.get("TheoGamma"))
                    oi = _safe_float(it.get("OpenInterest") or it.get("DailyOpenInterest"))
                    delta = _safe_float(it.get("Delta") or it.get("TheoDelta"))

                    if strike and oi:
                        rows.append({
                            "Strike": strike,
                            "Type": side,
                            "Gamma": gamma or 0,
                            "OpenInterest": oi,
                            "Delta": delta,
                            "IV": _safe_float(it.get("ImpliedVolatility")),
                            "Bid": _safe_float(it.get("Bid")),
                            "Ask": _safe_float(it.get("Ask")),
                            "Last": _safe_float(it.get("Last")),
                        })

        except Exception as e:
            print(f"[stock-gex-live] chain snapshot error {symbol}: {e}", flush=True)

        return rows if rows else None

    except Exception as e:
        print(f"[stock-gex-live] chain fetch error {symbol}: {e}", flush=True)
        return None


def _safe_float(v):
    """Convert to float safely, return 0 on failure."""
    if v is None:
        return 0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0


def _fetch_stock_quote(symbol):
    """Fetch current stock quote via TS API. Returns {last, bid, ask} or None."""
    if not _api_get:
        return None
    try:
        r = _api_get(f"/marketdata/quotes/{symbol}")
        for q in r.json().get("Quotes", []):
            return {
                "last": q.get("Last", q.get("Close", 0)),
                "bid": q.get("Bid", 0),
                "ask": q.get("Ask", 0),
            }
    except Exception as e:
        print(f"[stock-gex-live] quote error {symbol}: {e}", flush=True)
        return None


def _fetch_batch_quotes(symbols):
    """Fetch quotes for multiple stocks in one call."""
    if not _api_get or not symbols:
        return {}
    try:
        sym_str = ",".join(symbols)
        r = _api_get(f"/marketdata/quotes/{sym_str}")
        js = r.json()
        result = {}
        for q in js.get("Quotes", []):
            sym = q.get("Symbol", "")
            if sym:
                result[sym] = {
                    "last": q.get("Last", q.get("Close", 0)),
                    "bid": q.get("Bid", 0),
                    "ask": q.get("Ask", 0),
                }
        return result
    except Exception as e:
        print(f"[stock-gex-live] batch quote error: {e}", flush=True)
        return {}


def _fetch_option_quote(symbol, expiration, strike, right="C"):
    """Fetch live option quote for a specific strike.

    Fetches a mini chain for just this strike to get bid/ask/delta/iv.
    Returns {bid, ask, last, delta, iv} or None.
    """
    if not _api_get:
        return None
    try:
        exp_str = expiration.replace("-", "")
        opt_type = "Call" if right == "C" else "Put"

        # Use chain endpoint with tight strike proximity to get just this strike
        resp = _api_get(f"/marketdata/options/chains", params={
            "symbol": symbol,
            "enableGreeks": "true",
            "optionType": opt_type,
            "strikeProximity": 1,
            "priceCenter": f"{strike:.2f}",
            "spreadType": "Single",
            "expiration": exp_str,
        }, timeout=8)

        if resp and hasattr(resp, 'json'):
            js = resp.json()
            for it in js.get("Options", []):
                legs = it.get("Legs") or []
                leg0 = legs[0] if legs else {}
                st = _safe_float(leg0.get("StrikePrice"))
                if st and abs(st - strike) < 1:
                    return {
                        "bid": _safe_float(it.get("Bid")),
                        "ask": _safe_float(it.get("Ask")),
                        "last": _safe_float(it.get("Last")),
                        "delta": _safe_float(it.get("Delta") or it.get("TheoDelta")),
                        "iv": _safe_float(it.get("ImpliedVolatility") or it.get("TheoIV")),
                        "volume": _safe_float(it.get("TotalVolume") or it.get("Volume")),
                        "oi": _safe_float(it.get("OpenInterest") or it.get("DailyOpenInterest")),
                    }
        return None
    except Exception as e:
        print(f"[stock-gex-live] option quote error {symbol} {strike}: {e}", flush=True)
        _alert(f"ERROR: Option quote fetch failed for {symbol} ${strike} {right}: {e}")
        return None


# ── DB ──────────────────────────────────────────────────────────────

def _db_init():
    """Create tables if needed."""
    if not _engine:
        return
    with _engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_gex_live_levels (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                scan_ts TIMESTAMPTZ DEFAULT NOW(),
                scan_date DATE NOT NULL,
                spot FLOAT,
                expiration VARCHAR(12),
                levels JSONB,
                ratio FLOAT,
                zone_width FLOAT,
                passes_filter BOOLEAN DEFAULT FALSE,
                filter_reason VARCHAR(100)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_gex_live_trades (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                tier VARCHAR(1),
                grade VARCHAR(2),
                trade_date DATE NOT NULL,
                entry_ts TIMESTAMPTZ,
                entry_price FLOAT,
                entry_spot FLOAT,
                strike FLOAT,
                expiration VARCHAR(12),
                call_bid FLOAT,
                call_ask FLOAT,
                call_delta FLOAT,
                call_iv FLOAT,
                gex_ratio FLOAT,
                zone_width FLOAT,
                highest_neg FLOAT,
                lowest_pos FLOAT,
                neg_levels JSONB,
                pos_levels JSONB,
                t1_price FLOAT,
                t2_price FLOAT,
                exit_ts TIMESTAMPTZ,
                exit_price FLOAT,
                exit_spot FLOAT,
                exit_call_bid FLOAT,
                exit_call_ask FLOAT,
                exit_reason VARCHAR(20),
                option_pnl_pct FLOAT,
                stock_pnl_pct FLOAT,
                hold_minutes INT,
                status VARCHAR(20) DEFAULT 'open'
            )
        """))


def _save_levels(symbol, levels, exp, passes, reason):
    """Save GEX levels to DB."""
    if not _engine:
        return
    try:
        with _engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO stock_gex_live_levels
                (symbol, scan_date, spot, expiration, levels, ratio, zone_width,
                 passes_filter, filter_reason)
                VALUES (:sym, :d, :spot, :exp, :levels, :ratio, :zw, :pf, :fr)
            """), {
                "sym": symbol, "d": date.today(), "spot": levels.get("spot"),
                "exp": exp, "levels": json.dumps(levels),
                "ratio": levels.get("ratio"), "zw": levels.get("zone_width"),
                "pf": passes, "fr": reason,
            })
    except Exception as e:
        print(f"[stock-gex-live] DB save error: {e}", flush=True)


def _save_trade(trade):
    """Save trade to DB."""
    if not _engine:
        return
    try:
        with _engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO stock_gex_live_trades
                (symbol, tier, grade, trade_date, entry_ts, entry_price, entry_spot,
                 strike, expiration, call_bid, call_ask, call_delta, call_iv,
                 gex_ratio, zone_width, highest_neg, lowest_pos,
                 neg_levels, pos_levels, t1_price, t2_price, status)
                VALUES (:sym, :tier, :grade, :td, :ets, :ep, :es,
                        :strike, :exp, :cb, :ca, :cd, :civ,
                        :ratio, :zw, :hn, :lp,
                        :nl, :pl, :t1, :t2, 'open')
            """), {
                "sym": trade["symbol"], "tier": trade.get("tier", "B"),
                "grade": trade.get("grade", "B"),
                "td": date.today(),
                "ets": trade["entry_ts"], "ep": trade["entry_price"],
                "es": trade["entry_spot"],
                "strike": trade["strike"], "exp": trade["expiration"],
                "cb": trade.get("call_bid"), "ca": trade.get("call_ask"),
                "cd": trade.get("call_delta"), "civ": trade.get("call_iv"),
                "ratio": trade["ratio"], "zw": trade["zone_width"],
                "hn": trade["highest_neg"], "lp": trade["lowest_pos"],
                "nl": json.dumps(trade.get("neg_levels", [])),
                "pl": json.dumps(trade.get("pos_levels", [])),
                "t1": trade["t1_price"], "t2": trade["t2_price"],
            })
    except Exception as e:
        print(f"[stock-gex-live] trade save error: {e}", flush=True)


def _update_trade_exit(trade):
    """Update trade with exit details."""
    if not _engine:
        return
    try:
        with _engine.begin() as conn:
            conn.execute(text("""
                UPDATE stock_gex_live_trades
                SET exit_ts = :ets, exit_price = :ep, exit_spot = :es,
                    exit_call_bid = :ecb, exit_call_ask = :eca,
                    exit_reason = :er, option_pnl_pct = :opnl,
                    stock_pnl_pct = :spnl, hold_minutes = :hm,
                    status = 'closed'
                WHERE symbol = :sym AND trade_date = :td AND status = 'open'
            """), {
                "ets": trade["exit_ts"], "ep": trade["exit_price"],
                "es": trade["exit_spot"],
                "ecb": trade.get("exit_call_bid"), "eca": trade.get("exit_call_ask"),
                "er": trade["exit_reason"],
                "opnl": trade.get("option_pnl_pct"),
                "spnl": trade.get("stock_pnl_pct"),
                "hm": trade.get("hold_minutes"),
                "sym": trade["symbol"], "td": date.today(),
            })
    except Exception as e:
        print(f"[stock-gex-live] trade update error: {e}", flush=True)


# ── Telegram ────────────────────────────────────────────────────────

_error_cooldowns = {}  # {error_key: last_alert_time} to avoid spam


def _alert(message):
    """Send alert to stock GEX Telegram channel."""
    if _send_telegram:
        try:
            _send_telegram(message)
        except Exception as e:
            print(f"[stock-gex-live] telegram error: {e}", flush=True)
    print(f"[stock-gex-live] {message}", flush=True)


def _alert_error(error_key, message):
    """Send error alert with 10-min cooldown per error type to avoid spam."""
    now = datetime.now(ET)
    last = _error_cooldowns.get(error_key)
    if last and (now - last).total_seconds() < 600:
        print(f"[stock-gex-live] ERROR (cooldown): {message}", flush=True)
        return
    _error_cooldowns[error_key] = now
    _alert(f"<b>ERROR:</b> {message}")


# ── Scanner Job (every 30 min) ──────────────────────────────────────

def run_gex_scan():
    """Main GEX scan. Compute levels for all stocks, update watchlist."""
    global _gex_levels, _watchlist, _last_scan_at, _scan_count

    if not _initialized:
        return

    now = datetime.now(ET)
    t = now.time()
    if now.weekday() >= 5:  # Skip weekends (5=Sat, 6=Sun)
        return
    if not (dtime(9, 30) <= t <= dtime(16, 0)):
        return

    # Skip first 30 min (09:30 bar is noise)
    if t < dtime(10, 0):
        return

    try:
        _run_gex_scan_inner(now)
    except Exception as e:
        _alert_error("scan_crash", f"GEX scan crashed: {e}")
        print(f"[stock-gex-live] SCAN CRASH: {traceback.format_exc()}", flush=True)


def _run_gex_scan_inner(now):
    global _gex_levels, _watchlist, _last_scan_at, _scan_count

    print(f"[stock-gex-live] GEX scan starting ({len(STOCKS)} stocks)...", flush=True)

    exp = _get_weekly_expiration()

    # Batch fetch all stock quotes
    quotes = _fetch_batch_quotes(STOCKS)

    if not quotes:
        _alert_error("batch_quotes", "Failed to fetch batch stock quotes. TS API may be down.")
        return

    if len(quotes) < len(STOCKS) * 0.5:
        _alert_error("partial_quotes",
                     f"Only got {len(quotes)}/{len(STOCKS)} stock quotes. TS API partial failure.")

    new_levels = {}
    new_watchlist = {}
    scanned = 0
    passed = 0
    chain_errors = 0

    for symbol in STOCKS:
        try:
            quote = quotes.get(symbol)
            if not quote or quote["last"] <= 0:
                continue

            spot = quote["last"]

            # Fetch chain and compute GEX
            chain = _fetch_chain(symbol, exp, spot)
            if not chain:
                chain_errors += 1
                continue

            levels = _compute_stock_gex(symbol, chain, spot)
            if not levels:
                continue

            scanned += 1
            levels["expiration"] = exp

            # Check filters
            passes, reason = _passes_filters(levels)
            _save_levels(symbol, levels, exp, passes, reason)

            with _lock:
                new_levels[symbol] = levels

            if passes:
                trigger_price = levels["highest_neg"] * (1 - ENTRY_OFFSET_PCT / 100)
                levels["trigger_price"] = round(trigger_price, 2)
                levels["tier"] = "A" if symbol in TIER_A else "B"
                new_watchlist[symbol] = levels
                passed += 1

            # Rate limit: don't hammer TS API
            time.sleep(0.3)

        except Exception as e:
            print(f"[stock-gex-live] scan error {symbol}: {e}", flush=True)

    if chain_errors > len(STOCKS) * 0.5:
        _alert_error("chain_errors",
                     f"Chain fetch failed for {chain_errors}/{len(STOCKS)} stocks. "
                     f"TS API issue or expiration problem. exp={exp}")

    if scanned == 0:
        _alert_error("no_scans",
                     f"GEX scan completed but 0 stocks scanned. "
                     f"Quotes: {len(quotes)}, Chain errors: {chain_errors}")

    with _lock:
        _gex_levels = new_levels
        _watchlist = new_watchlist

    _last_scan_at = now
    _scan_count += 1

    print(f"[stock-gex-live] GEX scan done: {scanned} scanned, "
          f"{passed} on watchlist, exp={exp}", flush=True)

    if new_watchlist:
        msg = f"<b>Stock GEX Scan</b> ({now.strftime('%H:%M')} ET)\n"
        msg += f"Watchlist: {passed} stocks\n\n"
        for sym, lv in sorted(new_watchlist.items()):
            msg += (f"<b>{sym}</b> [{lv['tier']}] "
                    f"spot=${lv['spot']:.2f} "
                    f"-GEX=${lv['highest_neg']:.0f} "
                    f"+GEX=${lv['lowest_pos']:.0f} "
                    f"R:{lv['ratio']}x "
                    f"trigger=${lv['trigger_price']:.2f}\n")
        _alert(msg)


# ── Spot Monitor Job (every 1-2 min) ───────────────────────────────

def run_spot_monitor():
    """Monitor watchlist stocks for entry triggers and exit targets."""
    global _active_trades, _trade_log, _last_monitor_at, _today_trades, _today_pnl

    if not _initialized:
        return

    now = datetime.now(ET)
    if now.weekday() >= 5:  # Skip weekends
        return
    t = now.time()
    if not (dtime(10, 0) <= t <= dtime(15, 55)):
        return

    try:
        _run_spot_monitor_inner(now)
    except Exception as e:
        _alert_error("monitor_crash", f"Spot monitor crashed: {e}")
        print(f"[stock-gex-live] MONITOR CRASH: {traceback.format_exc()}", flush=True)


def _run_spot_monitor_inner(now):
    global _active_trades, _trade_log, _last_monitor_at, _today_trades, _today_pnl

    with _lock:
        watchlist = dict(_watchlist)
        active = list(_active_trades)

    if not watchlist and not active:
        return

    _last_monitor_at = now

    # Fetch quotes for watchlist + active trades
    all_symbols = list(set(list(watchlist.keys()) + [t["symbol"] for t in active]))
    if not all_symbols:
        return

    quotes = _fetch_batch_quotes(all_symbols)

    if not quotes:
        _alert_error("monitor_quotes", "Spot monitor: failed to fetch quotes. TS API may be down.")
        return

    if active and not any(quotes.get(t["symbol"]) for t in active):
        _alert_error("monitor_active_quotes",
                     f"Cannot fetch quotes for active trades: {[t['symbol'] for t in active]}")

    # ── Check active trades for exits ──
    for trade in list(active):
        sym = trade["symbol"]
        quote = quotes.get(sym)
        if not quote:
            continue

        spot = quote["last"]
        if spot <= 0:
            continue

        exit_reason = None
        exit_spot = spot

        # T2 first (bigger target)
        if spot >= trade["t2_price"]:
            exit_reason = "T2"
        elif spot >= trade["t1_price"]:
            exit_reason = "T1"
        elif t >= dtime(15, 50):
            exit_reason = "EOD"

        if exit_reason:
            # Fetch option quote for exit
            opt_quote = _fetch_option_quote(
                sym, trade["expiration"], trade["strike"], "C")

            exit_call_bid = opt_quote["bid"] if opt_quote else None
            exit_call_ask = opt_quote["ask"] if opt_quote else None

            # PNL
            entry_mid = (trade.get("call_bid", 0) + trade.get("call_ask", 0)) / 2
            exit_mid = (exit_call_bid + exit_call_ask) / 2 if exit_call_bid and exit_call_ask else 0
            opt_pnl = ((exit_mid - entry_mid) / entry_mid * 100) if entry_mid > 0 else 0
            stock_pnl = (exit_spot - trade["entry_spot"]) / trade["entry_spot"] * 100

            hold_min = int((now - trade["entry_ts"]).total_seconds() / 60)

            trade["exit_ts"] = now
            trade["exit_price"] = exit_mid
            trade["exit_spot"] = exit_spot
            trade["exit_call_bid"] = exit_call_bid
            trade["exit_call_ask"] = exit_call_ask
            trade["exit_reason"] = exit_reason
            trade["option_pnl_pct"] = round(opt_pnl, 1)
            trade["stock_pnl_pct"] = round(stock_pnl, 2)
            trade["hold_minutes"] = hold_min

            _update_trade_exit(trade)

            with _lock:
                _active_trades.remove(trade)
                _trade_log.append(trade)
                _today_pnl += opt_pnl

            tag = "WIN" if opt_pnl > 0 else "LOSS"
            exit_delta_str = ""
            if exit_call_bid and exit_call_ask:
                exit_mid_str = f"${(exit_call_bid+exit_call_ask)/2:.2f}"
                exit_ba_str = f"${exit_call_bid:.2f} / ${exit_call_ask:.2f}"
            else:
                exit_mid_str = "?"
                exit_ba_str = "?"

            msg = (f"<b>EXIT {exit_reason}: {sym}</b> [{tag}]\n"
                   f"\n"
                   f"<b>Stock:</b> ${trade['entry_spot']:.2f} -> ${exit_spot:.2f} ({stock_pnl:+.2f}%)\n"
                   f"<b>Time:</b> {trade['entry_ts'].strftime('%H:%M')} -> {now.strftime('%H:%M')} ET ({hold_min} min)\n"
                   f"\n"
                   f"<b>Option:</b> ${trade['strike']:.0f}C {trade['expiration']}\n"
                   f"  Entry: ${entry_mid:.2f} (bid ${trade.get('call_bid',0):.2f} / ask ${trade.get('call_ask',0):.2f})\n"
                   f"  Exit:  {exit_mid_str} (bid {exit_ba_str})\n"
                   f"  <b>P&L: {opt_pnl:+.0f}%</b>\n"
                   f"\n"
                   f"  Entry delta: {trade.get('call_delta','?')} | Entry IV: {trade.get('call_iv','?')}\n"
                   f"  -GEX: ${trade['highest_neg']:.0f} | +GEX: ${trade['lowest_pos']:.0f} | R: {trade.get('ratio','?')}x")
            _alert(msg)

    # ── Check watchlist for entry triggers ──
    # Only 1 trade per stock per day
    today_symbols = set(t["symbol"] for t in active)
    today_symbols.update(t["symbol"] for t in _trade_log
                         if t.get("entry_ts") and t["entry_ts"].date() == now.date())

    for sym, levels in watchlist.items():
        if sym in today_symbols:
            continue

        quote = quotes.get(sym)
        if not quote:
            continue

        spot = quote["last"]
        if spot <= 0:
            continue

        trigger = levels["trigger_price"]
        if spot > trigger:
            continue

        # TRIGGERED! Stock hit -GEX minus 1%
        day_name = now.strftime("%a")
        grade = _grade_setup(levels, day_name)
        print(f"[stock-gex-live] TRIGGER: {sym} [{grade}] spot=${spot:.2f} <= trigger=${trigger:.2f}", flush=True)

        # Fetch option quote for entry
        opt_quote = _fetch_option_quote(sym, levels["expiration"], levels["highest_neg"], "C")

        call_bid = opt_quote["bid"] if opt_quote else None
        call_ask = opt_quote["ask"] if opt_quote else None
        call_delta = opt_quote.get("delta") if opt_quote else None
        call_iv = opt_quote.get("iv") if opt_quote else None
        call_mid = (call_bid + call_ask) / 2 if call_bid and call_ask else None

        trade = {
            "symbol": sym,
            "tier": levels.get("tier", "B"),
            "grade": grade,
            "entry_ts": now,
            "entry_price": call_mid,
            "entry_spot": spot,
            "strike": levels["highest_neg"],
            "expiration": levels["expiration"],
            "call_bid": call_bid,
            "call_ask": call_ask,
            "call_delta": call_delta,
            "call_iv": call_iv,
            "ratio": levels["ratio"],
            "zone_width": levels["zone_width"],
            "highest_neg": levels["highest_neg"],
            "lowest_pos": levels["lowest_pos"],
            "neg_levels": levels.get("neg_levels", []),
            "pos_levels": levels.get("pos_levels", []),
            "t1_price": levels["highest_neg"],  # T1 = recover to -GEX
            "t2_price": levels["lowest_pos"],    # T2 = reach +GEX magnet
        }

        _save_trade(trade)

        with _lock:
            _active_trades.append(trade)
            _today_trades += 1

        delta_str = f"{call_delta:.2f}" if call_delta else "?"
        iv_str = f"{call_iv*100:.0f}%" if call_iv else "?"
        bid_str = f"${call_bid:.2f}" if call_bid else "?"
        ask_str = f"${call_ask:.2f}" if call_ask else "?"
        mid_str = f"${call_mid:.2f}" if call_mid else "?"
        oi_str = f"{int(opt_quote.get('oi', 0)):,}" if opt_quote else "?"
        vol_str = f"{int(opt_quote.get('volume', 0)):,}" if opt_quote else "?"
        neg_str = " | ".join(f"${s:.0f}" for s in levels["neg_strikes"])
        pos_str = " | ".join(f"${s:.0f}" for s in levels["pos_strikes"])

        msg = (f"<b>ENTRY: {sym}</b> [Grade {grade}] [Tier {levels.get('tier', 'B')}]\n"
               f"\n"
               f"<b>Stock:</b> ${spot:.2f} at {now.strftime('%H:%M:%S')} ET\n"
               f"<b>Trigger:</b> -GEX-1% = ${trigger:.2f}\n"
               f"\n"
               f"<b>Option:</b> ${levels['highest_neg']:.0f}C {levels['expiration']}\n"
               f"  Bid/Ask: {bid_str} / {ask_str} (mid {mid_str})\n"
               f"  Delta: {delta_str} | IV: {iv_str}\n"
               f"  OI: {oi_str} | Volume: {vol_str}\n"
               f"\n"
               f"<b>Targets:</b>\n"
               f"  T1: ${levels['highest_neg']:.0f} (+{ENTRY_OFFSET_PCT:.0f}%)\n"
               f"  T2: ${levels['lowest_pos']:.0f} (+{levels['zone_width']:.1f}%)\n"
               f"\n"
               f"<b>GEX:</b> Ratio {levels['ratio']}x | Zone {levels['zone_width']:.1f}%\n"
               f"  -GEX: {neg_str}\n"
               f"  +GEX: {pos_str}")
        _alert(msg)


# ── EOD Summary ─────────────────────────────────────────────────────

def run_eod_summary():
    """Send end-of-day summary. Run at 16:05 ET."""
    now = datetime.now(ET)

    # Close any remaining active trades
    with _lock:
        remaining = list(_active_trades)

    for trade in remaining:
        trade["exit_ts"] = now
        trade["exit_reason"] = "EOD"
        trade["exit_spot"] = trade.get("entry_spot", 0)
        _update_trade_exit(trade)
        with _lock:
            _active_trades.remove(trade)
            _trade_log.append(trade)

    # Summary
    today = [t for t in _trade_log
             if t.get("entry_ts") and t["entry_ts"].date() == now.date()]

    if not today:
        _alert("<b>Stock GEX EOD:</b> No trades today.")
        return

    wins = sum(1 for t in today if t.get("option_pnl_pct", 0) > 0)
    total_pnl = sum(t.get("option_pnl_pct", 0) for t in today)

    msg = f"<b>Stock GEX EOD Summary</b>\n"
    msg += f"Trades: {len(today)} | Wins: {wins} | WR: {wins/len(today)*100:.0f}%\n\n"
    for t in today:
        pnl = t.get("option_pnl_pct", 0)
        tag = "W" if pnl > 0 else "L"
        msg += (f"{tag} {t['symbol']} [{t.get('tier','?')}] "
                f"${t.get('entry_spot',0):.2f}->${t.get('exit_spot',0):.2f} "
                f"opt:{pnl:+.0f}% ({t.get('exit_reason','?')})\n")

    _alert(msg)


# ── API Getters ─────────────────────────────────────────────────────

def get_watchlist():
    """Current watchlist with levels and trigger prices."""
    with _lock:
        return dict(_watchlist)


def get_active_trades():
    """Currently open positions."""
    with _lock:
        result = []
        for t in _active_trades:
            result.append({
                "symbol": t["symbol"],
                "tier": t.get("tier"),
                "entry_ts": str(t["entry_ts"]) if t.get("entry_ts") else None,
                "entry_spot": t.get("entry_spot"),
                "strike": t.get("strike"),
                "t1_price": t.get("t1_price"),
                "t2_price": t.get("t2_price"),
                "ratio": t.get("ratio"),
                "call_bid": t.get("call_bid"),
                "call_ask": t.get("call_ask"),
                "call_delta": t.get("call_delta"),
            })
        return result


def get_trade_log(days=7):
    """Recent completed trades."""
    if not _engine:
        return []
    try:
        with _engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT * FROM stock_gex_live_trades
                WHERE trade_date >= :d
                ORDER BY entry_ts DESC
            """), {"d": date.today() - timedelta(days=days)}).mappings().all()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_all_levels():
    """Current GEX levels for all scanned stocks."""
    with _lock:
        return dict(_gex_levels)


def get_status():
    """Scanner status."""
    with _lock:
        return {
            "initialized": _initialized,
            "last_scan_at": str(_last_scan_at) if _last_scan_at else None,
            "last_monitor_at": str(_last_monitor_at) if _last_monitor_at else None,
            "scan_count": _scan_count,
            "watchlist_count": len(_watchlist),
            "active_trades": len(_active_trades),
            "today_trades": _today_trades,
            "stocks_tracked": len(STOCKS),
        }


# ── Init ────────────────────────────────────────────────────────────

def _load_latest_levels():
    """Load last known GEX levels from DB so page works after hours / before first scan."""
    global _gex_levels, _watchlist, _last_scan_at
    if not _engine:
        return
    try:
        with _engine.connect() as conn:
            # Get the most recent scan date
            row = conn.execute(text(
                "SELECT MAX(scan_date) as d FROM stock_gex_live_levels"
            )).mappings().first()
            if not row or not row["d"]:
                return
            last_date = row["d"]
            # Get latest levels per symbol from that date
            rows = conn.execute(text("""
                SELECT DISTINCT ON (symbol) symbol, levels, passes_filter, scan_ts
                FROM stock_gex_live_levels
                WHERE scan_date = :d
                ORDER BY symbol, scan_ts DESC
            """), {"d": last_date}).mappings().all()
            new_levels = {}
            new_watchlist = {}
            for r in rows:
                sym = r["symbol"]
                lvls = json.loads(r["levels"]) if isinstance(r["levels"], str) else r["levels"]
                if not lvls:
                    continue
                new_levels[sym] = lvls
                if r["passes_filter"] and lvls.get("highest_neg"):
                    trigger = lvls["highest_neg"] * (1 - ENTRY_OFFSET_PCT / 100)
                    lvls["trigger_price"] = round(trigger, 2)
                    lvls["tier"] = "A" if sym in TIER_A else "B"
                    new_watchlist[sym] = lvls
            with _lock:
                _gex_levels = new_levels
                _watchlist = new_watchlist
                if rows:
                    _last_scan_at = rows[0].get("scan_ts")
            print(f"[stock-gex-live] loaded {len(new_levels)} levels from DB (date={last_date}, "
                  f"watchlist={len(new_watchlist)})", flush=True)
    except Exception as e:
        print(f"[stock-gex-live] load latest levels error: {e}", flush=True)


def _startup_scan():
    """Run one GEX scan at startup regardless of market hours (uses last-close prices)."""
    try:
        now = datetime.now(ET)
        _run_gex_scan_inner(now)
    except Exception as e:
        print(f"[stock-gex-live] startup scan error: {e}", flush=True)


def init(engine, api_get_fn, send_telegram_fn=None):
    """Initialize module. Called from main.py at startup."""
    global _engine, _api_get, _send_telegram, _initialized

    _engine = engine
    _api_get = api_get_fn
    _send_telegram = send_telegram_fn
    _initialized = True

    _db_init()
    _load_latest_levels()

    print(f"[stock-gex-live] initialized. {len(STOCKS)} stocks, "
          f"ratio>{MIN_GEX_RATIO}, entry=-GEX-{ENTRY_OFFSET_PCT}%", flush=True)
