"""
Stock GEX Scanner — Independent data collection for stock GEX levels.

Completely isolated from 0DTE SPX pipeline.
Scans ~23 stocks every 30 min during market hours (Mon-Fri 9:30-16:00 ET).
Saves GEX per strike + current price to DB for future backtesting.

NO alerts, NO signals, NO Telegram — pure data collection.
"""

import json, time
from datetime import datetime, date, timedelta, time as dtime
from threading import Lock
from zoneinfo import ZoneInfo

ET = ZoneInfo("US/Eastern")

# ── Config ──────────────────────────────────────────────────────────

DEFAULT_STOCKS = [
    # Mega-cap tech
    "AAPL", "MSFT", "GOOGL", "GOOG", "META", "NVDA", "AMZN", "NFLX", "TSLA",
    # Semis
    "AMD", "INTC", "MU", "QCOM", "AVGO", "SMCI",
    # Fintech / payments
    "PYPL", "SQ", "SOFI", "COIN", "AFRM", "UPST",
    # Social / consumer
    "SNAP", "SHOP", "ROKU", "DKNG", "RBLX", "LULU", "SNOW",
    # Meme / speculative
    "AMC", "GME", "PLTR", "MARA", "RIOT", "NKLA", "LCID", "PLUG",
    # EV
    "RIVN", "NIO",
    # Financials
    "BAC", "JPM", "WFC", "C",
    # Legacy / industrial
    "BA", "DIS", "F", "GM", "T", "AAL", "CCL",
    # Energy
    "XOM", "OXY",
    # Pharma / healthcare
    "PFE", "JNJ",
    # Other high-volume
    "UBER", "CVNA", "AI", "BABA", "COST", "ENPH",
]

# ±10% of spot price for strike range
PROXIMITY_PCT = 0.10
# Delay between API calls per stock
INTER_STOCK_DELAY = 0.5

# ── State ───────────────────────────────────────────────────────────

_engine = None
_api_get = None
_initialized = False

# Latest scan data in memory: {symbol: {spot, levels, gex_data, ...}}
_latest: dict = {}
_lock = Lock()

# Cache expirations per symbol per day (don't re-fetch within same day)
_exp_cache: dict = {}  # {(symbol, date_str): expiration_str}

_last_scan_status = {"ts": None, "ok": False, "msg": "not started"}


# ── Init ────────────────────────────────────────────────────────────

def init(engine, api_get_fn):
    """Initialize the stock GEX scanner. Called from main.py on_startup()."""
    global _engine, _api_get, _initialized
    _engine = engine
    _api_get = api_get_fn
    _initialized = True
    _db_init()
    _load_latest()
    print("[stock-gex] initialized", flush=True)


# ── Database ────────────────────────────────────────────────────────

def _db_init():
    """Create table if it doesn't exist."""
    from sqlalchemy import text
    with _engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_gex_scans (
                id              BIGSERIAL PRIMARY KEY,
                symbol          VARCHAR(10) NOT NULL,
                scan_ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                scan_date       DATE NOT NULL,
                spot            DOUBLE PRECISION NOT NULL,
                expiration      DATE,
                exp_label       VARCHAR(10) NOT NULL DEFAULT 'weekly',
                key_levels      JSONB NOT NULL DEFAULT '{}',
                gex_data        JSONB NOT NULL DEFAULT '[]',
                total_call_gex  DOUBLE PRECISION,
                total_put_gex   DOUBLE PRECISION,
                total_net_gex   DOUBLE PRECISION
            );
            CREATE INDEX IF NOT EXISTS ix_stock_gex_scans_ts
                ON stock_gex_scans (scan_ts DESC);
            CREATE INDEX IF NOT EXISTS ix_stock_gex_scans_date_sym
                ON stock_gex_scans (scan_date, symbol);
        """))
        # Migration: add exp_label if table existed before this column
        conn.execute(text("""
            DO $$ BEGIN
                ALTER TABLE stock_gex_scans ADD COLUMN IF NOT EXISTS
                    exp_label VARCHAR(10) NOT NULL DEFAULT 'weekly';
            EXCEPTION WHEN OTHERS THEN NULL;
            END $$;
        """))
    print("[stock-gex] table ready", flush=True)


def _load_latest():
    """Load today's most recent scan per stock+exp_label into memory on startup."""
    global _latest
    from sqlalchemy import text
    today = _now_et().date()
    try:
        with _engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (symbol, exp_label)
                    symbol, exp_label, spot, key_levels, gex_data, expiration, scan_ts
                FROM stock_gex_scans
                WHERE scan_date = :d
                ORDER BY symbol, exp_label, scan_ts DESC
            """), {"d": today}).fetchall()
            with _lock:
                for r in rows:
                    key = f"{r.symbol}_{r.exp_label}"
                    _latest[key] = {
                        "symbol": r.symbol,
                        "exp_label": r.exp_label,
                        "spot": r.spot,
                        "levels": r.key_levels if isinstance(r.key_levels, dict) else json.loads(r.key_levels),
                        "gex_data": r.gex_data if isinstance(r.gex_data, list) else json.loads(r.gex_data),
                        "expiration": str(r.expiration) if r.expiration else None,
                        "scanned_at": str(r.scan_ts),
                    }
            if _latest:
                print(f"[stock-gex] loaded {len(_latest)} entries from today's scans", flush=True)
    except Exception as e:
        print(f"[stock-gex] load latest failed: {e}", flush=True)


# ── Utilities ───────────────────────────────────────────────────────

def _now_et():
    return datetime.now(ET)


def _fnum(x):
    """Parse numeric value from TS API response."""
    if x in (None, "", "-", "NaN", "nan"):
        return None
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return None


def _expiration_variants(ymd: str):
    """Yield date format variants for TS API compatibility."""
    yield ymd
    try:
        yield datetime.strptime(ymd, "%Y-%m-%d").strftime("%m-%d-%Y")
    except Exception:
        pass
    yield ymd + "T00:00:00Z"


# ── TradeStation API Helpers ────────────────────────────────────────

def _get_batch_quotes(symbols: list[str]) -> dict[str, float]:
    """Get current prices for multiple symbols in one API call."""
    if not symbols:
        return {}
    sym_str = ",".join(symbols)
    try:
        r = _api_get(f"/marketdata/quotes/{sym_str}", timeout=10)
        result = {}
        for q in r.json().get("Quotes", []):
            sym = q.get("Symbol", "")
            last = q.get("Last") or q.get("Close")
            if sym and last:
                try:
                    result[sym] = float(last)
                except (ValueError, TypeError):
                    pass
        return result
    except Exception as e:
        print(f"[stock-gex] batch quote failed: {e}", flush=True)
        return {}


def _get_target_expirations(symbol: str) -> list[dict]:
    """Get two expirations for GEX analysis. Cached per symbol per day.

    Returns list of {"exp": "YYYY-MM-DD", "label": "weekly"|"opex"}:
      1. This week's nearest Friday (weekly GEX)
      2. Nearest 3rd Friday of month (OpEx / monthly GEX)
    If they're the same date (OpEx week), returns just one entry.
    """
    today_str = _now_et().date().isoformat()
    cache_key = (symbol, today_str)
    if cache_key in _exp_cache:
        return _exp_cache[cache_key]

    try:
        r = _api_get(f"/marketdata/options/expirations/{symbol}", timeout=10)
        exps = []
        for e in r.json().get("Expirations", []):
            d = str(e.get("Date") or e.get("Expiration") or "")[:10]
            if d:
                exps.append(d)
    except Exception as e:
        print(f"[stock-gex] expirations for {symbol} failed: {e}", flush=True)
        return []

    today = _now_et().date()
    result = []

    # Parse all future expirations
    parsed = []
    for exp_str in sorted(exps):
        try:
            exp_date = date.fromisoformat(exp_str)
        except ValueError:
            continue
        days_out = (exp_date - today).days
        if days_out < 0 or days_out > 60:
            continue
        is_monthly = (exp_date.weekday() == 4 and 15 <= exp_date.day <= 21)
        parsed.append({"exp": exp_str, "date": exp_date, "days": days_out, "monthly": is_monthly})

    if not parsed:
        _exp_cache[cache_key] = []
        return []

    # 1. This week's Friday (nearest expiration that's a Friday, within 7 days)
    weekly = None
    for p in parsed:
        if p["date"].weekday() == 4 and p["days"] <= 7 and p["days"] >= 0:
            weekly = p
            break
    # Fallback: nearest expiration within 7 days (even if not Friday)
    if not weekly:
        near = [p for p in parsed if p["days"] <= 7 and p["days"] >= 0]
        if near:
            weekly = near[0]

    if weekly:
        result.append({"exp": weekly["exp"], "label": "weekly"})

    # 2. Nearest monthly OpEx (3rd Friday)
    monthlies = [p for p in parsed if p["monthly"] and p["days"] >= 0]
    opex = min(monthlies, key=lambda p: p["days"]) if monthlies else None

    if opex:
        # If same as weekly, don't duplicate — just relabel
        if weekly and opex["exp"] == weekly["exp"]:
            result[0]["label"] = "opex"  # It's OpEx week
        else:
            result.append({"exp": opex["exp"], "label": "opex"})

    _exp_cache[cache_key] = result
    return result


def _fetch_chain(symbol: str, exp: str, spot: float) -> list[dict]:
    """Fetch options chain for a stock from TS snapshot endpoint."""
    proximity = max(10, int(spot * PROXIMITY_PCT))

    for exp_fmt in _expiration_variants(exp):
        params = {
            "symbol": symbol,
            "enableGreeks": "true",
            "optionType": "All",
            "strikeProximity": proximity,
            "strikeInterval": 1,
            "spreadType": "Single",
            "expiration": exp_fmt,
        }
        try:
            r = _api_get("/marketdata/options/chains", params=params, timeout=12)
            js = r.json()
            rows = []
            for it in js.get("Options", []):
                legs = it.get("Legs") or []
                leg = legs[0] if legs else {}
                side = (leg.get("OptionType") or it.get("OptionType") or "").lower()
                side = "C" if side.startswith("c") else "P" if side.startswith("p") else "?"
                strike = _fnum(leg.get("StrikePrice"))
                if strike is None:
                    continue
                rows.append({
                    "Type": side,
                    "Strike": strike,
                    "Gamma": _fnum(it.get("Gamma") or it.get("TheoGamma")),
                    "Delta": _fnum(it.get("Delta") or it.get("TheoDelta")),
                    "OpenInterest": _fnum(it.get("OpenInterest") or it.get("DailyOpenInterest")) or 0,
                    "Volume": _fnum(it.get("TotalVolume") or it.get("Volume")) or 0,
                })
            if rows:
                return rows
        except Exception:
            continue

    print(f"[stock-gex] chain fetch failed for {symbol} exp={exp}", flush=True)
    return []


# ── GEX Computation ────────────────────────────────────────────────

def _compute_gex(rows: list[dict]) -> list[dict]:
    """Calculate GEX per strike from chain rows.

    call_gex = call_gamma x call_OI x 100
    put_gex  = -put_gamma x put_OI x 100
    net_gex  = call_gex + put_gex
    """
    calls, puts = {}, {}
    for r in rows:
        strike = r.get("Strike")
        gamma = r.get("Gamma")
        if strike is None or gamma is None:
            continue
        oi = float(r.get("OpenInterest") or 0)
        bucket = calls if r["Type"] == "C" else puts
        bucket[strike] = {"gamma": gamma, "oi": oi}

    all_strikes = sorted(set(calls) | set(puts))
    gex_data = []
    for strike in all_strikes:
        c = calls.get(strike, {"gamma": 0, "oi": 0})
        p = puts.get(strike, {"gamma": 0, "oi": 0})
        call_gex = c["gamma"] * c["oi"] * 100
        put_gex = -p["gamma"] * p["oi"] * 100
        net_gex = call_gex + put_gex
        gex_data.append({
            "strike": strike,
            "call_gex": round(call_gex, 2),
            "put_gex": round(put_gex, 2),
            "net_gex": round(net_gex, 2),
            "call_oi": c["oi"],
            "put_oi": p["oi"],
        })

    return gex_data


def _identify_key_levels(gex_data: list[dict], spot: float) -> dict:
    """
    Identify key GEX levels relative to current spot price.

    +GEX strikes = magnets (price pulled toward them via dealer hedging)
    -GEX strikes = support/resistance (dealer hedging pins price)
    """
    if not gex_data:
        return {"support": [], "magnets_above": []}

    active = [d for d in gex_data if d["net_gex"] != 0]
    if not active:
        return {"support": [], "magnets_above": []}

    # Significance threshold: top 25% by |GEX| (at least top 5)
    abs_gex = sorted([abs(d["net_gex"]) for d in active], reverse=True)
    cutoff_idx = max(4, len(abs_gex) // 4)
    threshold = abs_gex[min(cutoff_idx, len(abs_gex) - 1)]

    significant = [d for d in active if abs(d["net_gex"]) >= threshold]

    # Classify by sign
    positive_gex = sorted([d for d in significant if d["net_gex"] > 0], key=lambda d: d["strike"])
    negative_gex = sorted([d for d in significant if d["net_gex"] < 0], key=lambda d: d["strike"])

    # Relative to spot
    neg_below = [d for d in negative_gex if d["strike"] <= spot]
    pos_above = [d for d in positive_gex if d["strike"] >= spot]
    neg_above = [d for d in negative_gex if d["strike"] > spot]
    pos_below = [d for d in positive_gex if d["strike"] < spot]

    # GEX mass balance
    gex_above = sum(d["net_gex"] for d in active if d["strike"] > spot)
    gex_below = sum(d["net_gex"] for d in active if d["strike"] <= spot)

    # Strongest levels
    strongest_pos = max(positive_gex, key=lambda d: d["net_gex"]) if positive_gex else None
    strongest_neg = min(negative_gex, key=lambda d: d["net_gex"]) if negative_gex else None

    return {
        "support": [{"strike": d["strike"], "gex": d["net_gex"]} for d in neg_below],
        "magnets_above": [{"strike": d["strike"], "gex": d["net_gex"]} for d in pos_above],
        "magnets_below": [{"strike": d["strike"], "gex": d["net_gex"]} for d in pos_below],
        "resistance_above": [{"strike": d["strike"], "gex": d["net_gex"]} for d in neg_above],
        "all_significant": [{"strike": d["strike"], "gex": d["net_gex"]} for d in significant],
        "strongest_positive": {"strike": strongest_pos["strike"], "gex": strongest_pos["net_gex"]} if strongest_pos else None,
        "strongest_negative": {"strike": strongest_neg["strike"], "gex": strongest_neg["net_gex"]} if strongest_neg else None,
        "gex_above_spot": round(gex_above, 2),
        "gex_below_spot": round(gex_below, 2),
        "spot_at_scan": spot,
    }


# ── Scheduler Job ───────────────────────────────────────────────────

def run_scan():
    """Scan all stocks: fetch chain, compute GEX, save to DB. Every 30 min."""
    global _last_scan_status
    if not _initialized:
        return

    now = _now_et()
    t = now.time()
    # Only during market hours (9:30-16:00 ET)
    if not (dtime(9, 30) <= t <= dtime(16, 0)):
        return

    today = now.date()
    print(f"[stock-gex] scan: {len(DEFAULT_STOCKS)} stocks...", flush=True)

    # Get all quotes in one batch call
    quotes = _get_batch_quotes(DEFAULT_STOCKS)
    if not quotes:
        _last_scan_status = {"ts": str(now), "ok": False, "msg": "batch quote failed"}
        return

    from sqlalchemy import text
    scanned = 0
    failed = 0

    for symbol in DEFAULT_STOCKS:
        spot = quotes.get(symbol)
        if not spot:
            failed += 1
            continue

        try:
            # Get target expirations: weekly + opex (cached per day)
            targets = _get_target_expirations(symbol)
            if not targets:
                failed += 1
                continue

            for tgt in targets:
                exp = tgt["exp"]
                label = tgt["label"]

                # Fetch chain for this expiration
                rows = _fetch_chain(symbol, exp, spot)
                if len(rows) < 10:
                    continue

                # Compute GEX
                gex_data = _compute_gex(rows)
                if not gex_data:
                    continue

                # Identify key levels
                levels = _identify_key_levels(gex_data, spot)

                # Totals
                total_call = sum(d["call_gex"] for d in gex_data)
                total_put = sum(d["put_gex"] for d in gex_data)
                total_net = sum(d["net_gex"] for d in gex_data)

                # Save to DB
                with _engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO stock_gex_scans
                        (symbol, scan_date, spot, expiration, exp_label, key_levels,
                         gex_data, total_call_gex, total_put_gex, total_net_gex)
                        VALUES (:sym, :d, :spot, :exp, :label, :levels,
                                :gex, :cg, :pg, :ng)
                    """), {
                        "sym": symbol, "d": today, "spot": spot,
                        "exp": exp, "label": label,
                        "levels": json.dumps(levels),
                        "gex": json.dumps(gex_data),
                        "cg": total_call, "pg": total_put, "ng": total_net,
                    })

                # Update in-memory
                key = f"{symbol}_{label}"
                with _lock:
                    _latest[key] = {
                        "symbol": symbol,
                        "exp_label": label,
                        "spot": spot,
                        "levels": levels,
                        "gex_data": gex_data,
                        "expiration": exp,
                        "scanned_at": str(now),
                    }

            scanned += 1
            time.sleep(INTER_STOCK_DELAY)

        except Exception as e:
            print(f"[stock-gex] {symbol}: {e}", flush=True)
            failed += 1

    msg = f"scanned {scanned}/{len(DEFAULT_STOCKS)}, failed {failed}"
    _last_scan_status = {"ts": str(now), "ok": failed < len(DEFAULT_STOCKS) // 2, "msg": msg}
    print(f"[stock-gex] done: {msg}", flush=True)


# ── API Helpers (called from main.py endpoints) ────────────────────

def get_all_levels() -> dict:
    """Return latest scan data for all stocks, grouped by symbol.

    Each symbol has 'weekly' and/or 'opex' sub-keys.
    """
    with _lock:
        result = {}
        for key, data in _latest.items():
            sym = data["symbol"]
            label = data["exp_label"]
            levels = data["levels"]
            entry = {
                "spot": data["spot"],
                "expiration": data.get("expiration"),
                "scanned_at": data.get("scanned_at"),
                "support": levels.get("support", []),
                "magnets_above": levels.get("magnets_above", []),
                "magnets_below": levels.get("magnets_below", []),
                "resistance_above": levels.get("resistance_above", []),
                "strongest_positive": levels.get("strongest_positive"),
                "strongest_negative": levels.get("strongest_negative"),
                "gex_above_spot": levels.get("gex_above_spot"),
                "gex_below_spot": levels.get("gex_below_spot"),
            }
            if sym not in result:
                result[sym] = {}
            result[sym][label] = entry
        return result


def get_stock_detail(symbol: str) -> dict | None:
    """Return full detail for a stock (both weekly + opex if available)."""
    sym = symbol.upper()
    with _lock:
        out = {}
        for key, data in _latest.items():
            if data["symbol"] == sym:
                out[data["exp_label"]] = dict(data)
    return out if out else None


def get_status() -> dict:
    """Return scanner status."""
    with _lock:
        n_entries = len(_latest)
        symbols = sorted(set(d["symbol"] for d in _latest.values()))
    return {
        "initialized": _initialized,
        "stocks_tracked": len(symbols),
        "entries_in_memory": n_entries,
        "symbols": symbols,
        "stock_list": DEFAULT_STOCKS,
        "last_scan": _last_scan_status,
    }


def get_scan_history(symbol: str, days: int = 5, exp_label: str | None = None) -> list[dict]:
    """Return scan history for a stock from DB (for backtesting).

    Args:
        symbol: stock ticker
        days: how many days back
        exp_label: filter by 'weekly' or 'opex' (None = both)
    """
    if not _engine:
        return []
    from sqlalchemy import text
    cutoff = (_now_et().date() - timedelta(days=days)).isoformat()
    query = """
        SELECT scan_ts, spot, expiration, exp_label, key_levels,
               total_call_gex, total_put_gex, total_net_gex
        FROM stock_gex_scans
        WHERE symbol = :sym AND scan_date >= :cutoff
    """
    params: dict = {"sym": symbol.upper(), "cutoff": cutoff}
    if exp_label:
        query += " AND exp_label = :label"
        params["label"] = exp_label
    query += " ORDER BY scan_ts"
    try:
        with _engine.connect() as conn:
            rows = conn.execute(text(query), params).fetchall()
            return [
                {
                    "ts": str(r.scan_ts), "spot": r.spot,
                    "expiration": str(r.expiration) if r.expiration else None,
                    "exp_label": r.exp_label,
                    "key_levels": r.key_levels,
                    "total_call_gex": r.total_call_gex,
                    "total_put_gex": r.total_put_gex,
                    "total_net_gex": r.total_net_gex,
                }
                for r in rows
            ]
    except Exception as e:
        print(f"[stock-gex] get_scan_history failed: {e}", flush=True)
        return []
