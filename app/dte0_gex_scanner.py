"""
0DTE GEX Scanner — Multi-index 0DTE GEX data collection.

Independent from SPX 0DTE pipeline (lazy imports, separate table).
Schedule: 14 scans/day per symbol (every 30 min, 9:30-16:00 ET, on the wall clock).
Symbols: SPX (SPXW.X), SPY, QQQ, IWM.
Data collection only — no alerts, no setup detection, no live signals.

Replaces dead app/stock_gex_scanner.py for 0DTE-focused index/ETF analysis.
See Tasks.md S84.
"""

import json
from datetime import datetime, date, time as dtime
from threading import Lock
from zoneinfo import ZoneInfo

ET = ZoneInfo("US/Eastern")

# ── Config ──────────────────────────────────────────────────────────

SYMBOLS = [
    {"display": "SPX", "ts_symbol": "$SPXW.X", "quote_symbol": "$SPXW.X",
     "interval": 5,  "proximity": 125},
    {"display": "SPY", "ts_symbol": "SPY",      "quote_symbol": "SPY",
     "interval": 1,  "proximity": 25},
    {"display": "QQQ", "ts_symbol": "QQQ",      "quote_symbol": "QQQ",
     "interval": 1,  "proximity": 25},
    {"display": "IWM", "ts_symbol": "IWM",      "quote_symbol": "IWM",
     "interval": 1,  "proximity": 20},
]

DISPLAY_TO_CFG = {s["display"]: s for s in SYMBOLS}

# ── State ───────────────────────────────────────────────────────────

_engine = None
_api_get = None
_initialized = False
_latest: dict = {}  # {display: {spot, levels, gex_data, expiration, scanned_at}}
_lock = Lock()
_last_scan_status = {"ts": None, "ok": False, "msg": "not started",
                     "scanned": 0, "failed": 0, "per_symbol": {}}


# ── Init ────────────────────────────────────────────────────────────

def init(engine, api_get_fn):
    """Initialize the 0DTE GEX scanner. Called from main.py on_startup()."""
    global _engine, _api_get, _initialized
    _engine = engine
    _api_get = api_get_fn
    _initialized = True
    _db_init()
    _load_latest()
    print("[dte0-gex] initialized", flush=True)


# ── Database ────────────────────────────────────────────────────────

def _db_init():
    from sqlalchemy import text
    with _engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dte0_gex_scans (
                id              BIGSERIAL PRIMARY KEY,
                symbol          VARCHAR(10) NOT NULL,
                scan_ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                scan_date       DATE NOT NULL,
                spot            DOUBLE PRECISION NOT NULL,
                expiration      DATE,
                exp_label       VARCHAR(10) NOT NULL DEFAULT '0dte',
                key_levels      JSONB NOT NULL DEFAULT '{}',
                gex_data        JSONB NOT NULL DEFAULT '[]',
                total_call_gex  DOUBLE PRECISION,
                total_put_gex   DOUBLE PRECISION,
                total_net_gex   DOUBLE PRECISION
            );
            CREATE INDEX IF NOT EXISTS ix_dte0_gex_scans_ts
                ON dte0_gex_scans (scan_ts DESC);
            CREATE INDEX IF NOT EXISTS ix_dte0_gex_scans_date_sym
                ON dte0_gex_scans (scan_date, symbol);
        """))
    print("[dte0-gex] table ready", flush=True)


def _load_latest():
    """Load today's most recent scan per symbol into memory on startup."""
    global _latest
    from sqlalchemy import text
    today = _now_et().date()
    try:
        with _engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ON (symbol)
                    symbol, spot, key_levels, gex_data, expiration, scan_ts
                FROM dte0_gex_scans
                WHERE scan_date = :d
                ORDER BY symbol, scan_ts DESC
            """), {"d": today}).fetchall()
            with _lock:
                for r in rows:
                    _latest[r.symbol] = {
                        "symbol": r.symbol,
                        "spot": r.spot,
                        "levels": r.key_levels if isinstance(r.key_levels, dict) else json.loads(r.key_levels),
                        "gex_data": r.gex_data if isinstance(r.gex_data, list) else json.loads(r.gex_data),
                        "expiration": str(r.expiration) if r.expiration else None,
                        "scanned_at": str(r.scan_ts),
                    }
            if _latest:
                print(f"[dte0-gex] loaded {len(_latest)} entries from today's scans", flush=True)
    except Exception as e:
        print(f"[dte0-gex] load latest failed: {e}", flush=True)


# ── Utilities ───────────────────────────────────────────────────────

def _now_et():
    return datetime.now(ET)


def _get_batch_quotes(symbols: list[str]) -> dict[str, float]:
    """Get current prices for multiple symbols in one API call.
    Encodes $ as %24 for TS symbols like $SPXW.X.
    """
    if not symbols:
        return {}
    encoded = [s.replace("$", "%24") for s in symbols]
    sym_str = ",".join(encoded)
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
        print(f"[dte0-gex] batch quote failed: {e}", flush=True)
        return {}


# ── GEX Computation (copied from stock_gex_scanner) ────────────────

def _compute_gex(rows: list[dict]) -> list[dict]:
    """call_gex = gamma*OI*100; put_gex = -gamma*OI*100; net = sum."""
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
    """+GEX strikes = magnets, -GEX strikes = support/resistance."""
    if not gex_data:
        return {"support": [], "magnets_above": []}
    active = [d for d in gex_data if d["net_gex"] != 0]
    if not active:
        return {"support": [], "magnets_above": []}

    abs_gex = sorted([abs(d["net_gex"]) for d in active], reverse=True)
    cutoff_idx = max(4, len(abs_gex) // 4)
    threshold = abs_gex[min(cutoff_idx, len(abs_gex) - 1)]
    significant = [d for d in active if abs(d["net_gex"]) >= threshold]

    positive_gex = sorted([d for d in significant if d["net_gex"] > 0], key=lambda d: d["strike"])
    negative_gex = sorted([d for d in significant if d["net_gex"] < 0], key=lambda d: d["strike"])

    neg_below = [d for d in negative_gex if d["strike"] <= spot]
    pos_above = [d for d in positive_gex if d["strike"] >= spot]
    neg_above = [d for d in negative_gex if d["strike"] > spot]
    pos_below = [d for d in positive_gex if d["strike"] < spot]

    gex_above = sum(d["net_gex"] for d in active if d["strike"] > spot)
    gex_below = sum(d["net_gex"] for d in active if d["strike"] <= spot)

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
    """Scan all 4 symbols: fetch 0DTE chain, compute GEX, save to DB."""
    global _last_scan_status
    if not _initialized:
        return

    now = _now_et()
    if now.weekday() >= 5:
        return
    t = now.time()
    if not (dtime(9, 30) <= t <= dtime(16, 0)):
        return

    today = now.date()

    # Lazy import to avoid circular dep at module load
    from app.main import get_chain_rows, get_0dte_exp
    from sqlalchemy import text

    quote_syms = [c["quote_symbol"] for c in SYMBOLS]
    quotes = _get_batch_quotes(quote_syms)
    if not quotes:
        _last_scan_status = {"ts": str(now), "ok": False, "msg": "batch quote failed",
                             "scanned": 0, "failed": len(SYMBOLS), "per_symbol": {}}
        print("[dte0-gex] batch quote failed — skipping scan", flush=True)
        return

    scanned, failed = 0, 0
    per_symbol = {}

    for cfg in SYMBOLS:
        disp = cfg["display"]
        ts_sym = cfg["ts_symbol"]
        try:
            spot = quotes.get(cfg["quote_symbol"])
            if not spot:
                failed += 1
                per_symbol[disp] = {"ok": False, "msg": "no quote"}
                continue

            exp = get_0dte_exp(symbol=ts_sym)
            if not exp:
                failed += 1
                per_symbol[disp] = {"ok": False, "msg": "no 0dte exp"}
                continue

            rows = get_chain_rows(exp, spot, symbol=ts_sym,
                                  strike_interval=cfg["interval"],
                                  strike_proximity=cfg["proximity"])
            if not rows or len(rows) < 10:
                failed += 1
                per_symbol[disp] = {"ok": False, "msg": f"thin chain ({len(rows) if rows else 0})"}
                continue

            gex_data = _compute_gex(rows)
            if not gex_data:
                failed += 1
                per_symbol[disp] = {"ok": False, "msg": "empty gex"}
                continue

            levels = _identify_key_levels(gex_data, spot)
            total_call = sum(d["call_gex"] for d in gex_data)
            total_put = sum(d["put_gex"] for d in gex_data)
            total_net = sum(d["net_gex"] for d in gex_data)

            with _engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO dte0_gex_scans
                    (symbol, scan_date, spot, expiration, exp_label, key_levels,
                     gex_data, total_call_gex, total_put_gex, total_net_gex)
                    VALUES (:sym, :d, :spot, :exp, '0dte', :levels,
                            :gex, :cg, :pg, :ng)
                """), {
                    "sym": disp, "d": today, "spot": spot, "exp": exp,
                    "levels": json.dumps(levels),
                    "gex": json.dumps(gex_data),
                    "cg": total_call, "pg": total_put, "ng": total_net,
                })

            with _lock:
                _latest[disp] = {
                    "symbol": disp,
                    "spot": spot,
                    "levels": levels,
                    "gex_data": gex_data,
                    "expiration": exp,
                    "scanned_at": str(now),
                }

            scanned += 1
            per_symbol[disp] = {"ok": True, "strikes": len(gex_data),
                                "net_gex": round(total_net, 2)}

        except Exception as e:
            failed += 1
            per_symbol[disp] = {"ok": False, "msg": str(e)[:120]}
            print(f"[dte0-gex] {disp}: {e}", flush=True)
            continue

    _last_scan_status = {
        "ts": str(now), "ok": scanned > 0,
        "msg": f"scanned {scanned}/{len(SYMBOLS)}, failed {failed}",
        "scanned": scanned, "failed": failed, "per_symbol": per_symbol,
    }
    print(f"[dte0-gex] scan done: {scanned}/{len(SYMBOLS)} ok, {failed} failed", flush=True)


# ── API Helpers ─────────────────────────────────────────────────────

def get_all_levels() -> dict:
    """Return latest scan data for all 4 symbols."""
    with _lock:
        result = {}
        for sym, data in _latest.items():
            levels = data["levels"]
            result[sym] = {
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
        return result


def get_symbol_detail(symbol: str) -> dict | None:
    """Return full detail (incl. per-strike gex_data) for one symbol."""
    sym = symbol.upper()
    with _lock:
        d = _latest.get(sym)
        return dict(d) if d else None


def get_scan_history(symbol: str, days: int = 5) -> list[dict]:
    """Return scan history for a symbol from DB."""
    from sqlalchemy import text
    sym = symbol.upper()
    if sym not in DISPLAY_TO_CFG:
        return []
    try:
        with _engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT scan_ts, spot, expiration, key_levels,
                       total_call_gex, total_put_gex, total_net_gex
                FROM dte0_gex_scans
                WHERE symbol = :sym
                  AND scan_date >= CURRENT_DATE - INTERVAL ':d days'
                ORDER BY scan_ts DESC
            """.replace(":d", str(int(days)))), {"sym": sym}).fetchall()
            return [{
                "ts": str(r.scan_ts), "spot": r.spot,
                "expiration": str(r.expiration) if r.expiration else None,
                "levels": r.key_levels if isinstance(r.key_levels, dict) else json.loads(r.key_levels),
                "total_call_gex": r.total_call_gex,
                "total_put_gex": r.total_put_gex,
                "total_net_gex": r.total_net_gex,
            } for r in rows]
    except Exception as e:
        return [{"error": str(e)}]


def get_status() -> dict:
    with _lock:
        n = len(_latest)
        symbols = sorted(_latest.keys())
    return {
        "initialized": _initialized,
        "symbols_configured": [c["display"] for c in SYMBOLS],
        "symbols_in_memory": symbols,
        "entries_in_memory": n,
        "last_scan": _last_scan_status,
    }
