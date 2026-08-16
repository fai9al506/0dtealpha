"""
Swing GEX Scanner — stock + index-ETF GEX history.

🛑 NOT WIRED UP. NOTHING IMPORTS THIS FILE. IT NEVER RUNS.
=========================================================
This is a PARTS BIN for **Tasks.md S259**, not a live module. The main.py hooks
that used to start it were REMOVED on 2026-08-14 at the user's direction: the
pre-market / post-close schedule below is not what they want. They want GEX at
**10:00 and 14:00 ET**, which is inside market hours and needs a different and
more careful design (S259).

HOW TO PICK THIS UP LATER — read Tasks.md S259 first, then:

  REUSE AS-IS (these were tested and are correct):
    * `_target_expirations()` — picks one weekly + one monthly expiry from the
      broker's real list, and rolls to the next month when they collide in opex
      week. Verified on 6 edge cases incl. the year roll.
    * `_quality()` — measures whether a chain is USABLE (% of rows with non-zero
      gamma / OI / IV), not merely non-empty. Row count proves nothing.
    * `_chain_params()` — strike interval + proximity by price.
    * `_compute_gex` / `_identify_key_levels` — imported from dte0_gex_scanner.
    * The `get_latest` / `get_history` / `quality_report` read helpers.
    * The DB schema in `_db_init()`. NOTE: the table was DROPPED on 2026-08-14,
      so it will be created fresh on first run.

  MUST BE REWRITTEN for a 10:00/14:00 schedule (do NOT just move the cron):
    * The scan MUST run on its OWN dedicated thread. Today it runs on an
      APScheduler worker, and the pool has only 10 — one of those jobs is
      `_real_trade_fast_poll` every 3 SECONDS, which drives real-money fills and
      exits. Holding a worker for minutes can delay it. This is the single
      biggest hazard and the reason S259 is parked.
    * Pace: ONE stock per 30 s, aligned just after the SPX chain pull
      (PULL_EVERY=30 s, STREAM_SECONDS=5 → 5 s busy, 25 s free).
    * Before EVERY stock, check SPX chain freshness and ABORT if >90 s stale.
    * The `_BLOCK_START`/`_BLOCK_END` guard below must be inverted/removed, but
      keep the per-symbol re-check pattern — a start-only check is NOT a guard
      (proved 2026-08-14: a pass starting 09:23 was still streaming at 09:29).

WHY THE OLD SCANNERS DIED (S258, 2026-08-14) — root-caused with a live API test
-------------------------------------------------------------------------------
Two predecessors are dead and both causes were root-caused with a live API test:

  * `stock_gex_scanner.py` wrote 0 rows EVER because it called the SNAPSHOT
    endpoint `/marketdata/options/chains`, which returns HTTP 404 for every
    stock (verified NVDA / AAPL / SPY). It never worked for one second — it was
    never an API-load problem.
  * `stock_gex_live.py` is what took the TS API down in late March. The only
    endpoint that works for stocks is the STREAM, and each call holds a
    connection open 4-8 s (measured 5.74 / 8.26 / 4.19). It looped 56 stocks at
    1 s apart = ~6 minutes of near-continuous streaming, competing with the core
    SPX chain pull (same endpoint, every 30 s) and the ES quote stream. The
    failure was concurrent-stream contention, not request rate.

So this module is built the opposite way round:

  * It reuses `main.get_chain_rows()` — the same proven stream path the healthy
    `dte0_gex_scanner` uses (3,371 rows / 13 scans a day / unbroken since May).
  * It runs ONLY when the 0DTE pipeline is idle, and a hard guard refuses to run
    during market hours no matter what the cron says. Contention is impossible
    by construction, not merely reduced.
  * Swing needs one snapshot a day, not one every 30 minutes. That is the whole
    reason this is safe where the old design was not.

TWO PASSES A DAY, ON PURPOSE
----------------------------
  * `premkt`    ~09:05 ET — freshly-settled overnight OI against a pre-open spot.
  * `postclose` ~16:35 ET — the settled closing price against yesterday's OI.

Open interest ALWAYS settles overnight, so neither pass has both fresh. Running
both gives the ideal pairing (today's close from `postclose`, today's settled OI
from tomorrow's `premkt`) AND settles an open question with data: there is a
disagreement about whether TradeStation serves usable pre-market chains. Every
scan therefore stores a `quality` blob (row count, and the fraction of rows with
non-zero gamma / OI / IV). After a week, compare `premkt` vs `postclose` quality
and the answer is measured rather than argued.

DATA COLLECTION ONLY. No alerts, no signals, no trading. Nothing reads this yet
— it exists so that when a swing setup is proposed it can be backtested
immediately. TradeStation serves NO historical option chains, so there is no
backfill: every day not collected is permanently lost.
"""

import json
import os
import time
from datetime import datetime, date, timedelta, time as dtime
from threading import Lock
from zoneinfo import ZoneInfo

# Pure functions, no side effects, no main.py import at module scope.
from app.dte0_gex_scanner import _compute_gex, _identify_key_levels

ET = ZoneInfo("US/Eastern")

# ── Config ──────────────────────────────────────────────────────────

_DEFAULT_SYMBOLS = [
    # index ETFs — the swing expiries, distinct from dte0_gex_scanner's 0DTE
    "SPY", "QQQ", "IWM",
    # mega-cap / highest options volume
    "NVDA", "TSLA", "AAPL", "MSFT", "AMZN", "META", "GOOGL",
    "AMD", "AVGO", "NFLX", "MU", "PLTR", "COIN", "MSTR",
    "SMCI", "QCOM", "ORCL",
]

def _symbols() -> list[str]:
    raw = os.getenv("SWING_GEX_SYMBOLS", "").strip()
    if raw:
        return [s.strip().upper() for s in raw.split(",") if s.strip()]
    return list(_DEFAULT_SYMBOLS)


def _enabled() -> bool:
    # Ships DORMANT. The user deprioritised the daily swing snapshot on
    # 2026-08-14 ("I still don't see this thing useful") in favour of intraday
    # 10:00/14:00 GEX, which is parked pending a decision (Tasks S259).
    # Set SWING_GEX_ENABLED=true to start collecting — note TS serves NO
    # historical chains, so days not collected are lost permanently.
    return os.getenv("SWING_GEX_ENABLED", "false").lower() == "true"


# Seconds to wait between chain fetches. Each stream call holds a connection
# 4-8 s; this spacing keeps at most one open at a time with room to spare.
INTER_SYMBOL_DELAY = float(os.getenv("SWING_GEX_DELAY_S", "1.5"))

# ±% of spot to pull strikes for. Wider than 0DTE because multi-day moves are.
PROXIMITY_PCT = 0.10

# HARD SAFETY GUARD. Even if a cron is misconfigured, never stream during the
# session — this is the exact contention that killed the TS API in March.
_BLOCK_START = dtime(9, 25)
_BLOCK_END = dtime(16, 5)

# ── State ───────────────────────────────────────────────────────────

_engine = None
_api_get = None
_initialized = False
_lock = Lock()
_exp_cache: dict = {}          # {(symbol, 'YYYY-MM-DD'): [exp, ...]}
_last_scan_status = {"ts": None, "ok": False, "msg": "not started"}


# ── Init ────────────────────────────────────────────────────────────

def init(engine, api_get_fn):
    """Called from main.py on_startup(). Never raises."""
    global _engine, _api_get, _initialized
    _engine = engine
    _api_get = api_get_fn
    try:
        _db_init()
        _initialized = True
        print(f"[swing-gex] initialized — {len(_symbols())} symbols, "
              f"enabled={_enabled()}", flush=True)
    except Exception as e:
        print(f"[swing-gex] init failed (non-fatal): {e}", flush=True)


def _db_init():
    from sqlalchemy import text
    with _engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS swing_gex_scans (
                id              BIGSERIAL PRIMARY KEY,
                symbol          VARCHAR(10) NOT NULL,
                scan_ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                scan_date       DATE NOT NULL,
                scan_label      VARCHAR(12) NOT NULL,
                spot            DOUBLE PRECISION NOT NULL,
                expiration      DATE,
                exp_label       VARCHAR(10) NOT NULL,
                dte             INTEGER,
                key_levels      JSONB NOT NULL DEFAULT '{}',
                gex_data        JSONB NOT NULL DEFAULT '[]',
                total_call_gex  DOUBLE PRECISION,
                total_put_gex   DOUBLE PRECISION,
                total_net_gex   DOUBLE PRECISION,
                quality         JSONB NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS ix_swing_gex_date_sym
                ON swing_gex_scans (scan_date DESC, symbol);
            CREATE INDEX IF NOT EXISTS ix_swing_gex_sym_exp
                ON swing_gex_scans (symbol, exp_label, scan_date DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS ux_swing_gex_scan
                ON swing_gex_scans (scan_date, scan_label, symbol, exp_label);
        """))


# ── Utilities ───────────────────────────────────────────────────────

def _now_et():
    return datetime.now(ET)


def _third_friday(y: int, m: int) -> date:
    d = date(y, m, 1)
    fridays = [d + timedelta(days=i) for i in range(31)
               if (d + timedelta(days=i)).month == m
               and (d + timedelta(days=i)).weekday() == 4]
    return fridays[2]


def _target_expirations(symbol: str, today: date) -> list[tuple[str, str]]:
    """Return [(exp_ymd, label)] — one weekly, one monthly/opex.

    Chosen from the broker's real expiration list so we never request a date
    that does not exist. Cached per symbol per day.
    """
    key = (symbol, today.isoformat())
    if key in _exp_cache:
        avail = _exp_cache[key]
    else:
        avail = []
        try:
            r = _api_get(f"/marketdata/options/expirations/{symbol}", timeout=10)
            avail = sorted({e.get("Date", "")[:10]
                            for e in r.json().get("Expirations", [])
                            if e.get("Date")})
        except Exception as e:
            print(f"[swing-gex] {symbol}: expirations failed: {e}", flush=True)
        _exp_cache[key] = avail
    if not avail:
        return []

    out, seen = [], set()

    # Weekly: the first expiry at least 3 calendar days out, so we are never
    # looking at something that expires inside a short swing hold.
    wk = next((e for e in avail if e >= str(today + timedelta(days=3))), None)
    if wk:
        out.append((wk, "weekly"))
        seen.add(wk)

    # Opex: nearest monthly 3rd Friday still ahead of us.
    #
    # If that lands on the SAME expiry we just took as the weekly (it does in
    # opex week — 2026-08-14's weekly and opex were both 2026-08-21), roll to
    # the following month. Two rungs of the term structure is the entire point:
    # "which levels survive Friday" is what makes a level swing-eligible, and
    # you cannot answer that from one expiry.
    y, m = today.year, today.month
    for _ in range(3):
        tf = _third_friday(y, m)
        op = next((e for e in avail if e >= str(tf)), None)
        if op and op not in seen and op > (out[0][0] if out else ""):
            out.append((op, "opex"))
            break
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)

    return out


def _chain_params(spot: float) -> tuple[int, int]:
    """(strike_interval, strike_proximity) — proximity is in PRICE units."""
    if spot > 500:
        interval = 5
    else:
        interval = 1
    proximity = max(10, int(round(spot * PROXIMITY_PCT)))
    return interval, proximity


def _quality(rows: list[dict]) -> dict:
    """Measure whether this chain is USABLE, not merely non-empty.

    GEX needs gamma AND open interest. A row count proves nothing — this is what
    lets us settle the premkt-vs-postclose question with data.
    """
    n = len(rows)
    if not n:
        return {"n_rows": 0}

    def nz(k):
        c = 0
        for r in rows:
            v = r.get(k)
            try:
                if v is not None and float(v) != 0:
                    c += 1
            except (TypeError, ValueError):
                pass
        return c

    return {
        "n_rows": n,
        "gamma_nz_pct": round(100.0 * nz("Gamma") / n, 1),
        "oi_nz_pct": round(100.0 * nz("OpenInterest") / n, 1),
        "iv_nz_pct": round(100.0 * nz("IV") / n, 1),
        "bid_nz_pct": round(100.0 * nz("Bid") / n, 1),
    }


def _batch_quotes(symbols: list[str]) -> dict[str, float]:
    """One API call for every spot price."""
    out = {}
    # TS caps URL length; chunk defensively.
    for i in range(0, len(symbols), 25):
        chunk = symbols[i:i + 25]
        try:
            r = _api_get(f"/marketdata/quotes/{','.join(chunk)}", timeout=10)
            for q in r.json().get("Quotes", []):
                sym = q.get("Symbol", "")
                last = q.get("Last") or q.get("Close") or q.get("PreviousClose")
                if sym and last:
                    try:
                        out[sym] = float(last)
                    except (TypeError, ValueError):
                        pass
        except Exception as e:
            print(f"[swing-gex] batch quote failed for {chunk[:3]}...: {e}", flush=True)
    return out


# ── The scan ────────────────────────────────────────────────────────

def run_scan(label: str):
    """One full pass. `label` is 'premkt' or 'postclose'. Never raises."""
    global _last_scan_status
    try:
        _run_scan_inner(label)
    except Exception as e:
        _last_scan_status = {"ts": str(_now_et()), "ok": False,
                             "msg": f"fatal: {str(e)[:200]}"}
        print(f"[swing-gex] scan fatal (non-fatal to app): {e}", flush=True)


def _run_scan_inner(label: str):
    global _last_scan_status

    if not _initialized or not _enabled():
        return

    now = _now_et()
    if now.weekday() >= 5:
        return

    # HARD GUARD — never stream option chains while the 0DTE pipeline is live.
    if _BLOCK_START <= now.time() <= _BLOCK_END:
        print(f"[swing-gex] REFUSED {label} at {now.time()} — inside the "
              f"market-hours block. This guard exists because concurrent chain "
              f"streams took the TS API down in March 2026 (S258).", flush=True)
        return

    from sqlalchemy import text
    from app.main import get_chain_rows      # lazy — avoids circular import

    today = now.date()
    syms = _symbols()
    quotes = _batch_quotes(syms)
    if not quotes:
        _last_scan_status = {"ts": str(now), "ok": False,
                             "msg": f"{label}: batch quote returned nothing"}
        print(f"[swing-gex] {label}: no quotes — skipping scan", flush=True)
        return

    t0 = time.time()
    saved = failed = 0
    per_symbol = {}
    qual_acc = []

    for sym in syms:
        # Re-check the guard EVERY symbol, not just at scan start. A pass that
        # begins legally can still run long — the 2026-08-14 test started 09:23
        # and was still streaming at 09:29, i.e. it walked into the block it was
        # supposed to respect. A start-only check is not a guard.
        if _BLOCK_START <= _now_et().time() <= _BLOCK_END:
            print(f"[swing-gex] {label}: ABORTING at {sym} — reached the "
                  f"market-hours block mid-pass ({saved} saved so far).", flush=True)
            per_symbol["_aborted_at"] = sym
            break

        spot = quotes.get(sym)
        if not spot or spot <= 0:
            failed += 1
            per_symbol[sym] = "no quote"
            continue

        exps = _target_expirations(sym, today)
        if not exps:
            failed += 1
            per_symbol[sym] = "no expirations"
            continue

        interval, proximity = _chain_params(spot)
        ok_here = []

        for exp, exp_label in exps:
            try:
                rows = get_chain_rows(exp, spot, symbol=sym,
                                      strike_interval=interval,
                                      strike_proximity=proximity)
                if not rows or len(rows) < 10:
                    failed += 1
                    per_symbol[f"{sym}/{exp_label}"] = f"thin chain ({len(rows or [])})"
                    continue

                q = _quality(rows)
                qual_acc.append(q)

                gex_data = _compute_gex(rows)
                if not gex_data:
                    failed += 1
                    per_symbol[f"{sym}/{exp_label}"] = "empty gex"
                    continue

                levels = _identify_key_levels(gex_data, spot)
                tc = sum(d["call_gex"] for d in gex_data)
                tp = sum(d["put_gex"] for d in gex_data)
                tn = sum(d["net_gex"] for d in gex_data)
                dte = (date.fromisoformat(exp) - today).days

                with _engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO swing_gex_scans
                          (symbol, scan_date, scan_label, spot, expiration,
                           exp_label, dte, key_levels, gex_data,
                           total_call_gex, total_put_gex, total_net_gex, quality)
                        VALUES
                          (:sym, :d, :lbl, :spot, :exp, :elbl, :dte, :levels,
                           :gex, :cg, :pg, :ng, :q)
                        ON CONFLICT (scan_date, scan_label, symbol, exp_label)
                        DO UPDATE SET
                           scan_ts = NOW(), spot = EXCLUDED.spot,
                           expiration = EXCLUDED.expiration, dte = EXCLUDED.dte,
                           key_levels = EXCLUDED.key_levels,
                           gex_data = EXCLUDED.gex_data,
                           total_call_gex = EXCLUDED.total_call_gex,
                           total_put_gex = EXCLUDED.total_put_gex,
                           total_net_gex = EXCLUDED.total_net_gex,
                           quality = EXCLUDED.quality
                    """), {
                        "sym": sym, "d": today, "lbl": label, "spot": spot,
                        "exp": exp, "elbl": exp_label, "dte": dte,
                        "levels": json.dumps(levels),
                        "gex": json.dumps(gex_data),
                        "cg": tc, "pg": tp, "ng": tn,
                        "q": json.dumps(q),
                    })

                saved += 1
                ok_here.append(f"{exp_label}({q.get('gamma_nz_pct')}%γ)")

            except Exception as e:
                failed += 1
                per_symbol[f"{sym}/{exp_label}"] = str(e)[:100]
                print(f"[swing-gex] {sym}/{exp_label}: {e}", flush=True)

            time.sleep(INTER_SYMBOL_DELAY)

        if ok_here:
            per_symbol[sym] = " ".join(ok_here)

    # Aggregate quality — this is the premkt-vs-postclose evidence.
    agg = {}
    if qual_acc:
        for k in ("gamma_nz_pct", "oi_nz_pct", "iv_nz_pct", "bid_nz_pct"):
            vals = [q[k] for q in qual_acc if k in q]
            if vals:
                agg[k] = round(sum(vals) / len(vals), 1)

    _last_scan_status = {
        "ts": str(now), "label": label, "ok": saved > 0,
        "msg": f"{label}: saved {saved}, failed {failed}",
        "saved": saved, "failed": failed,
        "elapsed_s": round(time.time() - t0, 1),
        "avg_quality": agg, "per_symbol": per_symbol,
    }
    print(f"[swing-gex] {label} done in {time.time()-t0:.0f}s — "
          f"{saved} saved, {failed} failed, avg quality {agg}", flush=True)


# ── Read helpers (for a page/API later) ─────────────────────────────

def get_status() -> dict:
    return {
        "initialized": _initialized,
        "enabled": _enabled(),
        "symbols": _symbols(),
        "last_scan": _last_scan_status,
    }


def get_latest(symbol: str | None = None, exp_label: str | None = None) -> list[dict]:
    """Most recent scan per symbol/expiry."""
    from sqlalchemy import text
    where, params = [], {}
    if symbol:
        where.append("symbol = :sym")
        params["sym"] = symbol.upper()
    if exp_label:
        where.append("exp_label = :el")
        params["el"] = exp_label
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    try:
        with _engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT DISTINCT ON (symbol, exp_label)
                       symbol, exp_label, scan_label, scan_date, scan_ts, spot,
                       expiration, dte, key_levels, total_net_gex, quality
                FROM swing_gex_scans
                {clause}
                ORDER BY symbol, exp_label, scan_ts DESC
            """), params).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as e:
        print(f"[swing-gex] get_latest failed: {e}", flush=True)
        return []


def get_history(symbol: str, days: int = 30, exp_label: str | None = None) -> list[dict]:
    """Daily GEX history for backtesting a swing setup."""
    from sqlalchemy import text
    params = {"sym": symbol.upper(), "n": days}
    extra = ""
    if exp_label:
        extra = "AND exp_label = :el"
        params["el"] = exp_label
    try:
        with _engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT symbol, scan_date, scan_label, exp_label, spot, expiration,
                       dte, key_levels, gex_data, total_net_gex, quality
                FROM swing_gex_scans
                WHERE symbol = :sym {extra}
                  AND scan_date > CURRENT_DATE - :n
                ORDER BY scan_date DESC, scan_label, exp_label
            """), params).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as e:
        print(f"[swing-gex] get_history failed: {e}", flush=True)
        return []


def quality_report(days: int = 14) -> list[dict]:
    """premkt vs postclose data quality — settles whether TS pre-market is usable.

    Run this after ~a week: `python -c "from app import swing_gex_scanner as s; ..."`
    or via the status endpoint.
    """
    from sqlalchemy import text
    try:
        with _engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT scan_label,
                       COUNT(*)                                   AS n_scans,
                       COUNT(DISTINCT scan_date)                  AS n_days,
                       ROUND(AVG((quality->>'gamma_nz_pct')::numeric), 1) AS gamma_nz,
                       ROUND(AVG((quality->>'oi_nz_pct')::numeric), 1)    AS oi_nz,
                       ROUND(AVG((quality->>'iv_nz_pct')::numeric), 1)    AS iv_nz,
                       ROUND(AVG((quality->>'n_rows')::numeric), 0)       AS avg_rows
                FROM swing_gex_scans
                WHERE scan_date > CURRENT_DATE - :n
                GROUP BY scan_label
                ORDER BY scan_label
            """), {"n": days}).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as e:
        print(f"[swing-gex] quality_report failed: {e}", flush=True)
        return []
