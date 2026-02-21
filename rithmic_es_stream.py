# Rithmic ES Tick Data Stream — parallel pipeline writing to @ES-R
# Self-contained module. Imported by main.py, receives engine + send_telegram as params.
# NO imports from app.main — all state is local.

import os
import time
import asyncio
import threading
from datetime import datetime, time as dtime, timedelta
import pytz

ET = pytz.timezone("US/Eastern")

# ====== CONFIG ======
RITHMIC_USER = os.getenv("RITHMIC_USER", "")
RITHMIC_PASSWORD = os.getenv("RITHMIC_PASSWORD", "")
RITHMIC_SYSTEM_NAME = os.getenv("RITHMIC_SYSTEM_NAME", "Rithmic Paper Trading")
RITHMIC_URL = os.getenv("RITHMIC_URL", "")
RITHMIC_SYMBOL = "@ES-R"   # parallel symbol — change to @ES when migrating
RITHMIC_CONFORMANCE = os.getenv("RITHMIC_CONFORMANCE", "").lower() == "true"
RANGE_PTS = 5.0

# ====== STATE ======
_lock = threading.Lock()
_state = {
    "connected": False,
    "trade_date": None,
    "last_price": None,
    "last_bid": None,
    "last_ask": None,
    "cumulative_delta": 0,
    "total_volume": 0,
    "buy_volume": 0,
    "sell_volume": 0,
    "trade_count": 0,
    "_forming_bar": None,
    "_completed_bars": [],
    "_completed_bars_flushed": 0,
    "_cvd": 0,
    "_bar_idx": 0,
    "_flush_buffer": [],
    "_last_trade_time": None,
    # Rithmic-specific diagnostics
    "_aggressor_count": 0,
    "_inferred_count": 0,
    "_connection_errors": 0,
    "_last_connect_time": None,
    "_front_month": None,
}


def _now_et():
    return datetime.now(ET)


def _es_session_date():
    """ES session date: 6 PM ET → next calendar date."""
    t = _now_et()
    if t.hour >= 18:
        return (t + timedelta(days=1)).strftime("%Y-%m-%d")
    return t.strftime("%Y-%m-%d")


def _es_futures_open():
    """Check if ES futures are currently trading."""
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


# ====== TRADE CLASSIFICATION ======

def _classify_aggressor(aggressor, price, bid, ask, volume):
    """Classify trade using exchange aggressor field (Rithmic's key advantage).

    aggressor=1 (BUY)  → buyer hit the ask → buy_vol
    aggressor=2 (SELL) → seller hit the bid → sell_vol
    None/0             → fall back to bid/ask proximity (same as TradeStation)

    Returns (buy_vol, sell_vol, delta, used_aggressor: bool)
    """
    if aggressor == 1:  # BUY
        return volume, 0, volume, True
    if aggressor == 2:  # SELL
        return 0, volume, -volume, True
    # Fallback: bid/ask classification
    if price >= ask:
        return volume, 0, volume, False
    if price <= bid:
        return 0, volume, -volume, False
    mid = (bid + ask) / 2.0
    if price >= mid:
        return volume, 0, volume, False
    return 0, volume, -volume, False


# ====== RANGE BAR BUILDING ======

def _new_range_bar(price, ts):
    """Create a new forming range bar."""
    return {
        "open": price, "high": price, "low": price, "close": price,
        "volume": 0, "buy": 0, "sell": 0, "delta": 0,
        "ts_start": ts, "ts_end": ts,
        "cvd_open": _state["_cvd"],
        "cvd_high": _state["_cvd"],
        "cvd_low": _state["_cvd"],
    }


def _process_trade(price, volume, aggressor, bid, ask, ts):
    """Process a single trade tick into range bars. Must be called under _lock."""
    s = _state
    buy_vol, sell_vol, delta, used_agg = _classify_aggressor(
        aggressor, price, bid, ask, volume
    )

    s["last_price"] = price
    s["last_bid"] = bid
    s["last_ask"] = ask
    s["total_volume"] += volume
    s["buy_volume"] += buy_vol
    s["sell_volume"] += sell_vol
    s["cumulative_delta"] += delta
    s["trade_count"] += 1
    s["_last_trade_time"] = ts

    if used_agg:
        s["_aggressor_count"] += 1
    else:
        s["_inferred_count"] += 1

    # Ensure forming bar exists
    if s["_forming_bar"] is None:
        s["_forming_bar"] = _new_range_bar(price, ts)

    bar = s["_forming_bar"]
    bar["close"] = price
    bar["high"] = max(bar["high"], price)
    bar["low"] = min(bar["low"], price)
    bar["volume"] += volume
    bar["buy"] += buy_vol
    bar["sell"] += sell_vol
    bar["delta"] += delta
    bar["ts_end"] = ts

    # Track CVD within bar
    s["_cvd"] += delta
    bar["cvd_high"] = max(bar["cvd_high"], s["_cvd"])
    bar["cvd_low"] = min(bar["cvd_low"], s["_cvd"])

    # Check if range bar is complete
    if bar["high"] - bar["low"] >= RANGE_PTS - 0.001:
        completed = {
            "idx": s["_bar_idx"],
            "open": bar["open"], "high": bar["high"],
            "low": bar["low"], "close": bar["close"],
            "volume": bar["volume"], "delta": bar["delta"],
            "buy_volume": bar["buy"], "sell_volume": bar["sell"],
            "cvd": s["_cvd"],
            "cvd_open": bar["cvd_open"],
            "cvd_high": bar["cvd_high"],
            "cvd_low": bar["cvd_low"],
            "cvd_close": s["_cvd"],
            "ts_start": bar["ts_start"], "ts_end": bar["ts_end"],
            "status": "closed",
        }
        s["_completed_bars"].append(completed)
        s["_flush_buffer"].append(completed)
        s["_bar_idx"] += 1
        agg_pct = (s["_aggressor_count"] / max(s["trade_count"], 1)) * 100
        print(f"[rithmic] bar #{completed['idx']} closed: "
              f"O={completed['open']:.2f} H={completed['high']:.2f} "
              f"L={completed['low']:.2f} C={completed['close']:.2f} "
              f"vol={completed['volume']} delta={completed['delta']:+d} "
              f"cvd={completed['cvd']:+d} agg={agg_pct:.0f}%", flush=True)
        s["_forming_bar"] = _new_range_bar(price, ts)


# ====== SESSION RESET ======

def _reset_session(engine):
    """Reset state for new session. Reloads prior bars from DB."""
    session_date = _es_session_date()
    db_bars = []
    if engine:
        try:
            from sqlalchemy import text
            with engine.begin() as conn:
                rows = conn.execute(text("""
                    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                           bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                           cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
                           ts_start, ts_end, status
                    FROM es_range_bars
                    WHERE trade_date = :td AND symbol = :sym AND range_pts = :rp
                    ORDER BY bar_idx ASC
                """), {"td": session_date, "sym": RITHMIC_SYMBOL, "rp": RANGE_PTS}).mappings().all()
                for r in rows:
                    db_bars.append({
                        "idx": r["bar_idx"],
                        "open": r["bar_open"], "high": r["bar_high"],
                        "low": r["bar_low"], "close": r["bar_close"],
                        "volume": r["bar_volume"], "delta": r["bar_delta"],
                        "buy_volume": r["bar_buy_volume"], "sell_volume": r["bar_sell_volume"],
                        "cvd": r["cvd_close"],
                        "cvd_open": r["cvd_open"], "cvd_high": r["cvd_high"],
                        "cvd_low": r["cvd_low"], "cvd_close": r["cvd_close"],
                        "ts_start": r["ts_start"].isoformat() if r["ts_start"] else "",
                        "ts_end": r["ts_end"].isoformat() if r["ts_end"] else "",
                        "status": r["status"],
                    })
        except Exception as e:
            print(f"[rithmic] DB reload error: {e}", flush=True)

    with _lock:
        _state.update({
            "connected": False,
            "trade_date": session_date,
            "last_price": None,
            "last_bid": None,
            "last_ask": None,
            "cumulative_delta": 0,
            "total_volume": 0,
            "buy_volume": 0,
            "sell_volume": 0,
            "trade_count": 0,
            "_forming_bar": None,
            "_completed_bars": db_bars,
            "_completed_bars_flushed": len(db_bars),
            "_cvd": db_bars[-1]["cvd_close"] if db_bars else 0,
            "_bar_idx": (db_bars[-1]["idx"] + 1) if db_bars else 0,
            "_flush_buffer": [],
            "_last_trade_time": None,
            "_aggressor_count": 0,
            "_inferred_count": 0,
        })
    if db_bars:
        print(f"[rithmic] restored {len(db_bars)} bars from DB (session {session_date}, "
              f"cvd={_state['_cvd']:+d})", flush=True)
    else:
        print(f"[rithmic] fresh session {session_date} (no prior bars)", flush=True)


# ====== DB FLUSH ======

def flush_rithmic_bars(engine):
    """Scheduler job: flush completed range bars to DB. Writes symbol='@ES-R'."""
    try:
        if not _es_futures_open() or not engine:
            return
        from sqlalchemy import text

        with _lock:
            bars = _state["_flush_buffer"]
            _state["_flush_buffer"] = []
            diag_connected = _state.get("connected", False)
            diag_trades = _state.get("trade_count", 0)
            diag_completed = len(_state.get("_completed_bars", []))
            diag_forming = _state.get("_forming_bar")

        if not bars:
            forming_info = "none"
            if diag_forming:
                fr = diag_forming["high"] - diag_forming["low"]
                forming_info = f"{fr:.2f}/{RANGE_PTS}pt vol={diag_forming['volume']}"
            print(f"[rithmic-save] empty buffer: connected={diag_connected} trades={diag_trades} "
                  f"completed={diag_completed} forming={forming_info}", flush=True)
            return

        today = _state["trade_date"] or _es_session_date()
        with engine.begin() as conn:
            for b in bars:
                conn.execute(text("""
                    INSERT INTO es_range_bars
                        (trade_date, symbol, bar_idx, range_pts,
                         bar_open, bar_high, bar_low, bar_close,
                         bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                         cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
                         ts_start, ts_end, status, source)
                    VALUES (:td, :sym, :idx, :rp,
                            :bo, :bh, :bl, :bc,
                            :bv, :bbv, :bsv, :bd,
                            :cd, :co, :ch, :cl, :cc,
                            :ts0, :ts1, :st, 'rithmic')
                    ON CONFLICT (trade_date, symbol, bar_idx, range_pts) DO UPDATE SET
                        bar_open = EXCLUDED.bar_open, bar_high = EXCLUDED.bar_high,
                        bar_low = EXCLUDED.bar_low, bar_close = EXCLUDED.bar_close,
                        bar_volume = EXCLUDED.bar_volume, bar_buy_volume = EXCLUDED.bar_buy_volume,
                        bar_sell_volume = EXCLUDED.bar_sell_volume, bar_delta = EXCLUDED.bar_delta,
                        cumulative_delta = EXCLUDED.cumulative_delta,
                        cvd_open = EXCLUDED.cvd_open, cvd_high = EXCLUDED.cvd_high,
                        cvd_low = EXCLUDED.cvd_low, cvd_close = EXCLUDED.cvd_close,
                        ts_start = EXCLUDED.ts_start, ts_end = EXCLUDED.ts_end,
                        status = EXCLUDED.status
                """), {
                    "td": today, "sym": RITHMIC_SYMBOL, "idx": b["idx"], "rp": RANGE_PTS,
                    "bo": b["open"], "bh": b["high"], "bl": b["low"], "bc": b["close"],
                    "bv": b["volume"], "bbv": b["buy_volume"], "bsv": b["sell_volume"], "bd": b["delta"],
                    "cd": b["cvd"], "co": b["cvd_open"], "ch": b["cvd_high"],
                    "cl": b["cvd_low"], "cc": b["cvd_close"],
                    "ts0": b["ts_start"], "ts1": b["ts_end"], "st": b["status"],
                })
        print(f"[rithmic] flushed {len(bars)} range bars to DB", flush=True)
    except Exception as e:
        print(f"[rithmic] save error: {e}", flush=True)


# ====== PUBLIC API ======

def get_rithmic_bars():
    """Thread-safe snapshot of completed + forming bars for API."""
    with _lock:
        completed = list(_state["_completed_bars"])
        forming = _state["_forming_bar"]
        cvd_now = _state["_cvd"]

    result = list(completed)
    if forming and (forming["volume"] > 0 or abs(forming["open"] - forming["close"]) > 0.001):
        result.append({
            "idx": len(completed),
            "open": forming["open"], "high": forming["high"],
            "low": forming["low"], "close": forming["close"],
            "volume": forming["volume"], "delta": forming["delta"],
            "buy_volume": forming["buy"], "sell_volume": forming["sell"],
            "cvd": cvd_now,
            "cvd_open": forming["cvd_open"],
            "cvd_high": forming["cvd_high"],
            "cvd_low": forming["cvd_low"],
            "cvd_close": cvd_now,
            "ts_start": forming["ts_start"], "ts_end": forming["ts_end"],
            "status": "open",
        })
    return result


def get_rithmic_state():
    """Thread-safe status dict for health endpoint."""
    with _lock:
        total = _state["_aggressor_count"] + _state["_inferred_count"]
        agg_pct = (_state["_aggressor_count"] / total * 100) if total > 0 else 0
        return {
            "connected": _state["connected"],
            "trade_date": _state["trade_date"],
            "trade_count": _state["trade_count"],
            "total_volume": _state["total_volume"],
            "cumulative_delta": _state["cumulative_delta"],
            "completed_bars": len(_state["_completed_bars"]),
            "last_price": _state["last_price"],
            "last_trade_time": _state["_last_trade_time"],
            "aggressor_count": _state["_aggressor_count"],
            "inferred_count": _state["_inferred_count"],
            "aggressor_pct": round(agg_pct, 1),
            "connection_errors": _state["_connection_errors"],
            "last_connect_time": _state["_last_connect_time"],
            "front_month": _state["_front_month"],
            "symbol": RITHMIC_SYMBOL,
        }


# ====== ASYNC STREAM ======

async def _rithmic_stream_async(engine, send_telegram_fn):
    """Main async loop: connect to Rithmic, subscribe to ES ticks, build range bars."""
    from async_rithmic import RithmicClient, DataType, LastTradePresenceBits, BestBidOfferPresenceBits

    latest_bbo = {"bid": None, "ask": None}
    backoff = 1.0

    while True:
        try:
            # Wait for ES futures session (skip check during conformance)
            if not RITHMIC_CONFORMANCE and not _es_futures_open():
                backoff = 1.0
                await asyncio.sleep(30)
                continue

            # Session date check
            session_date = _es_session_date()
            if _state["trade_date"] != session_date:
                _reset_session(engine)
                latest_bbo = {"bid": None, "ask": None}

            client = RithmicClient(
                user=RITHMIC_USER,
                password=RITHMIC_PASSWORD,
                system_name=RITHMIC_SYSTEM_NAME,
                app_name="faal:0dte_alpha",
                app_version="1.0",
                url=RITHMIC_URL,
            )

            await client.connect()
            backoff = 1.0

            with _lock:
                _state["connected"] = True
                _state["_last_connect_time"] = _now_et().isoformat()
                _state["_connection_errors"] = max(0, _state["_connection_errors"])

            # Conformance mode: just stay logged in, don't subscribe to data
            if RITHMIC_CONFORMANCE:
                print("[rithmic] CONFORMANCE MODE: connected and staying logged in", flush=True)
                try:
                    if send_telegram_fn:
                        send_telegram_fn("[Rithmic] Conformance mode — connected to order plant, waiting for review")
                except Exception:
                    pass
                while True:
                    await asyncio.sleep(60)
                    print("[rithmic] conformance: still connected", flush=True)

            # Resolve front-month contract (e.g. "ESH6")
            front_month = await client.get_front_month_contract("ES", "CME")
            with _lock:
                _state["_front_month"] = front_month
            print(f"[rithmic] connected, front month: {front_month}", flush=True)

            try:
                if send_telegram_fn:
                    send_telegram_fn(f"[Rithmic] Connected — streaming {front_month} → {RITHMIC_SYMBOL}")
            except Exception:
                pass

            # Tick callback
            async def on_tick(data):
                if data["data_type"] == DataType.BBO:
                    # Update latest BBO
                    pb = data.get("presence_bits", 0)
                    if pb & BestBidOfferPresenceBits.BID and "bid_price" in data:
                        latest_bbo["bid"] = data["bid_price"]
                    if pb & BestBidOfferPresenceBits.ASK and "ask_price" in data:
                        latest_bbo["ask"] = data["ask_price"]

                elif data["data_type"] == DataType.LAST_TRADE:
                    pb = data.get("presence_bits", 0)
                    if not (pb & LastTradePresenceBits.LAST_TRADE):
                        return

                    price = data.get("trade_price")
                    size = data.get("trade_size")
                    if price is None or size is None:
                        return

                    aggressor = data.get("aggressor")  # 1=BUY, 2=SELL, None=unknown
                    bid = latest_bbo.get("bid")
                    ask = latest_bbo.get("ask")

                    # Need at least bid/ask for fallback classification
                    if bid is None or ask is None:
                        return

                    ts = _now_et().isoformat()
                    with _lock:
                        # Session rollover check
                        new_session = _es_session_date()
                        if _state["trade_date"] != new_session:
                            pass  # Will be caught in outer loop

                        _process_trade(price, size, aggressor, bid, ask, ts)
                        tc = _state["trade_count"]
                        if tc <= 5 or tc % 1000 == 0:
                            fb = _state.get("_forming_bar")
                            fb_range = f"{fb['high'] - fb['low']:.2f}" if fb else "?"
                            agg_pct = (_state["_aggressor_count"] / max(tc, 1)) * 100
                            print(f"[rithmic] trade #{tc}: price={price} vol={size} "
                                  f"agg={'BUY' if aggressor == 1 else 'SELL' if aggressor == 2 else '?'} "
                                  f"completed={len(_state['_completed_bars'])} "
                                  f"forming_range={fb_range}/{RANGE_PTS} "
                                  f"agg_pct={agg_pct:.0f}%", flush=True)

            client.on_tick += on_tick
            await client.subscribe_to_market_data(
                front_month, "CME", DataType.LAST_TRADE | DataType.BBO
            )

            # Keep alive — check session/connection periodically
            while True:
                await asyncio.sleep(10)
                if not _es_futures_open():
                    print("[rithmic] ES session closed, disconnecting", flush=True)
                    break
                # Session rollover
                new_session = _es_session_date()
                if _state["trade_date"] != new_session:
                    print(f"[rithmic] session rollover → {new_session}", flush=True)
                    _reset_session(engine)
                    latest_bbo = {"bid": None, "ask": None}

            # Clean disconnect
            try:
                await client.unsubscribe_from_market_data(
                    front_month, "CME", DataType.LAST_TRADE | DataType.BBO
                )
            except Exception:
                pass
            try:
                await client.disconnect()
            except Exception:
                pass

        except Exception as e:
            print(f"[rithmic] stream error: {e}", flush=True)
            with _lock:
                _state["connected"] = False
                _state["_connection_errors"] += 1

        with _lock:
            _state["connected"] = False

        reconnect_wait = min(backoff, 60)
        print(f"[rithmic] reconnecting in {reconnect_wait:.0f}s", flush=True)
        await asyncio.sleep(reconnect_wait)
        backoff = min(backoff * 2, 60)


def _run_async_loop(engine, send_telegram_fn):
    """Thread target: runs the async event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_rithmic_stream_async(engine, send_telegram_fn))
    except Exception as e:
        print(f"[rithmic] fatal loop error: {e}", flush=True)
    finally:
        loop.close()


# ====== PUBLIC ENTRY POINT ======

def start_rithmic_stream(engine, send_telegram_fn=None):
    """Start the Rithmic ES stream in a daemon thread.

    Skips silently if RITHMIC_USER is not set (no credentials = no stream).
    """
    if not RITHMIC_USER:
        print("[rithmic] RITHMIC_USER not set, skipping Rithmic stream", flush=True)
        return

    if not RITHMIC_URL:
        print("[rithmic] RITHMIC_URL not set, skipping Rithmic stream", flush=True)
        return

    t = threading.Thread(
        target=_run_async_loop,
        args=(engine, send_telegram_fn),
        daemon=True,
        name="rithmic-es-stream",
    )
    t.start()
    print(f"[rithmic] stream thread started (user={RITHMIC_USER}, system={RITHMIC_SYSTEM_NAME})", flush=True)
