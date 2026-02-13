# rithmic_delta_worker.py
# ES Cumulative Delta worker — connects to Rithmic, classifies trades, writes to PostgreSQL
import os, asyncio, traceback
from datetime import datetime, timezone, time as dtime, timedelta
import pytz

import psycopg
from psycopg.rows import dict_row
from async_rithmic import (
    RithmicClient, DataType,
    LastTradePresenceBits, BestBidOfferPresenceBits,
    ReconnectionSettings,
)

# ====== CONFIG ======
DB_URL = os.getenv("DATABASE_URL", "")

RITHMIC_USER       = os.getenv("RITHMIC_USER", "")
RITHMIC_PASSWORD   = os.getenv("RITHMIC_PASSWORD", "")
RITHMIC_SYSTEM     = os.getenv("RITHMIC_SYSTEM", "Rithmic Paper Trading")
RITHMIC_URL        = os.getenv("RITHMIC_URL", "rituz00100.rithmic.com:443")
RITHMIC_APP_NAME   = os.getenv("RITHMIC_APP_NAME", "0dte_alpha")
RITHMIC_APP_VERSION = os.getenv("RITHMIC_APP_VERSION", "1.0")

FLUSH_EVERY_SEC = 30
SYMBOL = "ES"
EXCHANGE = "CME"

NY = pytz.timezone("US/Eastern")

# ====== STATE ======
_state = {
    "cumulative_delta": 0,
    "total_volume": 0,
    "buy_volume": 0,
    "sell_volume": 0,
    "tick_count": 0,
    "last_price": None,
    "bid_price": None,
    "ask_price": None,
    "session_high": None,
    "session_low": None,
    "trade_date": None,
    "security_code": None,
    # 1-min bar state
    "bar_minute": None,       # current minute (floored datetime)
    "bar_open_delta": None,
    "bar_close_delta": None,
    "bar_high_delta": None,
    "bar_low_delta": None,
    "bar_volume": 0,
    "bar_buy_volume": 0,
    "bar_sell_volume": 0,
    "bar_open_price": None,
    "bar_close_price": None,
    "bar_high_price": None,
    "bar_low_price": None,
}


def market_open_now() -> bool:
    t = datetime.now(NY)
    return dtime(9, 20) <= t.time() <= dtime(16, 10)


def current_trade_date() -> str:
    return datetime.now(NY).strftime("%Y-%m-%d")


def db():
    return psycopg.connect(DB_URL, autocommit=True, row_factory=dict_row)


def ensure_tables():
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS rithmic_delta_snapshots (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            trade_date DATE NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            security_code VARCHAR(20),
            cumulative_delta BIGINT NOT NULL DEFAULT 0,
            total_volume BIGINT NOT NULL DEFAULT 0,
            buy_volume BIGINT NOT NULL DEFAULT 0,
            sell_volume BIGINT NOT NULL DEFAULT 0,
            last_price DOUBLE PRECISION,
            bid_price DOUBLE PRECISION,
            ask_price DOUBLE PRECISION,
            tick_count BIGINT NOT NULL DEFAULT 0,
            bar_high DOUBLE PRECISION,
            bar_low DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS idx_rithmic_delta_snap_ts ON rithmic_delta_snapshots(ts DESC);
        CREATE INDEX IF NOT EXISTS idx_rithmic_delta_snap_date ON rithmic_delta_snapshots(trade_date DESC);
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS rithmic_delta_bars (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL,
            trade_date DATE NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            bar_open_delta BIGINT,
            bar_close_delta BIGINT,
            bar_high_delta BIGINT,
            bar_low_delta BIGINT,
            bar_volume BIGINT NOT NULL DEFAULT 0,
            bar_buy_volume BIGINT NOT NULL DEFAULT 0,
            bar_sell_volume BIGINT NOT NULL DEFAULT 0,
            bar_open_price DOUBLE PRECISION,
            bar_close_price DOUBLE PRECISION,
            bar_high_price DOUBLE PRECISION,
            bar_low_price DOUBLE PRECISION,
            UNIQUE(ts, symbol)
        );
        CREATE INDEX IF NOT EXISTS idx_rithmic_delta_bars_ts ON rithmic_delta_bars(ts DESC);
        """)
    print("[rithmic] tables ready", flush=True)


def reset_daily():
    """Reset all in-memory state for a new trading day."""
    _state["cumulative_delta"] = 0
    _state["total_volume"] = 0
    _state["buy_volume"] = 0
    _state["sell_volume"] = 0
    _state["tick_count"] = 0
    _state["last_price"] = None
    _state["bid_price"] = None
    _state["ask_price"] = None
    _state["session_high"] = None
    _state["session_low"] = None
    _state["trade_date"] = current_trade_date()
    _state["bar_minute"] = None
    _state["bar_open_delta"] = None
    _state["bar_close_delta"] = None
    _state["bar_high_delta"] = None
    _state["bar_low_delta"] = None
    _state["bar_volume"] = 0
    _state["bar_buy_volume"] = 0
    _state["bar_sell_volume"] = 0
    _state["bar_open_price"] = None
    _state["bar_close_price"] = None
    _state["bar_high_price"] = None
    _state["bar_low_price"] = None
    print(f"[rithmic] daily reset for {_state['trade_date']}", flush=True)


def flush_bar_to_db(bar_minute):
    """Write the completed 1-minute bar to DB."""
    if bar_minute is None or _state["bar_open_delta"] is None:
        return
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rithmic_delta_bars
                    (ts, trade_date, symbol,
                     bar_open_delta, bar_close_delta, bar_high_delta, bar_low_delta,
                     bar_volume, bar_buy_volume, bar_sell_volume,
                     bar_open_price, bar_close_price, bar_high_price, bar_low_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts, symbol) DO UPDATE SET
                    bar_close_delta = EXCLUDED.bar_close_delta,
                    bar_high_delta  = GREATEST(rithmic_delta_bars.bar_high_delta, EXCLUDED.bar_high_delta),
                    bar_low_delta   = LEAST(rithmic_delta_bars.bar_low_delta, EXCLUDED.bar_low_delta),
                    bar_volume      = EXCLUDED.bar_volume,
                    bar_buy_volume  = EXCLUDED.bar_buy_volume,
                    bar_sell_volume = EXCLUDED.bar_sell_volume,
                    bar_close_price = EXCLUDED.bar_close_price,
                    bar_high_price  = GREATEST(rithmic_delta_bars.bar_high_price, EXCLUDED.bar_high_price),
                    bar_low_price   = LEAST(rithmic_delta_bars.bar_low_price, EXCLUDED.bar_low_price)
            """, (
                bar_minute, _state["trade_date"], SYMBOL,
                _state["bar_open_delta"], _state["bar_close_delta"],
                _state["bar_high_delta"], _state["bar_low_delta"],
                _state["bar_volume"], _state["bar_buy_volume"], _state["bar_sell_volume"],
                _state["bar_open_price"], _state["bar_close_price"],
                _state["bar_high_price"], _state["bar_low_price"],
            ))
    except Exception as e:
        print(f"[rithmic] bar flush error: {e}", flush=True)


def start_new_bar(minute_dt):
    """Flush previous bar and start a new 1-minute bar."""
    # Flush old bar first
    if _state["bar_minute"] is not None and market_open_now():
        flush_bar_to_db(_state["bar_minute"])

    _state["bar_minute"] = minute_dt
    _state["bar_open_delta"] = _state["cumulative_delta"]
    _state["bar_close_delta"] = _state["cumulative_delta"]
    _state["bar_high_delta"] = _state["cumulative_delta"]
    _state["bar_low_delta"] = _state["cumulative_delta"]
    _state["bar_volume"] = 0
    _state["bar_buy_volume"] = 0
    _state["bar_sell_volume"] = 0
    _state["bar_open_price"] = _state["last_price"]
    _state["bar_close_price"] = _state["last_price"]
    _state["bar_high_price"] = _state["last_price"]
    _state["bar_low_price"] = _state["last_price"]


def process_trade(data: dict):
    """Classify a trade and update cumulative delta + bar state."""
    price = data.get("trade_price")
    size = data.get("trade_size", 0)
    if price is None or size == 0:
        return

    # Check for daily reset
    today = current_trade_date()
    if _state["trade_date"] != today:
        reset_daily()

    # Classify trade direction
    aggressor = data.get("aggressor")
    if aggressor == 1:       # BUY
        delta = size
    elif aggressor == 2:     # SELL
        delta = -size
    else:
        # Fallback: compare to bid/ask
        bid = _state["bid_price"]
        ask = _state["ask_price"]
        if ask is not None and price >= ask:
            delta = size
        elif bid is not None and price <= bid:
            delta = -size
        else:
            # At midpoint or no BBO data — skip (neutral)
            delta = 0

    # Update cumulative state
    _state["cumulative_delta"] += delta
    _state["total_volume"] += size
    if delta > 0:
        _state["buy_volume"] += size
    elif delta < 0:
        _state["sell_volume"] += size
    _state["tick_count"] += 1
    _state["last_price"] = price

    # Session high/low
    if _state["session_high"] is None or price > _state["session_high"]:
        _state["session_high"] = price
    if _state["session_low"] is None or price < _state["session_low"]:
        _state["session_low"] = price

    # 1-minute bar tracking
    now = datetime.now(NY)
    minute_dt = now.replace(second=0, microsecond=0)
    if _state["bar_minute"] is None or minute_dt != _state["bar_minute"]:
        start_new_bar(minute_dt)

    _state["bar_close_delta"] = _state["cumulative_delta"]
    if _state["bar_high_delta"] is None or _state["cumulative_delta"] > _state["bar_high_delta"]:
        _state["bar_high_delta"] = _state["cumulative_delta"]
    if _state["bar_low_delta"] is None or _state["cumulative_delta"] < _state["bar_low_delta"]:
        _state["bar_low_delta"] = _state["cumulative_delta"]

    _state["bar_volume"] += size
    if delta > 0:
        _state["bar_buy_volume"] += size
    elif delta < 0:
        _state["bar_sell_volume"] += size

    _state["bar_close_price"] = price
    if _state["bar_high_price"] is None or price > _state["bar_high_price"]:
        _state["bar_high_price"] = price
    if _state["bar_low_price"] is None or price < _state["bar_low_price"]:
        _state["bar_low_price"] = price


def process_bbo(data: dict):
    """Update best bid/offer state."""
    if data.get("presence_bits", 0) & BestBidOfferPresenceBits.BID:
        bid = data.get("bid_price")
        if bid is not None:
            _state["bid_price"] = bid
    if data.get("presence_bits", 0) & BestBidOfferPresenceBits.ASK:
        ask = data.get("ask_price")
        if ask is not None:
            _state["ask_price"] = ask


def flush_snapshot_to_db():
    """Write current cumulative delta state to rithmic_delta_snapshots."""
    if _state["trade_date"] is None or _state["total_volume"] == 0:
        return
    if not market_open_now():
        return
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rithmic_delta_snapshots
                    (trade_date, symbol, security_code,
                     cumulative_delta, total_volume, buy_volume, sell_volume,
                     last_price, bid_price, ask_price, tick_count,
                     bar_high, bar_low)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                _state["trade_date"], SYMBOL, _state["security_code"],
                _state["cumulative_delta"], _state["total_volume"],
                _state["buy_volume"], _state["sell_volume"],
                _state["last_price"], _state["bid_price"], _state["ask_price"],
                _state["tick_count"],
                _state["session_high"], _state["session_low"],
            ))
        print(f"[rithmic] snapshot: delta={_state['cumulative_delta']:+d}  "
              f"vol={_state['total_volume']}  buy={_state['buy_volume']}  "
              f"sell={_state['sell_volume']}  price={_state['last_price']}", flush=True)
    except Exception as e:
        print(f"[rithmic] snapshot flush error: {e}", flush=True)


async def flush_loop():
    """Periodically flush snapshot + current bar to DB."""
    while True:
        await asyncio.sleep(FLUSH_EVERY_SEC)
        try:
            flush_snapshot_to_db()
            # Also flush the current (incomplete) bar so API has latest data
            if _state["bar_minute"] is not None and market_open_now():
                flush_bar_to_db(_state["bar_minute"])
        except Exception as e:
            print(f"[rithmic] flush loop error: {e}", flush=True)


async def on_tick(data: dict):
    """Callback for each tick from Rithmic."""
    if data["data_type"] == DataType.LAST_TRADE:
        if data.get("presence_bits", 0) & LastTradePresenceBits.LAST_TRADE:
            process_trade(data)
    elif data["data_type"] == DataType.BBO:
        process_bbo(data)


async def on_connected(plant_type: str):
    print(f"[rithmic] connected to {plant_type}", flush=True)


async def on_disconnected(plant_type: str):
    print(f"[rithmic] disconnected from {plant_type}", flush=True)


async def run():
    """Main loop — connect, subscribe, stream, reconnect on failure."""
    ensure_tables()
    reset_daily()

    while True:
        client = None
        try:
            client = RithmicClient(
                user=RITHMIC_USER,
                password=RITHMIC_PASSWORD,
                system_name=RITHMIC_SYSTEM,
                app_name=RITHMIC_APP_NAME,
                app_version=RITHMIC_APP_VERSION,
                url=RITHMIC_URL,
                reconnection_settings=ReconnectionSettings(
                    max_retries=None,
                    backoff_type="exponential",
                    interval=2,
                    max_delay=60,
                    jitter_range=(0.5, 2.0),
                ),
            )
            client.on_connected += on_connected
            client.on_disconnected += on_disconnected

            print("[rithmic] connecting...", flush=True)
            await client.connect()

            # Resolve front-month ES contract
            security_code = await client.get_front_month_contract(SYMBOL, EXCHANGE)
            _state["security_code"] = security_code
            print(f"[rithmic] front-month contract: {security_code}", flush=True)

            # Subscribe to trades + BBO
            client.on_tick += on_tick
            data_type = DataType.LAST_TRADE | DataType.BBO
            await client.subscribe_to_market_data(security_code, EXCHANGE, data_type)
            print(f"[rithmic] subscribed to {security_code} LAST_TRADE+BBO", flush=True)

            # Start flush loop alongside streaming
            flush_task = asyncio.create_task(flush_loop())
            try:
                # Run until error or cancellation — library handles reconnection internally
                while True:
                    await asyncio.sleep(60)
                    # Log heartbeat
                    if _state["total_volume"] > 0:
                        print(f"[rithmic] heartbeat: delta={_state['cumulative_delta']:+d}  "
                              f"vol={_state['total_volume']}  ticks={_state['tick_count']}", flush=True)
            finally:
                flush_task.cancel()
                try:
                    await flush_task
                except asyncio.CancelledError:
                    pass
                await client.unsubscribe_from_market_data(security_code, EXCHANGE, data_type)
                await client.disconnect()

        except Exception as e:
            print(f"[rithmic] error: {e}", flush=True)
            traceback.print_exc()
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            print("[rithmic] reconnecting in 30s...", flush=True)
            await asyncio.sleep(30)


if __name__ == "__main__":
    print("[rithmic] ES Cumulative Delta worker starting", flush=True)
    asyncio.run(run())
