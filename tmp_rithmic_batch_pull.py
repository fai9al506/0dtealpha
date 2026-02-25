"""Batch pull ES ticks from Rithmic historical API for all trading days with Volland data.

Saves range bars to es_range_bars DB table with source='rithmic_hist'.
Skips dates that already have data in the DB.

Usage:
    python tmp_rithmic_batch_pull.py              # pull all missing dates
    python tmp_rithmic_batch_pull.py 2026-02-10   # pull single date
"""
import asyncio
import sys
import json
import os
from datetime import datetime, timezone, timedelta, date
from collections import defaultdict
from sqlalchemy import create_engine, text

# ====== CONFIG ======
RITHMIC_USER = "faisal.a.d@msn.com"
RITHMIC_PASSWORD = "7fPwgvH2$@uT2H5"
RITHMIC_SYSTEM = "Rithmic Paper Trading"
RITHMIC_URL = "wss://rprotocol.rithmic.com:443"
RANGE_PTS = 5.0
DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
FRONT_MONTH = "ESH6"  # Mar 2026

engine = create_engine(DB_URL)


def get_volland_dates():
    """Get all trading dates that have Volland data."""
    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT DISTINCT DATE(ts) as day
            FROM volland_snapshots
            WHERE payload->'statistics'->>'lines_in_sand' IS NOT NULL
            ORDER BY day
        """)).fetchall()
    return [row[0] for row in r]


def get_existing_rithmic_dates():
    """Get dates that already have rithmic_hist data."""
    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT DISTINCT DATE(ts_start) as day
            FROM es_range_bars
            WHERE source = 'rithmic_hist'
            ORDER BY day
        """)).fetchall()
    return set(row[0] for row in r)


async def pull_ticks_for_date(date_str):
    """Pull ES ticks for a single date from Rithmic History Plant."""
    from async_rithmic import RithmicClient
    from async_rithmic.enums import SysInfraType

    client = RithmicClient(
        user=RITHMIC_USER, password=RITHMIC_PASSWORD,
        system_name=RITHMIC_SYSTEM, app_name="faal:0dte_alpha",
        app_version="1.0", url=RITHMIC_URL,
    )
    await client.connect(plants=[SysInfraType.HISTORY_PLANT])

    d = datetime.strptime(date_str, "%Y-%m-%d")
    rth_start = datetime(d.year, d.month, d.day, 14, 30, 0, tzinfo=timezone.utc)
    rth_end = datetime(d.year, d.month, d.day, 21, 0, 0, tzinfo=timezone.utc)

    raw_ticks = []
    s = rth_start
    chunk = timedelta(minutes=1)

    while s < rth_end:
        e = min(s + chunk, rth_end)
        try:
            ticks = await client.get_historical_tick_data(FRONT_MONTH, "CME", s, e)
            raw_ticks.extend(ticks)
        except Exception as ex:
            print(f"    ERROR at {s.strftime('%H:%M')}: {ex}")
        s = e

    await client.disconnect()
    return raw_ticks


def aggregate_ticks(raw_ticks):
    """Aggregate Rithmic sub-fills by timestamp + price + side."""
    agg = defaultdict(lambda: {"volume": 0, "buy_vol": 0, "sell_vol": 0, "count": 0})

    for t in raw_ticks:
        ssboe = t["data_bar_ssboe"][0]
        usecs = t["data_bar_usecs"][0]
        price = t["close_price"]
        is_buy = "bid_volume" in t
        side = "B" if is_buy else "A"

        key = (ssboe, usecs, price, side)
        agg[key]["volume"] += int(t.get("volume", 1))
        agg[key]["buy_vol"] += int(t.get("bid_volume", 0))
        agg[key]["sell_vol"] += int(t.get("ask_volume", 0))
        agg[key]["count"] += 1
        agg[key]["price"] = price
        agg[key]["ssboe"] = ssboe
        agg[key]["usecs"] = usecs

    trades = []
    for key, v in sorted(agg.items(), key=lambda x: (x[0][0], x[0][1])):
        trades.append({
            "price": v["price"],
            "volume": v["volume"],
            "buy_vol": v["buy_vol"],
            "sell_vol": v["sell_vol"],
            "delta": v["buy_vol"] - v["sell_vol"],
            "ssboe": v["ssboe"],
            "usecs": v["usecs"],
            "datetime": datetime.fromtimestamp(v["ssboe"], tz=timezone.utc).replace(microsecond=v["usecs"]),
        })
    return trades


def build_range_bars(trades):
    """Build 5-pt range bars from aggregated trades."""
    bars = []
    cvd = 0
    forming = None

    for t in trades:
        price = t["price"]
        vol = t["volume"]
        delta = t["delta"]
        ts = t["datetime"]

        if forming is None:
            forming = {"open": price, "high": price, "low": price, "close": price,
                       "volume": 0, "delta": 0, "buy": 0, "sell": 0,
                       "ts_start": ts, "cvd_open": cvd}

        forming["close"] = price
        forming["high"] = max(forming["high"], price)
        forming["low"] = min(forming["low"], price)
        forming["volume"] += vol
        forming["delta"] += delta
        forming["buy"] += t["buy_vol"]
        forming["sell"] += t["sell_vol"]
        forming["ts_end"] = ts
        cvd += delta

        if forming["high"] - forming["low"] >= RANGE_PTS - 0.001:
            forming["cvd_close"] = cvd
            bars.append(forming)
            forming = {"open": price, "high": price, "low": price, "close": price,
                       "volume": 0, "delta": 0, "buy": 0, "sell": 0,
                       "ts_start": ts, "cvd_open": cvd}

    return bars


def save_bars_to_db(bars, date_str):
    """Save range bars to es_range_bars table with source='rithmic_hist'."""
    if not bars:
        return 0

    with engine.begin() as conn:
        # Delete any existing rithmic_hist bars for this date
        conn.execute(text("""
            DELETE FROM es_range_bars
            WHERE source = 'rithmic_hist'
              AND DATE(ts_start) = :d
        """), {"d": date_str})

        for i, b in enumerate(bars):
            cvd_high = max(b["cvd_open"], b["cvd_close"])
            cvd_low = min(b["cvd_open"], b["cvd_close"])
            conn.execute(text("""
                INSERT INTO es_range_bars
                    (symbol, source, idx, open, high, low, close,
                     volume, buy_volume, sell_volume, delta,
                     cvd, cvd_open, cvd_high, cvd_low, cvd_close,
                     ts_start, ts_end, status)
                VALUES
                    (:sym, 'rithmic_hist', :idx, :o, :h, :l, :c,
                     :vol, :bv, :sv, :d,
                     :cvd, :cvd_o, :cvd_h, :cvd_l, :cvd_c,
                     :ts_s, :ts_e, 'closed')
            """), {
                "sym": "@ES", "idx": i,
                "o": b["open"], "h": b["high"], "l": b["low"], "c": b["close"],
                "vol": b["volume"], "bv": b["buy"], "sv": b["sell"], "d": b["delta"],
                "cvd": b["cvd_close"], "cvd_o": b["cvd_open"],
                "cvd_h": cvd_high, "cvd_l": cvd_low, "cvd_c": b["cvd_close"],
                "ts_s": b["ts_start"], "ts_e": b["ts_end"],
            })
    return len(bars)


def save_bars_to_json(bars, date_str):
    """Save range bars to JSON for local backup."""
    out_dir = "historical_data"
    os.makedirs(out_dir, exist_ok=True)
    bars_out = []
    for b in bars:
        bars_out.append({
            "open": b["open"], "high": b["high"], "low": b["low"], "close": b["close"],
            "volume": b["volume"], "delta": b["delta"], "buy": b["buy"], "sell": b["sell"],
            "cvd_close": b["cvd_close"], "cvd_open": b["cvd_open"],
            "ts_start": b["ts_start"].isoformat(), "ts_end": b["ts_end"].isoformat(),
        })
    path = f"{out_dir}/es_rangebars_{date_str}.json"
    with open(path, "w") as f:
        json.dump(bars_out, f)
    return path


async def pull_single_date(date_str):
    """Pull, aggregate, build bars, save to DB and JSON for one date."""
    print(f"\n{'='*60}")
    print(f"  Pulling {date_str}...")
    print(f"{'='*60}")

    raw_ticks = await pull_ticks_for_date(date_str)
    if not raw_ticks:
        print(f"  No ticks returned for {date_str} — skipping (holiday/no data)")
        return 0

    print(f"  Raw ticks: {len(raw_ticks)}")
    trades = aggregate_ticks(raw_ticks)
    print(f"  Aggregated: {len(trades)} trades")

    bars = build_range_bars(trades)
    print(f"  Range bars: {len(bars)}")

    if bars:
        n = save_bars_to_db(bars, date_str)
        jp = save_bars_to_json(bars, date_str)
        price_lo = min(b['low'] for b in bars)
        price_hi = max(b['high'] for b in bars)
        cvd_final = bars[-1]['cvd_close']
        print(f"  Saved {n} bars to DB + {jp}")
        print(f"  Price: {price_lo:.2f} - {price_hi:.2f} | CVD final: {cvd_final:+d}")
    return len(bars)


async def main():
    single_date = sys.argv[1] if len(sys.argv) > 1 else None

    if single_date:
        dates = [single_date]
    else:
        # Get all Volland dates, exclude already-pulled
        all_dates = get_volland_dates()
        existing = get_existing_rithmic_dates()
        dates = [str(d) for d in all_dates if d not in existing]
        print(f"Volland data covers {len(all_dates)} days")
        print(f"Already have rithmic_hist for {len(existing)} days")
        print(f"Need to pull: {len(dates)} days")
        if existing:
            print(f"Existing: {sorted(existing)}")

    if not dates:
        print("Nothing to pull!")
        return

    print(f"\nDates to pull: {dates}")
    total_bars = 0
    success = 0
    failed = []

    for date_str in dates:
        try:
            n = await pull_single_date(date_str)
            if n > 0:
                total_bars += n
                success += 1
            # Small delay between dates to be nice to Rithmic
            await asyncio.sleep(2)
        except Exception as ex:
            print(f"  FAILED {date_str}: {ex}")
            failed.append((date_str, str(ex)))
            # Reconnect pause
            await asyncio.sleep(5)

    print(f"\n{'='*60}")
    print(f"  BATCH COMPLETE")
    print(f"  Success: {success}/{len(dates)} days")
    print(f"  Total bars: {total_bars}")
    if failed:
        print(f"  Failed: {[f[0] for f in failed]}")
        for d, e in failed:
            print(f"    {d}: {e}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
