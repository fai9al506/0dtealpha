"""
VX (VIX Futures) Tick Data Analysis - VXM26
Reads Sierra Chart .scid file and performs comprehensive intraday analysis.
"""

import struct
from datetime import datetime, timedelta, timezone
from collections import defaultdict

SCID_FILE = r"C:\SierraChart\Data\VXM26_FUT_CFE.scid"
EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)
TODAY_STR = "2026-03-25"

# Market hours in UTC (ET+4 during EDT: 09:30 ET = 13:30 UTC, 16:00 ET = 20:00 UTC)
MKT_START_UTC = 13 * 3600 + 30 * 60  # 13:30 UTC in seconds
MKT_END_UTC = 20 * 3600              # 20:00 UTC in seconds


def parse_scid(filepath):
    """Parse .scid file, return list of tick records for today's market hours."""
    with open(filepath, "rb") as f:
        raw = f.read()

    # 56-byte header
    header = struct.unpack_from("<4sIIHHI36s", raw, 0)
    header_size = 56
    record_size = 40
    num_records = (len(raw) - header_size) // record_size

    print(f"File: {filepath}")
    print(f"Header magic: {header[0]}")
    print(f"Total records in file: {num_records:,}")
    print()

    ticks = []
    today_date = datetime.strptime(TODAY_STR, "%Y-%m-%d").date()

    for i in range(num_records):
        offset = header_size + i * record_size
        rec = struct.unpack_from("<qffffIIII", raw, offset)
        dt_int64, open_p, high_p, low_p, close_p, num_trades, total_vol, bid_vol, ask_vol = rec

        # Skip sub-trade markers
        if open_p < -1e30:
            continue

        # Convert datetime
        days = dt_int64 // 86_400_000_000
        remainder = dt_int64 % 86_400_000_000
        dt = EPOCH + timedelta(days=days, microseconds=remainder)

        # Filter to today only
        if dt.date() != today_date:
            continue

        # Filter to market hours (13:30 - 20:00 UTC)
        secs = dt.hour * 3600 + dt.minute * 60 + dt.second
        if secs < MKT_START_UTC or secs >= MKT_END_UTC:
            continue

        # Convert to ET for display (UTC - 4 during EDT)
        dt_et = dt - timedelta(hours=4)

        # Determine side
        if bid_vol > 0 and ask_vol == 0:
            side = "SELL"
            vol = bid_vol
        elif ask_vol > 0 and bid_vol == 0:
            side = "BUY"
            vol = ask_vol
        elif bid_vol > 0 and ask_vol > 0:
            side = "MIXED"
            vol = bid_vol + ask_vol
        else:
            side = "UNKNOWN"
            vol = total_vol if total_vol > 0 else 1

        ticks.append({
            "dt_utc": dt,
            "dt_et": dt_et,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "price": close_p,
            "num_trades": num_trades,
            "total_vol": total_vol,
            "bid_vol": bid_vol,
            "ask_vol": ask_vol,
            "side": side,
            "vol": vol,
        })

    print(f"Today's market-hours ticks: {len(ticks):,}")
    if ticks:
        print(f"First tick: {ticks[0]['dt_et'].strftime('%H:%M:%S')} ET  price={ticks[0]['price']:.2f}")
        print(f"Last tick:  {ticks[-1]['dt_et'].strftime('%H:%M:%S')} ET  price={ticks[-1]['price']:.2f}")
    print()
    return ticks


def bucket_label(dt_et):
    """Return 30-min bucket label like '09:30-10:00'."""
    h, m = dt_et.hour, dt_et.minute
    bucket_m = (m // 30) * 30
    start = f"{h:02d}:{bucket_m:02d}"
    end_m = bucket_m + 30
    if end_m >= 60:
        end = f"{h+1:02d}:00"
    else:
        end = f"{h:02d}:{end_m:02d}"
    return start, end


def analysis_30min_buckets(ticks):
    """30-minute bucket analysis."""
    print("=" * 90)
    print("1. 30-MINUTE BUCKET ANALYSIS")
    print("=" * 90)

    buckets = defaultdict(lambda: {
        "high": -999, "low": 999,
        "total_vol": 0, "buy_vol": 0, "sell_vol": 0,
        "ticks": 0
    })

    for t in ticks:
        start, end = bucket_label(t["dt_et"])
        key = f"{start}-{end}"
        b = buckets[key]
        b["high"] = max(b["high"], t["price"])
        b["low"] = min(b["low"], t["price"])
        b["total_vol"] += t["total_vol"]
        b["buy_vol"] += t["ask_vol"]
        b["sell_vol"] += t["bid_vol"]
        b["ticks"] += 1

    # Sort by bucket key
    sorted_keys = sorted(buckets.keys())

    cum_delta = 0
    print(f"{'Bucket':<14} {'High':>7} {'Low':>7} {'Range':>6} {'TotVol':>8} {'BuyVol':>8} {'SellVol':>8} {'Delta':>8} {'CumDelta':>9}")
    print("-" * 90)

    for key in sorted_keys:
        b = buckets[key]
        if b["ticks"] == 0:
            continue
        delta = b["buy_vol"] - b["sell_vol"]
        cum_delta += delta
        rng = b["high"] - b["low"]
        print(f"{key:<14} {b['high']:>7.2f} {b['low']:>7.2f} {rng:>6.2f} {b['total_vol']:>8,} {b['buy_vol']:>8,} {b['sell_vol']:>8,} {delta:>+8,} {cum_delta:>+9,}")

    print()


def analysis_large_trades(ticks):
    """Find trades >= 20 contracts."""
    print("=" * 90)
    print("2. LARGE TRADES (>= 20 contracts)")
    print("=" * 90)

    large = [t for t in ticks if t["total_vol"] >= 20]

    if not large:
        print("No trades >= 20 contracts found today.")
        print()
        return

    print(f"Found {len(large)} large trades")
    print()
    print(f"{'Time ET':<12} {'Price':>7} {'Size':>6} {'Side':<6} {'BidVol':>7} {'AskVol':>7}")
    print("-" * 55)

    for t in large:
        time_str = t["dt_et"].strftime("%H:%M:%S")
        print(f"{time_str:<12} {t['price']:>7.2f} {t['total_vol']:>6,} {t['side']:<6} {t['bid_vol']:>7,} {t['ask_vol']:>7,}")

    print()

    # Summary of large trades
    buy_large = sum(t["ask_vol"] for t in large)
    sell_large = sum(t["bid_vol"] for t in large)
    print(f"Large trade summary: BUY={buy_large:,}  SELL={sell_large:,}  NET={buy_large - sell_large:+,}")
    print()


def analysis_5min_clusters(ticks):
    """5-minute window vol seller/buyer cluster detection."""
    print("=" * 90)
    print("3. VOL SELLER/BUYER CLUSTERS (5-min windows, |net delta| > 50)")
    print("=" * 90)

    if not ticks:
        print("No ticks.")
        print()
        return

    # Build 5-min windows
    windows = defaultdict(lambda: {
        "buy_vol": 0, "sell_vol": 0,
        "first_price": None, "last_price": None,
        "high": -999, "low": 999,
    })

    for t in ticks:
        dt = t["dt_et"]
        win_min = (dt.minute // 5) * 5
        key = dt.replace(minute=win_min, second=0, microsecond=0)
        w = windows[key]
        w["buy_vol"] += t["ask_vol"]
        w["sell_vol"] += t["bid_vol"]
        if w["first_price"] is None:
            w["first_price"] = t["price"]
        w["last_price"] = t["price"]
        w["high"] = max(w["high"], t["price"])
        w["low"] = min(w["low"], t["price"])

    sorted_keys = sorted(windows.keys())

    seller_clusters = []
    buyer_clusters = []

    for key in sorted_keys:
        w = windows[key]
        net = w["buy_vol"] - w["sell_vol"]
        if net < -50:
            seller_clusters.append((key, w, net))
        elif net > 50:
            buyer_clusters.append((key, w, net))

    print(f"\nVOL SELLER clusters (net delta < -50 = selling VIX futs = bullish SPX):")
    print(f"{'Time ET':<10} {'NetDelta':>9} {'BuyVol':>8} {'SellVol':>8} {'PriceMove':>10} {'High':>7} {'Low':>7}")
    print("-" * 70)
    if seller_clusters:
        for key, w, net in seller_clusters:
            move = w["last_price"] - w["first_price"]
            print(f"{key.strftime('%H:%M'):<10} {net:>+9,} {w['buy_vol']:>8,} {w['sell_vol']:>8,} {move:>+10.2f} {w['high']:>7.2f} {w['low']:>7.2f}")
    else:
        print("None found.")

    print(f"\nVOL BUYER clusters (net delta > +50 = buying VIX futs = bearish SPX):")
    print(f"{'Time ET':<10} {'NetDelta':>9} {'BuyVol':>8} {'SellVol':>8} {'PriceMove':>10} {'High':>7} {'Low':>7}")
    print("-" * 70)
    if buyer_clusters:
        for key, w, net in buyer_clusters:
            move = w["last_price"] - w["first_price"]
            print(f"{key.strftime('%H:%M'):<10} {net:>+9,} {w['buy_vol']:>8,} {w['sell_vol']:>8,} {move:>+10.2f} {w['high']:>7.2f} {w['low']:>7.2f}")
    else:
        print("None found.")

    print()


def analysis_cvd(ticks):
    """Cumulative Volume Delta with direction change detection."""
    print("=" * 90)
    print("4. CVD (Cumulative Volume Delta) - Direction Changes")
    print("=" * 90)

    if not ticks:
        print("No ticks.")
        print()
        return

    # Build 1-minute CVD
    minutes = defaultdict(lambda: {"buy": 0, "sell": 0, "last_price": None})
    for t in ticks:
        dt = t["dt_et"]
        key = dt.replace(second=0, microsecond=0)
        m = minutes[key]
        m["buy"] += t["ask_vol"]
        m["sell"] += t["bid_vol"]
        m["last_price"] = t["price"]

    sorted_mins = sorted(minutes.keys())

    cvd = 0
    cvd_series = []
    for key in sorted_mins:
        m = minutes[key]
        delta = m["buy"] - m["sell"]
        cvd += delta
        cvd_series.append((key, cvd, m["last_price"], delta))

    # Detect direction changes (CVD reversal > 30 contracts)
    print("\nCVD direction changes (reversal magnitude > 30 contracts):")
    print(f"{'Time ET':<10} {'CVD':>8} {'Price':>7} {'Direction':<12} {'Reversal':>9}")
    print("-" * 55)

    prev_cvd = 0
    prev_direction = None
    local_extreme = 0
    change_count = 0

    for key, cvd_val, price, delta in cvd_series:
        if cvd_val > prev_cvd:
            direction = "RISING"
        elif cvd_val < prev_cvd:
            direction = "FALLING"
        else:
            direction = prev_direction if prev_direction else "FLAT"

        if prev_direction and direction != prev_direction and direction != "FLAT":
            reversal = abs(cvd_val - local_extreme)
            if reversal > 30:
                label = "-> RISING" if direction == "RISING" else "-> FALLING"
                print(f"{key.strftime('%H:%M'):<10} {cvd_val:>+8,} {price:>7.2f} {label:<12} {reversal:>9,}")
                change_count += 1
            local_extreme = cvd_val
        else:
            if direction == "RISING":
                local_extreme = max(local_extreme, cvd_val)
            elif direction == "FALLING":
                local_extreme = min(local_extreme, cvd_val)

        prev_cvd = cvd_val
        prev_direction = direction

    if change_count == 0:
        print("No significant CVD direction changes detected.")

    # Print CVD at key times
    print(f"\nCVD at key intervals:")
    print(f"{'Time ET':<10} {'CVD':>8} {'Price':>7}")
    print("-" * 30)

    key_times = set()
    for key, cvd_val, price, _ in cvd_series:
        if key.minute == 0 or key.minute == 30:
            key_times.add((key, cvd_val, price))

    # Also add first and last
    if cvd_series:
        key_times.add(cvd_series[0][:3])
        key_times.add(cvd_series[-1][:3])

    for key, cvd_val, price in sorted(key_times):
        print(f"{key.strftime('%H:%M'):<10} {cvd_val:>+8,} {price:>7.2f}")

    # CVD extremes
    if cvd_series:
        max_cvd = max(cvd_series, key=lambda x: x[1])
        min_cvd = min(cvd_series, key=lambda x: x[1])
        print(f"\nCVD High: {max_cvd[1]:+,} at {max_cvd[0].strftime('%H:%M')} (price {max_cvd[2]:.2f})")
        print(f"CVD Low:  {min_cvd[1]:+,} at {min_cvd[0].strftime('%H:%M')} (price {min_cvd[2]:.2f})")
        print(f"CVD Final: {cvd_series[-1][1]:+,} at {cvd_series[-1][0].strftime('%H:%M')} (price {cvd_series[-1][2]:.2f})")

    print()


def analysis_summary(ticks):
    """Overall summary."""
    print("=" * 90)
    print("5. SUMMARY")
    print("=" * 90)

    if not ticks:
        print("No ticks to summarize.")
        return

    total_buy = sum(t["ask_vol"] for t in ticks)
    total_sell = sum(t["bid_vol"] for t in ticks)
    total_vol = sum(t["total_vol"] for t in ticks)
    net_delta = total_buy - total_sell

    prices = [t["price"] for t in ticks]
    open_price = prices[0]
    close_price = prices[-1]
    high_price = max(prices)
    low_price = min(prices)

    print(f"VXM26 {TODAY_STR} Market Hours (09:30-16:00 ET)")
    print(f"  Open:  {open_price:.2f}")
    print(f"  High:  {high_price:.2f}")
    print(f"  Low:   {low_price:.2f}")
    print(f"  Close: {close_price:.2f}")
    print(f"  Range: {high_price - low_price:.2f}")
    print(f"  Change: {close_price - open_price:+.2f}")
    print()

    print(f"Volume:")
    print(f"  Total:     {total_vol:>10,}")
    print(f"  Buy (ask): {total_buy:>10,}  ({100*total_buy/total_vol:.1f}%)" if total_vol else "")
    print(f"  Sell (bid):{total_sell:>10,}  ({100*total_sell/total_vol:.1f}%)" if total_vol else "")
    print(f"  Net Delta: {net_delta:>+10,}")
    print()

    buy_sell_ratio = total_buy / total_sell if total_sell > 0 else float("inf")
    print(f"  Buy/Sell Ratio: {buy_sell_ratio:.2f}")
    if net_delta > 0:
        print(f"  --> Net VOL BUYERS (bearish SPX signal)")
    elif net_delta < 0:
        print(f"  --> Net VOL SELLERS (bullish SPX signal)")
    else:
        print(f"  --> Neutral")
    print()

    # Largest single trades
    by_vol = sorted(ticks, key=lambda t: t["total_vol"], reverse=True)[:10]
    print("Top 10 largest single trades:")
    print(f"{'Time ET':<12} {'Price':>7} {'Size':>6} {'Side':<6}")
    print("-" * 35)
    for t in by_vol:
        print(f"{t['dt_et'].strftime('%H:%M:%S'):<12} {t['price']:>7.2f} {t['total_vol']:>6,} {t['side']:<6}")

    print()

    # Key reversal moments: largest 1-min delta swings
    minutes = defaultdict(lambda: {"buy": 0, "sell": 0, "price": None})
    for t in ticks:
        key = t["dt_et"].replace(second=0, microsecond=0)
        m = minutes[key]
        m["buy"] += t["ask_vol"]
        m["sell"] += t["bid_vol"]
        m["price"] = t["price"]

    min_list = [(k, v["buy"] - v["sell"], v["price"]) for k, v in minutes.items()]
    min_list.sort(key=lambda x: abs(x[1]), reverse=True)

    print("Top 10 highest-delta minutes (potential reversal moments):")
    print(f"{'Time ET':<10} {'Delta':>8} {'Price':>7} {'Interpretation'}")
    print("-" * 60)
    for key, delta, price in min_list[:10]:
        if delta > 0:
            interp = "Vol buyers (bearish SPX)"
        elif delta < 0:
            interp = "Vol sellers (bullish SPX)"
        else:
            interp = "Neutral"
        print(f"{key.strftime('%H:%M'):<10} {delta:>+8,} {price:>7.2f} {interp}")

    print()


def main():
    print()
    print("VXM26 (VIX June Futures) Tick Analysis")
    print(f"Date: {TODAY_STR}")
    print()

    ticks = parse_scid(SCID_FILE)

    if not ticks:
        print("No ticks found for today's market hours. Exiting.")
        return

    analysis_30min_buckets(ticks)
    analysis_large_trades(ticks)
    analysis_5min_clusters(ticks)
    analysis_cvd(ticks)
    analysis_summary(ticks)


if __name__ == "__main__":
    main()
