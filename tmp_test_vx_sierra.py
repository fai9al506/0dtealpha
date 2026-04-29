"""Read VIX futures tick data from Sierra Chart .scid file."""
import struct
from datetime import datetime, timedelta

SCID_FILE = r"C:\SierraChart\Data\VXM26_FUT_CFE.scid"
HEADER_SIZE = 56
RECORD_SIZE = 40
SC_EPOCH = datetime(1899, 12, 30)
MICROS_PER_DAY = 86_400_000_000

HEADER_FMT = "<4sIIHHI36s"
RECORD_FMT = "<qffffIIII"


def sc_datetime_to_py(val):
    """Convert SCDateTimeMS (int64 microseconds since 1899-12-30) to Python datetime."""
    if val <= 0:
        return None
    days = val // MICROS_PER_DAY
    remainder = val % MICROS_PER_DAY
    return SC_EPOCH + timedelta(days=days, microseconds=remainder)


def read_scid(path, last_n=None):
    """Read .scid file and return records. If last_n, only return last N records."""
    with open(path, "rb") as f:
        header_data = f.read(HEADER_SIZE)
        magic, hdr_size, rec_size, version, _, _, _ = struct.unpack(HEADER_FMT, header_data)
        print(f"Magic: {magic}  Header: {hdr_size}  RecordSize: {rec_size}  Version: {version}")

        # Read all remaining data
        f.seek(0, 2)
        file_size = f.tell()
        num_records = (file_size - HEADER_SIZE) // RECORD_SIZE
        print(f"Total records: {num_records:,}")

        if last_n and last_n < num_records:
            start = num_records - last_n
            f.seek(HEADER_SIZE + start * RECORD_SIZE)
            data = f.read(last_n * RECORD_SIZE)
            count = last_n
        else:
            f.seek(HEADER_SIZE)
            data = f.read()
            count = num_records

        records = []
        for i in range(count):
            offset = i * RECORD_SIZE
            dt_raw, o, h, l, c, num_trades, vol, bid_vol, ask_vol = struct.unpack_from(
                RECORD_FMT, data, offset
            )
            dt = sc_datetime_to_py(dt_raw)
            records.append({
                "dt": dt,
                "open": o, "high": h, "low": l, "close": c,
                "num_trades": num_trades,
                "volume": vol,
                "bid_vol": bid_vol,
                "ask_vol": ask_vol,
            })
        return records


def main():
    print(f"Reading {SCID_FILE}...\n")
    records = read_scid(SCID_FILE, last_n=200)

    if not records:
        print("No records found!")
        return

    # Show first few
    print(f"\n--- First 10 of last 200 records ---")
    for r in records[:10]:
        dt = r["dt"].strftime("%Y-%m-%d %H:%M:%S") if r["dt"] else "?"
        is_tick = abs(r["open"]) < 0.001 or r["open"] < -1e30  # Open=0 or sub-trade marker
        if is_tick:
            # Tick record: High=Ask, Low=Bid, Close=Trade price
            side = "BUY" if r["ask_vol"] > 0 else "SELL" if r["bid_vol"] > 0 else "?"
            print(f"  {dt}  TRADE {r['close']:.2f}  x {r['volume']}  "
                  f"bid={r['low']:.2f} ask={r['high']:.2f}  side={side}")
        else:
            # Bar record
            print(f"  {dt}  O={r['open']:.2f} H={r['high']:.2f} L={r['low']:.2f} C={r['close']:.2f}  "
                  f"vol={r['volume']}  trades={r['num_trades']}  "
                  f"bid_vol={r['bid_vol']} ask_vol={r['ask_vol']}")

    # Show last 10
    print(f"\n--- Last 10 records ---")
    for r in records[-10:]:
        dt = r["dt"].strftime("%Y-%m-%d %H:%M:%S") if r["dt"] else "?"
        is_tick = abs(r["open"]) < 0.001 or r["open"] < -1e30
        if is_tick:
            side = "BUY" if r["ask_vol"] > 0 else "SELL" if r["bid_vol"] > 0 else "?"
            print(f"  {dt}  TRADE {r['close']:.2f}  x {r['volume']}  "
                  f"bid={r['low']:.2f} ask={r['high']:.2f}  side={side}")
        else:
            print(f"  {dt}  O={r['open']:.2f} H={r['high']:.2f} L={r['low']:.2f} C={r['close']:.2f}  "
                  f"vol={r['volume']}  trades={r['num_trades']}  "
                  f"bid_vol={r['bid_vol']} ask_vol={r['ask_vol']}")

    # Summary stats
    print(f"\n--- Summary ---")
    trades = [r for r in records if abs(r["open"]) < 0.001 or r["open"] < -1e30]
    bars = [r for r in records if not (abs(r["open"]) < 0.001 or r["open"] < -1e30)]
    print(f"Tick records: {len(trades)}")
    print(f"Bar records: {len(bars)}")

    if trades:
        buys = sum(1 for t in trades if t["ask_vol"] > 0)
        sells = sum(1 for t in trades if t["bid_vol"] > 0)
        total_buy_vol = sum(t["ask_vol"] for t in trades)
        total_sell_vol = sum(t["bid_vol"] for t in trades)
        prices = [t["close"] for t in trades if t["close"] > 0]
        print(f"Buy trades: {buys}  ({total_buy_vol} contracts)")
        print(f"Sell trades: {sells}  ({total_sell_vol} contracts)")
        if prices:
            print(f"Price range: {min(prices):.2f} - {max(prices):.2f}")
        first_dt = trades[0]["dt"]
        last_dt = trades[-1]["dt"]
        if first_dt and last_dt:
            print(f"Time range: {first_dt.strftime('%Y-%m-%d %H:%M:%S')} → {last_dt.strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
