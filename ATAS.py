"""
ES Futures - 20 Point Range Candle Builder
==========================================
Reads raw tick data (exported from ATAS or similar) and constructs:
- 20-point range candles (Open, High, Low, Close)
- Delta per bar (buy volume - sell volume)
- Session CVD (cumulative delta, resets each session)
- Volume per bar

ATAS Tick Export Format (expected):
  DateTime, Price, Volume, Side
  - Side: "Buy" or "Sell" (aggressor side)

If your export uses different column names or formats, adjust the
COLUMN_MAPPING section below.

Usage:
  python es_range_candles.py --input ticks.csv --output range_candles.csv
  python es_range_candles.py --input ticks.csv --output range_candles.csv --range 20 --session-start 18:00
"""

import pandas as pd
import sys
from datetime import time, datetime


# ============================================================
# CONFIGURATION - Adjust these to match your tick data export
# ============================================================

COLUMN_MAPPING = {
    # Map your CSV column names to the required fields
    # Adjust the right side to match your actual column headers
    "datetime": "DateTime",       # or "Date", "Time", "Timestamp"
    "price": "Price",             # or "Last", "TradePrice"
    "volume": "Volume",           # or "Size", "Qty"
    "side": "Side",               # or "AggressorSide", "Type", "Direction"
}

# How "Buy" and "Sell" are labeled in your data
BUY_LABELS = ["Buy", "buy", "BUY", "B", "Ask", "ask", "ASK", "1"]
SELL_LABELS = ["Sell", "sell", "SELL", "S", "Bid", "bid", "BID", "-1", "0"]

# ES tick size
TICK_SIZE = 0.25

# Range candle size in points (20 points = 80 ticks)
DEFAULT_RANGE_POINTS = 20

# Session start time (CME ES session starts 6:00 PM ET previous day)
# Adjust based on your timezone. This is used to reset CVD each session.
DEFAULT_SESSION_START = "18:00"  # 6:00 PM ET


# ============================================================
# RANGE CANDLE BUILDER
# ============================================================

class RangeCandleBuilder:
    def __init__(self, range_points=20, session_start_time="18:00"):
        self.range_size = range_points
        self.session_start = time(*map(int, session_start_time.split(":")))
        self.candles = []
        self.reset_candle()
        self.cumulative_delta = 0.0
        self.current_session_date = None

    def reset_candle(self):
        self.candle_open = None
        self.candle_high = None
        self.candle_low = None
        self.candle_close = None
        self.candle_volume = 0
        self.candle_buy_volume = 0
        self.candle_sell_volume = 0
        self.candle_start_time = None
        self.candle_end_time = None

    def get_session_date(self, dt):
        """Determine session date based on session start time."""
        t = dt.time() if isinstance(dt, datetime) else dt
        if t >= self.session_start:
            return dt.date()
        else:
            return (dt - pd.Timedelta(days=1)).date()

    def check_session_reset(self, dt):
        """Reset CVD if we've entered a new session."""
        session_date = self.get_session_date(dt)
        if self.current_session_date is None:
            self.current_session_date = session_date
        elif session_date != self.current_session_date:
            self.cumulative_delta = 0.0
            self.current_session_date = session_date

    def finalize_candle(self):
        """Save the current candle and reset."""
        if self.candle_open is None:
            return

        delta = self.candle_buy_volume - self.candle_sell_volume
        self.cumulative_delta += delta

        self.candles.append({
            "DateTime": self.candle_start_time,
            "CloseTime": self.candle_end_time,
            "Open": self.candle_open,
            "High": self.candle_high,
            "Low": self.candle_low,
            "Close": self.candle_close,
            "Volume": self.candle_volume,
            "Delta": delta,
            "SessionCVD": round(self.cumulative_delta, 2),
            "BuyVolume": self.candle_buy_volume,
            "SellVolume": self.candle_sell_volume,
        })
        self.reset_candle()

    def process_tick(self, dt, price, volume, is_buy):
        """Process a single tick and build range candles."""
        self.check_session_reset(dt)

        # Start a new candle if needed
        if self.candle_open is None:
            self.candle_open = price
            self.candle_high = price
            self.candle_low = price
            self.candle_close = price
            self.candle_start_time = dt
            self.candle_end_time = dt
            self.candle_volume = 0
            self.candle_buy_volume = 0
            self.candle_sell_volume = 0

        # Update candle
        self.candle_close = price
        self.candle_end_time = dt
        self.candle_volume += volume

        if is_buy:
            self.candle_buy_volume += volume
        else:
            self.candle_sell_volume += volume

        # Track high/low
        if price > self.candle_high:
            self.candle_high = price
        if price < self.candle_low:
            self.candle_low = price

        # Check if range is exceeded
        current_range = self.candle_high - self.candle_low
        if current_range >= self.range_size:
            self.finalize_candle()

    def get_dataframe(self):
        """Return all completed candles as a DataFrame."""
        # Finalize any remaining open candle
        if self.candle_open is not None:
            self.finalize_candle()
        return pd.DataFrame(self.candles)


# ============================================================
# DATA LOADING & PARSING
# ============================================================

def detect_columns(df):
    """Try to auto-detect column names if they don't match the mapping."""
    col_lower = {c.lower().strip(): c for c in df.columns}

    mapping = {}

    # Detect datetime column
    for key in ["datetime", "date", "time", "timestamp", "date/time"]:
        if key in col_lower:
            mapping["datetime"] = col_lower[key]
            break

    # Detect price column
    for key in ["price", "last", "tradeprice", "trade price", "lastprice"]:
        if key in col_lower:
            mapping["price"] = col_lower[key]
            break

    # Detect volume column
    for key in ["volume", "vol", "size", "qty", "quantity"]:
        if key in col_lower:
            mapping["volume"] = col_lower[key]
            break

    # Detect side/direction column
    for key in ["side", "aggressorside", "aggressor", "type", "direction", "buysell", "buy/sell"]:
        if key in col_lower:
            mapping["side"] = col_lower[key]
            break

    return mapping


def load_tick_data(filepath):
    """Load and parse tick data from CSV."""
    print(f"Loading tick data from: {filepath}")

    # Try different separators
    for sep in [",", ";", "\t", "|"]:
        try:
            df = pd.read_csv(filepath, sep=sep, engine="python", nrows=5)
            if len(df.columns) >= 3:
                df = pd.read_csv(filepath, sep=sep, engine="python")
                break
        except Exception:
            continue
    else:
        raise ValueError("Could not parse the CSV file. Check format/delimiter.")

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    print(f"Detected columns: {list(df.columns)}")
    print(f"Total rows: {len(df):,}")
    print(f"First row: {df.iloc[0].to_dict()}")

    # Auto-detect or use configured mapping
    mapping = detect_columns(df)
    print(f"Column mapping: {mapping}")

    if len(mapping) < 4:
        print("\n⚠️  Could not auto-detect all columns.")
        print("Please adjust COLUMN_MAPPING in the script to match your CSV headers.")
        print(f"Your columns are: {list(df.columns)}")

        # Try to proceed with what we have
        if len(mapping) < 3:
            sys.exit(1)

    # Rename columns
    rename_map = {v: k for k, v in mapping.items()}
    df = df.rename(columns=rename_map)

    # Parse datetime
    df["datetime"] = pd.to_datetime(df["datetime"], format="mixed", dayfirst=False)

    # Parse price
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Parse volume
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(1).astype(int)

    # Parse side
    if "side" in df.columns:
        df["is_buy"] = df["side"].astype(str).str.strip().isin(BUY_LABELS)
    else:
        print("⚠️  No 'side' column found — estimating using tick direction (less accurate)")
        df["is_buy"] = df["price"].diff() >= 0

    # Drop invalid rows
    before = len(df)
    df = df.dropna(subset=["datetime", "price"])
    after = len(df)
    if before != after:
        print(f"Dropped {before - after} invalid rows")

    # Sort by time
    df = df.sort_values("datetime").reset_index(drop=True)

    print(f"Data range: {df['datetime'].iloc[0]} → {df['datetime'].iloc[-1]}")
    print(f"Price range: {df['price'].min()} → {df['price'].max()}")
    print(f"Buy ticks: {df['is_buy'].sum():,} | Sell ticks: {(~df['is_buy']).sum():,}")

    return df


# ============================================================
# MAIN
# ============================================================

def main():
    # ============================================================
    # SET YOUR FILE PATHS HERE
    # ============================================================
    INPUT_FILE = r"C:\Users\Faisa\OneDrive\Desktop\Chart.csv"
    OUTPUT_FILE = r"C:\Users\Faisa\OneDrive\Desktop\es_range_candles.csv"
    RANGE_POINTS = 20       # Range candle size in points
    SESSION_START = "18:00" # Session start time for CVD reset
    # ============================================================

    # Load data
    ticks = load_tick_data(INPUT_FILE)

    # Build candles
    print(f"\nBuilding {RANGE_POINTS}-point range candles...")
    builder = RangeCandleBuilder(
        range_points=RANGE_POINTS,
        session_start_time=SESSION_START
    )

    for _, row in ticks.iterrows():
        builder.process_tick(
            dt=row["datetime"],
            price=row["price"],
            volume=row["volume"],
            is_buy=row["is_buy"]
        )

    candles = builder.get_dataframe()

    if candles.empty:
        print("No candles were generated. Check your data and range size.")
        sys.exit(1)

    # Summary
    print(f"\n{'='*50}")
    print(f"RESULTS")
    print(f"{'='*50}")
    print(f"Total candles: {len(candles):,}")
    print(f"Date range: {candles['DateTime'].iloc[0]} → {candles['DateTime'].iloc[-1]}")
    print(f"Avg volume/bar: {candles['Volume'].mean():,.0f}")
    print(f"Avg delta/bar: {candles['Delta'].mean():,.1f}")
    print(f"Max delta: {candles['Delta'].max():,.0f}")
    print(f"Min delta: {candles['Delta'].min():,.0f}")

    # Save
    candles.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Saved to: {OUTPUT_FILE}")
    print(f"\nColumns: {list(candles.columns)}")
    print(f"\nFirst 5 candles:")
    print(candles.head().to_string(index=False))


if __name__ == "__main__":
    main()