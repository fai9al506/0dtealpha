"""
Download historical daily VIX and VIX3M data from Yahoo Finance.
Compute Overvix (VIX - VIX3M) and output a clean table + CSV.
"""

import yfinance as yf
import pandas as pd
import sys

CSV_PATH = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\tmp_vix_history.csv"

START = "2026-02-01"
END   = "2026-03-18"  # yfinance end date is exclusive, so use day after 03-17

print(f"Downloading ^VIX data ({START} to {END})...")
try:
    vix = yf.download("^VIX", start=START, end=END, progress=False)
except Exception as e:
    print(f"ERROR downloading ^VIX: {e}")
    sys.exit(1)

print(f"Downloading ^VIX3M data ({START} to {END})...")
try:
    vix3m = yf.download("^VIX3M", start=START, end=END, progress=False)
except Exception as e:
    print(f"ERROR downloading ^VIX3M: {e}")
    sys.exit(1)

# Debug: show what we got
print(f"\n^VIX rows: {len(vix)}, columns: {list(vix.columns)}")
print(f"^VIX3M rows: {len(vix3m)}, columns: {list(vix3m.columns)}")

if vix.empty:
    print("ERROR: ^VIX returned no data. Yahoo Finance may not have data for this date range.")
    sys.exit(1)
if vix3m.empty:
    print("ERROR: ^VIX3M returned no data. Yahoo Finance may not have data for this date range.")
    sys.exit(1)

# Handle multi-level columns (yfinance sometimes returns MultiIndex)
if isinstance(vix.columns, pd.MultiIndex):
    vix_close = vix[("Close", "^VIX")].copy()
else:
    vix_close = vix["Close"].copy()

if isinstance(vix3m.columns, pd.MultiIndex):
    vix3m_close = vix3m[("Close", "^VIX3M")].copy()
else:
    vix3m_close = vix3m["Close"].copy()

# Build combined DataFrame
df = pd.DataFrame({
    "VIX_Close": vix_close,
    "VIX3M_Close": vix3m_close,
})

# Drop rows where either is NaN (non-trading days, mismatched dates)
df = df.dropna()

# Compute Overvix
df["Overvix"] = df["VIX_Close"] - df["VIX3M_Close"]

# Format index
df.index.name = "Date"
df.index = df.index.strftime("%Y-%m-%d")

# Round for display
df["VIX_Close"] = df["VIX_Close"].round(2)
df["VIX3M_Close"] = df["VIX3M_Close"].round(2)
df["Overvix"] = df["Overvix"].round(2)

# Print clean table
print(f"\n{'='*58}")
print(f"  VIX vs VIX3M History: {START} to 2026-03-17")
print(f"{'='*58}")
print(f"{'Date':<12} {'VIX Close':>10} {'VIX3M Close':>12} {'Overvix':>10}")
print(f"{'-'*12} {'-'*10} {'-'*12} {'-'*10}")

for date, row in df.iterrows():
    overvix_str = f"{row['Overvix']:+.2f}"
    print(f"{date:<12} {row['VIX_Close']:>10.2f} {row['VIX3M_Close']:>12.2f} {overvix_str:>10}")

print(f"{'-'*12} {'-'*10} {'-'*12} {'-'*10}")
print(f"{'Total days:':<12} {len(df)}")
print(f"{'Avg VIX:':<12} {df['VIX_Close'].mean():>10.2f}")
print(f"{'Avg VIX3M:':<12} {'':>10} {df['VIX3M_Close'].mean():>12.2f}")
print(f"{'Avg Overvix:':<12} {'':>10} {'':>12} {df['Overvix'].mean():>+10.2f}")
print(f"{'Max Overvix:':<12} {'':>10} {'':>12} {df['Overvix'].max():>+10.2f}")
print(f"{'Min Overvix:':<12} {'':>10} {'':>12} {df['Overvix'].min():>+10.2f}")

# Days where Overvix >= +2 (mean reversion bullish signal)
high_overvix = df[df["Overvix"] >= 2.0]
print(f"\nDays with Overvix >= +2.0 (bullish mean reversion): {len(high_overvix)}")
for date, row in high_overvix.iterrows():
    print(f"  {date}  VIX={row['VIX_Close']:.2f}  VIX3M={row['VIX3M_Close']:.2f}  Overvix={row['Overvix']:+.2f}")

# Save CSV
df.to_csv(CSV_PATH)
print(f"\nCSV saved to: {CSV_PATH}")
print("Done.")
