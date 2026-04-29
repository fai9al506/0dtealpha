"""
Export all Rithmic ES range bars for March 2026 with volume intensity metrics.
Base dataset for future backtests.
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

# ── 1. Query all rithmic bars for March 2026 ──────────────────────────────

query = text("""
SELECT bar_idx,
       bar_open AS open, bar_high AS high, bar_low AS low, bar_close AS close,
       bar_volume AS volume, bar_delta AS delta,
       bar_buy_volume AS buy_volume, bar_sell_volume AS sell_volume,
       cumulative_delta AS cvd, cvd_open, cvd_high, cvd_low, cvd_close,
       ts_start, ts_end, source
FROM es_range_bars
WHERE source = 'rithmic'
  AND ts_start >= '2026-03-01'
  AND ts_start < '2026-04-01'
ORDER BY ts_start, bar_idx
""")

with engine.connect() as conn:
    df = pd.read_sql(query, conn)

print(f"Raw rows from DB: {len(df)}")

if df.empty:
    print("No data found!")
    exit(1)

# ── 2. Per-bar computed columns ───────────────────────────────────────────

# Ensure timestamps are timezone-aware UTC, then convert to ET
df["ts_start"] = pd.to_datetime(df["ts_start"], utc=True)
df["ts_end"] = pd.to_datetime(df["ts_end"], utc=True)

# Save UTC strings for CSV
df["ts_start_utc"] = df["ts_start"].dt.strftime("%Y-%m-%d %H:%M:%S")
df["ts_end_utc"] = df["ts_end"].dt.strftime("%Y-%m-%d %H:%M:%S")

# ET conversions
ts_start_et = df["ts_start"].dt.tz_convert(ET)
df["date"] = ts_start_et.dt.date.astype(str)
df["time_et"] = ts_start_et.dt.strftime("%H:%M:%S")

# Duration
df["duration_sec"] = (df["ts_end"] - df["ts_start"]).dt.total_seconds()
df["duration_sec"] = df["duration_sec"].clip(lower=1)  # floor at 1s

# Intensity metrics
df["vol_per_sec"] = df["volume"] / df["duration_sec"]
df["delta_per_sec"] = df["delta"].abs() / df["duration_sec"]
df["buy_per_sec"] = df["buy_volume"] / df["duration_sec"]
df["sell_per_sec"] = df["sell_volume"] / df["duration_sec"]

# Bar characteristics
df["bar_color"] = np.where(df["close"] >= df["open"], "GREEN", "RED")
df["bar_range"] = df["high"] - df["low"]
df["body_size"] = (df["close"] - df["open"]).abs()
df["is_doji"] = np.where(df["body_size"] <= 1.0, 1, 0)

# ── 3. Per-day rolling metrics (reset each day) ──────────────────────────

rolling_cols = [
    "avg_20_volume", "avg_20_vol_rate", "vol_ratio_raw", "vol_ratio_rate",
    "avg_20_delta_rate", "delta_rate_ratio"
]
for c in rolling_cols:
    df[c] = np.nan

for date_val, group in df.groupby("date"):
    idxs = group.index
    volumes = group["volume"].values
    vol_rates = group["vol_per_sec"].values
    delta_rates = group["delta_per_sec"].values

    for i_pos, i_idx in enumerate(idxs):
        if i_pos < 20:
            # Not enough bars for 20-bar SMA
            continue

        # 20-bar lookback (preceding 20 bars, NOT including current)
        start = i_pos - 20
        end = i_pos

        avg_vol = volumes[start:end].mean()
        avg_vol_rate = vol_rates[start:end].mean()
        avg_delta_rate = delta_rates[start:end].mean()

        df.at[i_idx, "avg_20_volume"] = avg_vol
        df.at[i_idx, "avg_20_vol_rate"] = avg_vol_rate
        df.at[i_idx, "vol_ratio_raw"] = volumes[i_pos] / avg_vol if avg_vol > 0 else np.nan
        df.at[i_idx, "vol_ratio_rate"] = vol_rates[i_pos] / avg_vol_rate if avg_vol_rate > 0 else np.nan
        df.at[i_idx, "avg_20_delta_rate"] = avg_delta_rate
        df.at[i_idx, "delta_rate_ratio"] = delta_rates[i_pos] / avg_delta_rate if avg_delta_rate > 0 else np.nan

# ── 4. CVD divergence metrics (replicate setup_detector.py) ──────────────

div_cols = [
    "cvd_norm", "price_high_norm", "price_low_norm",
    "divergence_direction", "div_gap", "div_raw"
]
for c in div_cols:
    df[c] = np.nan if c != "divergence_direction" else "none"

LOOKBACK = 8

for date_val, group in df.groupby("date"):
    idxs = group.index.tolist()
    highs = group["high"].values
    lows = group["low"].values
    cvd_closes = group["cvd_close"].values

    for i_pos in range(len(idxs)):
        if i_pos < LOOKBACK:
            continue

        i_idx = idxs[i_pos]

        # Window: from (i_pos - LOOKBACK) to i_pos inclusive
        w_start = i_pos - LOOKBACK
        w_highs = highs[w_start:i_pos + 1]
        w_lows = lows[w_start:i_pos + 1]
        w_cvds = cvd_closes[w_start:i_pos + 1]

        cvd_start_val = w_cvds[0]
        cvd_end_val = w_cvds[-1]
        cvd_slope = cvd_end_val - cvd_start_val
        cvd_range = w_cvds.max() - w_cvds.min()

        if cvd_range == 0:
            continue

        price_low_start_val = w_lows[0]
        price_low_end_val = w_lows[-1]
        price_high_start_val = w_highs[0]
        price_high_end_val = w_highs[-1]
        price_range = w_highs.max() - w_lows.min()

        if price_range == 0:
            continue

        cvd_n = cvd_slope / cvd_range
        price_low_n = (price_low_end_val - price_low_start_val) / price_range
        price_high_n = (price_high_end_val - price_high_start_val) / price_range

        df.at[i_idx, "cvd_norm"] = round(cvd_n, 4)
        df.at[i_idx, "price_high_norm"] = round(price_high_n, 4)
        df.at[i_idx, "price_low_norm"] = round(price_low_n, 4)

        # Detect direction (same logic as setup_detector.py)
        direction = None
        gap_val = 0.0
        raw_score = 0

        if cvd_n < -0.15:
            gap_val = price_low_n - cvd_n
            if gap_val > 0.2:
                direction = "bullish"
                if gap_val > 1.2:
                    raw_score = 4
                elif gap_val > 0.8:
                    raw_score = 3
                elif gap_val > 0.4:
                    raw_score = 2
                else:
                    raw_score = 1

        if cvd_n > 0.15 and direction is None:
            gap_val = cvd_n - price_high_n
            if gap_val > 0.2:
                direction = "bearish"
                if gap_val > 1.2:
                    raw_score = 4
                elif gap_val > 0.8:
                    raw_score = 3
                elif gap_val > 0.4:
                    raw_score = 2
                else:
                    raw_score = 1

        if direction is not None:
            df.at[i_idx, "divergence_direction"] = direction
            df.at[i_idx, "div_gap"] = round(gap_val, 4)
            df.at[i_idx, "div_raw"] = raw_score
        else:
            df.at[i_idx, "divergence_direction"] = "none"
            df.at[i_idx, "div_gap"] = round(gap_val, 4)
            df.at[i_idx, "div_raw"] = 0

# ── 5. Select and order columns, save to CSV ─────────────────────────────

output_cols = [
    "date", "time_et", "bar_idx", "open", "high", "low", "close",
    "bar_color", "bar_range", "body_size", "is_doji",
    "volume", "delta", "buy_volume", "sell_volume", "duration_sec",
    "vol_per_sec", "delta_per_sec", "buy_per_sec", "sell_per_sec",
    "avg_20_volume", "avg_20_vol_rate", "vol_ratio_raw", "vol_ratio_rate",
    "avg_20_delta_rate", "delta_rate_ratio",
    "cvd", "cvd_open", "cvd_high", "cvd_low", "cvd_close",
    "cvd_norm", "price_high_norm", "price_low_norm",
    "divergence_direction", "div_gap", "div_raw",
    "ts_start_utc", "ts_end_utc",
]

out = df[output_cols].copy()

# Round float columns for readability
float_cols = [
    "vol_per_sec", "delta_per_sec", "buy_per_sec", "sell_per_sec",
    "avg_20_volume", "avg_20_vol_rate", "vol_ratio_raw", "vol_ratio_rate",
    "avg_20_delta_rate", "delta_rate_ratio",
    "bar_range", "body_size",
]
for c in float_cols:
    out[c] = out[c].round(4)

outpath = "G:/My Drive/Python/MyProject/GitHub/0dtealpha/exports/es_range_bars_march_volrate.csv"
out.to_csv(outpath, index=False)
print(f"\nSaved to: {outpath}")

# ── 6. Summary stats ─────────────────────────────────────────────────────

dates = out["date"].unique()
print(f"\nTotal rows: {len(out)}")
print(f"Date range: {dates[0]} to {dates[-1]}")
print(f"Trading days: {len(dates)}")
print(f"Avg bars/day: {len(out) / len(dates):.1f}")

# Sample 5 rows with vol_rate columns
print("\n-- Sample rows (vol_rate columns) --")
sample_cols = ["date", "time_et", "bar_idx", "volume", "duration_sec",
               "vol_per_sec", "vol_ratio_raw", "vol_ratio_rate"]
print(out[sample_cols].dropna(subset=["vol_ratio_raw"]).head(5).to_string(index=False))

# Distribution comparison: how many bars pass 1.4x threshold
valid = out.dropna(subset=["vol_ratio_raw", "vol_ratio_rate"])
pass_raw = (valid["vol_ratio_raw"] >= 1.4).sum()
pass_rate = (valid["vol_ratio_rate"] >= 1.4).sum()
both = ((valid["vol_ratio_raw"] >= 1.4) & (valid["vol_ratio_rate"] >= 1.4)).sum()
raw_only = ((valid["vol_ratio_raw"] >= 1.4) & (valid["vol_ratio_rate"] < 1.4)).sum()
rate_only = ((valid["vol_ratio_raw"] < 1.4) & (valid["vol_ratio_rate"] >= 1.4)).sum()

print(f"\n-- Volume gate comparison (1.4x threshold) --")
print(f"Bars with rolling data: {len(valid)}")
print(f"Pass vol_ratio_raw >= 1.4:  {pass_raw} ({pass_raw/len(valid)*100:.1f}%)")
print(f"Pass vol_ratio_rate >= 1.4: {pass_rate} ({pass_rate/len(valid)*100:.1f}%)")
print(f"Pass BOTH:                  {both}")
print(f"Pass raw ONLY (not rate):   {raw_only}")
print(f"Pass rate ONLY (not raw):   {rate_only}")

# 1.5x threshold (current live gate)
pass_raw_15 = (valid["vol_ratio_raw"] >= 1.5).sum()
pass_rate_15 = (valid["vol_ratio_rate"] >= 1.5).sum()
print(f"\n-- At 1.5x (current live gate) --")
print(f"Pass vol_ratio_raw >= 1.5:  {pass_raw_15} ({pass_raw_15/len(valid)*100:.1f}%)")
print(f"Pass vol_ratio_rate >= 1.5: {pass_rate_15} ({pass_rate_15/len(valid)*100:.1f}%)")

# Duration stats
print(f"\n-- Duration stats --")
print(f"Mean duration:   {out['duration_sec'].mean():.1f}s")
print(f"Median duration: {out['duration_sec'].median():.1f}s")
print(f"Min duration:    {out['duration_sec'].min():.1f}s")
print(f"Max duration:    {out['duration_sec'].max():.1f}s")
print(f"Bars < 5s:       {(out['duration_sec'] < 5).sum()}")
print(f"Bars > 120s:     {(out['duration_sec'] > 120).sum()}")

# Specific check: bars around 10:45 ET on March 27
print("\n-- March 27 around 10:45 ET (verification) --")
mar27 = out[(out["date"] == "2026-03-27") & (out["time_et"] >= "10:40:00") & (out["time_et"] <= "10:50:00")]
check_cols = ["time_et", "bar_idx", "volume", "duration_sec", "vol_per_sec",
              "vol_ratio_raw", "vol_ratio_rate", "divergence_direction", "div_raw"]
if len(mar27) > 0:
    print(mar27[check_cols].to_string(index=False))
else:
    print("No bars found in that window. Showing nearest bars around 10:45...")
    mar27_all = out[out["date"] == "2026-03-27"]
    if len(mar27_all) > 0:
        # Find bars closest to 10:45
        mar27_all = mar27_all.copy()
        mar27_all["_tdiff"] = mar27_all["time_et"].apply(
            lambda t: abs(int(t.split(":")[0])*3600 + int(t.split(":")[1])*60 - (10*3600 + 45*60))
        )
        nearest = mar27_all.nsmallest(10, "_tdiff")
        print(nearest[check_cols].to_string(index=False))
    else:
        print("No data for March 27")

print("\nDone.")
