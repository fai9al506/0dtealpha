"""
Analyze ES 5-pt range bars around 11:17-11:20 ET on March 27, 2026.
Temporary analysis script -- do NOT commit to production.
"""
import os
import sys
import tempfile
import pandas as pd
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

pd.set_option("display.max_columns", 30)
pd.set_option("display.width", 280)
pd.set_option("display.max_colwidth", 25)

ET = ZoneInfo("America/New_York")

# Get DATABASE_URL from temp file
db_url_path = os.path.join(tempfile.gettempdir(), "db_url.txt")
with open(db_url_path) as f:
    db_url = f.read().strip()
if not db_url:
    print("ERROR: db_url.txt is empty")
    sys.exit(1)
engine = create_engine(db_url)

# ─── Query: All rithmic bars for March 27, RTH only (9:30-16:00 ET) ───
print("Fetching rithmic RTH bars for 2026-03-27...")
query = text("""
    SELECT
        bar_idx,
        bar_open, bar_high, bar_low, bar_close,
        bar_volume, bar_delta, bar_buy_volume, bar_sell_volume,
        cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
        ts_start, ts_end,
        ts_start AT TIME ZONE 'America/New_York' as ts_start_et,
        ts_end AT TIME ZONE 'America/New_York' as ts_end_et
    FROM es_range_bars
    WHERE source = 'rithmic'
      AND ts_start::date = '2026-03-27'
      AND (ts_start AT TIME ZONE 'America/New_York')::time >= '09:30:00'
      AND (ts_start AT TIME ZONE 'America/New_York')::time < '16:05:00'
    ORDER BY bar_idx
""")

with engine.connect() as conn:
    df = pd.read_sql(query, conn)

# Rename for convenience
df.rename(columns={
    "bar_open": "open", "bar_high": "high", "bar_low": "low", "bar_close": "close",
    "bar_volume": "volume", "bar_delta": "delta",
    "bar_buy_volume": "buy_volume", "bar_sell_volume": "sell_volume",
    "cumulative_delta": "cvd"
}, inplace=True)

print(f"Total RTH bars: {len(df)}")
if len(df) == 0:
    print("No bars found! Exiting.")
    sys.exit(1)

# Show price range to confirm we have the right data
print(f"Price range: {df['low'].min():.2f} - {df['high'].max():.2f}")
print(f"Bar_idx range: {df['bar_idx'].min()} - {df['bar_idx'].max()}")
print(f"Time range: {df['ts_start_et'].iloc[0]} - {df['ts_end_et'].iloc[-1]}")

# ─── Compute derived columns ───
df["color"] = np.where(df["close"] >= df["open"], "GREEN", "RED")
df["body"] = abs(df["close"] - df["open"])
df["bar_range_pts"] = df["high"] - df["low"]

# Duration
ts_start_raw = pd.to_datetime(df["ts_start"], utc=True) if df["ts_start"].dtype == "object" else df["ts_start"]
ts_end_raw = pd.to_datetime(df["ts_end"], utc=True) if df["ts_end"].dtype == "object" else df["ts_end"]
df["duration_sec"] = (ts_end_raw - ts_start_raw).dt.total_seconds().astype(int)

df["vol_per_sec"] = np.where(
    df["duration_sec"] > 0,
    (df["volume"] / df["duration_sec"]).round(1),
    df["volume"]
).astype(float)

df["delta_per_sec"] = np.where(
    df["duration_sec"] > 0,
    (abs(df["delta"]) / df["duration_sec"]).round(1),
    abs(df["delta"])
).astype(float)

df["delta_pct"] = np.where(
    df["volume"] > 0,
    (df["delta"] / df["volume"] * 100).round(1),
    0
).astype(float)

df["cvd_bar_change"] = df["cvd_close"] - df["cvd_open"]

# 20-bar rolling averages
df["avg_20_vol"] = df["volume"].rolling(20, min_periods=1).mean().round(0)
df["avg_20_vol_rate"] = df["vol_per_sec"].astype(float).rolling(20, min_periods=1).mean().round(1)
df["vol_ratio_raw"] = np.where(df["avg_20_vol"] > 0, (df["volume"] / df["avg_20_vol"]).round(2), 0)
df["vol_ratio_rate"] = np.where(df["avg_20_vol_rate"] > 0, (df["vol_per_sec"].astype(float) / df["avg_20_vol_rate"]).round(2), 0)

# ─── Find signal bars closest to 11:17 and 11:20 ET ───
target_times = [
    datetime(2026, 3, 27, 11, 17),
    datetime(2026, 3, 27, 11, 20),
]

# Convert ts_start_et to comparable naive datetimes
df["ts_start_et_dt"] = pd.to_datetime(df["ts_start_et"]).dt.tz_localize(None)

signal_idxs = []
for t in target_times:
    diffs = abs(df["ts_start_et_dt"] - t)
    closest_pos = diffs.idxmin()
    closest_bar_idx = int(df.loc[closest_pos, "bar_idx"])
    signal_idxs.append(closest_bar_idx)
    print(f"Target {t.strftime('%H:%M')} ET -> closest bar_idx={closest_bar_idx} "
          f"(starts {df.loc[closest_pos, 'ts_start_et']}, ends {df.loc[closest_pos, 'ts_end_et']})")

# Use only the two target bars as signal bars (not the whole 11:15-11:25 window)
all_signal_bars = sorted(set(signal_idxs))
print(f"Signal bar_idxs: {all_signal_bars}")

# ─── Define display window: 10 before, 20 after ───
min_sig = min(all_signal_bars)
max_sig = max(all_signal_bars)
window_start = min_sig - 10
window_end = max_sig + 20

window_df = df[(df["bar_idx"] >= window_start) & (df["bar_idx"] <= window_end)].copy()

# Mark signal bars
window_df["marker"] = window_df["bar_idx"].apply(
    lambda x: ">>>" if x in all_signal_bars else "   "
)

# Format times for display
window_df["start_et"] = pd.to_datetime(window_df["ts_start_et"]).dt.strftime("%H:%M:%S")
window_df["end_et"] = pd.to_datetime(window_df["ts_end_et"]).dt.strftime("%H:%M:%S")

# ─── Print main table ───
print("\n" + "=" * 200)
print(f"ES 5-pt RANGE BARS -- March 27, 2026 -- Signal window 11:17-11:20 ET")
print(f"Showing bar_idx {window_start} to {window_end} ({len(window_df)} bars)")
print("=" * 200)

# Table 1: Price + Volume
print("\n--- PRICE & VOLUME ---")
cols1 = ["marker", "bar_idx", "start_et", "end_et",
         "open", "high", "low", "close", "color", "body", "bar_range_pts",
         "volume", "delta", "delta_pct", "duration_sec"]
t1 = window_df[cols1].copy()
for c in ["open", "high", "low", "close"]:
    t1[c] = t1[c].map("{:.2f}".format)
t1["body"] = t1["body"].map("{:.2f}".format)
t1["bar_range_pts"] = t1["bar_range_pts"].map("{:.2f}".format)
t1["delta_pct"] = t1["delta_pct"].map("{:.1f}%".format)
print(t1.to_string(index=False))

# Table 2: Volume analysis
print("\n--- VOLUME ANALYSIS ---")
cols2 = ["marker", "bar_idx", "start_et",
         "volume", "vol_per_sec", "avg_20_vol", "avg_20_vol_rate",
         "vol_ratio_raw", "vol_ratio_rate",
         "buy_volume", "sell_volume", "delta", "delta_pct"]
t2 = window_df[cols2].copy()
t2["vol_ratio_raw"] = t2["vol_ratio_raw"].map("{:.2f}x".format)
t2["vol_ratio_rate"] = t2["vol_ratio_rate"].map("{:.2f}x".format)
t2["delta_pct"] = t2["delta_pct"].map("{:.1f}%".format)
print(t2.to_string(index=False))

# Table 3: CVD analysis
print("\n--- CVD ANALYSIS ---")
cols3 = ["marker", "bar_idx", "start_et",
         "cvd_open", "cvd_high", "cvd_low", "cvd_close", "cvd_bar_change",
         "cvd", "delta", "color", "close"]
t3 = window_df[cols3].copy()
t3["close"] = t3["close"].map("{:.2f}".format)
print(t3.to_string(index=False))

# ─── AFTER analysis: what happened after signal bars ───
print("\n" + "=" * 200)
print("AFTER ANALYSIS: Price action following signal bars")
print("=" * 200)

for sig_idx in all_signal_bars:
    sig_row_df = df[df["bar_idx"] == sig_idx]
    if sig_row_df.empty:
        continue
    sig_row = sig_row_df.iloc[0]
    sig_close = float(sig_row["close"])
    sig_cvd = float(sig_row["cvd_close"])

    sig_time_start = pd.to_datetime(sig_row["ts_start_et"]).strftime("%H:%M:%S")
    sig_time_end = pd.to_datetime(sig_row["ts_end_et"]).strftime("%H:%M:%S")
    print(f"\n--- Signal bar_idx={sig_idx} | {sig_time_start}-{sig_time_end} ET | "
          f"O={sig_row['open']:.2f} H={sig_row['high']:.2f} L={sig_row['low']:.2f} C={sig_close:.2f} | "
          f"Color={sig_row['color']} | Vol={sig_row['volume']:.0f} | "
          f"Delta={sig_row['delta']:.0f} ({sig_row['delta_pct']:.1f}%) | CVD_close={sig_cvd:.0f} ---")

    # Get bars after signal
    after_df = df[df["bar_idx"] > sig_idx].head(25)
    if after_df.empty:
        print("  No bars after this signal.")
        continue

    # Track price evolution
    hdr = (f"  {'#':>3s} | {'Idx':>5s} | {'Time ET':>10s} | {'O':>8s} {'H':>8s} {'L':>8s} {'C':>8s} | "
           f"{'vs Sig':>7s} | {'MxUp':>7s} | {'MxDn':>7s} | {'Clr':>5s} | "
           f"{'Vol':>6s} | {'Delta':>6s} | {'D%':>6s} | {'CVDcl':>8s} | {'CVDchg':>8s}")
    print(f"\n{hdr}")
    print("  " + "-" * 130)

    max_up = 0.0
    max_dn = 0.0
    for i, (_, row) in enumerate(after_df.iterrows(), 1):
        price_vs_sig = float(row["close"]) - sig_close
        high_vs_sig = float(row["high"]) - sig_close
        low_vs_sig = float(row["low"]) - sig_close
        max_up = max(max_up, high_vs_sig)
        max_dn = min(max_dn, low_vs_sig)
        cvd_chg = float(row["cvd_close"]) - sig_cvd

        marker = " <--" if i in [5, 10, 15, 20] else ""
        row_time = pd.to_datetime(row["ts_start_et"]).strftime("%H:%M:%S")
        print(f"  {i:>3d} | {row['bar_idx']:>5.0f} | {row_time:>10s} | "
              f"{row['open']:>8.2f} {row['high']:>8.2f} {row['low']:>8.2f} {row['close']:>8.2f} | "
              f"{price_vs_sig:>+7.2f} | {max_up:>+7.2f} | "
              f"{max_dn:>+7.2f} | {row['color']:>5s} | {row['volume']:>6.0f} | {row['delta']:>6.0f} | "
              f"{row['delta_pct']:>5.1f}% | {row['cvd_close']:>8.0f} | {cvd_chg:>+8.0f}{marker}")

    # Summary at key intervals
    print(f"\n  Summary from signal bar_idx={sig_idx} close={sig_close:.2f}:")
    for n in [5, 10, 15, 20]:
        if len(after_df) >= n:
            row_n = after_df.iloc[n - 1]
            bars_to_n = after_df.iloc[:n]
            max_high = float(bars_to_n["high"].max()) - sig_close
            max_low = float(bars_to_n["low"].min()) - sig_close
            close_at_n = float(row_n["close"]) - sig_close
            t_n = pd.to_datetime(row_n["ts_start_et"]).strftime("%H:%M:%S")
            print(f"    +{n:>2d} bars: Close={row_n['close']:.2f} ({close_at_n:+.2f}) | "
                  f"MaxUp={max_high:+.2f} | MaxDn={max_low:+.2f} | "
                  f"Time={t_n} ET")

# ─── Trend assessment ───
print("\n" + "=" * 200)
print("TREND ASSESSMENT (20 bars after last signal)")
print("=" * 200)

last_sig = max(all_signal_bars)
last_sig_row = df[df["bar_idx"] == last_sig].iloc[0]
after_last = df[df["bar_idx"] > last_sig].head(20)

if not after_last.empty:
    greens = int((after_last["color"] == "GREEN").sum())
    reds = int((after_last["color"] == "RED").sum())
    net_move = float(after_last.iloc[-1]["close"]) - float(last_sig_row["close"])
    max_up_all = float(after_last["high"].max()) - float(last_sig_row["close"])
    max_dn_all = float(after_last["low"].min()) - float(last_sig_row["close"])
    avg_delta = float(after_last["delta"].mean())
    total_delta = float(after_last["delta"].sum())
    cvd_net = float(after_last.iloc[-1]["cvd_close"]) - float(last_sig_row["cvd_close"])

    # Count max consecutive same-direction streak
    colors = after_last["color"].tolist()
    max_streak = 1
    current_streak = 1
    for i in range(1, len(colors)):
        if colors[i] == colors[i - 1]:
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 1

    last_time = pd.to_datetime(last_sig_row["ts_start_et"]).strftime("%H:%M:%S")
    print(f"  From bar_idx={last_sig} ({last_time} ET) close={last_sig_row['close']:.2f}:")
    print(f"  Bars analyzed:            {len(after_last)}")
    print(f"  Net move:                 {net_move:+.2f}")
    print(f"  Max favorable up:         {max_up_all:+.2f}")
    print(f"  Max adverse down:         {max_dn_all:+.2f}")
    print(f"  Green/Red bars:           {greens}/{reds}")
    print(f"  Max same-dir streak:      {max_streak}")
    print(f"  Avg delta per bar:        {avg_delta:+.0f}")
    print(f"  Total delta ({len(after_last)} bars):    {total_delta:+.0f}")
    print(f"  CVD net change:           {cvd_net:+.0f}")

    last_bar = after_last.iloc[-1]
    last_bar_time = pd.to_datetime(last_bar["ts_start_et"]).strftime("%H:%M:%S")
    print(f"  Last bar ends at:         {last_bar_time} ET, close={last_bar['close']:.2f}")

    if abs(net_move) < 5:
        trend = "CHOP (< 5pt net)"
    elif net_move > 10:
        trend = "STRONG UP TREND"
    elif net_move > 5:
        trend = "MILD UP TREND"
    elif net_move < -10:
        trend = "STRONG DOWN TREND"
    elif net_move < -5:
        trend = "MILD DOWN TREND"
    else:
        trend = "MIXED"
    print(f"  Assessment:               {trend}")

print("\nDone.")
