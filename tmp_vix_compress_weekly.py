"""
VIX Compression - Find 1/week frequency with 20pt target.
Loosen criteria: shorter windows, smaller VIX drops, rolling detection.
"""
import os, sys, requests
from datetime import datetime
from collections import defaultdict
import pytz

NY = pytz.timezone("US/Eastern")

def get_access_token():
    r = requests.post("https://signin.tradestation.com/oauth/token",
        data={"grant_type": "refresh_token",
              "refresh_token": os.environ["TS_REFRESH_TOKEN"],
              "client_id": os.environ["TS_CLIENT_ID"],
              "client_secret": os.environ["TS_CLIENT_SECRET"],
              "scope": "openid profile MarketData ReadAccount Trade OptionSpreads offline_access"},
        timeout=15)
    return r.json()["access_token"]

def fetch_bars(token, symbol, barsback=20000):
    url_sym = symbol.replace("$", "%24")
    r = requests.get(f"https://api.tradestation.com/v3/marketdata/barcharts/{url_sym}",
        headers={"Authorization": f"Bearer {token}"},
        params={"interval": "5", "unit": "Minute", "barsback": str(barsback)},
        timeout=30)
    return r.json().get("Bars", [])

def parse_bars(bars):
    daily = defaultdict(list)
    for b in bars:
        try:
            dt = datetime.fromisoformat(b["TimeStamp"].replace("Z", "+00:00")).astimezone(NY)
        except:
            continue
        hr, mn = dt.hour, dt.minute
        if hr < 9 or (hr == 9 and mn < 30) or hr >= 16:
            continue
        daily[dt.strftime("%Y-%m-%d")].append({
            "time_min": hr*60+mn, "hr": hr, "mn": mn,
            "open": float(b["Open"]), "high": float(b["High"]),
            "low": float(b["Low"]), "close": float(b["Close"])
        })
    for d in daily:
        daily[d].sort(key=lambda x: x["time_min"])
    return daily

token = get_access_token()
print("Fetching 5-min bars...")
spx_daily = parse_bars(fetch_bars(token, "$SPX.X"))
vix_daily = parse_bars(fetch_bars(token, "$VIX.X"))
common = sorted(set(spx_daily) & set(vix_daily))
print(f"Days: {len(common)} ({common[0]} to {common[-1]})")
total_months = len(set(d[:7] for d in common))
total_weeks = len(common) / 5

# Helper: get value at time
def val_at(bars, target_min, field="close"):
    best = None
    best_diff = 999
    for b in bars:
        diff = abs(b["time_min"] - target_min)
        if diff < best_diff and diff <= 10:
            best_diff = diff
            best = b[field]
    return best

def mfe_mae_after(bars, entry_time_min, entry_price):
    """Get MFE and MAE from entry_time forward using bar-by-bar data."""
    max_up = 0
    max_down = 0
    close_price = entry_price
    for b in bars:
        if b["time_min"] <= entry_time_min:
            continue
        up = b["high"] - entry_price
        down = entry_price - b["low"]
        max_up = max(max_up, up)
        max_down = max(max_down, down)
        close_price = b["close"]
    return max_up, max_down, close_price - entry_price

# ================================================================
# TEST MANY FILTER COMBINATIONS
# ================================================================

print("\n" + "=" * 80)
print("GRID SEARCH: Find ~1/week frequency with best P&L")
print("=" * 80)

# Params to test:
# - VIX drop threshold: 0.2, 0.3, 0.5, 0.75
# - SPX flat threshold: 5, 10, 15, 20
# - Lookback window: open->10:30, open->11:00, open->12:00, open->13:00
# - Also: rolling 1hr and 2hr windows

print(f"\n{'Filter':55s} {'N':>4s} {'/wk':>5s} {'AvgMFE':>7s} {'Up%':>5s} {'SL15':>6s} {'Trail':>6s}")
print("-" * 95)

best_configs = []

for lookback_label, lb_start, lb_end in [
    ("open->10:30", 9*60+30, 10*60+30),
    ("open->11:00", 9*60+30, 11*60),
    ("open->11:30", 9*60+30, 11*60+30),
    ("open->12:00", 9*60+30, 12*60),
    ("open->13:00", 9*60+30, 13*60),
    ("10:00->12:00", 10*60, 12*60),
    ("10:30->12:30", 10*60+30, 12*60+30),
    ("rolling 1hr best", None, None),
    ("rolling 2hr best", None, None),
]:
    for vix_drop in [0.2, 0.3, 0.5, 0.75]:
        for spx_flat in [5, 10, 15, 20, 25]:
            trades = []
            for dt in common:
                spx_d = spx_daily[dt]
                vix_d = vix_daily[dt]

                if lookback_label.startswith("rolling 1hr"):
                    # Check every 30-min window for a 1hr VIX drop
                    found = False
                    for start_min in range(9*60+30, 14*60, 30):
                        end_min = start_min + 60
                        s0 = val_at(spx_d, start_min)
                        s1 = val_at(spx_d, end_min)
                        v0 = val_at(vix_d, start_min)
                        v1 = val_at(vix_d, end_min)
                        if all(x is not None for x in [s0, s1, v0, v1]):
                            if (v1 - v0) < -vix_drop and abs(s1 - s0) < spx_flat and v0 >= 15:
                                entry_price = s1
                                mfe, mae, cpnl = mfe_mae_after(spx_d, end_min, entry_price)
                                trades.append({"date": dt, "mfe": mfe, "mae": mae, "cpnl": cpnl,
                                              "entry_time": end_min, "entry": entry_price, "vix": v0})
                                found = True
                                break  # one signal per day
                    continue

                elif lookback_label.startswith("rolling 2hr"):
                    found = False
                    for start_min in range(9*60+30, 13*60+30, 30):
                        end_min = start_min + 120
                        if end_min > 15*60:
                            break
                        s0 = val_at(spx_d, start_min)
                        s1 = val_at(spx_d, end_min)
                        v0 = val_at(vix_d, start_min)
                        v1 = val_at(vix_d, end_min)
                        if all(x is not None for x in [s0, s1, v0, v1]):
                            if (v1 - v0) < -vix_drop and abs(s1 - s0) < spx_flat and v0 >= 15:
                                entry_price = s1
                                mfe, mae, cpnl = mfe_mae_after(spx_d, end_min, entry_price)
                                trades.append({"date": dt, "mfe": mfe, "mae": mae, "cpnl": cpnl,
                                              "entry_time": end_min, "entry": entry_price, "vix": v0})
                                found = True
                                break
                    continue

                else:
                    # Fixed window
                    s0 = val_at(spx_d, lb_start, "open" if lb_start == 9*60+30 else "close")
                    s1 = val_at(spx_d, lb_end)
                    v0 = val_at(vix_d, lb_start, "open" if lb_start == 9*60+30 else "close")
                    v1 = val_at(vix_d, lb_end)

                    if any(x is None for x in [s0, s1, v0, v1]):
                        continue
                    if not ((v1 - v0) < -vix_drop and abs(s1 - s0) < spx_flat and v0 >= 15):
                        continue

                    entry_price = s1
                    mfe, mae, cpnl = mfe_mae_after(spx_d, lb_end, entry_price)
                    trades.append({"date": dt, "mfe": mfe, "mae": mae, "cpnl": cpnl,
                                  "entry_time": lb_end, "entry": entry_price, "vix": v0})

            n = len(trades)
            if n < 3:
                continue

            per_week = n / total_weeks
            avg_mfe = sum(t["mfe"] for t in trades) / n
            up_pct = sum(1 for t in trades if t["cpnl"] > 0) / n * 100

            # Simulate SL=15 TP=20
            sl15_pnl = 0
            for t in trades:
                if t["mfe"] >= 20 and t["mae"] < 15:
                    sl15_pnl += 20
                elif t["mae"] >= 15:
                    sl15_pnl -= 15
                else:
                    sl15_pnl += t["cpnl"]

            # Simulate trail: SL=20, BE@15, trail act=25 gap=10
            trail_pnl = 0
            for t in trades:
                if t["mae"] >= 20:
                    trail_pnl -= 20
                elif t["mfe"] >= 25:
                    trail_pnl += max(t["mfe"] - 10, 15)  # approximate trail capture
                elif t["mfe"] >= 15:
                    trail_pnl += 5  # BE capture
                else:
                    trail_pnl += t["cpnl"]

            label = f"{lookback_label} VIX>{vix_drop:.1f} SPX<{spx_flat}"

            # Only show combos with 0.5-2 per week
            if 0.3 <= per_week <= 3.0:
                print(f"{label:55s} {n:4d} {per_week:5.1f} {avg_mfe:+7.1f} {up_pct:4.0f}% {sl15_pnl:+6.0f} {trail_pnl:+6.0f}")
                best_configs.append((label, n, per_week, avg_mfe, up_pct, sl15_pnl, trail_pnl, trades))

# Sort by trail P&L and show top 10
print("\n" + "=" * 80)
print("TOP 10 CONFIGS BY TRAIL P&L (0.5-3 trades/week)")
print("=" * 80)

best_configs.sort(key=lambda x: -x[6])  # sort by trail_pnl
print(f"\n{'#':>3s} {'Filter':55s} {'N':>4s} {'/wk':>5s} {'AvgMFE':>7s} {'Up%':>5s} {'TrailPnL':>9s} {'PnL/wk':>7s}")
print("-" * 100)

for i, (label, n, per_week, avg_mfe, up_pct, sl15_pnl, trail_pnl, trades) in enumerate(best_configs[:15]):
    pnl_per_wk = trail_pnl / total_weeks
    print(f"{i+1:3d} {label:55s} {n:4d} {per_week:5.1f} {avg_mfe:+7.1f} {up_pct:4.0f}% {trail_pnl:+9.0f} {pnl_per_wk:+7.1f}")

# Show detail of best config
if best_configs:
    best = best_configs[0]
    print(f"\n{'=' * 80}")
    print(f"BEST CONFIG DETAIL: {best[0]}")
    print(f"{'=' * 80}")
    trades = best[7]
    print(f"\n{'Date':12s} {'Entry':>8s} {'VIX':>6s} | {'MFE':>7s} {'MAE':>7s} {'Close':>7s}")
    print("-" * 55)
    for t in trades:
        print(f"{t['date']:12s} {t['entry']:8.1f} {t['vix']:6.1f} | {t['mfe']:+7.1f} {t['mae']:+7.1f} {t['cpnl']:+7.1f}")

print(f"\n{'=' * 80}")
print("DONE")
print("=" * 80)
