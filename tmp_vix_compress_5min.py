"""
VIX Compression - 5-min scan resolution
Scan every 5 min for rolling VIX drop windows.
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
    daily = defaultdict(dict)
    for b in bars:
        try:
            dt = datetime.fromisoformat(b["TimeStamp"].replace("Z", "+00:00")).astimezone(NY)
        except:
            continue
        hr, mn = dt.hour, dt.minute
        if hr < 9 or (hr == 9 and mn < 30) or hr >= 16:
            continue
        tmin = hr * 60 + mn
        daily[dt.strftime("%Y-%m-%d")][tmin] = {
            "open": float(b["Open"]), "high": float(b["High"]),
            "low": float(b["Low"]), "close": float(b["Close"])
        }
    return daily

def get_close_at(day, tmin, tolerance=5):
    """Get close price at or near tmin."""
    if tmin in day:
        return day[tmin]["close"]
    for offset in range(1, tolerance + 1):
        if tmin + offset in day:
            return day[tmin + offset]["close"]
        if tmin - offset in day:
            return day[tmin - offset]["close"]
    return None

token = get_access_token()
print("Fetching 5-min bars...")
spx_daily = parse_bars(fetch_bars(token, "$SPX.X"))
vix_daily = parse_bars(fetch_bars(token, "$VIX.X"))
common = sorted(set(spx_daily) & set(vix_daily))
print(f"Days: {len(common)} ({common[0]} to {common[-1]})")
total_weeks = len(common) / 5
total_months = len(set(d[:7] for d in common))

# ================================================================
# Scan every 5 min for rolling VIX drop windows
# ================================================================

print("\n" + "=" * 80)
print("GRID SEARCH: 5-min scan resolution")
print("=" * 80)

configs = []

# Test rolling windows: 30min, 45min, 60min, 90min, 120min
# Scan step: every 5 min
# VIX drop: 0.2, 0.3, 0.5, 0.75
# SPX flat: 5, 10, 15, 20

for window_min in [30, 45, 60, 90, 120]:
    for vix_drop in [0.2, 0.3, 0.5, 0.75, 1.0]:
        for spx_flat in [3, 5, 8, 10, 15, 20]:
            trades = []

            for dt in common:
                spx_d = spx_daily[dt]
                vix_d = vix_daily[dt]
                found = False

                # Scan every 5 min from 9:30 to 14:30
                for scan_min in range(9*60+30, 14*60+30, 5):
                    start_min = scan_min
                    end_min = scan_min + window_min
                    if end_min > 15*60+30:
                        break

                    v0 = get_close_at(vix_d, start_min)
                    v1 = get_close_at(vix_d, end_min)
                    s0 = get_close_at(spx_d, start_min)
                    s1 = get_close_at(spx_d, end_min)

                    if any(x is None for x in [v0, v1, s0, s1]):
                        continue
                    if v0 < 15:
                        continue
                    if not ((v1 - v0) < -vix_drop and abs(s1 - s0) < spx_flat):
                        continue

                    # SIGNAL! Entry at s1, track forward
                    entry = s1
                    entry_time = end_min
                    max_up = 0
                    max_down = 0
                    close_price = entry

                    for t in sorted(spx_d.keys()):
                        if t <= entry_time:
                            continue
                        bar = spx_d[t]
                        max_up = max(max_up, bar["high"] - entry)
                        max_down = max(max_down, entry - bar["low"])
                        close_price = bar["close"]

                    trades.append({
                        "date": dt,
                        "entry_time": entry_time,
                        "entry": entry,
                        "vix": v0,
                        "vix_chg": v1 - v0,
                        "spx_chg": s1 - s0,
                        "mfe": max_up,
                        "mae": max_down,
                        "cpnl": close_price - entry,
                    })
                    found = True
                    break  # one signal per day

            n = len(trades)
            if n < 3:
                continue

            per_week = n / total_weeks
            avg_mfe = sum(t["mfe"] for t in trades) / n
            up_pct = sum(1 for t in trades if t["cpnl"] > 0) / n * 100

            # Trail sim: SL=20, BE@15, continuous trail act=25 gap=10
            trail_pnl = 0
            trail_wins = 0
            trail_losses = 0
            for t in trades:
                entry = t["entry"]
                sl_level = 20  # initial
                max_profit = 0
                result = None
                dt_key = t["date"]
                entry_time = t["entry_time"]

                for tmin in sorted(spx_daily[dt_key].keys()):
                    if tmin <= entry_time:
                        continue
                    bar = spx_daily[dt_key][tmin]
                    profit = bar["high"] - entry
                    drawdown = entry - bar["low"]
                    max_profit = max(max_profit, profit)

                    # Check stop
                    if drawdown >= sl_level:
                        if max_profit >= 15:
                            result = max(0, max_profit - 10)
                        else:
                            result = -20
                        break

                    # Update trail
                    if max_profit >= 15:
                        new_sl = max_profit - 10
                        if new_sl > (20 - (entry - (entry - sl_level))):
                            sl_level = max(sl_level, 20 - (max_profit - new_sl))
                            # Simplified: once profit >= 15, trail at profit - 10
                            sl_level_from_entry = max_profit - 10
                            # Stop triggers when price drops to entry + sl_level_from_entry
                            # i.e. drawdown from HIGH >= 10

                if result is None:
                    result = t["cpnl"]

                trail_pnl += result
                if result > 0:
                    trail_wins += 1
                else:
                    trail_losses += 1

            # Fixed SL=15/TP=20 sim
            fixed_pnl = 0
            for t in trades:
                if t["mfe"] >= 20 and t["mae"] < 15:
                    fixed_pnl += 20
                elif t["mae"] >= 15:
                    fixed_pnl -= 15
                elif t["mfe"] >= 20:
                    # Both hit — use close as tiebreak
                    fixed_pnl += 20 if t["cpnl"] > 0 else -15
                else:
                    fixed_pnl += t["cpnl"]

            if 0.5 <= per_week <= 4.0:
                configs.append({
                    "label": f"roll {window_min}m VIX>{vix_drop:.1f} SPX<{spx_flat}",
                    "n": n, "per_week": per_week, "avg_mfe": avg_mfe,
                    "up_pct": up_pct, "fixed_pnl": fixed_pnl, "trail_pnl": trail_pnl,
                    "trail_wr": trail_wins / (trail_wins + trail_losses) * 100 if (trail_wins + trail_losses) > 0 else 0,
                    "trades": trades,
                })

# Sort by trail P&L
configs.sort(key=lambda x: -x["trail_pnl"])

print(f"\n{'#':>3s} {'Filter':35s} {'N':>4s} {'/wk':>5s} {'MFE':>6s} {'Up%':>5s} {'WR':>5s} {'FixPnL':>7s} {'Trail':>7s} {'T/wk':>6s}")
print("-" * 95)

for i, c in enumerate(configs[:25]):
    t_per_wk = c["trail_pnl"] / total_weeks
    print(f"{i+1:3d} {c['label']:35s} {c['n']:4d} {c['per_week']:5.1f} {c['avg_mfe']:+6.1f} {c['up_pct']:4.0f}% {c['trail_wr']:4.0f}% {c['fixed_pnl']:+7.0f} {c['trail_pnl']:+7.0f} {t_per_wk:+6.1f}")

# Show detail for top 3
for rank in range(min(3, len(configs))):
    c = configs[rank]
    print(f"\n{'=' * 80}")
    print(f"#{rank+1}: {c['label']} ({c['n']} trades, {c['per_week']:.1f}/wk)")
    print(f"{'=' * 80}")
    print(f"\n{'Date':12s} {'EntryT':>7s} {'Entry':>8s} {'VIX':>6s} {'VIXchg':>7s} {'SPXchg':>7s} | {'MFE':>7s} {'MAE':>7s} {'Close':>7s}")
    print("-" * 85)

    monthly = defaultdict(lambda: {"count": 0, "pnl": 0})
    for t in c["trades"]:
        hr = t["entry_time"] // 60
        mn = t["entry_time"] % 60
        entry_str = f"{hr}:{mn:02d}"
        print(f"{t['date']:12s} {entry_str:>7s} {t['entry']:8.1f} {t['vix']:6.1f} {t['vix_chg']:+7.2f} {t['spx_chg']:+7.1f} | {t['mfe']:+7.1f} {t['mae']:+7.1f} {t['cpnl']:+7.1f}")
        mo = t["date"][:7]
        monthly[mo]["count"] += 1
        monthly[mo]["pnl"] += t["cpnl"]

    print(f"\nMonthly distribution:")
    for mo in sorted(monthly):
        m = monthly[mo]
        print(f"  {mo}: {m['count']} trades, close PnL: {m['pnl']:+.1f}")

print(f"\n{'=' * 80}")
print("DONE")
print("=" * 80)
