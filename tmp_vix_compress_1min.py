"""
VIX Compression - 1-min scan resolution
Fetch 1-min bars, scan every 5 min for rolling VIX drop windows.
TS API limit: try 57600 bars (max), or fall back to smaller.
"""
import os, sys, requests, json
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

def fetch_bars_1min(token, symbol, barsback=57600):
    url_sym = symbol.replace("$", "%24")
    r = requests.get(f"https://api.tradestation.com/v3/marketdata/barcharts/{url_sym}",
        headers={"Authorization": f"Bearer {token}"},
        params={"interval": "1", "unit": "Minute", "barsback": str(barsback)},
        timeout=60)
    if r.status_code != 200:
        print(f"Error {symbol} barsback={barsback}: [{r.status_code}] {r.text[:200]}")
        # Try smaller
        if barsback > 10000:
            print(f"Retrying with barsback=20000...")
            r = requests.get(f"https://api.tradestation.com/v3/marketdata/barcharts/{url_sym}",
                headers={"Authorization": f"Bearer {token}"},
                params={"interval": "1", "unit": "Minute", "barsback": "20000"},
                timeout=60)
        if r.status_code != 200:
            print(f"Still error: [{r.status_code}]")
            return []
    return r.json().get("Bars", [])

def parse_bars(bars):
    """Parse into {date: {time_min: {open, high, low, close}}}"""
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

def get_val(day, tmin, field="close"):
    if tmin in day:
        return day[tmin][field]
    # nearest within 2 min
    for off in [1, -1, 2, -2]:
        if tmin + off in day:
            return day[tmin + off][field]
    return None

token = get_access_token()

print("Fetching 1-min $VIX.X bars...")
vix_bars = fetch_bars_1min(token, "$VIX.X")
print(f"  Got {len(vix_bars)} VIX bars")

print("Fetching 1-min $SPX.X bars...")
spx_bars = fetch_bars_1min(token, "$SPX.X")
print(f"  Got {len(spx_bars)} SPX bars")

spx_daily = parse_bars(spx_bars)
vix_daily = parse_bars(vix_bars)
common = sorted(set(spx_daily) & set(vix_daily))
print(f"Common days: {len(common)} ({common[0]} to {common[-1]})")

# Check data density
sample_day = common[-1]
print(f"Sample day {sample_day}: {len(spx_daily[sample_day])} SPX bars, {len(vix_daily[sample_day])} VIX bars")

total_weeks = len(common) / 5
total_months = len(set(d[:7] for d in common))

# ================================================================
# Scan every 5 min for rolling windows using 1-min data
# ================================================================

print("\n" + "=" * 80)
print("GRID SEARCH: 1-min data, scan every 5 min")
print("=" * 80)

configs = []

for window_min in [15, 20, 30, 45, 60, 90]:
    for vix_drop in [0.3, 0.5, 0.75, 1.0]:
        for spx_flat in [3, 5, 8, 10, 15, 20]:
            trades = []

            for dt in common:
                spx_d = spx_daily[dt]
                vix_d = vix_daily[dt]

                # Scan every 5 min
                for scan_min in range(9*60+30, 14*60+30, 5):
                    start_min = scan_min
                    end_min = scan_min + window_min
                    if end_min > 15*60+30:
                        break

                    v0 = get_val(vix_d, start_min)
                    v1 = get_val(vix_d, end_min)
                    s0 = get_val(spx_d, start_min)
                    s1 = get_val(spx_d, end_min)

                    if any(x is None for x in [v0, v1, s0, s1]):
                        continue
                    if v0 < 15:
                        continue
                    if not ((v1 - v0) < -vix_drop and abs(s1 - s0) < spx_flat):
                        continue

                    # SIGNAL
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
                        "date": dt, "entry_time": entry_time, "entry": entry,
                        "vix": v0, "vix_chg": v1 - v0, "spx_chg": s1 - s0,
                        "mfe": max_up, "mae": max_down, "cpnl": close_price - entry,
                    })
                    break  # one per day

            n = len(trades)
            if n < 3:
                continue

            per_week = n / total_weeks

            # Only show 0.5-4 per week
            if per_week < 0.5 or per_week > 4.0:
                continue

            avg_mfe = sum(t["mfe"] for t in trades) / n
            up_pct = sum(1 for t in trades if t["cpnl"] > 0) / n * 100

            # Trail sim: SL=20, BE@15, continuous trail gap=10
            trail_pnl = 0
            trail_w = 0
            trail_l = 0
            for t in trades:
                entry = t["entry"]
                max_profit = 0
                stop = 20  # initial SL from entry
                result = None

                for tmin in sorted(spx_daily[t["date"]].keys()):
                    if tmin <= t["entry_time"]:
                        continue
                    bar = spx_daily[t["date"]][tmin]
                    # Check bar low against stop
                    drawdown = entry - bar["low"]
                    profit_high = bar["high"] - entry
                    max_profit = max(max_profit, profit_high)

                    # Trail logic
                    if max_profit >= 15:
                        trail_stop = max_profit - 10
                        if drawdown >= (20 - trail_stop + 20):
                            pass
                        # Simpler: stop is at entry + trail_stop level
                        stop_price = entry + max(0, max_profit - 10)
                        if bar["low"] <= stop_price and max_profit >= 15:
                            result = max_profit - 10
                            break

                    if drawdown >= 20 and max_profit < 15:
                        result = -20
                        break

                if result is None:
                    result = t["cpnl"]

                trail_pnl += result
                if result > 0:
                    trail_w += 1
                else:
                    trail_l += 1

            trail_wr = trail_w / (trail_w + trail_l) * 100 if (trail_w + trail_l) else 0

            # Fixed SL=15/TP=20
            fixed_pnl = 0
            for t in trades:
                if t["mfe"] >= 20 and t["mae"] < 15:
                    fixed_pnl += 20
                elif t["mae"] >= 15:
                    fixed_pnl -= 15
                elif t["mfe"] >= 20:
                    fixed_pnl += 20 if t["cpnl"] > 0 else -15
                else:
                    fixed_pnl += t["cpnl"]

            configs.append({
                "label": f"roll {window_min:2d}m VIX>{vix_drop:.1f} SPX<{spx_flat:2d}",
                "n": n, "per_week": per_week, "avg_mfe": avg_mfe,
                "up_pct": up_pct, "trail_pnl": trail_pnl, "trail_wr": trail_wr,
                "fixed_pnl": fixed_pnl, "trades": trades,
            })

configs.sort(key=lambda x: -x["trail_pnl"])

print(f"\nTotal configs tested: {len(configs)}")
print(f"\n{'#':>3s} {'Filter':30s} {'N':>4s} {'/wk':>5s} {'MFE':>6s} {'Up%':>5s} {'TrWR':>5s} {'Fix15/20':>8s} {'Trail':>7s} {'T/wk':>6s}")
print("-" * 95)

for i, c in enumerate(configs[:30]):
    t_per_wk = c["trail_pnl"] / total_weeks
    print(f"{i+1:3d} {c['label']:30s} {c['n']:4d} {c['per_week']:5.1f} {c['avg_mfe']:+6.1f} {c['up_pct']:4.0f}% {c['trail_wr']:4.0f}% {c['fixed_pnl']:+8.0f} {c['trail_pnl']:+7.0f} {t_per_wk:+6.1f}")

# Detail for top 3
for rank in range(min(3, len(configs))):
    c = configs[rank]
    print(f"\n{'=' * 80}")
    print(f"#{rank+1}: {c['label']} -- {c['n']} trades, {c['per_week']:.1f}/wk, trail +{c['trail_pnl']:.0f}")
    print(f"{'=' * 80}")
    print(f"\n{'Date':12s} {'Entry':>7s} {'Price':>8s} {'VIX':>6s} {'Vchg':>6s} {'Schg':>6s} | {'MFE':>7s} {'MAE':>7s} {'Close':>7s}")
    print("-" * 85)

    monthly = defaultdict(lambda: {"count": 0, "mfe_sum": 0, "cpnl_sum": 0})
    for t in c["trades"]:
        hr = t["entry_time"] // 60
        mn = t["entry_time"] % 60
        et = f"{hr}:{mn:02d}"
        print(f"{t['date']:12s} {et:>7s} {t['entry']:8.1f} {t['vix']:6.1f} {t['vix_chg']:+6.2f} {t['spx_chg']:+6.1f} | {t['mfe']:+7.1f} {t['mae']:+7.1f} {t['cpnl']:+7.1f}")
        mo = t["date"][:7]
        monthly[mo]["count"] += 1
        monthly[mo]["mfe_sum"] += t["mfe"]
        monthly[mo]["cpnl_sum"] += t["cpnl"]

    print(f"\nMonthly:")
    for mo in sorted(monthly):
        m = monthly[mo]
        avg_mfe = m["mfe_sum"] / m["count"]
        print(f"  {mo}: {m['count']} trades | avg MFE: {avg_mfe:+.1f} | close PnL: {m['cpnl_sum']:+.1f}")

# Also compare: what does 5-min vs 1-min data change for the SAME config?
print(f"\n{'=' * 80}")
print("COMPARISON: Best 5-min config on 1-min data")
print("Same filter as previous winner: roll 45m VIX>0.8 SPX<20")
print(f"{'=' * 80}")

target = [c for c in configs if "45m VIX>0.8 SPX<20" in c["label"]]
if target:
    c = target[0]
    print(f"N={c['n']}, {c['per_week']:.1f}/wk, MFE={c['avg_mfe']:+.1f}, Trail={c['trail_pnl']:+.0f} ({c['trail_pnl']/total_weeks:+.1f}/wk)")
else:
    print("Not found in results (may be filtered by frequency range)")

print(f"\n{'=' * 80}")
print("DONE")
print(f"{'=' * 80}")
