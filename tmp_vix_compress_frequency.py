"""
VIX Compression Setup — Trade Frequency & P&L Analysis
Uses the same TS API data as the historical backtest.
Answers: How many trades/month? What P&L with realistic SL/TP?
"""
import os, sys, time, requests, json
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict
import pytz

NY = pytz.timezone("US/Eastern")

def get_access_token():
    cid = os.environ.get("TS_CLIENT_ID")
    secret = os.environ.get("TS_CLIENT_SECRET")
    refresh = os.environ.get("TS_REFRESH_TOKEN")
    r = requests.post("https://signin.tradestation.com/oauth/token",
        data={"grant_type": "refresh_token", "refresh_token": refresh,
              "client_id": cid, "client_secret": secret,
              "scope": "openid profile MarketData ReadAccount Trade OptionSpreads offline_access"},
        timeout=15)
    if r.status_code >= 400:
        print(f"Auth error: {r.text[:200]}")
        sys.exit(1)
    return r.json()["access_token"]

def fetch_bars(token, symbol, barsback=5000):
    url_sym = symbol.replace("$", "%24")
    r = requests.get(f"https://api.tradestation.com/v3/marketdata/barcharts/{url_sym}",
        headers={"Authorization": f"Bearer {token}"},
        params={"interval": "30", "unit": "Minute", "barsback": str(barsback)},
        timeout=30)
    if r.status_code != 200:
        print(f"Error fetching {symbol}: [{r.status_code}] {r.text[:200]}")
        return []
    data = r.json()
    return data.get("Bars", [])

def parse_bars(bars):
    """Parse TS bars into {date: {time_slot: {open, high, low, close}}}"""
    daily = defaultdict(dict)
    for b in bars:
        ts_str = b.get("TimeStamp", "")
        try:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            dt_et = dt.astimezone(NY)
        except:
            continue
        date_key = dt_et.strftime("%Y-%m-%d")
        hr = dt_et.hour
        mn = dt_et.minute
        if hr < 9 or (hr == 9 and mn < 30) or hr >= 16:
            continue
        close_val = float(b.get("Close", 0))
        open_val = float(b.get("Open", 0))
        high_val = float(b.get("High", 0))
        low_val = float(b.get("Low", 0))
        daily[date_key][(hr, mn)] = {"open": open_val, "high": high_val, "low": low_val, "close": close_val}
    return daily

def get_value_at(day_data, target_hr, target_mn, field="close", window=30):
    """Get value closest to target time within window minutes."""
    best = None
    best_diff = 9999
    for (hr, mn), vals in day_data.items():
        diff = abs((hr*60+mn) - (target_hr*60+target_mn))
        if diff < best_diff and diff <= window:
            best_diff = diff
            best = vals[field]
    return best

def get_range_after(day_data, after_hr, after_mn):
    """Get high and low of all bars after specified time."""
    highs, lows = [], []
    for (hr, mn), vals in day_data.items():
        if hr*60+mn >= after_hr*60+after_mn:
            highs.append(vals["high"])
            lows.append(vals["low"])
    if not highs:
        return None, None
    return max(highs), min(lows)

print("=" * 80)
print("VIX COMPRESSION - TRADE FREQUENCY & P&L ANALYSIS")
print("=" * 80)

token = get_access_token()
print("Fetching $SPX.X bars...")
spx_bars = fetch_bars(token, "$SPX.X", 5000)
print(f"  Got {len(spx_bars)} SPX bars")
print("Fetching $VIX.X bars...")
vix_bars = fetch_bars(token, "$VIX.X", 5000)
print(f"  Got {len(vix_bars)} VIX bars")

spx_daily = parse_bars(spx_bars)
vix_daily = parse_bars(vix_bars)

# Get common dates
common_dates = sorted(set(spx_daily.keys()) & set(vix_daily.keys()))
print(f"\nCommon trading days: {len(common_dates)}")
if common_dates:
    print(f"Range: {common_dates[0]} to {common_dates[-1]}")

# Analyze each day
results = []
for dt in common_dates:
    spx_d = spx_daily[dt]
    vix_d = vix_daily[dt]

    # Get key values
    spx_open = get_value_at(spx_d, 9, 30, "open") or get_value_at(spx_d, 9, 30, "close")
    spx_mid = get_value_at(spx_d, 13, 0, "close")
    spx_close = get_value_at(spx_d, 15, 30, "close") or get_value_at(spx_d, 15, 0, "close")
    vix_open = get_value_at(vix_d, 9, 30, "open") or get_value_at(vix_d, 9, 30, "close")
    vix_mid = get_value_at(vix_d, 13, 0, "close")

    if any(v is None for v in [spx_open, spx_mid, spx_close, vix_open, vix_mid]):
        continue

    pm_high, pm_low = get_range_after(spx_d, 13, 0)
    if pm_high is None:
        continue

    spx_chg = spx_mid - spx_open
    vix_chg = vix_mid - vix_open
    mid_to_close = spx_close - spx_mid
    mfe_up = pm_high - spx_mid
    mfe_down = spx_mid - pm_low

    # Classify
    vix_down = vix_chg < -0.5
    spx_flat = abs(spx_chg) < 15
    if vix_down and spx_flat:
        pattern = "VIX-COMPRESS"
    elif vix_chg > 0.5 and spx_flat:
        pattern = "VIX-EXPAND"
    elif vix_chg < -0.5 and spx_chg > 10:
        pattern = "NORMAL-BULL"
    elif vix_chg > 0.5 and spx_chg < -10:
        pattern = "NORMAL-BEAR"
    else:
        pattern = "mixed"

    month = dt[:7]  # YYYY-MM

    results.append({
        "date": dt, "month": month, "pattern": pattern,
        "spx_open": spx_open, "spx_mid": spx_mid, "spx_close": spx_close,
        "vix_open": vix_open, "vix_mid": vix_mid,
        "spx_chg": spx_chg, "vix_chg": vix_chg,
        "mid_to_close": mid_to_close, "mfe_up": mfe_up, "mfe_down": mfe_down,
    })

compress_days = [r for r in results if r["pattern"] == "VIX-COMPRESS"]
total_months = len(set(r["month"] for r in results))

print(f"\nTotal analyzed days: {len(results)}")
print(f"Total months: {total_months}")
print(f"VIX-COMPRESS days: {len(compress_days)}")
if total_months > 0:
    print(f"Avg VIX-COMPRESS per month: {len(compress_days)/total_months:.1f}")

# Monthly breakdown
print("\n" + "=" * 80)
print("MONTHLY BREAKDOWN - VIX-COMPRESS OCCURRENCES")
print("=" * 80)

months = sorted(set(r["month"] for r in results))
print(f"\n{'Month':10s} {'Total':>6s} {'Compress':>9s} {'Avg PM':>8s} {'MFE Up':>8s} {'Up%':>6s}")
print("-" * 50)

monthly_trades = []
for m in months:
    m_all = [r for r in results if r["month"] == m]
    m_comp = [r for r in m_all if r["pattern"] == "VIX-COMPRESS"]
    if m_comp:
        avg_pm = sum(r["mid_to_close"] for r in m_comp) / len(m_comp)
        avg_mfe = sum(r["mfe_up"] for r in m_comp) / len(m_comp)
        up_pct = sum(1 for r in m_comp if r["mid_to_close"] > 0) / len(m_comp) * 100
    else:
        avg_pm = avg_mfe = 0
        up_pct = 0
    print(f"{m:10s} {len(m_all):6d} {len(m_comp):9d} {avg_pm:+8.1f} {avg_mfe:+8.1f} {up_pct:5.0f}%")
    monthly_trades.append(len(m_comp))

print(f"\nMin trades/month: {min(monthly_trades)}")
print(f"Max trades/month: {max(monthly_trades)}")
print(f"Avg trades/month: {sum(monthly_trades)/len(monthly_trades):.1f}")
print(f"Months with 0 trades: {sum(1 for t in monthly_trades if t == 0)}")

# VIX level filter applied
print("\n" + "=" * 80)
print("WITH VIX >= 15 FILTER (removes low-vol losers)")
print("=" * 80)

compress_vix15 = [r for r in compress_days if r["vix_open"] >= 15]
print(f"VIX-COMPRESS with VIX>=15: {len(compress_vix15)} days")
if total_months > 0:
    print(f"Avg per month: {len(compress_vix15)/total_months:.1f}")

# Tighter filter: VIX >= 15, SPX flat < 10
compress_tight = [r for r in compress_days if r["vix_open"] >= 15 and abs(r["spx_chg"]) < 10]
print(f"VIX-COMPRESS tight (VIX>=15, SPX<10): {len(compress_tight)} days")
if total_months > 0:
    print(f"Avg per month: {len(compress_tight)/total_months:.1f}")

# SIMULATED P&L with different SL/TP combos
print("\n" + "=" * 80)
print("SIMULATED P&L (entry at 13:00, LONG)")
print("=" * 80)

# For each filter variant, simulate trades
filters = {
    "All VIX-COMPRESS": compress_days,
    "VIX >= 15": compress_vix15,
    "VIX>=15 + SPX<10": compress_tight,
}

sl_tp_combos = [
    (8, 10), (10, 15), (10, 20), (12, 20), (15, 25), (15, 30), (20, 30),
]

for filter_name, days in filters.items():
    if not days:
        continue
    print(f"\n--- {filter_name} ({len(days)} trades, {len(days)/total_months:.1f}/month) ---")
    print(f"  {'SL':>4s} {'TP':>4s} | {'Wins':>5s} {'Loss':>5s} {'WR':>5s} | {'Total PnL':>10s} {'Per Trade':>10s} {'Per Month':>10s}")
    print(f"  " + "-" * 70)

    for sl, tp in sl_tp_combos:
        wins = 0
        losses = 0
        total_pnl = 0
        for r in days:
            # Check if SL hit first or TP hit first using MFE
            hit_sl = r["mfe_down"] >= sl  # price dropped below entry by SL pts
            hit_tp = r["mfe_up"] >= tp    # price rose above entry by TP pts

            if hit_tp and not hit_sl:
                wins += 1
                total_pnl += tp
            elif hit_sl and not hit_tp:
                losses += 1
                total_pnl -= sl
            elif hit_tp and hit_sl:
                # Both hit -- assume worst case (SL first) for conservative estimate
                # Better: use mid_to_close as proxy
                if r["mid_to_close"] > 0:
                    wins += 1
                    total_pnl += tp
                else:
                    losses += 1
                    total_pnl -= sl
            else:
                # Neither hit -- close at end
                total_pnl += r["mid_to_close"]
                if r["mid_to_close"] > 0:
                    wins += 1
                else:
                    losses += 1

        n = wins + losses
        wr = wins / n * 100 if n else 0
        per_trade = total_pnl / n if n else 0
        per_month = total_pnl / total_months if total_months else 0
        print(f"  {sl:4d} {tp:4d} | {wins:5d} {losses:5d} {wr:4.0f}% | {total_pnl:+10.1f} {per_trade:+10.2f} {per_month:+10.1f}")

# Check which detection time is best: 11:00 or 13:00
print("\n" + "=" * 80)
print("DETECTION TIME: 11:00 vs 12:00 vs 13:00")
print("=" * 80)

for detect_hr, detect_mn in [(11, 0), (12, 0), (13, 0)]:
    early_compress = []
    for r_orig in results:
        dt = r_orig["date"]
        spx_d = spx_daily[dt]
        vix_d = vix_daily[dt]

        spx_at = get_value_at(spx_d, detect_hr, detect_mn, "close")
        vix_at = get_value_at(vix_d, detect_hr, detect_mn, "close")
        if spx_at is None or vix_at is None:
            continue

        s_chg = spx_at - r_orig["spx_open"]
        v_chg = vix_at - r_orig["vix_open"]

        if v_chg < -0.5 and abs(s_chg) < 15 and r_orig["vix_open"] >= 15:
            # Get afternoon from detection point
            pm_hi, pm_lo = get_range_after(spx_d, detect_hr, detect_mn)
            spx_close_val = r_orig["spx_close"]
            if pm_hi is None:
                continue
            detect_to_close = spx_close_val - spx_at
            mfe = pm_hi - spx_at
            mae = spx_at - pm_lo
            early_compress.append({
                "date": dt, "detect_to_close": detect_to_close,
                "mfe": mfe, "mae": mae, "vix_open": r_orig["vix_open"]
            })

    if early_compress:
        avg_fwd = sum(r["detect_to_close"] for r in early_compress) / len(early_compress)
        avg_mfe = sum(r["mfe"] for r in early_compress) / len(early_compress)
        up_pct = sum(1 for r in early_compress if r["detect_to_close"] > 0) / len(early_compress) * 100
        per_mo = len(early_compress) / total_months
        print(f"Detect at {detect_hr}:{detect_mn:02d}: {len(early_compress):3d} days ({per_mo:.1f}/mo) | Avg fwd: {avg_fwd:+6.1f} | MFE: {avg_mfe:+6.1f} | Up: {up_pct:.0f}%")

        # Best SL/TP for each detection time
        best_pnl = -999
        best_combo = None
        for sl, tp in sl_tp_combos:
            total = 0
            for r in early_compress:
                hit_sl = r["mae"] >= sl
                hit_tp = r["mfe"] >= tp
                if hit_tp and not hit_sl:
                    total += tp
                elif hit_sl and not hit_tp:
                    total -= sl
                elif hit_tp and hit_sl:
                    total += tp if r["detect_to_close"] > 0 else -sl
                else:
                    total += r["detect_to_close"]
            if total > best_pnl:
                best_pnl = total
                best_combo = (sl, tp)
        if best_combo:
            print(f"  Best SL/TP: {best_combo[0]}/{best_combo[1]} -> {best_pnl:+.1f} pts total ({best_pnl/total_months:+.1f}/month)")

# Summary
print("\n" + "=" * 80)
print("EXECUTIVE SUMMARY")
print("=" * 80)

if compress_vix15:
    avg_pm = sum(r["mid_to_close"] for r in compress_vix15) / len(compress_vix15)
    up_pct = sum(1 for r in compress_vix15 if r["mid_to_close"] > 0) / len(compress_vix15) * 100
    trades_per_mo = len(compress_vix15) / total_months
    print(f"\nRecommended filter: VIX>=15 + VIX drop >0.5 + SPX flat <15")
    print(f"Total signals: {len(compress_vix15)} across {total_months} months = {trades_per_mo:.1f} trades/month")
    print(f"Avg afternoon move: {avg_pm:+.1f} pts")
    print(f"Up rate: {up_pct:.0f}%")

    # Best realistic setup
    best_total = -999
    for sl, tp in sl_tp_combos:
        total = 0
        w = 0
        l = 0
        for r in compress_vix15:
            hit_sl = r["mfe_down"] >= sl
            hit_tp = r["mfe_up"] >= tp
            if hit_tp and not hit_sl:
                w += 1; total += tp
            elif hit_sl and not hit_tp:
                l += 1; total -= sl
            elif hit_tp and hit_sl:
                if r["mid_to_close"] > 0: w += 1; total += tp
                else: l += 1; total -= sl
            else:
                total += r["mid_to_close"]
                if r["mid_to_close"] > 0: w += 1
                else: l += 1
        if total > best_total:
            best_total = total
            best_sl, best_tp, best_w, best_l = sl, tp, w, l

    n = best_w + best_l
    print(f"\nBest SL/TP: {best_sl}/{best_tp}")
    print(f"  {best_w}W/{best_l}L ({best_w/(best_w+best_l)*100:.0f}% WR)")
    print(f"  Total PnL: {best_total:+.1f} pts across {len(compress_vix15)} trades")
    print(f"  Per trade: {best_total/n:+.1f} pts")
    print(f"  Per month: {best_total/total_months:+.1f} pts")
    print(f"  At 8 MES ($40/pt): ${best_total*40/total_months:+,.0f}/month")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
