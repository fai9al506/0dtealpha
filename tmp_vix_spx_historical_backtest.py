"""
VIX-SPX Divergence Historical Backtest
=======================================
Pull 6+ months of 30-min bars for $SPX.X and $VIX.X from TradeStation API.
For each trading day, classify the morning pattern and measure afternoon outcome.

Pattern: "VIX-COMPRESS" = VIX drops >0.5 while SPX is flat (<15 pts) from open to 13:00 ET.
Hypothesis: compressed VIX predicts afternoon rally (dealers unwinding hedges).
"""

import os
import sys
import time
import requests
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict

import pytz

NY = pytz.timezone("US/Eastern")

# ========== AUTH ==========

def get_access_token():
    cid = os.environ.get("TS_CLIENT_ID")
    secret = os.environ.get("TS_CLIENT_SECRET")
    refresh = os.environ.get("TS_REFRESH_TOKEN")
    if not all([cid, secret, refresh]):
        print("ERROR: Missing TS_CLIENT_ID / TS_CLIENT_SECRET / TS_REFRESH_TOKEN env vars")
        sys.exit(1)
    r = requests.post(
        "https://signin.tradestation.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh,
            "client_id": cid,
            "client_secret": secret,
            "scope": "openid profile MarketData ReadAccount Trade OptionSpreads offline_access",
        },
        timeout=15,
    )
    if r.status_code >= 400:
        print(f"Auth error [{r.status_code}]: {r.text[:300]}")
        sys.exit(1)
    tok = r.json()
    print(f"[auth] Token obtained, expires_in={tok.get('expires_in')}")
    return tok["access_token"]


# ========== DATA FETCH ==========

BASE = "https://api.tradestation.com/v3"

def fetch_bars(token, symbol, interval=30, barsback=5000, lastdate=None):
    """Fetch historical bars from TS API. symbol should be like '$SPX.X'."""
    encoded = symbol.replace("$", "%24")
    url = f"{BASE}/marketdata/barcharts/{encoded}"
    params = {
        "interval": str(interval),
        "unit": "Minute",
        "barsback": str(barsback),
    }
    if lastdate:
        params["lastdate"] = lastdate
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code >= 400:
        print(f"  API error for {symbol} [{r.status_code}]: {r.text[:300]}")
        return []
    data = r.json()
    bars = data.get("Bars", [])
    return bars


def parse_bars(bars):
    """Parse TS bars into list of dicts with ET datetime."""
    result = []
    for bar in bars:
        ts_raw = bar.get("TimeStamp", "")
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            dt_et = dt.astimezone(NY)
        except Exception:
            continue
        result.append({
            "dt": dt_et,
            "date": dt_et.strftime("%Y-%m-%d"),
            "time": dt_et.time(),
            "open": float(bar.get("Open", 0)),
            "high": float(bar.get("High", 0)),
            "low": float(bar.get("Low", 0)),
            "close": float(bar.get("Close", 0)),
            "volume": int(bar.get("TotalVolume", 0)),
        })
    return result


def fetch_all_bars(token, symbol, interval=30, total_bars=20000):
    """Fetch bars in chunks going backwards to get more history."""
    all_bars = []
    lastdate = None
    chunk_size = 5000
    fetched_dates = set()

    while len(all_bars) < total_bars:
        print(f"  Fetching {symbol} chunk (barsback={chunk_size}, lastdate={lastdate})...")
        bars = fetch_bars(token, symbol, interval=interval, barsback=chunk_size, lastdate=lastdate)
        if not bars:
            print(f"  No more bars returned for {symbol}")
            break

        # Parse and check for new data
        new_count = 0
        for bar in bars:
            ts = bar.get("TimeStamp", "")
            if ts not in fetched_dates:
                fetched_dates.add(ts)
                all_bars.append(bar)
                new_count += 1

        if new_count == 0:
            print(f"  No new bars (all duplicates), stopping")
            break

        print(f"  Got {new_count} new bars (total: {len(all_bars)})")

        # Set lastdate to the earliest bar's timestamp for next chunk
        earliest = bars[0].get("TimeStamp", "")
        for b in bars:
            if b.get("TimeStamp", "") < earliest:
                earliest = b.get("TimeStamp", "")

        # Convert to proper format for API
        try:
            dt = datetime.fromisoformat(earliest.replace("Z", "+00:00"))
            lastdate = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            lastdate = earliest

        time.sleep(0.5)  # Rate limit

    # Sort by timestamp
    all_bars.sort(key=lambda b: b.get("TimeStamp", ""))
    return all_bars


# ========== ANALYSIS ==========

def group_by_date(parsed_bars):
    """Group parsed bars by trading date, filter to market hours (9:30-16:00 ET)."""
    by_date = defaultdict(list)
    for bar in parsed_bars:
        t = bar["time"]
        # Market hours only: 9:30 to 16:00
        if t < dtime(9, 30) or t > dtime(16, 0):
            continue
        by_date[bar["date"]].append(bar)
    # Sort each day's bars by time
    for d in by_date:
        by_date[d].sort(key=lambda b: b["time"])
    return dict(by_date)


def analyze_day(spx_bars, vix_bars):
    """
    Analyze a single day's VIX-SPX divergence pattern.
    Returns dict with all metrics, or None if insufficient data.
    """
    if len(spx_bars) < 5 or len(vix_bars) < 5:
        return None

    # ---- Open values (first bar of the day, should be 9:30 or 10:00) ----
    spx_open = spx_bars[0]["open"]
    vix_open = vix_bars[0]["open"]

    # ---- Values at ~13:00 ET (find bar closest to 13:00) ----
    spx_1300 = None
    vix_1300 = None
    for bar in spx_bars:
        if bar["time"] >= dtime(12, 30) and bar["time"] <= dtime(13, 30):
            spx_1300 = bar
            break
    for bar in vix_bars:
        if bar["time"] >= dtime(12, 30) and bar["time"] <= dtime(13, 30):
            vix_1300 = bar
            break

    if not spx_1300 or not vix_1300:
        return None

    # ---- Close values (last bar) ----
    spx_close = spx_bars[-1]["close"]
    vix_close = vix_bars[-1]["close"]

    # ---- Morning metrics (open to 13:00) ----
    spx_morning_chg = spx_1300["close"] - spx_open
    vix_morning_chg = vix_1300["close"] - vix_open

    # ---- Afternoon metrics (13:00 to close) ----
    spx_afternoon_chg = spx_close - spx_1300["close"]

    # Afternoon high/low from SPX bars after 13:00
    afternoon_bars = [b for b in spx_bars if b["time"] >= dtime(13, 0)]
    if not afternoon_bars:
        return None

    afternoon_high = max(b["high"] for b in afternoon_bars)
    afternoon_low = min(b["low"] for b in afternoon_bars)
    afternoon_mfe_up = afternoon_high - spx_1300["close"]  # max favorable for longs
    afternoon_mfe_down = spx_1300["close"] - afternoon_low  # max favorable for shorts
    afternoon_range = afternoon_high - afternoon_low

    # Full day metrics
    spx_day_chg = spx_close - spx_open
    vix_day_chg = vix_close - vix_open

    # VIX morning high/low
    morning_vix_bars = [b for b in vix_bars if b["time"] <= dtime(13, 0)]
    vix_morning_high = max(b["high"] for b in morning_vix_bars) if morning_vix_bars else vix_open
    vix_morning_low = min(b["low"] for b in morning_vix_bars) if morning_vix_bars else vix_open

    return {
        "spx_open": spx_open,
        "vix_open": vix_open,
        "spx_1300": spx_1300["close"],
        "vix_1300": vix_1300["close"],
        "spx_close": spx_close,
        "vix_close": vix_close,
        "spx_morning_chg": spx_morning_chg,
        "vix_morning_chg": vix_morning_chg,
        "spx_afternoon_chg": spx_afternoon_chg,
        "spx_day_chg": spx_day_chg,
        "vix_day_chg": vix_day_chg,
        "afternoon_mfe_up": afternoon_mfe_up,
        "afternoon_mfe_down": afternoon_mfe_down,
        "afternoon_range": afternoon_range,
        "vix_morning_high": vix_morning_high,
        "vix_morning_low": vix_morning_low,
    }


def classify_day(metrics, vix_drop_thresh=0.5, spx_flat_thresh=15.0):
    """
    Classify a day into pattern categories.

    VIX-COMPRESS: VIX drops > vix_drop_thresh, SPX flat (|chg| < spx_flat_thresh) in morning
    VIX-EXPAND:   VIX rises > vix_drop_thresh, SPX flat in morning
    NORMAL-BULL:  SPX rises > spx_flat_thresh in morning (regardless of VIX)
    NORMAL-BEAR:  SPX drops > spx_flat_thresh in morning (regardless of VIX)
    MIXED:        Everything else (VIX flat, SPX flat)
    """
    spx_m = metrics["spx_morning_chg"]
    vix_m = metrics["vix_morning_chg"]

    spx_flat = abs(spx_m) < spx_flat_thresh

    if spx_flat and vix_m < -vix_drop_thresh:
        return "VIX-COMPRESS"
    elif spx_flat and vix_m > vix_drop_thresh:
        return "VIX-EXPAND"
    elif spx_m >= spx_flat_thresh:
        return "NORMAL-BULL"
    elif spx_m <= -spx_flat_thresh:
        return "NORMAL-BEAR"
    else:
        return "MIXED"


def print_separator(char="=", width=120):
    print(char * width)


def main():
    print("VIX-SPX Divergence Historical Backtest")
    print_separator()

    token = get_access_token()

    # ---- Fetch data ----
    print("\n[1/4] Fetching $SPX.X 30-min bars...")
    spx_raw = fetch_all_bars(token, "$SPX.X", interval=30, total_bars=15000)
    print(f"  Total SPX bars: {len(spx_raw)}")

    print("\n[2/4] Fetching $VIX.X 30-min bars...")
    vix_raw = fetch_all_bars(token, "$VIX.X", interval=30, total_bars=15000)
    print(f"  Total VIX bars: {len(vix_raw)}")

    if not spx_raw or not vix_raw:
        print("ERROR: No data fetched. Check API credentials and connectivity.")
        sys.exit(1)

    # Parse and group
    print("\n[3/4] Parsing and grouping by date...")
    spx_parsed = parse_bars(spx_raw)
    vix_parsed = parse_bars(vix_raw)

    spx_by_date = group_by_date(spx_parsed)
    vix_by_date = group_by_date(vix_parsed)

    # Find common dates
    common_dates = sorted(set(spx_by_date.keys()) & set(vix_by_date.keys()))
    print(f"  SPX trading days: {len(spx_by_date)}")
    print(f"  VIX trading days: {len(vix_by_date)}")
    print(f"  Common days: {len(common_dates)}")

    if not common_dates:
        print("ERROR: No overlapping dates between SPX and VIX data")
        sys.exit(1)

    print(f"  Date range: {common_dates[0]} to {common_dates[-1]}")

    # ---- Analyze each day ----
    print("\n[4/4] Analyzing daily patterns...")
    results = []
    for date in common_dates:
        metrics = analyze_day(spx_by_date[date], vix_by_date[date])
        if metrics:
            category = classify_day(metrics)
            metrics["date"] = date
            metrics["category"] = category
            results.append(metrics)

    print(f"  Analyzed {len(results)} trading days")

    if not results:
        print("ERROR: No valid analysis results")
        sys.exit(1)

    # ========== RESULTS ==========
    print("\n")
    print_separator("=")
    print("FULL DAY-BY-DAY RESULTS")
    print_separator("=")
    print(f"{'Date':<12} {'Cat':<14} {'VIX Open':>8} {'VIX 13:00':>9} {'VIX Chg AM':>10} "
          f"{'SPX Open':>9} {'SPX 13:00':>9} {'SPX Chg AM':>10} "
          f"{'SPX PM Chg':>10} {'PM MFE Up':>9} {'PM MFE Dn':>9} {'SPX Day':>8}")
    print_separator("-")

    for r in results:
        print(f"{r['date']:<12} {r['category']:<14} "
              f"{r['vix_open']:>8.2f} {r['vix_1300']:>9.2f} {r['vix_morning_chg']:>+10.2f} "
              f"{r['spx_open']:>9.2f} {r['spx_1300']:>9.2f} {r['spx_morning_chg']:>+10.2f} "
              f"{r['spx_afternoon_chg']:>+10.2f} {r['afternoon_mfe_up']:>9.2f} {r['afternoon_mfe_down']:>9.2f} "
              f"{r['spx_day_chg']:>+8.2f}")

    # ========== CATEGORY SUMMARY ==========
    print("\n")
    print_separator("=")
    print("CATEGORY SUMMARY")
    print_separator("=")

    categories = defaultdict(list)
    for r in results:
        categories[r["category"]].append(r)

    print(f"\n{'Category':<16} {'Count':>6} {'Avg PM Chg':>10} {'Med PM Chg':>10} "
          f"{'Avg MFE Up':>10} {'Avg MFE Dn':>10} {'% PM Up':>8} "
          f"{'Avg Day Chg':>11} {'% Day Up':>8}")
    print_separator("-")

    for cat in ["VIX-COMPRESS", "VIX-EXPAND", "NORMAL-BULL", "NORMAL-BEAR", "MIXED"]:
        days = categories.get(cat, [])
        if not days:
            continue
        pm_chgs = [d["spx_afternoon_chg"] for d in days]
        mfe_ups = [d["afternoon_mfe_up"] for d in days]
        mfe_dns = [d["afternoon_mfe_down"] for d in days]
        day_chgs = [d["spx_day_chg"] for d in days]

        avg_pm = sum(pm_chgs) / len(pm_chgs)
        sorted_pm = sorted(pm_chgs)
        med_pm = sorted_pm[len(sorted_pm) // 2]
        avg_mfe_up = sum(mfe_ups) / len(mfe_ups)
        avg_mfe_dn = sum(mfe_dns) / len(mfe_dns)
        pct_pm_up = 100 * sum(1 for x in pm_chgs if x > 0) / len(pm_chgs)
        avg_day = sum(day_chgs) / len(day_chgs)
        pct_day_up = 100 * sum(1 for x in day_chgs if x > 0) / len(day_chgs)

        print(f"{cat:<16} {len(days):>6} {avg_pm:>+10.2f} {med_pm:>+10.2f} "
              f"{avg_mfe_up:>10.2f} {avg_mfe_dn:>10.2f} {pct_pm_up:>7.1f}% "
              f"{avg_day:>+11.2f} {pct_day_up:>7.1f}%")

    # ========== VIX-COMPRESS DEEP DIVE ==========
    print("\n")
    print_separator("=")
    print("VIX-COMPRESS DEEP DIVE")
    print_separator("=")

    compress_days = categories.get("VIX-COMPRESS", [])
    if compress_days:
        print(f"\nTotal VIX-COMPRESS days: {len(compress_days)}")

        pm_chgs = [d["spx_afternoon_chg"] for d in compress_days]
        pm_up = [d for d in compress_days if d["spx_afternoon_chg"] > 0]
        pm_dn = [d for d in compress_days if d["spx_afternoon_chg"] <= 0]

        print(f"  Afternoon up:   {len(pm_up)} ({100*len(pm_up)/len(compress_days):.1f}%)")
        print(f"  Afternoon down: {len(pm_dn)} ({100*len(pm_dn)/len(compress_days):.1f}%)")
        print(f"  Avg PM change:  {sum(pm_chgs)/len(pm_chgs):+.2f} pts")
        print(f"  Avg MFE up:     {sum(d['afternoon_mfe_up'] for d in compress_days)/len(compress_days):.2f} pts")
        print(f"  Avg MFE down:   {sum(d['afternoon_mfe_down'] for d in compress_days)/len(compress_days):.2f} pts")

        if pm_up:
            print(f"\n  When PM rallied ({len(pm_up)} days):")
            print(f"    Avg PM gain:    {sum(d['spx_afternoon_chg'] for d in pm_up)/len(pm_up):+.2f} pts")
            print(f"    Avg MFE up:     {sum(d['afternoon_mfe_up'] for d in pm_up)/len(pm_up):.2f} pts")
            print(f"    Avg VIX drop AM: {sum(d['vix_morning_chg'] for d in pm_up)/len(pm_up):+.2f}")

        if pm_dn:
            print(f"\n  When PM sold off ({len(pm_dn)} days):")
            print(f"    Avg PM loss:    {sum(d['spx_afternoon_chg'] for d in pm_dn)/len(pm_dn):+.2f} pts")
            print(f"    Avg MFE down:   {sum(d['afternoon_mfe_down'] for d in pm_dn)/len(pm_dn):.2f} pts")
            print(f"    Avg VIX drop AM: {sum(d['vix_morning_chg'] for d in pm_dn)/len(pm_dn):+.2f}")

        # VIX level buckets within VIX-COMPRESS
        print(f"\n  VIX-COMPRESS by VIX level:")
        for lo, hi in [(0, 15), (15, 20), (20, 25), (25, 30), (30, 50)]:
            bucket = [d for d in compress_days if lo <= d["vix_open"] < hi]
            if bucket:
                avg_pm_b = sum(d["spx_afternoon_chg"] for d in bucket) / len(bucket)
                pct_up_b = 100 * sum(1 for d in bucket if d["spx_afternoon_chg"] > 0) / len(bucket)
                print(f"    VIX {lo}-{hi}: {len(bucket)} days, avg PM {avg_pm_b:+.2f}, {pct_up_b:.0f}% up")

        # VIX drop magnitude buckets
        print(f"\n  VIX-COMPRESS by VIX drop magnitude:")
        for lo, hi in [(0.5, 1.0), (1.0, 1.5), (1.5, 2.0), (2.0, 3.0), (3.0, 10.0)]:
            bucket = [d for d in compress_days if lo <= abs(d["vix_morning_chg"]) < hi]
            if bucket:
                avg_pm_b = sum(d["spx_afternoon_chg"] for d in bucket) / len(bucket)
                pct_up_b = 100 * sum(1 for d in bucket if d["spx_afternoon_chg"] > 0) / len(bucket)
                avg_mfe = sum(d["afternoon_mfe_up"] for d in bucket) / len(bucket)
                print(f"    VIX drop {lo:.1f}-{hi:.1f}: {len(bucket)} days, avg PM {avg_pm_b:+.2f}, "
                      f"MFE up {avg_mfe:.1f}, {pct_up_b:.0f}% up")

    # ========== VIX-EXPAND DEEP DIVE ==========
    print("\n")
    print_separator("=")
    print("VIX-EXPAND DEEP DIVE (Control Group)")
    print_separator("=")

    expand_days = categories.get("VIX-EXPAND", [])
    if expand_days:
        print(f"\nTotal VIX-EXPAND days: {len(expand_days)}")
        pm_chgs = [d["spx_afternoon_chg"] for d in expand_days]
        pm_up = sum(1 for x in pm_chgs if x > 0)
        print(f"  Afternoon up:   {pm_up} ({100*pm_up/len(expand_days):.1f}%)")
        print(f"  Afternoon down: {len(expand_days)-pm_up} ({100*(len(expand_days)-pm_up)/len(expand_days):.1f}%)")
        print(f"  Avg PM change:  {sum(pm_chgs)/len(pm_chgs):+.2f} pts")
        print(f"  Avg MFE up:     {sum(d['afternoon_mfe_up'] for d in expand_days)/len(expand_days):.2f} pts")
        print(f"  Avg MFE down:   {sum(d['afternoon_mfe_down'] for d in expand_days)/len(expand_days):.2f} pts")

    # ========== SENSITIVITY ANALYSIS ==========
    print("\n")
    print_separator("=")
    print("SENSITIVITY ANALYSIS: VIX Drop Threshold")
    print_separator("=")
    print(f"\n{'VIX Drop Thresh':>16} {'SPX Flat':>9} {'N Days':>7} {'Avg PM Chg':>11} {'% PM Up':>8} "
          f"{'Avg MFE Up':>11} {'Avg MFE Dn':>11} {'Edge vs All':>12}")

    # Calculate baseline
    all_pm = [r["spx_afternoon_chg"] for r in results]
    baseline_pm = sum(all_pm) / len(all_pm) if all_pm else 0

    for vix_thresh in [0.25, 0.50, 0.75, 1.00, 1.25, 1.50, 2.00, 2.50, 3.00]:
        for spx_thresh in [10, 15, 20]:
            compress = [r for r in results
                        if abs(r["spx_morning_chg"]) < spx_thresh
                        and r["vix_morning_chg"] < -vix_thresh]
            if len(compress) < 3:
                continue
            pm_chgs = [d["spx_afternoon_chg"] for d in compress]
            avg_pm = sum(pm_chgs) / len(pm_chgs)
            pct_up = 100 * sum(1 for x in pm_chgs if x > 0) / len(pm_chgs)
            avg_mfe_up = sum(d["afternoon_mfe_up"] for d in compress) / len(compress)
            avg_mfe_dn = sum(d["afternoon_mfe_down"] for d in compress) / len(compress)
            edge = avg_pm - baseline_pm
            print(f"  VIX>{vix_thresh:.2f} SPX<{spx_thresh:>2} {len(compress):>7} {avg_pm:>+11.2f} {pct_up:>7.1f}% "
                  f"{avg_mfe_up:>11.2f} {avg_mfe_dn:>11.2f} {edge:>+12.2f}")

    # ========== REVERSE: VIX RISE SENSITIVITY ==========
    print("\n")
    print_separator("=")
    print("SENSITIVITY: VIX Rise (Bearish Afternoon?)")
    print_separator("=")
    print(f"\n{'VIX Rise Thresh':>16} {'SPX Flat':>9} {'N Days':>7} {'Avg PM Chg':>11} {'% PM Down':>9} "
          f"{'Avg MFE Dn':>11}")

    for vix_thresh in [0.25, 0.50, 0.75, 1.00, 1.50, 2.00, 3.00]:
        for spx_thresh in [10, 15, 20]:
            expand = [r for r in results
                      if abs(r["spx_morning_chg"]) < spx_thresh
                      and r["vix_morning_chg"] > vix_thresh]
            if len(expand) < 3:
                continue
            pm_chgs = [d["spx_afternoon_chg"] for d in expand]
            avg_pm = sum(pm_chgs) / len(pm_chgs)
            pct_dn = 100 * sum(1 for x in pm_chgs if x < 0) / len(pm_chgs)
            avg_mfe_dn = sum(d["afternoon_mfe_down"] for d in expand) / len(expand)
            print(f"  VIX>+{vix_thresh:.2f} SPX<{spx_thresh:>2} {len(expand):>7} {avg_pm:>+11.2f} {pct_dn:>8.1f}% "
                  f"{avg_mfe_dn:>11.2f}")

    # ========== CORRELATION ANALYSIS ==========
    print("\n")
    print_separator("=")
    print("CORRELATION ANALYSIS")
    print_separator("=")

    # Simple Pearson correlation
    def pearson(xs, ys):
        n = len(xs)
        if n < 3:
            return 0.0
        mx = sum(xs) / n
        my = sum(ys) / n
        sx = (sum((x - mx) ** 2 for x in xs) / n) ** 0.5
        sy = (sum((y - my) ** 2 for y in ys) / n) ** 0.5
        if sx == 0 or sy == 0:
            return 0.0
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
        return cov / (sx * sy)

    vix_am_chgs = [r["vix_morning_chg"] for r in results]
    spx_pm_chgs = [r["spx_afternoon_chg"] for r in results]
    spx_am_chgs = [r["spx_morning_chg"] for r in results]
    vix_opens = [r["vix_open"] for r in results]

    print(f"\n  VIX morning chg vs SPX afternoon chg: r = {pearson(vix_am_chgs, spx_pm_chgs):+.4f}")
    print(f"  VIX morning chg vs SPX morning chg:   r = {pearson(vix_am_chgs, spx_am_chgs):+.4f}")
    print(f"  SPX morning chg vs SPX afternoon chg: r = {pearson(spx_am_chgs, spx_pm_chgs):+.4f}")
    print(f"  VIX open level vs SPX afternoon chg:  r = {pearson(vix_opens, spx_pm_chgs):+.4f}")

    # Correlation on VIX-COMPRESS days only
    if len(compress_days) >= 5:
        c_vix_am = [d["vix_morning_chg"] for d in compress_days]
        c_spx_pm = [d["spx_afternoon_chg"] for d in compress_days]
        print(f"\n  (VIX-COMPRESS only)")
        print(f"  VIX morning drop mag vs SPX afternoon chg: r = {pearson(c_vix_am, c_spx_pm):+.4f}")

    # ========== MONTHLY BREAKDOWN ==========
    print("\n")
    print_separator("=")
    print("MONTHLY BREAKDOWN")
    print_separator("=")

    by_month = defaultdict(list)
    for r in results:
        month = r["date"][:7]  # YYYY-MM
        by_month[month].append(r)

    print(f"\n{'Month':<10} {'Days':>5} {'VIX-C':>6} {'VIX-E':>6} {'BULL':>5} {'BEAR':>5} {'MIX':>5} "
          f"{'Avg PM All':>11} {'Avg PM VC':>10} {'VC % Up':>8}")

    for month in sorted(by_month.keys()):
        days = by_month[month]
        vc = [d for d in days if d["category"] == "VIX-COMPRESS"]
        ve = [d for d in days if d["category"] == "VIX-EXPAND"]
        nb = [d for d in days if d["category"] == "NORMAL-BULL"]
        nbr = [d for d in days if d["category"] == "NORMAL-BEAR"]
        mx = [d for d in days if d["category"] == "MIXED"]

        avg_pm_all = sum(d["spx_afternoon_chg"] for d in days) / len(days)
        if vc:
            avg_pm_vc = sum(d["spx_afternoon_chg"] for d in vc) / len(vc)
            pct_vc_up = 100 * sum(1 for d in vc if d["spx_afternoon_chg"] > 0) / len(vc)
            vc_str = f"{avg_pm_vc:>+10.2f}"
            vc_pct = f"{pct_vc_up:>7.0f}%"
        else:
            vc_str = "       n/a"
            vc_pct = "     n/a"

        print(f"{month:<10} {len(days):>5} {len(vc):>6} {len(ve):>6} {len(nb):>5} {len(nbr):>5} {len(mx):>5} "
              f"{avg_pm_all:>+11.2f} {vc_str} {vc_pct}")

    # ========== SIMULATED TRADE RESULTS ==========
    print("\n")
    print_separator("=")
    print("SIMULATED TRADE: Buy SPX at 13:00 on VIX-COMPRESS days")
    print_separator("=")

    if compress_days:
        # Fixed SL/TP trade sim
        for sl, tp in [(8, 10), (10, 15), (12, 20), (15, 25), (8, 15), (10, 20)]:
            wins = 0
            losses = 0
            total_pnl = 0
            for d in compress_days:
                mfe_up = d["afternoon_mfe_up"]
                mfe_dn = d["afternoon_mfe_down"]
                # Check stop first (conservative)
                if mfe_dn >= sl:
                    losses += 1
                    total_pnl -= sl
                elif mfe_up >= tp:
                    wins += 1
                    total_pnl += tp
                else:
                    # Neither hit -- close at EOD
                    total_pnl += d["spx_afternoon_chg"]
                    if d["spx_afternoon_chg"] > 0:
                        wins += 1
                    else:
                        losses += 1

            wr = 100 * wins / len(compress_days) if compress_days else 0
            print(f"  SL={sl:>2} TP={tp:>2}: {wins}W/{losses}L ({wr:.0f}% WR), "
                  f"PnL {total_pnl:+.1f} pts, avg {total_pnl/len(compress_days):+.2f}/trade")

    # ========== COMPARISON TABLE ==========
    print("\n")
    print_separator("=")
    print("HEAD-TO-HEAD: VIX-COMPRESS vs ALL OTHER DAYS")
    print_separator("=")

    other_days = [r for r in results if r["category"] != "VIX-COMPRESS"]
    if compress_days and other_days:
        metrics_compare = [
            ("Avg PM change (pts)",
             sum(d["spx_afternoon_chg"] for d in compress_days) / len(compress_days),
             sum(d["spx_afternoon_chg"] for d in other_days) / len(other_days)),
            ("% Afternoon up",
             100 * sum(1 for d in compress_days if d["spx_afternoon_chg"] > 0) / len(compress_days),
             100 * sum(1 for d in other_days if d["spx_afternoon_chg"] > 0) / len(other_days)),
            ("Avg MFE up (pts)",
             sum(d["afternoon_mfe_up"] for d in compress_days) / len(compress_days),
             sum(d["afternoon_mfe_up"] for d in other_days) / len(other_days)),
            ("Avg MFE down (pts)",
             sum(d["afternoon_mfe_down"] for d in compress_days) / len(compress_days),
             sum(d["afternoon_mfe_down"] for d in other_days) / len(other_days)),
            ("Avg day change (pts)",
             sum(d["spx_day_chg"] for d in compress_days) / len(compress_days),
             sum(d["spx_day_chg"] for d in other_days) / len(other_days)),
        ]

        print(f"\n{'Metric':<25} {'VIX-COMPRESS':>14} {'ALL OTHER':>14} {'Delta':>10}")
        print_separator("-", 65)
        for label, vc_val, oth_val in metrics_compare:
            delta = vc_val - oth_val
            if "%" in label:
                print(f"{label:<25} {vc_val:>13.1f}% {oth_val:>13.1f}% {delta:>+9.1f}%")
            else:
                print(f"{label:<25} {vc_val:>+14.2f} {oth_val:>+14.2f} {delta:>+10.2f}")

    # ========== DAY OF WEEK ANALYSIS ==========
    print("\n")
    print_separator("=")
    print("VIX-COMPRESS BY DAY OF WEEK")
    print_separator("=")

    if compress_days:
        dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri"]
        dow_groups = defaultdict(list)
        for d in compress_days:
            dt = datetime.strptime(d["date"], "%Y-%m-%d")
            dow_groups[dt.weekday()].append(d)

        print(f"\n{'Day':<6} {'Count':>6} {'Avg PM Chg':>11} {'% PM Up':>8} {'Avg MFE Up':>11}")
        for i in range(5):
            days = dow_groups.get(i, [])
            if days:
                avg_pm = sum(d["spx_afternoon_chg"] for d in days) / len(days)
                pct_up = 100 * sum(1 for d in days if d["spx_afternoon_chg"] > 0) / len(days)
                avg_mfe = sum(d["afternoon_mfe_up"] for d in days) / len(days)
                print(f"{dow_names[i]:<6} {len(days):>6} {avg_pm:>+11.2f} {pct_up:>7.0f}% {avg_mfe:>11.2f}")

    # ========== FINAL SUMMARY ==========
    print("\n")
    print_separator("=")
    print("FINAL SUMMARY")
    print_separator("=")
    print(f"\n  Dataset: {len(results)} trading days from {results[0]['date']} to {results[-1]['date']}")
    print(f"  Baseline avg PM change: {baseline_pm:+.2f} pts")

    if compress_days:
        vc_avg = sum(d["spx_afternoon_chg"] for d in compress_days) / len(compress_days)
        vc_pct = 100 * sum(1 for d in compress_days if d["spx_afternoon_chg"] > 0) / len(compress_days)
        print(f"\n  VIX-COMPRESS days: {len(compress_days)} ({100*len(compress_days)/len(results):.1f}% of all days)")
        print(f"  VIX-COMPRESS avg PM change: {vc_avg:+.2f} pts (edge: {vc_avg - baseline_pm:+.2f} vs baseline)")
        print(f"  VIX-COMPRESS % afternoon up: {vc_pct:.1f}%")

    print(f"\n  Conclusion: ", end="")
    if compress_days:
        edge = sum(d["spx_afternoon_chg"] for d in compress_days) / len(compress_days) - baseline_pm
        if edge > 3 and vc_pct > 55:
            print("STRONG bullish edge on VIX-COMPRESS days")
        elif edge > 1:
            print("Modest bullish edge on VIX-COMPRESS days")
        elif edge < -1:
            print("No bullish edge -- VIX-COMPRESS is WORSE than average")
        else:
            print("No meaningful edge detected")
    else:
        print("Insufficient VIX-COMPRESS days for conclusion")

    print()


if __name__ == "__main__":
    main()
