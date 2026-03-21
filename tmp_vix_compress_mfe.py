"""
VIX Compression - MFE Analysis
The question: these trades have huge MFE (+30 to +50 avg).
How do we CAPTURE that MFE instead of settling for close-to-close?
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

def fetch_bars(token, symbol, barsback=5000):
    url_sym = symbol.replace("$", "%24")
    r = requests.get(f"https://api.tradestation.com/v3/marketdata/barcharts/{url_sym}",
        headers={"Authorization": f"Bearer {token}"},
        params={"interval": "5", "unit": "Minute", "barsback": str(barsback)},
        timeout=30)
    if r.status_code != 200:
        print(f"Error {symbol}: [{r.status_code}]")
        return []
    return r.json().get("Bars", [])

def parse_bars_5min(bars):
    daily = defaultdict(list)
    for b in bars:
        ts_str = b.get("TimeStamp", "")
        try:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(NY)
        except:
            continue
        hr, mn = dt.hour, dt.minute
        if hr < 9 or (hr == 9 and mn < 30) or hr >= 16:
            continue
        daily[dt.strftime("%Y-%m-%d")].append({
            "hr": hr, "mn": mn, "time_min": hr*60+mn,
            "open": float(b["Open"]), "high": float(b["High"]),
            "low": float(b["Low"]), "close": float(b["Close"])
        })
    for d in daily:
        daily[d].sort(key=lambda x: x["time_min"])
    return daily

token = get_access_token()
print("Fetching 5-min SPX bars...")
spx_bars = fetch_bars(token, "$SPX.X", 20000)
print(f"  Got {len(spx_bars)} bars")
print("Fetching 5-min VIX bars...")
vix_bars = fetch_bars(token, "$VIX.X", 20000)
print(f"  Got {len(vix_bars)} bars")

spx_daily = parse_bars_5min(spx_bars)
vix_daily = parse_bars_5min(vix_bars)
common = sorted(set(spx_daily) & set(vix_daily))
print(f"Common days: {len(common)} ({common[0]} to {common[-1]})")

# For each day, get open VIX/SPX, 13:00 VIX/SPX, then simulate bar-by-bar forward
compress_trades = []

for dt in common:
    spx_d = spx_daily[dt]
    vix_d = vix_daily[dt]

    # Get open values (9:30-9:35)
    spx_open_bars = [b for b in spx_d if b["time_min"] <= 9*60+35]
    vix_open_bars = [b for b in vix_d if b["time_min"] <= 9*60+35]
    if not spx_open_bars or not vix_open_bars:
        continue
    spx_open = spx_open_bars[0]["open"]
    vix_open = vix_open_bars[0]["open"]

    # Get 13:00 values
    spx_mid_bars = [b for b in spx_d if 13*60 <= b["time_min"] <= 13*60+5]
    vix_mid_bars = [b for b in vix_d if 13*60 <= b["time_min"] <= 13*60+5]
    if not spx_mid_bars or not vix_mid_bars:
        continue
    spx_mid = spx_mid_bars[0]["close"]
    vix_mid = vix_mid_bars[0]["close"]

    spx_chg = spx_mid - spx_open
    vix_chg = vix_mid - vix_open

    # VIX-COMPRESS filter
    if not (vix_chg < -0.5 and abs(spx_chg) < 15 and vix_open >= 15):
        continue

    # Simulate bar-by-bar from 13:00 forward
    entry = spx_mid
    pm_bars = [b for b in spx_d if b["time_min"] > 13*60]
    if not pm_bars:
        continue

    # Track bar-by-bar path for trail simulation
    running_high = entry
    running_low = entry
    bar_path = []
    for b in pm_bars:
        running_high = max(running_high, b["high"])
        running_low = min(running_low, b["low"])
        bar_path.append({
            "time_min": b["time_min"],
            "high": b["high"], "low": b["low"], "close": b["close"],
            "max_up": running_high - entry,
            "max_down": entry - running_low,
        })

    mfe = running_high - entry
    mae = entry - running_low
    close_pnl = pm_bars[-1]["close"] - entry

    compress_trades.append({
        "date": dt, "entry": entry, "vix_open": vix_open, "vix_chg": vix_chg,
        "spx_chg": spx_chg, "mfe": mfe, "mae": mae, "close_pnl": close_pnl,
        "bar_path": bar_path,
    })

print(f"\nVIX-COMPRESS trades (VIX>=15): {len(compress_trades)}")
total_months = len(set(dt[:7] for dt in common))
print(f"Total months: {total_months}")
print(f"Trades/month: {len(compress_trades)/total_months:.1f}")

# Show each trade detail
print("\n" + "=" * 80)
print("ALL VIX-COMPRESS TRADES - MFE DETAIL")
print("=" * 80)
print(f"\n{'Date':12s} {'Entry':>8s} {'VIXo':>6s} {'VIXchg':>7s} {'SPXchg':>7s} | {'MFE':>7s} {'MAE':>7s} {'Close':>7s} | {'MFE Time':>10s}")
print("-" * 90)

total_mfe = 0
total_close = 0
for t in compress_trades:
    # Find when MFE occurred
    mfe_time = ""
    for b in t["bar_path"]:
        if b["max_up"] >= t["mfe"] - 0.1:
            hr = b["time_min"] // 60
            mn = b["time_min"] % 60
            mfe_time = f"{hr}:{mn:02d}"
            break

    print(f"{t['date']:12s} {t['entry']:8.1f} {t['vix_open']:6.1f} {t['vix_chg']:+7.2f} {t['spx_chg']:+7.1f} | {t['mfe']:+7.1f} {t['mae']:+7.1f} {t['close_pnl']:+7.1f} | {mfe_time:>10s}")
    total_mfe += t["mfe"]
    total_close += t["close_pnl"]

if compress_trades:
    n = len(compress_trades)
    print(f"\nAvg MFE: {total_mfe/n:+.1f} pts")
    print(f"Avg Close P&L: {total_close/n:+.1f} pts")
    print(f"MFE capture rate (close/MFE): {(total_close/total_mfe)*100:.0f}%")

# Simulate different trail strategies to CAPTURE the MFE
print("\n" + "=" * 80)
print("TRAIL STOP SIMULATION (bar-by-bar on 5-min data)")
print("=" * 80)

strategies = [
    # (name, initial_sl, be_trigger, trail_activation, trail_gap)
    ("Fixed SL=15 TP=30", 15, None, None, None, 30),
    ("Fixed SL=15 TP=40", 15, None, None, None, 40),
    ("Fixed SL=15 TP=50", 15, None, None, None, 50),
    ("BE@10 + Trail act=20 gap=8", 15, 10, 20, 8, None),
    ("BE@10 + Trail act=25 gap=10", 15, 10, 25, 10, None),
    ("BE@10 + Trail act=15 gap=5", 15, 10, 15, 5, None),
    ("BE@10 + Trail act=30 gap=12", 15, 10, 30, 12, None),
    ("BE@15 + Trail act=20 gap=8", 15, 15, 20, 8, None),
    ("BE@15 + Trail act=25 gap=10", 20, 15, 25, 10, None),
    ("BE@15 + Trail act=30 gap=10", 20, 15, 30, 10, None),
    ("BE@20 + Trail act=30 gap=10", 20, 20, 30, 10, None),
    ("BE@20 + Trail act=40 gap=15", 20, 20, 40, 15, None),
]

print(f"\n{'Strategy':40s} {'W':>3s} {'L':>3s} {'WR':>5s} {'Total':>8s} {'Avg':>7s} {'Mo':>7s} {'8MES':>9s}")
print("-" * 90)

for name, init_sl, be_trigger, trail_act, trail_gap, fixed_tp in strategies:
    wins = 0
    losses = 0
    total_pnl = 0

    for t in compress_trades:
        entry = t["entry"]
        sl = init_sl
        be_active = False
        trail_active = False
        max_profit = 0
        result = None

        for b in t["bar_path"]:
            bar_high = b["high"]
            bar_low = b["low"]
            profit = bar_high - entry
            loss = entry - bar_low
            max_profit = max(max_profit, profit)

            # Check SL hit
            if loss >= sl:
                if be_active:
                    result = 0  # breakeven
                elif trail_active:
                    result = max_profit - trail_gap  # trail stop
                else:
                    result = -init_sl  # initial stop
                break

            # Check fixed TP
            if fixed_tp and profit >= fixed_tp:
                result = fixed_tp
                break

            # Update trail
            if be_trigger and max_profit >= be_trigger and not be_active:
                be_active = True
                sl = max(0, max_profit - (trail_gap or init_sl))

            if trail_act and max_profit >= trail_act:
                trail_active = True
                new_sl_from_high = max_profit - trail_gap
                if new_sl_from_high > sl:
                    sl = new_sl_from_high

            # Continuous trail update
            if trail_active and trail_gap:
                new_trail = max_profit - trail_gap
                if new_trail > sl:
                    sl = new_trail

        if result is None:
            # EOD close
            result = t["close_pnl"]

        total_pnl += result
        if result > 0:
            wins += 1
        else:
            losses += 1

    n = wins + losses
    wr = wins / n * 100 if n else 0
    avg = total_pnl / n if n else 0
    mo = total_pnl / total_months
    mes8 = mo * 40
    print(f"{name:40s} {wins:3d} {losses:3d} {wr:4.0f}% {total_pnl:+8.1f} {avg:+7.1f} {mo:+7.1f} {mes8:+9.0f}")

# Best possible: what if we captured MFE perfectly?
print(f"\n--- THEORETICAL MAX (perfect MFE capture) ---")
if compress_trades:
    perfect = sum(t["mfe"] for t in compress_trades)
    perfect_mo = perfect / total_months
    print(f"Total: {perfect:+.1f} pts = {perfect_mo:+.1f}/mo = ${perfect_mo*40:+,.0f}/mo at 8 MES")

# What about using it with our existing setups as a BIAS FILTER?
print("\n" + "=" * 80)
print("ALTERNATIVE: VIX-COMPRESS AS BIAS FILTER (not standalone)")
print("=" * 80)
print("""
Instead of 1 trade/month standalone, use VIX-COMPRESS as a LONG BIAS filter:
- On VIX-COMPRESS days: ALLOW all long setups, BLOCK shorts
- On VIX-EXPAND days: ALLOW all short setups, BLOCK longs
- On other days: normal filter rules

This multiplies the value because it amplifies EXISTING setup frequency
(~20+ trades/month) rather than adding 1 standalone trade.

From the 757-day backtest:
- VIX-COMPRESS days: LONGS avg +5.16 pts (vs +0.52 baseline)
- VIX-EXPAND days: SHORTS avg +10.24 pts (vs normal)
- Blocking wrong-direction trades prevents ~4 pts/trade loss
""")

print("=" * 80)
print("DONE")
print("=" * 80)
