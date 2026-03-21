"""
VIX Compression Setup — Full Trade Simulation
Config A: rolling 45m, VIX drop >0.8, SPX flat <20, VIX >= 15
1-min bar-by-bar simulation with multiple RM strategies.
"""
import os, sys, requests, json, math
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

def fetch_bars(token, symbol, barsback=57600):
    url_sym = symbol.replace("$", "%24")
    r = requests.get(f"https://api.tradestation.com/v3/marketdata/barcharts/{url_sym}",
        headers={"Authorization": f"Bearer {token}"},
        params={"interval": "1", "unit": "Minute", "barsback": str(barsback)},
        timeout=60)
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
        daily[dt.strftime("%Y-%m-%d")][hr*60+mn] = {
            "open": float(b["Open"]), "high": float(b["High"]),
            "low": float(b["Low"]), "close": float(b["Close"])
        }
    return daily

def get_val(day, tmin, field="close"):
    if tmin in day:
        return day[tmin][field]
    for off in [1, -1, 2, -2]:
        if tmin + off in day:
            return day[tmin + off][field]
    return None

token = get_access_token()
print("Fetching 1-min bars...")
spx_daily = parse_bars(fetch_bars(token, "$SPX.X"))
vix_daily = parse_bars(fetch_bars(token, "$VIX.X"))
common = sorted(set(spx_daily) & set(vix_daily))
print(f"Days: {len(common)} ({common[0]} to {common[-1]})")
total_weeks = len(common) / 5
total_months = len(set(d[:7] for d in common))

# ================================================================
# Find all VIX Compression signals
# ================================================================
trades = []
for dt in common:
    spx_d = spx_daily[dt]
    vix_d = vix_daily[dt]
    for scan_min in range(9*60+30, 14*60+30, 5):
        end_min = scan_min + 45
        if end_min > 15*60+30:
            break
        v0 = get_val(vix_d, scan_min)
        v1 = get_val(vix_d, end_min)
        s0 = get_val(spx_d, scan_min)
        s1 = get_val(spx_d, end_min)
        if any(x is None for x in [v0, v1, s0, s1]):
            continue
        if v0 < 15:
            continue
        if not ((v1 - v0) < -0.8 and abs(s1 - s0) < 20):
            continue
        entry = s1
        entry_time = end_min
        # Collect ALL 1-min bars after entry
        forward_bars = []
        for t in sorted(spx_d.keys()):
            if t <= entry_time:
                continue
            forward_bars.append({"tmin": t, **spx_d[t]})

        if not forward_bars:
            continue

        # MFE/MAE bar by bar
        max_up = 0
        max_down = 0
        mfe_time = entry_time
        for fb in forward_bars:
            up = fb["high"] - entry
            dn = entry - fb["low"]
            if up > max_up:
                max_up = up
                mfe_time = fb["tmin"]
            max_down = max(max_down, dn)

        close_price = forward_bars[-1]["close"]
        bars_to_close = len(forward_bars)
        mins_to_mfe = mfe_time - entry_time

        trades.append({
            "date": dt, "entry_time": entry_time, "entry": entry,
            "vix": v0, "vix_chg": v1 - v0, "spx_chg": s1 - s0,
            "mfe": max_up, "mae": max_down, "cpnl": close_price - entry,
            "mfe_time": mfe_time, "mins_to_mfe": mins_to_mfe,
            "bars_remaining": bars_to_close,
            "forward_bars": forward_bars,
        })
        break

print(f"\nTotal trades: {len(trades)}")
print(f"Per week: {len(trades)/total_weeks:.1f}")

# ================================================================
# 1) WIN vs LOSS ANALYSIS
# ================================================================
print("\n" + "=" * 80)
print("1. WIN vs LOSS BREAKDOWN (close-based)")
print("=" * 80)

wins = [t for t in trades if t["cpnl"] > 0]
losses = [t for t in trades if t["cpnl"] <= 0]

print(f"\nWins:   {len(wins)}/{len(trades)} ({len(wins)/len(trades)*100:.0f}%)")
print(f"Losses: {len(losses)}/{len(trades)} ({len(losses)/len(trades)*100:.0f}%)")

if wins:
    avg_win = sum(t["cpnl"] for t in wins) / len(wins)
    avg_win_mfe = sum(t["mfe"] for t in wins) / len(wins)
    avg_win_mae = sum(t["mae"] for t in wins) / len(wins)
    avg_win_mins = sum(t["mins_to_mfe"] for t in wins) / len(wins)
    print(f"\nWINS ({len(wins)}):")
    print(f"  Avg close P&L:  {avg_win:+.1f} pts")
    print(f"  Avg MFE:        {avg_win_mfe:+.1f} pts")
    print(f"  Avg MAE:        {avg_win_mae:.1f} pts (worst drawdown before winning)")
    print(f"  Avg mins to MFE: {avg_win_mins:.0f} min")
    print(f"  Median win:     {sorted([t['cpnl'] for t in wins])[len(wins)//2]:+.1f} pts")
    print(f"  Biggest win:    {max(t['cpnl'] for t in wins):+.1f} pts")
    print(f"  Smallest win:   {min(t['cpnl'] for t in wins):+.1f} pts")

if losses:
    avg_loss = sum(t["cpnl"] for t in losses) / len(losses)
    avg_loss_mfe = sum(t["mfe"] for t in losses) / len(losses)
    avg_loss_mae = sum(t["mae"] for t in losses) / len(losses)
    avg_loss_mins = sum(t["mins_to_mfe"] for t in losses) / len(losses)
    print(f"\nLOSSES ({len(losses)}):")
    print(f"  Avg close P&L:  {avg_loss:+.1f} pts")
    print(f"  Avg MFE:        {avg_loss_mfe:+.1f} pts (profit left on table)")
    print(f"  Avg MAE:        {avg_loss_mae:.1f} pts")
    print(f"  Avg mins to MFE: {avg_loss_mins:.0f} min")
    print(f"  Worst loss:     {min(t['cpnl'] for t in losses):+.1f} pts")
    print(f"  Shallowest loss:{max(t['cpnl'] for t in losses):+.1f} pts")

print(f"\nProfit Factor: {sum(t['cpnl'] for t in wins) / abs(sum(t['cpnl'] for t in losses)):.2f}")
print(f"Expectancy: {sum(t['cpnl'] for t in trades) / len(trades):+.1f} pts/trade")

# ================================================================
# 2) DETAILED TRADE LOG with win/loss markers
# ================================================================
print("\n" + "=" * 80)
print("2. FULL TRADE LOG")
print("=" * 80)
print(f"\n{'Date':12s} {'Entry':>7s} {'Price':>8s} {'VIX':>5s} | {'MFE':>7s} {'t(MFE)':>7s} {'MAE':>7s} {'Close':>7s} | {'W/L':>4s} {'Bars':>5s}")
print("-" * 95)

for t in trades:
    hr = t["entry_time"] // 60
    mn = t["entry_time"] % 60
    et = f"{hr}:{mn:02d}"
    mfe_hr = t["mfe_time"] // 60
    mfe_mn = t["mfe_time"] % 60
    mfe_t = f"{mfe_hr}:{mfe_mn:02d}"
    wl = "WIN" if t["cpnl"] > 0 else "LOSS"
    print(f"{t['date']:12s} {et:>7s} {t['entry']:8.1f} {t['vix']:5.1f} | {t['mfe']:+7.1f} {mfe_t:>7s} {t['mae']:+7.1f} {t['cpnl']:+7.1f} | {wl:>4s} {t['bars_remaining']:5d}")

# ================================================================
# 3) MFE DISTRIBUTION — how big are these moves?
# ================================================================
print("\n" + "=" * 80)
print("3. MFE DISTRIBUTION (how big is the move?)")
print("=" * 80)

mfe_buckets = [(0, 10), (10, 20), (20, 30), (30, 50), (50, 75), (75, 200)]
print(f"\n{'MFE Range':15s} {'Count':>6s} {'%':>6s} {'Avg Close':>10s} {'Avg MAE':>9s}")
print("-" * 50)
for lo, hi in mfe_buckets:
    bucket = [t for t in trades if lo <= t["mfe"] < hi]
    if bucket:
        avg_c = sum(t["cpnl"] for t in bucket) / len(bucket)
        avg_m = sum(t["mae"] for t in bucket) / len(bucket)
        print(f"{lo:3d}-{hi:3d} pts     {len(bucket):6d} {len(bucket)/len(trades)*100:5.0f}% {avg_c:+10.1f} {avg_m:9.1f}")

# ================================================================
# 4) TIME OF ENTRY — morning vs afternoon
# ================================================================
print("\n" + "=" * 80)
print("4. ENTRY TIME ANALYSIS")
print("=" * 80)

time_buckets = [
    ("9:30-10:30", 9*60+30, 10*60+30),
    ("10:30-11:30", 10*60+30, 11*60+30),
    ("11:30-12:30", 11*60+30, 12*60+30),
    ("12:30-13:30", 12*60+30, 13*60+30),
    ("13:30-14:30", 13*60+30, 14*60+30),
    ("14:30+", 14*60+30, 16*60),
]

print(f"\n{'Time':15s} {'N':>4s} {'WR':>5s} {'AvgMFE':>8s} {'AvgMAE':>8s} {'AvgPnL':>8s} {'MinsMFE':>8s}")
print("-" * 65)
for label, lo, hi in time_buckets:
    bucket = [t for t in trades if lo <= t["entry_time"] < hi]
    if bucket:
        n = len(bucket)
        wr = sum(1 for t in bucket if t["cpnl"] > 0) / n * 100
        avg_mfe = sum(t["mfe"] for t in bucket) / n
        avg_mae = sum(t["mae"] for t in bucket) / n
        avg_pnl = sum(t["cpnl"] for t in bucket) / n
        avg_mins = sum(t["mins_to_mfe"] for t in bucket) / n
        print(f"{label:15s} {n:4d} {wr:4.0f}% {avg_mfe:+8.1f} {avg_mae:+8.1f} {avg_pnl:+8.1f} {avg_mins:8.0f}")

# ================================================================
# 5) BAR-BY-BAR TRAILING STOP SIMULATION (1-min precision)
# ================================================================
print("\n" + "=" * 80)
print("5. TRAILING STOP STRATEGIES (1-min bar-by-bar)")
print("=" * 80)

strategies = [
    # (name, init_sl, be_trigger, trail_activation, trail_gap, fixed_tp)
    ("Fixed SL=10 TP=20", 10, None, None, None, 20),
    ("Fixed SL=12 TP=20", 12, None, None, None, 20),
    ("Fixed SL=15 TP=25", 15, None, None, None, 25),
    ("Fixed SL=15 TP=30", 15, None, None, None, 30),
    ("Fixed SL=20 TP=30", 20, None, None, None, 30),
    ("Fixed SL=20 TP=40", 20, None, None, None, 40),
    ("BE@10 trail@20/gap8", 15, 10, 20, 8, None),
    ("BE@10 trail@25/gap10", 20, 10, 25, 10, None),
    ("BE@12 trail@20/gap8", 15, 12, 20, 8, None),
    ("BE@15 trail@25/gap8", 20, 15, 25, 8, None),
    ("BE@15 trail@25/gap10", 20, 15, 25, 10, None),
    ("BE@15 trail@30/gap10", 20, 15, 30, 10, None),
    ("BE@15 trail@30/gap12", 20, 15, 30, 12, None),
    ("BE@20 trail@30/gap10", 20, 20, 30, 10, None),
    ("BE@20 trail@40/gap12", 20, 20, 40, 12, None),
    ("BE@20 trail@40/gap15", 25, 20, 40, 15, None),
    # Split target: T1=half@fixed, T2=trail
    ("Split T1@15 + T2 trail@25/10", None, None, None, None, None),
    ("Split T1@20 + T2 trail@30/10", None, None, None, None, None),
]

def simulate_trail(t, init_sl, be_trigger, trail_act, trail_gap, fixed_tp):
    entry = t["entry"]
    max_profit = 0
    stop_level = -init_sl  # relative to entry

    for fb in t["forward_bars"]:
        high_profit = fb["high"] - entry
        low_profit = fb["low"] - entry  # negative = drawdown
        max_profit = max(max_profit, high_profit)

        # Check fixed TP
        if fixed_tp and high_profit >= fixed_tp:
            return fixed_tp, fb["tmin"]

        # Check stop hit (bar low below stop)
        if low_profit <= stop_level:
            return stop_level, fb["tmin"]

        # Update stop: BE
        if be_trigger and max_profit >= be_trigger:
            stop_level = max(stop_level, 0)

        # Update stop: trail
        if trail_act and max_profit >= trail_act:
            new_stop = max_profit - trail_gap
            stop_level = max(stop_level, new_stop)

    # EOD close
    return t["cpnl"], t["forward_bars"][-1]["tmin"]

def simulate_split(t, t1_pts, trail_act, trail_gap):
    """Split target: 50% at T1, 50% trails."""
    entry = t["entry"]
    max_profit = 0
    t1_filled = False
    t1_pnl = 0
    t2_stop = -20  # T2 initial stop
    t2_result = None

    for fb in t["forward_bars"]:
        high_profit = fb["high"] - entry
        low_profit = fb["low"] - entry
        max_profit = max(max_profit, high_profit)

        if not t1_filled and high_profit >= t1_pts:
            t1_filled = True
            t1_pnl = t1_pts
            t2_stop = max(t2_stop, 0)  # move T2 stop to BE on T1 fill
            continue

        # T2 trail
        if t1_filled:
            if trail_act and max_profit >= trail_act:
                t2_stop = max(t2_stop, max_profit - trail_gap)
            if low_profit <= t2_stop:
                t2_result = t2_stop
                break

        # Initial stop (both halves)
        if not t1_filled and low_profit <= -20:
            return -20, fb["tmin"]  # full loss

    if t2_result is None:
        t2_result = t["cpnl"]

    # Average of T1 and T2
    if t1_filled:
        return (t1_pnl + t2_result) / 2, 0
    else:
        return t["cpnl"], 0

print(f"\n{'Strategy':35s} {'W':>3s} {'L':>3s} {'WR':>5s} {'Total':>8s} {'/trade':>7s} {'/week':>7s} {'MaxDD':>7s} {'PF':>5s}")
print("-" * 95)

best_strat = None
best_pnl = -9999

for name, init_sl, be_trigger, trail_act, trail_gap, fixed_tp in strategies:
    results = []

    if name.startswith("Split T1@15"):
        for t in trades:
            pnl, _ = simulate_split(t, 15, 25, 10)
            results.append(pnl)
    elif name.startswith("Split T1@20"):
        for t in trades:
            pnl, _ = simulate_split(t, 20, 30, 10)
            results.append(pnl)
    else:
        for t in trades:
            pnl, _ = simulate_trail(t, init_sl, be_trigger, trail_act, trail_gap, fixed_tp)
            results.append(pnl)

    w = sum(1 for r in results if r > 0)
    l = sum(1 for r in results if r <= 0)
    total = sum(results)
    wr = w / (w + l) * 100 if (w + l) else 0
    per_trade = total / len(results)
    per_week = total / total_weeks

    # Max drawdown
    equity = 0
    peak = 0
    max_dd = 0
    for r in results:
        equity += r
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)

    # Profit factor
    gross_win = sum(r for r in results if r > 0)
    gross_loss = abs(sum(r for r in results if r <= 0))
    pf = gross_win / gross_loss if gross_loss > 0 else 999

    print(f"{name:35s} {w:3d} {l:3d} {wr:4.0f}% {total:+8.1f} {per_trade:+7.1f} {per_week:+7.1f} {max_dd:7.1f} {pf:5.2f}")

    if total > best_pnl:
        best_pnl = total
        best_strat = name
        best_results = results

# ================================================================
# 6) FUTURES vs OPTIONS ANALYSIS
# ================================================================
print("\n" + "=" * 80)
print("6. FUTURES vs OPTIONS COMPARISON")
print("=" * 80)

print("\nFor each trade, compare:")
print("- Futures (MES 8 contracts): $40/pt")
print("- 0DTE SPX call (0.30 delta, ~$5-15 premium)")
print("- 0DTE SPX call credit spread ($2 wide)")

print(f"\n{'Date':12s} {'Entry':>7s} {'MFE':>6s} {'Close':>6s} {'MinMFE':>6s} | {'MES$':>8s} {'Call$':>8s} {'Notes':>20s}")
print("-" * 90)

total_mes = 0
total_call = 0
total_spread = 0
for t in trades:
    hr = t["entry_time"] // 60
    mn = t["entry_time"] % 60
    et = f"{hr}:{mn:02d}"
    mins_left = 16*60 - t["entry_time"]  # mins until close

    # MES P&L (using close, but capped by strategy)
    mes_pnl = t["cpnl"] * 40 * 8  # 8 MES at $5/pt each = $40/pt total... wait
    # Actually 1 MES = $5/pt, 8 MES = $40/pt
    mes_pnl_pts = t["cpnl"]

    # Option estimate: 0.30 delta call
    # Premium rough estimate: SPX ~6800, 0DTE, 0.30 delta
    # At open delta ~0.30, gamma ~0.01-0.02 for 0DTE
    # Option gains MORE than delta on fast moves (gamma acceleration)
    # Rough: first 10 pts = 0.30 * 10 = $3, next 10 pts = 0.40 * 10 = $4, etc.
    # Theta decay: ~50% of premium in last 3 hours
    if t["mfe"] > 0:
        # Approximate option value change for a 0.30 delta 0DTE call
        # Using simple gamma model: delta increases ~0.015/pt for 0DTE
        move = min(t["mfe"], t["cpnl"] if t["cpnl"] > 0 else 0)
        if move > 0:
            # Integral of (0.30 + 0.015*x) from 0 to move
            option_gain = 0.30 * move + 0.015 * move * move / 2
            # But theta eats: roughly $0.05/min for a $10 premium option in last hours
            theta_cost = 0.03 * t["mins_to_mfe"] if mins_left < 180 else 0.01 * t["mins_to_mfe"]
            call_pnl = (option_gain - theta_cost) * 100  # per contract
        else:
            call_pnl = -500  # lost premium ~$5
    else:
        call_pnl = -500  # lost premium

    # Afternoon flag
    notes = ""
    if t["entry_time"] >= 13*60:
        notes = "PM entry (opt better)"
    elif t["entry_time"] >= 12*60:
        notes = "midday"
    else:
        notes = "AM entry"

    if t["mfe"] >= 40:
        notes += " | BIG MOVE"

    total_mes += mes_pnl_pts
    total_call += call_pnl

    print(f"{t['date']:12s} {et:>7s} {t['mfe']:+6.1f} {t['cpnl']:+6.1f} {t['mins_to_mfe']:6.0f} | {mes_pnl_pts*40:+8.0f} {call_pnl:+8.0f} {notes:>20s}")

print(f"\n--- TOTALS ---")
print(f"MES 8x (close-based): {total_mes:+.1f} pts = ${total_mes*40:+,.0f}")
print(f"Options (rough est):  ${total_call:+,.0f}")

# Better options analysis: entry time matters
print(f"\n--- OPTIONS: ENTRY TIME MATTERS ---")
for label, lo, hi in time_buckets:
    bucket = [t for t in trades if lo <= t["entry_time"] < hi]
    if bucket:
        n = len(bucket)
        avg_mfe = sum(t["mfe"] for t in bucket) / n
        avg_mins_left = sum(16*60 - t["entry_time"] for t in bucket) / n
        # Gamma acceleration is stronger with less time (higher gamma for 0DTE)
        # Options are BETTER for afternoon entries with fast moves
        gamma_mult = 1.5 if avg_mins_left < 180 else 1.0
        print(f"{label:15s}: {n:2d} trades, avg MFE {avg_mfe:+.1f}, avg {avg_mins_left:.0f}min left, gamma mult {gamma_mult:.1f}x")

# ================================================================
# 7) EQUITY CURVE for best strategy
# ================================================================
print("\n" + "=" * 80)
print(f"7. EQUITY CURVE — Best strategy: {best_strat}")
print("=" * 80)

equity = 0
peak = 0
max_dd = 0
print(f"\n{'#':>3s} {'Date':12s} {'PnL':>7s} {'Equity':>8s} {'Peak':>8s} {'DD':>7s}")
print("-" * 55)
for i, (t, r) in enumerate(zip(trades, best_results)):
    equity += r
    peak = max(peak, equity)
    dd = peak - equity
    max_dd = max(max_dd, dd)
    print(f"{i+1:3d} {t['date']:12s} {r:+7.1f} {equity:+8.1f} {peak:+8.1f} {dd:7.1f}")

print(f"\nFinal equity: {equity:+.1f} pts")
print(f"Max drawdown: {max_dd:.1f} pts")
print(f"Sharpe (approx): {(sum(best_results)/len(best_results)) / (sum((r - sum(best_results)/len(best_results))**2 for r in best_results) / len(best_results))**0.5:.2f}")

# ================================================================
# 8) EXECUTIVE SUMMARY
# ================================================================
print("\n" + "=" * 80)
print("8. EXECUTIVE SUMMARY — VIX COMPRESSION SETUP")
print("=" * 80)

print(f"""
DETECTION:
  Scan every 5 min from 9:30 to 14:30 ET
  Rolling 45-min window
  Signal: VIX drops >0.8 pt in 45 min AND SPX moves <20 pts
  Gate: VIX >= 15 at start of window
  One signal per day (first detection wins)

FREQUENCY: {len(trades)/total_weeks:.1f} trades/week ({len(trades)} in {len(common)} days)

ENTRY TIME DISTRIBUTION:""")
for label, lo, hi in time_buckets:
    bucket = [t for t in trades if lo <= t["entry_time"] < hi]
    if bucket:
        print(f"  {label}: {len(bucket)} trades ({len(bucket)/len(trades)*100:.0f}%)")

print(f"""
WIN/LOSS PROFILE:
  Close-based WR: {len(wins)}/{len(trades)} = {len(wins)/len(trades)*100:.0f}%
  Avg winner:  {avg_win:+.1f} pts (MFE {avg_win_mfe:+.1f})
  Avg loser:   {avg_loss:+.1f} pts (MFE {avg_loss_mfe:+.1f} — profit left on table)
  Profit factor: {sum(t['cpnl'] for t in wins) / abs(sum(t['cpnl'] for t in losses)):.2f}

MFE STATS:
  Avg MFE: {sum(t['mfe'] for t in trades)/len(trades):+.1f} pts
  Avg time to MFE: {sum(t['mins_to_mfe'] for t in trades)/len(trades):.0f} min
  MFE >= 20: {sum(1 for t in trades if t['mfe'] >= 20)}/{len(trades)} = {sum(1 for t in trades if t['mfe'] >= 20)/len(trades)*100:.0f}%
  MFE >= 30: {sum(1 for t in trades if t['mfe'] >= 30)}/{len(trades)} = {sum(1 for t in trades if t['mfe'] >= 30)/len(trades)*100:.0f}%
  MFE >= 50: {sum(1 for t in trades if t['mfe'] >= 50)}/{len(trades)} = {sum(1 for t in trades if t['mfe'] >= 50)/len(trades)*100:.0f}%

BEST STRATEGY: {best_strat}

FUTURES vs OPTIONS:
  Futures (MES): better for AM entries (more time, less theta)
  Options (0DTE calls): better for PM entries (gamma acceleration, fast moves)
  Recommendation: FUTURES first (simpler, already integrated).
  Add options layer later for PM-only signals.
""")
print("=" * 80)
