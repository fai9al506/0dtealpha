"""
DD Filter/Setup Backtest
========================
Tests 3 concepts from deep analysis:

A) STANDALONE SIGNAL: DD Extreme Level
   - LONG when total DD < -$5B (contrarian bounce, 70% from analysis)
   - SHORT when total DD < -$5B coming from positive (momentum exhaustion)

B) VOLATILITY GATE FILTER: DD Concentration
   - Block entries when concentration > X% (dead zone, small moves)
   - Allow entries when concentration < X% (big moves expected)

C) COMBO: DD Level + Concentration as entry trigger
   - Enter when extreme DD + low concentration (directional + volatile)

Each tested with fixed SL/TP and trailing stop variants.
"""
import json, os, psycopg2, statistics, math
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict
import pytz

DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()
NY = pytz.timezone("US/Eastern")

out = []
def p(s=""):
    out.append(str(s))

# ============================================================
# LOAD DATA
# ============================================================
p("Loading DD exposure data...")
cur.execute("""
    SELECT ts_utc, strike::float, value::float, current_price::float
    FROM volland_exposure_points
    WHERE greek = 'deltaDecay'
    ORDER BY ts_utc, strike
""")
raw = cur.fetchall()

snapshots = defaultdict(list)
snap_spots = {}
for ts, strike, value, spot in raw:
    snapshots[ts].append((strike, value))
    snap_spots[ts] = spot

timestamps = sorted(snapshots.keys())
p(f"Snapshots: {len(timestamps)}, Date range: {timestamps[0].astimezone(NY).date()} to {timestamps[-1].astimezone(NY).date()}")

# Also load spot price from chain_snapshots for finer resolution
p("Loading chain spot prices for outcome tracking...")
cur.execute("""
    SELECT ts, spot FROM chain_snapshots
    WHERE spot > 0
    ORDER BY ts
""")
chain_spots = cur.fetchall()
# Index by date for fast lookup
chain_by_date = defaultdict(list)
for ts, spot in chain_spots:
    ts_et = ts.astimezone(NY) if ts.tzinfo else NY.localize(ts)
    chain_by_date[ts_et.date()].append((ts_et, spot))

for d in chain_by_date:
    chain_by_date[d].sort()

p(f"Chain spot data: {len(chain_spots)} rows")

# ============================================================
# BUILD SNAPSHOT ANALYSIS
# ============================================================
snap_analysis = []
for ts in timestamps:
    points = snapshots[ts]
    spot = snap_spots[ts]
    ts_et = ts.astimezone(NY)

    if ts_et.time() < dtime(9, 30) or ts_et.time() > dtime(16, 0):
        continue

    total_dd = sum(v for _, v in points)
    total_abs = sum(abs(v) for _, v in points)
    near_abs = sum(abs(v) for s, v in points if abs(s - spot) <= 25)
    concentration = near_abs / total_abs * 100 if total_abs > 0 else 0

    # DD above vs below
    dd_above = sum(v for s, v in points if s > spot)
    dd_below = sum(v for s, v in points if s <= spot)

    snap_analysis.append({
        "ts": ts,
        "ts_et": ts_et,
        "date": ts_et.date(),
        "time": ts_et.time(),
        "spot": spot,
        "total_dd": total_dd,
        "concentration": concentration,
        "dd_above": dd_above,
        "dd_below": dd_below,
    })

p(f"Market-hours snapshots: {len(snap_analysis)}")

# ============================================================
# HELPER: Simulate trade outcome using chain spot prices
# ============================================================
def simulate_trade(entry_date, entry_time_et, entry_price, direction,
                   stop_pts, target_pts, trail_activation=None, trail_gap=None,
                   max_hold_min=120):
    """
    Simulate a trade using 30s chain_snapshots spot data.
    Returns: (result, pnl, max_profit, max_loss, hold_minutes, exit_price)
    """
    chain = chain_by_date.get(entry_date, [])
    if not chain:
        return ("NO_DATA", 0, 0, 0, 0, entry_price)

    # Find entries after entry time
    entry_dt = NY.localize(datetime.combine(entry_date, entry_time_et))
    future = [(t, s) for t, s in chain if t > entry_dt]
    if not future:
        return ("NO_DATA", 0, 0, 0, 0, entry_price)

    max_profit = 0
    max_loss = 0
    trail_stop = None

    for t, spot in future:
        elapsed = (t - entry_dt).total_seconds() / 60
        if elapsed > max_hold_min:
            # Expired
            pnl = (spot - entry_price) if direction == "long" else (entry_price - spot)
            return ("EXPIRED", pnl, max_profit, max_loss, elapsed, spot)

        if direction == "long":
            profit = spot - entry_price
        else:
            profit = entry_price - spot

        max_profit = max(max_profit, profit)
        max_loss = min(max_loss, profit)

        # Check stop
        if profit <= -stop_pts:
            return ("LOSS", -stop_pts, max_profit, max_loss, elapsed, spot)

        # Check target
        if target_pts and profit >= target_pts:
            return ("WIN", target_pts, max_profit, max_loss, elapsed, spot)

        # Trailing stop
        if trail_activation and trail_gap and max_profit >= trail_activation:
            trail_level = max_profit - trail_gap
            if trail_stop is None or trail_level > trail_stop:
                trail_stop = trail_level
            if profit <= trail_stop:
                return ("TRAIL", trail_stop, max_profit, max_loss, elapsed, spot)

    # End of data
    last_spot = future[-1][1]
    pnl = (last_spot - entry_price) if direction == "long" else (entry_price - last_spot)
    elapsed = (future[-1][0] - entry_dt).total_seconds() / 60
    return ("EOD", pnl, max_profit, max_loss, elapsed, last_spot)


# ============================================================
# SETUP A: DD Extreme Level Signal
# ============================================================
p("\n" + "=" * 140)
p("SETUP A: DD EXTREME LEVEL — Standalone Signal")
p("=" * 140)
p("LONG when total DD crosses below threshold (bearish extreme → contrarian bounce)")
p("SHORT when total DD crosses above threshold (bullish extreme → contrarian fade)")

# Test multiple thresholds and RM combos
thresholds_B = [-3e9, -4e9, -5e9, -6e9]  # For LONG (bearish DD)
thresholds_S = [3e9, 4e9, 5e9, 6e9]       # For SHORT (bullish DD)

rm_combos = [
    {"name": "SL5/T5", "stop": 5, "target": 5, "trail_act": None, "trail_gap": None},
    {"name": "SL5/T10", "stop": 5, "target": 10, "trail_act": None, "trail_gap": None},
    {"name": "SL8/T10", "stop": 8, "target": 10, "trail_act": None, "trail_gap": None},
    {"name": "SL10/T15", "stop": 10, "target": 15, "trail_act": None, "trail_gap": None},
    {"name": "SL5/Trail10g5", "stop": 5, "target": None, "trail_act": 10, "trail_gap": 5},
    {"name": "SL8/Trail15g5", "stop": 8, "target": None, "trail_act": 15, "trail_gap": 5},
    {"name": "SL10/Trail20g5", "stop": 10, "target": None, "trail_act": 20, "trail_gap": 5},
]

# Generate signals with cooldown
def generate_dd_level_signals(threshold_long, threshold_short, cooldown_min=30,
                               time_start=dtime(10, 0), time_end=dtime(14, 30)):
    """Generate signals when DD total crosses extreme thresholds."""
    signals = []
    last_signal_time = {}  # per date per direction

    for i, sa in enumerate(snap_analysis):
        if sa["time"] < time_start or sa["time"] > time_end:
            continue

        direction = None
        # LONG: DD total < threshold (bearish extreme → contrarian)
        if sa["total_dd"] < threshold_long:
            direction = "long"
        # SHORT: DD total > threshold (bullish extreme → contrarian)
        elif sa["total_dd"] > threshold_short:
            direction = "short"

        if direction is None:
            continue

        # Cooldown check
        key = (sa["date"], direction)
        if key in last_signal_time:
            elapsed = (sa["ts"] - last_signal_time[key]).total_seconds() / 60
            if elapsed < cooldown_min:
                continue

        last_signal_time[key] = sa["ts"]
        signals.append({
            "date": sa["date"],
            "time": sa["time"],
            "ts": sa["ts"],
            "ts_et": sa["ts_et"],
            "spot": sa["spot"],
            "direction": direction,
            "total_dd": sa["total_dd"],
            "concentration": sa["concentration"],
        })

    return signals


p("\n--- LONG Signals (Bearish DD → Contrarian Bounce) ---")
p(f"{'Threshold':>12} {'Signals':>8} {'RM Config':>18} {'W':>4} {'L':>4} {'T':>4} {'E':>4} "
  f"{'WR':>6} {'TotalPnL':>10} {'AvgPnL':>8} {'MaxDD':>8} {'PF':>6}")
p("-" * 120)

best_long = {"pnl": -999, "config": ""}
for thresh in thresholds_B:
    signals = generate_dd_level_signals(thresh, 999e9)  # Only long
    long_sigs = [s for s in signals if s["direction"] == "long"]

    if not long_sigs:
        continue

    for rm in rm_combos:
        trades = []
        for sig in long_sigs:
            result, pnl, mp, ml, hold, exit_p = simulate_trade(
                sig["date"], sig["time"], sig["spot"], sig["direction"],
                rm["stop"], rm["target"], rm["trail_act"], rm["trail_gap"])
            trades.append({"result": result, "pnl": pnl, "max_profit": mp,
                          "date": sig["date"], "time": sig["time"], "spot": sig["spot"],
                          "dd": sig["total_dd"], "conc": sig["concentration"]})

        wins = sum(1 for t in trades if t["result"] in ("WIN", "TRAIL") and t["pnl"] > 0)
        losses = sum(1 for t in trades if t["result"] == "LOSS")
        trails = sum(1 for t in trades if t["result"] == "TRAIL" and t["pnl"] > 0)
        expired = sum(1 for t in trades if t["result"] in ("EXPIRED", "EOD"))
        total_pnl = sum(t["pnl"] for t in trades)
        wr = wins / max(1, wins + losses) * 100

        gross_win = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else 999

        # Max drawdown
        running = 0
        max_dd = 0
        for t in trades:
            running += t["pnl"]
            max_dd = min(max_dd, running)

        flag = " ***" if total_pnl > best_long["pnl"] else ""
        if total_pnl > best_long["pnl"]:
            best_long = {"pnl": total_pnl, "config": f"DD<{thresh/1e9:.0f}B {rm['name']}"}

        p(f"{thresh/1e9:+11.0f}B {len(long_sigs):8} {rm['name']:>18} {wins:4} {losses:4} {trails:4} {expired:4} "
          f"{wr:5.1f}% {total_pnl:+10.1f} {total_pnl/max(1,len(trades)):+8.1f} {max_dd:+8.1f} {pf:6.2f}{flag}")

p(f"\nBest LONG config: {best_long['config']} → {best_long['pnl']:+.1f} pts")


p("\n--- SHORT Signals (Bullish DD → Contrarian Fade) ---")
p(f"{'Threshold':>12} {'Signals':>8} {'RM Config':>18} {'W':>4} {'L':>4} {'T':>4} {'E':>4} "
  f"{'WR':>6} {'TotalPnL':>10} {'AvgPnL':>8} {'MaxDD':>8} {'PF':>6}")
p("-" * 120)

best_short = {"pnl": -999, "config": ""}
for thresh in thresholds_S:
    signals = generate_dd_level_signals(-999e9, thresh)  # Only short
    short_sigs = [s for s in signals if s["direction"] == "short"]

    if not short_sigs:
        continue

    for rm in rm_combos:
        trades = []
        for sig in short_sigs:
            result, pnl, mp, ml, hold, exit_p = simulate_trade(
                sig["date"], sig["time"], sig["spot"], sig["direction"],
                rm["stop"], rm["target"], rm["trail_act"], rm["trail_gap"])
            trades.append({"result": result, "pnl": pnl})

        wins = sum(1 for t in trades if t["result"] in ("WIN", "TRAIL") and t["pnl"] > 0)
        losses = sum(1 for t in trades if t["result"] == "LOSS")
        trails = sum(1 for t in trades if t["result"] == "TRAIL" and t["pnl"] > 0)
        expired = sum(1 for t in trades if t["result"] in ("EXPIRED", "EOD"))
        total_pnl = sum(t["pnl"] for t in trades)
        wr = wins / max(1, wins + losses) * 100
        gross_win = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else 999

        running = 0
        max_dd = 0
        for t in trades:
            running += t["pnl"]
            max_dd = min(max_dd, running)

        flag = " ***" if total_pnl > best_short["pnl"] else ""
        if total_pnl > best_short["pnl"]:
            best_short = {"pnl": total_pnl, "config": f"DD>{thresh/1e9:.0f}B {rm['name']}"}

        p(f"{thresh/1e9:+11.0f}B {len(short_sigs):8} {rm['name']:>18} {wins:4} {losses:4} {trails:4} {expired:4} "
          f"{wr:5.1f}% {total_pnl:+10.1f} {total_pnl/max(1,len(trades)):+8.1f} {max_dd:+8.1f} {pf:6.2f}{flag}")

p(f"\nBest SHORT config: {best_short['config']} → {best_short['pnl']:+.1f} pts")


# ============================================================
# SETUP B: DD Concentration Filter for Existing Setups
# ============================================================
p("\n\n" + "=" * 140)
p("SETUP B: DD CONCENTRATION FILTER — Applied to Existing Setup Signals")
p("=" * 140)
p("Test: Block setups when DD concentration > threshold (dead zone)")
p("Hypothesis: Setups fire in low-concentration (volatile) periods → better results")

# Load actual setup_log entries
cur.execute("""
    SELECT id, ts, setup_name, direction, spot, grade, score,
           outcome_result, outcome_pnl
    FROM setup_log
    WHERE outcome_result IS NOT NULL
      AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
    ORDER BY ts
""")
setup_trades = cur.fetchall()
p(f"\nLoaded {len(setup_trades)} completed setup trades from DB")

# For each setup trade, find the nearest DD snapshot to get concentration
def find_dd_concentration(trade_ts, trade_date):
    """Find DD concentration at the time of the trade."""
    trade_ts_et = trade_ts.astimezone(NY) if trade_ts.tzinfo else NY.localize(trade_ts)

    # Find nearest DD snapshot
    best_match = None
    best_delta = float('inf')
    for sa in snap_analysis:
        if sa["date"] != trade_date:
            continue
        delta = abs((sa["ts_et"] - trade_ts_et).total_seconds())
        if delta < best_delta:
            best_delta = delta
            best_match = sa

    if best_match and best_delta < 600:  # Within 10 min
        return best_match["concentration"], best_match["total_dd"]
    return None, None

# Enrich setup trades with DD data
enriched = []
for row in setup_trades:
    sid, ts, name, direction, spot, grade, score, result, pnl = row
    ts_et = ts.astimezone(NY) if ts.tzinfo else NY.localize(ts)
    trade_date = ts_et.date()

    conc, dd_total = find_dd_concentration(ts, trade_date)

    enriched.append({
        "id": sid, "ts": ts, "ts_et": ts_et, "date": trade_date,
        "name": name, "direction": direction, "spot": spot,
        "grade": grade, "score": score,
        "result": result, "pnl": float(pnl) if pnl else 0,
        "concentration": conc, "total_dd": dd_total,
    })

has_dd = [e for e in enriched if e["concentration"] is not None]
p(f"Trades with DD data: {has_dd} / {len(enriched)}")
p(f"Trades matched to DD snapshots: {len(has_dd)}")

if has_dd:
    # Show by concentration bucket
    p(f"\n--- All Setups by DD Concentration at Entry ---")
    p(f"{'Concentration':25} {'n':>4} {'W':>4} {'L':>4} {'WR':>6} {'TotalPnL':>10} {'AvgPnL':>8}")
    p("-" * 75)

    for name, lo, hi in [("Low (<40%)", 0, 40), ("Med (40-60%)", 40, 60),
                          ("High (60-80%)", 60, 80), ("VHigh (>80%)", 80, 101)]:
        bucket = [e for e in has_dd if lo <= (e["concentration"] or 0) < hi]
        if not bucket:
            continue
        wins = sum(1 for e in bucket if e["result"] == "WIN")
        losses = sum(1 for e in bucket if e["result"] == "LOSS")
        total = wins + losses
        wr = wins / total * 100 if total > 0 else 0
        total_pnl = sum(e["pnl"] for e in bucket)
        p(f"{name:25} {len(bucket):4} {wins:4} {losses:4} {wr:5.1f}% {total_pnl:+10.1f} {total_pnl/max(1,len(bucket)):+8.1f}")

    # By setup name + concentration
    p(f"\n--- Per-Setup by DD Concentration ---")
    for setup_name in sorted(set(e["name"] for e in has_dd)):
        setup_trades_dd = [e for e in has_dd if e["name"] == setup_name]
        if len(setup_trades_dd) < 2:
            continue

        p(f"\n  {setup_name}:")
        for conc_name, lo, hi in [("Low (<50%)", 0, 50), ("Med (50-75%)", 50, 75), ("High (>75%)", 75, 101)]:
            bucket = [e for e in setup_trades_dd if lo <= (e["concentration"] or 0) < hi]
            if not bucket:
                continue
            wins = sum(1 for e in bucket if e["result"] == "WIN")
            losses = sum(1 for e in bucket if e["result"] == "LOSS")
            total = wins + losses
            wr = wins / total * 100 if total > 0 else 0
            total_pnl = sum(e["pnl"] for e in bucket)
            p(f"    {conc_name:20} n={len(bucket):3} W={wins} L={losses} WR={wr:.0f}% PnL={total_pnl:+.1f}")

    # DD Total Level filter on existing setups
    p(f"\n--- All Setups by DD Total Level at Entry ---")
    for name, filt in [
        ("Bearish DD (<-$2B)", lambda e: e["total_dd"] is not None and e["total_dd"] < -2e9),
        ("Mild Bear (-$2B to $0)", lambda e: e["total_dd"] is not None and -2e9 <= e["total_dd"] < 0),
        ("Mild Bull ($0 to $2B)", lambda e: e["total_dd"] is not None and 0 <= e["total_dd"] < 2e9),
        ("Bullish DD (>$2B)", lambda e: e["total_dd"] is not None and e["total_dd"] >= 2e9),
    ]:
        bucket = [e for e in has_dd if filt(e)]
        if not bucket:
            continue
        wins = sum(1 for e in bucket if e["result"] == "WIN")
        losses = sum(1 for e in bucket if e["result"] == "LOSS")
        total = wins + losses
        wr = wins / total * 100 if total > 0 else 0
        total_pnl = sum(e["pnl"] for e in bucket)
        p(f"  {name:25} n={len(bucket):3} W={wins} L={losses} WR={wr:.0f}% PnL={total_pnl:+.1f}")


# ============================================================
# SETUP C: Combined DD Level + Concentration
# ============================================================
p("\n\n" + "=" * 140)
p("SETUP C: DD EXTREME + LOW CONCENTRATION (Best Combo)")
p("=" * 140)
p("Entry: DD total at extreme level + concentration < 60% (volatile environment)")
p("This means: extreme dealer positioning + high potential for big move")

combo_configs = [
    {"name": "Long: DD<-3B & Conc<60%", "dd_lo": -999e9, "dd_hi": -3e9, "conc_max": 60, "dir": "long"},
    {"name": "Long: DD<-3B & Conc<50%", "dd_lo": -999e9, "dd_hi": -3e9, "conc_max": 50, "dir": "long"},
    {"name": "Long: DD<-4B & Conc<60%", "dd_lo": -999e9, "dd_hi": -4e9, "conc_max": 60, "dir": "long"},
    {"name": "Long: DD<-2B & Conc<50%", "dd_lo": -999e9, "dd_hi": -2e9, "conc_max": 50, "dir": "long"},
    {"name": "Short: DD>+2B & Conc<50%", "dd_lo": 2e9, "dd_hi": 999e9, "conc_max": 50, "dir": "short"},
    {"name": "Short: DD>+3B & Conc<60%", "dd_lo": 3e9, "dd_hi": 999e9, "conc_max": 60, "dir": "short"},
]

for cfg in combo_configs:
    p(f"\n--- {cfg['name']} ---")

    # Generate signals
    signals = []
    last_signal = {}
    for sa in snap_analysis:
        if sa["time"] < dtime(10, 0) or sa["time"] > dtime(14, 30):
            continue
        if sa["concentration"] > cfg["conc_max"]:
            continue
        if not (cfg["dd_lo"] <= sa["total_dd"] <= cfg["dd_hi"]):
            continue

        key = (sa["date"], cfg["dir"])
        if key in last_signal:
            elapsed = (sa["ts"] - last_signal[key]).total_seconds() / 60
            if elapsed < 30:
                continue
        last_signal[key] = sa["ts"]

        signals.append({
            "date": sa["date"], "time": sa["time"], "ts": sa["ts"],
            "ts_et": sa["ts_et"], "spot": sa["spot"], "direction": cfg["dir"],
            "total_dd": sa["total_dd"], "concentration": sa["concentration"],
        })

    if not signals:
        p(f"  No signals generated")
        continue

    p(f"  Signals: {len(signals)}")

    # Test each RM
    p(f"  {'RM':>18} {'W':>3} {'L':>3} {'T':>3} {'E':>3} {'WR':>6} {'PnL':>8} {'Avg':>7} {'MaxDD':>7} {'PF':>6}")
    p(f"  {'-'*75}")

    for rm in rm_combos:
        trades = []
        for sig in signals:
            result, pnl, mp, ml, hold, exit_p = simulate_trade(
                sig["date"], sig["time"], sig["spot"], sig["direction"],
                rm["stop"], rm["target"], rm["trail_act"], rm["trail_gap"])
            trades.append({"result": result, "pnl": pnl, "sig": sig})

        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] < 0)
        total_pnl = sum(t["pnl"] for t in trades)
        wr = wins / max(1, wins + losses) * 100
        gross_win = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else 999
        running = 0
        max_dd = 0
        for t in trades:
            running += t["pnl"]
            max_dd = min(max_dd, running)

        p(f"  {rm['name']:>18} {wins:3} {losses:3} {0:3} "
          f"{sum(1 for t in trades if t['result'] in ('EXPIRED','EOD')):3} "
          f"{wr:5.1f}% {total_pnl:+8.1f} {total_pnl/max(1,len(trades)):+7.1f} {max_dd:+7.1f} {pf:6.2f}")

    # Show individual trades for best combo
    if len(signals) <= 20:
        p(f"\n  Individual trades (SL5/T10):")
        for sig in signals:
            result, pnl, mp, ml, hold, exit_p = simulate_trade(
                sig["date"], sig["time"], sig["spot"], sig["direction"],
                5, 10, None, None)
            p(f"    {sig['date']} {sig['time'].strftime('%H:%M')} {sig['direction']:5} "
              f"Spot={sig['spot']:.1f} DD={sig['total_dd']/1e9:+.1f}B Conc={sig['concentration']:.0f}% "
              f"→ {result} PnL={pnl:+.1f} MaxP={mp:+.1f} MaxL={ml:+.1f} Hold={hold:.0f}m")


# ============================================================
# SETUP D: Pure Concentration Breakout
# ============================================================
p("\n\n" + "=" * 140)
p("SETUP D: DD CONCENTRATION DROP — Breakout Signal")
p("=" * 140)
p("Entry: When DD concentration drops by >15% in ~4 min (DD dispersing = volatility incoming)")
p("Direction: determined by DD total sign (negative = long bounce, positive = short fade)")

conc_drop_signals = []
last_conc_signal = {}
for i in range(2, len(snap_analysis)):
    curr = snap_analysis[i]
    prev = snap_analysis[i-2]  # ~4 min back

    if curr["date"] != prev["date"]:
        continue
    if curr["time"] < dtime(10, 0) or curr["time"] > dtime(14, 30):
        continue

    conc_drop = prev["concentration"] - curr["concentration"]
    if conc_drop < 15:  # Need 15%+ drop
        continue

    # Direction from DD total
    if curr["total_dd"] < -1e9:
        direction = "long"
    elif curr["total_dd"] > 1e9:
        direction = "short"
    else:
        continue  # Skip neutral DD

    key = (curr["date"], direction)
    if key in last_conc_signal:
        elapsed = (curr["ts"] - last_conc_signal[key]).total_seconds() / 60
        if elapsed < 30:
            continue
    last_conc_signal[key] = curr["ts"]

    conc_drop_signals.append({
        "date": curr["date"], "time": curr["time"], "ts": curr["ts"],
        "ts_et": curr["ts_et"], "spot": curr["spot"], "direction": direction,
        "total_dd": curr["total_dd"], "concentration": curr["concentration"],
        "conc_drop": conc_drop, "prev_conc": prev["concentration"],
    })

p(f"Concentration drop signals: {len(conc_drop_signals)}")
if conc_drop_signals:
    p(f"\n{'RM':>18} {'W':>3} {'L':>3} {'E':>3} {'WR':>6} {'PnL':>8} {'Avg':>7} {'MaxDD':>7} {'PF':>6}")
    p(f"{'-'*70}")

    for rm in rm_combos:
        trades = []
        for sig in conc_drop_signals:
            result, pnl, mp, ml, hold, exit_p = simulate_trade(
                sig["date"], sig["time"], sig["spot"], sig["direction"],
                rm["stop"], rm["target"], rm["trail_act"], rm["trail_gap"])
            trades.append({"result": result, "pnl": pnl})

        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] < 0)
        total_pnl = sum(t["pnl"] for t in trades)
        wr = wins / max(1, wins + losses) * 100
        gross_win = sum(t["pnl"] for t in trades if t["pnl"] > 0)
        gross_loss = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else 999
        running = 0
        max_dd = 0
        for t in trades:
            running += t["pnl"]
            max_dd = min(max_dd, running)

        p(f"{rm['name']:>18} {wins:3} {losses:3} "
          f"{sum(1 for t in trades if t['result'] in ('EXPIRED','EOD')):3} "
          f"{wr:5.1f}% {total_pnl:+8.1f} {total_pnl/max(1,len(trades)):+7.1f} {max_dd:+7.1f} {pf:6.2f}")

    if len(conc_drop_signals) <= 25:
        p(f"\n  Individual trades (SL5/T10):")
        for sig in conc_drop_signals:
            result, pnl, mp, ml, hold, exit_p = simulate_trade(
                sig["date"], sig["time"], sig["spot"], sig["direction"],
                5, 10, None, None)
            p(f"    {sig['date']} {sig['time'].strftime('%H:%M')} {sig['direction']:5} "
              f"Spot={sig['spot']:.1f} DD={sig['total_dd']/1e9:+.1f}B "
              f"Conc={sig['prev_conc']:.0f}%→{sig['concentration']:.0f}% (drop={sig['conc_drop']:.0f}%) "
              f"→ {result} PnL={pnl:+.1f}")


# ============================================================
# SUMMARY
# ============================================================
p("\n\n" + "=" * 140)
p("SUMMARY OF ALL SETUPS")
p("=" * 140)

p(f"\nBest LONG standalone: {best_long['config']} → {best_long['pnl']:+.1f} pts")
p(f"Best SHORT standalone: {best_short['config']} → {best_short['pnl']:+.1f} pts")
p(f"Concentration drop signals: {len(conc_drop_signals)}")

cur.close()
conn.close()

with open("tmp_dd_filter_output.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(out))
print(f"Done. {len(out)} lines -> tmp_dd_filter_output.txt")
