"""VIX Divergence Trail Optimization Study — Full sweep for longs AND shorts."""
import os, sqlalchemy
from datetime import timedelta

DB_URL = os.environ.get("DB_URL", "").strip()
if not DB_URL:
    with open("/tmp/db_url.txt") as f:
        DB_URL = f.read().strip()

engine = sqlalchemy.create_engine(DB_URL)

# Get ALL VIX Divergence trades
with engine.connect() as c:
    trades = c.execute(sqlalchemy.text("""
        SELECT id, ts, direction, spot, outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss, exit_price, outcome_elapsed_min,
               grade, score, vix, greek_alignment
        FROM setup_log
        WHERE setup_name = :sn AND outcome_result IS NOT NULL
        ORDER BY id
    """), {"sn": "VIX Divergence"}).mappings().all()

print(f"Total VIX Divergence trades: {len(trades)}")
longs = [t for t in trades if t["direction"] == "long"]
shorts = [t for t in trades if t["direction"] == "short"]
print(f"Longs: {len(longs)}, Shorts: {len(shorts)}")
print()

CONFIRM_OFFSET = 1.5

# ── Config grid ──
# SL x mode x activation x gap x be_trigger
sl_values = [6, 8, 10, 12]
activations = [0, 3, 5, 8, 10, 12, 15]
gaps = [5, 6, 8, 10]
be_triggers = [6, 8, 10]

configs = []

# Continuous configs
for sl in sl_values:
    for act in activations:
        for gap in gaps:
            if gap > sl + 4:
                continue  # gap wider than SL+4 makes no sense
            configs.append({
                "name": f"cont SL={sl} a={act} g={gap}",
                "mode": "continuous", "activation": act, "gap": gap, "sl": sl,
            })

# Hybrid configs
for sl in sl_values:
    for be in be_triggers:
        for act in activations:
            if act <= be:
                continue  # activation must be > be_trigger
            for gap in gaps:
                if gap > sl + 4:
                    continue
                configs.append({
                    "name": f"hyb SL={sl} be={be} a={act} g={gap}",
                    "mode": "hybrid", "be_trigger": be, "activation": act, "gap": gap, "sl": sl,
                })

print(f"Testing {len(configs)} trail configs x 2 directions")
print()


def simulate_trade(trade, cfg):
    """Simulate one trade with given trail config. Returns (outcome, pnl, mfe)."""
    signal_ts = trade["ts"]
    signal_spot = float(trade["spot"])
    direction = trade["direction"]
    is_long = direction == "long"
    sl = cfg["sl"]

    confirm_price = signal_spot + CONFIRM_OFFSET if is_long else signal_spot - CONFIRM_OFFSET

    with engine.connect() as c:
        bars = c.execute(sqlalchemy.text("""
            SELECT ts, bar_high, bar_low, bar_close FROM spx_ohlc_1m
            WHERE ts >= :start AND ts <= :end
            ORDER BY ts
        """), {
            "start": signal_ts - timedelta(minutes=2),
            "end": signal_ts + timedelta(hours=6),
        }).mappings().all()

    if len(bars) < 2:
        return "SKIP", 0, 0

    # Phase 1: find confirmation
    entry_price = None
    entry_idx = 0
    for i, bar in enumerate(bars):
        if is_long and float(bar["bar_high"]) >= confirm_price:
            entry_price = confirm_price
            entry_idx = i
            break
        elif not is_long and float(bar["bar_low"]) <= confirm_price:
            entry_price = confirm_price
            entry_idx = i
            break

    if entry_price is None:
        return "TIMEOUT", 0, 0

    # Phase 2: simulate trail
    remaining = bars[entry_idx:]
    stop_lvl = entry_price - sl if is_long else entry_price + sl
    max_fav = 0.0

    for bar in remaining:
        bh = float(bar["bar_high"])
        bl = float(bar["bar_low"])

        fav = (bh - entry_price) if is_long else (entry_price - bl)
        if fav > max_fav:
            max_fav = fav

        # Trail logic
        trail_lock = None
        if cfg["mode"] == "continuous":
            if max_fav >= cfg["activation"]:
                trail_lock = max_fav - cfg["gap"]
        elif cfg["mode"] == "hybrid":
            if max_fav >= cfg["activation"]:
                trail_lock = max_fav - cfg["gap"]
            elif max_fav >= cfg["be_trigger"]:
                trail_lock = 0

        if trail_lock is not None:
            if is_long:
                ns = entry_price + trail_lock
                if ns > stop_lvl:
                    stop_lvl = ns
            else:
                ns = entry_price - trail_lock
                if ns < stop_lvl:
                    stop_lvl = ns

        # Stop check
        if is_long and bl <= stop_lvl:
            pnl = stop_lvl - entry_price
            return ("WIN" if pnl >= 0 else "LOSS"), round(pnl, 1), round(max_fav, 1)
        elif not is_long and bh >= stop_lvl:
            pnl = entry_price - stop_lvl
            return ("WIN" if pnl >= 0 else "LOSS"), round(pnl, 1), round(max_fav, 1)

    # EOD
    last_close = float(remaining[-1]["bar_close"])
    pnl = (last_close - entry_price) if is_long else (entry_price - last_close)
    return "EXPIRED", round(pnl, 1), round(max_fav, 1)


# ── Run simulation ──
# Pre-fetch bars for each trade to avoid repeated DB queries
print("Pre-fetching 1-min bars for each trade...")
trade_bars = {}
for trade in trades:
    tid = trade["id"]
    signal_ts = trade["ts"]
    with engine.connect() as c:
        bars = c.execute(sqlalchemy.text("""
            SELECT ts, bar_high, bar_low, bar_close FROM spx_ohlc_1m
            WHERE ts >= :start AND ts <= :end
            ORDER BY ts
        """), {
            "start": signal_ts - timedelta(minutes=2),
            "end": signal_ts + timedelta(hours=6),
        }).mappings().all()
    trade_bars[tid] = bars
    print(f"  #{tid} {trade['direction']} {str(signal_ts)[:16]}: {len(bars)} bars")


def simulate_fast(trade, cfg, bars):
    """Simulate using pre-fetched bars."""
    signal_spot = float(trade["spot"])
    direction = trade["direction"]
    is_long = direction == "long"
    sl = cfg["sl"]

    confirm_price = signal_spot + CONFIRM_OFFSET if is_long else signal_spot - CONFIRM_OFFSET

    if len(bars) < 2:
        return "SKIP", 0, 0

    entry_price = None
    entry_idx = 0
    for i, bar in enumerate(bars):
        if is_long and float(bar["bar_high"]) >= confirm_price:
            entry_price = confirm_price
            entry_idx = i
            break
        elif not is_long and float(bar["bar_low"]) <= confirm_price:
            entry_price = confirm_price
            entry_idx = i
            break

    if entry_price is None:
        return "TIMEOUT", 0, 0

    remaining = bars[entry_idx:]
    stop_lvl = entry_price - sl if is_long else entry_price + sl
    max_fav = 0.0

    for bar in remaining:
        bh = float(bar["bar_high"])
        bl = float(bar["bar_low"])

        fav = (bh - entry_price) if is_long else (entry_price - bl)
        if fav > max_fav:
            max_fav = fav

        trail_lock = None
        if cfg["mode"] == "continuous":
            if max_fav >= cfg["activation"]:
                trail_lock = max_fav - cfg["gap"]
        elif cfg["mode"] == "hybrid":
            if max_fav >= cfg["activation"]:
                trail_lock = max_fav - cfg["gap"]
            elif max_fav >= cfg["be_trigger"]:
                trail_lock = 0

        if trail_lock is not None:
            if is_long:
                ns = entry_price + trail_lock
                if ns > stop_lvl:
                    stop_lvl = ns
            else:
                ns = entry_price - trail_lock
                if ns < stop_lvl:
                    stop_lvl = ns

        if is_long and bl <= stop_lvl:
            pnl = stop_lvl - entry_price
            return ("WIN" if pnl >= 0 else "LOSS"), round(pnl, 1), round(max_fav, 1)
        elif not is_long and bh >= stop_lvl:
            pnl = entry_price - stop_lvl
            return ("WIN" if pnl >= 0 else "LOSS"), round(pnl, 1), round(max_fav, 1)

    last_close = float(remaining[-1]["bar_close"])
    pnl = (last_close - entry_price) if is_long else (entry_price - last_close)
    return "EXPIRED", round(pnl, 1), round(max_fav, 1)


# Run all configs
print(f"\nRunning {len(configs)} configs across {len(trades)} trades...\n")

long_results = {}  # config_name -> [(outcome, pnl, mfe, trade_id)]
short_results = {}

for cfg in configs:
    lr = []
    sr = []
    for trade in trades:
        tid = trade["id"]
        direction = trade["direction"]
        bars = trade_bars[tid]
        outcome, pnl, mfe = simulate_fast(trade, cfg, bars)
        if direction == "long":
            lr.append((outcome, pnl, mfe, tid))
        else:
            sr.append((outcome, pnl, mfe, tid))
    long_results[cfg["name"]] = lr
    short_results[cfg["name"]] = sr


def summarize(data, label=""):
    """Return summary dict for a list of (outcome, pnl, mfe, tid)."""
    resolved = [(o, p, m, t) for o, p, m, t in data if o in ("WIN", "LOSS", "EXPIRED")]
    if not resolved:
        return None
    wins = sum(1 for o, p, m, t in resolved if o == "WIN")
    losses = sum(1 for o, p, m, t in resolved if o == "LOSS")
    expired = sum(1 for o, p, m, t in resolved if o == "EXPIRED")
    total_pnl = sum(p for o, p, m, t in resolved)
    wr = wins / len(resolved) * 100
    max_dd = min(p for o, p, m, t in resolved)
    neg_sum = sum(p for o, p, m, t in resolved if p < 0)
    pos_sum = sum(p for o, p, m, t in resolved if p > 0)
    pf = abs(pos_sum / neg_sum) if neg_sum != 0 else 999
    avg_w = pos_sum / wins if wins else 0
    avg_l = neg_sum / losses if losses else 0
    return {
        "wins": wins, "losses": losses, "expired": expired,
        "wr": wr, "pnl": total_pnl, "max_dd": max_dd,
        "pf": pf, "avg_w": avg_w, "avg_l": avg_l,
        "trades": resolved,
    }


# ── Print LONGS top 25 ──
print("=" * 120)
print("TOP 25 LONGS (sorted by PnL)")
print("=" * 120)

long_summaries = []
for cfg in configs:
    data = long_results[cfg["name"]]
    s = summarize(data)
    if s and len(s["trades"]) > 0:
        long_summaries.append((cfg["name"], s))

long_summaries.sort(key=lambda x: x[1]["pnl"], reverse=True)

print(f"{'Config':<38} {'W/L/E':>8} {'WR':>5} {'PnL':>7} {'MaxDD':>7} {'AvgW':>6} {'AvgL':>6} {'PF':>5}  Per-trade")
print("-" * 120)
for name, s in long_summaries[:25]:
    wle = f"{s['wins']}W/{s['losses']}L/{s['expired']}E"
    per_trade = " ".join(f"{o[0]}{p:+.0f}" for o, p, m, t in s["trades"])
    print(f"{name:<38} {wle:>8} {s['wr']:>4.0f}% {s['pnl']:>+6.1f} {s['max_dd']:>+6.1f} {s['avg_w']:>+5.1f} {s['avg_l']:>+5.1f} {s['pf']:>4.1f}x  {per_trade}")

# Current long config
print("\n--- CURRENT LONG CONFIG ---")
for name, s in long_summaries:
    if "cont" in name and "a=0" in name and "g=8" in name and "SL=8" in name:
        wle = f"{s['wins']}W/{s['losses']}L/{s['expired']}E"
        per_trade = " ".join(f"{o[0]}{p:+.0f}" for o, p, m, t in s["trades"])
        print(f"{name:<38} {wle:>8} {s['wr']:>4.0f}% {s['pnl']:>+6.1f} {s['max_dd']:>+6.1f} {s['avg_w']:>+5.1f} {s['avg_l']:>+5.1f} {s['pf']:>4.1f}x  {per_trade}")

# ── Print SHORTS top 25 ──
print()
print("=" * 120)
print("TOP 25 SHORTS (sorted by PnL)")
print("=" * 120)

short_summaries = []
for cfg in configs:
    data = short_results[cfg["name"]]
    s = summarize(data)
    if s and len(s["trades"]) > 0:
        short_summaries.append((cfg["name"], s))

short_summaries.sort(key=lambda x: x[1]["pnl"], reverse=True)

print(f"{'Config':<38} {'W/L/E':>8} {'WR':>5} {'PnL':>7} {'MaxDD':>7} {'AvgW':>6} {'AvgL':>6} {'PF':>5}  Per-trade")
print("-" * 120)
for name, s in short_summaries[:25]:
    wle = f"{s['wins']}W/{s['losses']}L/{s['expired']}E"
    per_trade = " ".join(f"{o[0]}{p:+.0f}" for o, p, m, t in s["trades"])
    print(f"{name:<38} {wle:>8} {s['wr']:>4.0f}% {s['pnl']:>+6.1f} {s['max_dd']:>+6.1f} {s['avg_w']:>+5.1f} {s['avg_l']:>+5.1f} {s['pf']:>4.1f}x  {per_trade}")

# Current short config
print("\n--- CURRENT SHORT CONFIG ---")
for name, s in short_summaries:
    if "hyb" in name and "be=8" in name and "a=10" in name and "g=5" in name and "SL=8" in name:
        wle = f"{s['wins']}W/{s['losses']}L/{s['expired']}E"
        per_trade = " ".join(f"{o[0]}{p:+.0f}" for o, p, m, t in s["trades"])
        print(f"{name:<38} {wle:>8} {s['wr']:>4.0f}% {s['pnl']:>+6.1f} {s['max_dd']:>+6.1f} {s['avg_w']:>+5.1f} {s['avg_l']:>+5.1f} {s['pf']:>4.1f}x  {per_trade}")

# ── Best overall per direction ──
print()
print("=" * 120)
print("RECOMMENDED CONFIGS")
print("=" * 120)

if long_summaries:
    best_l = long_summaries[0]
    print(f"LONGS BEST PnL:  {best_l[0]} -> PnL={best_l[1]['pnl']:+.1f} WR={best_l[1]['wr']:.0f}%")
    # Best by WR (min 3 resolved)
    wr_sorted = sorted(long_summaries, key=lambda x: (x[1]["wr"], x[1]["pnl"]), reverse=True)
    best_wr = wr_sorted[0]
    print(f"LONGS BEST WR:   {best_wr[0]} -> WR={best_wr[1]['wr']:.0f}% PnL={best_wr[1]['pnl']:+.1f}")
    # Best by PnL/MaxDD ratio (risk-adjusted)
    risk_sorted = sorted(long_summaries, key=lambda x: x[1]["pnl"] / abs(x[1]["max_dd"]) if x[1]["max_dd"] < 0 else x[1]["pnl"] * 10, reverse=True)
    best_risk = risk_sorted[0]
    print(f"LONGS BEST RISK: {best_risk[0]} -> PnL={best_risk[1]['pnl']:+.1f} MaxDD={best_risk[1]['max_dd']:+.1f}")

if short_summaries:
    best_s = short_summaries[0]
    print(f"SHORTS BEST PnL: {best_s[0]} -> PnL={best_s[1]['pnl']:+.1f} WR={best_s[1]['wr']:.0f}%")
    wr_sorted = sorted(short_summaries, key=lambda x: (x[1]["wr"], x[1]["pnl"]), reverse=True)
    best_wr = wr_sorted[0]
    print(f"SHORTS BEST WR:  {best_wr[0]} -> WR={best_wr[1]['wr']:.0f}% PnL={best_wr[1]['pnl']:+.1f}")
    risk_sorted = sorted(short_summaries, key=lambda x: x[1]["pnl"] / abs(x[1]["max_dd"]) if x[1]["max_dd"] < 0 else x[1]["pnl"] * 10, reverse=True)
    best_risk = risk_sorted[0]
    print(f"SHORTS BEST RISK: {best_risk[0]} -> PnL={best_risk[1]['pnl']:+.1f} MaxDD={best_risk[1]['max_dd']:+.1f}")
