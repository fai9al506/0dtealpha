"""
SB2 Absorption Trail Bug Study
Simulates different exit strategies using ES 5-pt range bars.
"""
import psycopg2
import json
from collections import defaultdict
from datetime import datetime, time as dtime

import os
_tmp = os.environ.get("TEMP", "/tmp")
DB_URL = open(os.path.join(_tmp, "db_url.txt")).read().strip()

def get_trades():
    """Fetch all SB2 Absorption trades with bar data."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, setup_log.ts, setup_log.ts::date as trade_date, direction, grade,
               abs_es_price, outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               abs_details, spot
        FROM setup_log
        WHERE setup_name = 'SB2 Absorption'
        ORDER BY setup_log.ts
    """)
    trades = []
    for r in cur.fetchall():
        det = r[10] or {}
        bar_idx = det.get("bar_idx")
        trades.append({
            "id": r[0],
            "ts": r[1],
            "trade_date": r[2],
            "direction": r[3],
            "grade": r[4],
            "es_price": r[5],
            "db_outcome": r[6],
            "db_pnl": r[7],
            "db_mfe": r[8],
            "db_mae": r[9],
            "bar_idx": bar_idx,
            "spot": r[11],
        })
    conn.close()
    return trades


def get_range_bars(trade_date):
    """Fetch 5-pt rithmic range bars for a given date."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume, ts_start, ts_end
        FROM es_range_bars
        WHERE trade_date = %s AND source = 'rithmic' AND range_pts = 5.0
        ORDER BY bar_idx
    """, (trade_date,))
    bars = []
    for r in cur.fetchall():
        bars.append({
            "idx": r[0],
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5],
            "ts_start": r[6],
            "ts_end": r[7],
        })
    conn.close()
    return bars


def find_entry_bar_idx(bars, es_price, trade_ts):
    """For trades without bar_idx, find the bar closest in time and price."""
    best = None
    best_dist = float("inf")
    for bar in bars:
        if bar["ts_end"] is None:
            continue
        # Bar must have ended before or around trade time
        if bar["ts_end"] > trade_ts:
            # This bar ended after the trade — could be the entry bar
            # Check if price is within bar range
            if bar["low"] <= es_price <= bar["high"]:
                time_dist = abs((bar["ts_end"] - trade_ts).total_seconds())
                if time_dist < best_dist:
                    best_dist = time_dist
                    best = bar["idx"]
            continue
        # Bar ended before trade — check if the close matches
        price_dist = abs(bar["close"] - es_price)
        time_dist = abs((bar["ts_end"] - trade_ts).total_seconds())
        combined = price_dist * 10 + time_dist  # weight price more
        if combined < best_dist:
            best_dist = combined
            best = bar["idx"]
    return best


def simulate_trade(bars, entry_bar_idx, es_price, direction, config):
    """
    Simulate a single trade through range bars.

    Config options:
      mode: "fixed" | "trail" | "split"
      sl: stop loss pts
      target: fixed target pts (for "fixed" mode)
      trail_mode: "hybrid" | "continuous"
      be_trigger: breakeven trigger pts (hybrid only)
      activation: trail activation pts
      gap: trail gap pts
      split_t1: first target pts (for "split" mode)
    """
    is_long = direction == "bullish"
    mode = config["mode"]
    sl = config["sl"]

    stop_lvl = (es_price - sl) if is_long else (es_price + sl)
    initial_stop = stop_lvl

    # Fixed target level
    if mode == "fixed":
        target_lvl = (es_price + config["target"]) if is_long else (es_price - config["target"])
    elif mode == "split":
        target_lvl = (es_price + config["split_t1"]) if is_long else (es_price - config["split_t1"])
    else:
        target_lvl = None  # trail only

    max_fav = 0.0
    seen_high = es_price
    seen_low = es_price
    t1_hit = False
    exit_bar = None

    # For split mode: after T1 hit, switch to trail-only for remainder
    in_trail_phase = (mode == "trail")

    for bar in bars:
        bidx = bar["idx"]
        if bidx <= entry_bar_idx:
            continue

        bh = bar["high"]
        bl = bar["low"]
        if bh is None or bl is None:
            continue

        seen_high = max(seen_high, bh)
        seen_low = min(seen_low, bl)

        # Calculate current favorable excursion
        fav_px = seen_high if is_long else seen_low
        fav = (fav_px - es_price) if is_long else (es_price - fav_px)
        if fav > max_fav:
            max_fav = fav

        # Trail advancement
        if mode == "trail" or (mode == "split" and t1_hit):
            trail_cfg = config
            trail_lock = None
            if trail_cfg.get("trail_mode") == "continuous":
                if max_fav >= trail_cfg["activation"]:
                    trail_lock = max_fav - trail_cfg["gap"]
            elif trail_cfg.get("trail_mode") == "hybrid":
                if max_fav >= trail_cfg["activation"]:
                    trail_lock = max_fav - trail_cfg["gap"]
                elif max_fav >= trail_cfg["be_trigger"]:
                    trail_lock = 0

            if trail_lock is not None:
                if is_long:
                    ns = es_price + trail_lock
                    if ns > stop_lvl:
                        stop_lvl = ns
                else:
                    ns = es_price - trail_lock
                    if ns < stop_lvl:
                        stop_lvl = ns

        # Stop check
        if is_long and bl <= stop_lvl:
            pnl = stop_lvl - es_price
            result = "WIN" if pnl >= 0 else "LOSS"
            return {
                "result": result, "pnl": round(pnl, 2), "mfe": round(max_fav, 2),
                "mae": round(min(0, (seen_low - es_price) if is_long else (es_price - seen_high)), 2),
                "exit_bar": bidx, "exit_type": "trail_stop" if stop_lvl != initial_stop else "stop"
            }
        if not is_long and bh >= stop_lvl:
            pnl = es_price - stop_lvl
            result = "WIN" if pnl >= 0 else "LOSS"
            return {
                "result": result, "pnl": round(pnl, 2), "mfe": round(max_fav, 2),
                "mae": round(min(0, (seen_low - es_price) if is_long else (es_price - seen_high)), 2),
                "exit_bar": bidx, "exit_type": "trail_stop" if stop_lvl != initial_stop else "stop"
            }

        # Fixed target check
        if target_lvl is not None and not t1_hit:
            if is_long and bh >= target_lvl:
                if mode == "split":
                    t1_hit = True
                    # T1 hit — now switch to trail for remainder
                    # Reset stop to breakeven after T1
                    stop_lvl = max(stop_lvl, es_price)
                    continue
                else:
                    pnl = config["target"]
                    return {
                        "result": "WIN", "pnl": round(pnl, 2), "mfe": round(max_fav, 2),
                        "mae": round(min(0, seen_low - es_price), 2),
                        "exit_bar": bidx, "exit_type": "target"
                    }
            if not is_long and bl <= target_lvl:
                if mode == "split":
                    t1_hit = True
                    stop_lvl = min(stop_lvl, es_price)
                    continue
                else:
                    pnl = config["target"]
                    return {
                        "result": "WIN", "pnl": round(pnl, 2), "mfe": round(max_fav, 2),
                        "mae": round(min(0, es_price - seen_high), 2),
                        "exit_bar": bidx, "exit_type": "target"
                    }

    # End of day — expired
    # Use last bar's close for P&L
    if bars:
        last_bar = bars[-1]
        close = last_bar["close"]
        pnl = (close - es_price) if is_long else (es_price - close)
    else:
        pnl = 0

    return {
        "result": "EXPIRED", "pnl": round(pnl, 2), "mfe": round(max_fav, 2),
        "mae": round(min(0, (seen_low - es_price) if is_long else (es_price - seen_high)), 2),
        "exit_bar": None, "exit_type": "expired"
    }


def simulate_split_trade(bars, entry_bar_idx, es_price, direction, config):
    """
    Split target simulation: 50% at T1, 50% trails.
    Returns combined PnL as average of both legs.
    """
    is_long = direction == "bullish"
    sl = config["sl"]
    t1_pts = config["split_t1"]

    stop_lvl = (es_price - sl) if is_long else (es_price + sl)
    initial_stop = stop_lvl
    trail_stop = stop_lvl  # separate trail for leg 2

    target_lvl = (es_price + t1_pts) if is_long else (es_price - t1_pts)

    max_fav = 0.0
    seen_high = es_price
    seen_low = es_price
    t1_hit = False
    leg1_pnl = None
    leg2_pnl = None

    for bar in bars:
        bidx = bar["idx"]
        if bidx <= entry_bar_idx:
            continue

        bh = bar["high"]
        bl = bar["low"]
        if bh is None or bl is None:
            continue

        seen_high = max(seen_high, bh)
        seen_low = min(seen_low, bl)

        fav_px = seen_high if is_long else seen_low
        fav = (fav_px - es_price) if is_long else (es_price - fav_px)
        if fav > max_fav:
            max_fav = fav

        # Trail advancement (for leg 2 after T1, or for both if no T1 yet)
        if t1_hit:
            trail_cfg = config
            trail_lock = None
            if trail_cfg.get("trail_mode") == "continuous":
                if max_fav >= trail_cfg["activation"]:
                    trail_lock = max_fav - trail_cfg["gap"]
            elif trail_cfg.get("trail_mode") == "hybrid":
                if max_fav >= trail_cfg["activation"]:
                    trail_lock = max_fav - trail_cfg["gap"]
                elif max_fav >= trail_cfg["be_trigger"]:
                    trail_lock = 0

            if trail_lock is not None:
                if is_long:
                    ns = es_price + trail_lock
                    if ns > trail_stop:
                        trail_stop = ns
                else:
                    ns = es_price - trail_lock
                    if ns < trail_stop:
                        trail_stop = ns

        # Stop check — affects both legs if T1 not hit, only leg 2 if T1 hit
        active_stop = trail_stop if t1_hit else stop_lvl
        if is_long and bl <= active_stop:
            if not t1_hit:
                # Both legs stopped out
                pnl = active_stop - es_price
                return {
                    "result": "WIN" if pnl >= 0 else "LOSS",
                    "pnl": round(pnl, 2), "mfe": round(max_fav, 2),
                    "mae": round(min(0, seen_low - es_price), 2),
                    "exit_bar": bidx, "exit_type": "stop",
                    "t1_hit": False
                }
            else:
                # Leg 2 stopped out — combine with T1
                leg2_pnl = trail_stop - es_price
                combined = (leg1_pnl + leg2_pnl) / 2
                return {
                    "result": "WIN" if combined >= 0 else "LOSS",
                    "pnl": round(combined, 2), "mfe": round(max_fav, 2),
                    "mae": round(min(0, seen_low - es_price), 2),
                    "exit_bar": bidx, "exit_type": "split_trail",
                    "t1_hit": True, "leg1": round(leg1_pnl, 2), "leg2": round(leg2_pnl, 2)
                }
        if not is_long and bh >= active_stop:
            if not t1_hit:
                pnl = es_price - active_stop
                return {
                    "result": "WIN" if pnl >= 0 else "LOSS",
                    "pnl": round(pnl, 2), "mfe": round(max_fav, 2),
                    "mae": round(min(0, es_price - seen_high), 2),
                    "exit_bar": bidx, "exit_type": "stop",
                    "t1_hit": False
                }
            else:
                leg2_pnl = es_price - trail_stop
                combined = (leg1_pnl + leg2_pnl) / 2
                return {
                    "result": "WIN" if combined >= 0 else "LOSS",
                    "pnl": round(combined, 2), "mfe": round(max_fav, 2),
                    "mae": round(min(0, es_price - seen_high), 2),
                    "exit_bar": bidx, "exit_type": "split_trail",
                    "t1_hit": True, "leg1": round(leg1_pnl, 2), "leg2": round(leg2_pnl, 2)
                }

        # T1 target check
        if not t1_hit and target_lvl is not None:
            if is_long and bh >= target_lvl:
                t1_hit = True
                leg1_pnl = t1_pts
                # Move trail stop to breakeven
                trail_stop = max(trail_stop, es_price)
                continue
            if not is_long and bl <= target_lvl:
                t1_hit = True
                leg1_pnl = t1_pts
                trail_stop = min(trail_stop, es_price)
                continue

    # End of day
    if bars:
        close = bars[-1]["close"]
        eod_pnl = (close - es_price) if is_long else (es_price - close)
    else:
        eod_pnl = 0

    if t1_hit:
        combined = (leg1_pnl + eod_pnl) / 2
    else:
        combined = eod_pnl

    return {
        "result": "EXPIRED", "pnl": round(combined, 2), "mfe": round(max_fav, 2),
        "mae": round(min(0, (seen_low - es_price) if is_long else (es_price - seen_high)), 2),
        "exit_bar": None, "exit_type": "expired",
        "t1_hit": t1_hit
    }


def run_configs():
    """Run all configurations."""
    trades = get_trades()
    print(f"Total SB2 trades: {len(trades)}")

    # Cache range bars by date
    bar_cache = {}
    dates = set(t["trade_date"] for t in trades)
    for d in dates:
        bar_cache[d] = get_range_bars(d)
        print(f"  Loaded {len(bar_cache[d])} bars for {d}")

    # Resolve missing bar_idx
    skipped = 0
    for t in trades:
        if t["bar_idx"] is None:
            bars = bar_cache.get(t["trade_date"], [])
            if bars:
                found = find_entry_bar_idx(bars, t["es_price"], t["ts"])
                if found is not None:
                    t["bar_idx"] = found
                else:
                    skipped += 1
            else:
                skipped += 1

    print(f"Resolved bar_idx for trades without it. Skipped: {skipped}")

    # Filter to trades with valid bar_idx
    valid_trades = [t for t in trades if t["bar_idx"] is not None]
    print(f"Valid trades for simulation: {len(valid_trades)}")

    # Define configs
    configs = {}

    # 1. Fixed target only (no trail)
    for sl, tgt in [(8, 10), (8, 12), (8, 15), (10, 12), (10, 15), (12, 15), (8, 8), (10, 10), (12, 12), (8, 20), (10, 20)]:
        name = f"Fixed SL={sl}/T={tgt}"
        configs[name] = {"mode": "fixed", "sl": sl, "target": tgt}

    # 2. Trail only (no fixed target)
    trail_configs = [
        ("Trail hybrid be=10 act=20 gap=10 SL=8 [CURRENT]", {"mode": "trail", "sl": 8, "trail_mode": "hybrid", "be_trigger": 10, "activation": 20, "gap": 10}),
        ("Trail hybrid be=10 act=20 gap=10 SL=12", {"mode": "trail", "sl": 12, "trail_mode": "hybrid", "be_trigger": 10, "activation": 20, "gap": 10}),
        ("Trail hybrid be=10 act=15 gap=5 SL=8", {"mode": "trail", "sl": 8, "trail_mode": "hybrid", "be_trigger": 10, "activation": 15, "gap": 5}),
        ("Trail hybrid be=10 act=15 gap=5 SL=12", {"mode": "trail", "sl": 12, "trail_mode": "hybrid", "be_trigger": 10, "activation": 15, "gap": 5}),
        ("Trail hybrid be=8 act=12 gap=5 SL=8", {"mode": "trail", "sl": 8, "trail_mode": "hybrid", "be_trigger": 8, "activation": 12, "gap": 5}),
        ("Trail hybrid be=8 act=12 gap=5 SL=10", {"mode": "trail", "sl": 10, "trail_mode": "hybrid", "be_trigger": 8, "activation": 12, "gap": 5}),
        ("Trail hybrid be=10 act=20 gap=5 SL=8", {"mode": "trail", "sl": 8, "trail_mode": "hybrid", "be_trigger": 10, "activation": 20, "gap": 5}),
        ("Trail hybrid be=10 act=20 gap=5 SL=12", {"mode": "trail", "sl": 12, "trail_mode": "hybrid", "be_trigger": 10, "activation": 20, "gap": 5}),
        ("Trail cont act=0 gap=8 SL=8", {"mode": "trail", "sl": 8, "trail_mode": "continuous", "activation": 0, "gap": 8}),
        ("Trail cont act=0 gap=8 SL=12", {"mode": "trail", "sl": 12, "trail_mode": "continuous", "activation": 0, "gap": 8}),
        ("Trail cont act=10 gap=8 SL=8", {"mode": "trail", "sl": 8, "trail_mode": "continuous", "activation": 10, "gap": 8}),
        ("Trail cont act=10 gap=8 SL=12", {"mode": "trail", "sl": 12, "trail_mode": "continuous", "activation": 10, "gap": 8}),
        ("Trail cont act=15 gap=5 SL=8", {"mode": "trail", "sl": 8, "trail_mode": "continuous", "activation": 15, "gap": 5}),
        ("Trail cont act=15 gap=5 SL=12", {"mode": "trail", "sl": 12, "trail_mode": "continuous", "activation": 15, "gap": 5}),
        ("Trail cont act=20 gap=5 SL=8", {"mode": "trail", "sl": 8, "trail_mode": "continuous", "activation": 20, "gap": 5}),
        ("Trail cont act=20 gap=5 SL=12", {"mode": "trail", "sl": 12, "trail_mode": "continuous", "activation": 20, "gap": 5}),
        ("Trail cont act=20 gap=8 SL=8", {"mode": "trail", "sl": 8, "trail_mode": "continuous", "activation": 20, "gap": 8}),
        ("Trail cont act=20 gap=8 SL=12", {"mode": "trail", "sl": 12, "trail_mode": "continuous", "activation": 20, "gap": 8}),
        ("Trail hybrid be=10 act=15 gap=8 SL=8", {"mode": "trail", "sl": 8, "trail_mode": "hybrid", "be_trigger": 10, "activation": 15, "gap": 8}),
        ("Trail hybrid be=10 act=15 gap=8 SL=12", {"mode": "trail", "sl": 12, "trail_mode": "hybrid", "be_trigger": 10, "activation": 15, "gap": 8}),
    ]
    for name, cfg in trail_configs:
        configs[name] = cfg

    # 3. Split target: T1 at various pts, then trail
    split_configs = [
        ("Split T1=10 + hybrid be=10 act=20 gap=10 SL=8", {"mode": "split", "sl": 8, "split_t1": 10, "trail_mode": "hybrid", "be_trigger": 10, "activation": 20, "gap": 10}),
        ("Split T1=10 + hybrid be=10 act=15 gap=5 SL=8", {"mode": "split", "sl": 8, "split_t1": 10, "trail_mode": "hybrid", "be_trigger": 10, "activation": 15, "gap": 5}),
        ("Split T1=12 + hybrid be=10 act=20 gap=10 SL=8", {"mode": "split", "sl": 8, "split_t1": 12, "trail_mode": "hybrid", "be_trigger": 10, "activation": 20, "gap": 10}),
        ("Split T1=10 + cont act=15 gap=5 SL=8", {"mode": "split", "sl": 8, "split_t1": 10, "trail_mode": "continuous", "activation": 15, "gap": 5}),
        ("Split T1=10 + cont act=20 gap=5 SL=8", {"mode": "split", "sl": 8, "split_t1": 10, "trail_mode": "continuous", "activation": 20, "gap": 5}),
        ("Split T1=12 + cont act=20 gap=5 SL=8", {"mode": "split", "sl": 8, "split_t1": 12, "trail_mode": "continuous", "activation": 20, "gap": 5}),
        ("Split T1=10 + hybrid be=10 act=20 gap=10 SL=12", {"mode": "split", "sl": 12, "split_t1": 10, "trail_mode": "hybrid", "be_trigger": 10, "activation": 20, "gap": 10}),
        ("Split T1=12 + hybrid be=10 act=20 gap=5 SL=8", {"mode": "split", "sl": 8, "split_t1": 12, "trail_mode": "hybrid", "be_trigger": 10, "activation": 20, "gap": 5}),
        ("Split T1=10 + hybrid be=10 act=20 gap=5 SL=8", {"mode": "split", "sl": 8, "split_t1": 10, "trail_mode": "hybrid", "be_trigger": 10, "activation": 20, "gap": 5}),
    ]
    for name, cfg in split_configs:
        configs[name] = cfg

    # Run all configs
    results = {}
    for cfg_name, cfg in configs.items():
        outcomes = []
        for t in valid_trades:
            bars = bar_cache.get(t["trade_date"], [])
            if not bars:
                continue

            if cfg["mode"] == "split":
                outcome = simulate_split_trade(bars, t["bar_idx"], t["es_price"], t["direction"], cfg)
            else:
                outcome = simulate_trade(bars, t["bar_idx"], t["es_price"], t["direction"], cfg)

            outcome["trade_id"] = t["id"]
            outcome["trade_date"] = t["trade_date"]
            outcome["direction"] = t["direction"]
            outcome["grade"] = t["grade"]
            outcome["es_price"] = t["es_price"]
            outcomes.append(outcome)

        results[cfg_name] = outcomes

    return results, valid_trades


def compute_stats(outcomes):
    """Compute stats from simulation outcomes."""
    if not outcomes:
        return None

    wins = [o for o in outcomes if o["result"] == "WIN"]
    losses = [o for o in outcomes if o["result"] == "LOSS"]
    expired = [o for o in outcomes if o["result"] == "EXPIRED"]

    total = len(outcomes)
    w = len(wins)
    l = len(losses)
    e = len(expired)

    pnls = [o["pnl"] for o in outcomes]
    total_pnl = sum(pnls)

    # Max drawdown (running)
    cumulative = 0
    peak = 0
    max_dd = 0
    for p in pnls:
        cumulative += p
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd

    # Profit factor
    gross_profit = sum(p for p in pnls if p > 0)
    gross_loss = abs(sum(p for p in pnls if p < 0))
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Avg win / avg loss
    avg_win = sum(o["pnl"] for o in wins) / w if w else 0
    avg_loss = sum(o["pnl"] for o in losses) / l if l else 0

    # Win rate
    wr = w / total * 100 if total else 0

    # Avg MFE
    avg_mfe = sum(o["mfe"] for o in outcomes) / total if total else 0

    return {
        "total": total,
        "wins": w,
        "losses": l,
        "expired": e,
        "wr": wr,
        "pnl": total_pnl,
        "max_dd": max_dd,
        "pf": pf,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "avg_mfe": avg_mfe,
        "pnls": pnls,
    }


def main():
    results, valid_trades = run_configs()

    # Compute stats for all configs
    all_stats = []
    for cfg_name, outcomes in results.items():
        stats = compute_stats(outcomes)
        if stats:
            all_stats.append((cfg_name, stats, outcomes))

    # Sort by PnL descending
    all_stats.sort(key=lambda x: x[1]["pnl"], reverse=True)

    # Print comparison table
    print("\n" + "=" * 140)
    print(f"{'Config':<55} {'W/L/E':>8} {'WR%':>6} {'PnL':>8} {'MaxDD':>7} {'PF':>6} {'AvgW':>7} {'AvgL':>7} {'AvgMFE':>7}")
    print("=" * 140)

    for cfg_name, stats, _ in all_stats:
        wle = f"{stats['wins']}/{stats['losses']}/{stats['expired']}"
        pf_str = f"{stats['pf']:.2f}" if stats['pf'] < 100 else "inf"
        print(f"{cfg_name:<55} {wle:>8} {stats['wr']:>5.1f}% {stats['pnl']:>+7.1f} {stats['max_dd']:>6.1f} {pf_str:>6} {stats['avg_win']:>+6.1f} {stats['avg_loss']:>+6.1f} {stats['avg_mfe']:>6.1f}")

    # Print per-trade details for top 5
    print("\n\n" + "=" * 140)
    print("PER-TRADE DETAILS — TOP 5 CONFIGS")
    print("=" * 140)

    for rank, (cfg_name, stats, outcomes) in enumerate(all_stats[:5], 1):
        print(f"\n{'-' * 100}")
        print(f"#{rank}: {cfg_name}")
        print(f"    PnL={stats['pnl']:+.1f} | WR={stats['wr']:.1f}% | MaxDD={stats['max_dd']:.1f} | PF={stats['pf']:.2f}")
        print(f"{'-' * 100}")
        print(f"  {'ID':>6} {'Date':>12} {'Dir':>8} {'Grade':>5} {'Entry':>10} {'Result':>8} {'PnL':>7} {'MFE':>7} {'Exit':>12}")

        running = 0
        for o in outcomes:
            running += o["pnl"]
            print(f"  {o['trade_id']:>6} {str(o['trade_date']):>12} {o['direction']:>8} {o['grade']:>5} "
                  f"{o['es_price']:>10.2f} {o['result']:>8} {o['pnl']:>+6.1f} {o['mfe']:>6.1f} "
                  f"{o['exit_type']:>12}  (cum={running:+.1f})")

    # Comparison: Current effective vs DB actual
    print("\n\n" + "=" * 100)
    print("VERIFICATION: Simulated 'CURRENT' config vs DB actual outcomes")
    print("=" * 100)

    # Find the current config
    current_name = "Trail hybrid be=10 act=20 gap=10 SL=8 [CURRENT]"
    current_outcomes = results.get(current_name, [])

    match = 0
    mismatch = 0
    for o in current_outcomes:
        # Find matching trade
        for t in valid_trades:
            if t["id"] == o["trade_id"]:
                db_res = t["db_outcome"]
                db_pnl = t["db_pnl"]
                sim_res = o["result"]
                sim_pnl = o["pnl"]
                if db_res == sim_res and abs((db_pnl or 0) - sim_pnl) < 1.0:
                    match += 1
                else:
                    mismatch += 1
                    print(f"  MISMATCH ID={t['id']}: DB={db_res}/{db_pnl} vs SIM={sim_res}/{sim_pnl}")
                break

    print(f"\nMatch: {match}/{match+mismatch} ({match/(match+mismatch)*100:.0f}%)")


if __name__ == "__main__":
    main()
