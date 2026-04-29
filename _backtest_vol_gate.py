"""
ES Absorption Volume Gate Backtest
===================================
Tests the impact of lowering/removing the volume gate threshold on ES Absorption signals.

Scenarios:
  1. Current (1.4x) — baseline
  2. Relaxed (1.0x) — medium volume minimum
  3. No gate (0.0x) — all divergence signals fire
  4. Grade by volume bucket — all signals, but tag by volume level

Uses post-March 18 rithmic range bar data only (V12 era).
"""
import os
import sys
from datetime import datetime, date, time as dtime
from collections import defaultdict

import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL env var required")
    sys.exit(1)

# ─── Parameters (match setup_detector.py exactly) ───
LOOKBACK = 8
VOL_WINDOW = 20
MIN_BARS_NEEDED = VOL_WINDOW + LOOKBACK
COOLDOWN_BARS = 10
SL_PTS = 8.0
TARGET_PTS = 10.0
EXPIRY_BARS = 30  # ~2.5 hours
MIN_DATE = date(2026, 3, 18)  # V12 era start
MIN_BARS_PER_DAY = 50

# Divergence thresholds (from evaluate_absorption)
CVD_NORM_THRESHOLD = 0.15
GAP_THRESHOLD = 0.2

# Volume gate scenarios
SCENARIOS = {
    "1.4x (current)": 1.4,
    "1.0x (relaxed)": 1.0,
    "0.0x (no gate)": 0.0,
}


def fetch_bars():
    """Fetch all rithmic range bars, grouped by date."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT trade_date, bar_idx,
               bar_open, bar_high, bar_low, bar_close,
               bar_volume, cumulative_delta AS cvd,
               ts_start, ts_end, status
        FROM es_range_bars
        WHERE source = 'rithmic'
          AND trade_date >= %s
          AND status = 'closed'
        ORDER BY trade_date, bar_idx
    """, (MIN_DATE,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Group by date
    by_date = defaultdict(list)
    for r in rows:
        by_date[r["trade_date"]].append(r)

    # Filter out incomplete days
    valid = {}
    for d, bars in sorted(by_date.items()):
        if len(bars) >= MIN_BARS_PER_DAY:
            # Deduplicate by bar_idx (take last occurrence)
            seen = {}
            for b in bars:
                seen[b["bar_idx"]] = b
            valid[d] = sorted(seen.values(), key=lambda x: x["bar_idx"])
    return valid


def detect_divergence(bars_window):
    """
    Replicate the divergence detection from evaluate_absorption.
    bars_window = last (LOOKBACK+1) closed bars.
    Returns (direction, div_raw) or (None, 0).
    """
    lows = [b["bar_low"] for b in bars_window]
    highs = [b["bar_high"] for b in bars_window]
    cvds = [b["cvd"] for b in bars_window]

    cvd_start, cvd_end = cvds[0], cvds[-1]
    cvd_slope = cvd_end - cvd_start
    cvd_range = max(cvds) - min(cvds)
    if cvd_range == 0:
        return None, 0

    price_low_start, price_low_end = lows[0], lows[-1]
    price_high_start, price_high_end = highs[0], highs[-1]
    price_range = max(highs) - min(lows)
    if price_range == 0:
        return None, 0

    cvd_norm = cvd_slope / cvd_range
    price_low_norm = (price_low_end - price_low_start) / price_range
    price_high_norm = (price_high_end - price_high_start) / price_range

    direction = None
    div_raw = 0

    # Bullish: CVD falling while price holds/rises
    if cvd_norm < -CVD_NORM_THRESHOLD:
        gap = price_low_norm - cvd_norm
        if gap > GAP_THRESHOLD:
            direction = "bullish"
            if gap > 1.2:
                div_raw = 4
            elif gap > 0.8:
                div_raw = 3
            elif gap > 0.4:
                div_raw = 2
            else:
                div_raw = 1

    # Bearish: CVD rising while price stalls/falls
    if cvd_norm > CVD_NORM_THRESHOLD and direction is None:
        gap = cvd_norm - price_high_norm
        if gap > GAP_THRESHOLD:
            direction = "bearish"
            if gap > 1.2:
                div_raw = 4
            elif gap > 0.8:
                div_raw = 3
            elif gap > 0.4:
                div_raw = 2
            else:
                div_raw = 1

    return direction, div_raw


def simulate_outcome(bars_after, entry_price, direction):
    """
    Simulate trade outcome using subsequent bars.
    Returns dict with outcome, pnl, mfe, mae, bars_held.
    """
    mfe = 0.0
    mae = 0.0

    for i, bar in enumerate(bars_after):
        if direction == "bullish":
            profit_high = bar["bar_high"] - entry_price
            profit_low = bar["bar_low"] - entry_price
        else:  # bearish
            profit_high = entry_price - bar["bar_low"]
            profit_low = entry_price - bar["bar_high"]

        mfe = max(mfe, profit_high)
        mae = min(mae, profit_low)

        # Check SL first (worst case within bar)
        if profit_low <= -SL_PTS:
            return {
                "outcome": "LOSS",
                "pnl": -SL_PTS,
                "mfe": mfe,
                "mae": -SL_PTS,
                "bars_held": i + 1,
            }

        # Check target
        if profit_high >= TARGET_PTS:
            return {
                "outcome": "WIN",
                "pnl": TARGET_PTS,
                "mfe": TARGET_PTS,
                "mae": mae,
                "bars_held": i + 1,
            }

        # Expiry check
        if i + 1 >= EXPIRY_BARS:
            close_price = bar["bar_close"]
            if direction == "bullish":
                final_pnl = close_price - entry_price
            else:
                final_pnl = entry_price - close_price
            return {
                "outcome": "EXPIRED",
                "pnl": round(final_pnl, 2),
                "mfe": mfe,
                "mae": mae,
                "bars_held": i + 1,
            }

    # Ran out of bars (end of day)
    if bars_after:
        last = bars_after[-1]
        if direction == "bullish":
            final_pnl = last["bar_close"] - entry_price
        else:
            final_pnl = entry_price - last["bar_close"]
        return {
            "outcome": "EXPIRED",
            "pnl": round(final_pnl, 2),
            "mfe": mfe,
            "mae": mae,
            "bars_held": len(bars_after),
        }
    return {
        "outcome": "EXPIRED",
        "pnl": 0.0,
        "mfe": 0.0,
        "mae": 0.0,
        "bars_held": 0,
    }


def classify_time(ts_end):
    """Classify bar time into morning/midday/afternoon."""
    if ts_end is None:
        return "unknown"
    # ts_end is timestamptz — convert to ET
    # The DB stores in UTC, but we need ET
    from zoneinfo import ZoneInfo
    et = ts_end.astimezone(ZoneInfo("America/New_York"))
    t = et.time()
    if t < dtime(11, 30):
        return "morning"
    elif t < dtime(14, 0):
        return "midday"
    else:
        return "afternoon"


def run_backtest():
    print("=" * 80)
    print("ES Absorption Volume Gate Backtest")
    print("=" * 80)

    print("\nFetching rithmic range bars...")
    bars_by_date = fetch_bars()
    total_days = len(bars_by_date)
    total_bars = sum(len(v) for v in bars_by_date.values())
    date_range = sorted(bars_by_date.keys())

    print(f"Date range: {date_range[0]} to {date_range[-1]}")
    print(f"Trading days: {total_days}")
    print(f"Total closed bars: {total_bars}")
    print(f"Avg bars/day: {total_bars / total_days:.0f}")

    if total_days < 3:
        # Fall back to March 1
        print("\nWARNING: <3 days of data post-March 18. Falling back to March 1.")
        # Would need to re-fetch — skip for now
        return

    # ──── Run all scenarios ────
    # We detect ALL divergence signals (no gate), then filter by vol_ratio per scenario
    all_signals = []  # each: {date, bar_idx, direction, div_raw, vol_ratio, entry_price, ts_end, outcome_dict}

    for trade_date, bars in sorted(bars_by_date.items()):
        cooldown_bull = -100
        cooldown_bear = -100
        last_checked = -1

        for i in range(MIN_BARS_NEEDED, len(bars)):
            trigger = bars[i]
            trigger_idx = trigger["bar_idx"]

            # Skip already checked (dedup)
            if trigger_idx <= last_checked:
                continue
            last_checked = trigger_idx

            # Volume calculation (20-bar average)
            recent = bars[max(0, i - VOL_WINDOW):i]  # 20 bars before trigger
            if len(recent) < VOL_WINDOW:
                continue
            vol_avg = sum(b["bar_volume"] for b in recent) / len(recent)
            if vol_avg <= 0:
                continue
            vol_ratio = trigger["bar_volume"] / vol_avg

            # Divergence detection
            window = bars[i - LOOKBACK:i + 1]  # LOOKBACK+1 bars including trigger
            if len(window) < LOOKBACK + 1:
                continue

            direction, div_raw = detect_divergence(window)
            if direction is None:
                continue

            # Cooldown check (10 bars between same direction)
            if direction == "bullish":
                if trigger_idx - cooldown_bull < COOLDOWN_BARS:
                    continue
                cooldown_bull = trigger_idx
            else:
                if trigger_idx - cooldown_bear < COOLDOWN_BARS:
                    continue
                cooldown_bear = trigger_idx

            # Simulate outcome
            remaining_bars = bars[i + 1:]
            entry_price = trigger["bar_close"]
            outcome = simulate_outcome(remaining_bars, entry_price, direction)

            all_signals.append({
                "date": trade_date,
                "bar_idx": trigger_idx,
                "direction": direction,
                "div_raw": div_raw,
                "vol_ratio": round(vol_ratio, 2),
                "entry_price": entry_price,
                "ts_end": trigger["ts_end"],
                "outcome": outcome["outcome"],
                "pnl": outcome["pnl"],
                "mfe": outcome["mfe"],
                "mae": outcome["mae"],
                "bars_held": outcome["bars_held"],
            })

    print(f"\nTotal divergence signals detected (no gate, with cooldown): {len(all_signals)}")

    # ──── Scenario analysis ────
    print("\n" + "=" * 80)
    print("SCENARIO COMPARISON")
    print("=" * 80)

    def analyze(signals, label):
        """Compute stats for a set of signals."""
        if not signals:
            return {
                "count": 0, "wr": 0, "pnl": 0, "maxdd": 0,
                "per_day": 0, "wins": 0, "losses": 0, "expired": 0,
            }
        wins = sum(1 for s in signals if s["outcome"] == "WIN")
        losses = sum(1 for s in signals if s["outcome"] == "LOSS")
        expired = sum(1 for s in signals if s["outcome"] == "EXPIRED")
        total_pnl = sum(s["pnl"] for s in signals)
        wr = wins / len(signals) * 100 if signals else 0

        # Max drawdown (running PnL)
        running = 0
        peak = 0
        maxdd = 0
        for s in sorted(signals, key=lambda x: (x["date"], x["bar_idx"])):
            running += s["pnl"]
            peak = max(peak, running)
            dd = running - peak
            maxdd = min(maxdd, dd)

        # Signals per day
        dates_with_signals = set(s["date"] for s in signals)
        per_day = len(signals) / total_days  # across ALL trading days

        return {
            "count": len(signals),
            "wins": wins,
            "losses": losses,
            "expired": expired,
            "wr": wr,
            "pnl": total_pnl,
            "maxdd": maxdd,
            "per_day": per_day,
            "active_days": len(dates_with_signals),
        }

    def print_table(stats_dict, title):
        print(f"\n--- {title} ---")
        print(f"{'Scenario':<22} {'Signals':>8} {'WR%':>7} {'PnL':>9} {'MaxDD':>8} {'Sig/Day':>8} {'W':>4} {'L':>4} {'E':>4} {'Days':>5}")
        print("-" * 95)
        for name, st in stats_dict.items():
            print(f"{name:<22} {st['count']:>8} {st['wr']:>6.1f}% {st['pnl']:>+8.1f} {st['maxdd']:>+7.1f} {st['per_day']:>7.1f} {st['wins']:>4} {st['losses']:>4} {st['expired']:>4} {st['active_days']:>5}")

    # ── Scenario 1-3: Volume gate thresholds ──
    scenario_results = {}
    for name, threshold in SCENARIOS.items():
        filtered = [s for s in all_signals if s["vol_ratio"] >= threshold]
        scenario_results[name] = analyze(filtered, name)

    print_table(scenario_results, "OVERALL (all directions)")

    # ── Direction breakdown ──
    for dir_label in ["bullish", "bearish"]:
        dir_results = {}
        for name, threshold in SCENARIOS.items():
            filtered = [s for s in all_signals if s["vol_ratio"] >= threshold and s["direction"] == dir_label]
            dir_results[name] = analyze(filtered, f"{name} {dir_label}")
        print_table(dir_results, f"{dir_label.upper()} only")

    # ── Time of day breakdown ──
    print(f"\n--- TIME-OF-DAY BREAKDOWN (no volume gate) ---")
    print(f"{'Period':<15} {'Signals':>8} {'WR%':>7} {'PnL':>9} {'MaxDD':>8} {'W':>4} {'L':>4} {'E':>4}")
    print("-" * 70)
    for period in ["morning", "midday", "afternoon"]:
        sigs = [s for s in all_signals if classify_time(s["ts_end"]) == period]
        st = analyze(sigs, period)
        print(f"{period:<15} {st['count']:>8} {st['wr']:>6.1f}% {st['pnl']:>+8.1f} {st['maxdd']:>+7.1f} {st['wins']:>4} {st['losses']:>4} {st['expired']:>4}")

    # Cross time-of-day for 1.4x specifically
    print(f"\n--- TIME-OF-DAY BREAKDOWN (1.4x gate - current) ---")
    print(f"{'Period':<15} {'Signals':>8} {'WR%':>7} {'PnL':>9} {'MaxDD':>8} {'W':>4} {'L':>4} {'E':>4}")
    print("-" * 70)
    for period in ["morning", "midday", "afternoon"]:
        sigs = [s for s in all_signals if classify_time(s["ts_end"]) == period and s["vol_ratio"] >= 1.4]
        st = analyze(sigs, period)
        print(f"{period:<15} {st['count']:>8} {st['wr']:>6.1f}% {st['pnl']:>+8.1f} {st['maxdd']:>+7.1f} {st['wins']:>4} {st['losses']:>4} {st['expired']:>4}")

    # ── Scenario 4: Volume as grade component (bucket analysis) ──
    print(f"\n--- SCENARIO 4: VOLUME AS GRADE COMPONENT (all signals, bucketed) ---")
    print(f"{'Volume Bucket':<22} {'Signals':>8} {'WR%':>7} {'PnL':>9} {'MaxDD':>8} {'Sig/Day':>8} {'W':>4} {'L':>4} {'E':>4}")
    print("-" * 95)
    buckets = {
        "high (>= 1.4x)": lambda s: s["vol_ratio"] >= 1.4,
        "medium (1.0-1.4x)": lambda s: 1.0 <= s["vol_ratio"] < 1.4,
        "low (< 1.0x)": lambda s: s["vol_ratio"] < 1.0,
    }
    for bname, bfilter in buckets.items():
        sigs = [s for s in all_signals if bfilter(s)]
        st = analyze(sigs, bname)
        print(f"{bname:<22} {st['count']:>8} {st['wr']:>6.1f}% {st['pnl']:>+8.1f} {st['maxdd']:>+7.1f} {st['per_day']:>7.1f} {st['wins']:>4} {st['losses']:>4} {st['expired']:>4}")

    # ── Volume bucket x direction ──
    print(f"\n--- VOLUME BUCKET x DIRECTION ---")
    print(f"{'Bucket + Dir':<30} {'Signals':>8} {'WR%':>7} {'PnL':>9} {'MaxDD':>8} {'W':>4} {'L':>4} {'E':>4}")
    print("-" * 85)
    for bname, bfilter in buckets.items():
        for dir_label in ["bullish", "bearish"]:
            sigs = [s for s in all_signals if bfilter(s) and s["direction"] == dir_label]
            st = analyze(sigs, f"{bname} {dir_label}")
            label = f"{bname} {dir_label}"
            print(f"{label:<30} {st['count']:>8} {st['wr']:>6.1f}% {st['pnl']:>+8.1f} {st['maxdd']:>+7.1f} {st['wins']:>4} {st['losses']:>4} {st['expired']:>4}")

    # ── Incremental signals: what does lowering from 1.4x to 1.0x ADD? ──
    print(f"\n--- INCREMENTAL ANALYSIS: Signals ADDED by lowering gate ---")
    added_1_0 = [s for s in all_signals if 1.0 <= s["vol_ratio"] < 1.4]
    added_0_0 = [s for s in all_signals if s["vol_ratio"] < 1.0]
    for label, sigs in [("Added at 1.0-1.4x", added_1_0), ("Added at <1.0x", added_0_0)]:
        st = analyze(sigs, label)
        print(f"  {label}: {st['count']} signals, {st['wr']:.1f}% WR, {st['pnl']:+.1f} PnL, {st['maxdd']:+.1f} MaxDD")
        # By direction
        for d in ["bullish", "bearish"]:
            dsigs = [s for s in sigs if s["direction"] == d]
            dst = analyze(dsigs, f"{label} {d}")
            print(f"    {d}: {dst['count']} signals, {dst['wr']:.1f}% WR, {dst['pnl']:+.1f} PnL")

    # ── Div_raw quality distribution ──
    print(f"\n--- DIVERGENCE RAW SCORE DISTRIBUTION ---")
    print(f"{'Div Raw':<10} {'Count':>8} {'WR%':>7} {'PnL':>9} {'Avg Vol':>10}")
    print("-" * 50)
    for dr in sorted(set(s["div_raw"] for s in all_signals)):
        sigs = [s for s in all_signals if s["div_raw"] == dr]
        st = analyze(sigs, f"div_{dr}")
        avg_vol = sum(s["vol_ratio"] for s in sigs) / len(sigs) if sigs else 0
        print(f"  {dr:<8} {st['count']:>8} {st['wr']:>6.1f}% {st['pnl']:>+8.1f} {avg_vol:>9.2f}x")

    # ── Cross-check with DB ──
    print(f"\n--- CROSS-CHECK ---")
    print(f"Total 1.4x signals in backtest: {scenario_results['1.4x (current)']['count']}")
    # Query setup_log for ES Absorption count in same period
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM setup_log
            WHERE setup_name = 'ES Absorption'
              AND trade_date >= %s
        """, (MIN_DATE,))
        db_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"ES Absorption in setup_log (same period): {db_count}")
        pct_diff = abs(scenario_results['1.4x (current)']['count'] - db_count) / max(db_count, 1) * 100
        print(f"Difference: {pct_diff:.0f}%")
        if pct_diff > 30:
            print("  WARNING: >30% difference. Backtest may not perfectly match live "
                  "(live has Volland confluence, time gates, V12 filter, etc.)")
        else:
            print("  OK: within expected range (backtest has no Volland gates or V12 filter)")
    except Exception as e:
        print(f"  Could not cross-check: {e}")

    # ── Confidence note ──
    total_sigs = scenario_results["1.4x (current)"]["count"]
    print(f"\n--- CONFIDENCE ---")
    if total_sigs < 50:
        print(f"  {total_sigs} signals at 1.4x = DIRECTIONAL SIGNAL ONLY (<50 trades)")
    elif total_sigs < 100:
        print(f"  {total_sigs} signals at 1.4x = MODERATE CONFIDENCE (50-100 trades)")
    else:
        print(f"  {total_sigs} signals at 1.4x = HIGH CONFIDENCE (100+ trades)")
    print(f"  Date range: {date_range[0]} to {date_range[-1]} ({total_days} trading days)")
    print(f"  Excluded: dates with <{MIN_BARS_PER_DAY} bars, non-rithmic sources, open bars")
    print(f"  NOTE: This backtest uses FIXED SL={SL_PTS}/T={TARGET_PTS}. No trail, no Volland")
    print(f"         confluence scoring, no V12 filter. Pure volume gate impact only.")


if __name__ == "__main__":
    run_backtest()
