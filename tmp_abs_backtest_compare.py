"""
ES Absorption Backtest: Compare Approach A (current swing-to-swing) vs Approach B (fire-on-volume-bar).

Pulls range bar data from es_range_bars (source='rithmic'), replays each day,
runs both detection approaches, tracks outcomes (fixed +10/-12), and prints
a comprehensive comparison table.

Run: railway run python -u tmp_abs_backtest_compare.py
"""

import os
import json
import statistics
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict

import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ["DATABASE_URL"]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TARGET_PTS = 10.0
STOP_PTS = 12.0
COOLDOWN_MINUTES = 30   # 30-min cooldown per direction
SIGNAL_START = dtime(10, 0)   # 10:00 ET
SIGNAL_END = dtime(15, 30)    # 15:30 ET

# Approach A settings (current production)
A_PIVOT_LEFT = 2
A_PIVOT_RIGHT = 2
A_VOL_WINDOW = 10
A_MIN_VOL_RATIO = 1.4
A_CVD_Z_MIN = 0.5
A_CVD_STD_WINDOW = 20
A_MAX_TRIGGER_DIST = 40

# Approach B settings (proposed)
B_PIVOT_LEFT = 2
B_PIVOT_RIGHT = 2
B_VOL_WINDOW = 10
B_MIN_VOL_RATIO = 1.4
B_CVD_Z_MIN = 0.5
B_CVD_STD_WINDOW = 20
B_MAX_BAR_DIST = 20  # tighter — bar must be within 20 bars of swing

# DST-aware UTC offset for each date
# EST = UTC-5, EDT = UTC-4
# DST 2026 starts March 8
DST_START_2026 = datetime(2026, 3, 8)


def utc_offset_hours(trade_date):
    """Return UTC offset for Eastern Time on a given date."""
    dt = datetime.combine(trade_date, dtime(12, 0))
    if dt >= DST_START_2026:
        return -4  # EDT
    return -5  # EST


def bar_et_time(bar, trade_date):
    """Parse bar's ts_start into ET time-of-day, handling timezone."""
    ts = bar.get("ts_start") or bar.get("ts_end")
    if ts is None:
        return None
    if isinstance(ts, str):
        # Try parsing ISO format
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return None
    # ts should be a datetime — if naive, assume UTC
    if ts.tzinfo is not None:
        from datetime import timezone
        ts = ts.astimezone(tz=None)  # local
        # Convert to UTC first
        import pytz
        utc_ts = ts.astimezone(pytz.utc)
    else:
        import pytz
        utc_ts = pytz.utc.localize(ts)

    offset = utc_offset_hours(trade_date)
    et_ts = utc_ts + timedelta(hours=offset)
    return et_ts.time()


# ---------------------------------------------------------------------------
# Swing Tracker (shared logic)
# ---------------------------------------------------------------------------

def make_swing_tracker():
    return {
        "swings": [],
        "last_type": None,
        "last_pivot_idx": -1,
    }


def add_swing(tracker, new_swing):
    """Add swing with alternating enforcement and adaptive invalidation."""
    swings = tracker["swings"]
    last_type = tracker["last_type"]

    if not swings or last_type is None:
        swings.append(new_swing)
        tracker["last_type"] = new_swing["type"]
        return

    if new_swing["type"] == last_type:
        # Same direction — adaptive invalidation only
        if new_swing["type"] == "L" and new_swing["price"] <= swings[-1]["price"]:
            swings[-1] = new_swing  # lower low replaces
        elif new_swing["type"] == "H" and new_swing["price"] >= swings[-1]["price"]:
            swings[-1] = new_swing  # higher high replaces
        # else: skip
    else:
        swings.append(new_swing)
        tracker["last_type"] = new_swing["type"]


def update_swings(tracker, closed, pivot_left, pivot_right):
    """Detect pivot highs/lows and update swing tracker."""
    last_scanned = tracker["last_pivot_idx"]
    max_pos = len(closed) - 1 - pivot_right
    if max_pos < pivot_left:
        return

    for pos in range(pivot_left, max_pos + 1):
        bar = closed[pos]
        if bar["idx"] <= last_scanned:
            continue

        # Check swing low: bar.low <= all neighbors
        is_low = True
        for j in range(1, pivot_left + 1):
            if bar["low"] > closed[pos - j]["low"]:
                is_low = False
                break
        if is_low:
            for j in range(1, pivot_right + 1):
                if bar["low"] > closed[pos + j]["low"]:
                    is_low = False
                    break

        # Check swing high: bar.high >= all neighbors
        is_high = True
        for j in range(1, pivot_left + 1):
            if bar["high"] < closed[pos - j]["high"]:
                is_high = False
                break
        if is_high:
            for j in range(1, pivot_right + 1):
                if bar["high"] < closed[pos + j]["high"]:
                    is_high = False
                    break

        if not is_low and not is_high:
            continue

        # If both qualify on same bar, prefer alternation
        if is_low and is_high:
            lt = tracker["last_type"]
            if lt == "L":
                is_low = False
            elif lt == "H":
                is_high = False
            else:
                is_high = False  # default: low first

        if is_low:
            add_swing(tracker, {
                "type": "L", "price": bar["low"], "cvd": bar["cvd"],
                "volume": bar["volume"], "bar_idx": bar["idx"],
            })
        elif is_high:
            add_swing(tracker, {
                "type": "H", "price": bar["high"], "cvd": bar["cvd"],
                "volume": bar["volume"], "bar_idx": bar["idx"],
            })

    # Update last scanned
    if max_pos >= pivot_left and closed[max_pos]["idx"] > last_scanned:
        tracker["last_pivot_idx"] = closed[max_pos]["idx"]


def compute_cvd_stats(closed, std_window):
    """Compute CVD std dev and ATR proxy from closed bars."""
    start_i = max(1, len(closed) - std_window)
    deltas = [closed[i]["cvd"] - closed[i - 1]["cvd"] for i in range(start_i, len(closed))]
    if len(deltas) < 5:
        return None, None
    mean_d = sum(deltas) / len(deltas)
    cvd_std = (sum((d - mean_d) ** 2 for d in deltas) / len(deltas)) ** 0.5
    if cvd_std < 1:
        cvd_std = 1

    atr_moves = [abs(closed[i]["close"] - closed[i - 1]["close"])
                 for i in range(start_i, len(closed))]
    atr = sum(atr_moves) / len(atr_moves) if atr_moves else 1.0
    if atr < 0.01:
        atr = 0.01
    return cvd_std, atr


def divergence_score(cvd_z, price_atr):
    """Score a divergence (0-100)."""
    base = min(100, cvd_z / 3.0 * 100)
    mult = min(2.0, 0.5 + price_atr * 0.5)
    return min(100, base * mult)


# Pattern tier
PATTERN_TIER = {
    "sell_exhaustion": 2, "buy_exhaustion": 2,
    "sell_absorption": 1, "buy_absorption": 1,
}


# ---------------------------------------------------------------------------
# Approach A: Current swing-to-swing with delayed trigger
# ---------------------------------------------------------------------------

def approach_a_evaluate(closed_so_far, tracker_a):
    """
    Current production logic: update swings, then on high-volume trigger bar
    compare consecutive same-type swings. Trigger bar within max_trigger_dist
    bars of most recent swing in pair.
    """
    min_bars = max(A_VOL_WINDOW, A_CVD_STD_WINDOW, A_PIVOT_LEFT + A_PIVOT_RIGHT + 1) + 1
    if len(closed_so_far) < min_bars:
        return None

    trigger = closed_so_far[-1]
    trigger_idx = trigger["idx"]

    # Update swings
    update_swings(tracker_a, closed_so_far, A_PIVOT_LEFT, A_PIVOT_RIGHT)

    # Volume gate
    recent_vols = [b["volume"] for b in closed_so_far[-(A_VOL_WINDOW + 1):-1]]
    if not recent_vols:
        return None
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0:
        return None
    vol_ratio = trigger["volume"] / vol_avg
    if vol_ratio < A_MIN_VOL_RATIO:
        return None

    # CVD stats
    cvd_std, atr = compute_cvd_stats(closed_so_far, A_CVD_STD_WINDOW)
    if cvd_std is None:
        return None

    # Swing-to-swing divergence scan
    swings = tracker_a["swings"]
    swing_lows = [s for s in swings if s["type"] == "L"]
    swing_highs = [s for s in swings if s["type"] == "H"]

    bullish_divs = []
    bearish_divs = []

    # Bullish patterns: consecutive swing lows
    for i in range(1, len(swing_lows)):
        s1, s2 = swing_lows[i - 1], swing_lows[i]
        if trigger_idx - s2["bar_idx"] > A_MAX_TRIGGER_DIST:
            continue

        cvd_gap = abs(s2["cvd"] - s1["cvd"])
        cvd_z = cvd_gap / cvd_std
        if cvd_z < A_CVD_Z_MIN:
            continue

        price_dist = abs(s2["price"] - s1["price"])
        price_atr = price_dist / atr
        score = divergence_score(cvd_z, price_atr)

        if s2["price"] < s1["price"] and s2["cvd"] > s1["cvd"]:
            bullish_divs.append({
                "pattern": "sell_exhaustion", "score": score,
                "cvd_z": cvd_z, "swing": s2, "ref_swing": s1,
            })
        elif s2["price"] >= s1["price"] and s2["cvd"] < s1["cvd"]:
            bullish_divs.append({
                "pattern": "sell_absorption", "score": score,
                "cvd_z": cvd_z, "swing": s2, "ref_swing": s1,
            })

    # Bearish patterns: consecutive swing highs
    for i in range(1, len(swing_highs)):
        s1, s2 = swing_highs[i - 1], swing_highs[i]
        if trigger_idx - s2["bar_idx"] > A_MAX_TRIGGER_DIST:
            continue

        cvd_gap = abs(s2["cvd"] - s1["cvd"])
        cvd_z = cvd_gap / cvd_std
        if cvd_z < A_CVD_Z_MIN:
            continue

        price_dist = abs(s2["price"] - s1["price"])
        price_atr = price_dist / atr
        score = divergence_score(cvd_z, price_atr)

        if s2["price"] > s1["price"] and s2["cvd"] < s1["cvd"]:
            bearish_divs.append({
                "pattern": "buy_exhaustion", "score": score,
                "cvd_z": cvd_z, "swing": s2, "ref_swing": s1,
            })
        elif s2["price"] <= s1["price"] and s2["cvd"] > s1["cvd"]:
            bearish_divs.append({
                "pattern": "buy_absorption", "score": score,
                "cvd_z": cvd_z, "swing": s2, "ref_swing": s1,
            })

    # Direction resolution with tier priority
    best_bull = max(bullish_divs, key=lambda d: d["score"]) if bullish_divs else None
    best_bear = max(bearish_divs, key=lambda d: d["score"]) if bearish_divs else None

    if not best_bull and not best_bear:
        return None

    if best_bull and best_bear:
        bull_tier = PATTERN_TIER.get(best_bull["pattern"], 1)
        bear_tier = PATTERN_TIER.get(best_bear["pattern"], 1)
        if bull_tier > bear_tier:
            direction, best = "bullish", best_bull
        elif bear_tier > bull_tier:
            direction, best = "bearish", best_bear
        elif best_bull["score"] >= best_bear["score"]:
            direction, best = "bullish", best_bull
        else:
            direction, best = "bearish", best_bear
    elif best_bull:
        direction, best = "bullish", best_bull
    else:
        direction, best = "bearish", best_bear

    return {
        "direction": direction,
        "entry": trigger["close"],
        "bar_idx": trigger_idx,
        "pattern": best["pattern"],
        "score": best["score"],
        "cvd_z": best["cvd_z"],
        "vol_ratio": round(vol_ratio, 2),
        "swing_bar_idx": best["swing"]["bar_idx"],
        "ref_swing_bar_idx": best["ref_swing"]["bar_idx"],
    }


# ---------------------------------------------------------------------------
# Approach B: Fire on high-volume bar (compare bar directly to prior swings)
# ---------------------------------------------------------------------------

def approach_b_evaluate(closed_so_far, tracker_b):
    """
    Proposed logic: when a high-volume bar completes, compare IT directly to
    the most recent confirmed swing(s) of each type. No need for consecutive
    same-type swing pairs — the current bar IS the second point.

    For bullish: compare current bar's low to most recent swing low's price/CVD.
    For bearish: compare current bar's high to most recent swing high's price/CVD.
    """
    min_bars = max(B_VOL_WINDOW, B_CVD_STD_WINDOW, B_PIVOT_LEFT + B_PIVOT_RIGHT + 1) + 1
    if len(closed_so_far) < min_bars:
        return None

    trigger = closed_so_far[-1]
    trigger_idx = trigger["idx"]

    # Update swings (same pivot detection)
    update_swings(tracker_b, closed_so_far, B_PIVOT_LEFT, B_PIVOT_RIGHT)

    # Volume gate (same threshold)
    recent_vols = [b["volume"] for b in closed_so_far[-(B_VOL_WINDOW + 1):-1]]
    if not recent_vols:
        return None
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0:
        return None
    vol_ratio = trigger["volume"] / vol_avg
    if vol_ratio < B_MIN_VOL_RATIO:
        return None

    # CVD stats
    cvd_std, atr = compute_cvd_stats(closed_so_far, B_CVD_STD_WINDOW)
    if cvd_std is None:
        return None

    swings = tracker_b["swings"]
    if not swings:
        return None

    bullish_divs = []
    bearish_divs = []

    # Bullish: compare trigger bar's low/CVD to most recent swing low(s)
    # Walk backwards through swings to find swing lows within distance
    for s in reversed(swings):
        if s["type"] != "L":
            continue
        bars_away = trigger_idx - s["bar_idx"]
        if bars_away > B_MAX_BAR_DIST:
            break  # swings are chronological, older ones are even further
        if bars_away <= 0:
            continue  # shouldn't compare with self

        # Use trigger bar's low and CVD at close as "current point"
        cvd_gap = abs(trigger["cvd"] - s["cvd"])
        cvd_z = cvd_gap / cvd_std
        if cvd_z < B_CVD_Z_MIN:
            continue

        price_dist = abs(trigger["low"] - s["price"])
        price_atr = price_dist / atr
        score = divergence_score(cvd_z, price_atr)

        # Sell exhaustion: bar makes lower low + higher CVD than swing low
        if trigger["low"] < s["price"] and trigger["cvd"] > s["cvd"]:
            bullish_divs.append({
                "pattern": "sell_exhaustion", "score": score,
                "cvd_z": cvd_z, "swing": s, "bars_away": bars_away,
            })
        # Sell absorption: bar holds at/above swing low + lower CVD
        elif trigger["low"] >= s["price"] and trigger["cvd"] < s["cvd"]:
            bullish_divs.append({
                "pattern": "sell_absorption", "score": score,
                "cvd_z": cvd_z, "swing": s, "bars_away": bars_away,
            })

    # Bearish: compare trigger bar's high/CVD to most recent swing high(s)
    for s in reversed(swings):
        if s["type"] != "H":
            continue
        bars_away = trigger_idx - s["bar_idx"]
        if bars_away > B_MAX_BAR_DIST:
            break
        if bars_away <= 0:
            continue

        cvd_gap = abs(trigger["cvd"] - s["cvd"])
        cvd_z = cvd_gap / cvd_std
        if cvd_z < B_CVD_Z_MIN:
            continue

        price_dist = abs(trigger["high"] - s["price"])
        price_atr = price_dist / atr
        score = divergence_score(cvd_z, price_atr)

        # Buy exhaustion: bar makes higher high + lower CVD than swing high
        if trigger["high"] > s["price"] and trigger["cvd"] < s["cvd"]:
            bearish_divs.append({
                "pattern": "buy_exhaustion", "score": score,
                "cvd_z": cvd_z, "swing": s, "bars_away": bars_away,
            })
        # Buy absorption: bar makes lower high + higher CVD
        elif trigger["high"] <= s["price"] and trigger["cvd"] > s["cvd"]:
            bearish_divs.append({
                "pattern": "buy_absorption", "score": score,
                "cvd_z": cvd_z, "swing": s, "bars_away": bars_away,
            })

    # Direction resolution with tier priority (same as Approach A)
    best_bull = max(bullish_divs, key=lambda d: d["score"]) if bullish_divs else None
    best_bear = max(bearish_divs, key=lambda d: d["score"]) if bearish_divs else None

    if not best_bull and not best_bear:
        return None

    if best_bull and best_bear:
        bull_tier = PATTERN_TIER.get(best_bull["pattern"], 1)
        bear_tier = PATTERN_TIER.get(best_bear["pattern"], 1)
        if bull_tier > bear_tier:
            direction, best = "bullish", best_bull
        elif bear_tier > bull_tier:
            direction, best = "bearish", best_bear
        elif best_bull["score"] >= best_bear["score"]:
            direction, best = "bullish", best_bull
        else:
            direction, best = "bearish", best_bear
    elif best_bull:
        direction, best = "bullish", best_bull
    else:
        direction, best = "bearish", best_bear

    return {
        "direction": direction,
        "entry": trigger["close"],
        "bar_idx": trigger_idx,
        "pattern": best["pattern"],
        "score": best["score"],
        "cvd_z": best["cvd_z"],
        "vol_ratio": round(vol_ratio, 2),
        "swing_bar_idx": best["swing"]["bar_idx"],
        "bars_away": best.get("bars_away", 0),
    }


# ---------------------------------------------------------------------------
# Outcome tracker
# ---------------------------------------------------------------------------

def track_outcome(signal, remaining_bars):
    """
    Walk forward through remaining bars to determine WIN/LOSS/EXPIRED.
    Returns dict with outcome, pnl, mfe, mae.
    """
    entry = signal["entry"]
    direction = signal["direction"]
    is_long = direction == "bullish"

    target = entry + TARGET_PTS if is_long else entry - TARGET_PTS
    stop = entry - STOP_PTS if is_long else entry + STOP_PTS

    mfe = 0.0  # max favorable excursion (positive)
    mae = 0.0  # max adverse excursion (positive = how far against)

    for bar in remaining_bars:
        if is_long:
            favorable = bar["high"] - entry
            adverse = entry - bar["low"]
        else:
            favorable = entry - bar["low"]
            adverse = bar["high"] - entry

        mfe = max(mfe, favorable)
        mae = max(mae, adverse)

        # Check stop first (conservative)
        if is_long:
            if bar["low"] <= stop:
                return {"outcome": "LOSS", "pnl": -STOP_PTS, "mfe": round(mfe, 2), "mae": round(mae, 2)}
            if bar["high"] >= target:
                return {"outcome": "WIN", "pnl": TARGET_PTS, "mfe": round(mfe, 2), "mae": round(mae, 2)}
        else:
            if bar["high"] >= stop:
                return {"outcome": "LOSS", "pnl": -STOP_PTS, "mfe": round(mfe, 2), "mae": round(mae, 2)}
            if bar["low"] <= target:
                return {"outcome": "WIN", "pnl": TARGET_PTS, "mfe": round(mfe, 2), "mae": round(mae, 2)}

    # Expired — mark-to-market on last bar
    if remaining_bars:
        last_close = remaining_bars[-1]["close"]
        mtm = (last_close - entry) if is_long else (entry - last_close)
    else:
        mtm = 0.0

    return {"outcome": "EXPIRED", "pnl": round(mtm, 2), "mfe": round(mfe, 2), "mae": round(mae, 2)}


# ---------------------------------------------------------------------------
# Cooldown check
# ---------------------------------------------------------------------------

def check_cooldown(signal, last_signals, bars_lookup):
    """
    30-minute cooldown per direction.
    Returns True if signal should be SKIPPED (still in cooldown).
    """
    direction = signal["direction"]
    bar_idx = signal["bar_idx"]

    for prev in reversed(last_signals):
        if prev["direction"] != direction:
            continue
        # Compute approximate time difference using bar indices
        # Range bars don't have fixed time, so we use ts_end from bars_lookup
        prev_ts = bars_lookup.get(prev["bar_idx"], {}).get("ts_end")
        curr_ts = bars_lookup.get(bar_idx, {}).get("ts_end")
        if prev_ts and curr_ts:
            if isinstance(prev_ts, str):
                try:
                    prev_ts = datetime.fromisoformat(prev_ts.replace("Z", "+00:00"))
                except Exception:
                    prev_ts = None
            if isinstance(curr_ts, str):
                try:
                    curr_ts = datetime.fromisoformat(curr_ts.replace("Z", "+00:00"))
                except Exception:
                    curr_ts = None
            if prev_ts and curr_ts:
                # Make both offset-aware or both naive for comparison
                if prev_ts.tzinfo is not None and curr_ts.tzinfo is not None:
                    diff = (curr_ts - prev_ts).total_seconds()
                elif prev_ts.tzinfo is None and curr_ts.tzinfo is None:
                    diff = (curr_ts - prev_ts).total_seconds()
                else:
                    # Mixed — strip tzinfo for comparison
                    diff = (curr_ts.replace(tzinfo=None) - prev_ts.replace(tzinfo=None)).total_seconds()
                if diff < COOLDOWN_MINUTES * 60:
                    return True
    return False


# ---------------------------------------------------------------------------
# Time check using bar timestamps
# ---------------------------------------------------------------------------

def bar_in_signal_window(bar, trade_date):
    """Check if bar's time is within 10:00 - 15:30 ET."""
    ts = bar.get("ts_end")
    if ts is None:
        return False

    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return False

    # Convert to ET
    offset = utc_offset_hours(trade_date)
    if ts.tzinfo is not None:
        # Convert to UTC then to ET
        import pytz
        utc_ts = ts.astimezone(pytz.utc)
        et_ts = utc_ts + timedelta(hours=offset)
    else:
        # Assume UTC
        et_ts = ts + timedelta(hours=offset)

    t = et_ts.time()
    return SIGNAL_START <= t <= SIGNAL_END


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 80)
    print("ES ABSORPTION BACKTEST: Approach A (current) vs Approach B (proposed)")
    print("=" * 80)
    print(f"Target: +{TARGET_PTS} pts | Stop: -{STOP_PTS} pts | Cooldown: {COOLDOWN_MINUTES} min")
    print(f"Signal window: {SIGNAL_START} - {SIGNAL_END} ET")
    print()

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get available dates
    cur.execute("""
        SELECT DISTINCT trade_date
        FROM es_range_bars
        WHERE source = 'rithmic'
        ORDER BY trade_date
    """)
    dates = [row["trade_date"] for row in cur.fetchall()]
    print(f"Available Rithmic dates: {len(dates)}")
    for d in dates:
        print(f"  {d}")
    print()

    # Per-date results
    all_trades_a = []
    all_trades_b = []
    daily_summary = []

    for trade_date in dates:
        print(f"\n{'='*60}")
        print(f"Processing {trade_date}...")
        print(f"{'='*60}")

        # Pull bars for this date
        cur.execute("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                   cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
                   ts_start, ts_end, status
            FROM es_range_bars
            WHERE trade_date = %s AND source = 'rithmic'
            ORDER BY bar_idx
        """, (trade_date,))

        rows = cur.fetchall()
        if not rows:
            print(f"  No bars for {trade_date}")
            continue

        # Convert to bar dicts (matching the format setup_detector expects)
        bars = []
        for r in rows:
            bars.append({
                "idx": r["bar_idx"],
                "open": float(r["bar_open"]),
                "high": float(r["bar_high"]),
                "low": float(r["bar_low"]),
                "close": float(r["bar_close"]),
                "volume": int(r["bar_volume"]),
                "buy_volume": int(r["bar_buy_volume"] or 0),
                "sell_volume": int(r["bar_sell_volume"] or 0),
                "delta": float(r["bar_delta"] or 0),
                "cvd": float(r["cvd_close"] or r["cumulative_delta"] or 0),
                "cvd_open": float(r["cvd_open"] or 0),
                "cvd_high": float(r["cvd_high"] or 0),
                "cvd_low": float(r["cvd_low"] or 0),
                "cvd_close": float(r["cvd_close"] or r["cumulative_delta"] or 0),
                "ts_start": r["ts_start"],
                "ts_end": r["ts_end"],
                "status": r["status"] or "closed",
            })

        # Only use closed bars
        closed_bars = [b for b in bars if b["status"] == "closed"]
        print(f"  Total closed bars: {len(closed_bars)}")

        if len(closed_bars) < 25:
            print(f"  Not enough bars, skipping")
            continue

        # Build lookup for timestamps
        bars_lookup = {b["idx"]: b for b in closed_bars}

        # Initialize trackers for this day
        tracker_a = make_swing_tracker()
        tracker_b = make_swing_tracker()

        signals_a = []
        signals_b = []
        trades_a = []
        trades_b = []

        # Replay bar-by-bar
        for i in range(1, len(closed_bars) + 1):
            window = closed_bars[:i]
            current_bar = window[-1]

            # Check time window
            if not bar_in_signal_window(current_bar, trade_date):
                # Still update swings even outside window
                if len(window) >= A_PIVOT_LEFT + A_PIVOT_RIGHT + 1:
                    update_swings(tracker_a, window, A_PIVOT_LEFT, A_PIVOT_RIGHT)
                    update_swings(tracker_b, window, B_PIVOT_LEFT, B_PIVOT_RIGHT)
                continue

            # --- Approach A ---
            result_a = approach_a_evaluate(window, tracker_a)
            if result_a:
                if not check_cooldown(result_a, signals_a, bars_lookup):
                    signals_a.append(result_a)
                    remaining = closed_bars[i:]  # bars AFTER current
                    outcome = track_outcome(result_a, remaining)
                    trade = {**result_a, **outcome, "trade_date": str(trade_date)}
                    trades_a.append(trade)

            # --- Approach B ---
            result_b = approach_b_evaluate(window, tracker_b)
            if result_b:
                if not check_cooldown(result_b, signals_b, bars_lookup):
                    signals_b.append(result_b)
                    remaining = closed_bars[i:]
                    outcome = track_outcome(result_b, remaining)
                    trade = {**result_b, **outcome, "trade_date": str(trade_date)}
                    trades_b.append(trade)

        # Print daily trades
        print(f"\n  --- Approach A (Current) [{trade_date}] ---")
        print(f"  Signals: {len(trades_a)}")
        for t in trades_a:
            swing_info = f"swing@{t.get('swing_bar_idx','?')}"
            if t.get("ref_swing_bar_idx"):
                swing_info += f",ref@{t['ref_swing_bar_idx']}"
            print(f"    bar#{t['bar_idx']:>3d} {t['direction']:>7s} {t['pattern']:<20s} "
                  f"entry={t['entry']:.2f} vol={t['vol_ratio']:.1f}x z={t['cvd_z']:.2f} "
                  f"-> {t['outcome']:>7s} {t['pnl']:>+7.1f} (MFE={t['mfe']:.1f} MAE={t['mae']:.1f}) "
                  f"{swing_info}")

        print(f"\n  --- Approach B (Proposed) [{trade_date}] ---")
        print(f"  Signals: {len(trades_b)}")
        for t in trades_b:
            print(f"    bar#{t['bar_idx']:>3d} {t['direction']:>7s} {t['pattern']:<20s} "
                  f"entry={t['entry']:.2f} vol={t['vol_ratio']:.1f}x z={t['cvd_z']:.2f} "
                  f"-> {t['outcome']:>7s} {t['pnl']:>+7.1f} (MFE={t['mfe']:.1f} MAE={t['mae']:.1f}) "
                  f"swing@{t.get('swing_bar_idx','?')} {t.get('bars_away',0)}bars")

        # Daily stats
        a_wins = sum(1 for t in trades_a if t["outcome"] == "WIN")
        a_losses = sum(1 for t in trades_a if t["outcome"] == "LOSS")
        a_expired = sum(1 for t in trades_a if t["outcome"] == "EXPIRED")
        a_pnl = sum(t["pnl"] for t in trades_a)
        a_wr = a_wins / len(trades_a) * 100 if trades_a else 0

        b_wins = sum(1 for t in trades_b if t["outcome"] == "WIN")
        b_losses = sum(1 for t in trades_b if t["outcome"] == "LOSS")
        b_expired = sum(1 for t in trades_b if t["outcome"] == "EXPIRED")
        b_pnl = sum(t["pnl"] for t in trades_b)
        b_wr = b_wins / len(trades_b) * 100 if trades_b else 0

        daily_summary.append({
            "date": str(trade_date),
            "a_trades": len(trades_a), "a_wins": a_wins, "a_losses": a_losses,
            "a_expired": a_expired, "a_pnl": a_pnl, "a_wr": a_wr,
            "b_trades": len(trades_b), "b_wins": b_wins, "b_losses": b_losses,
            "b_expired": b_expired, "b_pnl": b_pnl, "b_wr": b_wr,
        })

        all_trades_a.extend(trades_a)
        all_trades_b.extend(trades_b)

    conn.close()

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    print("\n\n")
    print("=" * 100)
    print("DAILY COMPARISON TABLE")
    print("=" * 100)
    print(f"{'Date':<12} | {'A Trades':>8} {'A W/L/E':>10} {'A WR%':>6} {'A PnL':>8} | "
          f"{'B Trades':>8} {'B W/L/E':>10} {'B WR%':>6} {'B PnL':>8} | {'Delta PnL':>10}")
    print("-" * 100)

    for ds in daily_summary:
        a_wle = f"{ds['a_wins']}/{ds['a_losses']}/{ds['a_expired']}"
        b_wle = f"{ds['b_wins']}/{ds['b_losses']}/{ds['b_expired']}"
        delta = ds["b_pnl"] - ds["a_pnl"]
        print(f"{ds['date']:<12} | {ds['a_trades']:>8} {a_wle:>10} {ds['a_wr']:>5.1f}% {ds['a_pnl']:>+7.1f} | "
              f"{ds['b_trades']:>8} {b_wle:>10} {ds['b_wr']:>5.1f}% {ds['b_pnl']:>+7.1f} | {delta:>+9.1f}")

    print("-" * 100)

    # Totals
    total_a = len(all_trades_a)
    total_b = len(all_trades_b)
    a_w = sum(1 for t in all_trades_a if t["outcome"] == "WIN")
    a_l = sum(1 for t in all_trades_a if t["outcome"] == "LOSS")
    a_e = sum(1 for t in all_trades_a if t["outcome"] == "EXPIRED")
    b_w = sum(1 for t in all_trades_b if t["outcome"] == "WIN")
    b_l = sum(1 for t in all_trades_b if t["outcome"] == "LOSS")
    b_e = sum(1 for t in all_trades_b if t["outcome"] == "EXPIRED")
    a_total_pnl = sum(t["pnl"] for t in all_trades_a)
    b_total_pnl = sum(t["pnl"] for t in all_trades_b)
    a_total_wr = a_w / total_a * 100 if total_a else 0
    b_total_wr = b_w / total_b * 100 if total_b else 0

    a_wle_t = f"{a_w}/{a_l}/{a_e}"
    b_wle_t = f"{b_w}/{b_l}/{b_e}"
    delta_t = b_total_pnl - a_total_pnl

    print(f"{'TOTAL':<12} | {total_a:>8} {a_wle_t:>10} {a_total_wr:>5.1f}% {a_total_pnl:>+7.1f} | "
          f"{total_b:>8} {b_wle_t:>10} {b_total_wr:>5.1f}% {b_total_pnl:>+7.1f} | {delta_t:>+9.1f}")

    # MFE/MAE summary
    print("\n\n")
    print("=" * 80)
    print("MFE / MAE ANALYSIS")
    print("=" * 80)

    for label, trades in [("Approach A (Current)", all_trades_a), ("Approach B (Proposed)", all_trades_b)]:
        print(f"\n  {label} ({len(trades)} trades):")
        if not trades:
            print("    No trades")
            continue

        mfes = [t["mfe"] for t in trades]
        maes = [t["mae"] for t in trades]
        win_mfes = [t["mfe"] for t in trades if t["outcome"] == "WIN"]
        loss_maes = [t["mae"] for t in trades if t["outcome"] == "LOSS"]

        print(f"    Avg MFE: {sum(mfes)/len(mfes):.1f} | Median MFE: {statistics.median(mfes):.1f} | Max MFE: {max(mfes):.1f}")
        print(f"    Avg MAE: {sum(maes)/len(maes):.1f} | Median MAE: {statistics.median(maes):.1f} | Max MAE: {max(maes):.1f}")
        if win_mfes:
            print(f"    Win avg MFE: {sum(win_mfes)/len(win_mfes):.1f}")
        if loss_maes:
            print(f"    Loss avg MAE: {sum(loss_maes)/len(loss_maes):.1f}")

    # Pattern breakdown
    print("\n\n")
    print("=" * 80)
    print("PATTERN BREAKDOWN")
    print("=" * 80)

    for label, trades in [("Approach A (Current)", all_trades_a), ("Approach B (Proposed)", all_trades_b)]:
        print(f"\n  {label}:")
        patterns = defaultdict(lambda: {"count": 0, "wins": 0, "losses": 0, "expired": 0, "pnl": 0})
        for t in trades:
            p = t["pattern"]
            patterns[p]["count"] += 1
            patterns[p]["pnl"] += t["pnl"]
            if t["outcome"] == "WIN":
                patterns[p]["wins"] += 1
            elif t["outcome"] == "LOSS":
                patterns[p]["losses"] += 1
            else:
                patterns[p]["expired"] += 1

        if not patterns:
            print("    No trades")
            continue

        print(f"    {'Pattern':<25s} {'Count':>6} {'W/L/E':>10} {'WR%':>6} {'PnL':>8}")
        print(f"    {'-'*60}")
        for p, s in sorted(patterns.items(), key=lambda x: -x[1]["pnl"]):
            wr = s["wins"] / s["count"] * 100 if s["count"] else 0
            wle = f"{s['wins']}/{s['losses']}/{s['expired']}"
            print(f"    {p:<25s} {s['count']:>6} {wle:>10} {wr:>5.1f}% {s['pnl']:>+7.1f}")

    # Direction breakdown
    print("\n\n")
    print("=" * 80)
    print("DIRECTION BREAKDOWN")
    print("=" * 80)

    for label, trades in [("Approach A (Current)", all_trades_a), ("Approach B (Proposed)", all_trades_b)]:
        print(f"\n  {label}:")
        for dir_name in ["bullish", "bearish"]:
            dir_trades = [t for t in trades if t["direction"] == dir_name]
            if not dir_trades:
                continue
            wins = sum(1 for t in dir_trades if t["outcome"] == "WIN")
            losses = sum(1 for t in dir_trades if t["outcome"] == "LOSS")
            expired = sum(1 for t in dir_trades if t["outcome"] == "EXPIRED")
            pnl = sum(t["pnl"] for t in dir_trades)
            wr = wins / len(dir_trades) * 100
            print(f"    {dir_name:<10s}: {len(dir_trades)} trades, "
                  f"{wins}W/{losses}L/{expired}E, WR={wr:.1f}%, PnL={pnl:+.1f}")

    # Signal timing: avg bars from swing to trigger
    print("\n\n")
    print("=" * 80)
    print("SIGNAL PROXIMITY ANALYSIS")
    print("=" * 80)

    print("\n  Approach A — distance from trigger bar to most recent swing in pair:")
    a_dists = [t["bar_idx"] - t["swing_bar_idx"] for t in all_trades_a if "swing_bar_idx" in t]
    if a_dists:
        print(f"    Avg: {sum(a_dists)/len(a_dists):.1f} bars | "
              f"Median: {statistics.median(a_dists):.1f} | "
              f"Min: {min(a_dists)} | Max: {max(a_dists)}")
        # Win vs loss distances
        a_win_dists = [t["bar_idx"] - t["swing_bar_idx"] for t in all_trades_a if t["outcome"] == "WIN"]
        a_loss_dists = [t["bar_idx"] - t["swing_bar_idx"] for t in all_trades_a if t["outcome"] == "LOSS"]
        if a_win_dists:
            print(f"    Win avg distance: {sum(a_win_dists)/len(a_win_dists):.1f} bars")
        if a_loss_dists:
            print(f"    Loss avg distance: {sum(a_loss_dists)/len(a_loss_dists):.1f} bars")

    print("\n  Approach B — distance from trigger bar to swing reference:")
    b_dists = [t.get("bars_away", t["bar_idx"] - t["swing_bar_idx"]) for t in all_trades_b]
    if b_dists:
        print(f"    Avg: {sum(b_dists)/len(b_dists):.1f} bars | "
              f"Median: {statistics.median(b_dists):.1f} | "
              f"Min: {min(b_dists)} | Max: {max(b_dists)}")
        b_win_dists = [t.get("bars_away", t["bar_idx"] - t["swing_bar_idx"])
                       for t in all_trades_b if t["outcome"] == "WIN"]
        b_loss_dists = [t.get("bars_away", t["bar_idx"] - t["swing_bar_idx"])
                        for t in all_trades_b if t["outcome"] == "LOSS"]
        if b_win_dists:
            print(f"    Win avg distance: {sum(b_win_dists)/len(b_win_dists):.1f} bars")
        if b_loss_dists:
            print(f"    Loss avg distance: {sum(b_loss_dists)/len(b_loss_dists):.1f} bars")

    # Overlap analysis: how many signals are shared?
    print("\n\n")
    print("=" * 80)
    print("OVERLAP ANALYSIS")
    print("=" * 80)

    a_bar_set = set((t["trade_date"], t["bar_idx"], t["direction"]) for t in all_trades_a)
    b_bar_set = set((t["trade_date"], t["bar_idx"], t["direction"]) for t in all_trades_b)
    overlap = a_bar_set & b_bar_set
    only_a = a_bar_set - b_bar_set
    only_b = b_bar_set - a_bar_set

    print(f"  Both A and B: {len(overlap)} signals")
    print(f"  Only A:        {len(only_a)} signals")
    print(f"  Only B:        {len(only_b)} signals")

    # PnL for overlapping vs unique
    overlap_a_pnl = sum(t["pnl"] for t in all_trades_a
                        if (t["trade_date"], t["bar_idx"], t["direction"]) in overlap)
    overlap_b_pnl = sum(t["pnl"] for t in all_trades_b
                        if (t["trade_date"], t["bar_idx"], t["direction"]) in overlap)
    only_a_pnl = sum(t["pnl"] for t in all_trades_a
                     if (t["trade_date"], t["bar_idx"], t["direction"]) in only_a)
    only_b_pnl = sum(t["pnl"] for t in all_trades_b
                     if (t["trade_date"], t["bar_idx"], t["direction"]) in only_b)

    print(f"\n  Overlap PnL — A: {overlap_a_pnl:+.1f}, B: {overlap_b_pnl:+.1f}")
    print(f"  Only-A PnL: {only_a_pnl:+.1f}")
    print(f"  Only-B PnL: {only_b_pnl:+.1f}")

    # Only-A trade details
    if only_a:
        print(f"\n  Trades ONLY in A (not in B):")
        for t in all_trades_a:
            key = (t["trade_date"], t["bar_idx"], t["direction"])
            if key in only_a:
                dist = t["bar_idx"] - t.get("swing_bar_idx", 0)
                print(f"    {t['trade_date']} bar#{t['bar_idx']:>3d} {t['direction']:>7s} "
                      f"{t['pattern']:<20s} dist={dist} -> {t['outcome']} {t['pnl']:+.1f}")

    if only_b:
        print(f"\n  Trades ONLY in B (not in A):")
        for t in all_trades_b:
            key = (t["trade_date"], t["bar_idx"], t["direction"])
            if key in only_b:
                print(f"    {t['trade_date']} bar#{t['bar_idx']:>3d} {t['direction']:>7s} "
                      f"{t['pattern']:<20s} dist={t.get('bars_away',0)} -> {t['outcome']} {t['pnl']:+.1f}")

    print("\n\n" + "=" * 80)
    print("FINAL VERDICT")
    print("=" * 80)
    print(f"  Approach A: {total_a} trades, {a_total_wr:.1f}% WR, {a_total_pnl:+.1f} pts")
    print(f"  Approach B: {total_b} trades, {b_total_wr:.1f}% WR, {b_total_pnl:+.1f} pts")
    print(f"  Delta:      {delta_t:+.1f} pts ({'B wins' if delta_t > 0 else 'A wins'})")
    if total_a and total_b:
        a_per = a_total_pnl / total_a
        b_per = b_total_pnl / total_b
        print(f"  Per-trade:  A={a_per:+.2f} pts/trade, B={b_per:+.2f} pts/trade")
    print("=" * 80)


if __name__ == "__main__":
    main()
