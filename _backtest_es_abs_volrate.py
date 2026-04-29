"""
Backtest: ES Absorption — Raw Volume Gate vs Volume Rate Gate
=============================================================
Replicates the production evaluate_absorption() logic from setup_detector.py
with three scenarios:
  A) Raw volume gate (current): vol / avg_20_vol >= 1.4
  B) Rate volume gate (proposed): vol_per_sec / avg_20_vol_rate >= 1.4
  C) Both must pass: A AND B

All other criteria identical:
  - 8-bar lookback for CVD divergence
  - Same normalization (min-max over lookback window)
  - Same divergence thresholds (cvd_norm > 0.15 or < -0.15, gap > 0.2)
  - Same cooldown (10 bars same-direction)
  - SL=8, Target=10, timeout=60 bars

Volland confluence zeroed (conservative — no DD, paradigm, LIS data in CSV).

Data: G:/My Drive/Python/MyProject/GitHub/0dtealpha/exports/es_range_bars_march_volrate.csv
Source: Rithmic ES 5-pt range bars, March 2026
"""

import csv
import os
from collections import defaultdict
from datetime import datetime, time as dtime

CSV_PATH = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\exports\es_range_bars_march_volrate.csv"
OUT_PATH = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\exports\es_abs_backtest_volrate.csv"

# ── Production detector constants ──────────────────────────────────
LOOKBACK = 8
VOL_WINDOW = 20
MIN_VOL_RATIO = 1.4    # main.py override (default 1.5 in setup_detector, overridden to 1.4)
MIN_BARS = VOL_WINDOW + LOOKBACK  # 28

# Divergence thresholds
CVD_NORM_THRESH = 0.15
DIV_GAP_THRESH = 0.2

# Cooldown
COOLDOWN_BARS = 10

# Outcome simulation
SL_PTS = 8
TARGET_PTS = 10
TIMEOUT_BARS = 60

# Market hours filter (ET)
MARKET_OPEN = dtime(9, 30)
MARKET_CLOSE = dtime(16, 0)


def load_csv():
    """Load CSV and return list of bar dicts with parsed numeric fields."""
    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            try:
                bar = {
                    "date": r["date"],
                    "time_et": r["time_et"],
                    "bar_idx": int(r["bar_idx"]),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": int(r["volume"]),
                    "delta": int(r["delta"]),
                    "buy_volume": int(r["buy_volume"]),
                    "sell_volume": int(r["sell_volume"]),
                    "duration_sec": float(r["duration_sec"]),
                    "vol_per_sec": float(r["vol_per_sec"]),
                    "cvd": int(r["cvd"]),
                    "cvd_open": int(r["cvd_open"]),
                    "cvd_high": int(r["cvd_high"]),
                    "cvd_low": int(r["cvd_low"]),
                    "cvd_close": int(r["cvd_close"]),
                    "ts_start_utc": r["ts_start_utc"],
                    "ts_end_utc": r["ts_end_utc"],
                }
                rows.append(bar)
            except (ValueError, KeyError):
                continue
    return rows


def is_market_hours(time_str):
    """Check if time_et string is within 9:30-16:00 ET."""
    parts = time_str.split(":")
    h, m = int(parts[0]), int(parts[1])
    t = dtime(h, m)
    return MARKET_OPEN <= t <= MARKET_CLOSE


def get_time_bucket(time_str):
    """Return time bucket for reporting."""
    parts = time_str.split(":")
    h, m = int(parts[0]), int(parts[1])
    t = dtime(h, m)
    if t < dtime(11, 30):
        return "09:30-11:30"
    elif t < dtime(14, 0):
        return "11:30-14:00"
    else:
        return "14:00-16:00"


def deduplicate_bars(bars_by_date):
    """For dates with duplicate bar_idx, keep only one source.

    Strategy: For dates with overlapping idx, separate into two sequences
    by building a monotonically increasing idx chain. Keep the sequence
    with more market-hours bars (the live/primary source).

    For dates without duplicates, keep all bars as-is.
    """
    clean = {}
    for date_str, bars in bars_by_date.items():
        from collections import Counter
        idx_counts = Counter(b["bar_idx"] for b in bars)
        dupes = {k for k, v in idx_counts.items() if v > 1}

        if not dupes:
            # No duplicates — single source, keep all
            clean[date_str] = sorted(bars, key=lambda b: b["ts_start_utc"])
            continue

        # Has duplicates — two interleaved sources
        # Sort by timestamp
        sorted_bars = sorted(bars, key=lambda b: b["ts_start_utc"])

        # Build two sequences: when idx jumps backward significantly, switch
        seq_a = []
        seq_b = []
        current_seq = "a"
        last_idx_a = -1
        last_idx_b = -1

        for b in sorted_bars:
            idx = b["bar_idx"]
            # Try to assign to the sequence where idx fits monotonically
            fits_a = (idx > last_idx_a - 2)  # allow small jitter
            fits_b = (idx > last_idx_b - 2)

            if fits_a and (not fits_b or len(seq_a) <= len(seq_b)):
                seq_a.append(b)
                last_idx_a = idx
            elif fits_b:
                seq_b.append(b)
                last_idx_b = idx
            else:
                # Neither fits cleanly, add to shorter
                if len(seq_a) <= len(seq_b):
                    seq_a.append(b)
                    last_idx_a = idx
                else:
                    seq_b.append(b)
                    last_idx_b = idx

        # Pick the sequence with more market-hours bars
        mh_a = sum(1 for b in seq_a if is_market_hours(b["time_et"]))
        mh_b = sum(1 for b in seq_b if is_market_hours(b["time_et"]))

        if mh_a >= mh_b:
            clean[date_str] = seq_a
        else:
            clean[date_str] = seq_b

    return clean


def evaluate_divergence(window):
    """Replicate production divergence logic on a lookback window.

    Args:
        window: list of bar dicts (lookback+1 bars)

    Returns:
        (direction, div_raw, div_gap, cvd_norm, price_norm) or (None, 0, 0, 0, 0)
    """
    lows = [b["low"] for b in window]
    highs = [b["high"] for b in window]
    cvds = [b["cvd"] for b in window]

    cvd_start, cvd_end = cvds[0], cvds[-1]
    cvd_slope = cvd_end - cvd_start
    cvd_range = max(cvds) - min(cvds)
    if cvd_range == 0:
        return None, 0, 0.0, 0.0, 0.0

    price_low_start, price_low_end = lows[0], lows[-1]
    price_high_start, price_high_end = highs[0], highs[-1]
    price_range = max(highs) - min(lows)
    if price_range == 0:
        return None, 0, 0.0, 0.0, 0.0

    cvd_norm = cvd_slope / cvd_range
    price_low_norm = (price_low_end - price_low_start) / price_range
    price_high_norm = (price_high_end - price_high_start) / price_range

    direction = None
    div_raw = 0
    div_gap = 0.0

    if cvd_norm < -CVD_NORM_THRESH:
        gap = price_low_norm - cvd_norm
        if gap > DIV_GAP_THRESH:
            direction = "bullish"
            div_gap = gap
            if gap > 1.2:
                div_raw = 4
            elif gap > 0.8:
                div_raw = 3
            elif gap > 0.4:
                div_raw = 2
            else:
                div_raw = 1

    if cvd_norm > CVD_NORM_THRESH and direction is None:
        gap = cvd_norm - price_high_norm
        if gap > DIV_GAP_THRESH:
            direction = "bearish"
            div_gap = gap
            if gap > 1.2:
                div_raw = 4
            elif gap > 0.8:
                div_raw = 3
            elif gap > 0.4:
                div_raw = 2
            else:
                div_raw = 1

    return direction, div_raw, div_gap, cvd_norm, price_low_norm if direction == "bullish" else price_high_norm


def compute_grade_estimate(div_raw, vol_ratio):
    """Estimate grade with zeroed Volland (DD=0, paradigm=0, LIS=0).

    Production scoring v2 uses: paradigm(0-25) + direction(5-15) + time(0-20) + align(0-20) + vix(0-15)
    Without Volland, we can only score direction and a rough VIX.
    For consistency, we'll just use the div+vol weighted composite from v1 scoring.
    """
    # v1-style composite (only divergence 25% + volume 25%)
    div_score = {0: 0, 1: 25, 2: 50, 3: 75, 4: 100}.get(div_raw, 0)
    if vol_ratio >= 3.0:
        vol_score = 100
    elif vol_ratio >= 2.0:
        vol_score = 67
    else:
        vol_score = 33
    # With zeroed Volland components: weighted = div*0.25 + vol*0.25 + 0 + 0 + 0
    composite = div_score * 0.25 + vol_score * 0.25
    # Scale to 0-100 (max possible with 50% weight = 50)
    # Map to grade using production thresholds on the full 0-100 scale
    # But since max=50, we'd never get A+. Just report the raw composite.
    if composite >= 37.5:
        grade = "A+"
    elif composite >= 27.5:
        grade = "A"
    elif composite >= 17.5:
        grade = "B"
    elif composite >= 10:
        grade = "C"
    else:
        grade = "LOG"
    return grade, round(composite, 1)


def simulate_outcome(entry_price, direction, subsequent_bars):
    """Simulate fixed SL/Target outcome on subsequent bars.

    Returns: (outcome, pnl, mfe, mae, bars_held)
    """
    if direction == "bullish":
        target_price = entry_price + TARGET_PTS
        stop_price = entry_price - SL_PTS
    else:
        target_price = entry_price - TARGET_PTS
        stop_price = entry_price + SL_PTS

    mfe = 0.0
    mae = 0.0

    for i, bar in enumerate(subsequent_bars):
        if i >= TIMEOUT_BARS:
            break

        if direction == "bullish":
            unrealized_high = bar["high"] - entry_price
            unrealized_low = bar["low"] - entry_price
            mfe = max(mfe, unrealized_high)
            mae = min(mae, unrealized_low)

            hit_target = bar["high"] >= target_price
            hit_stop = bar["low"] <= stop_price
        else:
            unrealized_high = entry_price - bar["low"]
            unrealized_low = entry_price - bar["high"]
            mfe = max(mfe, unrealized_high)
            mae = min(mae, unrealized_low)

            hit_target = bar["low"] <= target_price
            hit_stop = bar["high"] >= stop_price

        if hit_target and hit_stop:
            # Both hit on same bar — check which is closer to open
            if direction == "bullish":
                dist_to_target = abs(bar["open"] - target_price)
                dist_to_stop = abs(bar["open"] - stop_price)
            else:
                dist_to_target = abs(bar["open"] - target_price)
                dist_to_stop = abs(bar["open"] - stop_price)
            if dist_to_target <= dist_to_stop:
                return "WIN", TARGET_PTS, mfe, mae, i + 1
            else:
                return "LOSS", -SL_PTS, mfe, mae, i + 1
        elif hit_target:
            return "WIN", TARGET_PTS, mfe, mae, i + 1
        elif hit_stop:
            return "LOSS", -SL_PTS, mfe, mae, i + 1

    # Timeout — exit at last bar's close
    if direction == "bullish":
        pnl = subsequent_bars[min(TIMEOUT_BARS - 1, len(subsequent_bars) - 1)]["close"] - entry_price
    else:
        pnl = entry_price - subsequent_bars[min(TIMEOUT_BARS - 1, len(subsequent_bars) - 1)]["close"]
    return "EXPIRED", round(pnl, 2), mfe, mae, min(TIMEOUT_BARS, len(subsequent_bars))


def run_detector(bars, vol_gate_mode="raw"):
    """Run ES Absorption detector on a day's bars.

    vol_gate_mode: "raw" (current), "rate" (proposed), "both"

    Returns list of signal dicts.
    """
    signals = []
    last_checked_idx = -1
    last_bullish_bar = -100
    last_bearish_bar = -100

    for i in range(MIN_BARS, len(bars)):
        trigger = bars[i]
        trigger_idx = trigger["bar_idx"]

        # Skip already checked
        if trigger_idx <= last_checked_idx:
            continue
        last_checked_idx = trigger_idx

        # Skip pre/post market
        if not is_market_hours(trigger["time_et"]):
            continue

        # --- Volume gate (compute from bars, not CSV pre-computed) ---
        recent_bars = bars[max(0, i - VOL_WINDOW):i]
        if len(recent_bars) < VOL_WINDOW:
            continue

        # Raw volume gate
        vol_avg = sum(b["volume"] for b in recent_bars) / len(recent_bars)
        if vol_avg <= 0:
            continue
        vol_ratio_raw = trigger["volume"] / vol_avg

        # Rate volume gate
        rates = [b["vol_per_sec"] for b in recent_bars]
        rate_avg = sum(rates) / len(rates)
        if rate_avg <= 0:
            continue
        trigger_rate = trigger["vol_per_sec"]
        vol_ratio_rate = trigger_rate / rate_avg

        # Apply gate based on mode
        if vol_gate_mode == "raw":
            if vol_ratio_raw < MIN_VOL_RATIO:
                continue
        elif vol_gate_mode == "rate":
            if vol_ratio_rate < MIN_VOL_RATIO:
                continue
        elif vol_gate_mode == "both":
            if vol_ratio_raw < MIN_VOL_RATIO or vol_ratio_rate < MIN_VOL_RATIO:
                continue

        # --- Divergence over lookback window ---
        window = bars[i - LOOKBACK:i + 1]  # lookback+1 bars (inclusive of trigger)
        direction, div_raw, div_gap, cvd_norm, price_norm = evaluate_divergence(window)
        if direction is None:
            continue

        # --- Cooldown (10 bars same-direction) ---
        if direction == "bullish":
            if trigger_idx - last_bullish_bar < COOLDOWN_BARS:
                continue
            last_bullish_bar = trigger_idx
        else:
            if trigger_idx - last_bearish_bar < COOLDOWN_BARS:
                continue
            last_bearish_bar = trigger_idx

        # --- Volume spike score ---
        if vol_ratio_raw >= 3.0:
            vol_raw = 3
        elif vol_ratio_raw >= 2.0:
            vol_raw = 2
        else:
            vol_raw = 1

        # --- Grade estimate (Volland zeroed) ---
        grade, score = compute_grade_estimate(div_raw, vol_ratio_raw)

        # --- Simulate outcome ---
        subsequent = bars[i + 1:]
        if not subsequent:
            outcome, pnl, mfe, mae, bars_held = "EXPIRED", 0.0, 0.0, 0.0, 0
        else:
            outcome, pnl, mfe, mae, bars_held = simulate_outcome(trigger["close"], direction, subsequent)

        signals.append({
            "date": trigger["date"],
            "time_et": trigger["time_et"],
            "bar_idx": trigger_idx,
            "direction": "long" if direction == "bullish" else "short",
            "entry_price": trigger["close"],
            "vol_ratio_raw": round(vol_ratio_raw, 4),
            "vol_ratio_rate": round(vol_ratio_rate, 4),
            "vol_per_sec": round(trigger_rate, 4),
            "duration_sec": round(trigger["duration_sec"], 2),
            "volume": trigger["volume"],
            "div_raw": div_raw,
            "div_gap": round(div_gap, 4),
            "cvd_norm": round(cvd_norm, 4),
            "grade_estimate": grade,
            "score_estimate": score,
            "outcome": outcome,
            "pnl": round(pnl, 2),
            "mfe": round(mfe, 2),
            "mae": round(mae, 2),
            "bars_held": bars_held,
        })

    return signals


def print_summary(signals, label):
    """Print summary statistics for a signal list."""
    if not signals:
        print(f"\n{'='*60}")
        print(f"  {label}: NO SIGNALS")
        print(f"{'='*60}")
        return

    total = len(signals)
    wins = sum(1 for s in signals if s["outcome"] == "WIN")
    losses = sum(1 for s in signals if s["outcome"] == "LOSS")
    expired = sum(1 for s in signals if s["outcome"] == "EXPIRED")
    wr = wins / total * 100 if total > 0 else 0
    total_pnl = sum(s["pnl"] for s in signals)
    avg_mfe = sum(s["mfe"] for s in signals) / total
    avg_mae = sum(s["mae"] for s in signals) / total

    # Max drawdown (running PnL)
    running = 0
    peak = 0
    max_dd = 0
    for s in sorted(signals, key=lambda x: (x["date"], x["time_et"])):
        running += s["pnl"]
        peak = max(peak, running)
        dd = peak - running
        max_dd = max(max_dd, dd)

    # Unique dates
    dates = set(s["date"] for s in signals)
    avg_per_day = total / len(dates) if dates else 0

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"  Total signals:   {total}")
    print(f"  Win/Loss/Exp:    {wins}W / {losses}L / {expired}E")
    print(f"  Win Rate:        {wr:.1f}%")
    print(f"  Total PnL:       {total_pnl:+.1f} pts")
    print(f"  Max Drawdown:    {max_dd:.1f} pts")
    print(f"  Avg MFE / MAE:   {avg_mfe:+.1f} / {avg_mae:+.1f}")
    print(f"  Trading days:    {len(dates)}")
    print(f"  Avg signals/day: {avg_per_day:.1f}")

    # By direction
    for d_label, d_val in [("Longs", "long"), ("Shorts", "short")]:
        d_sigs = [s for s in signals if s["direction"] == d_val]
        if not d_sigs:
            print(f"\n  {d_label}: 0 signals")
            continue
        d_wins = sum(1 for s in d_sigs if s["outcome"] == "WIN")
        d_wr = d_wins / len(d_sigs) * 100
        d_pnl = sum(s["pnl"] for s in d_sigs)
        # MaxDD for direction
        r = 0; p = 0; dd = 0
        for s in sorted(d_sigs, key=lambda x: (x["date"], x["time_et"])):
            r += s["pnl"]; p = max(p, r); dd = max(dd, p - r)
        print(f"\n  {d_label}: {len(d_sigs)} signals, {d_wr:.1f}% WR, {d_pnl:+.1f} pts, MaxDD={dd:.1f}")

    # By time of day
    print(f"\n  By Time of Day:")
    for bucket in ["09:30-11:30", "11:30-14:00", "14:00-16:00"]:
        b_sigs = [s for s in signals if get_time_bucket(s["time_et"]) == bucket]
        if not b_sigs:
            print(f"    {bucket}: 0 signals")
            continue
        b_wins = sum(1 for s in b_sigs if s["outcome"] == "WIN")
        b_wr = b_wins / len(b_sigs) * 100
        b_pnl = sum(s["pnl"] for s in b_sigs)
        print(f"    {bucket}: {len(b_sigs)} signals, {b_wr:.1f}% WR, {b_pnl:+.1f} pts")

    # By div_raw
    print(f"\n  By div_raw:")
    for dr in [1, 2, 3, 4]:
        dr_sigs = [s for s in signals if s["div_raw"] == dr]
        if not dr_sigs:
            print(f"    div_raw={dr}: 0 signals")
            continue
        dr_wins = sum(1 for s in dr_sigs if s["outcome"] == "WIN")
        dr_wr = dr_wins / len(dr_sigs) * 100
        dr_pnl = sum(s["pnl"] for s in dr_sigs)
        print(f"    div_raw={dr}: {len(dr_sigs)} signals, {dr_wr:.1f}% WR, {dr_pnl:+.1f} pts")


def compare_signal_sets(raw_signals, rate_signals, label_new, label_old):
    """Compare two signal sets and report new/lost signals."""
    # Create keys for matching
    raw_keys = {(s["date"], s["bar_idx"]) for s in raw_signals}
    rate_keys = {(s["date"], s["bar_idx"]) for s in rate_signals}

    new_keys = rate_keys - raw_keys
    lost_keys = raw_keys - rate_keys
    shared_keys = raw_keys & rate_keys

    new_signals = [s for s in rate_signals if (s["date"], s["bar_idx"]) in new_keys]
    lost_signals = [s for s in raw_signals if (s["date"], s["bar_idx"]) in lost_keys]

    print(f"\n{'='*60}")
    print(f"  {label_new} vs {label_old} — Differential Analysis")
    print(f"{'='*60}")
    print(f"  Shared signals:  {len(shared_keys)}")
    print(f"  NEW signals ({label_new} only):  {len(new_signals)}")
    print(f"  LOST signals ({label_old} only): {len(lost_signals)}")

    if new_signals:
        nw = sum(1 for s in new_signals if s["outcome"] == "WIN")
        nwr = nw / len(new_signals) * 100
        npnl = sum(s["pnl"] for s in new_signals)
        print(f"\n  NEW signals detail: {len(new_signals)} signals, {nwr:.1f}% WR, {npnl:+.1f} pts")

        # Top 5 best
        best = sorted(new_signals, key=lambda s: s["pnl"], reverse=True)[:5]
        print(f"\n  Top 5 BEST new signals:")
        for s in best:
            print(f"    {s['date']} {s['time_et']} {s['direction']} entry={s['entry_price']:.2f} "
                  f"vrRate={s['vol_ratio_rate']:.2f} vrRaw={s['vol_ratio_raw']:.2f} "
                  f"div={s['div_raw']} -> {s['outcome']} {s['pnl']:+.1f} pts")

        # Top 5 worst
        worst = sorted(new_signals, key=lambda s: s["pnl"])[:5]
        print(f"\n  Top 5 WORST new signals:")
        for s in worst:
            print(f"    {s['date']} {s['time_et']} {s['direction']} entry={s['entry_price']:.2f} "
                  f"vrRate={s['vol_ratio_rate']:.2f} vrRaw={s['vol_ratio_raw']:.2f} "
                  f"div={s['div_raw']} -> {s['outcome']} {s['pnl']:+.1f} pts")

    if lost_signals:
        lw = sum(1 for s in lost_signals if s["outcome"] == "WIN")
        lwr = lw / len(lost_signals) * 100
        lpnl = sum(s["pnl"] for s in lost_signals)
        print(f"\n  LOST signals detail: {len(lost_signals)} signals, {lwr:.1f}% WR, {lpnl:+.1f} pts")

        # Show all if <= 10, else top 5 best and worst
        if len(lost_signals) <= 10:
            print(f"\n  All LOST signals:")
            for s in sorted(lost_signals, key=lambda x: (x["date"], x["time_et"])):
                print(f"    {s['date']} {s['time_et']} {s['direction']} entry={s['entry_price']:.2f} "
                      f"vrRaw={s['vol_ratio_raw']:.2f} vrRate={s['vol_ratio_rate']:.2f} "
                      f"div={s['div_raw']} -> {s['outcome']} {s['pnl']:+.1f} pts")
        else:
            best = sorted(lost_signals, key=lambda s: s["pnl"], reverse=True)[:5]
            print(f"\n  Top 5 BEST lost signals:")
            for s in best:
                print(f"    {s['date']} {s['time_et']} {s['direction']} entry={s['entry_price']:.2f} "
                      f"vrRaw={s['vol_ratio_raw']:.2f} vrRate={s['vol_ratio_rate']:.2f} "
                      f"div={s['div_raw']} -> {s['outcome']} {s['pnl']:+.1f} pts")
            worst = sorted(lost_signals, key=lambda s: s["pnl"])[:5]
            print(f"\n  Top 5 WORST lost signals:")
            for s in worst:
                print(f"    {s['date']} {s['time_et']} {s['direction']} entry={s['entry_price']:.2f} "
                      f"vrRaw={s['vol_ratio_raw']:.2f} vrRate={s['vol_ratio_rate']:.2f} "
                      f"div={s['div_raw']} -> {s['outcome']} {s['pnl']:+.1f} pts")


def check_mar27_1045(all_bars_by_date, raw_signals, rate_signals):
    """Specifically check March 27 around 10:45 ET."""
    print(f"\n{'='*60}")
    print(f"  March 27 ~10:45 ET — Special Check")
    print(f"{'='*60}")

    if "2026-03-27" not in all_bars_by_date:
        print("  No data for Mar 27!")
        return

    bars = all_bars_by_date["2026-03-27"]
    # Show bars around 10:40-10:50
    print(f"\n  Bars 10:40-10:55 ET:")
    for b in bars:
        t = b["time_et"]
        h, m = int(t.split(":")[0]), int(t.split(":")[1])
        if h == 10 and 40 <= m <= 55:
            # Compute vol ratios for this bar
            idx_in_list = bars.index(b)
            if idx_in_list >= VOL_WINDOW:
                recent = bars[idx_in_list - VOL_WINDOW:idx_in_list]
                va = sum(x["volume"] for x in recent) / len(recent)
                ra = sum(x["vol_per_sec"] for x in recent) / len(recent)
                vr = b["volume"] / va if va > 0 else 0
                rr = b["vol_per_sec"] / ra if ra > 0 else 0
            else:
                vr, rr = 0, 0
            print(f"    idx={b['bar_idx']} {t} vol={b['volume']} dur={b['duration_sec']:.1f}s "
                  f"vps={b['vol_per_sec']:.1f} vrRaw={vr:.2f} vrRate={rr:.2f} cvd={b['cvd']}")

    # Check if any signals fired near 10:45
    raw_1045 = [s for s in raw_signals if s["date"] == "2026-03-27"
                and 1040 <= int(s["time_et"].replace(":", "")[:4]) <= 1050]
    rate_1045 = [s for s in rate_signals if s["date"] == "2026-03-27"
                 and 1040 <= int(s["time_et"].replace(":", "")[:4]) <= 1050]

    print(f"\n  Raw gate signals 10:40-10:50: {len(raw_1045)}")
    for s in raw_1045:
        print(f"    {s['time_et']} {s['direction']} entry={s['entry_price']:.2f} vrRaw={s['vol_ratio_raw']:.2f} "
              f"div={s['div_raw']} -> {s['outcome']} {s['pnl']:+.1f}")

    print(f"  Rate gate signals 10:40-10:50: {len(rate_1045)}")
    for s in rate_1045:
        print(f"    {s['time_et']} {s['direction']} entry={s['entry_price']:.2f} vrRate={s['vol_ratio_rate']:.2f} "
              f"div={s['div_raw']} -> {s['outcome']} {s['pnl']:+.1f}")

    # Wider check 10:30-11:00
    raw_wide = [s for s in raw_signals if s["date"] == "2026-03-27"
                and "10:" in s["time_et"]]
    rate_wide = [s for s in rate_signals if s["date"] == "2026-03-27"
                 and "10:" in s["time_et"]]
    print(f"\n  All 10:xx signals — Raw: {len(raw_wide)}, Rate: {len(rate_wide)}")
    for s in rate_wide:
        in_raw = any(r["bar_idx"] == s["bar_idx"] for r in raw_wide)
        tag = "" if in_raw else " [NEW]"
        print(f"    {s['time_et']} {s['direction']} idx={s['bar_idx']} entry={s['entry_price']:.2f} "
              f"vrRaw={s['vol_ratio_raw']:.2f} vrRate={s['vol_ratio_rate']:.2f} "
              f"div={s['div_raw']} -> {s['outcome']} {s['pnl']:+.1f}{tag}")


def save_csv(all_signals, path):
    """Save all signals to CSV."""
    if not all_signals:
        print(f"\nNo signals to save.")
        return

    fieldnames = [
        "date", "time_et", "bar_idx", "direction", "entry_price", "scenario",
        "vol_ratio_raw", "vol_ratio_rate", "vol_per_sec", "duration_sec",
        "volume", "div_raw", "div_gap", "cvd_norm", "grade_estimate",
        "score_estimate", "outcome", "pnl", "mfe", "mae", "bars_held",
    ]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for s in sorted(all_signals, key=lambda x: (x["date"], x["time_et"], x["scenario"])):
            writer.writerow({k: s.get(k, "") for k in fieldnames})

    print(f"\nSaved {len(all_signals)} signals to {path}")


def main():
    print("Loading CSV...")
    rows = load_csv()
    print(f"Loaded {len(rows)} bars")

    # Group by date
    by_date = defaultdict(list)
    for r in rows:
        by_date[r["date"]].append(r)

    # Deduplicate sources
    clean_by_date = deduplicate_bars(dict(by_date))
    total_clean = sum(len(v) for v in clean_by_date.values())
    print(f"After dedup: {total_clean} bars across {len(clean_by_date)} dates")

    # Filter to market days only (skip weekends — dates with < 50 market-hours bars)
    trading_dates = {}
    for d, bars in sorted(clean_by_date.items()):
        mh_bars = [b for b in bars if is_market_hours(b["time_et"])]
        if len(mh_bars) >= 20:  # need at least MIN_BARS market-hours bars
            trading_dates[d] = bars  # keep ALL bars (pre-market needed for lookback)
    print(f"Trading dates: {len(trading_dates)} ({min(trading_dates.keys())} to {max(trading_dates.keys())})")

    # Run all three scenarios
    all_signals = []

    for scenario, mode, label in [
        ("A", "raw", "Scenario A: Raw Volume Gate (current)"),
        ("B", "rate", "Scenario B: Rate Volume Gate (proposed)"),
        ("C", "both", "Scenario C: Both Gates Must Pass"),
    ]:
        scenario_signals = []
        for date_str in sorted(trading_dates.keys()):
            bars = trading_dates[date_str]
            signals = run_detector(bars, vol_gate_mode=mode)
            for s in signals:
                s["scenario"] = scenario
            scenario_signals.extend(signals)

        print_summary(scenario_signals, label)

        for s in scenario_signals:
            all_signals.append(s)

        if scenario == "A":
            raw_signals = scenario_signals
        elif scenario == "B":
            rate_signals = scenario_signals

    # Differential analysis
    compare_signal_sets(raw_signals, rate_signals, "Rate (B)", "Raw (A)")

    # March 27 special check
    check_mar27_1045(trading_dates, raw_signals, rate_signals)

    # Save CSV
    save_csv(all_signals, OUT_PATH)

    # Summary table
    print(f"\n{'='*60}")
    print(f"  COMPARISON SUMMARY")
    print(f"{'='*60}")
    print(f"  {'Metric':<20} {'A (Raw)':<15} {'B (Rate)':<15} {'C (Both)':<15}")
    print(f"  {'-'*20} {'-'*15} {'-'*15} {'-'*15}")

    for label, sigs in [
        ("A (Raw)", [s for s in all_signals if s["scenario"] == "A"]),
        ("B (Rate)", [s for s in all_signals if s["scenario"] == "B"]),
        ("C (Both)", [s for s in all_signals if s["scenario"] == "C"]),
    ]:
        pass  # data collected above

    a_sigs = [s for s in all_signals if s["scenario"] == "A"]
    b_sigs = [s for s in all_signals if s["scenario"] == "B"]
    c_sigs = [s for s in all_signals if s["scenario"] == "C"]

    for metric_name, func in [
        ("Signals", lambda sigs: str(len(sigs))),
        ("Win Rate", lambda sigs: f"{sum(1 for s in sigs if s['outcome'] == 'WIN') / len(sigs) * 100:.1f}%" if sigs else "n/a"),
        ("PnL", lambda sigs: f"{sum(s['pnl'] for s in sigs):+.1f}" if sigs else "n/a"),
        ("MaxDD", lambda sigs: f"{_maxdd(sigs):.1f}" if sigs else "n/a"),
        ("Avg/day", lambda sigs: f"{len(sigs) / len(set(s['date'] for s in sigs)):.1f}" if sigs else "n/a"),
        ("Longs", lambda sigs: str(sum(1 for s in sigs if s["direction"] == "long"))),
        ("Shorts", lambda sigs: str(sum(1 for s in sigs if s["direction"] == "short"))),
        ("Long WR", lambda sigs: f"{sum(1 for s in sigs if s['direction'] == 'long' and s['outcome'] == 'WIN') / max(1, sum(1 for s in sigs if s['direction'] == 'long')) * 100:.1f}%"),
        ("Short WR", lambda sigs: f"{sum(1 for s in sigs if s['direction'] == 'short' and s['outcome'] == 'WIN') / max(1, sum(1 for s in sigs if s['direction'] == 'short')) * 100:.1f}%"),
    ]:
        print(f"  {metric_name:<20} {func(a_sigs):<15} {func(b_sigs):<15} {func(c_sigs):<15}")

    print(f"\n  Date range: {min(trading_dates.keys())} to {max(trading_dates.keys())}")
    print(f"  Trading days: {len(trading_dates)}")
    print(f"  Volland confluence: ZEROED (conservative)")
    print(f"  Confidence: {'directional signal only' if len(a_sigs) < 50 else 'moderate' if len(a_sigs) < 100 else 'high'}")
    print(f"  Note: SL=8, Target=10, Timeout=60 bars, Cooldown=10 bars")


def _maxdd(sigs):
    """Compute max drawdown from a signal list."""
    running = 0
    peak = 0
    max_dd = 0
    for s in sorted(sigs, key=lambda x: (x["date"], x["time_et"])):
        running += s["pnl"]
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
    return max_dd


if __name__ == "__main__":
    main()
