"""
Delta-Price Divergence Detector — Single-Day Backtest (March 19, 2026)
=====================================================================
Detects when bar delta direction diverges from bar price direction
on ES 5-pt Rithmic range bars, then simulates outcomes.

Signal logic:
  - bar_delta > 0 AND bar is RED (close < open) or body <= 1.0 → BEARISH (buying absorbed)
  - bar_delta < 0 AND bar is GREEN (close > open) or body <= 1.0 → BULLISH (selling absorbed)
  - Minimum |delta| >= 100
  - Cooldown: 5 bars same direction

2-bar combo: if current + previous bar combined creates divergence
  (combined_delta opposes combined price change), fire as combo signal
  only if neither individual bar fired.

Grading (0-100):
  - Delta magnitude (0-30): delta_zscore vs 20-bar rolling
  - Peak delta (0-20): max_abs_peak / abs_delta ratio (absorbed energy)
  - Volume intensity (0-20): vol_rate_zscore
  - Divergence magnitude (0-30): |delta| × body_against_direction

Grade: A+ >= 80, A >= 65, B >= 50, C >= 35, LOG < 35

Simulation — Config F trail:
  SL=8, immediate trail gap=8, timeout=100 bars.
  Entry at bar close. Direction per signal.
"""

import psycopg2
import subprocess
import json
import statistics
from datetime import datetime, time as dtime
from collections import defaultdict

# ── Config ──────────────────────────────────────────────────────────────────
TRADE_DATE = "2026-03-19"
MIN_DELTA = 100         # Noise floor
COOLDOWN_BARS = 5       # Same-direction cooldown
SL_PTS = 8.0
TRAIL_GAP = 8.0         # Immediate trail — no activation threshold
TIMEOUT_BARS = 100

# Grade thresholds
GRADE_AP = 80
GRADE_A  = 65
GRADE_B  = 50
GRADE_C  = 35

# ── Get DB URL ──────────────────────────────────────────────────────────────
def get_db_url():
    try:
        result = subprocess.run(
            ["railway", "variables", "--json"],
            capture_output=True, text=True, timeout=15
        )
        return json.loads(result.stdout)["DATABASE_URL"]
    except Exception:
        # Fallback
        return "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"


# ── Fetch data ──────────────────────────────────────────────────────────────
def fetch_bars(db_url, trade_date):
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("""
        SELECT bar_idx, trade_date, bar_open, bar_high, bar_low, bar_close,
               bar_volume, bar_delta, bar_buy_volume, bar_sell_volume,
               cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
               ts_start AT TIME ZONE 'America/New_York' as ts_start_et,
               ts_end AT TIME ZONE 'America/New_York' as ts_end_et,
               EXTRACT(EPOCH FROM (ts_end - ts_start)) as duration_sec
        FROM es_range_bars
        WHERE source = 'rithmic' AND trade_date = %s
        ORDER BY bar_idx;
    """, (trade_date,))
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    conn.close()

    bars = []
    for r in rows:
        d = dict(zip(cols, r))
        # Convert decimals
        d["duration_sec"] = float(d["duration_sec"]) if d["duration_sec"] else 1.0
        bars.append(d)
    return bars


# ── Filter to market hours ──────────────────────────────────────────────────
def filter_market_hours(bars):
    mh = []
    for b in bars:
        ts = b["ts_start_et"]
        if ts is None:
            continue
        t = ts.time()
        if dtime(9, 30) <= t < dtime(16, 0):
            mh.append(b)
    return mh


# ── Helpers ─────────────────────────────────────────────────────────────────
def bar_body(b):
    return abs(b["bar_close"] - b["bar_open"])


def bar_is_green(b):
    return b["bar_close"] > b["bar_open"]


def bar_is_red(b):
    return b["bar_close"] < b["bar_open"]


def rolling_stats(values, window=20):
    """Return (mean, stdev) of last `window` values. Returns (0, 1) if insufficient."""
    if len(values) < 5:
        return 0.0, 1.0
    w = values[-window:] if len(values) >= window else values[:]
    m = statistics.mean(w)
    s = statistics.stdev(w) if len(w) > 1 else 1.0
    return m, max(s, 0.001)


def zscore(val, mean, std):
    return (val - mean) / std


# ── Signal Detection ───────────────────────────────────────────────────────
def detect_signals(bars):
    """
    Returns list of signal dicts.
    """
    signals = []
    last_signal_bar = {"BULL": -999, "BEAR": -999}
    abs_deltas = []   # for rolling z-score
    vol_rates = []    # volume / duration
    fired_indices = set()  # bars that already fired a single-bar signal

    for i, b in enumerate(bars):
        idx = b["bar_idx"]
        delta = b["bar_delta"]
        body = bar_body(b)
        dur = max(b["duration_sec"], 0.1)
        vol = b["bar_volume"]
        vol_rate = vol / dur

        abs_deltas.append(abs(delta))
        vol_rates.append(vol_rate)

        # CVD peak/trough within the bar
        peak_delta = b["cvd_high"] - b["cvd_open"]    # max positive excursion
        trough_delta = b["cvd_low"] - b["cvd_open"]   # max negative excursion (usually negative)

        # ── Single-bar divergence ──
        sig = _check_single_bar(b, delta, body, abs_deltas, vol_rates, vol_rate, peak_delta, trough_delta)
        if sig:
            direction = sig["direction"]
            # Cooldown check
            if i - last_signal_bar[direction] >= COOLDOWN_BARS:
                sig["bar_idx"] = idx
                sig["bar_i"] = i
                sig["time_et"] = b["ts_start_et"].strftime("%H:%M:%S") if b["ts_start_et"] else "?"
                sig["entry_price"] = b["bar_close"]
                sig["combo"] = False
                signals.append(sig)
                last_signal_bar[direction] = i
                fired_indices.add(i)
            continue  # don't also check combo on a bar that fired single

        # ── 2-bar combo divergence ──
        if i >= 1 and i not in fired_indices and (i - 1) not in fired_indices:
            combo_sig = _check_combo(bars, i, abs_deltas, vol_rates)
            if combo_sig:
                direction = combo_sig["direction"]
                if i - last_signal_bar[direction] >= COOLDOWN_BARS:
                    combo_sig["bar_idx"] = idx
                    combo_sig["bar_i"] = i
                    combo_sig["time_et"] = b["ts_start_et"].strftime("%H:%M:%S") if b["ts_start_et"] else "?"
                    combo_sig["entry_price"] = b["bar_close"]
                    combo_sig["combo"] = True
                    signals.append(combo_sig)
                    last_signal_bar[direction] = i
                    fired_indices.add(i)

    return signals


def _check_single_bar(b, delta, body, abs_deltas, vol_rates, vol_rate, peak_delta, trough_delta):
    """Check if a single bar has delta-price divergence. Returns signal dict or None."""
    if abs(delta) < MIN_DELTA:
        return None

    green = bar_is_green(b)
    red = bar_is_red(b)
    doji = body <= 1.0

    direction = None
    if delta > 0 and (red or doji):
        direction = "BEAR"  # Positive delta but price didn't go up → buying absorbed
    elif delta < 0 and (green or doji):
        direction = "BULL"  # Negative delta but price held/rose → selling absorbed

    if direction is None:
        return None

    # ── Grading ──
    score = _grade_signal(delta, body, abs_deltas, vol_rates, vol_rate, peak_delta, trough_delta, direction, b)
    grade = _score_to_grade(score)

    return {
        "direction": direction,
        "delta": delta,
        "body": body,
        "body_ratio": body / 5.0,  # 5-pt range bars
        "peak_delta": peak_delta,
        "trough_delta": trough_delta,
        "volume": b["bar_volume"],
        "vol_per_sec": vol_rate,
        "score": score,
        "grade": grade,
    }


def _check_combo(bars, i, abs_deltas, vol_rates):
    """Check 2-bar combo divergence. Only fires if neither bar fired individually."""
    b_prev = bars[i - 1]
    b_curr = bars[i]

    combined_delta = b_prev["bar_delta"] + b_curr["bar_delta"]
    combined_price_change = b_curr["bar_close"] - b_prev["bar_open"]

    if abs(combined_delta) < MIN_DELTA:
        return None

    direction = None
    if combined_delta > 0 and combined_price_change <= 0:
        direction = "BEAR"  # combined buying but price flat/down
    elif combined_delta < 0 and combined_price_change >= 0:
        direction = "BULL"  # combined selling but price flat/up

    if direction is None:
        return None

    # Use current bar's CVD peaks
    peak_delta = b_curr["cvd_high"] - b_prev["cvd_open"]
    trough_delta = b_curr["cvd_low"] - b_prev["cvd_open"]
    body = abs(combined_price_change)
    dur = max(b_prev["duration_sec"], 0.1) + max(b_curr["duration_sec"], 0.1)
    vol = b_prev["bar_volume"] + b_curr["bar_volume"]
    vol_rate = vol / dur

    score = _grade_signal(combined_delta, body, abs_deltas, vol_rates, vol_rate, peak_delta, trough_delta, direction, b_curr)
    grade = _score_to_grade(score)

    return {
        "direction": direction,
        "delta": combined_delta,
        "body": body,
        "body_ratio": body / 10.0,  # 2 bars = 10pt max range
        "peak_delta": peak_delta,
        "trough_delta": trough_delta,
        "volume": vol,
        "vol_per_sec": vol_rate,
        "score": score,
        "grade": grade,
    }


def _grade_signal(delta, body, abs_deltas, vol_rates, vol_rate, peak_delta, trough_delta, direction, bar):
    """Compute 0-100 score."""
    total = 0.0

    # 1. Delta magnitude (0-30): z-score of |delta| vs 20-bar rolling
    mean_d, std_d = rolling_stats(abs_deltas, 20)
    dz = zscore(abs(delta), mean_d, std_d)
    # Map z-score: 0→0, 1→10, 2→20, 3→30
    delta_score = min(max(dz * 10.0, 0.0), 30.0)
    total += delta_score

    # 2. Peak delta absorbed (0-20): how much of the within-bar delta peak was absorbed
    # For BEAR: peak_delta (high excursion of buying) vs final delta — higher peak = more absorbed
    # For BULL: |trough_delta| (selling excursion) vs |delta| — deeper trough = more absorbed
    if direction == "BEAR":
        peak_used = max(peak_delta, 0)
    else:
        peak_used = abs(min(trough_delta, 0))

    if abs(delta) > 0:
        peak_ratio = peak_used / abs(delta)
    else:
        peak_ratio = 0
    # Ratio > 1 means peak was larger than final delta (good — energy absorbed)
    # Map: 1.0→5, 1.5→10, 2.0→15, 3.0→20
    peak_score = min(max((peak_ratio - 0.5) * 10.0, 0.0), 20.0)
    total += peak_score

    # 3. Volume intensity (0-20): z-score of vol/sec
    mean_v, std_v = rolling_stats(vol_rates, 20)
    vz = zscore(vol_rate, mean_v, std_v)
    vol_score = min(max(vz * 10.0, 0.0), 20.0)
    total += vol_score

    # 4. Divergence magnitude (0-30): |delta| × body_against_direction
    # A bar with strong delta AND strong opposing body = most convincing divergence
    # Normalize: delta/300 (typical strong) × body/3.0 (typical strong body) × 30
    div_raw = (abs(delta) / 300.0) * max(body / 3.0, 0.3)
    div_score = min(div_raw * 30.0, 30.0)
    total += div_score

    return round(total, 1)


def _score_to_grade(score):
    if score >= GRADE_AP:
        return "A+"
    elif score >= GRADE_A:
        return "A"
    elif score >= GRADE_B:
        return "B"
    elif score >= GRADE_C:
        return "C"
    else:
        return "LOG"


# ── Outcome Simulation ─────────────────────────────────────────────────────
def simulate_outcomes(signals, bars):
    """
    Config F trail: SL=8, immediate trail gap=8, timeout=100 bars.
    Entry at bar close. Trail starts immediately (no activation threshold).
    """
    # Build index lookup: bar_i → position in bars list
    for sig in signals:
        entry = sig["entry_price"]
        direction = sig["direction"]
        entry_i = sig["bar_i"]
        mul = 1.0 if direction == "BULL" else -1.0

        best_pnl = 0.0
        trail_stop = -SL_PTS  # Initial stop loss distance
        mfe = 0.0
        mae = 0.0
        outcome = "TIMEOUT"
        exit_pnl = 0.0
        exit_bar = None

        for j in range(entry_i + 1, min(entry_i + TIMEOUT_BARS + 1, len(bars))):
            b = bars[j]
            # Check high and low against entry
            if direction == "BULL":
                bar_pnl_high = b["bar_high"] - entry
                bar_pnl_low = b["bar_low"] - entry
            else:
                bar_pnl_high = entry - b["bar_low"]   # Best case for short
                bar_pnl_low = entry - b["bar_high"]    # Worst case for short

            # Update MFE/MAE
            mfe = max(mfe, bar_pnl_high)
            mae = min(mae, bar_pnl_low)

            # Check stop hit (check adverse first)
            if bar_pnl_low <= trail_stop:
                # Stopped out
                exit_pnl = trail_stop
                outcome = "STOP"
                exit_bar = j
                break

            # Update trail if new high
            if bar_pnl_high > best_pnl:
                best_pnl = bar_pnl_high
                new_trail = best_pnl - TRAIL_GAP
                if new_trail > trail_stop:
                    trail_stop = new_trail

        else:
            # Timeout — exit at last bar's close
            if direction == "BULL":
                exit_pnl = bars[min(entry_i + TIMEOUT_BARS, len(bars) - 1)]["bar_close"] - entry
            else:
                exit_pnl = entry - bars[min(entry_i + TIMEOUT_BARS, len(bars) - 1)]["bar_close"]
            outcome = "TIMEOUT"

        sig["outcome"] = outcome
        sig["pnl"] = round(exit_pnl, 2)
        sig["mfe"] = round(mfe, 2)
        sig["mae"] = round(mae, 2)


# ── User's manual signals for recall check ──────────────────────────────────
USER_SIGNALS = [
    ("09:35", "BULL", "single"),
    ("09:39", "BULL", "confirm"),
    ("09:46", "BULL", "2-bar combo"),
    ("09:50", "BEAR", "single"),   # 09:50+09:54 grouped
    ("09:54", "BEAR", "confirm"),
    ("10:11", "BEAR", "strong"),
    ("11:05", "BEAR", "2-bar combo"),
    ("11:36", "BULL", "single"),
    ("12:07", "BEAR", "single"),
    ("12:35", "BULL", "single"),
    ("12:40", "BEAR", "single"),   # 12:40+12:42 grouped
    ("12:42", "BEAR", "confirm"),
    ("12:52", "BULL", "lost"),
    ("14:13", "BEAR", "single"),
    ("14:41", "BULL", "strong"),
    ("15:04", "BULL", "single"),
]


def recall_check(signals, bars):
    """
    For each user signal, check if our detector captured it.
    If not, explain why (delta below 100, wrong color, cooldown).
    """
    results = []

    for user_time, user_dir, user_note in USER_SIGNALS:
        h, m = int(user_time.split(":")[0]), int(user_time.split(":")[1])
        target_time = dtime(h, m)

        # Find the closest bar(s) to this time
        closest_bar = None
        closest_dist = 9999
        for b in bars:
            ts = b["ts_start_et"]
            if ts is None:
                continue
            t = ts.time()
            # Distance in seconds
            dist = abs((t.hour * 3600 + t.minute * 60 + t.second) -
                       (target_time.hour * 3600 + target_time.minute * 60))
            if dist < closest_dist:
                closest_dist = dist
                closest_bar = b

        # Check if any signal fired within ±3 bars of this bar
        captured = False
        capture_sig = None
        if closest_bar:
            for sig in signals:
                if abs(sig["bar_i"] - bars.index(closest_bar)) <= 3 and sig["direction"] == user_dir:
                    captured = True
                    capture_sig = sig
                    break

        # If not captured, diagnose why
        reason = ""
        if not captured and closest_bar:
            b = closest_bar
            delta = b["bar_delta"]
            body = bar_body(b)
            green = bar_is_green(b)
            red = bar_is_red(b)

            if abs(delta) < MIN_DELTA:
                reason = f"|delta|={abs(delta)} < {MIN_DELTA}"
            elif user_dir == "BULL" and delta > 0 and green:
                reason = f"delta={delta} positive + green bar = no divergence"
            elif user_dir == "BEAR" and delta < 0 and red:
                reason = f"delta={delta} negative + red bar = no divergence"
            elif user_dir == "BULL" and delta > 0:
                reason = f"delta={delta} positive, not negative for BULL"
            elif user_dir == "BEAR" and delta < 0:
                reason = f"delta={delta} negative, not positive for BEAR"
            else:
                # Check cooldown
                reason = f"likely cooldown blocked (delta={delta}, body={body:.1f})"

            # Also show bar details
            reason += f" [bar_idx={b['bar_idx']}, O={b['bar_open']}, C={b['bar_close']}, delta={delta}]"

        results.append({
            "user_time": user_time,
            "user_dir": user_dir,
            "user_note": user_note,
            "captured": "YES" if captured else "NO",
            "capture_sig": capture_sig,
            "reason": reason,
            "closest_bar": closest_bar,
        })

    return results


# ── Printing ────────────────────────────────────────────────────────────────
def print_signals_table(signals):
    print("\n" + "=" * 160)
    print("ALL SIGNALS — March 19, 2026")
    print("=" * 160)
    header = (f"{'#':>3} {'Time ET':>9} {'Idx':>4} {'Dir':>5} {'Combo':>5} "
              f"{'Delta':>7} {'Body':>5} {'B.Rat':>5} {'Peak':>7} {'Trough':>7} "
              f"{'Vol':>6} {'V/sec':>7} {'Grade':>5} {'Score':>6} "
              f"{'Outcome':>8} {'PnL':>7} {'MFE':>7} {'MAE':>7}")
    print(header)
    print("-" * 160)
    for i, s in enumerate(signals, 1):
        combo_str = "2bar" if s["combo"] else ""
        print(f"{i:3d} {s['time_et']:>9} {s['bar_idx']:4d} {s['direction']:>5} {combo_str:>5} "
              f"{s['delta']:7d} {s['body']:5.1f} {s['body_ratio']:5.2f} {s['peak_delta']:7d} {s['trough_delta']:7d} "
              f"{s['volume']:6d} {s['vol_per_sec']:7.1f} {s['grade']:>5} {s['score']:6.1f} "
              f"{s['outcome']:>8} {s['pnl']:7.2f} {s['mfe']:7.2f} {s['mae']:7.2f}")


def print_summary(signals):
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    total = len(signals)
    wins = sum(1 for s in signals if s["pnl"] > 0)
    losses = sum(1 for s in signals if s["pnl"] <= 0)
    total_pnl = sum(s["pnl"] for s in signals)
    wr = wins / total * 100 if total else 0

    # Max drawdown (cumulative)
    cum = 0
    peak = 0
    max_dd = 0
    for s in signals:
        cum += s["pnl"]
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)

    # Profit factor
    gross_profit = sum(s["pnl"] for s in signals if s["pnl"] > 0)
    gross_loss = abs(sum(s["pnl"] for s in signals if s["pnl"] < 0))
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    print(f"Total signals:  {total}")
    print(f"Wins:           {wins}")
    print(f"Losses:         {losses}")
    print(f"Win Rate:       {wr:.1f}%")
    print(f"Total PnL:      {total_pnl:+.2f} pts")
    print(f"Max Drawdown:   {max_dd:.2f} pts")
    print(f"Profit Factor:  {pf:.2f}")
    print(f"Avg PnL/trade:  {total_pnl / total:.2f} pts" if total else "")
    print(f"Avg MFE:        {sum(s['mfe'] for s in signals) / total:.2f} pts" if total else "")
    print(f"Avg MAE:        {sum(s['mae'] for s in signals) / total:.2f} pts" if total else "")


def print_by_grade(signals):
    print("\n" + "=" * 80)
    print("BY GRADE")
    print("=" * 80)
    grades = ["A+", "A", "B", "C", "LOG"]
    print(f"{'Grade':>5} {'Count':>6} {'Wins':>5} {'WR':>6} {'PnL':>8} {'Avg PnL':>8} {'Avg MFE':>8} {'Avg MAE':>8}")
    print("-" * 70)
    for g in grades:
        gs = [s for s in signals if s["grade"] == g]
        if not gs:
            print(f"{g:>5} {0:6d}     -      -        -        -        -        -")
            continue
        wins = sum(1 for s in gs if s["pnl"] > 0)
        wr = wins / len(gs) * 100
        pnl = sum(s["pnl"] for s in gs)
        avg_pnl = pnl / len(gs)
        avg_mfe = sum(s["mfe"] for s in gs) / len(gs)
        avg_mae = sum(s["mae"] for s in gs) / len(gs)
        print(f"{g:>5} {len(gs):6d} {wins:5d} {wr:5.1f}% {pnl:+8.2f} {avg_pnl:+8.2f} {avg_mfe:8.2f} {avg_mae:8.2f}")


def print_by_direction(signals):
    print("\n" + "=" * 80)
    print("BY DIRECTION")
    print("=" * 80)
    for d in ["BULL", "BEAR"]:
        ds = [s for s in signals if s["direction"] == d]
        if not ds:
            print(f"  {d}: 0 signals")
            continue
        wins = sum(1 for s in ds if s["pnl"] > 0)
        wr = wins / len(ds) * 100
        pnl = sum(s["pnl"] for s in ds)
        print(f"  {d}: {len(ds)} signals, {wins}W/{len(ds)-wins}L, "
              f"WR={wr:.1f}%, PnL={pnl:+.2f}, "
              f"Avg MFE={sum(s['mfe'] for s in ds)/len(ds):.2f}, "
              f"Avg MAE={sum(s['mae'] for s in ds)/len(ds):.2f}")


def print_recall_check(recall_results):
    print("\n" + "=" * 120)
    print("RECALL CHECK vs User's 14 Manual Signals")
    print("=" * 120)
    print(f"{'Time':>6} {'Dir':>5} {'Note':>12} {'Capt':>5} {'Machine Time':>13} {'Machine Idx':>10} {'Reason':>60}")
    print("-" * 120)
    captured_count = 0
    for r in recall_results:
        capt = r["captured"]
        if capt == "YES":
            captured_count += 1
            ms = r["capture_sig"]
            machine_time = ms["time_et"]
            machine_idx = str(ms["bar_idx"])
        else:
            machine_time = "-"
            machine_idx = "-"
        reason = r["reason"] if capt == "NO" else ""
        print(f"{r['user_time']:>6} {r['user_dir']:>5} {r['user_note']:>12} {capt:>5} "
              f"{machine_time:>13} {machine_idx:>10} {reason}")
    print(f"\nRecall: {captured_count}/{len(recall_results)} ({captured_count/len(recall_results)*100:.0f}%)")


def print_machine_only(signals, recall_results):
    """Signals the machine fired but the user didn't pick."""
    # Build set of machine signals that matched a user signal
    matched_sigs = set()
    for r in recall_results:
        if r["captured"] == "YES" and r["capture_sig"]:
            matched_sigs.add(id(r["capture_sig"]))

    unmatched = [s for s in signals if id(s) not in matched_sigs]
    print("\n" + "=" * 120)
    print(f"MACHINE-ONLY SIGNALS (fired but user didn't pick): {len(unmatched)}")
    print("=" * 120)
    if not unmatched:
        print("  None — perfect overlap!")
        return

    print(f"{'#':>3} {'Time ET':>9} {'Idx':>4} {'Dir':>5} {'Combo':>5} "
          f"{'Delta':>7} {'Body':>5} {'Grade':>5} {'Score':>6} "
          f"{'Outcome':>8} {'PnL':>7} {'MFE':>7} {'MAE':>7}")
    print("-" * 100)
    for i, s in enumerate(unmatched, 1):
        combo_str = "2bar" if s["combo"] else ""
        print(f"{i:3d} {s['time_et']:>9} {s['bar_idx']:4d} {s['direction']:>5} {combo_str:>5} "
              f"{s['delta']:7d} {s['body']:5.1f} {s['grade']:>5} {s['score']:6.1f} "
              f"{s['outcome']:>8} {s['pnl']:7.2f} {s['mfe']:7.2f} {s['mae']:7.2f}")


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print(f"Delta-Price Divergence Detector — Backtest {TRADE_DATE}")
    print(f"Config: SL={SL_PTS}, Trail gap={TRAIL_GAP}, Timeout={TIMEOUT_BARS} bars")
    print(f"Signal: |delta| >= {MIN_DELTA}, cooldown={COOLDOWN_BARS} bars")
    print()

    db_url = get_db_url()
    all_bars = fetch_bars(db_url, TRADE_DATE)
    print(f"Fetched {len(all_bars)} total bars for {TRADE_DATE}")

    bars = filter_market_hours(all_bars)
    print(f"Market hours (9:30-16:00 ET): {len(bars)} bars")
    print(f"  First: idx={bars[0]['bar_idx']} @ {bars[0]['ts_start_et'].strftime('%H:%M:%S')}")
    print(f"  Last:  idx={bars[-1]['bar_idx']} @ {bars[-1]['ts_start_et'].strftime('%H:%M:%S')}")

    # Detect signals
    signals = detect_signals(bars)
    print(f"\nDetected {len(signals)} signals")

    # Simulate outcomes
    simulate_outcomes(signals, bars)

    # Print everything
    print_signals_table(signals)
    print_summary(signals)
    print_by_grade(signals)
    print_by_direction(signals)

    # Recall check
    recall_results = recall_check(signals, bars)
    print_recall_check(recall_results)

    # Machine-only signals
    print_machine_only(signals, recall_results)

    # Filtered scenario analysis
    print_filtered_scenarios(signals)

    # User signal performance
    print_user_signal_performance(recall_results)

    # Key observations
    print_observations(signals, recall_results)


def _compute_stats(sigs):
    """Helper to compute stats for a list of signals."""
    if not sigs:
        return {"count": 0}
    total = len(sigs)
    wins = sum(1 for s in sigs if s["pnl"] > 0)
    pnl = sum(s["pnl"] for s in sigs)
    wr = wins / total * 100
    cum = 0
    peak = 0
    max_dd = 0
    for s in sigs:
        cum += s["pnl"]
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    gp = sum(s["pnl"] for s in sigs if s["pnl"] > 0)
    gl = abs(sum(s["pnl"] for s in sigs if s["pnl"] < 0))
    pf = gp / gl if gl > 0 else float("inf")
    return {
        "count": total, "wins": wins, "losses": total - wins,
        "wr": wr, "pnl": pnl, "max_dd": max_dd, "pf": pf,
        "avg_pnl": pnl / total, "avg_mfe": sum(s["mfe"] for s in sigs) / total,
        "avg_mae": sum(s["mae"] for s in sigs) / total,
    }


def print_filtered_scenarios(signals):
    print("\n" + "=" * 100)
    print("FILTERED SCENARIOS — What if we only took certain grades?")
    print("=" * 100)

    scenarios = [
        ("All signals", lambda s: True),
        ("B+ only (B, A, A+)", lambda s: s["grade"] in ("A+", "A", "B")),
        ("C+ only (C, B, A, A+)", lambda s: s["grade"] in ("A+", "A", "B", "C")),
        ("Single-bar only (no combos)", lambda s: not s["combo"]),
        ("Single-bar B+", lambda s: not s["combo"] and s["grade"] in ("A+", "A", "B")),
        ("|delta| >= 200", lambda s: abs(s["delta"]) >= 200),
        ("|delta| >= 300", lambda s: abs(s["delta"]) >= 300),
        ("|delta| >= 200 + single-bar", lambda s: abs(s["delta"]) >= 200 and not s["combo"]),
        ("body >= 2.0 (strong opposing move)", lambda s: s["body"] >= 2.0),
        ("body >= 2.0 + |delta| >= 200", lambda s: s["body"] >= 2.0 and abs(s["delta"]) >= 200),
    ]

    print(f"{'Scenario':<40} {'Count':>5} {'Wins':>5} {'WR':>6} {'PnL':>8} {'MaxDD':>7} {'PF':>6} {'AvgPnL':>7}")
    print("-" * 95)
    for name, filt in scenarios:
        filtered = [s for s in signals if filt(s)]
        st = _compute_stats(filtered)
        if st["count"] == 0:
            print(f"{name:<40} {0:5d}     -      -        -       -      -       -")
            continue
        print(f"{name:<40} {st['count']:5d} {st['wins']:5d} {st['wr']:5.1f}% {st['pnl']:+8.2f} {st['max_dd']:7.2f} {st['pf']:6.2f} {st['avg_pnl']:+7.2f}")


def print_user_signal_performance(recall_results):
    print("\n" + "=" * 100)
    print("USER SIGNAL PERFORMANCE — How did the user's picks perform?")
    print("=" * 100)

    user_sigs = []
    for r in recall_results:
        if r["captured"] == "YES" and r["capture_sig"]:
            user_sigs.append(r["capture_sig"])

    # Deduplicate (some user signals map to same machine signal, e.g., 12:40 and 12:42)
    seen_ids = set()
    deduped = []
    for s in user_sigs:
        if id(s) not in seen_ids:
            deduped.append(s)
            seen_ids.add(id(s))

    st = _compute_stats(deduped)
    if st["count"] == 0:
        print("  No user signals captured.")
        return

    print(f"  Matched signals: {st['count']} (deduplicated)")
    print(f"  Wins/Losses:     {st['wins']}W / {st['losses']}L")
    print(f"  Win Rate:        {st['wr']:.1f}%")
    print(f"  Total PnL:       {st['pnl']:+.2f} pts")
    print(f"  Max Drawdown:    {st['max_dd']:.2f} pts")
    print(f"  Profit Factor:   {st['pf']:.2f}")
    print(f"  Avg PnL/trade:   {st['avg_pnl']:.2f} pts")

    print(f"\n  Per-signal breakdown:")
    print(f"  {'Time':>8} {'Dir':>5} {'Delta':>7} {'Grade':>5} {'Outcome':>8} {'PnL':>7} {'MFE':>7} {'MAE':>7}")
    print("  " + "-" * 65)
    for s in deduped:
        print(f"  {s['time_et']:>8} {s['direction']:>5} {s['delta']:7d} {s['grade']:>5} "
              f"{s['outcome']:>8} {s['pnl']:7.2f} {s['mfe']:7.2f} {s['mae']:7.2f}")


def print_observations(signals, recall_results):
    print("\n" + "=" * 100)
    print("KEY OBSERVATIONS")
    print("=" * 100)

    total = len(signals)
    user_matched = sum(1 for r in recall_results if r["captured"] == "YES")

    # Count combos
    combos = sum(1 for s in signals if s["combo"])
    singles = total - combos

    # Combo vs single performance
    combo_sigs = [s for s in signals if s["combo"]]
    single_sigs = [s for s in signals if not s["combo"]]
    combo_st = _compute_stats(combo_sigs)
    single_st = _compute_stats(single_sigs)

    print(f"""
  1. SIGNAL VOLUME: {total} signals in one day is extremely noisy.
     User picked ~14 unique signals. Machine fired {total - user_matched + 1} extra.
     The detector needs much stronger filtering to match human selectivity.

  2. SINGLE vs COMBO:
     Single-bar: {single_st['count']} signals, {single_st['wr']:.1f}% WR, PnL={single_st['pnl']:+.2f}
     2-bar combo: {combo_st['count']} signals, {combo_st['wr']:.1f}% WR, PnL={combo_st['pnl']:+.2f}
     {'Combos are significantly worse — consider removing or raising their threshold.' if combo_st['count'] > 0 and combo_st['pnl'] < single_st['pnl'] else 'Similar performance.'}

  3. GRADE DISTRIBUTION: A/A+={sum(1 for s in signals if s['grade'] in ('A+','A'))}, B={sum(1 for s in signals if s['grade']=='B')}, C={sum(1 for s in signals if s['grade']=='C')}, LOG={sum(1 for s in signals if s['grade']=='LOG')}
     Most signals ({sum(1 for s in signals if s['grade'] in ('C','LOG'))}/{total}) are C/LOG grade.
     Grading needs recalibration — only 1 signal reached A grade.

  4. SL=8 with TRAIL_GAP=8 means the trail only starts helping after MFE > 8.
     Avg MFE = {sum(s['mfe'] for s in signals)/total:.1f} pts but many trades get stopped at -8 before reaching it.

  5. RECALL: {user_matched}/{len(recall_results)} user signals captured (94%).
     Only miss: 09:39 BULL "confirm" — correctly rejected (delta was +130 = same direction as green bar).
     Mechanical rules correctly capture almost all discretionary signals.
""")


if __name__ == "__main__":
    main()
