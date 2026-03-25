"""
Backtest: ES Absorption signals during PRE-MARKET hours (before 10:00 ET).
Uses the same detection logic from setup_detector.py but WITHOUT Volland confluence.
Simulates SL=8 / T=10 outcomes using subsequent bar data.
"""
import os, json
from datetime import datetime, time as dtime, timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

ET = ZoneInfo("US/Eastern")

# ── Detection parameters (same as setup_detector.py) ──
LOOKBACK = 8
VOL_WINDOW = 20
MIN_VOL_RATIO = 1.5
MIN_BARS = VOL_WINDOW + LOOKBACK

# ── Risk management ──
SL_PTS = 8
TARGET_PTS = 10
COOLDOWN_BARS = 10

# ── Pre-market window ──
# ES futures open Sunday 6 PM ET. Pre-market = before 09:30 ET on weekdays
# We'll test: 00:00 ET (midnight) through 09:29 ET
PRE_MARKET_END = dtime(9, 30)
# Also test extended: 04:00 - 09:30 (most volume is here)


def detect_absorption(bars_window, trigger_bar):
    """
    Run absorption detection logic on a window of bars.
    Returns (direction, div_raw, vol_ratio) or None.
    Same logic as evaluate_absorption() but stripped of Volland.
    """
    # Volume gate
    recent_vols = [b["bar_volume"] for b in bars_window[-(VOL_WINDOW + 1):-1]]
    if not recent_vols:
        return None
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0:
        return None
    vol_ratio = trigger_bar["bar_volume"] / vol_avg
    if vol_ratio < MIN_VOL_RATIO:
        return None

    # Divergence over lookback window
    window = bars_window[-(LOOKBACK + 1):]
    lows = [b["bar_low"] for b in window]
    highs = [b["bar_high"] for b in window]
    cvds = [b["cvd_close"] for b in window]

    cvd_start, cvd_end = cvds[0], cvds[-1]
    cvd_slope = cvd_end - cvd_start
    cvd_range = max(cvds) - min(cvds)
    if cvd_range == 0:
        return None

    price_low_start, price_low_end = lows[0], lows[-1]
    price_high_start, price_high_end = highs[0], highs[-1]
    price_range = max(highs) - min(lows)
    if price_range == 0:
        return None

    cvd_norm = cvd_slope / cvd_range
    price_low_norm = (price_low_end - price_low_start) / price_range
    price_high_norm = (price_high_end - price_high_start) / price_range

    direction = None
    div_raw = 0

    # Bullish: CVD falling while price holds/rises
    if cvd_norm < -0.15:
        gap = price_low_norm - cvd_norm
        if gap > 0.2:
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
    if cvd_norm > 0.15 and direction is None:
        gap = cvd_norm - price_high_norm
        if gap > 0.2:
            direction = "bearish"
            if gap > 1.2:
                div_raw = 4
            elif gap > 0.8:
                div_raw = 3
            elif gap > 0.4:
                div_raw = 2
            else:
                div_raw = 1

    if direction is None:
        return None

    return {
        "direction": direction,
        "div_raw": div_raw,
        "vol_ratio": round(vol_ratio, 2),
        "entry_price": trigger_bar["bar_close"],
        "bar_idx": trigger_bar["bar_idx"],
    }


def simulate_outcome(signal, future_bars):
    """
    Simulate SL/T outcome using subsequent bars after signal.
    Returns (result, exit_price, bars_to_exit, max_fav, max_adv).
    """
    entry = signal["entry_price"]
    is_long = signal["direction"] == "bullish"

    if is_long:
        stop = entry - SL_PTS
        target = entry + TARGET_PTS
    else:
        stop = entry + SL_PTS
        target = entry - TARGET_PTS

    max_fav = 0.0
    max_adv = 0.0

    for i, bar in enumerate(future_bars):
        if is_long:
            fav = bar["bar_high"] - entry
            adv = entry - bar["bar_low"]
            if bar["bar_low"] <= stop:
                return "LOSS", stop, i + 1, max_fav, max_adv
            if bar["bar_high"] >= target:
                return "WIN", target, i + 1, max_fav, max_adv
        else:
            fav = entry - bar["bar_low"]
            adv = bar["bar_high"] - entry
            if bar["bar_high"] >= stop:
                return "LOSS", stop, i + 1, max_fav, max_adv
            if bar["bar_low"] <= target:
                return "WIN", target, i + 1, max_fav, max_adv

        max_fav = max(max_fav, fav)
        max_adv = max(max_adv, adv)

    return "EXPIRED", entry, len(future_bars), max_fav, max_adv


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        return

    engine = create_engine(db_url)

    # Query all 5-pt range bars from rithmic source
    print("Querying all 5-pt rithmic range bars...")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume, bar_delta, cvd_close, ts_start, ts_end, range_pts
            FROM es_range_bars
            WHERE source = 'rithmic'
              AND range_pts = 5
            ORDER BY trade_date, bar_idx
        """)).fetchall()

    print(f"Total bars: {len(rows)}")

    # Group bars by trade_date
    bars_by_date = defaultdict(list)
    for r in rows:
        bar = {
            "trade_date": str(r[0]),
            "bar_idx": r[1],
            "bar_open": float(r[2]),
            "bar_high": float(r[3]),
            "bar_low": float(r[4]),
            "bar_close": float(r[5]),
            "bar_volume": int(r[6]),
            "bar_delta": int(r[7]),
            "cvd_close": float(r[8]),
            "ts_start": r[9],
            "ts_end": r[10],
            "range_pts": float(r[11]) if r[11] else 5.0,
        }
        bars_by_date[bar["trade_date"]].append(bar)

    print(f"Trading dates: {len(bars_by_date)}")

    # Run detection on pre-market bars
    signals = []
    for trade_date in sorted(bars_by_date.keys()):
        day_bars = bars_by_date[trade_date]

        last_bullish_bar = -100
        last_bearish_bar = -100
        last_checked_idx = -1

        for i, bar in enumerate(day_bars):
            ts = bar["ts_start"]
            if ts is None:
                continue

            # Convert to ET
            if hasattr(ts, 'astimezone'):
                ts_et = ts.astimezone(ET)
            else:
                continue

            bar_time_et = ts_et.time()

            # Only pre-market (before 09:30 ET)
            if bar_time_et >= PRE_MARKET_END:
                continue

            # Skip bars before we have enough history
            if i < MIN_BARS:
                continue

            # Skip already checked
            if bar["bar_idx"] <= last_checked_idx:
                continue
            last_checked_idx = bar["bar_idx"]

            # Run detection
            window = day_bars[max(0, i - MIN_BARS):i + 1]
            if len(window) < MIN_BARS:
                continue

            result = detect_absorption(window, bar)
            if result is None:
                continue

            # Cooldown
            if result["direction"] == "bullish":
                if bar["bar_idx"] - last_bullish_bar < COOLDOWN_BARS:
                    continue
                last_bullish_bar = bar["bar_idx"]
            else:
                if bar["bar_idx"] - last_bearish_bar < COOLDOWN_BARS:
                    continue
                last_bearish_bar = bar["bar_idx"]

            # Simulate outcome using ALL remaining bars for the day
            future_bars = day_bars[i + 1:]
            if not future_bars:
                continue

            outcome, exit_price, bars_to_exit, max_fav, max_adv = simulate_outcome(
                result, future_bars
            )

            pnl_pts = TARGET_PTS if outcome == "WIN" else (-SL_PTS if outcome == "LOSS" else 0)

            sig = {
                "date": trade_date,
                "time_et": bar_time_et.strftime("%H:%M"),
                "direction": result["direction"],
                "entry": result["entry_price"],
                "div_raw": result["div_raw"],
                "vol_ratio": result["vol_ratio"],
                "outcome": outcome,
                "exit_price": exit_price,
                "pnl_pts": pnl_pts,
                "bars_to_exit": bars_to_exit,
                "max_fav": round(max_fav, 1),
                "max_adv": round(max_adv, 1),
            }
            signals.append(sig)

    # ── Report ──
    print(f"\n{'='*80}")
    print(f"  PRE-MARKET ES ABSORPTION BACKTEST")
    print(f"  Window: before 09:30 ET | SL={SL_PTS} | T={TARGET_PTS}")
    print(f"  Detection: lookback={LOOKBACK}, vol_ratio>={MIN_VOL_RATIO}, cooldown={COOLDOWN_BARS} bars")
    print(f"  No Volland confluence (pre-market)")
    print(f"{'='*80}\n")

    if not signals:
        print("NO SIGNALS FOUND")
        return

    wins = [s for s in signals if s["outcome"] == "WIN"]
    losses = [s for s in signals if s["outcome"] == "LOSS"]
    expired = [s for s in signals if s["outcome"] == "EXPIRED"]
    total = len(signals)
    wr = len(wins) / total * 100 if total else 0
    total_pnl = sum(s["pnl_pts"] for s in signals)
    longs = [s for s in signals if s["direction"] == "bullish"]
    shorts = [s for s in signals if s["direction"] == "bearish"]

    print(f"Total signals: {total}")
    print(f"  Wins: {len(wins)}  Losses: {len(losses)}  Expired: {len(expired)}")
    print(f"  Win Rate: {wr:.1f}%")
    print(f"  Total PnL: {total_pnl:+.0f} pts")
    print(f"  Avg PnL/trade: {total_pnl/total:+.1f} pts")
    print(f"  Longs: {len(longs)} ({sum(1 for s in longs if s['outcome']=='WIN')}/{len(longs)} "
          f"= {sum(1 for s in longs if s['outcome']=='WIN')/len(longs)*100:.0f}% WR)" if longs else "")
    print(f"  Shorts: {len(shorts)} ({sum(1 for s in shorts if s['outcome']=='WIN')}/{len(shorts)} "
          f"= {sum(1 for s in shorts if s['outcome']=='WIN')/len(shorts)*100:.0f}% WR)" if shorts else "")

    # By time bucket
    print(f"\n--- By Time Bucket (ET) ---")
    buckets = defaultdict(lambda: {"wins": 0, "losses": 0, "expired": 0, "pnl": 0})
    for s in signals:
        h = int(s["time_et"].split(":")[0])
        bucket = f"{h:02d}:00-{h:02d}:59"
        if s["outcome"] == "WIN":
            buckets[bucket]["wins"] += 1
        elif s["outcome"] == "LOSS":
            buckets[bucket]["losses"] += 1
        else:
            buckets[bucket]["expired"] += 1
        buckets[bucket]["pnl"] += s["pnl_pts"]

    for bucket in sorted(buckets.keys()):
        b = buckets[bucket]
        t = b["wins"] + b["losses"] + b["expired"]
        wr_b = b["wins"] / t * 100 if t else 0
        print(f"  {bucket}: {t:>3} trades | {b['wins']:>2}W {b['losses']:>2}L {b['expired']:>2}E | "
              f"WR={wr_b:5.1f}% | PnL={b['pnl']:+5.0f} pts")

    # By divergence strength
    print(f"\n--- By Divergence Strength ---")
    for div in sorted(set(s["div_raw"] for s in signals)):
        subset = [s for s in signals if s["div_raw"] == div]
        w = sum(1 for s in subset if s["outcome"] == "WIN")
        t = len(subset)
        pnl = sum(s["pnl_pts"] for s in subset)
        print(f"  div_raw={div}: {t:>3} trades | {w}W | WR={w/t*100:.0f}% | PnL={pnl:+.0f} pts")

    # Individual trades
    print(f"\n--- All Trades ---")
    print(f"{'Date':<12} {'Time':>5} {'Dir':>7} {'Entry':>8} {'Div':>3} {'VolR':>5} "
          f"{'Result':>7} {'PnL':>5} {'Bars':>4} {'MaxFav':>6} {'MaxAdv':>6}")
    print("-" * 80)
    for s in signals:
        print(f"{s['date']:<12} {s['time_et']:>5} {s['direction']:>7} {s['entry']:>8.2f} "
              f"{s['div_raw']:>3} {s['vol_ratio']:>5.1f} "
              f"{s['outcome']:>7} {s['pnl_pts']:>+5.0f} {s['bars_to_exit']:>4} "
              f"{s['max_fav']:>6.1f} {s['max_adv']:>6.1f}")

    # Running PnL
    print(f"\n--- Running PnL ---")
    running = 0
    max_dd = 0
    peak = 0
    for s in signals:
        running += s["pnl_pts"]
        peak = max(peak, running)
        dd = peak - running
        max_dd = max(max_dd, dd)
    print(f"  Final PnL: {running:+.0f} pts")
    print(f"  Peak PnL:  {peak:+.0f} pts")
    print(f"  Max DD:    {max_dd:.0f} pts")
    if total > 0:
        print(f"  PF: {abs(sum(s['pnl_pts'] for s in wins)) / abs(sum(s['pnl_pts'] for s in losses)):.2f}x" if losses else "  PF: inf")


if __name__ == "__main__":
    main()
