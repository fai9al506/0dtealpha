"""
Backtest: ES Absorption d40 — Risk Management optimization.
Tests multiple SL/target combos to find optimal RM for swing-to-swing signals.
"""
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import time as dtime

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)

PIVOT_LEFT = 2
PIVOT_RIGHT = 2
VOL_WINDOW = 10
MIN_VOL_RATIO = 1.4
CVD_Z_MIN = 0.5
CVD_STD_WINDOW = 20
MAX_SWING_AGE = 50
MAX_TRIGGER_DIST = 40
SIGNAL_START = dtime(15, 0)  # 10:00 ET = 15:00 UTC
COOLDOWN = 10
OUTCOME_BARS = 30  # extended lookback for RM analysis


def score_divergence(cvd_z, price_atr):
    base = min(100, cvd_z / 3.0 * 100)
    mult = min(2.0, 0.5 + price_atr * 0.5)
    return min(100, base * mult)


def detect_swings(closed, pl, pr):
    swings = []
    last_type = None
    for pos in range(pl, len(closed) - pr):
        bar = closed[pos]
        is_low = True
        for j in range(1, pl + 1):
            if bar["low"] > closed[pos - j]["low"]: is_low = False; break
        if is_low:
            for j in range(1, pr + 1):
                if bar["low"] > closed[pos + j]["low"]: is_low = False; break
        is_high = True
        for j in range(1, pl + 1):
            if bar["high"] < closed[pos - j]["high"]: is_high = False; break
        if is_high:
            for j in range(1, pr + 1):
                if bar["high"] < closed[pos + j]["high"]: is_high = False; break
        if not is_low and not is_high: continue
        if is_low and is_high:
            if last_type == "L": is_low = False
            elif last_type == "H": is_high = False
            else: is_high = False
        if is_low:
            sw = {"type": "L", "price": bar["low"], "cvd": bar["cvd"],
                  "volume": bar["volume"], "bar_idx": bar["idx"]}
            if not swings or last_type != "L":
                swings.append(sw); last_type = "L"
            elif sw["price"] <= swings[-1]["price"]:
                swings[-1] = sw
        elif is_high:
            sw = {"type": "H", "price": bar["high"], "cvd": bar["cvd"],
                  "volume": bar["volume"], "bar_idx": bar["idx"]}
            if not swings or last_type != "H":
                swings.append(sw); last_type = "H"
            elif sw["price"] >= swings[-1]["price"]:
                swings[-1] = sw
    return swings


def scan_d40(closed, swings, trigger, cvd_std, atr):
    """Swing-to-swing scan with d=40."""
    trigger_idx = trigger["idx"]
    recent_vols = [b["volume"] for b in closed[-(VOL_WINDOW + 1):-1]]
    if not recent_vols: return []
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0: return []
    vol_ratio = trigger["volume"] / vol_avg
    if vol_ratio < MIN_VOL_RATIO: return []

    active = [s for s in swings if trigger_idx - s["bar_idx"] <= MAX_SWING_AGE]
    slows = [s for s in active if s["type"] == "L"]
    shighs = [s for s in active if s["type"] == "H"]
    bull, bear = [], []

    for i in range(1, len(slows)):
        L1, L2 = slows[i - 1], slows[i]
        if trigger_idx - L2["bar_idx"] > MAX_TRIGGER_DIST: continue
        cg = abs(L2["cvd"] - L1["cvd"]); cz = cg / cvd_std
        if cz < CVD_Z_MIN: continue
        is_abs = L2["price"] >= L1["price"] and L2["cvd"] < L1["cvd"]
        is_exh = L2["price"] < L1["price"] and L2["cvd"] > L1["cvd"]
        if is_abs or is_exh:
            pd_ = abs(L2["price"] - L1["price"]); pa = pd_ / atr
            pat = "sell_absorption" if is_abs else "sell_exhaustion"
            bull.append({"cvd_z": round(cz, 2), "score": round(score_divergence(cz, pa), 1), "pattern": pat})

    for i in range(1, len(shighs)):
        H1, H2 = shighs[i - 1], shighs[i]
        if trigger_idx - H2["bar_idx"] > MAX_TRIGGER_DIST: continue
        cg = abs(H2["cvd"] - H1["cvd"]); cz = cg / cvd_std
        if cz < CVD_Z_MIN: continue
        is_abs = H2["price"] <= H1["price"] and H2["cvd"] > H1["cvd"]
        is_exh = H2["price"] > H1["price"] and H2["cvd"] < H1["cvd"]
        if is_abs or is_exh:
            pd_ = abs(H2["price"] - H1["price"]); pa = pd_ / atr
            pat = "buy_absorption" if is_abs else "buy_exhaustion"
            bear.append({"cvd_z": round(cz, 2), "score": round(score_divergence(cz, pa), 1), "pattern": pat})

    bb = max(bull, key=lambda d: d["score"]) if bull else None
    br = max(bear, key=lambda d: d["score"]) if bear else None
    signals = []
    if bb and br:
        if bb["score"] >= br["score"]: signals.append(("bullish", bb, vol_ratio))
        else: signals.append(("bearish", br, vol_ratio))
    elif bb: signals.append(("bullish", bb, vol_ratio))
    elif br: signals.append(("bearish", br, vol_ratio))
    return signals


def check_outcome_multi(closed, trigger_pos, direction, stop_pts, target_pts, max_bars=30):
    """Check outcome with specific SL/target. Returns (result, pnl, max_profit, max_loss, bars_to_result)."""
    entry = closed[trigger_pos]["close"]
    max_profit = max_loss = 0
    end_pos = min(trigger_pos + max_bars, len(closed) - 1)

    for j in range(trigger_pos + 1, end_pos + 1):
        bar = closed[j]
        if direction == "bullish":
            profit = bar["high"] - entry
            loss = entry - bar["low"]
        else:
            profit = entry - bar["low"]
            loss = bar["high"] - entry
        if profit > max_profit: max_profit = profit
        if loss > max_loss: max_loss = loss
        # Check stop first (conservative — assume stop hit before target on same bar)
        if loss >= stop_pts:
            return "LOSS", -stop_pts, max_profit, max_loss, j - trigger_pos
        if profit >= target_pts:
            return "WIN", target_pts, max_profit, max_loss, j - trigger_pos

    if direction == "bullish":
        final = closed[end_pos]["close"] - entry
    else:
        final = entry - closed[end_pos]["close"]
    return "EXP", round(final, 2), max_profit, max_loss, end_pos - trigger_pos


def run():
    with engine.connect() as conn:
        days = conn.execute(text(
            "SELECT DISTINCT trade_date FROM es_range_bars ORDER BY trade_date"
        )).fetchall()

        all_days = {}
        for (trade_date,) in days:
            rows = conn.execute(text("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume, cumulative_delta, ts_start, ts_end, status
                FROM es_range_bars WHERE trade_date = :d AND status = 'closed'
                ORDER BY bar_idx
            """), {"d": trade_date}).mappings().all()
            if len(rows) < 25: continue
            bars = [{
                "idx": r["bar_idx"], "open": float(r["bar_open"]),
                "high": float(r["bar_high"]), "low": float(r["bar_low"]),
                "close": float(r["bar_close"]), "volume": int(r["bar_volume"]),
                "cvd": int(r["cumulative_delta"]),
                "ts_end": str(r["ts_end"]), "status": r["status"],
            } for r in rows]
            all_days[str(trade_date)] = bars

    min_bars = max(VOL_WINDOW, CVD_STD_WINDOW, PIVOT_LEFT + PIVOT_RIGHT + 1) + 1

    # Collect all d40 signals with full price path data
    all_signals = []

    for day_str, bars in sorted(all_days.items()):
        swings = detect_swings(bars, PIVOT_LEFT, PIVOT_RIGHT)
        last_fire = {"bullish": -100, "bearish": -100}

        for pos in range(min_bars, len(bars)):
            trigger = bars[pos]
            ts = trigger.get("ts_end", "")
            if ts:
                try:
                    hh = int(ts[11:13]); mm = int(ts[14:16])
                    t = dtime(hh, mm)
                    if t < SIGNAL_START: continue
                except: pass

            start_i = max(1, pos - CVD_STD_WINDOW)
            deltas = [bars[i]["cvd"] - bars[i - 1]["cvd"] for i in range(start_i, pos + 1)]
            if len(deltas) < 5: continue
            mean_d = sum(deltas) / len(deltas)
            cvd_std = max(1, (sum((d - mean_d) ** 2 for d in deltas) / len(deltas)) ** 0.5)
            atr_moves = [abs(bars[i]["close"] - bars[i - 1]["close"]) for i in range(start_i, pos + 1)]
            atr = max(0.01, sum(atr_moves) / len(atr_moves))

            sigs = scan_d40(bars[:pos + 1], swings, trigger, cvd_std, atr)
            for direction, info, vol_ratio in sigs:
                if pos - last_fire[direction] < COOLDOWN: continue
                last_fire[direction] = pos

                # Get full forward price path (up to 30 bars)
                entry = trigger["close"]
                path = []
                end_pos = min(pos + OUTCOME_BARS, len(bars) - 1)
                for j in range(pos + 1, end_pos + 1):
                    bar = bars[j]
                    if direction == "bullish":
                        profit = bar["high"] - entry
                        loss = entry - bar["low"]
                    else:
                        profit = entry - bar["low"]
                        loss = bar["high"] - entry
                    path.append({"bar": j - pos, "profit": round(profit, 2), "loss": round(loss, 2)})

                all_signals.append({
                    "day": day_str, "ts": ts, "pos": pos,
                    "direction": direction, "pattern": info["pattern"],
                    "entry": entry, "cvd_z": info["cvd_z"],
                    "path": path,
                })

    print(f"Total d40 signals: {len(all_signals)}")
    print()

    # Test RM combos
    rm_combos = [
        # (stop, target, label)
        (5, 10, "SL=5  T=10"),
        (6, 10, "SL=6  T=10"),
        (7, 10, "SL=7  T=10"),
        (8, 10, "SL=8  T=10"),
        (10, 10, "SL=10 T=10"),
        (12, 10, "SL=12 T=10"),
        (15, 10, "SL=15 T=10"),
        (5, 8, "SL=5  T=8 "),
        (6, 8, "SL=6  T=8 "),
        (7, 8, "SL=7  T=8 "),
        (8, 8, "SL=8  T=8 "),
        (5, 5, "SL=5  T=5 "),
        (7, 5, "SL=7  T=5 "),
        (5, 15, "SL=5  T=15"),
        (8, 15, "SL=8  T=15"),
        (10, 15, "SL=10 T=15"),
        (12, 15, "SL=12 T=15"),
    ]

    print(f"{'RM Combo':>16} | {'Sigs':>4} | {'W':>3} | {'L':>3} | {'E':>3} | {'WR':>5} | {'PnL':>8} | {'PnL/sig':>8} | {'PF':>5} | {'AvgBars':>7} | {'MaxDD':>6}")
    print("-" * 110)

    best_pnl = -9999
    best_label = ""

    for stop, target, label in rm_combos:
        wins = losses = exps = 0
        total_pnl = 0
        gross_win = gross_loss = 0
        total_bars = 0
        running_pnl = 0
        max_dd = 0
        peak = 0

        for sig in all_signals:
            # Simulate outcome from price path
            result = "EXP"
            pnl = 0
            bars_used = len(sig["path"])

            for step in sig["path"]:
                # Stop first (conservative)
                if step["loss"] >= stop:
                    result = "LOSS"
                    pnl = -stop
                    bars_used = step["bar"]
                    break
                if step["profit"] >= target:
                    result = "WIN"
                    pnl = target
                    bars_used = step["bar"]
                    break

            if result == "EXP":
                if sig["path"]:
                    last = sig["path"][-1]
                    pnl = last["profit"] - last["loss"]
                    # More accurate: net at expiry
                    pnl = last["profit"] if last["profit"] > last["loss"] else -last["loss"]
                    # Actually just use close-to-close
                    pnl = round(last["profit"] - last["loss"], 2)

            if result == "WIN":
                wins += 1
                gross_win += pnl
            elif result == "LOSS":
                losses += 1
                gross_loss += abs(pnl)
            else:
                exps += 1

            total_pnl += pnl
            total_bars += bars_used

            # Max drawdown tracking
            running_pnl += pnl
            if running_pnl > peak:
                peak = running_pnl
            dd = peak - running_pnl
            if dd > max_dd:
                max_dd = dd

        n = len(all_signals)
        wr = wins / n * 100 if n else 0
        pnl_per = total_pnl / n if n else 0
        pf = gross_win / gross_loss if gross_loss > 0 else 999
        avg_bars = total_bars / n if n else 0

        if total_pnl > best_pnl:
            best_pnl = total_pnl
            best_label = label

        print(f"{label:>16} | {n:>4} | {wins:>3} | {losses:>3} | {exps:>3} | "
              f"{wr:4.0f}% | {total_pnl:+7.1f} | {pnl_per:+7.2f} | "
              f"{pf:5.2f} | {avg_bars:6.1f} | {max_dd:5.1f}")

    print(f"\nBest: {best_label} with {best_pnl:+.1f} pts")

    # === Detailed analysis: max profit reached before stop ===
    print(f"\n{'='*80}")
    print(f"  MAX PROFIT BEFORE STOP (how far does price go our way before reversing?)")
    print(f"{'='*80}")

    # For each signal, track max favorable excursion (MFE) and max adverse excursion (MAE)
    mfes = []
    maes = []
    for sig in all_signals:
        mfe = 0  # max favorable
        mae = 0  # max adverse
        for step in sig["path"]:
            if step["profit"] > mfe: mfe = step["profit"]
            if step["loss"] > mae: mae = step["loss"]
        mfes.append(mfe)
        maes.append(mae)

    print(f"\nMax Favorable Excursion (MFE) — how far price goes in our favor:")
    for thresh in [3, 5, 7, 8, 10, 12, 15, 20]:
        count = sum(1 for m in mfes if m >= thresh)
        print(f"  MFE >= {thresh:>2}pt: {count:>3}/{len(mfes)} ({count/len(mfes)*100:4.0f}%)")

    print(f"\nMax Adverse Excursion (MAE) — how far price goes against us:")
    for thresh in [3, 5, 7, 8, 10, 12, 15]:
        count = sum(1 for m in maes if m >= thresh)
        print(f"  MAE >= {thresh:>2}pt: {count:>3}/{len(maes)} ({count/len(maes)*100:4.0f}%)")

    # Winners vs Losers MAE (with SL=12 T=10 as reference)
    print(f"\n--- MAE of WINNERS vs LOSERS (reference: SL=12, T=10) ---")
    w_maes = []
    l_maes = []
    for sig in all_signals:
        hit_target = False
        hit_stop = False
        mae = 0
        for step in sig["path"]:
            if step["loss"] > mae: mae = step["loss"]
            if step["loss"] >= 12: hit_stop = True; break
            if step["profit"] >= 10: hit_target = True; break
        if hit_target:
            w_maes.append(mae)
        elif hit_stop:
            l_maes.append(mae)

    if w_maes:
        print(f"  Winners ({len(w_maes)}): avg MAE={sum(w_maes)/len(w_maes):.1f} | "
              f"MAE<=5: {sum(1 for m in w_maes if m <= 5)}/{len(w_maes)} ({sum(1 for m in w_maes if m <= 5)/len(w_maes)*100:.0f}%) | "
              f"MAE<=3: {sum(1 for m in w_maes if m <= 3)}/{len(w_maes)} ({sum(1 for m in w_maes if m <= 3)/len(w_maes)*100:.0f}%)")
    if l_maes:
        print(f"  Losers ({len(l_maes)}): avg MAE={sum(l_maes)/len(l_maes):.1f} (all hit SL=12)")

    # Speed to target: how many bars to hit +10?
    print(f"\n--- BARS TO TARGET (+10pt) for winners ---")
    bars_to_target = []
    for sig in all_signals:
        for step in sig["path"]:
            if step["loss"] >= 12: break
            if step["profit"] >= 10:
                bars_to_target.append(step["bar"])
                break
    if bars_to_target:
        print(f"  Count: {len(bars_to_target)} winners")
        print(f"  Avg: {sum(bars_to_target)/len(bars_to_target):.1f} bars")
        print(f"  Median: {sorted(bars_to_target)[len(bars_to_target)//2]} bars")
        for thresh in [1, 2, 3, 5, 8, 10, 15]:
            count = sum(1 for b in bars_to_target if b <= thresh)
            print(f"  Within {thresh:>2} bars: {count}/{len(bars_to_target)} ({count/len(bars_to_target)*100:.0f}%)")

    # By pattern breakdown with best RM
    print(f"\n--- BY PATTERN (SL=8, T=10) ---")
    by_pat = defaultdict(list)
    for sig in all_signals:
        result = "EXP"; pnl = 0
        for step in sig["path"]:
            if step["loss"] >= 8: result = "LOSS"; pnl = -8; break
            if step["profit"] >= 10: result = "WIN"; pnl = 10; break
        by_pat[sig["pattern"]].append((result, pnl))

    for pat, outcomes in sorted(by_pat.items()):
        w = sum(1 for r, _ in outcomes if r == "WIN")
        pnl = sum(p for _, p in outcomes)
        print(f"  {pat:>20}: {len(outcomes):>3} sigs, {w}W, "
              f"WR={w/len(outcomes)*100:.0f}%, PnL={pnl:+.1f}")


if __name__ == "__main__":
    run()
