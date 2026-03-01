"""
Backtest: ES Absorption d40 — Advanced RM with trailing stops.
Tests BE trails, continuous trails, and rung-based trails.
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
SIGNAL_START = dtime(15, 0)
COOLDOWN = 10
OUTCOME_BARS = 30


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


def sim_fixed(path, stop, target):
    """Fixed SL/target."""
    for step in path:
        if step["loss"] >= stop: return -stop
        if step["profit"] >= target: return target
    return path[-1]["profit"] - path[-1]["loss"] if path else 0


def sim_be_trail(path, initial_stop, target, be_trigger, be_offset=0.5):
    """Fixed target + breakeven trail at be_trigger."""
    stop = initial_stop
    for step in path:
        # Move stop to BE once profit hits be_trigger
        if step["profit"] >= be_trigger and stop > be_offset:
            stop = be_offset  # BE + commissions
        if step["loss"] >= stop:
            return -stop
        if step["profit"] >= target:
            return target
    return path[-1]["profit"] - path[-1]["loss"] if path else 0


def sim_continuous_trail(path, initial_stop, target, trail_activation, trail_gap):
    """Fixed target + continuous trail after activation."""
    stop = initial_stop
    max_profit = 0
    for step in path:
        if step["profit"] > max_profit:
            max_profit = step["profit"]
        # Activate trail when max profit reaches activation
        if max_profit >= trail_activation:
            trail_stop = max_profit - trail_gap
            if trail_stop > -stop:  # only tighten, never widen
                stop = -trail_stop  # convert to positive loss amount
                # Actually: stop is the max loss allowed
                # If trail_stop > 0, stop = trail_stop (profit locked)
                # Check if price pulled back beyond trail
                if step["loss"] >= 0 and step["profit"] < trail_stop:
                    # Price dropped below trail
                    return trail_stop  # lock in this profit
        if step["loss"] >= initial_stop:
            return -initial_stop
        if target is not None and step["profit"] >= target:
            return target
    return path[-1]["profit"] - path[-1]["loss"] if path else 0


def sim_continuous_trail_v2(path, initial_stop, target, trail_activation, trail_gap):
    """Fixed target + continuous trail. Simulates bar-by-bar."""
    max_profit = 0
    for step in path:
        profit = step["profit"]
        loss = step["loss"]

        if profit > max_profit:
            max_profit = profit

        # Determine current stop level
        if max_profit >= trail_activation:
            trail_lock = max_profit - trail_gap
            effective_stop = max(-trail_lock, 0)  # if trail_lock > 0, stop is a profit
            # Check if loss exceeds what trail allows
            # trail_lock is the min profit we lock in
            # If trail_lock = 5 and current loss = 2, net = profit - loss
            # Actually simpler: price dropped by `loss` from entry
            # Trail says: max allowed drop from max_profit level
            # max_profit - current_profit <= trail_gap
            # current_profit = profit (or -loss if negative)
            # Actually use raw distances:
            if loss > 0 and (max_profit - (-loss)) > trail_gap:
                # Trailed out
                return max(trail_lock, 0)
            if profit < trail_lock and loss > 0:
                return max(trail_lock, 0)
        else:
            # Not activated — use initial stop
            if loss >= initial_stop:
                return -initial_stop

        # Target check
        if target is not None and profit >= target:
            return target

    return path[-1]["profit"] - path[-1]["loss"] if path else 0


def sim_trail_clean(path, initial_stop, target, trail_activation, trail_gap):
    """Clean trail sim: track max_profit, once activated lock in max_profit-gap."""
    max_profit = 0
    for step in path:
        p = step["profit"]
        l = step["loss"]

        if p > max_profit:
            max_profit = p

        # Initial stop
        if l >= initial_stop:
            return -initial_stop

        # Trail activated?
        if max_profit >= trail_activation:
            lock = max_profit - trail_gap
            # Price went against us: current P&L = p - l (but that's not right)
            # Actually: profit = max favorable in this bar, loss = max adverse in this bar
            # Both can be positive in same bar (bar range covers both sides of entry)
            # If loss side reaches beyond lock point, we're trailed out
            # Lock means: we lock in `lock` pts. If price drops `lock` pts below entry,
            # our P&L is -lock. But lock is positive, so we need price to drop below
            # entry - lock distance? No:
            # lock = max_profit - gap. E.g., max_profit=8, gap=3, lock=5
            # This means: stop moves to entry + 5 pts. If price drops below that, exit at +5
            # In terms of loss: if loss > max_profit - lock = gap? No.
            # Actually: trail stop is at entry + lock. Loss is distance below entry.
            # So if the bar's low is at entry - loss, and our trail stop is at entry + lock,
            # the bar would hit the trail stop if the bar's low <= entry + lock
            # Wait, I'm confusing myself. Let me think simply:
            #
            # Entry = 100. Bullish. Price goes to 108 (max_profit=8).
            # trail_activation=5, trail_gap=3. lock = 8-3 = 5.
            # Trail stop = entry + lock = 105. (lock in +5 pts)
            # If next bar low = 104 → below 105 → trailed out at +5.
            # loss = entry - low = 100 - 104 = actually for bullish, loss = entry - low = -4?
            # No: for bullish, loss = entry - bar["low"]. If entry=100, low=104, loss = -4 (negative = no loss)
            # If low=94, loss = 6.
            # Trail stop at +5 means: if price drops to entry + 5 = 105 from high side.
            # But loss is measured from entry downward. Trail is measured from entry upward.
            # Trail stop price = entry + lock. Hit when bar price <= entry + lock.
            # For bullish: bar drops below entry + lock when bar["low"] < entry + lock
            # In our path, profit = bar["high"] - entry, loss = entry - bar["low"]
            # bar["low"] = entry - loss. Trail stop at entry + lock.
            # bar["low"] < entry + lock → entry - loss < entry + lock → -loss < lock → always true when lock > 0!
            # That can't be right.
            #
            # Oh I see the issue. The trail stop should be:
            # When max_profit reaches trail_activation, stop moves UP.
            # Trail stop PRICE = entry + (max_profit - trail_gap)
            # For bullish: exit if price drops to trail stop price.
            # price drops to trail stop when: current price <= entry + lock
            # In bar terms: bar low <= entry + lock
            # bar low = entry - loss (where loss = entry - bar_low)
            # entry - loss <= entry + lock → -loss <= lock → loss >= -lock
            # Since lock > 0, this is always true (loss >= 0 >= -lock)
            # Hmm, that means EVERY bar would trigger the trail. That can't be right.
            #
            # I think the issue is: for an already-profitable trade, the "loss" in our path
            # represents how far below ENTRY the bar went, not how far below MAX PROFIT.
            # The trail stop is relative to entry, not to max profit.
            #
            # Let me reconsider:
            # Entry = 100, bullish. Max profit = 8 (price hit 108).
            # Trail activation = 5, gap = 3. Trail stop = entry + 5 = 105. (lock = 5 pts)
            # If bar: high=106, low=104. Profit=6, loss=entry-104=-4 (bar above entry, no loss)
            # → bar low 104 < trail stop 105 → TRAILED OUT at +5?
            # Yes! Bar went below 105 (the trail stop), so we exit at +5.
            #
            # If bar: high=109, low=106. Profit=9, loss=entry-106=-6 (no loss).
            # Trail stop = 105. Bar low = 106 > 105 → NOT trailed.
            # Update max_profit = 9. New trail stop = entry + (9-3) = 106.
            #
            # OK so the check is: bar_low < trail_stop_price
            # bar_low = entry - loss (when loss is negative, bar is above entry)
            # trail_stop_price = entry + lock
            # bar_low < trail_stop_price → entry - loss < entry + lock → loss > -lock
            #
            # When lock = 5: loss > -5. Since loss is "entry - bar_low":
            # If bar_low = 104, entry = 100: loss = 100 - 104 = -4. -4 > -5? YES → trailed.
            # If bar_low = 106, entry = 100: loss = 100 - 106 = -6. -6 > -5? NO → not trailed. ✓
            #
            # Wait, that's backwards. loss = -6 means bar is ABOVE entry. -6 > -5 is FALSE.
            # So not trailed. Correct, because bar_low=106 > trail_stop=105.
            #
            # loss = -4 means bar_low = 104. -4 > -5 is TRUE. Trailed at 105. But bar went to 104
            # which is below 105. ✓
            #
            # OK so the condition: loss > -lock (or equivalently, entry - bar_low > -(max_profit - gap))
            # But wait, in our path, `loss` is `entry - bar_low` for bullish. When bar is above entry,
            # loss is NEGATIVE. When bar is below entry, loss is POSITIVE.
            #
            # Hmm, actually in the original code:
            # if direction == "bullish": loss = entry - bar["low"]
            # If entry=100, bar_low=95: loss = 5 (positive)
            # If entry=100, bar_low=105: loss = -5 (negative)
            #
            # So trail check: loss > -lock means entry - bar_low > -(max_profit - gap)
            # bar_low < entry + max_profit - gap
            # bar_low < entry + lock
            # That's correct!
            #
            # For the example: lock=5, loss=-4 (bar_low=104): -4 > -5 → TRUE → trailed ✓
            # lock=5, loss=-6 (bar_low=106): -6 > -5 → FALSE → not trailed ✓
            # lock=5, loss=2 (bar_low=98): 2 > -5 → TRUE → trailed ✓ (bar went way below trail)

            if l > -lock:
                return max(lock, 0)

        # Target
        if target is not None and p >= target:
            return target

    # Expired
    if path:
        return path[-1]["profit"] - path[-1]["loss"]
    return 0


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
                FROM es_range_bars WHERE trade_date = :d AND status = 'closed' AND source = 'live'
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

    # Collect signals with price paths
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
                    if dtime(hh, mm) < SIGNAL_START: continue
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
                    "entry": entry, "cvd_z": info["cvd_z"], "path": path,
                })

    n = len(all_signals)
    print(f"Total d40 signals: {n} across {len(all_days)} days\n")

    # RM strategies to test
    strategies = [
        ("Fixed SL=12 T=10 (current)", lambda p: sim_fixed(p, 12, 10)),
        ("Fixed SL=10 T=10",           lambda p: sim_fixed(p, 10, 10)),
        ("Fixed SL=8  T=10",           lambda p: sim_fixed(p, 8, 10)),
        ("Fixed SL=5  T=5",            lambda p: sim_fixed(p, 5, 5)),
        ("BE@3 SL=10 T=10",            lambda p: sim_be_trail(p, 10, 10, 3, 0.5)),
        ("BE@5 SL=10 T=10",            lambda p: sim_be_trail(p, 10, 10, 5, 0.5)),
        ("BE@5 SL=12 T=10",            lambda p: sim_be_trail(p, 12, 10, 5, 0.5)),
        ("BE@3 SL=8  T=10",            lambda p: sim_be_trail(p, 8, 10, 3, 0.5)),
        ("BE@5 SL=8  T=10",            lambda p: sim_be_trail(p, 8, 10, 5, 0.5)),
        ("BE@3 SL=12 T=10",            lambda p: sim_be_trail(p, 12, 10, 3, 0.5)),
        ("Trail act=5 gap=3 SL=10",    lambda p: sim_trail_clean(p, 10, None, 5, 3)),
        ("Trail act=5 gap=3 SL=12",    lambda p: sim_trail_clean(p, 12, None, 5, 3)),
        ("Trail act=5 gap=3 SL=8",     lambda p: sim_trail_clean(p, 8, None, 5, 3)),
        ("Trail act=8 gap=3 SL=10",    lambda p: sim_trail_clean(p, 10, None, 8, 3)),
        ("Trail act=8 gap=5 SL=10",    lambda p: sim_trail_clean(p, 10, None, 8, 5)),
        ("Trail act=8 gap=3 SL=12",    lambda p: sim_trail_clean(p, 12, None, 8, 3)),
        ("Trail act=10 gap=5 SL=12",   lambda p: sim_trail_clean(p, 12, None, 10, 5)),
        ("T=10 + trail act=10 gap=3 SL=12", lambda p: sim_trail_clean(p, 12, 10, 10, 3)),
        ("T=10 + trail act=10 gap=3 SL=10", lambda p: sim_trail_clean(p, 10, 10, 10, 3)),
        ("T=10 + trail act=10 gap=5 SL=12", lambda p: sim_trail_clean(p, 12, 10, 10, 5)),
    ]

    print(f"{'Strategy':>35} | {'W':>3} | {'L':>3} | {'E':>3} | {'WR':>5} | {'PnL':>8} | {'PnL/sig':>8} | {'MaxDD':>6}")
    print("-" * 95)

    best_pnl = -9999
    best_label = ""

    for label, sim_fn in strategies:
        wins = losses = exps = 0
        total_pnl = 0
        running = 0; peak = 0; max_dd = 0

        for sig in all_signals:
            pnl = sim_fn(sig["path"])
            total_pnl += pnl
            if pnl > 0: wins += 1
            elif pnl < 0: losses += 1
            else: exps += 1

            running += pnl
            if running > peak: peak = running
            dd = peak - running
            if dd > max_dd: max_dd = dd

        wr = wins / n * 100 if n else 0
        pnl_per = total_pnl / n if n else 0

        if total_pnl > best_pnl:
            best_pnl = total_pnl; best_label = label

        print(f"{label:>35} | {wins:>3} | {losses:>3} | {exps:>3} | "
              f"{wr:4.0f}% | {total_pnl:+7.1f} | {pnl_per:+7.2f} | {max_dd:5.1f}")

    print(f"\nBest: {best_label} with {best_pnl:+.1f} pts")

    # Per-pattern breakdown for top 3
    print(f"\n{'='*80}")
    print(f"  PER-PATTERN BREAKDOWN (top strategies)")
    print(f"{'='*80}")

    top_strats = [
        ("Fixed SL=12 T=10", lambda p: sim_fixed(p, 12, 10)),
        ("BE@5 SL=12 T=10", lambda p: sim_be_trail(p, 12, 10, 5, 0.5)),
        ("Trail act=5 gap=3 SL=12", lambda p: sim_trail_clean(p, 12, None, 5, 3)),
        ("Trail act=8 gap=3 SL=12", lambda p: sim_trail_clean(p, 12, None, 8, 3)),
    ]

    for label, sim_fn in top_strats:
        print(f"\n  {label}:")
        by_pat = defaultdict(list)
        for sig in all_signals:
            pnl = sim_fn(sig["path"])
            by_pat[sig["pattern"]].append(pnl)
        for pat, pnls in sorted(by_pat.items()):
            w = sum(1 for p in pnls if p > 0)
            total = sum(pnls)
            print(f"    {pat:>20}: {len(pnls):>3} sigs, {w}W, "
                  f"WR={w/len(pnls)*100:.0f}%, PnL={total:+.1f}")

    # Per-day breakdown for best strategy
    print(f"\n{'='*80}")
    print(f"  PER-DAY BREAKDOWN (best: {best_label})")
    print(f"{'='*80}")
    best_fn = [fn for lbl, fn in strategies if lbl == best_label][0]
    by_day = defaultdict(list)
    for sig in all_signals:
        pnl = best_fn(sig["path"])
        by_day[sig["day"]].append(pnl)
    for day, pnls in sorted(by_day.items()):
        w = sum(1 for p in pnls if p > 0)
        total = sum(pnls)
        print(f"  {day}: {len(pnls):>3} sigs, {w}W/{len(pnls)-w}L, PnL={total:+.1f}")


if __name__ == "__main__":
    run()
