"""
Backtest: ES Absorption — Current Logic vs Proposed Swing-to-Swing Fix
Tests multiple MAX_TRIGGER_DISTANCE values (5, 8, 10, 15).
"""
import json
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import time as dtime

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)

# ── Settings ─────────────────────────────────────────────────────────
PIVOT_LEFT = 2
PIVOT_RIGHT = 2
VOL_WINDOW = 10
MIN_VOL_RATIO = 1.4
CVD_Z_MIN = 0.5
CVD_STD_WINDOW = 20
MAX_SWING_AGE = 50
OUTCOME_BARS = 15
TARGET_PTS = 10
STOP_PTS = 12
RTH_START = dtime(14, 30)
RTH_END = dtime(21, 0)
SIGNAL_START = dtime(15, 0)
COOLDOWN = 10

TRIGGER_DISTANCES = [40]


def detect_swings(closed, pivot_left, pivot_right):
    swings = []
    last_type = None
    for pos in range(pivot_left, len(closed) - pivot_right):
        bar = closed[pos]
        is_low = True
        for j in range(1, pivot_left + 1):
            if bar["low"] > closed[pos - j]["low"]:
                is_low = False; break
        if is_low:
            for j in range(1, pivot_right + 1):
                if bar["low"] > closed[pos + j]["low"]:
                    is_low = False; break
        is_high = True
        for j in range(1, pivot_left + 1):
            if bar["high"] < closed[pos - j]["high"]:
                is_high = False; break
        if is_high:
            for j in range(1, pivot_right + 1):
                if bar["high"] < closed[pos + j]["high"]:
                    is_high = False; break
        if not is_low and not is_high:
            continue
        if is_low and is_high:
            if last_type == "L": is_low = False
            elif last_type == "H": is_high = False
            else: is_high = False
        new_type = "L" if is_low else "H"
        new_swing = {
            "type": new_type,
            "price": bar["low"] if is_low else bar["high"],
            "cvd": bar["cvd"], "volume": bar["volume"],
            "bar_idx": bar["idx"], "pos": pos,
        }
        if not swings or last_type is None:
            swings.append(new_swing); last_type = new_type
        elif new_type == last_type:
            if new_type == "L" and new_swing["price"] <= swings[-1]["price"]:
                swings[-1] = new_swing
            elif new_type == "H" and new_swing["price"] >= swings[-1]["price"]:
                swings[-1] = new_swing
        else:
            swings.append(new_swing); last_type = new_type
    return swings


def compute_cvd_stats(closed, cvd_std_window):
    start_i = max(1, len(closed) - cvd_std_window)
    deltas = [closed[i]["cvd"] - closed[i - 1]["cvd"] for i in range(start_i, len(closed))]
    if len(deltas) < 5:
        return None, None
    mean_d = sum(deltas) / len(deltas)
    cvd_std = (sum((d - mean_d) ** 2 for d in deltas) / len(deltas)) ** 0.5
    if cvd_std < 1: cvd_std = 1
    atr_moves = [abs(closed[i]["close"] - closed[i - 1]["close"]) for i in range(start_i, len(closed))]
    atr = sum(atr_moves) / len(atr_moves) if atr_moves else 1.0
    if atr < 0.01: atr = 0.01
    return cvd_std, atr


def score_divergence(cvd_z, price_atr):
    base = min(100, cvd_z / 3.0 * 100)
    mult = min(2.0, 0.5 + price_atr * 0.5)
    return min(100, base * mult)


def scan_current(closed, swings, trigger, cvd_std, atr):
    trigger_idx = trigger["idx"]
    recent_vols = [b["volume"] for b in closed[-(VOL_WINDOW + 1):-1]]
    if not recent_vols: return []
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0: return []
    vol_ratio = trigger["volume"] / vol_avg
    if vol_ratio < MIN_VOL_RATIO: return []
    signals = []
    bull, bear = [], []
    for sw in swings:
        if sw["type"] == "L":
            if sw["price"] <= trigger["low"] and trigger["cvd"] < sw["cvd"]:
                cg = abs(trigger["cvd"] - sw["cvd"]); cz = cg / cvd_std
                if cz >= CVD_Z_MIN:
                    pd_ = abs(trigger["low"] - sw["price"]); pa = pd_ / atr
                    bull.append({"cvd_z": round(cz, 2), "score": round(score_divergence(cz, pa), 1),
                                 "pattern": "trigger_vs_swing_low"})
        elif sw["type"] == "H":
            if sw["price"] >= trigger["high"] and trigger["cvd"] > sw["cvd"]:
                cg = abs(trigger["cvd"] - sw["cvd"]); cz = cg / cvd_std
                if cz >= CVD_Z_MIN:
                    pd_ = abs(trigger["high"] - sw["price"]); pa = pd_ / atr
                    bear.append({"cvd_z": round(cz, 2), "score": round(score_divergence(cz, pa), 1),
                                 "pattern": "trigger_vs_swing_high"})
    bb = max(bull, key=lambda d: d["score"]) if bull else None
    br = max(bear, key=lambda d: d["score"]) if bear else None
    if bb and br:
        if bb["score"] >= br["score"]: signals.append(("bullish", bb, vol_ratio))
        else: signals.append(("bearish", br, vol_ratio))
    elif bb: signals.append(("bullish", bb, vol_ratio))
    elif br: signals.append(("bearish", br, vol_ratio))
    return signals


def scan_proposed(closed, swings, trigger, cvd_std, atr, max_trigger_dist):
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
    signals = []
    bull, bear = [], []
    for i in range(1, len(slows)):
        L1, L2 = slows[i - 1], slows[i]
        if trigger_idx - L2["bar_idx"] > max_trigger_dist: continue
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
        if trigger_idx - H2["bar_idx"] > max_trigger_dist: continue
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
    if bb and br:
        if bb["score"] >= br["score"]: signals.append(("bullish", bb, vol_ratio))
        else: signals.append(("bearish", br, vol_ratio))
    elif bb: signals.append(("bullish", bb, vol_ratio))
    elif br: signals.append(("bearish", br, vol_ratio))
    return signals


def check_outcome(closed, trigger_pos, direction):
    entry = closed[trigger_pos]["close"]
    max_profit = max_loss = 0
    end_pos = min(trigger_pos + OUTCOME_BARS, len(closed) - 1)
    for j in range(trigger_pos + 1, end_pos + 1):
        bar = closed[j]
        if direction == "bullish":
            profit = bar["high"] - entry; loss = entry - bar["low"]
        else:
            profit = entry - bar["low"]; loss = bar["high"] - entry
        if profit > max_profit: max_profit = profit
        if loss > max_loss: max_loss = loss
        if loss >= STOP_PTS: return "LOSS", -STOP_PTS, max_profit, max_loss
        if profit >= TARGET_PTS: return "WIN", TARGET_PTS, max_profit, max_loss
    if direction == "bullish": final = closed[end_pos]["close"] - entry
    else: final = entry - closed[end_pos]["close"]
    return "EXPIRED", round(final, 2), max_profit, max_loss


def run_backtest():
    with engine.connect() as conn:
        days = conn.execute(text(
            "SELECT DISTINCT trade_date FROM es_range_bars ORDER BY trade_date"
        )).fetchall()

        # Load all bars per day
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

    # Run current + each proposed distance
    results = {}
    for label in ["current"] + [f"proposed_d{d}" for d in TRIGGER_DISTANCES]:
        results[label] = []

    for day_str, bars in sorted(all_days.items()):
        # Per-method cooldown tracking
        cooldowns = {}
        for label in results:
            cooldowns[label] = {"bull": -100, "bear": -100}

        for end_i in range(min_bars, len(bars)):
            trigger = bars[end_i]
            ts_str = trigger["ts_end"]
            try:
                h, m = int(ts_str[11:13]), int(ts_str[14:16])
                bar_time = dtime(h, m)
            except: continue
            if bar_time < SIGNAL_START or bar_time > RTH_END: continue

            closed = bars[:end_i + 1]
            swings = detect_swings(closed, PIVOT_LEFT, PIVOT_RIGHT)
            cvd_std, atr = compute_cvd_stats(closed, CVD_STD_WINDOW)
            if cvd_std is None: continue

            # Current
            for direction, best, vr in scan_current(closed, swings, trigger, cvd_std, atr):
                dk = "bull" if direction == "bullish" else "bear"
                if trigger["idx"] - cooldowns["current"][dk] < COOLDOWN: continue
                outcome, pnl, mp, ml = check_outcome(bars, end_i, direction)
                results["current"].append({
                    "date": day_str, "time": ts_str[11:19], "direction": direction,
                    "price": trigger["close"], "pattern": best["pattern"],
                    "cvd_z": best["cvd_z"], "score": best["score"],
                    "outcome": outcome, "pnl": pnl,
                })
                cooldowns["current"][dk] = trigger["idx"]

            # Proposed (each distance)
            for dist in TRIGGER_DISTANCES:
                lbl = f"proposed_d{dist}"
                for direction, best, vr in scan_proposed(closed, swings, trigger, cvd_std, atr, dist):
                    dk = "bull" if direction == "bullish" else "bear"
                    if trigger["idx"] - cooldowns[lbl][dk] < COOLDOWN: continue
                    outcome, pnl, mp, ml = check_outcome(bars, end_i, direction)
                    results[lbl].append({
                        "date": day_str, "time": ts_str[11:19], "direction": direction,
                        "price": trigger["close"], "pattern": best["pattern"],
                        "cvd_z": best["cvd_z"], "score": best["score"],
                        "outcome": outcome, "pnl": pnl,
                    })
                    cooldowns[lbl][dk] = trigger["idx"]

    # ── Summary table ────────────────────────────────────────────────
    print(f'\n{"=" * 95}')
    print(f'  ES ABSORPTION BACKTEST — {len(all_days)} days ({", ".join(sorted(all_days.keys()))})')
    print(f'  Target={TARGET_PTS}pt, Stop={STOP_PTS}pt, Cooldown={COOLDOWN} bars, Signal after 10:00 ET')
    print(f'{"=" * 95}')
    print(f'{"Method":>18} | {"Signals":>7} | {"Bull":>4} | {"Bear":>4} | {"Wins":>4} | {"WR":>5} | {"PnL":>8} | {"W/day":>5} | {"PnL/sig":>7}')
    print('-' * 95)

    for label in ["current"] + [f"proposed_d{d}" for d in TRIGGER_DISTANCES]:
        sigs = results[label]
        n = len(sigs)
        if n == 0:
            print(f'{label:>18} | {0:>7} | {0:>4} | {0:>4} | {0:>4} | {"N/A":>5} | {0:>+8.1f} | {"N/A":>5} | {"N/A":>7}')
            continue
        w = sum(1 for s in sigs if s["outcome"] == "WIN")
        bl = sum(1 for s in sigs if s["direction"] == "bullish")
        br = sum(1 for s in sigs if s["direction"] == "bearish")
        pnl = sum(s["pnl"] for s in sigs)
        wr = w / n * 100
        ndays = len(all_days)
        print(f'{label:>18} | {n:>7} | {bl:>4} | {br:>4} | {w:>4} | {wr:>4.0f}% | {pnl:>+8.1f} | {n/ndays:>5.1f} | {pnl/n:>+7.1f}')

    # ── Detailed per-method ──────────────────────────────────────────
    for label in ["current"] + [f"proposed_d{d}" for d in TRIGGER_DISTANCES]:
        sigs = results[label]
        if not sigs: continue
        print(f'\n--- {label} ({len(sigs)} signals) ---')

        # By pattern
        by_pat = defaultdict(list)
        for s in sigs: by_pat[s["pattern"]].append(s)
        for pat, ps in sorted(by_pat.items()):
            w = sum(1 for s in ps if s["outcome"] == "WIN")
            pnl = sum(s["pnl"] for s in ps)
            print(f'  {pat:>22}: {len(ps):>3} sigs, {w:>2}W, WR={w/len(ps)*100:>3.0f}%, PnL={pnl:>+6.1f}')

        # By date
        by_date = defaultdict(list)
        for s in sigs: by_date[s["date"]].append(s)
        for dt, ds in sorted(by_date.items()):
            w = sum(1 for s in ds if s["outcome"] == "WIN")
            pnl = sum(s["pnl"] for s in ds)
            bl = sum(1 for s in ds if s["direction"] == "bullish")
            br = sum(1 for s in ds if s["direction"] == "bearish")
            print(f'  {dt}: {len(ds):>2} sigs ({bl}B/{br}S), {w}W, PnL={pnl:>+6.1f}')

    # ── Check if today's winning signal is captured ──────────────────
    print(f'\n{"=" * 60}')
    print(f'  TODAY (2026-02-25) SIGNALS BY METHOD')
    print(f'{"=" * 60}')
    for label in ["current"] + [f"proposed_d{d}" for d in TRIGGER_DISTANCES]:
        sigs = [s for s in results[label] if s["date"] == "2026-02-25"]
        print(f'\n  {label}:')
        if not sigs:
            print(f'    No signals')
            continue
        for s in sigs:
            print(f'    {s["time"]} {s["direction"]:>7} @ {s["price"]:.1f} '
                  f'{s["pattern"]:>20} z={s["cvd_z"]:.2f} -> {s["outcome"]} {s["pnl"]:+.1f}')

    # ── Direction flips: current vs best proposed ────────────────────
    best_label = None
    best_pnl = -9999
    for d in TRIGGER_DISTANCES:
        lbl = f"proposed_d{d}"
        pnl = sum(s["pnl"] for s in results[lbl])
        if pnl > best_pnl:
            best_pnl = pnl
            best_label = lbl

    if best_label:
        print(f'\n{"=" * 60}')
        print(f'  DIRECTION FLIPS: current vs {best_label}')
        print(f'{"=" * 60}')
        c_by = {(s["date"], s["time"]): s for s in results["current"]}
        p_by = {(s["date"], s["time"]): s for s in results[best_label]}
        flips = 0
        flip_current_right = 0
        flip_proposed_right = 0
        for key in c_by:
            if key in p_by and c_by[key]["direction"] != p_by[key]["direction"]:
                cs, ps = c_by[key], p_by[key]
                flips += 1
                c_ok = "WIN" if cs["outcome"] == "WIN" else ("LOSS" if cs["outcome"] == "LOSS" else "EXP")
                p_ok = "WIN" if ps["outcome"] == "WIN" else ("LOSS" if ps["outcome"] == "LOSS" else "EXP")
                if cs["pnl"] > ps["pnl"]: flip_current_right += 1
                else: flip_proposed_right += 1
                print(f'  {key[0]} {key[1]}: curr={cs["direction"]}({c_ok} {cs["pnl"]:+.1f}) '
                      f'-> prop={ps["direction"]}({p_ok} {ps["pnl"]:+.1f})')
        print(f'\n  Total flips: {flips} | Current right: {flip_current_right} | Proposed right: {flip_proposed_right}')


if __name__ == "__main__":
    run_backtest()
