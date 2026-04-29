"""
VIX-SPX Divergence v2 — Exit Strategy & Entry Optimization
===========================================================
Studies:
1. Trail-only exits (no fixed TP) vs fixed TP vs split-target
2. Entry timing: immediate vs pullback entry
3. Grading by Phase 1 strength → does grade predict outcome?
4. Both LONG and SHORT directions

Uses chain_snapshots for signal detection, spx_ohlc_1m for forward sim.
"""

import psycopg2
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict
import statistics

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

# ── Detection config: Loose (best SHORT WR) ──
# Phase 1: SPX moves > 6 pts, VIX reacts < 0.20
# Phase 2: VIX compresses > 0.25, SPX flat < 10
P1_SPX_MOVE = 6
P1_VIX_REACT_MAX = 0.20
P1_WIN_MIN, P1_WIN_MAX = 10, 30
P2_VIX_COMPRESS = 0.25
P2_SPX_FLAT = 10
P2_WIN_MIN, P2_WIN_MAX = 15, 60

BAD_DATES = {"2026-03-26"}


def load_data():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT ts AT TIME ZONE 'America/New_York' as et, spot, vix
        FROM chain_snapshots
        WHERE spot IS NOT NULL AND vix IS NOT NULL
        AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '15:30'
        ORDER BY ts;
    """)
    snaps = [{"ts": r[0], "date": r[0].date(), "spot": float(r[1]), "vix": float(r[2])} for r in cur.fetchall()]

    cur.execute("""
        SELECT ts AT TIME ZONE 'America/New_York' as et,
               bar_open, bar_high, bar_low, bar_close
        FROM spx_ohlc_1m
        WHERE (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
        ORDER BY ts;
    """)
    ohlc = [{"ts": r[0], "date": r[0].date(), "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4])} for r in cur.fetchall()]

    conn.close()
    return snaps, ohlc


def group_by_date(data):
    groups = defaultdict(list)
    for d in data:
        groups[d["date"]].append(d)
    return dict(groups)


def detect_signals(snaps_day, direction="long"):
    """Detect two-phase VIX-SPX divergence signals. One per day per direction."""
    n = len(snaps_day)
    signals = []

    # Phase 1: SPX moves but VIX doesn't react
    phase1_events = []
    for i in range(n):
        for j in range(i + 1, n):
            mins = (snaps_day[j]["ts"] - snaps_day[i]["ts"]).total_seconds() / 60
            if mins < P1_WIN_MIN:
                continue
            if mins > P1_WIN_MAX:
                break

            if direction == "long":
                spx_change = snaps_day[i]["spot"] - snaps_day[j]["spot"]  # drop
                vix_react = snaps_day[j]["vix"] - snaps_day[i]["vix"]     # rise expected
            else:
                spx_change = snaps_day[j]["spot"] - snaps_day[i]["spot"]  # rally
                vix_react = snaps_day[i]["vix"] - snaps_day[j]["vix"]     # drop expected

            if spx_change >= P1_SPX_MOVE and vix_react <= P1_VIX_REACT_MAX:
                phase1_events.append({
                    "end_idx": j,
                    "spx_move": spx_change,
                    "vix_react": vix_react,
                    "end_ts": snaps_day[j]["ts"],
                })

    if not phase1_events:
        return signals

    # Phase 2: VIX compresses while SPX flat
    used = set()
    for p1 in phase1_events:
        if p1["end_idx"] in used:
            continue
        p2_start = p1["end_idx"]
        for j in range(p2_start + 1, n):
            mins = (snaps_day[j]["ts"] - snaps_day[p2_start]["ts"]).total_seconds() / 60
            if mins < P2_WIN_MIN:
                continue
            if mins > P2_WIN_MAX:
                break

            if direction == "long":
                vix_compress = snaps_day[p2_start]["vix"] - snaps_day[j]["vix"]
            else:
                vix_compress = snaps_day[j]["vix"] - snaps_day[p2_start]["vix"]
            spx_range = abs(snaps_day[j]["spot"] - snaps_day[p2_start]["spot"])

            if vix_compress >= P2_VIX_COMPRESS and spx_range <= P2_SPX_FLAT:
                # Grade by Phase 1 strength
                p1_strength = p1["spx_move"]
                if p1_strength >= 12:
                    grade = "A+"
                elif p1_strength >= 10:
                    grade = "A"
                elif p1_strength >= 8:
                    grade = "B"
                else:
                    grade = "C"

                signals.append({
                    "ts": snaps_day[j]["ts"],
                    "spot": snaps_day[j]["spot"],
                    "vix": snaps_day[j]["vix"],
                    "p1_spx_move": p1["spx_move"],
                    "p1_vix_react": p1["vix_react"],
                    "p2_vix_compress": vix_compress,
                    "p2_spx_range": spx_range,
                    "direction": direction,
                    "grade": grade,
                })
                used.add(p1["end_idx"])
                return signals  # one per day per direction
    return signals


def simulate_trade(ohlc_day, signal_ts, direction, sl, trail_config, max_hold=120):
    """
    Simulate a single trade with trailing stop.
    trail_config: dict with keys:
      - be_trigger: pts profit to move stop to breakeven (None = no BE)
      - activation: pts profit to start trailing (None = no trail)
      - gap: trail gap behind max profit
      - fixed_tp: fixed take-profit level (None = no TP, ride trail)
    Returns: dict with outcome, pnl, mfe, mae, duration, exit_reason
    """
    start_idx = None
    for i, bar in enumerate(ohlc_day):
        if bar["ts"] >= signal_ts:
            start_idx = i
            break
    if start_idx is None:
        return None

    entry = ohlc_day[start_idx]["open"]
    stop = -sl  # in pts, negative
    max_profit = 0.0
    current_stop = stop  # pts from entry (negative = below for long)
    mfe = 0.0
    mae = 0.0

    be_trigger = trail_config.get("be_trigger")
    activation = trail_config.get("activation")
    gap = trail_config.get("gap")
    fixed_tp = trail_config.get("fixed_tp")

    for i in range(start_idx, min(start_idx + max_hold, len(ohlc_day))):
        bar = ohlc_day[i]
        elapsed = i - start_idx

        if direction == "long":
            bar_high_pnl = bar["high"] - entry
            bar_low_pnl = bar["low"] - entry
        else:
            bar_high_pnl = entry - bar["low"]
            bar_low_pnl = entry - bar["high"]

        mfe = max(mfe, bar_high_pnl)
        mae = max(mae, -bar_low_pnl if bar_low_pnl < 0 else 0)

        # Check stop hit (using worst price first)
        if bar_low_pnl <= current_stop:
            pnl = current_stop
            reason = "STOP" if current_stop == stop else ("BE" if current_stop == 0 else "TRAIL")
            return {"outcome": "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "BE"),
                    "pnl": pnl, "mfe": mfe, "mae": mae, "bars": elapsed, "reason": reason}

        # Check fixed TP
        if fixed_tp and bar_high_pnl >= fixed_tp:
            return {"outcome": "WIN", "pnl": fixed_tp, "mfe": mfe, "mae": mae,
                    "bars": elapsed, "reason": "TP"}

        # Update max profit
        max_profit = max(max_profit, bar_high_pnl)

        # BE trigger
        if be_trigger and max_profit >= be_trigger and current_stop < 0:
            current_stop = 0

        # Trail
        if activation and gap and max_profit >= activation:
            trail_level = max_profit - gap
            if trail_level > current_stop:
                current_stop = trail_level

    # Expired
    last_bar = ohlc_day[min(start_idx + max_hold - 1, len(ohlc_day) - 1)]
    if direction == "long":
        pnl = last_bar["close"] - entry
    else:
        pnl = entry - last_bar["close"]
    return {"outcome": "EXPIRED", "pnl": pnl, "mfe": mfe, "mae": mae,
            "bars": max_hold, "reason": "EXPIRED"}


def run_study():
    print("Loading data...")
    snaps, ohlc = load_data()
    snaps_by_date = group_by_date(snaps)
    ohlc_by_date = group_by_date(ohlc)
    dates = sorted(set(snaps_by_date.keys()) & set(ohlc_by_date.keys()))
    print(f"{len(dates)} trading days, {dates[0]} to {dates[-1]}\n")

    # ── Exit strategies to test ──
    trail_configs = {
        # name: (SL, trail_config_dict)
        "Fixed SL=8/T=10":          (8,  {"fixed_tp": 10}),
        "Fixed SL=8/T=20":          (8,  {"fixed_tp": 20}),
        "Fixed SL=12/T=20":         (12, {"fixed_tp": 20}),
        "Fixed SL=20/T=20":         (20, {"fixed_tp": 20}),
        "BE@8 Trail@15/g8":         (12, {"be_trigger": 8, "activation": 15, "gap": 8}),
        "BE@10 Trail@15/g8":        (12, {"be_trigger": 10, "activation": 15, "gap": 8}),
        "BE@10 Trail@20/g10":       (12, {"be_trigger": 10, "activation": 20, "gap": 10}),
        "BE@10 Trail@25/g10":       (20, {"be_trigger": 10, "activation": 25, "gap": 10}),
        "IMM Trail stop=max-8,-8":  (8,  {"be_trigger": None, "activation": 0.01, "gap": 8}),
        "IMM Trail stop=max-10,-10":(10, {"be_trigger": None, "activation": 0.01, "gap": 10}),
        "IMM Trail stop=max-12,-12":(12, {"be_trigger": None, "activation": 0.01, "gap": 12}),
        "Ride-to-close SL=12":      (12, {"be_trigger": 10}),  # BE@10 + hold 120min
        "Ride-to-close SL=20":      (20, {"be_trigger": 15}),  # BE@15 + hold 120min
        "BE@8 Trail@10/g5":         (8,  {"be_trigger": 8, "activation": 10, "gap": 5}),
        "BE@10 Trail@15/g5":        (12, {"be_trigger": 10, "activation": 15, "gap": 5}),
    }

    for direction in ["long", "short"]:
        print(f"\n{'='*80}")
        print(f"  DIRECTION: {direction.upper()}")
        print(f"{'='*80}")

        # Collect all signals
        all_signals = []
        for date in dates:
            if str(date) in BAD_DATES:
                continue
            if date not in ohlc_by_date:
                continue
            sigs = detect_signals(snaps_by_date[date], direction)
            for sig in sigs:
                all_signals.append(sig)

        if not all_signals:
            print("  0 signals")
            continue

        print(f"  {len(all_signals)} signals across {len(set(s['ts'].date() for s in all_signals))} days\n")

        # ── STUDY 1: Exit Strategy Comparison ──
        print(f"  {'Exit Strategy':>30} {'W':>3} {'L':>3} {'E':>3} {'BE':>3} {'WR':>6} {'PnL':>8} {'AvgPnL':>8} {'MaxDD':>7} {'PF':>6}")
        print(f"  {'-'*88}")

        best_pnl = -9999
        best_name = ""

        for tc_name, (sl, tc) in trail_configs.items():
            results = []
            for sig in all_signals:
                date = sig["ts"].date()
                if date not in ohlc_by_date:
                    continue
                res = simulate_trade(ohlc_by_date[date], sig["ts"], direction, sl, tc)
                if res:
                    results.append(res)

            if not results:
                continue

            wins = sum(1 for r in results if r["outcome"] == "WIN")
            losses = sum(1 for r in results if r["outcome"] == "LOSS")
            expired = sum(1 for r in results if r["outcome"] == "EXPIRED")
            be = sum(1 for r in results if r["outcome"] == "BE")
            total_pnl = sum(r["pnl"] for r in results)
            avg_pnl = total_pnl / len(results)
            gross_win = sum(r["pnl"] for r in results if r["pnl"] > 0)
            gross_loss = abs(sum(r["pnl"] for r in results if r["pnl"] < 0))
            pf = gross_win / max(0.01, gross_loss)

            # Running PnL for MaxDD
            running = 0
            peak = 0
            max_dd = 0
            for r in results:
                running += r["pnl"]
                peak = max(peak, running)
                dd = peak - running
                max_dd = max(max_dd, dd)

            wr = wins / max(1, wins + losses) * 100 if (wins + losses) > 0 else 0
            wl = wins + losses
            if wl == 0:
                wr = 0

            marker = " <--" if total_pnl > best_pnl else ""
            if total_pnl > best_pnl:
                best_pnl = total_pnl
                best_name = tc_name

            print(f"  {tc_name:>30} {wins:>3} {losses:>3} {expired:>3} {be:>3} {wr:>5.0f}% {total_pnl:>+7.1f} {avg_pnl:>+7.1f} {max_dd:>6.1f} {pf:>5.2f}{marker}")

        print(f"\n  >> Best exit: {best_name} ({best_pnl:+.1f} pts)")

        # ── STUDY 2: Grade Analysis ──
        print(f"\n  === GRADING (Phase 1 SPX move strength) ===")
        grade_order = ["A+", "A", "B", "C"]
        # Use the best trail config for grading analysis
        best_sl, best_tc = trail_configs[best_name]

        print(f"  Using exit: {best_name}")
        print(f"  {'Grade':>5} {'N':>3} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'AvgMFE':>8} {'AvgMAE':>8} {'P1 SPX':>8}")

        for grade in grade_order:
            grade_sigs = [s for s in all_signals if s["grade"] == grade]
            if not grade_sigs:
                print(f"  {grade:>5} {0:>3}")
                continue

            results = []
            mfes = []
            maes = []
            p1_moves = []
            for sig in grade_sigs:
                date = sig["ts"].date()
                if date not in ohlc_by_date:
                    continue
                res = simulate_trade(ohlc_by_date[date], sig["ts"], direction, best_sl, best_tc)
                if res:
                    results.append(res)
                    mfes.append(res["mfe"])
                    maes.append(res["mae"])
                    p1_moves.append(sig["p1_spx_move"])

            wins = sum(1 for r in results if r["outcome"] == "WIN")
            losses = sum(1 for r in results if r["outcome"] == "LOSS")
            total_pnl = sum(r["pnl"] for r in results)
            wr = wins / max(1, wins + losses) * 100 if (wins + losses) > 0 else 0
            avg_mfe = sum(mfes) / len(mfes) if mfes else 0
            avg_mae = sum(maes) / len(maes) if maes else 0
            avg_p1 = sum(p1_moves) / len(p1_moves) if p1_moves else 0

            print(f"  {grade:>5} {len(results):>3} {wins:>3} {losses:>3} {wr:>5.0f}% {total_pnl:>+7.1f} {avg_mfe:>+7.1f} {avg_mae:>7.1f} {avg_p1:>7.1f}")

        # ── STUDY 3: Entry Timing ──
        print(f"\n  === ENTRY TIMING (signal time-of-day) ===")
        print(f"  Using exit: {best_name}")

        # Bucket by signal hour
        hour_buckets = defaultdict(list)
        for sig in all_signals:
            h = sig["ts"].hour
            bucket = f"{h:02d}:00-{h:02d}:59"
            date = sig["ts"].date()
            if date not in ohlc_by_date:
                continue
            res = simulate_trade(ohlc_by_date[date], sig["ts"], direction, best_sl, best_tc)
            if res:
                hour_buckets[bucket].append(res)

        print(f"  {'Window':>14} {'N':>3} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'AvgMFE':>8}")
        for bucket in sorted(hour_buckets.keys()):
            results = hour_buckets[bucket]
            wins = sum(1 for r in results if r["outcome"] == "WIN")
            losses = sum(1 for r in results if r["outcome"] == "LOSS")
            total_pnl = sum(r["pnl"] for r in results)
            wr = wins / max(1, wins + losses) * 100 if (wins + losses) > 0 else 0
            avg_mfe = sum(r["mfe"] for r in results) / len(results)
            print(f"  {bucket:>14} {len(results):>3} {wins:>3} {losses:>3} {wr:>5.0f}% {total_pnl:>+7.1f} {avg_mfe:>+7.1f}")

        # ── STUDY 4: Pullback Entry ──
        # After signal fires, does waiting for a 3-pt pullback improve entry?
        print(f"\n  === PULLBACK ENTRY vs IMMEDIATE ===")
        print(f"  Using exit: {best_name}")

        pullback_amounts = [0, 2, 3, 5]
        print(f"  {'Pullback':>10} {'N':>3} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'Filled%':>8}")

        for pb in pullback_amounts:
            filled = 0
            not_filled = 0
            results = []
            for sig in all_signals:
                date = sig["ts"].date()
                if date not in ohlc_by_date:
                    continue
                ohlc_day = ohlc_by_date[date]

                if pb == 0:
                    # Immediate entry
                    res = simulate_trade(ohlc_day, sig["ts"], direction, best_sl, best_tc)
                    if res:
                        results.append(res)
                        filled += 1
                else:
                    # Wait for pullback within 30 min
                    entry_price = None
                    entry_ts = None
                    for bar in ohlc_day:
                        if bar["ts"] < sig["ts"]:
                            continue
                        elapsed = (bar["ts"] - sig["ts"]).total_seconds() / 60
                        if elapsed > 30:
                            break

                        if direction == "long":
                            # Wait for price to dip pb pts below signal spot
                            if bar["low"] <= sig["spot"] - pb:
                                entry_ts = bar["ts"]
                                break
                        else:
                            # Wait for price to rally pb pts above signal spot
                            if bar["high"] >= sig["spot"] + pb:
                                entry_ts = bar["ts"]
                                break

                    if entry_ts:
                        res = simulate_trade(ohlc_day, entry_ts, direction, best_sl, best_tc)
                        if res:
                            results.append(res)
                            filled += 1
                    else:
                        not_filled += 1

            total = filled + not_filled
            fill_pct = filled / max(1, total) * 100
            wins = sum(1 for r in results if r["outcome"] == "WIN")
            losses = sum(1 for r in results if r["outcome"] == "LOSS")
            total_pnl = sum(r["pnl"] for r in results)
            wr = wins / max(1, wins + losses) * 100 if (wins + losses) > 0 else 0
            label = "Immediate" if pb == 0 else f"{pb}pt pullback"
            print(f"  {label:>10} {len(results):>3} {wins:>3} {losses:>3} {wr:>5.0f}% {total_pnl:>+7.1f} {fill_pct:>6.0f}%")

        # ── STUDY 5: VIX Level Impact ──
        print(f"\n  === VIX LEVEL AT SIGNAL ===")
        print(f"  Using exit: {best_name}")

        vix_buckets = defaultdict(list)
        for sig in all_signals:
            v = sig["vix"]
            if v < 22:
                bucket = "VIX < 22"
            elif v < 26:
                bucket = "VIX 22-26"
            elif v < 30:
                bucket = "VIX 26-30"
            else:
                bucket = "VIX >= 30"

            date = sig["ts"].date()
            if date not in ohlc_by_date:
                continue
            res = simulate_trade(ohlc_by_date[date], sig["ts"], direction, best_sl, best_tc)
            if res:
                vix_buckets[bucket].append((res, sig))

        print(f"  {'VIX Range':>12} {'N':>3} {'W':>3} {'L':>3} {'WR':>6} {'PnL':>8} {'AvgMFE':>8}")
        for bucket in ["VIX < 22", "VIX 22-26", "VIX 26-30", "VIX >= 30"]:
            if bucket not in vix_buckets:
                print(f"  {bucket:>12} {0:>3}")
                continue
            results = [r for r, s in vix_buckets[bucket]]
            wins = sum(1 for r in results if r["outcome"] == "WIN")
            losses = sum(1 for r in results if r["outcome"] == "LOSS")
            total_pnl = sum(r["pnl"] for r in results)
            wr = wins / max(1, wins + losses) * 100 if (wins + losses) > 0 else 0
            avg_mfe = sum(r["mfe"] for r in results) / len(results)
            print(f"  {bucket:>12} {len(results):>3} {wins:>3} {losses:>3} {wr:>5.0f}% {total_pnl:>+7.1f} {avg_mfe:>+7.1f}")

        # ── Per-signal detail with best exit ──
        print(f"\n  === PER-SIGNAL DETAIL ({best_name}) ===")
        print(f"  {'Date':>12} {'Time':>6} {'Grade':>5} {'Spot':>8} {'VIX':>7} {'Result':>8} {'PnL':>7} {'MFE':>7} {'MAE':>6} {'Bars':>5} {'Reason':>8}")

        for sig in sorted(all_signals, key=lambda x: x["ts"]):
            date = sig["ts"].date()
            if date not in ohlc_by_date:
                continue
            res = simulate_trade(ohlc_by_date[date], sig["ts"], direction, best_sl, best_tc)
            if res:
                print(f"  {str(date):>12} {str(sig['ts'].time())[:5]:>6} {sig['grade']:>5} {sig['spot']:>8.1f} {sig['vix']:>7.2f} "
                      f"{res['outcome']:>8} {res['pnl']:>+6.1f} {res['mfe']:>+6.1f} {res['mae']:>5.1f} {res['bars']:>5} {res['reason']:>8}")


if __name__ == "__main__":
    run_study()
