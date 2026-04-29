"""
VIX-SPX Two-Phase Divergence Backtest
======================================
Detects the pattern user spotted on Mar 27:
  Phase 1: SPX drops but VIX stays flat (vol sellers suppressing)
  Phase 2: VIX drops while SPX flat (spring loading)
  Release: SPX explodes up

Also tests bearish mirror:
  Phase 1: SPX rallies but VIX stays flat (vol buyers not reacting)
  Phase 2: VIX rises while SPX flat
  Release: SPX drops

Uses chain_snapshots (2-min VIX+SPX) for detection,
spx_ohlc_1m (1-min bars) for forward MFE/MAE measurement.
"""

import psycopg2
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

# ── Thresholds to test ──
# Phase 1: SPX drops > SPX_DROP_MIN but VIX rises < VIX_REACT_MAX
# Phase 2: VIX drops > VIX_COMPRESS_MIN while SPX range < SPX_FLAT_MAX
# Windows in minutes

CONFIGS = [
    # name, p1_spx_drop, p1_vix_react_max, p1_window_min, p1_window_max,
    #        p2_vix_compress, p2_spx_flat, p2_window_min, p2_window_max
    ("Loose",   6, 0.20, 10, 30,   0.25, 10, 15, 60),
    ("Medium",  8, 0.15, 10, 30,   0.30,  8, 15, 50),
    ("Tight",  10, 0.15, 12, 30,   0.40,  6, 15, 45),
    ("XTight", 10, 0.10, 12, 30,   0.50,  5, 20, 45),
    # Also test single-phase (current VIX Compression style) for comparison
    ("Current_VixComp", 0, 99, 0, 0,  1.0, 20, 10, 45),  # no Phase 1, just VIX drop >1 + SPX <20
]


def load_data():
    """Load chain_snapshots and spx_ohlc_1m from DB."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Chain snapshots: 2-min VIX+SPX
    cur.execute("""
        SELECT ts AT TIME ZONE 'America/New_York' as et,
               spot, vix
        FROM chain_snapshots
        WHERE spot IS NOT NULL AND vix IS NOT NULL
        AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '15:30'
        ORDER BY ts;
    """)
    snaps = []
    for row in cur.fetchall():
        snaps.append({
            "ts": row[0],
            "date": row[0].date(),
            "time": row[0].time(),
            "spot": float(row[1]),
            "vix": float(row[2]),
        })

    # SPX 1-min bars for forward measurement
    cur.execute("""
        SELECT ts AT TIME ZONE 'America/New_York' as et,
               bar_open, bar_high, bar_low, bar_close
        FROM spx_ohlc_1m
        WHERE (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
        ORDER BY ts;
    """)
    ohlc = []
    for row in cur.fetchall():
        ohlc.append({
            "ts": row[0],
            "date": row[0].date(),
            "time": row[0].time(),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
        })

    conn.close()
    return snaps, ohlc


def group_by_date(data):
    """Group list of dicts by date."""
    groups = defaultdict(list)
    for d in data:
        groups[d["date"]].append(d)
    return dict(groups)


def measure_forward(ohlc_day, signal_ts, direction="long", max_minutes=90):
    """
    Measure forward MFE, MAE, and outcome using 1-min OHLC bars.
    Returns dict with mfe, mae, close_at_N min, etc.
    """
    # Find bars starting from signal time
    start_idx = None
    for i, bar in enumerate(ohlc_day):
        if bar["ts"] >= signal_ts:
            start_idx = i
            break

    if start_idx is None:
        return None

    entry_price = ohlc_day[start_idx]["open"]
    mfe = 0.0  # max favorable excursion
    mae = 0.0  # max adverse excursion
    close_15 = None
    close_30 = None
    close_45 = None
    close_60 = None
    close_90 = None

    for i in range(start_idx, min(start_idx + max_minutes, len(ohlc_day))):
        bar = ohlc_day[i]
        elapsed = i - start_idx

        if direction == "long":
            excursion_fav = bar["high"] - entry_price
            excursion_adv = entry_price - bar["low"]
        else:
            excursion_fav = entry_price - bar["low"]
            excursion_adv = bar["high"] - entry_price

        mfe = max(mfe, excursion_fav)
        mae = max(mae, excursion_adv)

        close_price = bar["close"]
        pnl = (close_price - entry_price) if direction == "long" else (entry_price - close_price)

        if elapsed == 15 and close_15 is None:
            close_15 = pnl
        if elapsed == 30 and close_30 is None:
            close_30 = pnl
        if elapsed == 45 and close_45 is None:
            close_45 = pnl
        if elapsed == 60 and close_60 is None:
            close_60 = pnl
        if elapsed == 90 and close_90 is None:
            close_90 = pnl

    # Fixed RM outcomes
    sl8 = "WIN" if mfe >= 10 else ("LOSS" if mae >= 8 else "EXPIRED")
    sl12 = "WIN" if mfe >= 10 else ("LOSS" if mae >= 12 else "EXPIRED")
    sl20 = "WIN" if mfe >= 20 else ("LOSS" if mae >= 20 else "EXPIRED")

    return {
        "entry": entry_price,
        "mfe": mfe,
        "mae": mae,
        "close_15": close_15,
        "close_30": close_30,
        "close_45": close_45,
        "close_60": close_60,
        "close_90": close_90,
        "sl8_t10": sl8,
        "sl12_t10": sl12,
        "sl20_t20": sl20,
    }


def detect_signals(snaps_day, config, direction="long"):
    """
    Detect two-phase VIX-SPX divergence signals on a single day.
    Returns list of signal dicts.
    """
    name, p1_spx_drop, p1_vix_react_max, p1_win_min, p1_win_max, \
        p2_vix_compress, p2_spx_flat, p2_win_min, p2_win_max = config

    signals = []
    used_p1_indices = set()  # prevent re-using same Phase 1

    # Skip Phase 1 for "Current_VixComp" comparison
    skip_phase1 = (name == "Current_VixComp")

    n = len(snaps_day)

    if skip_phase1:
        # Single-phase: just scan for VIX drop + SPX flat (like current code)
        for i in range(n):
            for j in range(i + 1, n):
                mins = (snaps_day[j]["ts"] - snaps_day[i]["ts"]).total_seconds() / 60
                if mins < p2_win_min or mins > p2_win_max:
                    if mins > p2_win_max:
                        break
                    continue

                if direction == "long":
                    vix_drop = snaps_day[i]["vix"] - snaps_day[j]["vix"]
                    spx_range = abs(snaps_day[j]["spot"] - snaps_day[i]["spot"])
                else:
                    vix_drop = snaps_day[j]["vix"] - snaps_day[i]["vix"]  # VIX rise for shorts
                    spx_range = abs(snaps_day[j]["spot"] - snaps_day[i]["spot"])

                if vix_drop >= p2_vix_compress and spx_range <= p2_spx_flat:
                    # One signal per day for comparison
                    signals.append({
                        "ts": snaps_day[j]["ts"],
                        "spot": snaps_day[j]["spot"],
                        "vix": snaps_day[j]["vix"],
                        "p1_spx_drop": 0,
                        "p1_vix_react": 0,
                        "p2_vix_compress": vix_drop,
                        "p2_spx_range": spx_range,
                        "direction": direction,
                    })
                    return signals  # one per day
        return signals

    # ── Two-phase detection ──
    # Phase 1: Find windows where SPX drops but VIX doesn't react
    phase1_events = []
    for i in range(n):
        for j in range(i + 1, n):
            mins = (snaps_day[j]["ts"] - snaps_day[i]["ts"]).total_seconds() / 60
            if mins < p1_win_min:
                continue
            if mins > p1_win_max:
                break

            if direction == "long":
                spx_change = snaps_day[i]["spot"] - snaps_day[j]["spot"]  # positive = drop
                vix_change = snaps_day[j]["vix"] - snaps_day[i]["vix"]    # positive = rise
            else:
                spx_change = snaps_day[j]["spot"] - snaps_day[i]["spot"]  # positive = rally
                vix_change = snaps_day[i]["vix"] - snaps_day[j]["vix"]    # positive = drop (expected on rally)

            if spx_change >= p1_spx_drop and vix_change <= p1_vix_react_max:
                phase1_events.append({
                    "start_idx": i,
                    "end_idx": j,
                    "end_ts": snaps_day[j]["ts"],
                    "spx_drop": spx_change,
                    "vix_react": vix_change,
                    "end_spot": snaps_day[j]["spot"],
                    "end_vix": snaps_day[j]["vix"],
                })

    if not phase1_events:
        return signals

    # Phase 2: After each Phase 1, look for VIX compression while SPX flat
    for p1 in phase1_events:
        if p1["end_idx"] in used_p1_indices:
            continue

        p2_start = p1["end_idx"]
        for j in range(p2_start + 1, n):
            mins = (snaps_day[j]["ts"] - snaps_day[p2_start]["ts"]).total_seconds() / 60
            if mins < p2_win_min:
                continue
            if mins > p2_win_max:
                break

            if direction == "long":
                vix_compress = snaps_day[p2_start]["vix"] - snaps_day[j]["vix"]  # positive = VIX dropping
                spx_range = abs(snaps_day[j]["spot"] - snaps_day[p2_start]["spot"])
            else:
                vix_compress = snaps_day[j]["vix"] - snaps_day[p2_start]["vix"]  # positive = VIX rising
                spx_range = abs(snaps_day[j]["spot"] - snaps_day[p2_start]["spot"])

            if vix_compress >= p2_vix_compress and spx_range <= p2_spx_flat:
                signals.append({
                    "ts": snaps_day[j]["ts"],
                    "spot": snaps_day[j]["spot"],
                    "vix": snaps_day[j]["vix"],
                    "p1_spx_drop": p1["spx_drop"],
                    "p1_vix_react": p1["vix_react"],
                    "p2_vix_compress": vix_compress,
                    "p2_spx_range": spx_range,
                    "direction": direction,
                })
                used_p1_indices.add(p1["end_idx"])
                return signals  # one signal per day per direction

    return signals


def run_backtest():
    print("Loading data from DB...")
    snaps, ohlc = load_data()

    snaps_by_date = group_by_date(snaps)
    ohlc_by_date = group_by_date(ohlc)

    dates = sorted(set(snaps_by_date.keys()) & set(ohlc_by_date.keys()))
    print(f"Trading days with both VIX+SPX and OHLC: {len(dates)}")
    print(f"Date range: {dates[0]} to {dates[-1]}")
    print()

    # Known bad dates (TS outage)
    BAD_DATES = {"2026-03-26"}

    for config in CONFIGS:
        name = config[0]
        print(f"{'='*70}")
        print(f"CONFIG: {name}")
        print(f"  Phase1: SPX drop >= {config[1]}, VIX react <= {config[2]}, window {config[3]}-{config[4]} min")
        print(f"  Phase2: VIX compress >= {config[5]}, SPX flat <= {config[6]}, window {config[7]}-{config[8]} min")
        print(f"{'='*70}")

        for direction in ["long", "short"]:
            all_signals = []

            for date in dates:
                if str(date) in BAD_DATES:
                    continue
                if date not in ohlc_by_date:
                    continue

                day_signals = detect_signals(snaps_by_date[date], config, direction)
                for sig in day_signals:
                    fwd = measure_forward(ohlc_by_date[date], sig["ts"], direction)
                    if fwd:
                        sig.update(fwd)
                        all_signals.append(sig)

            if not all_signals:
                print(f"\n  [{direction.upper()}] 0 signals")
                continue

            # Stats
            n_sig = len(all_signals)
            avg_mfe = sum(s["mfe"] for s in all_signals) / n_sig
            avg_mae = sum(s["mae"] for s in all_signals) / n_sig

            # Close-based WR at different horizons
            for horizon, key in [(15, "close_15"), (30, "close_30"), (45, "close_45"),
                                  (60, "close_60"), (90, "close_90")]:
                vals = [s[key] for s in all_signals if s[key] is not None]
                if vals:
                    wins = sum(1 for v in vals if v > 0)
                    total_pnl = sum(vals)
                    wr = wins / len(vals) * 100
                else:
                    wr = 0
                    total_pnl = 0
                    vals = []

                # Store for printing
                for s in all_signals:
                    pass  # already stored

            # Fixed RM results
            for rm_key, sl_label in [("sl8_t10", "SL=8/T=10"), ("sl12_t10", "SL=12/T=10"), ("sl20_t20", "SL=20/T=20")]:
                wins = sum(1 for s in all_signals if s[rm_key] == "WIN")
                losses = sum(1 for s in all_signals if s[rm_key] == "LOSS")
                expired = sum(1 for s in all_signals if s[rm_key] == "EXPIRED")
                if wins + losses > 0:
                    wr = wins / (wins + losses) * 100
                    pnl_map = {"sl8_t10": wins*10 - losses*8,
                               "sl12_t10": wins*10 - losses*12,
                               "sl20_t20": wins*20 - losses*20}
                    pnl = pnl_map[rm_key]
                else:
                    wr = 0
                    pnl = 0

            print(f"\n  [{direction.upper()}] {n_sig} signals across {len(set(s['ts'].date() for s in all_signals))} days")
            print(f"  Avg MFE: {avg_mfe:+.1f} pts | Avg MAE: {avg_mae:.1f} pts")
            print()

            # Close-based table
            print(f"  {'Hold':>6} {'N':>4} {'WR':>6} {'Avg P&L':>8} {'Total':>8} {'Winners':>8} {'Losers':>8}")
            for horizon, key in [(15, "close_15"), (30, "close_30"), (45, "close_45"),
                                  (60, "close_60"), (90, "close_90")]:
                vals = [s[key] for s in all_signals if s[key] is not None]
                if vals:
                    wins = sum(1 for v in vals if v > 0)
                    wr = wins / len(vals) * 100
                    avg_pnl = sum(vals) / len(vals)
                    total_pnl = sum(vals)
                    avg_win = sum(v for v in vals if v > 0) / max(1, wins)
                    avg_loss = sum(v for v in vals if v <= 0) / max(1, len(vals) - wins)
                    print(f"  {horizon:>4}m {len(vals):>4} {wr:>5.0f}% {avg_pnl:>+7.1f} {total_pnl:>+7.1f} {avg_win:>+7.1f} {avg_loss:>+7.1f}")

            # Fixed RM table
            print()
            print(f"  {'RM':>12} {'W':>4} {'L':>4} {'E':>4} {'WR':>6} {'PnL':>8}")
            for rm_key, sl_label, sl_val, t_val in [
                ("sl8_t10", "SL=8/T=10", 8, 10),
                ("sl12_t10", "SL=12/T=10", 12, 10),
                ("sl20_t20", "SL=20/T=20", 20, 20),
            ]:
                wins = sum(1 for s in all_signals if s[rm_key] == "WIN")
                losses = sum(1 for s in all_signals if s[rm_key] == "LOSS")
                expired = sum(1 for s in all_signals if s[rm_key] == "EXPIRED")
                wr = wins / max(1, wins + losses) * 100
                pnl = wins * t_val - losses * sl_val
                print(f"  {sl_label:>12} {wins:>4} {losses:>4} {expired:>4} {wr:>5.0f}% {pnl:>+7.1f}")

            # Per-signal detail
            print()
            print(f"  {'Date':>12} {'Time':>6} {'Spot':>8} {'VIX':>7} {'P1 SPX':>7} {'P1 VIX':>7} {'P2 VIX':>7} {'MFE':>6} {'MAE':>6} {'30m':>7} {'60m':>7}")
            for s in sorted(all_signals, key=lambda x: x["ts"]):
                dt = s["ts"]
                c30 = s["close_30"] if s["close_30"] is not None else 0
                c60 = s["close_60"] if s["close_60"] is not None else 0
                print(f"  {str(dt.date()):>12} {str(dt.time())[:5]:>6} {s['spot']:>8.1f} {s['vix']:>7.2f} "
                      f"{s['p1_spx_drop']:>+6.1f} {s['p1_vix_react']:>+6.2f} {s['p2_vix_compress']:>+6.2f} "
                      f"{s['mfe']:>+5.1f} {s['mae']:>5.1f} {c30:>+6.1f} {c60:>+6.1f}")

        print()


if __name__ == "__main__":
    run_backtest()
