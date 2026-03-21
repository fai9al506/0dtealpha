"""
Fixed Strike Vol Direction Analysis - Part 2
Deep dive into the REVERSED finding: Apollo's "aligned" trades UNDERPERFORM.
"""
import subprocess, json, sys, statistics
from datetime import timedelta, datetime
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

railway_path = r'C:\Users\Faisa\AppData\Roaming\npm\railway.cmd'
result = subprocess.run([railway_path, 'variables', '-s', '0dtealpha', '--json'], capture_output=True, text=True, shell=True)
db_url = json.loads(result.stdout)['DATABASE_URL']

import sqlalchemy as sa
engine = sa.create_engine(db_url)

def safe_float(v):
    if v is None or v == '' or v == 'None':
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None

def build_iv_map(rows):
    iv_map = {}
    if isinstance(rows, str):
        rows = json.loads(rows)
    for r in rows:
        strike = safe_float(r[10])
        c_iv = safe_float(r[2])
        p_iv = safe_float(r[18])
        if strike is not None and c_iv is not None and p_iv is not None and c_iv > 0 and p_iv > 0:
            iv_map[strike] = {"c_iv": c_iv, "p_iv": p_iv}
    return iv_map

print("=" * 80)
print("PART 2: DEEP DIVE INTO REVERSED IV FINDING")
print("=" * 80)
print()
print("KEY FINDING FROM PART 1:")
print("Apollo said: 'vol coming down at fixed strikes = support works'")
print("Expected: LONGS with falling put IV should WIN more")
print("ACTUAL: LONGS with RISING put IV win 61.7% (+430 PnL)")
print("        LONGS with FALLING put IV win only 50.4% (-115 PnL)")
print()
print("This needs investigation - WHY is the effect REVERSED?")

with engine.connect() as conn:
    # Rebuild the same data
    trades = conn.execute(sa.text("""
        SELECT id, ts, setup_name, direction, grade, outcome_result, outcome_pnl,
               spot, paradigm, greek_alignment, vix
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS')
        AND spot IS NOT NULL
        ORDER BY ts
    """)).fetchall()

    trade_dates = sorted(set(t[1].date() for t in trades))
    chain_by_date = defaultdict(list)
    for td in trade_dates:
        day_start = datetime(td.year, td.month, td.day)
        day_end = day_start + timedelta(days=1)
        snaps = conn.execute(sa.text("""
            SELECT ts, spot, rows FROM chain_snapshots
            WHERE ts >= :start AND ts < :end AND spot IS NOT NULL ORDER BY ts
        """), {"start": day_start, "end": day_end}).fetchall()
        for s in snaps:
            chain_by_date[td].append((s[0], s[1], s[2]))

    import bisect
    results = []

    for trade in trades:
        trade_id, trade_ts, setup_name, direction, grade, outcome, pnl, spot, paradigm, alignment, vix = trade
        td = trade_ts.date()
        day_snaps = chain_by_date.get(td, [])
        if not day_snaps:
            continue
        snap_times = [s[0] for s in day_snaps]

        idx = bisect.bisect_left(snap_times, trade_ts)
        best_idx = None
        best_diff = 999999
        for ci in [idx - 1, idx]:
            if 0 <= ci < len(snap_times):
                diff = abs((snap_times[ci] - trade_ts).total_seconds())
                if diff < best_diff:
                    best_diff = diff
                    best_idx = ci
        if best_idx is None or best_diff > 180:
            continue

        snap_now = day_snaps[best_idx]
        target_before = trade_ts - timedelta(minutes=15)
        idx_b = bisect.bisect_left(snap_times, target_before)
        best_idx_b = None
        best_diff_b = 999999
        for ci in [idx_b - 1, idx_b]:
            if 0 <= ci < len(snap_times):
                diff = abs((snap_times[ci] - target_before).total_seconds())
                if diff < best_diff_b:
                    best_diff_b = diff
                    best_idx_b = ci
        if best_idx_b is None or best_diff_b > 180:
            continue

        snap_before = day_snaps[best_idx_b]
        iv_now = build_iv_map(snap_now[2])
        iv_before = build_iv_map(snap_before[2])
        atm_strike = round(spot / 5) * 5

        def get_iv_change(strike, side):
            key = f"{side}_iv"
            if strike in iv_now and strike in iv_before:
                now_val = iv_now[strike][key]
                before_val = iv_before[strike][key]
                if now_val > 0 and before_val > 0:
                    return now_val - before_val
            return None

        c_iv_atm = get_iv_change(atm_strike, "c")
        p_iv_atm = get_iv_change(atm_strike, "p")
        p_near = [get_iv_change(atm_strike - d, "p") for d in [5, 10]]
        p_near = [x for x in p_near if x is not None]
        c_near = [get_iv_change(atm_strike + d, "c") for d in [5, 10]]
        c_near = [x for x in c_near if x is not None]

        if c_iv_atm is None and p_iv_atm is None:
            continue

        # Also compute absolute IV level at signal time
        atm_c_iv_now = iv_now.get(atm_strike, {}).get("c_iv")
        atm_p_iv_now = iv_now.get(atm_strike, {}).get("p_iv")
        atm_iv_level = None
        if atm_c_iv_now and atm_p_iv_now:
            atm_iv_level = (atm_c_iv_now + atm_p_iv_now) / 2

        # Spot change over 15 min
        spot_before = snap_before[1]
        spot_now = snap_now[1]
        spot_change = spot_now - spot_before if spot_before else None

        near_put_iv_change = statistics.mean(p_near) if p_near else None
        near_call_iv_change = statistics.mean(c_near) if c_near else None
        atm_iv_change = None
        if c_iv_atm is not None and p_iv_atm is not None:
            atm_iv_change = (c_iv_atm + p_iv_atm) / 2
        elif c_iv_atm is not None:
            atm_iv_change = c_iv_atm
        else:
            atm_iv_change = p_iv_atm

        results.append({
            "id": trade_id, "ts": trade_ts, "setup": setup_name,
            "direction": direction, "grade": grade, "outcome": outcome,
            "pnl": pnl, "spot": spot, "paradigm": paradigm,
            "alignment": alignment, "vix": vix,
            "atm_iv_change": atm_iv_change,
            "near_put_iv_change": near_put_iv_change,
            "near_call_iv_change": near_call_iv_change,
            "atm_iv_level": atm_iv_level,
            "spot_change_15m": spot_change,
        })

    # ==============================
    # HYPOTHESIS 1: Is IV change just a proxy for momentum?
    # If spot moved up in last 15 min AND IV fell, that's just normal vol-spot
    # correlation. The "IV falling = support" might just be "spot already moving up"
    # ==============================
    print("\n" + "=" * 80)
    print("HYPOTHESIS A: IV change is proxy for momentum (spot-vol correlation)")
    print("If spot rallied 15min, IV naturally drops. That's just momentum.")
    print("=" * 80)

    long_trades = [r for r in results if r["direction"] in ("long", "bullish")
                   and r["near_put_iv_change"] is not None and r["spot_change_15m"] is not None]

    print(f"\n  LONG trades with data: {len(long_trades)}")

    # Quadrant analysis: spot direction vs IV direction
    q1 = [r for r in long_trades if r["spot_change_15m"] > 0 and r["near_put_iv_change"] < 0]  # spot up, IV down (normal)
    q2 = [r for r in long_trades if r["spot_change_15m"] > 0 and r["near_put_iv_change"] > 0]  # spot up, IV up (unusual)
    q3 = [r for r in long_trades if r["spot_change_15m"] < 0 and r["near_put_iv_change"] < 0]  # spot down, IV down (unusual)
    q4 = [r for r in long_trades if r["spot_change_15m"] < 0 and r["near_put_iv_change"] > 0]  # spot down, IV up (normal)

    def quad_stats(label, grp):
        if not grp:
            return
        wins = sum(1 for r in grp if r["outcome"] == "WIN")
        total = len(grp)
        wr = wins / total * 100
        pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
        avg_spot = statistics.mean([r["spot_change_15m"] for r in grp])
        avg_iv = statistics.mean([r["near_put_iv_change"] for r in grp])
        print(f"  {label}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={pnl:+.1f}, AvgSpotChg={avg_spot:+.1f}, AvgIVchg={avg_iv:+.5f}")

    print("\n  LONG trade quadrants:")
    quad_stats("Q1: Spot UP   + IV DOWN (normal correlation)  ", q1)
    quad_stats("Q2: Spot UP   + IV UP   (vol expanding w/rally)", q2)
    quad_stats("Q3: Spot DOWN + IV DOWN (vol compressing)      ", q3)
    quad_stats("Q4: Spot DOWN + IV UP   (normal correlation)  ", q4)

    # Same for shorts
    short_trades = [r for r in results if r["direction"] in ("short", "bearish")
                    and r["near_put_iv_change"] is not None and r["spot_change_15m"] is not None]

    print(f"\n  SHORT trades with data: {len(short_trades)}")

    q1s = [r for r in short_trades if r["spot_change_15m"] < 0 and r["near_put_iv_change"] > 0]  # spot down, IV up (normal)
    q2s = [r for r in short_trades if r["spot_change_15m"] < 0 and r["near_put_iv_change"] < 0]  # spot down, IV down (unusual)
    q3s = [r for r in short_trades if r["spot_change_15m"] > 0 and r["near_put_iv_change"] > 0]  # spot up, IV up (unusual)
    q4s = [r for r in short_trades if r["spot_change_15m"] > 0 and r["near_put_iv_change"] < 0]  # spot up, IV down (normal)

    print("\n  SHORT trade quadrants:")
    quad_stats("Q1: Spot DOWN + IV UP   (normal correlation)  ", q1s)
    quad_stats("Q2: Spot DOWN + IV DOWN (vol compressing)      ", q2s)
    quad_stats("Q3: Spot UP   + IV UP   (vol expanding)       ", q3s)
    quad_stats("Q4: Spot UP   + IV DOWN (normal correlation)  ", q4s)

    # ==============================
    # HYPOTHESIS B: The REAL insight is momentum (spot direction), not IV
    # ==============================
    print("\n" + "=" * 80)
    print("HYPOTHESIS B: Is 15-min spot momentum the real signal?")
    print("=" * 80)

    for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
        dt = [r for r in results if r["direction"] in dir_values and r["spot_change_15m"] is not None]
        if not dt:
            continue
        print(f"\n  --- {dir_label} trades ({len(dt)}) ---")

        if dir_label == "LONG":
            # Momentum aligned = spot already moving up
            aligned = [r for r in dt if r["spot_change_15m"] > 0]
            against = [r for r in dt if r["spot_change_15m"] < 0]
        else:
            aligned = [r for r in dt if r["spot_change_15m"] < 0]
            against = [r for r in dt if r["spot_change_15m"] > 0]

        for lbl, grp in [("MOMENTUM ALIGNED  ", aligned), ("MOMENTUM AGAINST  ", against)]:
            if not grp:
                continue
            wins = sum(1 for r in grp if r["outcome"] == "WIN")
            total = len(grp)
            wr = wins / total * 100
            pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
            avg = pnl / total
            print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={pnl:+.1f}, Avg={avg:+.2f}")

    # ==============================
    # HYPOTHESIS C: Re-do Apollo's test controlling for momentum
    # Only look at IV change when spot was FLAT (< 3 pts move in 15 min)
    # ==============================
    print("\n" + "=" * 80)
    print("HYPOTHESIS C: IV direction AFTER controlling for momentum")
    print("Only trades where spot moved < 3 pts in 15 min (removing momentum)")
    print("=" * 80)

    for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
        dt = [r for r in results if r["direction"] in dir_values
              and r["near_put_iv_change"] is not None
              and r["spot_change_15m"] is not None
              and abs(r["spot_change_15m"]) < 3]
        if len(dt) < 10:
            print(f"\n  {dir_label}: only {len(dt)} trades with flat spot - skipping")
            continue
        print(f"\n  --- {dir_label} (flat spot, {len(dt)} trades) ---")

        if dir_label == "LONG":
            iv_ok = [r for r in dt if r["near_put_iv_change"] < 0]
            iv_bad = [r for r in dt if r["near_put_iv_change"] > 0]
        else:
            iv_ok = [r for r in dt if r["near_put_iv_change"] > 0]
            iv_bad = [r for r in dt if r["near_put_iv_change"] < 0]

        for lbl, grp in [("IV ALIGNED  ", iv_ok), ("IV MISALIGN ", iv_bad)]:
            if not grp:
                continue
            wins = sum(1 for r in grp if r["outcome"] == "WIN")
            total = len(grp)
            wr = wins / total * 100
            pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
            print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={pnl:+.1f}")

    # ==============================
    # HYPOTHESIS D: VIX level interaction
    # Maybe IV direction matters differently at high vs low VIX?
    # ==============================
    print("\n" + "=" * 80)
    print("HYPOTHESIS D: VIX level interaction with IV direction")
    print("=" * 80)

    for vix_label, vix_cond in [("Low VIX (<18)", lambda v: v < 18),
                                 ("Med VIX (18-25)", lambda v: 18 <= v < 25),
                                 ("High VIX (>=25)", lambda v: v >= 25)]:
        vt = [r for r in results if r["vix"] is not None and vix_cond(r["vix"]) and r["atm_iv_change"] is not None]
        if len(vt) < 10:
            continue
        falling = [r for r in vt if r["atm_iv_change"] < 0]
        rising = [r for r in vt if r["atm_iv_change"] > 0]
        print(f"\n  --- {vix_label} ({len(vt)} trades) ---")
        for lbl, grp in [("IV FALLING", falling), ("IV RISING ", rising)]:
            if not grp:
                continue
            wins = sum(1 for r in grp if r["outcome"] == "WIN")
            total = len(grp)
            wr = wins / total * 100
            pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
            print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={pnl:+.1f}")

    # ==============================
    # HYPOTHESIS E: Maybe Apollo's insight applies to CALL IV for shorts
    # "When fixed strike vols come down, that is where support plays out"
    # For shorts going AGAINST support: you want vol RISING (support failing)
    # Maybe check: short trades where CALL IV is rising (overhead vol pressure)
    # ==============================
    print("\n" + "=" * 80)
    print("HYPOTHESIS E: Call IV direction for SHORTS")
    print("Short winners: call IV should be rising (vol selling pressure overhead)?")
    print("=" * 80)

    short_with_call = [r for r in results if r["direction"] in ("short", "bearish") and r["near_call_iv_change"] is not None]
    print(f"\n  SHORT trades with call IV data: {len(short_with_call)}")

    call_falling = [r for r in short_with_call if r["near_call_iv_change"] < 0]
    call_rising = [r for r in short_with_call if r["near_call_iv_change"] > 0]

    for lbl, grp in [("Call IV FALLING (vol subsiding overhead) ", call_falling),
                     ("Call IV RISING  (vol pressure overhead)  ", call_rising)]:
        if not grp:
            continue
        wins = sum(1 for r in grp if r["outcome"] == "WIN")
        total = len(grp)
        wr = wins / total * 100
        pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
        print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={pnl:+.1f}")

    # ==============================
    # HYPOTHESIS F: Maybe the right metric is IV change RATE relative to spot
    # Normalized: IV change per point of spot move
    # ==============================
    print("\n" + "=" * 80)
    print("HYPOTHESIS F: Correlation coefficient (spot vs IV) as quality signal")
    print("=" * 80)

    # For each trade, compute sign agreement of spot_change and atm_iv_change
    # If spot up and IV down (negative corr) = NORMAL
    # If spot up and IV up (positive corr) = ABNORMAL (vol buyers active despite rally)
    # LONGS: abnormal positive corr (IV rising with spot up) = strong conviction rally?
    both = [r for r in results if r["spot_change_15m"] is not None and r["atm_iv_change"] is not None and r["spot_change_15m"] != 0]

    normal_corr = [r for r in both if (r["spot_change_15m"] > 0 and r["atm_iv_change"] < 0) or
                                       (r["spot_change_15m"] < 0 and r["atm_iv_change"] > 0)]
    abnormal_corr = [r for r in both if (r["spot_change_15m"] > 0 and r["atm_iv_change"] > 0) or
                                         (r["spot_change_15m"] < 0 and r["atm_iv_change"] < 0)]

    for lbl, grp in [("NORMAL  (spot-vol inverse) ", normal_corr),
                     ("ABNORMAL (spot-vol same dir)", abnormal_corr)]:
        if not grp:
            continue
        wins = sum(1 for r in grp if r["outcome"] == "WIN")
        total = len(grp)
        wr = wins / total * 100
        pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
        print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={pnl:+.1f}")

    # Split by direction
    for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
        dt = [r for r in both if r["direction"] in dir_values]
        normal = [r for r in dt if (r["spot_change_15m"] > 0 and r["atm_iv_change"] < 0) or
                                    (r["spot_change_15m"] < 0 and r["atm_iv_change"] > 0)]
        abnormal = [r for r in dt if (r["spot_change_15m"] > 0 and r["atm_iv_change"] > 0) or
                                      (r["spot_change_15m"] < 0 and r["atm_iv_change"] < 0)]
        print(f"\n  {dir_label}:")
        for lbl, grp in [("  NORMAL  ", normal), ("  ABNORMAL", abnormal)]:
            if not grp:
                continue
            wins = sum(1 for r in grp if r["outcome"] == "WIN")
            total = len(grp)
            wr = wins / total * 100
            pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
            print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={pnl:+.1f}")

    # ==============================
    # ANALYSIS: 5-min lookback instead of 15
    # ==============================
    print("\n" + "=" * 80)
    print("BONUS: Shorter lookback (5-min instead of 15-min)")
    print("=" * 80)

    results_5m = []
    for trade in trades:
        trade_id, trade_ts, setup_name, direction, grade, outcome, pnl, spot, paradigm, alignment, vix = trade
        td = trade_ts.date()
        day_snaps = chain_by_date.get(td, [])
        if not day_snaps:
            continue
        snap_times = [s[0] for s in day_snaps]

        idx = bisect.bisect_left(snap_times, trade_ts)
        best_idx = None
        best_diff = 999999
        for ci in [idx - 1, idx]:
            if 0 <= ci < len(snap_times):
                diff = abs((snap_times[ci] - trade_ts).total_seconds())
                if diff < best_diff:
                    best_diff = diff
                    best_idx = ci
        if best_idx is None or best_diff > 180:
            continue

        # 5-min lookback (3-7 min window)
        target_before = trade_ts - timedelta(minutes=5)
        idx_b = bisect.bisect_left(snap_times, target_before)
        best_idx_b = None
        best_diff_b = 999999
        for ci in [idx_b - 1, idx_b]:
            if 0 <= ci < len(snap_times):
                diff = abs((snap_times[ci] - target_before).total_seconds())
                if diff < best_diff_b:
                    best_diff_b = diff
                    best_idx_b = ci
        if best_idx_b is None or best_diff_b > 180:
            continue

        snap_now = day_snaps[best_idx]
        snap_before = day_snaps[best_idx_b]
        iv_now = build_iv_map(snap_now[2])
        iv_before = build_iv_map(snap_before[2])
        atm_strike = round(spot / 5) * 5

        def get_iv_change(strike, side):
            key = f"{side}_iv"
            if strike in iv_now and strike in iv_before:
                now_val = iv_now[strike][key]
                before_val = iv_before[strike][key]
                if now_val > 0 and before_val > 0:
                    return now_val - before_val
            return None

        p_near = [get_iv_change(atm_strike - d, "p") for d in [5, 10]]
        p_near = [x for x in p_near if x is not None]
        if not p_near:
            continue

        near_put_iv_change = statistics.mean(p_near)
        results_5m.append({
            "direction": direction, "outcome": outcome, "pnl": pnl,
            "near_put_iv_change": near_put_iv_change,
        })

    print(f"  Trades with 5-min IV data: {len(results_5m)}")
    for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
        dt = [r for r in results_5m if r["direction"] in dir_values]
        if not dt:
            continue
        print(f"\n  --- {dir_label} ({len(dt)}) ---")
        if dir_label == "LONG":
            aligned = [r for r in dt if r["near_put_iv_change"] < 0]
            misaligned = [r for r in dt if r["near_put_iv_change"] > 0]
        else:
            aligned = [r for r in dt if r["near_put_iv_change"] > 0]
            misaligned = [r for r in dt if r["near_put_iv_change"] < 0]
        for lbl, grp in [("ALIGNED  ", aligned), ("MISALIGN ", misaligned)]:
            if not grp:
                continue
            wins = sum(1 for r in grp if r["outcome"] == "WIN")
            total = len(grp)
            wr = wins / total * 100
            pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
            print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={pnl:+.1f}")

    print("\n" + "=" * 80)
    print("DEEP DIVE COMPLETE")
    print("=" * 80)
