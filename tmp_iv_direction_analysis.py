"""
Fixed Strike Vol Direction Analysis
Apollo's insight: Vanna support at a strike only WORKS if IV at that strike is DECLINING.
Optimized: batch-fetch chain snapshots per date instead of per-trade.
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

# Column indices: [2]=Call IV, [10]=Strike, [18]=Put IV

print("=" * 80)
print("FIXED STRIKE VOL DIRECTION ANALYSIS")
print("Apollo: 'vol coming down at fixed strikes = support works'")
print("=" * 80)

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

with engine.connect() as conn:
    # 1. Get all WIN/LOSS trades
    trades = conn.execute(sa.text("""
        SELECT id, ts, setup_name, direction, grade, outcome_result, outcome_pnl,
               spot, paradigm, greek_alignment, vix
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS')
        AND spot IS NOT NULL
        ORDER BY ts
    """)).fetchall()
    print(f"\nTotal WIN/LOSS trades: {len(trades)}")

    # 2. Get unique trade dates
    trade_dates = sorted(set(t[1].date() for t in trades))
    print(f"Unique trade dates: {len(trade_dates)}")

    # 3. Batch-fetch ALL chain snapshots for all trade dates
    print("Loading chain snapshots...")
    # Store as {date: [(ts, spot, rows), ...]} sorted by ts
    chain_by_date = defaultdict(list)

    for td in trade_dates:
        day_start = datetime(td.year, td.month, td.day)
        day_end = day_start + timedelta(days=1)
        snaps = conn.execute(sa.text("""
            SELECT ts, spot, rows FROM chain_snapshots
            WHERE ts >= :start AND ts < :end
            AND spot IS NOT NULL
            ORDER BY ts
        """), {"start": day_start, "end": day_end}).fetchall()
        for s in snaps:
            chain_by_date[td].append((s[0], s[1], s[2]))

    total_snaps = sum(len(v) for v in chain_by_date.values())
    print(f"Loaded {total_snaps} chain snapshots across {len(chain_by_date)} dates")

    # 4. For each trade, find nearest snapshot and one ~15 min earlier
    import bisect

    results = []
    missing = 0

    for trade in trades:
        trade_id, trade_ts, setup_name, direction, grade, outcome, pnl, spot, paradigm, alignment, vix = trade
        td = trade_ts.date()
        day_snaps = chain_by_date.get(td, [])
        if not day_snaps:
            missing += 1
            continue

        # Find nearest snapshot (binary search by ts)
        snap_times = [s[0] for s in day_snaps]

        # Nearest to trade_ts
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
            missing += 1
            continue

        snap_now = day_snaps[best_idx]

        # Find snapshot ~15 min earlier
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
            missing += 1
            continue

        snap_before = day_snaps[best_idx_b]

        # Parse IV maps
        iv_now = build_iv_map(snap_now[2])
        iv_before = build_iv_map(snap_before[2])

        # ATM strike
        atm_strike = round(spot / 5) * 5

        def get_iv_change(strike, side):
            key = f"{side}_iv"
            if strike in iv_now and strike in iv_before:
                now_val = iv_now[strike][key]
                before_val = iv_before[strike][key]
                if now_val > 0 and before_val > 0:
                    return now_val - before_val
            return None

        c_iv_change_atm = get_iv_change(atm_strike, "c")
        p_iv_change_atm = get_iv_change(atm_strike, "p")

        # Near puts: 5-10 below
        p_iv_changes_near = [get_iv_change(atm_strike - d, "p") for d in [5, 10]]
        p_iv_changes_near = [x for x in p_iv_changes_near if x is not None]

        # Near calls: 5-10 above
        c_iv_changes_near = [get_iv_change(atm_strike + d, "c") for d in [5, 10]]
        c_iv_changes_near = [x for x in c_iv_changes_near if x is not None]

        if c_iv_change_atm is None and p_iv_change_atm is None:
            missing += 1
            continue

        atm_iv_change = None
        if c_iv_change_atm is not None and p_iv_change_atm is not None:
            atm_iv_change = (c_iv_change_atm + p_iv_change_atm) / 2
        elif c_iv_change_atm is not None:
            atm_iv_change = c_iv_change_atm
        else:
            atm_iv_change = p_iv_change_atm

        near_put_iv_change = statistics.mean(p_iv_changes_near) if p_iv_changes_near else None
        near_call_iv_change = statistics.mean(c_iv_changes_near) if c_iv_changes_near else None

        results.append({
            "id": trade_id,
            "ts": trade_ts,
            "setup": setup_name,
            "direction": direction,
            "grade": grade,
            "outcome": outcome,
            "pnl": pnl,
            "spot": spot,
            "paradigm": paradigm,
            "alignment": alignment,
            "vix": vix,
            "atm_iv_change": atm_iv_change,
            "near_put_iv_change": near_put_iv_change,
            "near_call_iv_change": near_call_iv_change,
            "c_iv_change_atm": c_iv_change_atm,
            "p_iv_change_atm": p_iv_change_atm,
        })

    print(f"Trades with IV data: {len(results)}")
    print(f"Trades missing chain data: {missing}")

    # ========== HELPER ==========
    def print_stats(label, subset, iv_key="atm_iv_change"):
        if not subset:
            print(f"  {label}: no trades")
            return
        wins = sum(1 for r in subset if r["outcome"] == "WIN")
        losses = sum(1 for r in subset if r["outcome"] == "LOSS")
        total = len(subset)
        wr = wins / total * 100
        total_pnl = sum(r["pnl"] for r in subset if r["pnl"] is not None)
        avg_pnl = total_pnl / total
        iv_vals = [r[iv_key] for r in subset if r[iv_key] is not None]
        avg_iv = statistics.mean(iv_vals) if iv_vals else 0
        print(f"  {label}: {total:3d}t, {wins:3d}W/{losses:3d}L, WR={wr:.1f}%, PnL={total_pnl:+.1f}, Avg={avg_pnl:+.2f}/trade, AvgIVchg={avg_iv:+.5f}")

    # ==============================
    # ANALYSIS 1: Overall ATM IV Change vs Outcome
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 1: ATM IV Change (15-min lookback) vs Outcome")
    print("=" * 80)

    iv_falling = [r for r in results if r["atm_iv_change"] is not None and r["atm_iv_change"] < 0]
    iv_rising = [r for r in results if r["atm_iv_change"] is not None and r["atm_iv_change"] > 0]
    iv_flat = [r for r in results if r["atm_iv_change"] is not None and r["atm_iv_change"] == 0]

    print_stats("IV FALLING (ATM)", iv_falling)
    print_stats("IV RISING  (ATM)", iv_rising)
    print_stats("IV FLAT    (ATM)", iv_flat)

    # ==============================
    # ANALYSIS 2: Split by Trade Direction
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 2: IV Direction split by Trade Direction")
    print("=" * 80)

    for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
        dir_trades = [r for r in results if r["direction"] in dir_values and r["atm_iv_change"] is not None]
        if not dir_trades:
            continue
        print(f"\n  --- {dir_label} trades ({len(dir_trades)}) ---")
        falling = [r for r in dir_trades if r["atm_iv_change"] < 0]
        rising = [r for r in dir_trades if r["atm_iv_change"] > 0]
        print_stats(f"  IV FALLING", falling)
        print_stats(f"  IV RISING ", rising)

    # ==============================
    # ANALYSIS 3: Near-Strike IV
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 3: Near-Strike IV Changes by Direction")
    print("=" * 80)

    for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
        dir_trades = [r for r in results if r["direction"] in dir_values]
        if not dir_trades:
            continue
        print(f"\n  --- {dir_label} trades ---")
        for iv_key, iv_label in [("near_put_iv_change", "Near Put IV (5-10 below)"), ("near_call_iv_change", "Near Call IV (5-10 above)")]:
            has_data = [r for r in dir_trades if r[iv_key] is not None]
            if not has_data:
                continue
            falling = [r for r in has_data if r[iv_key] < 0]
            rising = [r for r in has_data if r[iv_key] > 0]
            print(f"\n  {iv_label}:")
            print_stats(f"    FALLING", falling, iv_key)
            print_stats(f"    RISING ", rising, iv_key)

    # ==============================
    # ANALYSIS 4: By Setup Name
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 4: ATM IV Change by Setup Name")
    print("=" * 80)

    setup_names = sorted(set(r["setup"] for r in results))
    for setup in setup_names:
        setup_trades = [r for r in results if r["setup"] == setup and r["atm_iv_change"] is not None]
        if len(setup_trades) < 5:
            continue
        falling = [r for r in setup_trades if r["atm_iv_change"] < 0]
        rising = [r for r in setup_trades if r["atm_iv_change"] > 0]
        print(f"\n  --- {setup} ({len(setup_trades)} trades) ---")
        print_stats(f"  IV FALLING", falling)
        print_stats(f"  IV RISING ", rising)

    # ==============================
    # ANALYSIS 5: Magnitude Buckets
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 5: IV Change Magnitude Buckets (ATM, 15-min)")
    print("=" * 80)

    buckets = [
        ("Big drop    (< -0.10)", lambda x: x < -0.10),
        ("Med drop    (-0.10 to -0.03)", lambda x: -0.10 <= x < -0.03),
        ("Small drop  (-0.03 to 0)", lambda x: -0.03 <= x < 0),
        ("Flat        (= 0)", lambda x: x == 0),
        ("Small rise  (0 to +0.03)", lambda x: 0 < x <= 0.03),
        ("Med rise    (+0.03 to +0.10)", lambda x: 0.03 < x <= 0.10),
        ("Big rise    (> +0.10)", lambda x: x > 0.10),
    ]

    print(f"  {'Bucket':<32s} {'Trades':>6s} {'Wins':>5s} {'WR':>6s} {'PnL':>8s} {'Avg':>7s}")
    print("  " + "-" * 70)
    for label, cond in buckets:
        subset = [r for r in results if r["atm_iv_change"] is not None and cond(r["atm_iv_change"])]
        if not subset:
            print(f"  {label:<32s} {'0':>6s}")
            continue
        wins = sum(1 for r in subset if r["outcome"] == "WIN")
        total = len(subset)
        wr = wins / total * 100
        total_pnl = sum(r["pnl"] for r in subset if r["pnl"] is not None)
        avg_pnl = total_pnl / total
        print(f"  {label:<32s} {total:>6d} {wins:>5d} {wr:>5.1f}% {total_pnl:>+8.1f} {avg_pnl:>+7.2f}")

    # ==============================
    # ANALYSIS 6: Apollo's Hypothesis — Direction-Adjusted
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 6: Apollo's Hypothesis Test")
    print("LONG: put IV falling = support holds = ALIGNED")
    print("SHORT: put IV rising = bearish vol pressure = ALIGNED")
    print("=" * 80)

    for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
        dir_trades = [r for r in results if r["direction"] in dir_values and r["near_put_iv_change"] is not None]
        if not dir_trades:
            continue
        print(f"\n  --- {dir_label} trades ({len(dir_trades)} with near-put IV) ---")
        if dir_label == "LONG":
            aligned = [r for r in dir_trades if r["near_put_iv_change"] < 0]
            misaligned = [r for r in dir_trades if r["near_put_iv_change"] > 0]
        else:
            aligned = [r for r in dir_trades if r["near_put_iv_change"] > 0]
            misaligned = [r for r in dir_trades if r["near_put_iv_change"] < 0]
        print_stats(f"  ALIGNED   ", aligned, "near_put_iv_change")
        print_stats(f"  MISALIGNED", misaligned, "near_put_iv_change")

    # ==============================
    # ANALYSIS 7: Winner vs Loser IV Distributions
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 7: Average IV Changes -- Winners vs Losers")
    print("=" * 80)

    winners = [r for r in results if r["outcome"] == "WIN"]
    losers = [r for r in results if r["outcome"] == "LOSS"]

    print(f"\n  {'Metric':<28s} {'Win_Avg':>10s} {'Win_Med':>10s} | {'Loss_Avg':>10s} {'Loss_Med':>10s} | {'Diff':>10s}")
    print("  " + "-" * 85)

    for metric in ["atm_iv_change", "near_put_iv_change", "near_call_iv_change", "c_iv_change_atm", "p_iv_change_atm"]:
        w_vals = [r[metric] for r in winners if r[metric] is not None]
        l_vals = [r[metric] for r in losers if r[metric] is not None]
        if w_vals and l_vals:
            w_avg = statistics.mean(w_vals)
            l_avg = statistics.mean(l_vals)
            w_med = statistics.median(w_vals)
            l_med = statistics.median(l_vals)
            diff = w_avg - l_avg
            print(f"  {metric:<28s} {w_avg:>+10.5f} {w_med:>+10.5f} | {l_avg:>+10.5f} {l_med:>+10.5f} | {diff:>+10.5f}")

    print("\n  Split by direction:")
    for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
        dir_w = [r for r in winners if r["direction"] in dir_values]
        dir_l = [r for r in losers if r["direction"] in dir_values]
        if not dir_w or not dir_l:
            continue
        print(f"\n    --- {dir_label} ---")
        for metric in ["atm_iv_change", "near_put_iv_change", "near_call_iv_change"]:
            w_vals = [r[metric] for r in dir_w if r[metric] is not None]
            l_vals = [r[metric] for r in dir_l if r[metric] is not None]
            if w_vals and l_vals:
                w_avg = statistics.mean(w_vals)
                l_avg = statistics.mean(l_vals)
                diff = w_avg - l_avg
                print(f"    {metric:<28s}: W={w_avg:+.5f} | L={l_avg:+.5f} | diff={diff:+.5f}")

    # ==============================
    # ANALYSIS 8: Potential Filter Impact
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 8: Potential Filter -- Block trades where IV moves AGAINST direction")
    print("LONG blocked if near-put IV rising (support failing)")
    print("SHORT blocked if near-put IV falling (support holding, bad to short)")
    print("=" * 80)

    passed = []
    blocked = []
    no_data = []

    for r in results:
        put_iv = r["near_put_iv_change"]
        if put_iv is None:
            no_data.append(r)
            continue
        d = r["direction"]
        if d in ("long", "bullish"):
            if put_iv > 0:
                blocked.append(r)
            else:
                passed.append(r)
        else:
            if put_iv < 0:
                blocked.append(r)
            else:
                passed.append(r)

    print(f"\n  Trades with IV data: {len(passed) + len(blocked)}")
    print(f"  Trades without IV data: {len(no_data)}")

    def group_stats(label, group):
        if not group:
            print(f"\n  {label}: no trades")
            return
        wins = sum(1 for r in group if r["outcome"] == "WIN")
        losses = sum(1 for r in group if r["outcome"] == "LOSS")
        total = len(group)
        wr = wins / total * 100
        total_pnl = sum(r["pnl"] for r in group if r["pnl"] is not None)
        avg_pnl = total_pnl / total
        print(f"\n  {label}: {total} trades, {wins}W/{losses}L")
        print(f"    WR={wr:.1f}%, Total PnL={total_pnl:+.1f}, Avg={avg_pnl:+.2f}/trade")
        setups = sorted(set(r["setup"] for r in group))
        for s in setups:
            sg = [r for r in group if r["setup"] == s]
            sw = sum(1 for r in sg if r["outcome"] == "WIN")
            sp = sum(r["pnl"] for r in sg if r["pnl"] is not None)
            swr = sw / len(sg) * 100
            print(f"    {s:<20s}: {len(sg):3d}t, {sw:3d}W, WR={swr:.0f}%, PnL={sp:+.1f}")

    group_stats("PASSED (IV aligned)", passed)
    group_stats("BLOCKED (IV against)", blocked)

    # ==============================
    # ANALYSIS 9: Skew Change (Call IV - Put IV direction)
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 9: IV Skew Change (Call IV chg minus Put IV chg)")
    print("Negative skew shift = puts getting relatively pricier (bearish)")
    print("Positive skew shift = calls getting relatively pricier (bullish)")
    print("=" * 80)

    skew_trades = []
    for r in results:
        if r["c_iv_change_atm"] is not None and r["p_iv_change_atm"] is not None:
            r2 = dict(r)
            r2["skew_change"] = r["c_iv_change_atm"] - r["p_iv_change_atm"]
            skew_trades.append(r2)

    for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
        dir_trades = [r for r in skew_trades if r["direction"] in dir_values]
        if not dir_trades:
            continue
        print(f"\n  --- {dir_label} trades ({len(dir_trades)}) ---")
        if dir_label == "LONG":
            aligned = [r for r in dir_trades if r["skew_change"] > 0]
            misaligned = [r for r in dir_trades if r["skew_change"] < 0]
        else:
            aligned = [r for r in dir_trades if r["skew_change"] < 0]
            misaligned = [r for r in dir_trades if r["skew_change"] > 0]
        for lbl, grp in [("ALIGNED  ", aligned), ("MISALIGN ", misaligned)]:
            if not grp:
                continue
            wins = sum(1 for r in grp if r["outcome"] == "WIN")
            total = len(grp)
            wr = wins / total * 100
            total_pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
            avg_skew = statistics.mean([r["skew_change"] for r in grp])
            print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={total_pnl:+.1f}, AvgSkewChg={avg_skew:+.5f}")

    # ==============================
    # ANALYSIS 10: Per-setup Apollo test (direction-aware near-put IV)
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 10: Apollo Test by Setup (near-put IV aligned vs misaligned)")
    print("=" * 80)

    for setup in setup_names:
        st = [r for r in results if r["setup"] == setup and r["near_put_iv_change"] is not None]
        if len(st) < 10:
            continue
        print(f"\n  --- {setup} ({len(st)} trades) ---")
        for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
            dt = [r for r in st if r["direction"] in dir_values]
            if len(dt) < 5:
                continue
            if dir_label == "LONG":
                aligned = [r for r in dt if r["near_put_iv_change"] < 0]
                misaligned = [r for r in dt if r["near_put_iv_change"] > 0]
            else:
                aligned = [r for r in dt if r["near_put_iv_change"] > 0]
                misaligned = [r for r in dt if r["near_put_iv_change"] < 0]
            for lbl, grp in [(f"  {dir_label} ALIGNED  ", aligned), (f"  {dir_label} MISALIGN ", misaligned)]:
                if not grp:
                    continue
                wins = sum(1 for r in grp if r["outcome"] == "WIN")
                total = len(grp)
                wr = wins / total * 100
                total_pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
                print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={total_pnl:+.1f}")

    # ==============================
    # ANALYSIS 11: Combine with existing alignment filter
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 11: IV Filter ON TOP of existing Greek alignment")
    print("Only trades with alignment >= 0 (V10 filter vicinity)")
    print("=" * 80)

    aligned_trades = [r for r in results if r["alignment"] is not None and r["alignment"] >= 0 and r["near_put_iv_change"] is not None]
    if aligned_trades:
        for dir_label, dir_values in [("LONG", ("long", "bullish")), ("SHORT", ("short", "bearish"))]:
            dt = [r for r in aligned_trades if r["direction"] in dir_values]
            if not dt:
                continue
            print(f"\n  --- {dir_label} (align >= 0, {len(dt)} trades) ---")
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
                total_pnl = sum(r["pnl"] for r in grp if r["pnl"] is not None)
                avg_pnl = total_pnl / total
                print(f"  {lbl}: {total:3d}t, {wins:3d}W, WR={wr:.1f}%, PnL={total_pnl:+.1f}, Avg={avg_pnl:+.2f}/trade")

    # ==============================
    # ANALYSIS 12: Larger IV change thresholds
    # ==============================
    print("\n" + "=" * 80)
    print("ANALYSIS 12: Different IV change thresholds (near-put)")
    print("Only block if IV change exceeds threshold")
    print("=" * 80)

    thresholds = [0, 0.01, 0.02, 0.03, 0.05, 0.08, 0.10]
    iv_data = [r for r in results if r["near_put_iv_change"] is not None]

    print(f"\n  {'Threshold':<12s} {'Passed':>7s} {'P_WR':>6s} {'P_PnL':>9s} {'Blocked':>8s} {'B_WR':>6s} {'B_PnL':>9s} {'Net_Chg':>9s}")
    print("  " + "-" * 75)

    base_pnl = sum(r["pnl"] for r in iv_data if r["pnl"] is not None)

    for thresh in thresholds:
        p = []
        b = []
        for r in iv_data:
            d = r["direction"]
            iv = r["near_put_iv_change"]
            if d in ("long", "bullish"):
                if iv > thresh:
                    b.append(r)
                else:
                    p.append(r)
            else:
                if iv < -thresh:
                    b.append(r)
                else:
                    p.append(r)
        p_wins = sum(1 for r in p if r["outcome"] == "WIN")
        p_wr = p_wins / len(p) * 100 if p else 0
        p_pnl = sum(r["pnl"] for r in p if r["pnl"] is not None)
        b_wins = sum(1 for r in b if r["outcome"] == "WIN")
        b_wr = b_wins / len(b) * 100 if b else 0
        b_pnl = sum(r["pnl"] for r in b if r["pnl"] is not None)
        net_chg = p_pnl - base_pnl
        print(f"  {thresh:<12.2f} {len(p):>7d} {p_wr:>5.1f}% {p_pnl:>+9.1f} {len(b):>8d} {b_wr:>5.1f}% {b_pnl:>+9.1f} {net_chg:>+9.1f}")

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
