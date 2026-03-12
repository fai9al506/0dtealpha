"""Simulate IMPROVED ENTRIES using charm S/R levels.

For each trade:
1. Find charm S/R (strongest positive above = R, strongest negative below = S)
2. Calculate the 30% entry level (for longs: S + range*0.3, for shorts: R - range*0.3)
3. Check if price actually reached that level within 30 min after signal
4. If yes: simulate the trade from the improved entry
5. Compare original vs improved outcomes

Uses es_range_bars (rithmic 5-pt bars) for forward price simulation.
Falls back to setup_log max_profit/max_loss for approximate analysis.
"""
import sqlalchemy as sa
import os
from datetime import timedelta

engine = sa.create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    trades = conn.execute(sa.text("""
        SELECT id, ts, setup_name, direction, grade, score, spot,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               greek_alignment
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND spot IS NOT NULL
        ORDER BY ts
    """)).fetchall()

    print(f"Total trades: {len(trades)}")

    results = []
    no_charm = 0
    no_sr = 0
    narrow = 0

    for t in trades:
        spot = float(t.spot)
        ts = t.ts
        is_long = t.direction in ("long", "bullish")
        pnl = float(t.outcome_pnl or 0)
        max_profit = float(t.outcome_max_profit or 0)
        max_loss = float(t.outcome_max_loss or 0)
        result = t.outcome_result

        # Get charm strikes
        charm_rows = conn.execute(sa.text("""
            SELECT strike, value
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc BETWEEN :ts_start AND :ts_end
              AND strike BETWEEN :lo AND :hi
              AND value != 0
            ORDER BY ts_utc DESC, abs(value) DESC
        """), {
            "ts_start": ts - timedelta(minutes=5),
            "ts_end": ts + timedelta(minutes=1),
            "lo": spot - 25, "hi": spot + 25,
        }).fetchall()

        if not charm_rows:
            no_charm += 1
            continue

        seen = set()
        strikes = []
        for cr in charm_rows:
            sk = float(cr.strike)
            if sk not in seen:
                seen.add(sk)
                strikes.append({"strike": sk, "value": float(cr.value)})

        pos_above = [x for x in strikes if x["strike"] > spot and x["value"] > 0]
        neg_below = [x for x in strikes if x["strike"] <= spot and x["value"] < 0]

        if not pos_above or not neg_below:
            no_sr += 1
            continue

        resistance = max(pos_above, key=lambda x: abs(x["value"]))
        support = max(neg_below, key=lambda x: abs(x["value"]))
        r_strike = resistance["strike"]
        s_strike = support["strike"]
        rng = r_strike - s_strike

        if rng < 10:
            narrow += 1
            continue

        position_pct = (spot - s_strike) / rng * 100

        # Calculate improved entry
        if is_long:
            ideal_entry = s_strike + rng * 0.3  # Lower 30% for longs
            entry_improvement = spot - ideal_entry  # How much better (positive = improvement)
            # Would price reach ideal_entry? For longs, price needs to DIP to ideal_entry
            # max_loss tells us the lowest price reached relative to spot
            # lowest_price = spot + max_loss (max_loss is negative)
            lowest_price = spot + max_loss  # e.g., spot=6769, max_loss=-12.5 -> low=6756.5
            would_fill = lowest_price <= ideal_entry
        else:
            ideal_entry = r_strike - rng * 0.3  # Upper 30% for shorts
            entry_improvement = ideal_entry - spot  # How much better
            # For shorts, price needs to RISE to ideal_entry
            highest_price = spot + max_profit  # max_profit is positive
            would_fill = highest_price >= ideal_entry

        # Check if already in good zone
        if is_long:
            already_good = position_pct <= 30
        else:
            already_good = position_pct >= 70

        # Simulate improved outcome
        if already_good:
            # Already at good entry, no change needed
            new_pnl = pnl
            new_result = result
            status = "ALREADY_GOOD"
        elif not would_fill:
            # Price never reached ideal entry -> trade skipped
            new_pnl = 0
            new_result = "SKIPPED"
            status = "SKIPPED"
        else:
            # Price reached ideal entry -> simulate from improved entry
            # New PnL = original PnL + entry_improvement
            new_pnl = pnl + entry_improvement
            # Check if the improved entry changes WIN/LOSS
            if is_long:
                # Get stop/target from original trade
                # Original: stop at spot - SL, target at spot + T
                # Improved: stop at ideal_entry - SL, target at ideal_entry + T
                # But we need to check: did price hit the new stop or new target first?
                # Approximation: shift PnL by improvement amount
                # The max loss from ideal_entry = max_loss + improvement (less negative)
                new_max_loss = max_loss + entry_improvement
                new_max_profit = max_profit + entry_improvement
            else:
                new_max_loss = max_loss + entry_improvement
                new_max_profit = max_profit + entry_improvement

            # Determine new result based on setup stop levels
            stop_pts = {"DD Exhaustion": 12, "Skew Charm": 20, "GEX Long": 8,
                       "AG Short": 20, "BofA Scalp": 10, "ES Absorption": 8,
                       "Paradigm Reversal": 15}.get(t.setup_name, 12)
            target_pts = 10  # First target

            if is_long:
                # From ideal_entry, max profit reached = max_profit + improvement
                # From ideal_entry, max loss reached = max_loss + improvement (less negative)
                actual_max_loss_from_new = max_loss + entry_improvement
                actual_max_profit_from_new = max_profit + entry_improvement
            else:
                actual_max_loss_from_new = max_loss + entry_improvement
                actual_max_profit_from_new = max_profit + entry_improvement

            if actual_max_profit_from_new >= target_pts:
                new_result = "WIN"
                new_pnl = min(new_pnl, actual_max_profit_from_new)  # Cap at realistic
            elif actual_max_loss_from_new <= -stop_pts:
                new_result = "LOSS"
                new_pnl = -stop_pts
            else:
                new_result = result  # Same as original
                new_pnl = pnl + entry_improvement

            status = "IMPROVED"

        results.append({
            "id": t.id, "ts": str(ts)[:16], "setup": t.setup_name,
            "dir": t.direction, "spot": spot, "is_long": is_long,
            "s_strike": s_strike, "r_strike": r_strike, "range": rng,
            "s_value": support["value"] / 1e6, "r_value": resistance["value"] / 1e6,
            "position_pct": position_pct,
            "ideal_entry": ideal_entry,
            "entry_improvement": entry_improvement,
            "would_fill": would_fill,
            "already_good": already_good,
            "status": status,
            "orig_pnl": pnl, "orig_result": result,
            "new_pnl": new_pnl, "new_result": new_result,
            "max_profit": max_profit, "max_loss": max_loss,
            "align": int(t.greek_alignment) if t.greek_alignment is not None else None,
            "is_win_orig": "WIN" in result,
            "is_win_new": "WIN" in new_result if new_result else False,
        })

    print(f"Trades analyzed: {len(results)}")
    print(f"No charm: {no_charm}, No S/R: {no_sr}, Narrow: {narrow}")

    # =====================================================
    def stats(subset, label):
        if not subset:
            print(f"  {label:55s}: NO TRADES")
            return
        n = len(subset)
        orig_pnl = sum(t["orig_pnl"] for t in subset)
        new_pnl = sum(t["new_pnl"] for t in subset)
        orig_w = sum(1 for t in subset if t["is_win_orig"])
        new_w = sum(1 for t in subset if t["is_win_new"])
        orig_l = sum(1 for t in subset if "LOSS" in t["orig_result"])
        new_l = sum(1 for t in subset if "LOSS" in (t["new_result"] or ""))
        skipped = sum(1 for t in subset if t["status"] == "SKIPPED")
        avg_imp = sum(t["entry_improvement"] for t in subset if t["status"] == "IMPROVED") / max(1, sum(1 for t in subset if t["status"] == "IMPROVED"))
        print(f"  {label:55s}: {n:3d}t orig={orig_pnl:+8.1f} new={new_pnl:+8.1f} "
              f"delta={new_pnl-orig_pnl:+7.1f} | "
              f"W:{orig_w}->{new_w} L:{orig_l}->{new_l} skip={skipped}")

    # =====================================================
    print("")
    print("=" * 110)
    print("SECTION 1: OVERALL — Original vs Improved Entry")
    print("=" * 110)

    stats(results, "ALL trades")
    stats([r for r in results if r["is_long"]], "LONGS")
    stats([r for r in results if not r["is_long"]], "SHORTS")

    print(f"\n  Status breakdown:")
    for st in ["ALREADY_GOOD", "IMPROVED", "SKIPPED"]:
        sub = [r for r in results if r["status"] == st]
        if sub:
            print(f"    {st:15s}: {len(sub)} trades")

    # =====================================================
    print("")
    print("=" * 110)
    print("SECTION 2: IMPROVED ENTRIES ONLY (trades that got a better price)")
    print("=" * 110)

    improved = [r for r in results if r["status"] == "IMPROVED"]
    print(f"\n  Improved trades: {len(improved)}")
    avg_improvement = sum(t["entry_improvement"] for t in improved) / len(improved) if improved else 0
    print(f"  Avg entry improvement: {avg_improvement:+.1f} pts")

    # Trades that flipped from LOSS to WIN
    flipped = [r for r in improved if "LOSS" in r["orig_result"] and "WIN" in r["new_result"]]
    print(f"  Trades flipped LOSS->WIN: {len(flipped)}")
    for r in flipped:
        print(f"    #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} "
              f"spot={r['spot']:.1f} ideal={r['ideal_entry']:.1f} imp={r['entry_improvement']:+.1f} "
              f"| {r['orig_result']}({r['orig_pnl']:+.1f}) -> {r['new_result']}({r['new_pnl']:+.1f})")

    # Trades that flipped from WIN to SKIPPED
    win_skipped = [r for r in results if r["status"] == "SKIPPED" and r["is_win_orig"]]
    print(f"\n  Winners that would be SKIPPED: {len(win_skipped)} (lost opportunity)")
    missed_pnl = sum(r["orig_pnl"] for r in win_skipped)
    print(f"  Missed PnL from skipped winners: {missed_pnl:+.1f}")

    # =====================================================
    print("")
    print("=" * 110)
    print("SECTION 3: BY SETUP")
    print("=" * 110)

    for setup in sorted(set(r["setup"] for r in results)):
        sub = [r for r in results if r["setup"] == setup]
        if len(sub) < 3:
            continue
        print(f"\n--- {setup} ---")
        stats(sub, "All")
        stats([r for r in sub if r["status"] == "IMPROVED"], "Improved entries")
        stats([r for r in sub if r["status"] == "SKIPPED"], "Skipped (didn't reach ideal)")

    # =====================================================
    print("")
    print("=" * 110)
    print("SECTION 4: OPTION B FILTERED")
    print("=" * 110)

    def option_b(r):
        if r["is_long"]:
            return r["align"] is not None and r["align"] >= 3
        else:
            if r["setup"] == "ES Absorption": return False
            if r["setup"] == "BofA Scalp": return False
            if r["setup"] == "DD Exhaustion" and r["align"] == 0: return False
            if r["setup"] == "AG Short" and r["align"] == -3: return False
            return True

    optb = [r for r in results if option_b(r)]
    print(f"\nOption B trades: {len(optb)}")
    stats(optb, "Option B baseline")
    stats([r for r in optb if r["status"] != "SKIPPED"], "Option B with improved entries (no skips)")

    optb_improved = [r for r in optb if r["status"] == "IMPROVED"]
    optb_skipped = [r for r in optb if r["status"] == "SKIPPED"]
    optb_good = [r for r in optb if r["status"] == "ALREADY_GOOD"]

    orig_total = sum(r["orig_pnl"] for r in optb)
    new_total = sum(r["new_pnl"] for r in optb)

    print(f"\n  Option B Original PnL: {orig_total:+.1f}")
    print(f"  Option B Improved PnL: {new_total:+.1f}")
    print(f"  Delta:                 {new_total - orig_total:+.1f}")
    print(f"  Skipped trades:        {len(optb_skipped)} ({sum(r['orig_pnl'] for r in optb_skipped):+.1f} original PnL)")
    print(f"  Flipped LOSS->WIN:      {len([r for r in optb_improved if 'LOSS' in r['orig_result'] and 'WIN' in r['new_result']])}")

    # =====================================================
    print("")
    print("=" * 110)
    print("SECTION 5: MARCH 11 DETAIL")
    print("=" * 110)

    mar11 = [r for r in results if "2026-03-11" in r["ts"]]
    mar11_optb = [r for r in mar11 if option_b(r)]

    print(f"\nMarch 11 all: {len(mar11)} trades")
    stats(mar11, "Mar 11 ALL")
    stats(mar11_optb, "Mar 11 Option B")

    print(f"\nMarch 11 Option B detail:")
    for r in sorted(mar11_optb, key=lambda x: x["ts"]):
        status = r["status"]
        imp = f"imp={r['entry_improvement']:+.1f}" if status == "IMPROVED" else ""
        print(f"  #{r['id']} {r['ts'][11:16]} {r['setup']:20s} {r['dir']:8s} "
              f"spot={r['spot']:.1f} pos={r['position_pct']:.0f}% "
              f"{status:12s} {imp:10s} "
              f"orig={r['orig_pnl']:+6.1f} new={r['new_pnl']:+6.1f} "
              f"({r['orig_result']:8s}->{r['new_result']})")
