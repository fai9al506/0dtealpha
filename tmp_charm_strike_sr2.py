"""Backtest v2: Charm per-STRIKE S/R with STRONGEST strikes (not nearest).

Changes from v1:
- Instead of nearest positive-above / negative-below, find the STRONGEST
  (highest absolute charm value) in each category
- Require minimum 10 pt range (narrow ranges are noise)
- Also test: strength ratio (strong support + weak resistance = bullish edge)
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

    print(f"Total trades with outcomes: {len(trades)}")

    results = []
    no_charm = 0
    no_sr_pair = 0
    narrow_range = 0

    for t in trades:
        spot = float(t.spot)
        ts = t.ts

        # Get individual charm strikes within ±25 pts of spot
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
            "lo": spot - 25,
            "hi": spot + 25,
        }).fetchall()

        if not charm_rows:
            no_charm += 1
            continue

        # Deduplicate: keep most recent snapshot only
        seen = set()
        strikes = []
        for cr in charm_rows:
            sk = float(cr.strike)
            if sk not in seen:
                seen.add(sk)
                strikes.append({"strike": sk, "value": float(cr.value)})

        # Categorize strikes by sign and position
        # RESISTANCE = positive charm ABOVE spot (bearish = dealers sell)
        # SUPPORT = negative charm BELOW spot (bullish = dealers buy)
        # MAGNET UP = negative charm ABOVE spot (bullish = pulls price up)
        # MAGNET DOWN = positive charm BELOW spot (bearish = pushes price down)
        pos_above = [x for x in strikes if x["strike"] > spot and x["value"] > 0]
        neg_below = [x for x in strikes if x["strike"] <= spot and x["value"] < 0]
        neg_above = [x for x in strikes if x["strike"] > spot and x["value"] < 0]
        pos_below = [x for x in strikes if x["strike"] <= spot and x["value"] > 0]

        if not pos_above or not neg_below:
            no_sr_pair += 1
            continue

        # Find STRONGEST (highest abs value) in each category, not nearest
        resistance = max(pos_above, key=lambda x: abs(x["value"]))
        support = max(neg_below, key=lambda x: abs(x["value"]))

        r_strike = resistance["strike"]
        r_value = resistance["value"]
        s_strike = support["strike"]
        s_value = support["value"]
        rng = r_strike - s_strike

        if rng < 10:
            narrow_range += 1
            continue

        # Position in range: 0% = at support, 100% = at resistance
        position_pct = (spot - s_strike) / rng * 100

        is_long = t.direction in ("long", "bullish")
        is_win = "WIN" in t.outcome_result
        pnl = float(t.outcome_pnl or 0)
        max_loss = float(t.outcome_max_loss or 0)
        max_profit = float(t.outcome_max_profit or 0)

        # Strength ratio: support vs resistance
        # For LONGS: want strong support, weak resistance (ratio > 1 = support dominates)
        # For SHORTS: want strong resistance, weak support (ratio < 1 = resistance dominates)
        support_strength = abs(s_value)
        resist_strength = abs(r_value)
        strength_ratio = support_strength / max(resist_strength, 1)  # > 1 = support stronger

        # Count magnets
        magnet_up_val = max([abs(x["value"]) for x in neg_above], default=0)
        magnet_down_val = max([abs(x["value"]) for x in pos_below], default=0)

        # R:R classification
        if is_long:
            good_rr = position_pct <= 30
            bad_rr = position_pct >= 70
        else:
            good_rr = position_pct >= 70
            bad_rr = position_pct <= 30

        # Force alignment classification
        # For LONGS: support stronger + position near support = strong setup
        # For SHORTS: resistance stronger + position near resistance = strong setup
        if is_long:
            force_aligned = strength_ratio > 1 and position_pct <= 50
            force_opposed = strength_ratio < 0.5 and position_pct >= 50
        else:
            force_aligned = strength_ratio < 1 and position_pct >= 50
            force_opposed = strength_ratio > 2 and position_pct <= 50

        results.append({
            "id": t.id, "ts": str(ts)[:16], "setup": t.setup_name,
            "dir": t.direction, "spot": spot,
            "r_strike": r_strike, "s_strike": s_strike, "range": rng,
            "r_value": r_value / 1e6, "s_value": s_value / 1e6,  # Convert to M
            "position_pct": position_pct,
            "good_rr": good_rr, "bad_rr": bad_rr,
            "is_long": is_long, "is_win": is_win, "pnl": pnl,
            "max_loss": max_loss, "max_profit": max_profit,
            "result": t.outcome_result,
            "align": int(t.greek_alignment) if t.greek_alignment is not None else None,
            "strength_ratio": strength_ratio,
            "support_str": support_strength / 1e6,
            "resist_str": resist_strength / 1e6,
            "force_aligned": force_aligned,
            "force_opposed": force_opposed,
            "magnet_up": magnet_up_val / 1e6,
            "magnet_down": magnet_down_val / 1e6,
        })

    print(f"Trades with valid charm S/R (range >= 10): {len(results)}")
    print(f"No charm data: {no_charm}")
    print(f"No valid S/R pair: {no_sr_pair}")
    print(f"Narrow range (<10 pts): {narrow_range}")

    # =====================================================
    def stats(subset, label):
        if not subset:
            print(f"  {label:50s}: NO TRADES")
            return
        n = len(subset)
        w = sum(1 for t in subset if t["is_win"])
        l = sum(1 for t in subset if "LOSS" in t["result"])
        pnl = sum(t["pnl"] for t in subset)
        wr = w / (w + l) * 100 if (w + l) else 0
        avg_dd = sum(t["max_loss"] for t in subset) / n
        avg_mp = sum(t["max_profit"] for t in subset) / n
        ppt = pnl / n
        print(f"  {label:50s}: {n:3d}t {pnl:+8.1f}pts {wr:5.1f}%WR {w}W/{l}L dd={avg_dd:+.1f} ppt={ppt:+.1f}")

    # =====================================================
    print("")
    print("=" * 95)
    print("SECTION 1: R:R POSITION (strongest strikes, range >= 10)")
    print("=" * 95)

    good = [r for r in results if r["good_rr"]]
    bad = [r for r in results if r["bad_rr"]]
    mid = [r for r in results if not r["good_rr"] and not r["bad_rr"]]

    stats(results, "ALL")
    stats(good, "GOOD R:R (near favorable S/R)")
    stats(mid, "MID zone (30-70%)")
    stats(bad, "BAD R:R (near opposing S/R)")
    print()
    stats([r for r in results if r["is_long"]], "All LONGS")
    stats([r for r in good if r["is_long"]], "  Long GOOD R:R (pos <= 30%)")
    stats([r for r in mid if r["is_long"]], "  Long MID")
    stats([r for r in bad if r["is_long"]], "  Long BAD R:R (pos >= 70%)")
    print()
    stats([r for r in results if not r["is_long"]], "All SHORTS")
    stats([r for r in good if not r["is_long"]], "  Short GOOD R:R (pos >= 70%)")
    stats([r for r in mid if not r["is_long"]], "  Short MID")
    stats([r for r in bad if not r["is_long"]], "  Short BAD R:R (pos <= 30%)")

    # =====================================================
    print("")
    print("=" * 95)
    print("SECTION 2: POSITION % BUCKETS (10%)")
    print("=" * 95)

    for direction in ["ALL", "LONG", "SHORT"]:
        print(f"\n  --- {direction} ---")
        for lo_pct in range(0, 100, 10):
            hi_pct = lo_pct + 10
            if direction == "ALL":
                sub = [r for r in results if lo_pct <= r["position_pct"] < hi_pct]
            elif direction == "LONG":
                sub = [r for r in results if lo_pct <= r["position_pct"] < hi_pct and r["is_long"]]
            else:
                sub = [r for r in results if lo_pct <= r["position_pct"] < hi_pct and not r["is_long"]]
            if not sub:
                continue
            n = len(sub)
            w = sum(1 for t in sub if t["is_win"])
            l = sum(1 for t in sub if "LOSS" in t["result"])
            pnl = sum(t["pnl"] for t in sub)
            wr = w / (w + l) * 100 if (w + l) else 0
            bar = "#" * int(wr / 2)
            print(f"    {lo_pct:3d}-{hi_pct:3d}%: {n:3d}t {pnl:+8.1f}pts {wr:5.1f}%WR {bar}")

    # =====================================================
    print("")
    print("=" * 95)
    print("SECTION 3: STRENGTH RATIO (support_strength / resist_strength)")
    print("=" * 95)
    print("  ratio > 1 = support stronger than resistance (bullish)")
    print("  ratio < 1 = resistance stronger than support (bearish)")

    for lo_r, hi_r, rl in [(0, 0.25, "< 0.25 (very strong R)"), (0.25, 0.5, "0.25-0.5 (strong R)"),
                            (0.5, 1, "0.5-1.0 (slight R)"), (1, 2, "1.0-2.0 (slight S)"),
                            (2, 4, "2.0-4.0 (strong S)"), (4, 9999, "> 4.0 (very strong S)")]:
        sub = [r for r in results if lo_r <= r["strength_ratio"] < hi_r]
        stats(sub, f"Ratio {rl}")

    print("\n  --- By direction ---")
    for direction in ["LONG", "SHORT"]:
        is_l = direction == "LONG"
        print(f"\n  {direction}:")
        for lo_r, hi_r, rl in [(0, 0.5, "R dominates"), (0.5, 2, "balanced"),
                                (2, 9999, "S dominates")]:
            sub = [r for r in results if lo_r <= r["strength_ratio"] < hi_r and r["is_long"] == is_l]
            stats(sub, f"  {rl}")

    # =====================================================
    print("")
    print("=" * 95)
    print("SECTION 4: FORCE ALIGNMENT (position + strength combined)")
    print("=" * 95)
    print("  Aligned = support dominates + spot near support (longs)")
    print("           = resistance dominates + spot near resistance (shorts)")

    fa = [r for r in results if r["force_aligned"]]
    fo = [r for r in results if r["force_opposed"]]
    fn = [r for r in results if not r["force_aligned"] and not r["force_opposed"]]

    stats(results, "ALL")
    stats(fa, "FORCE ALIGNED")
    stats(fn, "NEUTRAL")
    stats(fo, "FORCE OPPOSED")

    for direction in ["LONG", "SHORT"]:
        is_l = direction == "LONG"
        sub = [r for r in results if r["is_long"] == is_l]
        print(f"\n  {direction}:")
        stats([r for r in sub if r["force_aligned"]], f"  {direction} Force Aligned")
        stats([r for r in sub if not r["force_aligned"] and not r["force_opposed"]], f"  {direction} Neutral")
        stats([r for r in sub if r["force_opposed"]], f"  {direction} Force Opposed")

    # =====================================================
    print("")
    print("=" * 95)
    print("SECTION 5: BY SETUP")
    print("=" * 95)

    for setup in sorted(set(r["setup"] for r in results)):
        sub = [r for r in results if r["setup"] == setup]
        if len(sub) < 3:
            continue
        print(f"\n--- {setup} ({len(sub)} trades) ---")
        stats(sub, "All")
        stats([r for r in sub if r["good_rr"]], "GOOD R:R")
        stats([r for r in sub if not r["good_rr"] and not r["bad_rr"]], "MID")
        stats([r for r in sub if r["bad_rr"]], "BAD R:R")
        stats([r for r in sub if r["force_aligned"]], "Force Aligned")
        stats([r for r in sub if r["force_opposed"]], "Force Opposed")

    # =====================================================
    print("")
    print("=" * 95)
    print("SECTION 6: COMBINED WITH OPTION B")
    print("=" * 95)

    def option_b_filter(r):
        if r["is_long"]:
            return r["align"] is not None and r["align"] >= 3
        else:
            if r["setup"] == "ES Absorption":
                return False
            if r["setup"] == "BofA Scalp":
                return False
            if r["setup"] == "DD Exhaustion" and r["align"] == 0:
                return False
            if r["setup"] == "AG Short" and r["align"] == -3:
                return False
            return True

    optb = [r for r in results if option_b_filter(r)]
    print(f"\nOption B trades with charm S/R data (range >= 10): {len(optb)}")
    stats(optb, "Option B baseline")
    stats([r for r in optb if r["good_rr"]], "Option B + GOOD R:R only")
    stats([r for r in optb if not r["good_rr"] and not r["bad_rr"]], "Option B + MID only")
    stats([r for r in optb if r["bad_rr"]], "Option B + BAD R:R only")

    # Filter strategies
    print("\n  --- Filter strategies ---")
    # F7a: Remove BAD R:R from Option B
    f7a = [r for r in optb if not r["bad_rr"]]
    stats(f7a, "F7a: Option B - BAD R:R")

    # F7b: Remove MID zone from Option B
    f7b = [r for r in optb if not (not r["good_rr"] and not r["bad_rr"])]
    stats(f7b, "F7b: Option B - MID zone")

    # F7c: Remove FORCE OPPOSED from Option B
    f7c = [r for r in optb if not r["force_opposed"]]
    stats(f7c, "F7c: Option B - Force Opposed")

    # F7d: Keep only FORCE ALIGNED from Option B
    f7d = [r for r in optb if r["force_aligned"]]
    stats(f7d, "F7d: Option B + Force Aligned only")

    # F7e: Remove MID zone SHORTS only
    f7e = [r for r in optb if not (not r["is_long"] and not r["good_rr"] and not r["bad_rr"])]
    stats(f7e, "F7e: Option B - MID shorts")

    # =====================================================
    print("")
    print("=" * 95)
    print("SECTION 7: SAMPLE TRADES (verify logic)")
    print("=" * 95)

    # User's example near 6765
    print("\n--- Trades near 6765 (user's example) ---")
    for r in sorted(results, key=lambda x: abs(x["spot"] - 6765))[:8]:
        rr = "GOOD" if r["good_rr"] else ("BAD" if r["bad_rr"] else "MID")
        fa = "FA" if r["force_aligned"] else ("FO" if r["force_opposed"] else "  ")
        print(f"  #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} "
              f"spot={r['spot']:.1f} S={r['s_strike']:.0f}({r['s_value']:+.1f}M) "
              f"R={r['r_strike']:.0f}({r['r_value']:+.1f}M) "
              f"rng={r['range']:.0f} pos={r['position_pct']:.0f}% "
              f"str_ratio={r['strength_ratio']:.1f} {rr} {fa} | "
              f"{r['result']:8s} {r['pnl']:+.1f}")

    # Best GOOD R:R trades
    print("\n--- Best GOOD R:R wins ---")
    for r in sorted([r for r in results if r["good_rr"] and r["is_win"]],
                    key=lambda x: x["pnl"], reverse=True)[:8]:
        fa = "FA" if r["force_aligned"] else ("FO" if r["force_opposed"] else "  ")
        print(f"  #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} "
              f"spot={r['spot']:.1f} S={r['s_strike']:.0f}({r['s_value']:+.1f}M) "
              f"R={r['r_strike']:.0f}({r['r_value']:+.1f}M) "
              f"rng={r['range']:.0f} pos={r['position_pct']:.0f}% "
              f"str_ratio={r['strength_ratio']:.1f} {fa} | "
              f"{r['result']:8s} {r['pnl']:+.1f} dd={r['max_loss']:+.1f}")

    # Worst FORCE OPPOSED trades
    print("\n--- Force Opposed trades (worst) ---")
    for r in sorted([r for r in results if r["force_opposed"]], key=lambda x: x["pnl"])[:8]:
        print(f"  #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} "
              f"spot={r['spot']:.1f} S={r['s_strike']:.0f}({r['s_value']:+.1f}M) "
              f"R={r['r_strike']:.0f}({r['r_value']:+.1f}M) "
              f"rng={r['range']:.0f} pos={r['position_pct']:.0f}% "
              f"str_ratio={r['strength_ratio']:.1f} | "
              f"{r['result']:8s} {r['pnl']:+.1f}")

    print("\n--- Force Opposed trades (best) ---")
    for r in sorted([r for r in results if r["force_opposed"]], key=lambda x: x["pnl"], reverse=True)[:8]:
        print(f"  #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} "
              f"spot={r['spot']:.1f} S={r['s_strike']:.0f}({r['s_value']:+.1f}M) "
              f"R={r['r_strike']:.0f}({r['r_value']:+.1f}M) "
              f"rng={r['range']:.0f} pos={r['position_pct']:.0f}% "
              f"str_ratio={r['strength_ratio']:.1f} | "
              f"{r['result']:8s} {r['pnl']:+.1f}")
