"""Backtest: Charm per-STRIKE S/R range entry quality.

For each historical trade:
1. Find charm exposure at individual strikes within ±20 pts of spot
2. Identify the nearest POSITIVE charm strike ABOVE spot = RESISTANCE
   (positive charm = bearish = dealers sell = ceiling)
3. Identify the nearest NEGATIVE charm strike BELOW spot = SUPPORT
   (negative charm = bullish = dealers buy = floor)
4. Calculate where spot sits in this S/R range (0% = at support, 100% = at resistance)
5. For LONGS:  good R:R = near support (lower 30%)
   For SHORTS: good R:R = near resistance (upper 30%)
6. Compare performance of good vs bad R:R entries

Key difference from aggregate charm: we use INDIVIDUAL strike levels,
not sums of charm across multiple strikes.
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

    for t in trades:
        spot = float(t.spot)
        ts = t.ts

        # Get individual charm strikes near spot
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
            "lo": spot - 20,
            "hi": spot + 20,
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

        # Top 8 by absolute value (most significant levels)
        top8 = sorted(strikes, key=lambda x: abs(x["value"]), reverse=True)[:8]

        # Find S/R pair using charm sign + position:
        # RESISTANCE = positive charm ABOVE spot (dealers sell = ceiling)
        # SUPPORT = negative charm BELOW spot (dealers buy = floor)
        pos_above = [x for x in top8 if x["strike"] > spot and x["value"] > 0]
        neg_below = [x for x in top8 if x["strike"] <= spot and x["value"] < 0]

        if not pos_above or not neg_below:
            no_sr_pair += 1
            continue

        # Nearest resistance above, nearest support below
        resistance = min(pos_above, key=lambda x: x["strike"])
        support = max(neg_below, key=lambda x: x["strike"])

        r_strike = resistance["strike"]
        r_value = resistance["value"]  # positive (bearish)
        s_strike = support["strike"]
        s_value = support["value"]     # negative (bullish)
        rng = r_strike - s_strike

        if rng < 3:  # Too narrow
            no_sr_pair += 1
            continue

        # Position in range: 0% = at support, 100% = at resistance
        position_pct = (spot - s_strike) / rng * 100

        is_long = t.direction in ("long", "bullish")
        is_win = "WIN" in t.outcome_result
        pnl = float(t.outcome_pnl or 0)
        max_loss = float(t.outcome_max_loss or 0)
        max_profit = float(t.outcome_max_profit or 0)

        # Also find magnets (opposite sign/position combos)
        neg_above = [x for x in top8 if x["strike"] > spot and x["value"] < 0]  # magnet UP
        pos_below = [x for x in top8 if x["strike"] <= spot and x["value"] > 0]  # magnet DOWN

        # For LONGS: good R:R = near support (lower 30%), bad = near resistance (upper 30%)
        # For SHORTS: good R:R = near resistance (upper 30%), bad = near support (lower 30%)
        if is_long:
            good_rr = position_pct <= 30
            bad_rr = position_pct >= 70
        else:
            good_rr = position_pct >= 70
            bad_rr = position_pct <= 30

        mid = not good_rr and not bad_rr

        results.append({
            "id": t.id, "ts": str(ts)[:16], "setup": t.setup_name,
            "dir": t.direction, "spot": spot,
            "r_strike": r_strike, "s_strike": s_strike, "range": rng,
            "r_value": r_value, "s_value": s_value,
            "position_pct": position_pct,
            "good_rr": good_rr, "bad_rr": bad_rr, "mid": mid,
            "is_long": is_long, "is_win": is_win, "pnl": pnl,
            "max_loss": max_loss, "max_profit": max_profit,
            "result": t.outcome_result,
            "align": int(t.greek_alignment) if t.greek_alignment is not None else None,
            "has_magnet_up": len(neg_above) > 0,
            "has_magnet_down": len(pos_below) > 0,
        })

    print(f"Trades with valid charm S/R pair: {len(results)}")
    print(f"No charm data: {no_charm}")
    print(f"No valid S/R pair: {no_sr_pair}")

    # =====================================================
    def stats(subset, label):
        if not subset:
            print(f"  {label:45s}: NO TRADES")
            return
        n = len(subset)
        w = sum(1 for t in subset if t["is_win"])
        l = sum(1 for t in subset if "LOSS" in t["result"])
        pnl = sum(t["pnl"] for t in subset)
        wr = w / (w + l) * 100 if (w + l) else 0
        avg_dd = sum(t["max_loss"] for t in subset) / n
        avg_mp = sum(t["max_profit"] for t in subset) / n
        print(f"  {label:45s}: {n:3d}t {pnl:+8.1f}pts {wr:5.1f}%WR {w}W/{l}L dd={avg_dd:+.1f} mp={avg_mp:+.1f}")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 1: OVERALL — Good R:R vs Mid vs Bad R:R")
    print("=" * 90)

    good = [r for r in results if r["good_rr"]]
    bad = [r for r in results if r["bad_rr"]]
    mid = [r for r in results if r["mid"]]

    stats(results, "ALL trades with charm S/R")
    stats(good, "GOOD R:R (near support/resistance)")
    stats(mid, "MID zone (30-70%)")
    stats(bad, "BAD R:R (against S/R)")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 2: BY DIRECTION")
    print("=" * 90)

    for direction in ["long", "short"]:
        is_l = direction == "long"
        sub = [r for r in results if r["is_long"] == is_l]
        print(f"\n--- {direction.upper()} ---")
        stats(sub, f"All {direction}")
        stats([r for r in sub if r["good_rr"]], f"{direction} GOOD R:R")
        stats([r for r in sub if r["mid"]], f"{direction} MID")
        stats([r for r in sub if r["bad_rr"]], f"{direction} BAD R:R")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 3: POSITION % DISTRIBUTION (10% buckets)")
    print("=" * 90)

    for lo_pct in range(0, 100, 10):
        hi_pct = lo_pct + 10
        sub = [r for r in results if lo_pct <= r["position_pct"] < hi_pct]
        if not sub:
            continue
        n = len(sub)
        w = sum(1 for t in sub if t["is_win"])
        l = sum(1 for t in sub if "LOSS" in t["result"])
        pnl = sum(t["pnl"] for t in sub)
        wr = w / (w + l) * 100 if (w + l) else 0
        avg_dd = sum(t["max_loss"] for t in sub) / n
        bar = "#" * int(wr / 2)
        print(f"  {lo_pct:3d}-{hi_pct:3d}%: {n:3d}t {pnl:+8.1f}pts {wr:5.1f}%WR dd={avg_dd:+.1f} {bar}")

    for direction in ["long", "short"]:
        is_l = direction == "long"
        print(f"\n  --- {direction.upper()} by position % ---")
        for lo_pct in range(0, 100, 10):
            hi_pct = lo_pct + 10
            sub = [r for r in results if lo_pct <= r["position_pct"] < hi_pct and r["is_long"] == is_l]
            if not sub:
                continue
            n = len(sub)
            w = sum(1 for t in sub if t["is_win"])
            l = sum(1 for t in sub if "LOSS" in t["result"])
            pnl = sum(t["pnl"] for t in sub)
            wr = w / (w + l) * 100 if (w + l) else 0
            avg_dd = sum(t["max_loss"] for t in sub) / n
            bar = "#" * int(wr / 2)
            print(f"    {lo_pct:3d}-{hi_pct:3d}%: {n:3d}t {pnl:+8.1f}pts {wr:5.1f}%WR dd={avg_dd:+.1f} {bar}")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 4: BY SETUP")
    print("=" * 90)

    for setup in sorted(set(r["setup"] for r in results)):
        sub = [r for r in results if r["setup"] == setup]
        if len(sub) < 3:
            continue
        print(f"\n--- {setup} ---")
        stats(sub, "All")
        stats([r for r in sub if r["good_rr"]], "GOOD R:R")
        stats([r for r in sub if r["mid"]], "MID")
        stats([r for r in sub if r["bad_rr"]], "BAD R:R")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 5: THRESHOLD SWEEP (what cutoff works best?)")
    print("=" * 90)

    for threshold in [20, 25, 30, 35, 40, 50]:
        good_t = [r for r in results if
                  (r["is_long"] and r["position_pct"] <= threshold) or
                  (not r["is_long"] and r["position_pct"] >= (100 - threshold))]
        bad_t = [r for r in results if
                 (r["is_long"] and r["position_pct"] >= (100 - threshold)) or
                 (not r["is_long"] and r["position_pct"] <= threshold)]

        n_good = len(good_t)
        n_bad = len(bad_t)
        pnl_good = sum(t["pnl"] for t in good_t)
        pnl_bad = sum(t["pnl"] for t in bad_t)
        w_good = sum(1 for t in good_t if t["is_win"])
        l_good = sum(1 for t in good_t if "LOSS" in t["result"])
        w_bad = sum(1 for t in bad_t if t["is_win"])
        l_bad = sum(1 for t in bad_t if "LOSS" in t["result"])
        wr_good = w_good / max(1, w_good + l_good) * 100
        wr_bad = w_bad / max(1, w_bad + l_bad) * 100
        dd_good = sum(t["max_loss"] for t in good_t) / max(1, n_good)
        dd_bad = sum(t["max_loss"] for t in bad_t) / max(1, n_bad)

        print(f"  Threshold {threshold}/{100-threshold}: "
              f"GOOD={n_good}t {pnl_good:+.1f}pts {wr_good:.0f}%WR dd={dd_good:+.1f} | "
              f"BAD={n_bad}t {pnl_bad:+.1f}pts {wr_bad:.0f}%WR dd={dd_bad:+.1f} | "
              f"delta={pnl_good-pnl_bad:+.1f}")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 6: S/R STRENGTH (does charm value magnitude matter?)")
    print("=" * 90)

    # Check if stronger S/R levels (higher abs charm value) produce better results
    for label, subset in [("ALL", results), ("GOOD R:R only", good), ("BAD R:R only", bad)]:
        if not subset:
            continue
        print(f"\n  --- {label} by resistance strength (abs charm value at resistance strike) ---")
        for lo_v, hi_v, vlabel in [(0, 20, "<20M"), (20, 50, "20-50M"), (50, 100, "50-100M"), (100, 9999, ">100M")]:
            sub = [r for r in subset if lo_v <= abs(r["r_value"]) < hi_v]
            if not sub:
                continue
            stats(sub, f"Resistance {vlabel}")

        print(f"\n  --- {label} by support strength (abs charm value at support strike) ---")
        for lo_v, hi_v, vlabel in [(0, 20, "<20M"), (20, 50, "20-50M"), (50, 100, "50-100M"), (100, 9999, ">100M")]:
            sub = [r for r in subset if lo_v <= abs(r["s_value"]) < hi_v]
            if not sub:
                continue
            stats(sub, f"Support {vlabel}")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 7: RANGE WIDTH (does wider S/R range matter?)")
    print("=" * 90)

    for lo_r, hi_r, rlabel in [(3, 5, "3-5pts"), (5, 10, "5-10pts"), (10, 15, "10-15pts"), (15, 25, "15-25pts"), (25, 50, "25+pts")]:
        sub = [r for r in results if lo_r <= r["range"] < hi_r]
        if not sub:
            continue
        stats(sub, f"Range {rlabel}")
        stats([r for r in sub if r["good_rr"]], f"  GOOD R:R in {rlabel}")
        stats([r for r in sub if r["bad_rr"]], f"  BAD R:R in {rlabel}")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 8: MAGNETS (do bullish/bearish magnets help?)")
    print("=" * 90)

    # For LONGS: having a negative charm ABOVE spot = magnet pulling UP = good
    # For SHORTS: having a positive charm BELOW spot = magnet pulling DOWN = good
    long_trades = [r for r in results if r["is_long"]]
    short_trades = [r for r in results if not r["is_long"]]

    print("\n--- LONGS ---")
    stats(long_trades, "All longs")
    stats([r for r in long_trades if r["has_magnet_up"]], "Longs WITH magnet up (neg charm above)")
    stats([r for r in long_trades if not r["has_magnet_up"]], "Longs WITHOUT magnet up")

    print("\n--- SHORTS ---")
    stats(short_trades, "All shorts")
    stats([r for r in short_trades if r["has_magnet_down"]], "Shorts WITH magnet down (pos charm below)")
    stats([r for r in short_trades if not r["has_magnet_down"]], "Shorts WITHOUT magnet down")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 9: COMBINED WITH OPTION B (SIM filter)")
    print("=" * 90)

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
    print(f"\nOption B trades with charm S/R data: {len(optb)}")
    stats(optb, "Option B (no charm filter)")
    stats([r for r in optb if r["good_rr"]], "Option B + GOOD R:R only")
    stats([r for r in optb if r["mid"]], "Option B + MID only")
    stats([r for r in optb if r["bad_rr"]], "Option B + BAD R:R only")

    # What if we remove BAD R:R from Option B?
    optb_no_bad = [r for r in optb if not r["bad_rr"]]
    stats(optb_no_bad, "Option B - BAD R:R (F7 filter)")

    pnl_b = sum(t["pnl"] for t in optb)
    pnl_f7 = sum(t["pnl"] for t in optb_no_bad)
    pnl_removed = sum(t["pnl"] for t in optb if t["bad_rr"])
    print(f"\n  Option B PnL:       {pnl_b:+.1f}")
    print(f"  Option B + F7:      {pnl_f7:+.1f}")
    print(f"  Removed BAD R:R:    {len([r for r in optb if r['bad_rr']])} trades")
    print(f"  PnL improvement:    {pnl_f7 - pnl_b:+.1f}")

    # =====================================================
    print("")
    print("=" * 90)
    print("SECTION 10: SAMPLE TRADES (verify the logic)")
    print("=" * 90)

    print("\n--- BAD R:R trades (worst PnL first) ---")
    for r in sorted([r for r in results if r["bad_rr"]], key=lambda x: x["pnl"])[:10]:
        print(f"  #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} "
              f"spot={r['spot']:.1f} S={r['s_strike']:.0f}({r['s_value']:+.0f}M) "
              f"R={r['r_strike']:.0f}({r['r_value']:+.0f}M) "
              f"rng={r['range']:.0f} pos={r['position_pct']:.0f}% | "
              f"{r['result']:8s} {r['pnl']:+.1f} dd={r['max_loss']:+.1f}")

    print("\n--- GOOD R:R trades (best PnL first) ---")
    for r in sorted([r for r in results if r["good_rr"]], key=lambda x: x["pnl"], reverse=True)[:10]:
        print(f"  #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} "
              f"spot={r['spot']:.1f} S={r['s_strike']:.0f}({r['s_value']:+.0f}M) "
              f"R={r['r_strike']:.0f}({r['r_value']:+.0f}M) "
              f"rng={r['range']:.0f} pos={r['position_pct']:.0f}% | "
              f"{r['result']:8s} {r['pnl']:+.1f} dd={r['max_loss']:+.1f}")

    print("\n--- The DD Exhaustion trade #? near 6765 (user's example) ---")
    for r in sorted(results, key=lambda x: abs(x["spot"] - 6765))[:5]:
        print(f"  #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} "
              f"spot={r['spot']:.1f} S={r['s_strike']:.0f}({r['s_value']:+.0f}M) "
              f"R={r['r_strike']:.0f}({r['r_value']:+.0f}M) "
              f"rng={r['range']:.0f} pos={r['position_pct']:.0f}% | "
              f"{r['result']:8s} {r['pnl']:+.1f} dd={r['max_loss']:+.1f}")
