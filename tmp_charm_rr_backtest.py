"""Backtest: Charm S/R range entry R:R filter.

For each historical trade:
1. Find charm exposure points at strikes within ±20 pts of spot at signal time
2. Identify top 8 significant charm levels (by abs value)
3. Find nearest S/R above and below spot
4. Check where spot sits in the range (0% = at support, 100% = at resistance)
5. Compare performance of "good R:R" entries (lower 30% for longs, upper 30% for shorts)
   vs "bad R:R" entries (opposite)
"""
import sqlalchemy as sa
import os
from collections import defaultdict
from datetime import timedelta

engine = sa.create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    # Get all trades with outcomes
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
    no_charm_data = 0
    no_range = 0

    for t in trades:
        spot = float(t.spot)
        ts = t.ts

        # Find charm exposure points near this trade's timestamp
        # Look for the most recent charm data within 5 minutes before the trade
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
            no_charm_data += 1
            continue

        # Deduplicate: keep only the most recent timestamp's data
        # (all rows from the most recent snapshot)
        first_ts_rows = []
        seen_strikes = set()
        for cr in charm_rows:
            strike = float(cr.strike)
            if strike not in seen_strikes:
                seen_strikes.add(strike)
                first_ts_rows.append({"strike": strike, "points": float(cr.value)})

        if len(first_ts_rows) < 2:
            no_range += 1
            continue

        # Top 8 by absolute value
        top8 = sorted(first_ts_rows, key=lambda x: abs(x["points"]), reverse=True)[:8]

        # Find levels above and below spot
        above = [x for x in top8 if x["strike"] > spot]
        below = [x for x in top8 if x["strike"] <= spot]

        if not above or not below:
            no_range += 1
            continue

        # Nearest significant level above and below
        resistance = min(above, key=lambda x: x["strike"])
        support = max(below, key=lambda x: x["strike"])

        r_strike = resistance["strike"]
        s_strike = support["strike"]
        r_pts = resistance["points"]
        s_pts = support["points"]
        rng = r_strike - s_strike

        if rng < 5:  # Too narrow range
            no_range += 1
            continue

        # Position in range: 0% = at support, 100% = at resistance
        position_pct = (spot - s_strike) / rng * 100

        is_long = t.direction in ("long", "bullish")
        is_short = not is_long
        is_win = "WIN" in t.outcome_result
        pnl = float(t.outcome_pnl or 0)
        max_loss = float(t.outcome_max_loss or 0)

        # For LONGS: good R:R = position_pct < 30 (near support)
        # For SHORTS: good R:R = position_pct > 70 (near resistance)
        if is_long:
            good_rr = position_pct <= 30
            bad_rr = position_pct >= 70
        else:
            good_rr = position_pct >= 70
            bad_rr = position_pct <= 30

        mid_zone = not good_rr and not bad_rr

        results.append({
            "id": t.id, "ts": str(ts)[:16], "setup": t.setup_name,
            "dir": t.direction, "spot": spot,
            "r_strike": r_strike, "s_strike": s_strike, "range": rng,
            "r_pts": r_pts, "s_pts": s_pts,
            "position_pct": position_pct,
            "good_rr": good_rr, "bad_rr": bad_rr, "mid": mid_zone,
            "is_long": is_long, "is_win": is_win, "pnl": pnl,
            "max_loss": max_loss,
            "result": t.outcome_result,
            "align": int(t.greek_alignment) if t.greek_alignment is not None else None,
        })

    print(f"Trades with charm data: {len(results)}")
    print(f"No charm data: {no_charm_data}")
    print(f"No valid range: {no_range}")

    # =====================================================
    print("")
    print("=" * 80)
    print("OVERALL: Good R:R vs Bad R:R vs Mid Zone")
    print("=" * 80)

    def stats(subset, label):
        if not subset:
            print(f"  {label:40s}: NO TRADES")
            return
        n = len(subset)
        w = sum(1 for t in subset if t["is_win"])
        l = sum(1 for t in subset if "LOSS" in t["result"])
        pnl = sum(t["pnl"] for t in subset)
        wr = w / (w + l) * 100 if (w + l) else 0
        avg_dd = sum(t["max_loss"] for t in subset) / n if n else 0
        print(f"  {label:40s}: {n:3d}t {pnl:+8.1f}pts {wr:5.1f}%WR {w}W/{l}L avg_dd={avg_dd:+.1f}")

    good = [r for r in results if r["good_rr"]]
    bad = [r for r in results if r["bad_rr"]]
    mid = [r for r in results if r["mid"]]

    stats(results, "ALL trades with charm data")
    stats(good, "GOOD R:R (near support/resistance)")
    stats(mid, "MID zone (30-70%)")
    stats(bad, "BAD R:R (against support/resistance)")

    # =====================================================
    print("")
    print("=" * 80)
    print("BY DIRECTION")
    print("=" * 80)

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
    print("=" * 80)
    print("BY SETUP + R:R ZONE")
    print("=" * 80)

    for setup in sorted(set(r["setup"] for r in results)):
        sub = [r for r in results if r["setup"] == setup]
        if len(sub) < 3:
            continue
        print(f"\n--- {setup} ---")
        stats(sub, f"All")
        stats([r for r in sub if r["good_rr"]], f"GOOD R:R")
        stats([r for r in sub if r["mid"]], f"MID")
        stats([r for r in sub if r["bad_rr"]], f"BAD R:R")

    # =====================================================
    print("")
    print("=" * 80)
    print("POSITION % DISTRIBUTION (10% buckets)")
    print("=" * 80)

    for lo in range(0, 100, 10):
        hi = lo + 10
        sub = [r for r in results if lo <= r["position_pct"] < hi]
        if not sub:
            continue
        n = len(sub)
        w = sum(1 for t in sub if t["is_win"])
        l = sum(1 for t in sub if "LOSS" in t["result"])
        pnl = sum(t["pnl"] for t in sub)
        wr = w / (w + l) * 100 if (w + l) else 0
        avg_dd = sum(t["max_loss"] for t in sub) / n
        bar = "#" * int(wr / 2)
        print(f"  {lo:3d}-{hi:3d}%: {n:3d}t {pnl:+8.1f}pts {wr:5.1f}%WR dd={avg_dd:+.1f} {bar}")

    # Also split by direction
    for direction in ["long", "short"]:
        is_l = direction == "long"
        print(f"\n  --- {direction.upper()} by position % ---")
        for lo in range(0, 100, 10):
            hi = lo + 10
            sub = [r for r in results if lo <= r["position_pct"] < hi and r["is_long"] == is_l]
            if not sub:
                continue
            n = len(sub)
            w = sum(1 for t in sub if t["is_win"])
            l = sum(1 for t in sub if "LOSS" in t["result"])
            pnl = sum(t["pnl"] for t in sub)
            wr = w / (w + l) * 100 if (w + l) else 0
            avg_dd = sum(t["max_loss"] for t in sub) / n
            bar = "#" * int(wr / 2)
            print(f"    {lo:3d}-{hi:3d}%: {n:3d}t {pnl:+8.1f}pts {wr:5.1f}%WR dd={avg_dd:+.1f} {bar}")

    # =====================================================
    print("")
    print("=" * 80)
    print("DIFFERENT THRESHOLDS: What cutoff works best?")
    print("=" * 80)

    for threshold in [20, 25, 30, 35, 40]:
        good_t = [r for r in results if
                  (r["is_long"] and r["position_pct"] <= threshold) or
                  (not r["is_long"] and r["position_pct"] >= (100 - threshold))]
        bad_t = [r for r in results if
                 (r["is_long"] and r["position_pct"] >= (100 - threshold)) or
                 (not r["is_long"] and r["position_pct"] <= threshold)]
        mid_t = [r for r in results if r not in good_t and r not in bad_t]

        n_good = len(good_t)
        n_bad = len(bad_t)
        pnl_good = sum(t["pnl"] for t in good_t)
        pnl_bad = sum(t["pnl"] for t in bad_t)
        wr_good = sum(1 for t in good_t if t["is_win"]) / max(1, sum(1 for t in good_t if t["is_win"] or "LOSS" in t["result"])) * 100 if good_t else 0
        wr_bad = sum(1 for t in bad_t if t["is_win"]) / max(1, sum(1 for t in bad_t if t["is_win"] or "LOSS" in t["result"])) * 100 if bad_t else 0

        print(f"  Threshold {threshold}/{100-threshold}: GOOD={n_good}t {pnl_good:+.1f}pts {wr_good:.0f}%WR | BAD={n_bad}t {pnl_bad:+.1f}pts {wr_bad:.0f}%WR | delta={pnl_good-pnl_bad:+.1f}")

    # =====================================================
    print("")
    print("=" * 80)
    print("COMBINED WITH ASYMMETRIC FILTER (Analysis #9)")
    print("=" * 80)

    # Apply our existing Option B filter + charm R:R
    def option_b_filter(r):
        """Option B asymmetric filter."""
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
    print(f"\nOption B trades with charm data: {len(optb)}")
    stats(optb, "Option B (no charm R:R filter)")

    optb_good = [r for r in optb if r["good_rr"]]
    optb_bad = [r for r in optb if r["bad_rr"]]
    optb_mid = [r for r in optb if r["mid"]]
    stats(optb_good, "Option B + GOOD R:R only")
    stats(optb_mid, "Option B + MID only")
    stats(optb_bad, "Option B + BAD R:R only")

    # Option B minus BAD R:R = "Option B + F7"
    optb_no_bad = [r for r in optb if not r["bad_rr"]]
    stats(optb_no_bad, "Option B + F7 (exclude BAD R:R)")

    # Show improvement
    pnl_b = sum(t["pnl"] for t in optb)
    pnl_b_f7 = sum(t["pnl"] for t in optb_no_bad)
    pnl_bad_removed = sum(t["pnl"] for t in optb_bad)
    print(f"\n  Option B PnL: {pnl_b:+.1f}")
    print(f"  Option B + F7: {pnl_b_f7:+.1f}")
    print(f"  BAD R:R removed: {pnl_bad_removed:+.1f} pts from {len(optb_bad)} trades")
    print(f"  Improvement: {pnl_b_f7 - pnl_b:+.1f} pts")

    # =====================================================
    print("")
    print("=" * 80)
    print("SAMPLE BAD R:R TRADES (to verify)")
    print("=" * 80)

    bad_rr_optb = [r for r in optb if r["bad_rr"]]
    for r in sorted(bad_rr_optb, key=lambda x: x["pnl"])[:15]:
        print(f"  #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} spot={r['spot']:.1f} "
              f"range=[{r['s_strike']:.0f}-{r['r_strike']:.0f}] pos={r['position_pct']:.0f}% "
              f"charm_r={r['r_pts']:.0f} charm_s={r['s_pts']:.0f} | {r['result']:8s} {r['pnl']:+.1f}")

    # =====================================================
    print("")
    print("=" * 80)
    print("SAMPLE GOOD R:R TRADES (to verify)")
    print("=" * 80)

    good_rr_optb = [r for r in optb if r["good_rr"]]
    for r in sorted(good_rr_optb, key=lambda x: x["pnl"], reverse=True)[:15]:
        print(f"  #{r['id']} {r['ts']} {r['setup']:20s} {r['dir']:8s} spot={r['spot']:.1f} "
              f"range=[{r['s_strike']:.0f}-{r['r_strike']:.0f}] pos={r['position_pct']:.0f}% "
              f"charm_r={r['r_pts']:.0f} charm_s={r['s_pts']:.0f} | {r['result']:8s} {r['pnl']:+.1f}")
