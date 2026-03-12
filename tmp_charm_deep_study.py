"""Deep study: Charm exposure near spot as trade quality filter.

Charm mechanics:
- Negative charm = BULLISH (dealers buy as options decay)
  - Below spot = SUPPORT, Above spot = MAGNET UP
- Positive charm = BEARISH (dealers sell as options decay)
  - Above spot = RESISTANCE, Below spot = MAGNET DOWN

For LONGS: want negative charm (bullish). Positive charm above = resistance = BAD.
For SHORTS: want positive charm (bearish). Negative charm below = support = BAD.
"""
import sqlalchemy as sa
import os
from collections import defaultdict
from datetime import timedelta

engine = sa.create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    trades = conn.execute(sa.text("""
        SELECT id, ts, setup_name, direction, grade, score, spot,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               greek_alignment, spot_vol_beta
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND spot IS NOT NULL
        ORDER BY ts
    """)).fetchall()

    print(f"Total trades: {len(trades)}")

    results = []
    for t in trades:
        spot = float(t.spot)
        ts = t.ts

        # Get charm exposure within ±20 pts of spot
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
            continue

        # Deduplicate: most recent snapshot only
        seen = set()
        strikes = []
        for cr in charm_rows:
            strike = float(cr.strike)
            if strike not in seen:
                seen.add(strike)
                strikes.append({"strike": strike, "value": float(cr.value)})

        # Compute charm forces
        charm_above_pos = 0  # positive charm above = RESISTANCE (bearish)
        charm_above_neg = 0  # negative charm above = MAGNET UP (bullish)
        charm_below_pos = 0  # positive charm below = MAGNET DOWN (bearish)
        charm_below_neg = 0  # negative charm below = SUPPORT (bullish)

        # Also track top strikes by abs value
        top_above = None  # most significant strike above
        top_below = None  # most significant strike below

        for s in strikes:
            if s["strike"] > spot:
                if s["value"] > 0:
                    charm_above_pos += s["value"]
                else:
                    charm_above_neg += s["value"]
                if top_above is None or abs(s["value"]) > abs(top_above["value"]):
                    top_above = s
            else:
                if s["value"] > 0:
                    charm_below_pos += s["value"]
                else:
                    charm_below_neg += s["value"]
                if top_below is None or abs(s["value"]) > abs(top_below["value"]):
                    top_below = s

        # Total forces
        bullish_force = abs(charm_above_neg) + abs(charm_below_neg)  # negative charm = bullish
        bearish_force = charm_above_pos + charm_below_pos  # positive charm = bearish
        net_charm = bearish_force - bullish_force  # positive = net bearish, negative = net bullish

        is_long = t.direction in ("long", "bullish")
        is_short = not is_long
        is_win = "WIN" in t.outcome_result
        pnl = float(t.outcome_pnl or 0)
        max_loss = float(t.outcome_max_loss or 0)
        max_profit = float(t.outcome_max_profit or 0)

        # For LONGS: charm_favor = bullish_force - bearish_force (want positive = bullish)
        # For SHORTS: charm_favor = bearish_force - bullish_force (want positive = bearish)
        if is_long:
            charm_favor = bullish_force - bearish_force
        else:
            charm_favor = bearish_force - bullish_force

        # Specific: does the trade face resistance in its direction?
        if is_long:
            # Resistance = positive charm above spot
            resistance = charm_above_pos
            support = abs(charm_below_neg)  # negative below = support
            opposing_force = charm_above_pos + charm_below_pos  # all bearish charm
        else:
            # For shorts: "resistance" = negative charm below (support blocking short)
            resistance = abs(charm_below_neg)  # support blocking short move down
            support = charm_above_pos  # positive above = resistance keeps price from recovering
            opposing_force = abs(charm_above_neg) + abs(charm_below_neg)  # all bullish charm

        # R:R using correct charm signs
        # For LONGS: find bearish resistance above and bullish support below
        if is_long and top_above and top_below:
            # Best entry: near support (negative charm below), far from resistance (positive charm above)
            r_level = None
            s_level = None
            # Find highest positive charm above (resistance)
            for s in sorted(strikes, key=lambda x: x["strike"]):
                if s["strike"] > spot and s["value"] > 0:
                    r_level = s["strike"]
                    break
            # Find highest negative charm below (support)
            for s in sorted(strikes, key=lambda x: x["strike"], reverse=True):
                if s["strike"] <= spot and s["value"] < 0:
                    s_level = s["strike"]
                    break
            if r_level and s_level and r_level - s_level >= 10:
                rng = r_level - s_level
                pos_pct = (spot - s_level) / rng * 100
            else:
                pos_pct = None
        elif is_short and top_above and top_below:
            r_level = None  # For shorts: negative charm below = "resistance" (support blocking short)
            s_level = None  # For shorts: positive charm above = "support" (resistance keeping price down)
            for s in sorted(strikes, key=lambda x: x["strike"], reverse=True):
                if s["strike"] <= spot and s["value"] < 0:
                    r_level = s["strike"]  # bullish support blocking short
                    break
            for s in sorted(strikes, key=lambda x: x["strike"]):
                if s["strike"] > spot and s["value"] > 0:
                    s_level = s["strike"]  # bearish resistance helping short
                    break
            if r_level and s_level:
                rng = spot - r_level + s_level - spot  # just for context
                # For shorts: how far from the support blocking us?
                pos_pct = (spot - r_level) / max(1, spot - r_level + 5) * 100  # approximate
            else:
                pos_pct = None
        else:
            pos_pct = None

        results.append({
            "id": t.id, "ts": str(ts)[:16], "setup": t.setup_name,
            "dir": t.direction, "spot": spot,
            "is_long": is_long, "is_win": is_win, "pnl": pnl,
            "max_loss": max_loss, "max_profit": max_profit,
            "result": t.outcome_result,
            "align": int(t.greek_alignment) if t.greek_alignment is not None else None,
            "svb": float(t.spot_vol_beta) if t.spot_vol_beta is not None else None,
            # Charm forces
            "bullish_force": bullish_force,
            "bearish_force": bearish_force,
            "net_charm": net_charm,
            "charm_favor": charm_favor,
            "resistance": resistance,
            "support": support,
            "opposing_force": opposing_force,
            "charm_above_pos": charm_above_pos,
            "charm_above_neg": charm_above_neg,
            "charm_below_pos": charm_below_pos,
            "charm_below_neg": charm_below_neg,
            "pos_pct": pos_pct,
        })

    print(f"Trades with charm data: {len(results)}")

    def stats(subset, label):
        if not subset:
            print(f"  {label:45s}: NO TRADES")
            return {}
        n = len(subset)
        w = sum(1 for t in subset if t["is_win"])
        l = sum(1 for t in subset if "LOSS" in t["result"])
        pnl = sum(t["pnl"] for t in subset)
        wr = w / (w + l) * 100 if (w + l) else 0
        avg_dd = sum(t["max_loss"] for t in subset) / n
        avg_mfe = sum(t["max_profit"] for t in subset) / n
        print(f"  {label:45s}: {n:3d}t {pnl:+8.1f}pts {wr:5.1f}%WR {w}W/{l}L dd={avg_dd:+.1f} mfe={avg_mfe:+.1f}")
        return {"n": n, "pnl": pnl, "wr": wr}

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 1: CHARM FAVOR — Does charm near spot favor the trade direction?")
    print("(charm_favor > 0 = charm supports trade, < 0 = charm opposes trade)")
    print("=" * 80)

    # Bucket by charm_favor magnitude
    for label, lo, hi in [
        ("Strong oppose (< -10M)", -1e15, -10e6),
        ("Mild oppose (-10M to 0)", -10e6, 0),
        ("Mild favor (0 to 10M)", 0, 10e6),
        ("Strong favor (> 10M)", 10e6, 1e15),
    ]:
        sub = [r for r in results if lo <= r["charm_favor"] < hi]
        stats(sub, label)

    # Split by direction
    for direction in ["LONG", "SHORT"]:
        is_l = direction == "LONG"
        print(f"\n  --- {direction} ---")
        sub_dir = [r for r in results if r["is_long"] == is_l]
        for label, lo, hi in [
            ("Strong oppose (< -10M)", -1e15, -10e6),
            ("Mild oppose (-10M to 0)", -10e6, 0),
            ("Mild favor (0 to 10M)", 0, 10e6),
            ("Strong favor (> 10M)", 10e6, 1e15),
        ]:
            sub = [r for r in sub_dir if lo <= r["charm_favor"] < hi]
            stats(sub, f"  {label}")

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 2: OPPOSING FORCE — How much charm opposes the trade?")
    print("(For longs: positive charm near spot. For shorts: negative charm near spot)")
    print("=" * 80)

    for label, lo, hi in [
        ("Low opposition (< 1M)", 0, 1e6),
        ("Medium opposition (1-10M)", 1e6, 10e6),
        ("High opposition (10-50M)", 10e6, 50e6),
        ("Very high opposition (50-200M)", 50e6, 200e6),
        ("Extreme opposition (> 200M)", 200e6, 1e15),
    ]:
        sub = [r for r in results if lo <= r["opposing_force"] < hi]
        stats(sub, label)

    for direction in ["LONG", "SHORT"]:
        is_l = direction == "LONG"
        print(f"\n  --- {direction} ---")
        sub_dir = [r for r in results if r["is_long"] == is_l]
        for label, lo, hi in [
            ("Low opposition (< 1M)", 0, 1e6),
            ("Medium (1-10M)", 1e6, 10e6),
            ("High (10-50M)", 10e6, 50e6),
            ("Very high (50-200M)", 50e6, 200e6),
            ("Extreme (> 200M)", 200e6, 1e15),
        ]:
            sub = [r for r in sub_dir if lo <= r["opposing_force"] < hi]
            stats(sub, f"  {label}")

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 3: RESISTANCE IN TRADE DIRECTION")
    print("For LONGS: positive charm ABOVE spot (resistance blocking upside)")
    print("For SHORTS: negative charm BELOW spot (support blocking downside)")
    print("=" * 80)

    for label, lo, hi in [
        ("No resistance (0)", 0, 1),
        ("Tiny (< 500K)", 1, 500000),
        ("Small (500K-5M)", 500000, 5e6),
        ("Medium (5-20M)", 5e6, 20e6),
        ("Large (20-100M)", 20e6, 100e6),
        ("Huge (> 100M)", 100e6, 1e15),
    ]:
        sub = [r for r in results if lo <= r["resistance"] < hi]
        stats(sub, label)

    for direction in ["LONG", "SHORT"]:
        is_l = direction == "LONG"
        print(f"\n  --- {direction} ---")
        sub_dir = [r for r in results if r["is_long"] == is_l]
        for label, lo, hi in [
            ("No resistance (0)", 0, 1),
            ("Tiny (< 500K)", 1, 500000),
            ("Small (500K-5M)", 500000, 5e6),
            ("Medium (5-20M)", 5e6, 20e6),
            ("Large (20-100M)", 20e6, 100e6),
            ("Huge (> 100M)", 100e6, 1e15),
        ]:
            sub = [r for r in sub_dir if lo <= r["resistance"] < hi]
            stats(sub, f"  {label}")

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 4: LONG-SPECIFIC — Positive charm above spot (resistance)")
    print("Does positive charm above spot predict long failures?")
    print("=" * 80)

    longs = [r for r in results if r["is_long"]]
    for label, lo, hi in [
        ("No pos charm above (0)", 0, 1),
        ("Tiny (< 500K)", 1, 500000),
        ("Small (500K-2M)", 500000, 2e6),
        ("Medium (2-10M)", 2e6, 10e6),
        ("Large (10-50M)", 10e6, 50e6),
        ("Huge (> 50M)", 50e6, 1e15),
    ]:
        sub = [r for r in longs if lo <= r["charm_above_pos"] < hi]
        stats(sub, label)

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 5: SHORT-SPECIFIC — Negative charm below spot (support blocking)")
    print("Does negative charm below spot predict short failures?")
    print("=" * 80)

    shorts = [r for r in results if not r["is_long"]]
    for label, lo, hi in [
        ("No neg charm below (0)", 0, 1),
        ("Tiny (< 500K)", 1, 500000),
        ("Small (500K-2M)", 500000, 2e6),
        ("Medium (2-10M)", 2e6, 10e6),
        ("Large (10-50M)", 10e6, 50e6),
        ("Huge (> 50M)", 50e6, 1e15),
    ]:
        sub = [r for r in shorts if lo <= abs(r["charm_below_neg"]) < hi]
        stats(sub, label)

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 6: NET CHARM — Positive=bearish, Negative=bullish")
    print("Should longs fire in bullish charm? Shorts in bearish charm?")
    print("=" * 80)

    for direction in ["LONG", "SHORT"]:
        is_l = direction == "LONG"
        sub_dir = [r for r in results if r["is_long"] == is_l]
        print(f"\n  --- {direction} ---")
        for label, lo, hi in [
            ("Strong bullish (< -50M)", -1e15, -50e6),
            ("Mild bullish (-50M to -5M)", -50e6, -5e6),
            ("Neutral (-5M to 5M)", -5e6, 5e6),
            ("Mild bearish (5M to 50M)", 5e6, 50e6),
            ("Strong bearish (> 50M)", 50e6, 1e15),
        ]:
            sub = [r for r in sub_dir if lo <= r["net_charm"] < hi]
            stats(sub, f"  {label}")

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 7: PER-SETUP — Charm favor impact")
    print("=" * 80)

    for setup in sorted(set(r["setup"] for r in results)):
        sub_setup = [r for r in results if r["setup"] == setup]
        if len(sub_setup) < 10:
            continue
        print(f"\n  --- {setup} ---")
        favor = [r for r in sub_setup if r["charm_favor"] > 0]
        oppose = [r for r in sub_setup if r["charm_favor"] <= 0]
        stats(favor, f"  Charm FAVORS trade")
        stats(oppose, f"  Charm OPPOSES trade")

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 8: COMBINED WITH OPTION B — Can charm improve it?")
    print("=" * 80)

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
    print(f"\nOption B: {len(optb)} trades")
    stats(optb, "Option B baseline")

    # Test: Option B + charm favor > 0
    stats([r for r in optb if r["charm_favor"] > 0], "Option B + charm favors")
    stats([r for r in optb if r["charm_favor"] <= 0], "Option B + charm opposes")

    # Test: Option B + block high resistance
    for thresh in [5e6, 10e6, 20e6, 50e6]:
        sub = [r for r in optb if r["resistance"] < thresh]
        blocked = [r for r in optb if r["resistance"] >= thresh]
        s = stats(sub, f"Opt B + resistance < {thresh/1e6:.0f}M (kept)")
        b = stats(blocked, f"  (blocked)")

    # Test: Option B + charm favor > 0 + low resistance
    print("\n  --- Combining charm favor + resistance ---")
    combo1 = [r for r in optb if r["charm_favor"] > 0 and r["resistance"] < 20e6]
    combo2 = [r for r in optb if r["charm_favor"] > 0 or r["resistance"] < 5e6]
    stats(combo1, "Opt B + favor>0 AND resist<20M")
    stats(combo2, "Opt B + favor>0 OR resist<5M")

    # Test: just block when charm strongly opposes
    for thresh in [5e6, 10e6, 20e6, 50e6]:
        sub = [r for r in optb if r["charm_favor"] > -thresh]
        blocked = [r for r in optb if r["charm_favor"] <= -thresh]
        s = stats(sub, f"Opt B + charm_favor > -{thresh/1e6:.0f}M (kept)")
        b = stats(blocked, f"  (blocked)")

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 9: OPTION B PER-SETUP — Where does charm filter help/hurt?")
    print("=" * 80)

    for setup in sorted(set(r["setup"] for r in optb)):
        sub_setup = [r for r in optb if r["setup"] == setup]
        if len(sub_setup) < 5:
            continue
        print(f"\n  --- {setup} ({len(sub_setup)} trades in Opt B) ---")
        stats(sub_setup, f"All")
        stats([r for r in sub_setup if r["charm_favor"] > 0], f"Charm favors")
        stats([r for r in sub_setup if r["charm_favor"] <= 0], f"Charm opposes")
        stats([r for r in sub_setup if r["resistance"] < 10e6], f"Low resistance (<10M)")
        stats([r for r in sub_setup if r["resistance"] >= 10e6], f"High resistance (>=10M)")

    # =====================================================
    print("\n" + "=" * 80)
    print("TEST 10: OPTIMAL CHARM FILTER — What single rule helps most?")
    print("=" * 80)

    print("\n  Testing: Block when charm_favor < threshold")
    best_rule = None
    best_improvement = -999
    for thresh in [-100e6, -50e6, -20e6, -10e6, -5e6, 0, 5e6, 10e6]:
        kept = [r for r in optb if r["charm_favor"] > thresh]
        blocked = [r for r in optb if r["charm_favor"] <= thresh]
        pnl_kept = sum(t["pnl"] for t in kept)
        pnl_blocked = sum(t["pnl"] for t in blocked)
        n_kept = len(kept)
        n_blocked = len(blocked)
        w_kept = sum(1 for t in kept if t["is_win"])
        l_kept = sum(1 for t in kept if "LOSS" in t["result"])
        wr_kept = w_kept / (w_kept + l_kept) * 100 if (w_kept + l_kept) else 0
        w_blk = sum(1 for t in blocked if t["is_win"])
        l_blk = sum(1 for t in blocked if "LOSS" in t["result"])
        wr_blk = w_blk / (w_blk + l_blk) * 100 if (w_blk + l_blk) else 0

        # Max drawdown
        cum = 0; peak = 0; dd = 0
        for t in sorted(kept, key=lambda x: x["id"]):
            cum += t["pnl"]
            if cum > peak: peak = cum
            if peak - cum > dd: dd = peak - cum

        print(f"  favor > {thresh/1e6:+6.0f}M: kept={n_kept:3d}t {pnl_kept:+8.1f}pts {wr_kept:5.1f}%WR DD={dd:.0f} | blocked={n_blocked:3d}t {pnl_blocked:+8.1f}pts {wr_blk:5.1f}%WR")

    print("\n  Testing: Block when resistance > threshold")
    for thresh in [1e6, 5e6, 10e6, 20e6, 50e6, 100e6]:
        kept = [r for r in optb if r["resistance"] < thresh]
        blocked = [r for r in optb if r["resistance"] >= thresh]
        pnl_kept = sum(t["pnl"] for t in kept)
        pnl_blocked = sum(t["pnl"] for t in blocked)
        n_kept = len(kept)
        n_blocked = len(blocked)
        w_kept = sum(1 for t in kept if t["is_win"])
        l_kept = sum(1 for t in kept if "LOSS" in t["result"])
        wr_kept = w_kept / (w_kept + l_kept) * 100 if (w_kept + l_kept) else 0

        cum = 0; peak = 0; dd = 0
        for t in sorted(kept, key=lambda x: x["id"]):
            cum += t["pnl"]
            if cum > peak: peak = cum
            if peak - cum > dd: dd = peak - cum

        print(f"  resist < {thresh/1e6:+6.0f}M: kept={n_kept:3d}t {pnl_kept:+8.1f}pts {wr_kept:5.1f}%WR DD={dd:.0f} | blocked={n_blocked:3d}t {pnl_blocked:+8.1f}pts")

    print("\n  Testing: Block when opposing_force > threshold")
    for thresh in [5e6, 10e6, 20e6, 50e6, 100e6, 200e6]:
        kept = [r for r in optb if r["opposing_force"] < thresh]
        blocked = [r for r in optb if r["opposing_force"] >= thresh]
        pnl_kept = sum(t["pnl"] for t in kept)
        pnl_blocked = sum(t["pnl"] for t in blocked)
        n_kept = len(kept)
        n_blocked = len(blocked)
        w_kept = sum(1 for t in kept if t["is_win"])
        l_kept = sum(1 for t in kept if "LOSS" in t["result"])
        wr_kept = w_kept / (w_kept + l_kept) * 100 if (w_kept + l_kept) else 0

        cum = 0; peak = 0; dd = 0
        for t in sorted(kept, key=lambda x: x["id"]):
            cum += t["pnl"]
            if cum > peak: peak = cum
            if peak - cum > dd: dd = peak - cum

        print(f"  oppose < {thresh/1e6:+6.0f}M: kept={n_kept:3d}t {pnl_kept:+8.1f}pts {wr_kept:5.1f}%WR DD={dd:.0f} | blocked={n_blocked:3d}t {pnl_blocked:+8.1f}pts")
