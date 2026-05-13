"""
Track E — Charm-Driven Edge Analysis

Pipeline:
1. Pull all resolved setup_log trades since 2026-03-01
2. For each trade, fetch the closest pre-signal charm snapshot (per-strike)
3. Compute charm features (wall distance, symmetry, gradient, magnitude near LIS, etc.)
4. Save features JSON
5. Test 8 hypotheses with bootstrap CI + OOS halves
6. Render dark-theme HTML report

Run: python _tmp_track_e_charm_edges.py
"""
import json, math, time, random, os
from datetime import datetime, timezone
from collections import defaultdict, Counter

import numpy as np
from sqlalchemy import create_engine, text

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START_DATE = "2026-03-01"
TICKER = "SPX"
FEATURES_JSON = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_charm_features.json"

random.seed(0)
np.random.seed(0)

eng = create_engine(DB)


def fetch_trades():
    """Pull all resolved trades with key context columns."""
    with eng.connect() as c:
        rs = c.execute(text(f"""
            SELECT
                id, ts, setup_name, direction, grade, score, paradigm, spot, lis, target,
                vix, vix3m, vix_vix3m_ratio, overvix,
                greek_alignment, vanna_all, vanna_weekly, vanna_monthly,
                charm_limit_entry,
                vanna_cliff_side, vanna_peak_side, vanna_regime,
                v13_gex_above, v13_dd_near,
                trail_sl, trail_activation, trail_gap,
                outcome_result, outcome_pnl,
                outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
                first_hour
            FROM setup_log
            WHERE ts::date >= '{START_DATE}'
              AND outcome_result IS NOT NULL
              AND setup_name IN ('Skew Charm', 'AG Short', 'DD Exhaustion',
                                 'GEX Long', 'GEX Velocity', 'ES Absorption',
                                 'Paradigm Reversal', 'BofA Scalp',
                                 'Vanna Pivot Bounce', 'VIX Divergence',
                                 'SB Absorption', 'SB2 Absorption',
                                 'SB10 Absorption', 'Delta Absorption')
            ORDER BY ts ASC
        """))
        cols = list(rs.keys())
        rows = [dict(zip(cols, r)) for r in rs]
    return rows


def fetch_charm_snapshots_for_trades(trades):
    """Bulk-fetch ALL charm snapshot timestamps + per-strike data, then map in memory.

    Step 1: Fetch all distinct charm snapshot timestamps since start date
    Step 2: For each trade ts, find nearest snapshot ts (in memory) within 10 min lookback
    Step 3: Bulk-fetch per-strike data only for the unique snapshot timestamps needed
    """
    print(f"  step 1: fetching all charm snapshot timestamps...", flush=True)
    with eng.connect() as c:
        rs = c.execute(text(f"""
            SELECT DISTINCT ts_utc FROM volland_exposure_points
            WHERE LOWER(greek)='charm' AND ticker=:tk
              AND ts_utc::date >= '{START_DATE}'
            ORDER BY ts_utc
        """), {"tk": TICKER})
        snap_ts_list = sorted([r[0] for r in rs])
    print(f"    {len(snap_ts_list)} distinct snapshot timestamps", flush=True)

    # Convert to numpy for fast lookup
    import bisect
    print(f"  step 2: mapping each trade ts -> nearest snapshot ts (in memory)...", flush=True)
    ts_list = sorted({t["ts"] for t in trades})
    trade_to_snap = {}  # trade_ts -> snap_ts (or None)
    from datetime import timedelta
    cutoff = timedelta(minutes=10)
    for trade_ts in ts_list:
        # Find latest snap_ts <= trade_ts (bisect_right - 1)
        idx = bisect.bisect_right(snap_ts_list, trade_ts) - 1
        if idx < 0:
            continue
        snap_ts = snap_ts_list[idx]
        if trade_ts - snap_ts > cutoff:
            continue
        trade_to_snap[trade_ts] = snap_ts
    print(f"    {len(trade_to_snap)}/{len(ts_list)} trades have nearest snapshot within 10 min", flush=True)

    # Unique snapshot timestamps that are actually needed
    needed_snaps = sorted(set(trade_to_snap.values()))
    print(f"  step 3: bulk-fetching {len(needed_snaps)} unique snapshots (batches of 100)...", flush=True)

    snap_data = {}  # snap_ts -> list of (strike, value, current_price)
    batch_size = 100
    with eng.connect() as c:
        for i in range(0, len(needed_snaps), batch_size):
            batch = needed_snaps[i:i+batch_size]
            rs = c.execute(text("""
                SELECT ts_utc, strike, value, current_price
                FROM volland_exposure_points
                WHERE LOWER(greek)='charm' AND ticker=:tk
                  AND ts_utc = ANY(:ts_list)
                ORDER BY ts_utc, strike
            """), {"tk": TICKER, "ts_list": batch})
            for r in rs:
                snap_ts = r[0]
                if snap_ts not in snap_data:
                    snap_data[snap_ts] = []
                snap_data[snap_ts].append((
                    float(r[1]),
                    float(r[2]) if r[2] is not None else 0.0,
                    float(r[3]) if r[3] is not None else None,
                    snap_ts,
                ))
            print(f"    batch {i//batch_size + 1}/{(len(needed_snaps)+batch_size-1)//batch_size}: "
                  f"snap_data has {len(snap_data)} keys", flush=True)

    # Map trades -> charm rows
    snapshots = {}
    for trade_ts, snap_ts in trade_to_snap.items():
        snapshots[trade_ts] = snap_data.get(snap_ts, [])
    print(f"  Done: {len(snapshots)} trade timestamps mapped", flush=True)
    return snapshots


def compute_charm_features(trade, charm_rows):
    """Compute charm features for one trade.

    Volland charm convention:
      negative value = bullish hedging (dealers must buy)
      positive value = bearish hedging (dealers must sell)
    """
    if not charm_rows:
        return None
    spot = trade.get("spot")
    if spot is None or spot <= 0:
        return None
    direction = (trade.get("direction") or "").lower()
    # Normalize bullish/long, bearish/short
    is_long = direction in ("long", "bullish")
    is_short = direction in ("short", "bearish")
    if not (is_long or is_short):
        return None

    strikes = np.array([r[0] for r in charm_rows], dtype=float)
    values = np.array([r[1] for r in charm_rows], dtype=float)
    snap_price = None
    for r in charm_rows:
        if r[2] is not None:
            snap_price = r[2]
            break
    if snap_price is None:
        snap_price = spot

    # Use snap_price for distance calcs (charm rows are anchored to that price)
    px = snap_price

    # Filter to strikes within ±100 pts of spot to focus on actionable range
    mask = np.abs(strikes - px) <= 100
    s = strikes[mask]
    v = values[mask]
    if len(s) < 10:
        return None

    # 1) Charm at nearest strike to spot
    near_idx = int(np.argmin(np.abs(s - px)))
    charm_at_spot = float(v[near_idx])

    # 2) Sums above/below within bands
    def sum_band(low, high, sign_filter=None):
        # Sum of strikes where (s - px) in [low, high]
        m = (s - px > low) & (s - px <= high)
        if sign_filter == "pos":
            m = m & (v > 0)
        elif sign_filter == "neg":
            m = m & (v < 0)
        return float(np.sum(v[m]))

    feat = {
        "charm_at_spot": charm_at_spot,
        "snap_price": float(px),
        "spot": float(spot),
    }
    # Bands above spot (positive = bearish per Volland convention)
    for lo, hi, label in [(0, 5, "0_5"), (0, 10, "0_10"), (0, 15, "0_15"), (0, 30, "0_30"), (5, 15, "5_15"), (15, 30, "15_30")]:
        feat[f"charm_above_{label}"] = sum_band(lo, hi)
    # Bands below spot
    for lo, hi, label in [(-5, 0, "0_5"), (-10, 0, "0_10"), (-15, 0, "0_15"), (-30, 0, "0_30"), (-15, -5, "5_15"), (-30, -15, "15_30")]:
        feat[f"charm_below_{label}"] = sum_band(lo, hi)

    # 3) Net total charm in ±30
    feat["charm_net_30"] = sum_band(-30, 30)
    feat["charm_abs_above_30"] = float(np.sum(np.abs(v[(s - px > 0) & (s - px <= 30)])))
    feat["charm_abs_below_30"] = float(np.sum(np.abs(v[(s - px < 0) & (s - px >= -30)])))

    # 4) Dominant strike (max |value|)
    abs_v = np.abs(v)
    if abs_v.max() > 0:
        dom_idx = int(np.argmax(abs_v))
        feat["charm_max_strike"] = float(s[dom_idx])
        feat["charm_max_value"] = float(v[dom_idx])
        feat["charm_max_distance"] = float(s[dom_idx] - px)  # signed
        feat["charm_max_abs"] = float(abs_v[dom_idx])
    else:
        feat["charm_max_strike"] = None
        feat["charm_max_value"] = 0.0
        feat["charm_max_distance"] = None
        feat["charm_max_abs"] = 0.0

    # 5) Charm wall: where charm changes sign closest to spot
    # walk strikes outward from spot in each direction, find first sign change with magnitude > 50M
    THRESHOLD = 50_000_000  # 50M minimum to count as a "wall"
    wall_above_dist = None  # distance to first significant sign-change ABOVE spot
    wall_above_sign = None
    wall_below_dist = None
    wall_below_sign = None

    # Above spot: look for strikes above with significant sign change vs the strike before
    above_mask = s - px > 0
    if above_mask.any():
        s_above = s[above_mask]
        v_above = v[above_mask]
        # Sort by distance from spot
        order = np.argsort(s_above)
        s_a = s_above[order]
        v_a = v_above[order]
        prev_sign = np.sign(charm_at_spot) if charm_at_spot != 0 else 0
        for i in range(len(s_a)):
            sgn = np.sign(v_a[i])
            if abs(v_a[i]) >= THRESHOLD and sgn != 0 and prev_sign != 0 and sgn != prev_sign:
                wall_above_dist = float(s_a[i] - px)
                wall_above_sign = int(sgn)
                break
            if abs(v_a[i]) >= THRESHOLD:
                prev_sign = sgn
    # Below spot
    below_mask = s - px < 0
    if below_mask.any():
        s_below = s[below_mask]
        v_below = v[below_mask]
        order = np.argsort(-s_below)  # closest to spot first (descending)
        s_b = s_below[order]
        v_b = v_below[order]
        prev_sign = np.sign(charm_at_spot) if charm_at_spot != 0 else 0
        for i in range(len(s_b)):
            sgn = np.sign(v_b[i])
            if abs(v_b[i]) >= THRESHOLD and sgn != 0 and prev_sign != 0 and sgn != prev_sign:
                wall_below_dist = float(px - s_b[i])
                wall_below_sign = int(sgn)
                break
            if abs(v_b[i]) >= THRESHOLD:
                prev_sign = sgn

    feat["charm_wall_above_dist"] = wall_above_dist
    feat["charm_wall_above_sign"] = wall_above_sign
    feat["charm_wall_below_dist"] = wall_below_dist
    feat["charm_wall_below_sign"] = wall_below_sign

    # 6) Charm gradient: slope of charm vs strike within ±15 pts (linear regression)
    grad_mask = np.abs(s - px) <= 15
    if grad_mask.sum() >= 5:
        x_gr = s[grad_mask] - px
        y_gr = v[grad_mask]
        # slope per 1pt of strike
        slope = float(np.polyfit(x_gr, y_gr, 1)[0])
        feat["charm_gradient_15"] = slope
    else:
        feat["charm_gradient_15"] = 0.0

    # 7) Symmetry: |above_15| / |below_15|
    ab = feat["charm_abs_above_30"]
    bel = feat["charm_abs_below_30"]
    if ab + bel > 0:
        feat["charm_symmetry"] = float((ab - bel) / (ab + bel))  # +1 = all above, -1 = all below
    else:
        feat["charm_symmetry"] = 0.0

    # 8) Charm at LIS (interpolated)
    lis = trade.get("lis")
    if lis is not None and lis > 0:
        # find nearest strike to LIS within range
        dists_to_lis = np.abs(s - lis)
        if dists_to_lis.min() <= 25:
            lis_idx = int(np.argmin(dists_to_lis))
            feat["charm_at_lis"] = float(v[lis_idx])
            feat["lis_distance_signed"] = float(lis - px)
        else:
            feat["charm_at_lis"] = None
            feat["lis_distance_signed"] = float(lis - px)
    else:
        feat["charm_at_lis"] = None
        feat["lis_distance_signed"] = None

    # 9) Time of day (ET hour, decimal) — proxy for charm potency
    ts = trade["ts"]
    # ts is UTC; ET in May is UTC-4
    et_hour = (ts.hour - 4) + ts.minute / 60.0
    if et_hour < 0:
        et_hour += 24
    feat["et_hour"] = float(et_hour)

    # 10) Direction-relative wall distances (the meaningful version!)
    # For LONGS: wall_below is SUPPORT (good), wall_above is RESISTANCE (bad if close)
    # For SHORTS: wall_above is RESISTANCE (good), wall_below is SUPPORT (bad if close)
    if is_long:
        feat["wall_with_dir_dist"] = wall_below_dist  # support
        feat["wall_against_dir_dist"] = wall_above_dist  # resistance
        feat["wall_with_dir_sign"] = wall_below_sign
        feat["wall_against_dir_sign"] = wall_above_sign
    else:
        feat["wall_with_dir_dist"] = wall_above_dist  # resistance
        feat["wall_against_dir_dist"] = wall_below_dist  # support
        feat["wall_with_dir_sign"] = wall_above_sign
        feat["wall_against_dir_sign"] = wall_below_sign

    # 11) Direction-relative charm bias
    # For longs: want NEGATIVE charm (bullish hedging) — measure net_30 with sign flipped if short
    if is_long:
        feat["charm_bias_with_dir"] = -feat["charm_net_30"]  # negative net = bullish = good for long
    else:
        feat["charm_bias_with_dir"] = feat["charm_net_30"]  # positive net = bearish = good for short

    # 12) Charm at spot relative to direction (negative = bullish, so longs prefer negative)
    if is_long:
        feat["charm_at_spot_with_dir"] = -charm_at_spot  # high = good for long
    else:
        feat["charm_at_spot_with_dir"] = charm_at_spot

    return feat


def bootstrap_ci(values, n_boot=2000, alpha=0.05):
    """Bootstrap 95% CI for the mean."""
    if not values:
        return (0.0, 0.0, 0.0)
    arr = np.array(values, dtype=float)
    n = len(arr)
    if n < 5:
        return (float(np.mean(arr)), float(np.mean(arr)), float(np.mean(arr)))
    boots = np.array([np.mean(arr[np.random.randint(0, n, n)]) for _ in range(n_boot)])
    lo = float(np.percentile(boots, 100 * alpha / 2))
    hi = float(np.percentile(boots, 100 * (1 - alpha / 2)))
    return (float(np.mean(arr)), lo, hi)


def hypothesis_test(records, pos_filter, name, baseline_records=None):
    """Test a hypothesis: filter records by predicate, return summary with CI."""
    pos = [r for r in records if pos_filter(r)]
    neg = [r for r in records if not pos_filter(r)]
    pos_pnl = [r["pnl"] for r in pos]
    neg_pnl = [r["pnl"] for r in neg]
    pos_wins = sum(1 for r in pos if r["pnl"] > 0)
    neg_wins = sum(1 for r in neg if r["pnl"] > 0)

    def wr(records_list):
        wins = sum(1 for r in records_list if r["pnl"] > 0)
        return wins / len(records_list) if records_list else 0

    pos_mean, pos_lo, pos_hi = bootstrap_ci(pos_pnl)
    neg_mean, neg_lo, neg_hi = bootstrap_ci(neg_pnl)

    # Difference bootstrap
    if pos_pnl and neg_pnl:
        diffs = []
        for _ in range(2000):
            a = np.array(pos_pnl)[np.random.randint(0, len(pos_pnl), len(pos_pnl))]
            b = np.array(neg_pnl)[np.random.randint(0, len(neg_pnl), len(neg_pnl))]
            diffs.append(a.mean() - b.mean())
        diff_lo = float(np.percentile(diffs, 2.5))
        diff_hi = float(np.percentile(diffs, 97.5))
        diff_mean = float(np.mean(diffs))
    else:
        diff_mean = diff_lo = diff_hi = 0.0

    return {
        "name": name,
        "n_pos": len(pos),
        "n_neg": len(neg),
        "wr_pos": wr(pos),
        "wr_neg": wr(neg),
        "pnl_pos_total": sum(pos_pnl),
        "pnl_neg_total": sum(neg_pnl),
        "pnl_pos_mean": pos_mean,
        "pnl_pos_ci": (pos_lo, pos_hi),
        "pnl_neg_mean": neg_mean,
        "pnl_neg_ci": (neg_lo, neg_hi),
        "diff_mean": diff_mean,
        "diff_ci": (diff_lo, diff_hi),
        "ci_clears_zero": diff_lo > 0 or diff_hi < 0,
    }


def main():
    print("=" * 80)
    print("TRACK E — Charm-Driven Edge Analysis")
    print("=" * 80)

    t0 = time.time()
    print(f"\n[1] Fetching trades since {START_DATE}...")
    trades = fetch_trades()
    print(f"  {len(trades)} resolved trades")

    print(f"\n[2] Fetching charm snapshots...")
    snapshots = fetch_charm_snapshots_for_trades(trades)
    print(f"  {len(snapshots)} unique snapshot lookups")
    # Stats
    with_snap = sum(1 for t in trades if snapshots.get(t["ts"]))
    print(f"  {with_snap}/{len(trades)} trades have charm snapshots within 10 min")

    print(f"\n[3] Computing features for each trade...")
    feature_records = []
    skipped = 0
    for tr in trades:
        rows = snapshots.get(tr["ts"], [])
        feat = compute_charm_features(tr, rows)
        if feat is None:
            skipped += 1
            continue
        # Merge trade context
        record = {
            "trade_id": tr["id"],
            "ts": tr["ts"].isoformat() if tr["ts"] else None,
            "setup": tr["setup_name"],
            "direction": tr["direction"],
            "grade": tr["grade"],
            "score": tr["score"],
            "paradigm": tr["paradigm"],
            "spot": tr["spot"],
            "lis": tr["lis"],
            "target": tr["target"],
            "vix": tr["vix"],
            "alignment": tr["greek_alignment"],
            "outcome": tr["outcome_result"],
            "pnl": tr["outcome_pnl"] if tr["outcome_pnl"] is not None else 0.0,
            "max_profit": tr["outcome_max_profit"],
            "max_loss": tr["outcome_max_loss"],
            "elapsed_min": tr["outcome_elapsed_min"],
            "charm_features": feat,
        }
        feature_records.append(record)

    print(f"  Computed features for {len(feature_records)} trades ({skipped} skipped — no charm)")

    # Save JSON
    print(f"\n[4] Saving features to {FEATURES_JSON}...")
    with open(FEATURES_JSON, "w") as f:
        json.dump(feature_records, f, default=str, indent=None)
    print(f"  saved {os.path.getsize(FEATURES_JSON)} bytes")

    # ==============================================================
    # Hypothesis testing
    # ==============================================================
    print(f"\n[5] Hypothesis testing...")
    # Flatten records for easier access
    recs = []
    for r in feature_records:
        flat = {
            "trade_id": r["trade_id"], "ts": r["ts"], "setup": r["setup"],
            "direction": r["direction"], "grade": r["grade"], "paradigm": r["paradigm"],
            "alignment": r["alignment"], "pnl": r["pnl"], "vix": r["vix"],
            "max_profit": r["max_profit"], "max_loss": r["max_loss"],
            "spot": r["spot"],
        }
        flat.update(r["charm_features"])
        # is_long flag
        flat["is_long"] = r["direction"].lower() in ("long", "bullish")
        flat["is_short"] = r["direction"].lower() in ("short", "bearish")
        recs.append(flat)

    print(f"\nTotal records: {len(recs)}")
    print(f"Overall WR: {sum(1 for r in recs if r['pnl']>0)/len(recs):.1%}")
    print(f"Overall PnL: {sum(r['pnl'] for r in recs):.1f} pts")
    print(f"Bull/Long records: {sum(1 for r in recs if r['is_long'])}")
    print(f"Bear/Short records: {sum(1 for r in recs if r['is_short'])}")

    # Filter to active V14 setups for realistic policy testing
    LIVE_SETUPS = ("Skew Charm", "AG Short", "DD Exhaustion", "GEX Long",
                   "ES Absorption", "VIX Divergence", "Paradigm Reversal",
                   "Vanna Pivot Bounce", "BofA Scalp")
    live = [r for r in recs if r["setup"] in LIVE_SETUPS]
    print(f"Live-relevant records: {len(live)}")

    # Build OOS halves (sort by ts, split 50/50)
    live_sorted = sorted(live, key=lambda r: r["ts"])
    half = len(live_sorted) // 2
    is_half = live_sorted[:half]
    oos_half = live_sorted[half:]
    print(f"IS half: {len(is_half)} (until {is_half[-1]['ts'][:10]})")
    print(f"OOS half: {len(oos_half)} (from {oos_half[0]['ts'][:10]})")

    hypotheses = []

    # H1: SHORTS work best when wall_above (resistance) is close (5-15 pts)
    def h1_filter(r):
        if not r["is_short"]:
            return False
        d = r.get("charm_wall_above_dist")
        return d is not None and 5 <= d <= 15
    hypotheses.append(("H1_shorts_wall_above_5_15", h1_filter,
                       "Shorts with charm wall above spot in 5-15pt range"))

    # H2: LONGS work best when wall_below (support) is close (5-15 pts)
    def h2_filter(r):
        if not r["is_long"]:
            return False
        d = r.get("charm_wall_below_dist")
        return d is not None and 5 <= d <= 15
    hypotheses.append(("H2_longs_wall_below_5_15", h2_filter,
                       "Longs with charm wall below spot in 5-15pt range"))

    # H3: Charm symmetric range (|symmetry| < 0.2) for scalps; trend setups fail
    def h3_filter(r):
        return abs(r.get("charm_symmetry", 0)) < 0.2
    hypotheses.append(("H3_symmetric_charm", h3_filter,
                       "Trades when charm is symmetric (|sym|<0.2)"))

    # H4: After 14:00 ET (charm potency dominant) — setups more directional
    def h4_filter(r):
        return r.get("et_hour", 12) >= 14.0
    hypotheses.append(("H4_after_14_ET", h4_filter,
                       "Trades after 14:00 ET (dealer o'clock)"))

    # H5: Strong LIS — high |charm_at_lis| (>200M)
    def h5_filter(r):
        cl = r.get("charm_at_lis")
        return cl is not None and abs(cl) >= 200_000_000
    hypotheses.append(("H5_strong_lis_charm", h5_filter,
                       "Trades when charm at LIS strike >= 200M (strong LIS)"))

    # H6: Charm gradient steep in trade direction (longs prefer negative slope, shorts prefer positive)
    # Volland: negative charm = bullish. Steep negative slope across strikes = strong bullish bias
    def h6_filter(r):
        gr = r.get("charm_gradient_15", 0)
        if r["is_long"]:
            return gr < -1_000_000  # negative slope = increasing bullish charm to the upside
        else:
            return gr > 1_000_000  # positive slope = increasing bearish charm to the upside
    hypotheses.append(("H6_gradient_with_dir", h6_filter,
                       "Trades when charm gradient aligns with trade direction"))

    # H7: charm at spot near zero (|<25M|) — charm-neutral entry
    def h7_filter(r):
        return abs(r.get("charm_at_spot", 0)) < 25_000_000
    hypotheses.append(("H7_charm_neutral_spot", h7_filter,
                       "Trades when charm at spot strike is near-zero (<25M)"))

    # H8: LONGS with charm wall ABOVE spot within 3-5 pts = BAD (resistance traps)
    def h8_filter(r):
        if not r["is_long"]:
            return False
        d = r.get("charm_wall_above_dist")
        return d is not None and d <= 5
    hypotheses.append(("H8_longs_resistance_close", h8_filter,
                       "Longs with charm RESISTANCE wall within 5 pts above spot"))

    # H9 — bonus: charm_bias_with_dir is strongly favorable (>1B)
    def h9_filter(r):
        return r.get("charm_bias_with_dir", 0) > 1_000_000_000
    hypotheses.append(("H9_strong_with_dir_charm", h9_filter,
                       "Trades where net charm in ±30 is strongly with direction (>1B)"))

    # H10 — bonus: wall_against_dir very close (<5 pts) = BAD
    def h10_filter(r):
        d = r.get("wall_against_dir_dist")
        return d is not None and d <= 5
    hypotheses.append(("H10_against_wall_close", h10_filter,
                       "Trades where wall AGAINST trade direction is within 5 pts"))

    print(f"\n{'-' * 80}")
    print(f"{'HYPOTHESIS':50s} {'POS-WR':>7s} {'NEG-WR':>7s} {'POS-N':>5s} {'NEG-N':>5s} {'DIFF-MEAN':>10s} {'CI':>20s}")
    print("-" * 80)
    results = []
    for hid, hfilter, hdesc in hypotheses:
        res = hypothesis_test(live, hfilter, hid)
        res["desc"] = hdesc
        # OOS
        res_is = hypothesis_test(is_half, hfilter, hid + "_IS")
        res_oos = hypothesis_test(oos_half, hfilter, hid + "_OOS")
        res["is_diff"] = res_is["diff_mean"]
        res["oos_diff"] = res_oos["diff_mean"]
        res["oos_clears_zero"] = res_oos["ci_clears_zero"]
        results.append(res)
        ci_str = f"[{res['diff_ci'][0]:+.2f},{res['diff_ci'][1]:+.2f}]"
        print(f"{hid:50s} {res['wr_pos']:6.1%} {res['wr_neg']:6.1%} "
              f"{res['n_pos']:5d} {res['n_neg']:5d} {res['diff_mean']:+10.3f} {ci_str:>20s}")

    # Save results
    with open(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_e_results.json", "w") as f:
        json.dump(results, f, default=str, indent=2)

    print(f"\n[6] Done in {time.time()-t0:.1f}s")
    return live, results


if __name__ == "__main__":
    live, results = main()
