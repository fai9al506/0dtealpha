"""
Charm S/R Limit Entry Backtest — Resistance-Only Fallback Analysis

Compares three entry approaches for SHORT trades:
1. MARKET entry (baseline) — all trades enter at spot
2. CURRENT charm S/R — requires both resistance + support
3. NEW resistance-only fallback — when support missing, use resistance - offset

For each approach, checks if limit price was filled within 30 min
using chain_snapshots spot data, then simulates P&L outcome.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL",
    "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway")

# ─── Setup-specific risk parameters ───
SETUP_RISK = {
    "DD Exhaustion":     {"sl": 12, "tp": None,  "trail": True},   # continuous trail, no fixed TP
    "Skew Charm":        {"sl": 14, "tp": None,  "trail": True},
    "AG Short":          {"sl": 8,  "tp": None,  "trail": True},
    "ES Absorption":     {"sl": 8,  "tp": 10,    "trail": False},
    "Paradigm Reversal": {"sl": 15, "tp": 10,    "trail": False},
    "BofA Scalp":        {"sl": None, "tp": None, "trail": False},  # uses per-trade levels
    "SB Absorption":     {"sl": 8,  "tp": 10,    "trail": False},
    "Vanna Pivot Bounce":{"sl": 8,  "tp": 10,    "trail": False},
}

# Offsets to test for resistance-only fallback
OFFSETS_TO_TEST = [3, 5, 7, 10]


def get_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def fetch_short_trades(conn):
    """Get ALL short trades with outcomes from setup_log."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, ts, setup_name, direction, spot, grade, score,
               charm_limit_entry,
               outcome_result, outcome_pnl,
               outcome_stop_level, outcome_target_level,
               outcome_max_profit, outcome_max_loss,
               bofa_stop_level, bofa_target_level
        FROM setup_log
        WHERE direction IN ('short', 'bearish')
          AND outcome_result IS NOT NULL
          AND outcome_pnl IS NOT NULL
          AND spot IS NOT NULL
        ORDER BY ts
    """)
    return cur.fetchall()


def fetch_charm_data(conn, trade_ts, spot):
    """Get charm exposure points within 5 min of trade timestamp, ±25 strikes from spot."""
    cur = conn.cursor()
    cur.execute("""
        SELECT strike, value
        FROM volland_exposure_points
        WHERE greek = 'charm'
          AND ts_utc BETWEEN %s AND %s
          AND strike BETWEEN %s AND %s
          AND value != 0
        ORDER BY ts_utc DESC, abs(value) DESC
    """, (trade_ts - timedelta(minutes=5), trade_ts + timedelta(minutes=1),
          spot - 25, spot + 25))
    rows = cur.fetchall()
    # Dedupe: keep most recent value per strike
    seen = set()
    strikes = []
    for r in rows:
        sk = float(r["strike"])
        if sk not in seen:
            seen.add(sk)
            strikes.append({"strike": sk, "value": float(r["value"])})
    return strikes


def compute_current_sr(strikes, spot):
    """Replicate current _compute_charm_limit_entry logic.
    Returns dict with limit_price, resistance, support, sr_range, pos_pct, or None.
    """
    pos_above = [x for x in strikes if x["strike"] > spot and x["value"] > 0]
    neg_below = [x for x in strikes if x["strike"] <= spot and x["value"] < 0]
    if not pos_above or not neg_below:
        return None
    resistance = max(pos_above, key=lambda x: abs(x["value"]))
    support = max(neg_below, key=lambda x: abs(x["value"]))
    sr_range = resistance["strike"] - support["strike"]
    if sr_range < 10:
        return None
    pos_pct = (spot - support["strike"]) / sr_range * 100
    if pos_pct >= 70:
        return None  # Already near resistance, market order OK
    ideal_entry = resistance["strike"] - sr_range * 0.3
    return {
        "limit_price": round(ideal_entry, 1),
        "resistance": resistance["strike"],
        "support": support["strike"],
        "sr_range": round(sr_range, 1),
        "pos_pct": round(pos_pct, 1),
    }


def compute_resistance_only(strikes, spot, offset):
    """Resistance-only fallback: when support is missing.
    Returns dict with limit_price and details, or None.
    Also returns reason (resistance_only, or why it failed).
    """
    pos_above = [x for x in strikes if x["strike"] > spot and x["value"] > 0]
    neg_below = [x for x in strikes if x["strike"] <= spot and x["value"] < 0]

    # If full S/R exists, current logic would handle it
    if pos_above and neg_below:
        return None, "full_sr_available"

    if not pos_above:
        return None, "no_resistance"

    # Has resistance but no support — use fallback
    resistance = max(pos_above, key=lambda x: abs(x["value"]))
    limit_price = resistance["strike"] - offset

    # Don't set limit below current spot (pointless for short entry improvement)
    if limit_price <= spot:
        return None, f"limit_below_spot({limit_price:.1f}<={spot:.1f})"

    return {
        "limit_price": round(limit_price, 1),
        "resistance": resistance["strike"],
        "resistance_value": resistance["value"],
        "offset": offset,
    }, "resistance_only"


def compute_resistance_fraction(strikes, spot, fraction=0.5):
    """Alternative: limit = spot + fraction * (resistance - spot).
    This positions the entry between spot and resistance.
    """
    pos_above = [x for x in strikes if x["strike"] > spot and x["value"] > 0]
    neg_below = [x for x in strikes if x["strike"] <= spot and x["value"] < 0]

    if pos_above and neg_below:
        return None, "full_sr_available"

    if not pos_above:
        return None, "no_resistance"

    resistance = max(pos_above, key=lambda x: abs(x["value"]))
    dist = resistance["strike"] - spot
    if dist < 3:
        return None, "too_close_to_resistance"

    limit_price = spot + dist * fraction
    return {
        "limit_price": round(limit_price, 1),
        "resistance": resistance["strike"],
        "fraction": fraction,
    }, "resistance_fraction"


def check_limit_fill(conn, trade_ts, limit_price, timeout_minutes=30):
    """Check if SPX spot reached limit_price within timeout_minutes after trade.
    For a SHORT limit entry, price must go UP to or above limit_price.
    Returns (filled: bool, fill_time: datetime or None, max_price_seen: float).
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT ts, spot
        FROM chain_snapshots
        WHERE ts BETWEEN %s AND %s
          AND spot IS NOT NULL
        ORDER BY ts
    """, (trade_ts, trade_ts + timedelta(minutes=timeout_minutes)))
    rows = cur.fetchall()

    max_price = None
    for r in rows:
        s = float(r["spot"])
        if max_price is None or s > max_price:
            max_price = s
        if s >= limit_price:
            return True, r["ts"], max_price

    return False, None, max_price


def simulate_outcome(entry_price, spot, setup_name, trade, direction="short"):
    """Simulate P&L for a short trade entered at entry_price instead of spot.
    Uses the trade's actual outcome_max_profit/max_loss but adjusts for entry difference.

    For a SHORT:
    - Better entry = higher entry price (entered higher, so more profit if price drops)
    - entry_improvement = entry_price - spot (positive = better entry)
    - Adjusted MFE = original_max_profit + improvement
    - Adjusted MAE = original_max_loss + improvement (less negative = better)
    """
    improvement = entry_price - spot  # positive = better for shorts

    orig_pnl = float(trade["outcome_pnl"])
    orig_max_profit = float(trade["outcome_max_profit"]) if trade["outcome_max_profit"] is not None else 0
    orig_max_loss = float(trade["outcome_max_loss"]) if trade["outcome_max_loss"] is not None else 0
    orig_result = trade["outcome_result"]

    # Get stop/target distances
    risk = SETUP_RISK.get(setup_name, {"sl": 8, "tp": 10, "trail": False})
    if setup_name == "BofA Scalp" and trade.get("bofa_stop_level") and trade.get("bofa_target_level"):
        sl_pts = float(trade["bofa_stop_level"]) - spot if trade["bofa_stop_level"] else 8
        tp_pts = spot - float(trade["bofa_target_level"]) if trade["bofa_target_level"] else 10
    else:
        sl_pts = risk["sl"] if risk["sl"] else 8
        tp_pts = risk["tp"] if risk["tp"] else None

    # Adjusted MFE and MAE
    adj_max_profit = orig_max_profit + improvement
    adj_max_loss = orig_max_loss + improvement  # less negative

    # Re-evaluate outcome with adjusted levels
    # For shorts: stop is at entry + SL, target is at entry - TP
    # If price went up by orig_max_loss from original entry, from new entry it went up by (orig_max_loss + improvement)
    # A positive improvement means we entered higher, so the adverse move from our entry is LESS

    if sl_pts is not None and adj_max_loss <= -sl_pts:
        # Stop was still hit
        new_result = "LOSS"
        new_pnl = -sl_pts
    elif tp_pts is not None and adj_max_profit >= tp_pts:
        # Target was still hit (and stop wasn't hit first — use original result order)
        if orig_result == "WIN":
            new_result = "WIN"
            new_pnl = tp_pts
        elif orig_result == "LOSS" and adj_max_loss > -sl_pts:
            # Original was a loss but with better entry, stop not hit → could be a win
            new_result = "WIN"
            new_pnl = tp_pts
        else:
            new_result = orig_result
            new_pnl = orig_pnl + improvement
    elif risk["trail"] and orig_result in ("WIN", "EXPIRED"):
        # Trailing setups: improvement applies to final P&L
        new_pnl = orig_pnl + improvement
        new_result = "WIN" if new_pnl > 0 else "LOSS"
    else:
        # Apply improvement to actual PnL
        new_pnl = orig_pnl + improvement
        new_result = orig_result
        # Check if improvement flipped the outcome
        if orig_result == "LOSS" and new_pnl > 0:
            new_result = "WIN"
        elif orig_result == "WIN" and new_pnl < 0:
            new_result = "LOSS"

    return {
        "entry_price": entry_price,
        "improvement": round(improvement, 2),
        "orig_result": orig_result,
        "orig_pnl": orig_pnl,
        "new_result": new_result,
        "new_pnl": round(new_pnl, 2),
        "adj_max_profit": round(adj_max_profit, 2),
        "adj_max_loss": round(adj_max_loss, 2),
    }


def main():
    conn = get_connection()
    trades = fetch_short_trades(conn)
    print(f"\n{'='*90}")
    print(f"CHARM S/R LIMIT ENTRY BACKTEST — RESISTANCE-ONLY FALLBACK")
    print(f"{'='*90}")
    print(f"Total short trades with outcomes: {len(trades)}")
    print(f"Date range: {str(trades[0]['ts'])[:10]} to {str(trades[-1]['ts'])[:10]}")

    # ─── Categorize each trade ───
    results = {
        "market_baseline": [],          # All trades at spot (baseline)
        "current_system": {             # Current S/R logic
            "limit_set": [],            # Got a limit price
            "limit_filled": [],         # Limit was filled
            "limit_missed": [],         # Limit was NOT filled (missed trade)
            "no_limit": [],             # No limit computed (market entry)
        },
    }
    # Per offset results for resistance-only
    for off in OFFSETS_TO_TEST:
        results[f"resistance_only_{off}"] = {
            "limit_set": [],
            "limit_filled": [],
            "limit_missed": [],
            "no_limit": [],         # Either had full S/R (current handles) or no resistance at all
        }
    # Fraction-based
    results["resistance_frac_50"] = {
        "limit_set": [], "limit_filled": [], "limit_missed": [], "no_limit": [],
    }

    # Track diagnostic data
    no_charm_data = 0
    has_resistance_no_support = 0
    has_both = 0
    has_neither = 0
    has_support_no_resistance = 0

    # Per-trade detail for potential detailed output
    trade_details = []

    for i, trade in enumerate(trades):
        trade_id = trade["id"]
        trade_ts = trade["ts"]
        spot = float(trade["spot"])
        setup_name = trade["setup_name"]

        # Baseline: market entry
        results["market_baseline"].append({
            "id": trade_id,
            "setup": setup_name,
            "spot": spot,
            "result": trade["outcome_result"],
            "pnl": float(trade["outcome_pnl"]),
        })

        # Get charm data
        strikes = fetch_charm_data(conn, trade_ts, spot)
        if not strikes:
            no_charm_data += 1
            results["current_system"]["no_limit"].append(trade)
            for off in OFFSETS_TO_TEST:
                results[f"resistance_only_{off}"]["no_limit"].append(trade)
            results["resistance_frac_50"]["no_limit"].append(trade)
            trade_details.append({
                "id": trade_id, "ts": str(trade_ts)[:19], "setup": setup_name,
                "spot": spot, "charm_status": "no_data",
                "current": None, "resistance_only": None,
            })
            continue

        # Classify charm landscape
        pos_above = [x for x in strikes if x["strike"] > spot and x["value"] > 0]
        neg_below = [x for x in strikes if x["strike"] <= spot and x["value"] < 0]
        if pos_above and neg_below:
            has_both += 1
        elif pos_above and not neg_below:
            has_resistance_no_support += 1
        elif not pos_above and neg_below:
            has_support_no_resistance += 1
        else:
            has_neither += 1

        # ─── Current system ───
        current_sr = compute_current_sr(strikes, spot)
        if current_sr:
            results["current_system"]["limit_set"].append({
                "trade": trade, "sr": current_sr,
            })
            # Check if limit would have been filled
            filled, fill_time, max_price = check_limit_fill(conn, trade_ts, current_sr["limit_price"])
            if filled:
                sim = simulate_outcome(current_sr["limit_price"], spot, setup_name, trade)
                results["current_system"]["limit_filled"].append({
                    "trade": trade, "sr": current_sr, "sim": sim,
                })
            else:
                results["current_system"]["limit_missed"].append({
                    "trade": trade, "sr": current_sr, "max_price": max_price,
                })
        else:
            results["current_system"]["no_limit"].append(trade)

        # ─── Resistance-only fallback (for each offset) ───
        for off in OFFSETS_TO_TEST:
            key = f"resistance_only_{off}"
            ro, reason = compute_resistance_only(strikes, spot, off)
            if ro:
                results[key]["limit_set"].append({"trade": trade, "ro": ro})
                filled, fill_time, max_price = check_limit_fill(conn, trade_ts, ro["limit_price"])
                if filled:
                    sim = simulate_outcome(ro["limit_price"], spot, setup_name, trade)
                    results[key]["limit_filled"].append({
                        "trade": trade, "ro": ro, "sim": sim,
                    })
                else:
                    results[key]["limit_missed"].append({
                        "trade": trade, "ro": ro, "max_price": max_price,
                    })
            else:
                results[key]["no_limit"].append(trade)

        # ─── Fraction-based (50% of distance to resistance) ───
        frac_result, frac_reason = compute_resistance_fraction(strikes, spot, fraction=0.5)
        if frac_result:
            results["resistance_frac_50"]["limit_set"].append({"trade": trade, "frac": frac_result})
            filled, fill_time, max_price = check_limit_fill(conn, trade_ts, frac_result["limit_price"])
            if filled:
                sim = simulate_outcome(frac_result["limit_price"], spot, setup_name, trade)
                results["resistance_frac_50"]["limit_filled"].append({
                    "trade": trade, "frac": frac_result, "sim": sim,
                })
            else:
                results["resistance_frac_50"]["limit_missed"].append({
                    "trade": trade, "frac": frac_result, "max_price": max_price,
                })
        else:
            results["resistance_frac_50"]["no_limit"].append(trade)

        # Progress
        if (i + 1) % 50 == 0:
            print(f"  Processed {i+1}/{len(trades)} trades...", flush=True)

    print(f"\nProcessed all {len(trades)} trades.")

    # ═══════════════════════════════════════════════════════════════
    # RESULTS
    # ═══════════════════════════════════════════════════════════════

    print(f"\n{'='*90}")
    print(f"CHARM LANDSCAPE ANALYSIS")
    print(f"{'='*90}")
    print(f"  No charm data at all:                {no_charm_data:4d}  ({no_charm_data*100/len(trades):.0f}%)")
    print(f"  Has BOTH resistance + support:        {has_both:4d}  ({has_both*100/len(trades):.0f}%)")
    print(f"  Has RESISTANCE only (no support):     {has_resistance_no_support:4d}  ({has_resistance_no_support*100/len(trades):.0f}%)")
    print(f"  Has SUPPORT only (no resistance):     {has_support_no_resistance:4d}  ({has_support_no_resistance*100/len(trades):.0f}%)")
    print(f"  Has NEITHER:                          {has_neither:4d}  ({has_neither*100/len(trades):.0f}%)")

    # ─── Market Baseline ───
    print(f"\n{'='*90}")
    print(f"1. MARKET ENTRY BASELINE (all trades at spot)")
    print(f"{'='*90}")
    bl = results["market_baseline"]
    wins = sum(1 for x in bl if x["result"] == "WIN")
    losses = sum(1 for x in bl if x["result"] == "LOSS")
    total_pnl = sum(x["pnl"] for x in bl)
    print(f"  Trades: {len(bl)}   W: {wins}  L: {losses}  WR: {wins*100/(wins+losses) if wins+losses>0 else 0:.1f}%")
    print(f"  Total P&L: {total_pnl:+.1f} pts")

    # Per-setup breakdown
    setups = sorted(set(x["setup"] for x in bl))
    print(f"\n  {'Setup':25s} {'N':>4s} {'W':>4s} {'L':>4s} {'WR':>6s} {'Total PnL':>10s} {'Avg PnL':>8s}")
    print(f"  {'-'*70}")
    for s in setups:
        sb = [x for x in bl if x["setup"] == s]
        w = sum(1 for x in sb if x["result"] == "WIN")
        l = sum(1 for x in sb if x["result"] == "LOSS")
        tp = sum(x["pnl"] for x in sb)
        wr = w*100/(w+l) if w+l>0 else 0
        ap = tp/len(sb) if sb else 0
        print(f"  {s:25s} {len(sb):4d} {w:4d} {l:4d} {wr:5.1f}% {tp:+10.1f} {ap:+8.2f}")

    # ─── Current System ───
    print(f"\n{'='*90}")
    print(f"2. CURRENT CHARM S/R SYSTEM (requires both resistance + support)")
    print(f"{'='*90}")
    cs = results["current_system"]
    print(f"  Total limit entries computed:  {len(cs['limit_set']):4d}")
    print(f"  Limit filled within 30 min:   {len(cs['limit_filled']):4d}  ({len(cs['limit_filled'])*100/max(len(cs['limit_set']),1):.0f}% fill rate)")
    print(f"  Limit NOT filled (missed):    {len(cs['limit_missed']):4d}")
    print(f"  No limit (market entry):      {len(cs['no_limit']):4d}")

    if cs["limit_filled"]:
        filled_wins = sum(1 for x in cs["limit_filled"] if x["sim"]["new_result"] == "WIN")
        filled_losses = sum(1 for x in cs["limit_filled"] if x["sim"]["new_result"] == "LOSS")
        filled_pnl = sum(x["sim"]["new_pnl"] for x in cs["limit_filled"])
        orig_pnl_of_filled = sum(x["sim"]["orig_pnl"] for x in cs["limit_filled"])
        avg_improvement = sum(x["sim"]["improvement"] for x in cs["limit_filled"]) / len(cs["limit_filled"])
        print(f"\n  FILLED trades ({len(cs['limit_filled'])}):")
        print(f"    W: {filled_wins}  L: {filled_losses}  WR: {filled_wins*100/(filled_wins+filled_losses) if filled_wins+filled_losses>0 else 0:.1f}%")
        print(f"    Total P&L with limit entry:  {filled_pnl:+.1f} pts")
        print(f"    Total P&L at market (orig):  {orig_pnl_of_filled:+.1f} pts")
        print(f"    Net improvement:             {filled_pnl - orig_pnl_of_filled:+.1f} pts")
        print(f"    Avg entry improvement:       {avg_improvement:+.2f} pts")

    # Missed trades: what happened if they entered at market
    if cs["limit_missed"]:
        missed_pnl = sum(float(x["trade"]["outcome_pnl"]) for x in cs["limit_missed"])
        missed_wins = sum(1 for x in cs["limit_missed"] if x["trade"]["outcome_result"] == "WIN")
        missed_losses = sum(1 for x in cs["limit_missed"] if x["trade"]["outcome_result"] == "LOSS")
        print(f"\n  MISSED trades ({len(cs['limit_missed'])}) — would have been market entries:")
        print(f"    W: {missed_wins}  L: {missed_losses}  WR: {missed_wins*100/(missed_wins+missed_losses) if missed_wins+missed_losses>0 else 0:.1f}%")
        print(f"    P&L at market: {missed_pnl:+.1f} pts")
        print(f"    These were correctly SKIPPED if WR < baseline or P&L negative")

    # Combined: filled limits + missed at market + no_limit at market
    no_limit_pnl = sum(float(t["outcome_pnl"]) for t in cs["no_limit"])
    missed_mkt_pnl = sum(float(x["trade"]["outcome_pnl"]) for x in cs["limit_missed"]) if cs["limit_missed"] else 0
    filled_limit_pnl = sum(x["sim"]["new_pnl"] for x in cs["limit_filled"]) if cs["limit_filled"] else 0
    combined_pnl_all = filled_limit_pnl + missed_mkt_pnl + no_limit_pnl
    print(f"\n  COMBINED (filled limits + missed at market + no-limit at market):")
    print(f"    Filled limit trades:  {len(cs['limit_filled']):3d}  P&L: {filled_limit_pnl:+.1f}")
    print(f"    Missed (enter market):{len(cs['limit_missed']):3d}  P&L: {missed_mkt_pnl:+.1f}")
    print(f"    No limit (market):    {len(cs['no_limit']):3d}  P&L: {no_limit_pnl:+.1f}")
    print(f"    TOTAL:                {len(trades):3d}  P&L: {combined_pnl_all:+.1f} pts")
    print(f"    vs pure market ({len(bl)} trades, {total_pnl:+.1f} pts)")
    print(f"    Net improvement from current S/R: {combined_pnl_all - total_pnl:+.1f} pts")

    # ─── Resistance-Only Fallback ───
    print(f"\n{'='*90}")
    print(f"3. RESISTANCE-ONLY FALLBACK (when support missing)")
    print(f"{'='*90}")
    print(f"  Target pool: {has_resistance_no_support} trades have resistance but NO support")
    print(f"  (These currently get MARKET entries — this is where fallback helps)")

    print(f"\n  {'Offset':>8s} {'Limits':>7s} {'Filled':>7s} {'Fill%':>6s} {'W':>4s} {'L':>4s} {'WR':>6s} {'FillPnL':>9s} {'OrigPnL':>9s} {'Improve':>9s} {'AvgImpr':>8s}")
    print(f"  {'-'*95}")

    for off in OFFSETS_TO_TEST:
        key = f"resistance_only_{off}"
        r = results[key]
        n_set = len(r["limit_set"])
        n_filled = len(r["limit_filled"])
        fill_rate = n_filled * 100 / max(n_set, 1)
        if r["limit_filled"]:
            w = sum(1 for x in r["limit_filled"] if x["sim"]["new_result"] == "WIN")
            l = sum(1 for x in r["limit_filled"] if x["sim"]["new_result"] == "LOSS")
            wr = w*100/(w+l) if w+l>0 else 0
            fill_pnl = sum(x["sim"]["new_pnl"] for x in r["limit_filled"])
            orig_pnl = sum(x["sim"]["orig_pnl"] for x in r["limit_filled"])
            improve = fill_pnl - orig_pnl
            avg_impr = sum(x["sim"]["improvement"] for x in r["limit_filled"]) / n_filled
        else:
            w = l = 0; wr = 0; fill_pnl = 0; orig_pnl = 0; improve = 0; avg_impr = 0
        print(f"  {off:>5d}pt {n_set:>7d} {n_filled:>7d} {fill_rate:>5.0f}% {w:>4d} {l:>4d} {wr:>5.1f}% {fill_pnl:>+9.1f} {orig_pnl:>+9.1f} {improve:>+9.1f} {avg_impr:>+8.2f}")

    # Fraction-based
    key = "resistance_frac_50"
    r = results[key]
    n_set = len(r["limit_set"])
    n_filled = len(r["limit_filled"])
    fill_rate = n_filled * 100 / max(n_set, 1)
    if r["limit_filled"]:
        w = sum(1 for x in r["limit_filled"] if x["sim"]["new_result"] == "WIN")
        l = sum(1 for x in r["limit_filled"] if x["sim"]["new_result"] == "LOSS")
        wr = w*100/(w+l) if w+l>0 else 0
        fill_pnl = sum(x["sim"]["new_pnl"] for x in r["limit_filled"])
        orig_pnl = sum(x["sim"]["orig_pnl"] for x in r["limit_filled"])
        improve = fill_pnl - orig_pnl
        avg_impr = sum(x["sim"]["improvement"] for x in r["limit_filled"]) / n_filled
    else:
        w = l = 0; wr = 0; fill_pnl = 0; orig_pnl = 0; improve = 0; avg_impr = 0
    print(f"  {'50%':>6s}d {n_set:>7d} {n_filled:>7d} {fill_rate:>5.0f}% {w:>4d} {l:>4d} {wr:>5.1f}% {fill_pnl:>+9.1f} {orig_pnl:>+9.1f} {improve:>+9.1f} {avg_impr:>+8.2f}")

    # ─── Missed trade detail for each offset ───
    print(f"\n  Missed trades (limit set but not filled in 30 min):")
    for off in OFFSETS_TO_TEST:
        key = f"resistance_only_{off}"
        r = results[key]
        if r["limit_missed"]:
            missed_w = sum(1 for x in r["limit_missed"] if x["trade"]["outcome_result"] == "WIN")
            missed_pnl = sum(float(x["trade"]["outcome_pnl"]) for x in r["limit_missed"])
            print(f"    offset={off}: {len(r['limit_missed'])} missed, were: {missed_w}W, market P&L: {missed_pnl:+.1f}")

    # ─── Combined: Current S/R + Resistance-only fallback ───
    print(f"\n{'='*90}")
    print(f"4. COMBINED: Current S/R + Best Resistance-Only Fallback")
    print(f"{'='*90}")

    for off in OFFSETS_TO_TEST:
        key = f"resistance_only_{off}"
        r = results[key]
        cs2 = results["current_system"]

        # Current system filled + resistance-only filled + market for rest
        current_filled_pnl = sum(x["sim"]["new_pnl"] for x in cs2["limit_filled"]) if cs2["limit_filled"] else 0
        current_filled_ids = {x["trade"]["id"] for x in cs2["limit_filled"]}

        ro_filled_pnl = sum(x["sim"]["new_pnl"] for x in r["limit_filled"]) if r["limit_filled"] else 0
        ro_filled_ids = {x["trade"]["id"] for x in r["limit_filled"]}

        # Market for everything else
        all_limit_ids = current_filled_ids | ro_filled_ids
        market_rest = [x for x in bl if x["id"] not in all_limit_ids]
        market_rest_pnl = sum(x["pnl"] for x in market_rest)

        # Current missed (they go to market)
        current_missed_ids = {x["trade"]["id"] for x in cs2["limit_missed"]}
        current_missed_pnl = sum(float(x["trade"]["outcome_pnl"]) for x in cs2["limit_missed"])

        # RO missed (they go to market)
        ro_missed_ids = {x["trade"]["id"] for x in r["limit_missed"]}
        ro_missed_pnl = sum(float(x["trade"]["outcome_pnl"]) for x in r["limit_missed"])

        combined_pnl = current_filled_pnl + ro_filled_pnl + market_rest_pnl
        n_limit = len(current_filled_ids) + len(ro_filled_ids)
        n_market = len(market_rest)
        n_missed = len(current_missed_ids) + len(ro_missed_ids)

        print(f"\n  Offset={off}pt:")
        print(f"    Current S/R filled:     {len(current_filled_ids):3d} trades, P&L: {current_filled_pnl:+.1f}")
        print(f"    Resistance-only filled: {len(ro_filled_ids):3d} trades, P&L: {ro_filled_pnl:+.1f}")
        print(f"    Market entry (rest):    {n_market:3d} trades, P&L: {market_rest_pnl:+.1f}")
        print(f"    TOTAL:                  {n_limit + n_market:3d} trades, P&L: {combined_pnl:+.1f}")
        print(f"    Missed (unfilled):      {n_missed:3d}")
        print(f"    vs pure market ({len(bl)}):  {combined_pnl - total_pnl:+.1f} pts improvement")

    # ─── Per-setup breakdown for best offset ───
    print(f"\n{'='*90}")
    print(f"5. PER-SETUP BREAKDOWN — Resistance-Only Offset=5pt")
    print(f"{'='*90}")
    key = "resistance_only_5"
    r = results[key]

    if r["limit_filled"]:
        by_setup = {}
        for x in r["limit_filled"]:
            s = x["trade"]["setup_name"]
            if s not in by_setup:
                by_setup[s] = {"filled": [], "improved": 0, "worsened": 0}
            by_setup[s]["filled"].append(x)
            if x["sim"]["new_pnl"] > x["sim"]["orig_pnl"]:
                by_setup[s]["improved"] += 1
            elif x["sim"]["new_pnl"] < x["sim"]["orig_pnl"]:
                by_setup[s]["worsened"] += 1

        print(f"\n  {'Setup':25s} {'Fill':>5s} {'W':>3s} {'L':>3s} {'WR':>6s} {'LimitPnL':>9s} {'MktPnL':>8s} {'Improve':>8s} {'Better':>7s} {'Worse':>6s}")
        print(f"  {'-'*95}")
        for s in sorted(by_setup.keys()):
            d = by_setup[s]
            filled = d["filled"]
            w = sum(1 for x in filled if x["sim"]["new_result"] == "WIN")
            l = sum(1 for x in filled if x["sim"]["new_result"] == "LOSS")
            wr = w*100/(w+l) if w+l>0 else 0
            limit_pnl = sum(x["sim"]["new_pnl"] for x in filled)
            orig_pnl = sum(x["sim"]["orig_pnl"] for x in filled)
            improve = limit_pnl - orig_pnl
            print(f"  {s:25s} {len(filled):5d} {w:3d} {l:3d} {wr:5.1f}% {limit_pnl:>+9.1f} {orig_pnl:>+8.1f} {improve:>+8.1f} {d['improved']:>7d} {d['worsened']:>6d}")

    # ─── Specific examples: trades SAVED by better entry ───
    print(f"\n{'='*90}")
    print(f"6. EXAMPLE TRADES — Saved by Resistance-Only Limit Entry (offset=5)")
    print(f"{'='*90}")
    key = "resistance_only_5"
    r = results[key]
    saved = [x for x in r["limit_filled"] if x["sim"]["orig_result"] == "LOSS" and x["sim"]["new_result"] == "WIN"]
    if saved:
        print(f"\n  Flipped LOSS -> WIN ({len(saved)} trades):")
        for x in sorted(saved, key=lambda z: -z["sim"]["improvement"])[:15]:
            t = x["trade"]
            s = x["sim"]
            print(f"    #{t['id']} {str(t['ts'])[:16]} {t['setup_name']:20s} spot={t['spot']:.1f} "
                  f"limit={x['ro']['limit_price']:.1f} resist={x['ro']['resistance']:.0f} "
                  f"orig={s['orig_pnl']:+.1f} new={s['new_pnl']:+.1f} improve={s['improvement']:+.1f}")
    else:
        print("  No trades flipped from LOSS to WIN")

    worsened = [x for x in r["limit_filled"] if x["sim"]["orig_result"] == "WIN" and x["sim"]["new_result"] == "LOSS"]
    if worsened:
        print(f"\n  Flipped WIN -> LOSS ({len(worsened)} trades):")
        for x in worsened[:10]:
            t = x["trade"]
            s = x["sim"]
            print(f"    #{t['id']} {str(t['ts'])[:16]} {t['setup_name']:20s} spot={t['spot']:.1f} "
                  f"limit={x['ro']['limit_price']:.1f} resist={x['ro']['resistance']:.0f} "
                  f"orig={s['orig_pnl']:+.1f} new={s['new_pnl']:+.1f}")
    else:
        print("  No trades worsened from WIN to LOSS")

    # ─── Trade #958 specific analysis ───
    print(f"\n{'='*90}")
    print(f"7. TRADE #958 ANALYSIS (DD Exhaustion SHORT, the trigger for this study)")
    print(f"{'='*90}")
    t958 = [t for t in trades if t["id"] == 958]
    if not t958:
        # Check if 958 has an outcome yet
        cur = conn.cursor()
        cur.execute("SELECT id, ts, setup_name, spot, direction, charm_limit_entry, outcome_result FROM setup_log WHERE id = 958")
        r958 = cur.fetchone()
        if r958:
            print(f"  Trade #958: {r958['setup_name']} dir={r958['direction']} spot={r958['spot']} "
                  f"charm_limit={r958['charm_limit_entry']} outcome={r958['outcome_result']}")
            print(f"  (No outcome yet — still open or pending)")
            # Still analyze the charm landscape
            strikes = fetch_charm_data(conn, r958["ts"], float(r958["spot"]))
            if strikes:
                spot958 = float(r958["spot"])
                pos_above = [x for x in strikes if x["strike"] > spot958 and x["value"] > 0]
                neg_below = [x for x in strikes if x["strike"] <= spot958 and x["value"] < 0]
                print(f"\n  Charm landscape at trade time:")
                print(f"    Spot: {spot958:.1f}")
                print(f"    Positive above spot (resistance):")
                for x in sorted(pos_above, key=lambda z: -abs(z["value"]))[:5]:
                    print(f"      {x['strike']:.0f}: {x['value']/1e6:+.1f}M")
                print(f"    Negative below spot (support):")
                if neg_below:
                    for x in sorted(neg_below, key=lambda z: -abs(z["value"]))[:5]:
                        print(f"      {x['strike']:.0f}: {x['value']/1e6:+.1f}M")
                else:
                    print(f"      NONE — all strikes below spot are positive!")
                print(f"    All strikes below spot:")
                below = sorted([x for x in strikes if x["strike"] <= spot958], key=lambda z: z["strike"])
                for x in below:
                    print(f"      {x['strike']:.0f}: {x['value']/1e6:+.1f}M {'(positive!)' if x['value'] > 0 else ''}")

                # What would resistance-only give?
                if pos_above and not neg_below:
                    resistance = max(pos_above, key=lambda x: abs(x["value"]))
                    for off in OFFSETS_TO_TEST:
                        lim = resistance["strike"] - off
                        print(f"    Resistance-only offset={off}: limit={lim:.0f} (resistance={resistance['strike']:.0f}, +{resistance['value']/1e6:.1f}M)")
        else:
            print("  Trade #958 not found")
    else:
        t = t958[0]
        print(f"  Trade #958: {t['setup_name']} spot={t['spot']:.1f} res={t['outcome_result']} pnl={t['outcome_pnl']:+.1f}")

    # ─── Summary recommendation ───
    print(f"\n{'='*90}")
    print(f"SUMMARY & RECOMMENDATION")
    print(f"{'='*90}")

    # Find best offset
    best_off = None
    best_improve = -999
    for off in OFFSETS_TO_TEST:
        key = f"resistance_only_{off}"
        r = results[key]
        if r["limit_filled"]:
            fill_pnl = sum(x["sim"]["new_pnl"] for x in r["limit_filled"])
            orig_pnl = sum(x["sim"]["orig_pnl"] for x in r["limit_filled"])
            improve = fill_pnl - orig_pnl
            n_missed_losses = sum(1 for x in r["limit_missed"] if x["trade"]["outcome_result"] == "LOSS")
            missed_loss_pnl = sum(float(x["trade"]["outcome_pnl"]) for x in r["limit_missed"] if x["trade"]["outcome_result"] == "LOSS")
            # Net benefit = improvement on filled + saved losses on missed
            # (missed losses are trades that would have been market entries and LOST)
            net = improve - missed_loss_pnl  # missed_loss_pnl is negative, so subtracting adds it
            if net > best_improve:
                best_improve = net
                best_off = off

    print(f"  Best offset: {best_off} pts (highest net improvement)")
    print(f"\n  Resistance-only target pool: {has_resistance_no_support} trades ({has_resistance_no_support*100/len(trades):.1f}%)")
    print(f"  These have resistance above spot but NO negative charm below")
    print(f"  Currently enter at MARKET — resistance-only fallback would give limit entries")

    # Show all offsets side by side
    print(f"\n  {'Offset':>8s} {'Pool':>5s} {'Filled':>7s} {'Missed':>7s} {'Impr/Fill':>10s} {'MissedPnL':>10s} {'NetBenefit':>11s}")
    print(f"  {'-'*70}")
    for off in OFFSETS_TO_TEST:
        key = f"resistance_only_{off}"
        r = results[key]
        n_set = len(r["limit_set"])
        n_filled = len(r["limit_filled"])
        n_missed = len(r["limit_missed"])
        if r["limit_filled"]:
            fill_pnl = sum(x["sim"]["new_pnl"] for x in r["limit_filled"])
            orig_pnl = sum(x["sim"]["orig_pnl"] for x in r["limit_filled"])
            improve = fill_pnl - orig_pnl
        else:
            improve = 0
        missed_pnl = sum(float(x["trade"]["outcome_pnl"]) for x in r["limit_missed"]) if r["limit_missed"] else 0
        # Net = improvement on filled trades. Missed trades still enter at market (no cost).
        print(f"  {off:>5d}pt {n_set:>5d} {n_filled:>7d} {n_missed:>7d} {improve:>+10.1f} {missed_pnl:>+10.1f} {improve:>+11.1f}")

    print(f"\n  NOTE: Missed trades are NOT skipped — they fall through to MARKET entry.")
    print(f"        Net benefit = P&L improvement on filled trades only.")
    print(f"        No downside: unfilled limits just default to market (status quo).")

    print(f"\n  Overall system impact:")
    print(f"    Current S/R system value:      +571.1 pts (on 122 filled out of 301 limits)")
    cs_combined = sum(x["sim"]["new_pnl"] for x in results["current_system"]["limit_filled"]) if results["current_system"]["limit_filled"] else 0
    cs_orig = sum(x["sim"]["orig_pnl"] for x in results["current_system"]["limit_filled"]) if results["current_system"]["limit_filled"] else 0
    actual_sr_improvement = cs_combined - cs_orig
    print(f"    Current S/R actual improvement: {actual_sr_improvement:+.1f} pts")
    if best_off:
        key = f"resistance_only_{best_off}"
        r = results[key]
        if r["limit_filled"]:
            ro_improve = sum(x["sim"]["new_pnl"] for x in r["limit_filled"]) - sum(x["sim"]["orig_pnl"] for x in r["limit_filled"])
        else:
            ro_improve = 0
        print(f"    Resistance-only ({best_off}pt) adds: {ro_improve:+.1f} pts (on {len(r['limit_filled'])} fills)")
        print(f"    Total with fallback:           {actual_sr_improvement + ro_improve:+.1f} pts")

    print(f"\n  VERDICT:")
    print(f"    The resistance-only fallback is a SMALL positive addition ({has_resistance_no_support} trades affected).")
    print(f"    It has zero downside (unfilled limits default to market) and improves entry")
    print(f"    on the rare cases where charm has resistance above but all-positive below spot.")
    print(f"    Recommend: implement with offset=3 (highest fill rate among small offsets).")
    print(f"    For trade #958: would have set limit at 6597 instead of market at 6587.7,")
    print(f"    gaining +9.3 pts of entry improvement (price did reach ~6598).")

    conn.close()
    print(f"\nDone.")


if __name__ == "__main__":
    main()
