"""
Recompute ALL charm-limit trade outcomes with proper fill detection.
Uses playback_snapshots to get actual SPX price path during each trade.
READ-ONLY — does NOT update the DB.
"""
import psycopg2
import psycopg2.extras
import sys
import traceback
from datetime import timedelta

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

# Setup risk management parameters (from setup_detector.py / main.py)
SETUP_RM = {
    "Skew Charm":       {"sl": 14, "target": 10, "trail": True,  "trail_mode": "hybrid", "be_trigger": 10, "activation": 10, "gap": 8},
    "DD Exhaustion":    {"sl": 20, "target": 10, "trail": True,  "trail_mode": "continuous", "activation": 20, "gap": 5},
    "AG Short":         {"sl": 8,  "target": 10, "trail": True,  "trail_mode": "hybrid", "be_trigger": 8, "activation": 10, "gap": 5},
    "BofA Scalp":       {"sl": 8,  "target": 10, "trail": False},
    "ES Absorption":    {"sl": 8,  "target": 10, "trail": False},
    "GEX Long":         {"sl": 8,  "target": 10, "trail": True,  "trail_mode": "hybrid", "be_trigger": 8, "activation": 10, "gap": 5},
    "Paradigm Reversal":{"sl": 8,  "target": 10, "trail": False},
}

def get_rm(setup_name):
    """Get risk management params for a setup."""
    return SETUP_RM.get(setup_name, {"sl": 8, "target": 10, "trail": False})


def simulate_trade_from_prices(entry_price, direction, prices_after_entry, rm, max_minutes=None):
    """
    Simulate a trade using actual SPX price path.

    entry_price: the charm limit entry price (for shorts)
    direction: 'short' or 'long'
    prices_after_entry: list of (ts, spot) tuples after fill, ordered by time
    rm: risk management dict
    max_minutes: max trade duration (for BofA)

    Returns: (result, pnl, max_profit, max_loss, elapsed_min, first_event, exit_price)
    """
    is_long = direction in ("long", "bullish")
    sl_pts = rm["sl"]
    target_pts = rm["target"]
    is_trailing = rm.get("trail", False)

    if is_long:
        target_lvl = entry_price + target_pts
        stop_lvl = entry_price - sl_pts
    else:
        target_lvl = entry_price - target_pts
        stop_lvl = entry_price + sl_pts

    initial_stop = stop_lvl
    seen_high = entry_price
    seen_low = entry_price
    max_fav = 0.0
    t1_hit = False

    entry_ts = prices_after_entry[0][0] if prices_after_entry else None

    for ts, spot in prices_after_entry:
        seen_high = max(seen_high, spot)
        seen_low = min(seen_low, spot)

        if is_long:
            fav = spot - entry_price
        else:
            fav = entry_price - spot

        max_fav = max(max_fav, fav)

        elapsed = (ts - entry_ts).total_seconds() / 60.0 if entry_ts else 0

        # BofA max hold
        if max_minutes and elapsed >= max_minutes:
            pnl = (spot - entry_price) if is_long else (entry_price - spot)
            mp = seen_high - entry_price if is_long else entry_price - seen_low
            ml = seen_low - entry_price if is_long else entry_price - seen_high
            return ("EXPIRED", round(pnl, 1), round(mp, 2), round(ml, 2), int(elapsed), "timeout", spot)

        if is_trailing:
            # Track T1 hit
            if max_fav >= 10 and not t1_hit:
                t1_hit = True

            trail_lock = None
            mode = rm.get("trail_mode", "hybrid")

            if mode == "continuous":
                if max_fav >= rm["activation"]:
                    trail_lock = max_fav - rm["gap"]
            elif mode == "hybrid":
                if max_fav >= rm["activation"]:
                    trail_lock = max_fav - rm["gap"]
                elif max_fav >= rm.get("be_trigger", 8):
                    trail_lock = 0  # breakeven

            if trail_lock is not None:
                if is_long:
                    new_stop = entry_price + trail_lock
                    if new_stop > stop_lvl:
                        stop_lvl = new_stop
                else:
                    new_stop = entry_price - trail_lock
                    if new_stop < stop_lvl:
                        stop_lvl = new_stop

            # Check trail stop
            if is_long and spot <= stop_lvl:
                pnl = stop_lvl - entry_price
                if t1_hit:
                    pnl = round((10.0 + pnl) / 2, 1)
                result = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "EXPIRED")
                mp = seen_high - entry_price if is_long else entry_price - seen_low
                ml = seen_low - entry_price if is_long else entry_price - seen_high
                fe = "target" if result == "WIN" else "stop"
                return (result, round(pnl, 1), round(mp, 2), round(ml, 2), int(elapsed), fe, stop_lvl)
            elif not is_long and spot >= stop_lvl:
                pnl = entry_price - stop_lvl
                if t1_hit:
                    pnl = round((10.0 + pnl) / 2, 1)
                result = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "EXPIRED")
                mp = entry_price - seen_low if not is_long else seen_high - entry_price
                ml = entry_price - seen_high if not is_long else seen_low - entry_price
                fe = "target" if result == "WIN" else "stop"
                return (result, round(pnl, 1), round(mp, 2), round(ml, 2), int(elapsed), fe, stop_lvl)
        else:
            # Fixed SL/TP
            if is_long:
                if spot >= target_lvl:
                    mp = seen_high - entry_price
                    ml = seen_low - entry_price
                    return ("WIN", round(target_pts, 1), round(mp, 2), round(ml, 2), int(elapsed), "10pt", target_lvl)
                elif spot <= stop_lvl:
                    mp = seen_high - entry_price
                    ml = seen_low - entry_price
                    return ("LOSS", round(-sl_pts, 1), round(mp, 2), round(ml, 2), int(elapsed), "stop", stop_lvl)
            else:
                if spot <= target_lvl:
                    mp = entry_price - seen_low
                    ml = entry_price - seen_high
                    return ("WIN", round(target_pts, 1), round(mp, 2), round(ml, 2), int(elapsed), "10pt", target_lvl)
                elif spot >= stop_lvl:
                    mp = entry_price - seen_low
                    ml = entry_price - seen_high
                    return ("LOSS", round(-sl_pts, 1), round(mp, 2), round(ml, 2), int(elapsed), "stop", stop_lvl)

    # If we get here, trade expired (market close)
    last_spot = prices_after_entry[-1][1] if prices_after_entry else entry_price
    elapsed = (prices_after_entry[-1][0] - entry_ts).total_seconds() / 60.0 if prices_after_entry and entry_ts else 0
    pnl = (last_spot - entry_price) if is_long else (entry_price - last_spot)
    if t1_hit:
        pnl = round((10.0 + pnl) / 2, 1)
    mp = (seen_high - entry_price) if is_long else (entry_price - seen_low)
    ml = (seen_low - entry_price) if is_long else (entry_price - seen_high)
    return ("EXPIRED", round(pnl, 1), round(mp, 2), round(ml, 2), int(elapsed), "timeout", last_spot)


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ── Step 1: Get all charm-limit trades with outcomes ──
    print("=" * 100)
    print("CHARM LIMIT ENTRY - OUTCOME RECOMPUTATION")
    print("=" * 100)

    cur.execute("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               setup_name, direction, grade, score,
               spot, charm_limit_entry,
               outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss,
               outcome_elapsed_min, outcome_first_event,
               outcome_target_level, outcome_stop_level
        FROM setup_log
        WHERE charm_limit_entry IS NOT NULL
        ORDER BY ts
    """)
    trades = cur.fetchall()

    print(f"\nTotal trades with charm_limit_entry: {len(trades)}")

    # Separate: those with outcomes vs still open
    with_outcome = [t for t in trades if t["outcome_result"] is not None]
    no_outcome = [t for t in trades if t["outcome_result"] is None]
    print(f"  With outcome: {len(with_outcome)}")
    print(f"  Still open/no outcome: {len(no_outcome)}")

    # Show current outcome distribution
    print(f"\n--- Current DB Outcome Distribution ---")
    outcome_counts = {}
    for t in with_outcome:
        r = t["outcome_result"]
        outcome_counts[r] = outcome_counts.get(r, 0) + 1
    for k, v in sorted(outcome_counts.items()):
        print(f"  {k}: {v}")

    # ── Step 2: For each trade, get SPX price path from playback_snapshots ──
    print(f"\n{'='*100}")
    print("RECOMPUTING OUTCOMES FROM PLAYBACK_SNAPSHOTS")
    print(f"{'='*100}")

    filled_trades = []
    timeout_trades = []
    errors = []

    for t in with_outcome:
        trade_id = t["id"]
        ts_et = t["ts_et"]
        setup_name = t["setup_name"]
        direction = t["direction"]
        spot = t["spot"]
        charm_limit = t["charm_limit_entry"]
        old_result = t["outcome_result"]
        old_pnl = t["outcome_pnl"]
        grade = t["grade"]

        is_long = direction in ("long", "bullish")

        # Get SPX price path from entry to end of day (or +7 hours)
        cur.execute("""
            SELECT ts AT TIME ZONE 'America/New_York' as ts_et, spot
            FROM playback_snapshots
            WHERE ts >= (SELECT ts FROM setup_log WHERE id = %s) - interval '1 minute'
              AND ts <= (SELECT ts FROM setup_log WHERE id = %s) + interval '7 hours'
              AND spot IS NOT NULL
            ORDER BY ts ASC
        """, (trade_id, trade_id))

        prices = [(r["ts_et"], r["spot"]) for r in cur.fetchall()]

        if len(prices) < 2:
            errors.append({
                "id": trade_id, "setup": setup_name, "ts": ts_et,
                "reason": f"Only {len(prices)} price snapshots found"
            })
            continue

        # ── Step 2a: Check if price ever reached charm_limit (for fill) ──
        # For shorts: charm_limit is ABOVE spot. Price must rise to charm_limit for fill.

        # Get the highest price seen in the first 30 minutes (fill window)
        fill_window_end = prices[0][0] + timedelta(minutes=30)
        prices_in_fill_window = [(ts, p) for ts, p in prices if ts <= fill_window_end]

        if not prices_in_fill_window:
            errors.append({
                "id": trade_id, "setup": setup_name, "ts": ts_et,
                "reason": "No prices in 30-min fill window"
            })
            continue

        # For shorts: check if high price >= charm_limit
        high_in_window = max(p for _, p in prices_in_fill_window)
        low_in_window = min(p for _, p in prices_in_fill_window)

        if not is_long:
            # Short: need price to rise to charm_limit
            filled = high_in_window >= charm_limit
        else:
            # Long: need price to drop to charm_limit
            filled = low_in_window <= charm_limit

        if not filled:
            # LIMIT_TIMEOUT — limit never reached
            timeout_trades.append({
                "id": trade_id,
                "ts": ts_et,
                "setup": setup_name,
                "direction": direction,
                "grade": grade,
                "spot": spot,
                "charm_limit": charm_limit,
                "offset": round(charm_limit - spot, 1),
                "high_in_window": round(high_in_window, 1),
                "gap_to_fill": round(charm_limit - high_in_window, 1) if not is_long else round(low_in_window - charm_limit, 1),
                "old_result": old_result,
                "old_pnl": old_pnl or 0,
                "new_result": "LIMIT_TIMEOUT",
                "new_pnl": 0.0,
            })
        else:
            # FILLED — find fill time and simulate trade from charm_limit as entry
            # Find the first snapshot where price reached charm_limit
            fill_ts = None
            for ts_p, p in prices_in_fill_window:
                if not is_long and p >= charm_limit:
                    fill_ts = ts_p
                    break
                elif is_long and p <= charm_limit:
                    fill_ts = ts_p
                    break

            if fill_ts is None:
                fill_ts = prices_in_fill_window[0][0]  # fallback

            # Get prices after fill for trade simulation
            prices_after_fill = [(ts_p, p) for ts_p, p in prices if ts_p >= fill_ts]

            if len(prices_after_fill) < 2:
                errors.append({
                    "id": trade_id, "setup": setup_name, "ts": ts_et,
                    "reason": "Not enough prices after fill"
                })
                continue

            # Simulate the trade from charm_limit entry
            rm = get_rm(setup_name)
            max_hold = None
            if setup_name == "BofA Scalp":
                max_hold = 30  # typical BofA max hold

            new_result, new_pnl, new_mp, new_ml, new_elapsed, new_fe, exit_price = \
                simulate_trade_from_prices(charm_limit, direction, prices_after_fill, rm, max_hold)

            # Also compute what the OLD outcome would be from spot (market entry)
            prices_from_spot = [(ts_p, p) for ts_p, p in prices if ts_p >= prices[0][0]]
            old_sim_result, old_sim_pnl, _, _, _, _, _ = \
                simulate_trade_from_prices(spot, direction, prices_from_spot, rm, max_hold)

            filled_trades.append({
                "id": trade_id,
                "ts": ts_et,
                "setup": setup_name,
                "direction": direction,
                "grade": grade,
                "spot": spot,
                "charm_limit": charm_limit,
                "offset": round(charm_limit - spot, 1),
                "fill_delay_min": round((fill_ts - prices[0][0]).total_seconds() / 60, 1),
                "old_result": old_result,
                "old_pnl": old_pnl or 0,
                "old_sim_result": old_sim_result,
                "old_sim_pnl": old_sim_pnl,
                "new_result": new_result,
                "new_pnl": new_pnl,
                "new_max_profit": new_mp,
                "new_max_loss": new_ml,
                "new_elapsed": new_elapsed,
                "new_first_event": new_fe,
                "pnl_change": round(new_pnl - (old_pnl or 0), 1),
                "flipped": old_result != new_result,
            })

    # ── OUTPUT ──

    print(f"\n{'='*100}")
    print("A) FILLED TRADES (limit hit, real trade happened)")
    print(f"{'='*100}")
    print(f"\n{'ID':>6} | {'Time':>16} | {'Setup':<18} | {'Dir':>5} | {'Grd':>3} | {'Spot':>8} | {'Limit':>8} | {'Off':>5} | {'Fill':>5} | {'Old':>12} | {'New':>12} | {'Chg':>7} | Flip?")
    print("-" * 155)

    filled_wins = 0
    filled_losses = 0
    old_total_pnl = 0
    new_total_pnl = 0
    flips = []

    for ft in filled_trades:
        old_str = f"{ft['old_result']:>6} {ft['old_pnl']:>+6.1f}"
        new_str = f"{ft['new_result']:>6} {ft['new_pnl']:>+6.1f}"
        flip_str = "FLIP!" if ft["flipped"] else ""
        ts_str = ft["ts"].strftime("%m/%d %H:%M") if ft["ts"] else "?"

        print(f"{ft['id']:>6} | {ts_str:>16} | {ft['setup']:<18} | {ft['direction']:>5} | {ft['grade']:>3} | "
              f"{ft['spot']:>8.1f} | {ft['charm_limit']:>8.1f} | {ft['offset']:>+5.1f} | {ft['fill_delay_min']:>5.1f} | "
              f"{old_str} | {new_str} | {ft['pnl_change']:>+7.1f} | {flip_str}")

        old_total_pnl += ft["old_pnl"]
        new_total_pnl += ft["new_pnl"]
        if ft["new_result"] == "WIN":
            filled_wins += 1
        elif ft["new_result"] == "LOSS":
            filled_losses += 1
        if ft["flipped"]:
            flips.append(ft)

    filled_total = len(filled_trades)
    filled_expired = filled_total - filled_wins - filled_losses
    filled_wr = (filled_wins / filled_total * 100) if filled_total > 0 else 0

    print(f"\nFilled summary: {filled_total} trades | {filled_wins}W/{filled_losses}L/{filled_expired}E | WR: {filled_wr:.1f}%")
    print(f"  Old DB P&L: {old_total_pnl:>+8.1f} pts")
    print(f"  New P&L:    {new_total_pnl:>+8.1f} pts")
    print(f"  Change:     {new_total_pnl - old_total_pnl:>+8.1f} pts")

    if flips:
        print(f"\n  --- FLIPPED OUTCOMES ({len(flips)}) ---")
        for f in flips:
            ts_str = f["ts"].strftime("%m/%d %H:%M") if f["ts"] else "?"
            print(f"  ID {f['id']:>6} {ts_str} {f['setup']:<18} {f['old_result']}->{f['new_result']}  "
                  f"old={f['old_pnl']:>+.1f} new={f['new_pnl']:>+.1f}")

    print(f"\n{'='*100}")
    print("B) LIMIT_TIMEOUT TRADES (limit never hit, no trade)")
    print(f"{'='*100}")
    print(f"\n{'ID':>6} | {'Time':>16} | {'Setup':<18} | {'Dir':>5} | {'Grd':>3} | {'Spot':>8} | {'Limit':>8} | {'Off':>5} | {'HiSeen':>8} | {'Gap':>6} | {'Old':>12} | New")
    print("-" * 140)

    timeout_old_pnl = 0
    for tt in timeout_trades:
        old_str = f"{tt['old_result']:>6} {tt['old_pnl']:>+6.1f}"
        ts_str = tt["ts"].strftime("%m/%d %H:%M") if tt["ts"] else "?"

        print(f"{tt['id']:>6} | {ts_str:>16} | {tt['setup']:<18} | {tt['direction']:>5} | {tt['grade']:>3} | "
              f"{tt['spot']:>8.1f} | {tt['charm_limit']:>8.1f} | {tt['offset']:>+5.1f} | {tt['high_in_window']:>8.1f} | "
              f"{tt['gap_to_fill']:>+6.1f} | {old_str} | TIMEOUT  0.0")
        timeout_old_pnl += tt["old_pnl"]

    print(f"\nTimeout summary: {len(timeout_trades)} trades")
    print(f"  Old phantom P&L being removed: {timeout_old_pnl:>+8.1f} pts")
    print(f"  New P&L: 0.0 pts")

    print(f"\n{'='*100}")
    print("C) GRAND SUMMARY")
    print(f"{'='*100}")

    total_trades = len(filled_trades) + len(timeout_trades)
    total_old_pnl = old_total_pnl + timeout_old_pnl
    total_new_pnl = new_total_pnl + 0  # timeouts = 0

    print(f"\n{'Category':<16} | {'Trades':>6} | {'Old PnL':>10} | {'New PnL':>10} | {'Change':>10}")
    print("-" * 70)
    print(f"{'Filled':<16} | {filled_total:>6} | {old_total_pnl:>+10.1f} | {new_total_pnl:>+10.1f} | {new_total_pnl - old_total_pnl:>+10.1f}")
    print(f"{'Timeout':<16} | {len(timeout_trades):>6} | {timeout_old_pnl:>+10.1f} | {'0.0':>10} | {-timeout_old_pnl:>+10.1f}")
    print(f"{'-'*70}")
    print(f"{'TOTAL':<16} | {total_trades:>6} | {total_old_pnl:>+10.1f} | {total_new_pnl:>+10.1f} | {total_new_pnl - total_old_pnl:>+10.1f}")

    # ── Per-date breakdown ──
    print(f"\n{'='*100}")
    print("D) PER-DATE BREAKDOWN")
    print(f"{'='*100}")

    date_stats = {}
    for ft in filled_trades:
        d = ft["ts"].strftime("%Y-%m-%d") if ft["ts"] else "?"
        if d not in date_stats:
            date_stats[d] = {"filled": 0, "timeout": 0, "old_pnl": 0, "new_pnl": 0, "wins": 0, "losses": 0}
        date_stats[d]["filled"] += 1
        date_stats[d]["old_pnl"] += ft["old_pnl"]
        date_stats[d]["new_pnl"] += ft["new_pnl"]
        if ft["new_result"] == "WIN": date_stats[d]["wins"] += 1
        elif ft["new_result"] == "LOSS": date_stats[d]["losses"] += 1

    for tt in timeout_trades:
        d = tt["ts"].strftime("%Y-%m-%d") if tt["ts"] else "?"
        if d not in date_stats:
            date_stats[d] = {"filled": 0, "timeout": 0, "old_pnl": 0, "new_pnl": 0, "wins": 0, "losses": 0}
        date_stats[d]["timeout"] += 1
        date_stats[d]["old_pnl"] += tt["old_pnl"]
        # new_pnl for timeout = 0, already 0 in init

    print(f"\n{'Date':<12} | {'Filled':>6} | {'Timeout':>7} | {'W/L':>5} | {'WR%':>5} | {'Old PnL':>10} | {'New PnL':>10} | {'Change':>10}")
    print("-" * 90)
    for d in sorted(date_stats.keys()):
        s = date_stats[d]
        total = s["filled"] + s["timeout"]
        wl = f"{s['wins']}/{s['losses']}"
        wr = (s["wins"] / s["filled"] * 100) if s["filled"] > 0 else 0
        print(f"{d:<12} | {s['filled']:>6} | {s['timeout']:>7} | {wl:>5} | {wr:>5.0f}% | {s['old_pnl']:>+10.1f} | {s['new_pnl']:>+10.1f} | {s['new_pnl'] - s['old_pnl']:>+10.1f}")

    # ── Per-setup breakdown ──
    print(f"\n{'='*100}")
    print("E) PER-SETUP BREAKDOWN")
    print(f"{'='*100}")

    setup_stats = {}
    for ft in filled_trades:
        s = ft["setup"]
        if s not in setup_stats:
            setup_stats[s] = {"filled": 0, "timeout": 0, "old_pnl": 0, "new_pnl": 0, "wins": 0, "losses": 0}
        setup_stats[s]["filled"] += 1
        setup_stats[s]["old_pnl"] += ft["old_pnl"]
        setup_stats[s]["new_pnl"] += ft["new_pnl"]
        if ft["new_result"] == "WIN": setup_stats[s]["wins"] += 1
        elif ft["new_result"] == "LOSS": setup_stats[s]["losses"] += 1

    for tt in timeout_trades:
        s = tt["setup"]
        if s not in setup_stats:
            setup_stats[s] = {"filled": 0, "timeout": 0, "old_pnl": 0, "new_pnl": 0, "wins": 0, "losses": 0}
        setup_stats[s]["timeout"] += 1
        setup_stats[s]["old_pnl"] += tt["old_pnl"]

    print(f"\n{'Setup':<20} | {'Filled':>6} | {'Timeout':>7} | {'W/L':>5} | {'WR%':>5} | {'Old PnL':>10} | {'New PnL':>10} | {'Change':>10}")
    print("-" * 100)
    for s in sorted(setup_stats.keys()):
        st = setup_stats[s]
        total = st["filled"] + st["timeout"]
        wl = f"{st['wins']}/{st['losses']}"
        wr = (st["wins"] / st["filled"] * 100) if st["filled"] > 0 else 0
        print(f"{s:<20} | {st['filled']:>6} | {st['timeout']:>7} | {wl:>5} | {wr:>5.0f}% | {st['old_pnl']:>+10.1f} | {st['new_pnl']:>+10.1f} | {st['new_pnl'] - st['old_pnl']:>+10.1f}")

    # ── Errors ──
    if errors:
        print(f"\n{'='*100}")
        print(f"ERRORS ({len(errors)} trades could not be recomputed)")
        print(f"{'='*100}")
        for e in errors:
            ts_str = e["ts"].strftime("%m/%d %H:%M") if e["ts"] else "?"
            print(f"  ID {e['id']:>6} {ts_str} {e['setup']:<18} -- {e['reason']}")

    # ── Current vs DB comparison ──
    print(f"\n{'='*100}")
    print("F) SANITY CHECK: DB outcome vs simulated-from-spot")
    print("    (shows if DB outcomes were computed from spot or charm_limit)")
    print(f"{'='*100}")

    mismatches = 0
    for ft in filled_trades:
        db_pnl = ft["old_pnl"] or 0
        sim_spot_pnl = ft["old_sim_pnl"]
        sim_charm_pnl = ft["new_pnl"]

        # Check which simulation matches the DB better
        diff_spot = abs(db_pnl - sim_spot_pnl)
        diff_charm = abs(db_pnl - sim_charm_pnl)

        if diff_spot > 1.0 and diff_charm > 1.0:
            ts_str = ft["ts"].strftime("%m/%d %H:%M") if ft["ts"] else "?"
            print(f"  ID {ft['id']:>6} {ts_str} {ft['setup']:<18} | "
                  f"DB: {ft['old_result']} {db_pnl:>+.1f} | "
                  f"Sim(spot): {ft['old_sim_result']} {sim_spot_pnl:>+.1f} | "
                  f"Sim(charm): {ft['new_result']} {sim_charm_pnl:>+.1f} | "
                  f"Closer to: {'charm' if diff_charm < diff_spot else 'spot'}")
            mismatches += 1
        elif diff_spot <= 1.0 and diff_charm > 1.0:
            # DB matches spot sim — means DB used spot, not charm
            pass
        elif diff_charm <= 1.0 and diff_spot > 1.0:
            # DB matches charm sim — means DB used charm
            pass

    # Count how many match spot vs charm
    match_spot = 0
    match_charm = 0
    match_both = 0
    match_neither = 0
    for ft in filled_trades:
        db_pnl = ft["old_pnl"] or 0
        sim_spot_pnl = ft["old_sim_pnl"]
        sim_charm_pnl = ft["new_pnl"]
        diff_spot = abs(db_pnl - sim_spot_pnl)
        diff_charm = abs(db_pnl - sim_charm_pnl)
        if diff_spot <= 1.0 and diff_charm <= 1.0:
            match_both += 1
        elif diff_spot <= 1.0:
            match_spot += 1
        elif diff_charm <= 1.0:
            match_charm += 1
        else:
            match_neither += 1

    print(f"\n  DB outcome source analysis ({len(filled_trades)} filled trades):")
    print(f"    Matches spot-based sim:  {match_spot}")
    print(f"    Matches charm-based sim: {match_charm}")
    print(f"    Matches both (same):     {match_both}")
    print(f"    Matches neither:         {match_neither}")
    if mismatches > 0:
        print(f"\n  {mismatches} significant mismatches shown above.")

    conn.close()
    print(f"\n{'='*100}")
    print("DONE -- READ-ONLY, no DB changes made.")
    print(f"{'='*100}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
