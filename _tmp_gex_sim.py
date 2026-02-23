"""Simulate GEX Long filter changes against actual trade data.
For each trade, replay price action from playback snapshots (chain_snapshots)
with different stop/filter combos to see hypothetical outcomes."""
import os, datetime
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)

def replay_trade(conn, trade, stop_pts, trail_activation=None, trail_gap=None):
    """Replay a trade with given stop, optionally with continuous trail.
    Returns (result, pnl, max_profit, min_after_entry, time_to_resolution)"""
    ts_utc = trade['ts_utc']
    spot = float(trade['spot'])
    direction = trade['direction']  # always 'long' for GEX Long
    is_long = direction == 'long'

    stop_lvl = spot - stop_pts if is_long else spot + stop_pts

    # Get price history from chain_snapshots (has 'spot' column directly)
    prices = conn.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as ts_et,
               ts as ts_utc,
               spot
        FROM chain_snapshots
        WHERE ts > :ts_start
          AND ts <= :ts_end
          AND spot IS NOT NULL
        ORDER BY ts ASC
    """), {
        "ts_start": ts_utc,
        "ts_end": ts_utc + datetime.timedelta(hours=7)  # until market close
    }).mappings().all()

    if not prices:
        # Fallback: return stored outcome
        return trade['outcome_result'], float(trade['outcome_pnl'] or 0), 0, 0, 0

    max_profit = 0.0
    min_profit = 0.0
    trail_lock = None

    for p in prices:
        price = float(p['spot'])
        profit = (price - spot) if is_long else (spot - price)

        if profit > max_profit:
            max_profit = profit
        if profit < min_profit:
            min_profit = profit

        # Trail logic (continuous, like DD)
        if trail_activation is not None and trail_gap is not None:
            if max_profit >= trail_activation:
                new_lock = max_profit - trail_gap
                if trail_lock is None or new_lock > trail_lock:
                    trail_lock = new_lock
                # Check trail stop
                if profit <= trail_lock:
                    pnl = trail_lock
                    elapsed = (p['ts_utc'] - ts_utc).total_seconds() / 60
                    result = "WIN" if pnl > 0 else "LOSS"
                    return result, round(pnl, 1), round(max_profit, 1), round(min_profit, 1), int(elapsed)

        # Fixed target check (10pt)
        if profit >= 10:
            elapsed = (p['ts_utc'] - ts_utc).total_seconds() / 60
            return "WIN", 10.0, round(max_profit, 1), round(min_profit, 1), int(elapsed)

        # Stop check
        if is_long and price <= stop_lvl:
            pnl = -(stop_pts)
            elapsed = (p['ts_utc'] - ts_utc).total_seconds() / 60
            return "LOSS", round(pnl, 1), round(max_profit, 1), round(min_profit, 1), int(elapsed)
        elif not is_long and price >= stop_lvl:
            pnl = -(stop_pts)
            elapsed = (p['ts_utc'] - ts_utc).total_seconds() / 60
            return "LOSS", round(pnl, 1), round(max_profit, 1), round(min_profit, 1), int(elapsed)

    # Market close — mark to market
    last_price = float(prices[-1]['spot'])
    pnl = (last_price - spot) if is_long else (spot - last_price)

    # Check trail at close
    if trail_lock is not None and pnl < trail_lock:
        pnl = trail_lock

    elapsed = (prices[-1]['ts_utc'] - ts_utc).total_seconds() / 60
    result = "EXPIRED"
    return result, round(pnl, 1), round(max_profit, 1), round(min_profit, 1), int(elapsed)


with engine.begin() as conn:
    # Get all GEX Long trades
    trades = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               ts as ts_utc,
               direction, grade, score, paradigm,
               spot, lis, target,
               max_plus_gex, max_minus_gex,
               gap_to_lis, first_hour,
               outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss
        FROM setup_log
        WHERE setup_name = 'GEX Long' AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    print(f"Total GEX Long trades: {len(trades)}")
    print()

    # Define scenarios
    scenarios = {
        "CURRENT (8pt stop, all paradigms, all hours)": {
            "paradigm_filter": None,  # accept all
            "time_filter": None,      # accept all hours
            "stop_pts": 8,
            "trail": None,
        },
        "FIX 1: Paradigm PURE/GEX only": {
            "paradigm_filter": ["GEX-PURE", "GEX"],
            "time_filter": None,
            "stop_pts": 8,
            "trail": None,
        },
        "FIX 2: Morning only (9:30-11:00)": {
            "paradigm_filter": None,
            "time_filter": (9, 30, 11, 0),  # start_h, start_m, end_h, end_m
            "stop_pts": 8,
            "trail": None,
        },
        "FIX 3: Wider stop (12pts)": {
            "paradigm_filter": None,
            "time_filter": None,
            "stop_pts": 12,
            "trail": None,
        },
        "FIX 4: Wider stop (15pts)": {
            "paradigm_filter": None,
            "time_filter": None,
            "stop_pts": 15,
            "trail": None,
        },
        "FIX 5: 12pt stop + continuous trail (act=12, gap=5)": {
            "paradigm_filter": None,
            "time_filter": None,
            "stop_pts": 12,
            "trail": (12, 5),
        },
        "FIX 6: 15pt stop + continuous trail (act=15, gap=5)": {
            "paradigm_filter": None,
            "time_filter": None,
            "stop_pts": 15,
            "trail": (15, 5),
        },
        "COMBO A: PURE paradigm + morning only": {
            "paradigm_filter": ["GEX-PURE", "GEX"],
            "time_filter": (9, 30, 11, 0),
            "stop_pts": 8,
            "trail": None,
        },
        "COMBO B: PURE paradigm + 12pt stop": {
            "paradigm_filter": ["GEX-PURE", "GEX"],
            "time_filter": None,
            "stop_pts": 12,
            "trail": None,
        },
        "COMBO C: Morning + 12pt stop": {
            "paradigm_filter": None,
            "time_filter": (9, 30, 11, 0),
            "stop_pts": 12,
            "trail": None,
        },
        "COMBO D: PURE + morning + 12pt stop": {
            "paradigm_filter": ["GEX-PURE", "GEX"],
            "time_filter": (9, 30, 11, 0),
            "stop_pts": 12,
            "trail": None,
        },
        "COMBO E: Morning + 12pt stop + trail (act=12, gap=5)": {
            "paradigm_filter": None,
            "time_filter": (9, 30, 11, 0),
            "stop_pts": 12,
            "trail": (12, 5),
        },
        "COMBO F: PURE + morning + 15pt stop + trail (act=15, gap=5)": {
            "paradigm_filter": ["GEX-PURE", "GEX"],
            "time_filter": (9, 30, 11, 0),
            "stop_pts": 15,
            "trail": (15, 5),
        },
        "COMBO G: No MESSY paradigm + before 14:00": {
            "paradigm_filter": "NO_MESSY",  # special: exclude MESSY
            "time_filter": (9, 30, 14, 0),
            "stop_pts": 8,
            "trail": None,
        },
        "COMBO H: No MESSY + before 14:00 + 12pt stop": {
            "paradigm_filter": "NO_MESSY",
            "time_filter": (9, 30, 14, 0),
            "stop_pts": 12,
            "trail": None,
        },
    }

    # Run all scenarios
    print("=" * 130)
    print("SCENARIO COMPARISON")
    print("=" * 130)

    results_table = []

    for name, cfg in scenarios.items():
        trades_in = []
        trades_out = []

        for t in trades:
            ts_et = t['ts_et']
            paradigm = t['paradigm'] or ''

            # Paradigm filter
            if cfg['paradigm_filter'] == "NO_MESSY":
                if 'MESSY' in paradigm.upper():
                    trades_out.append(t)
                    continue
            elif cfg['paradigm_filter'] is not None:
                if paradigm not in cfg['paradigm_filter']:
                    trades_out.append(t)
                    continue

            # Time filter
            if cfg['time_filter'] is not None:
                sh, sm, eh, em = cfg['time_filter']
                t_time = ts_et.hour * 60 + ts_et.minute
                start = sh * 60 + sm
                end = eh * 60 + em
                if t_time < start or t_time >= end:
                    trades_out.append(t)
                    continue

            trades_in.append(t)

        # Replay included trades
        total_pnl = 0
        wins, losses, expired = 0, 0, 0
        trade_results = []

        for t in trades_in:
            trail = cfg['trail']
            if trail:
                result, pnl, mp, mn, elapsed = replay_trade(conn, t, cfg['stop_pts'], trail[0], trail[1])
            else:
                result, pnl, mp, mn, elapsed = replay_trade(conn, t, cfg['stop_pts'])
            total_pnl += pnl
            if result == 'WIN': wins += 1
            elif result == 'LOSS': losses += 1
            else: expired += 1
            trade_results.append((t, result, pnl, mp, mn, elapsed))

        total = len(trades_in)
        wr = 100 * wins / total if total > 0 else 0
        filtered = len(trades_out)
        avg = total_pnl / total if total > 0 else 0

        results_table.append((name, total, filtered, wins, losses, expired, wr, total_pnl, avg, trade_results))

    # Print summary table
    print(f"\n{'Scenario':<55} {'Trades':>6} {'Filt':>5} {'W':>3} {'L':>3} {'E':>3} {'WR%':>6} {'PnL':>8} {'Avg':>6}")
    print("-" * 100)
    for name, total, filt, w, l, e, wr, pnl, avg, _ in results_table:
        print(f"{name:<55} {total:>6} {filt:>5} {w:>3} {l:>3} {e:>3} {wr:>5.1f}% {pnl:>+8.1f} {avg:>+6.1f}")

    # Print detailed per-trade results for top 3 scenarios
    print("\n")
    # Find top 3 by PnL (excluding CURRENT)
    ranked = sorted(results_table[1:], key=lambda x: x[7], reverse=True)[:5]

    for name, total, filt, w, l, e, wr, pnl, avg, trade_results in ranked:
        print(f"\n{'=' * 100}")
        print(f"DETAIL: {name}")
        print(f"{'=' * 100}")
        print(f"{'#':>4} {'Date':>5} {'Time':>5} {'Paradigm':>12} {'Spot':>7} {'Grd':>5} | {'Orig':>7} {'Sim':>7} {'SimPnL':>7} {'MaxP':>6} {'Min':>6} {'Mins':>5}")
        print("-" * 90)
        for t, result, sim_pnl, mp, mn, elapsed in trade_results:
            orig_pnl = float(t['outcome_pnl'])
            changed = " ***" if result != t['outcome_result'] else ""
            par = (t['paradigm'] or '')[:12]
            print(f"{t['id']:>4} {t['ts_et'].strftime('%m/%d'):>5} {t['ts_et'].strftime('%H:%M'):>5} {par:>12} {float(t['spot']):>7.1f} {t['grade']:>5} | {t['outcome_result']:>7} {result:>7} {sim_pnl:>+7.1f} {mp:>+6.1f} {mn:>+6.1f} {elapsed:>5}{changed}")
        print(f"\nTotal: {total} trades, {w}W/{l}L/{e}E, WR={wr:.1f}%, PnL={pnl:+.1f}")
