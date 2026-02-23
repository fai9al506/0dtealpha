"""GEX Long simulation — detailed view of key scenarios with trade-by-trade breakdown"""
import os, datetime
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)

def replay_trade(conn, trade, stop_pts, target_pts=10, trail_activation=None, trail_gap=None):
    """Replay with configurable target"""
    ts_utc = trade['ts_utc']
    spot = float(trade['spot'])
    is_long = trade['direction'] == 'long'
    stop_lvl = spot - stop_pts if is_long else spot + stop_pts

    prices = conn.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as ts_et,
               ts as ts_utc, spot
        FROM chain_snapshots
        WHERE ts > :ts_start AND ts <= :ts_end AND spot IS NOT NULL
        ORDER BY ts ASC
    """), {"ts_start": ts_utc, "ts_end": ts_utc + datetime.timedelta(hours=7)}).mappings().all()

    if not prices:
        return trade['outcome_result'], float(trade['outcome_pnl'] or 0), 0, 0, 0

    max_profit = 0.0
    min_profit = 0.0
    trail_lock = None

    for p in prices:
        price = float(p['spot'])
        profit = (price - spot) if is_long else (spot - price)
        if profit > max_profit: max_profit = profit
        if profit < min_profit: min_profit = profit

        # Trail
        if trail_activation and trail_gap:
            if max_profit >= trail_activation:
                new_lock = max_profit - trail_gap
                if trail_lock is None or new_lock > trail_lock:
                    trail_lock = new_lock
                if profit <= trail_lock:
                    elapsed = (p['ts_utc'] - ts_utc).total_seconds() / 60
                    return ("WIN" if trail_lock > 0 else "LOSS"), round(trail_lock, 1), round(max_profit, 1), round(min_profit, 1), int(elapsed)

        # Target
        if profit >= target_pts:
            elapsed = (p['ts_utc'] - ts_utc).total_seconds() / 60
            return "WIN", target_pts, round(max_profit, 1), round(min_profit, 1), int(elapsed)

        # Stop
        if (is_long and price <= stop_lvl) or (not is_long and price >= stop_lvl):
            elapsed = (p['ts_utc'] - ts_utc).total_seconds() / 60
            return "LOSS", round(-stop_pts, 1), round(max_profit, 1), round(min_profit, 1), int(elapsed)

    last = float(prices[-1]['spot'])
    pnl = (last - spot) if is_long else (spot - last)
    if trail_lock is not None and pnl < trail_lock: pnl = trail_lock
    elapsed = (prices[-1]['ts_utc'] - ts_utc).total_seconds() / 60
    return "EXPIRED", round(pnl, 1), round(max_profit, 1), round(min_profit, 1), int(elapsed)


with engine.begin() as conn:
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

    # Scenario definitions - more refined combos
    scenarios = [
        ("CURRENT (DB stored results)", None),
        ("SIM-BASELINE: 8pt stop, 10pt tgt (replay)", {"stop": 8, "tgt": 10}),
        ("SIM A: No MESSY, all hours, 8pt stop", {"stop": 8, "tgt": 10, "no_messy": True}),
        ("SIM B: No MESSY, before 14:00, 8pt stop", {"stop": 8, "tgt": 10, "no_messy": True, "before": 14}),
        ("SIM C: No MESSY, before 14:00, 12pt stop", {"stop": 12, "tgt": 10, "no_messy": True, "before": 14}),
        ("SIM D: No MESSY, before 14:00, 12pt stop, trail(20,5)", {"stop": 12, "tgt": None, "no_messy": True, "before": 14, "trail": (20, 5)}),
        ("SIM E: PURE/GEX only, all hours, 8pt stop", {"stop": 8, "tgt": 10, "pure_only": True}),
        ("SIM F: Morning only (9:30-11), 8pt stop", {"stop": 8, "tgt": 10, "morning": True}),
        ("SIM G: No MESSY, morning, 8pt stop", {"stop": 8, "tgt": 10, "no_messy": True, "morning": True}),
        ("SIM H: No MESSY, before 14:00, 10pt stop", {"stop": 10, "tgt": 10, "no_messy": True, "before": 14}),
        ("SIM I: All trades, 12pt stop, trail(20,5)", {"stop": 12, "tgt": None, "trail": (20, 5)}),
        ("SIM J: No MESSY, before 14, 12pt stop, 15pt tgt", {"stop": 12, "tgt": 15, "no_messy": True, "before": 14}),
        ("SIM K: Grade A+ only, all hours, 8pt stop", {"stop": 8, "tgt": 10, "grade_min": "A+"}),
        ("SIM L: Grade A/A+ only, all hours, 8pt stop", {"stop": 8, "tgt": 10, "grade_min": "A"}),
        ("SIM M: No MESSY/TARGET, before 14, 12pt stop", {"stop": 12, "tgt": 10, "no_messy": True, "no_target": True, "before": 14}),
    ]

    all_results = []

    for name, cfg in scenarios:
        if cfg is None:
            # Use DB stored results
            total_pnl = sum(float(t['outcome_pnl']) for t in trades)
            wins = sum(1 for t in trades if t['outcome_result'] == 'WIN')
            losses = sum(1 for t in trades if t['outcome_result'] == 'LOSS')
            expired = len(trades) - wins - losses
            wr = 100 * wins / len(trades)
            all_results.append((name, len(trades), 0, wins, losses, expired, wr, total_pnl, []))
            continue

        included = []
        excluded = 0
        for t in trades:
            ts_et = t['ts_et']
            par = t['paradigm'] or ''
            grade = t['grade'] or ''
            hour = ts_et.hour + ts_et.minute / 60

            # Filters
            if cfg.get('no_messy') and 'MESSY' in par.upper():
                excluded += 1; continue
            if cfg.get('no_target') and 'TARGET' in par.upper():
                excluded += 1; continue
            if cfg.get('pure_only') and par not in ('GEX-PURE', 'GEX'):
                excluded += 1; continue
            if cfg.get('morning') and (hour < 9.5 or hour >= 11):
                excluded += 1; continue
            if cfg.get('before') and hour >= cfg['before']:
                excluded += 1; continue

            grade_order = {"A+": 3, "A": 2, "A-Entry": 1}
            if cfg.get('grade_min'):
                min_rank = grade_order.get(cfg['grade_min'], 0)
                if grade_order.get(grade, 0) < min_rank:
                    excluded += 1; continue

            included.append(t)

        # Replay
        trade_details = []
        total_pnl = 0
        wins = losses = expired = 0
        for t in included:
            trail = cfg.get('trail')
            tgt = cfg.get('tgt', 10)
            if trail:
                res, pnl, mp, mn, el = replay_trade(conn, t, cfg['stop'], tgt or 999, trail[0], trail[1])
            else:
                res, pnl, mp, mn, el = replay_trade(conn, t, cfg['stop'], tgt or 10)
            total_pnl += pnl
            if res == 'WIN': wins += 1
            elif res == 'LOSS': losses += 1
            else: expired += 1
            trade_details.append((t, res, pnl, mp, mn, el))

        total = len(included)
        wr = 100 * wins / total if total > 0 else 0
        all_results.append((name, total, excluded, wins, losses, expired, wr, total_pnl, trade_details))

    # Summary table
    print(f"\n{'Scenario':<55} {'#':>3} {'Flt':>3} {'W':>2} {'L':>2} {'E':>2} {'WR%':>5} {'PnL':>7} {'Avg':>6}")
    print("-" * 95)
    for name, total, filt, w, l, e, wr, pnl, _ in all_results:
        avg = pnl / total if total > 0 else 0
        print(f"{name:<55} {total:>3} {filt:>3} {w:>2} {l:>2} {e:>2} {wr:>4.0f}% {pnl:>+7.1f} {avg:>+6.1f}")

    # Detail for most promising scenarios
    for name, total, filt, w, l, e, wr, pnl, details in all_results:
        if not details or total < 3:
            continue
        if wr < 40 and pnl < 0:
            continue
        print(f"\n{'=' * 110}")
        print(f"{name}  --  {total} trades, {w}W/{l}L/{e}E, WR={wr:.0f}%, PnL={pnl:+.1f}")
        print(f"{'=' * 110}")
        print(f"{'#':>4} {'Date':>5} {'Time':>5} {'Paradigm':>12} {'Spot':>7} {'Grd':>5} | {'DB_Res':>7} {'Sim':>7} {'PnL':>7} {'MaxP':>6} {'MaxAdv':>6} {'Min':>4}")
        print("-" * 95)
        for t, res, sim_pnl, mp, mn, el in details:
            par = (t['paradigm'] or '')[:12]
            changed = " ***" if res != t['outcome_result'] else ""
            print(f"{t['id']:>4} {t['ts_et'].strftime('%m/%d'):>5} {t['ts_et'].strftime('%H:%M'):>5} {par:>12} {float(t['spot']):>7.1f} {t['grade']:>5} | {t['outcome_result']:>7} {res:>7} {sim_pnl:>+7.1f} {mp:>+6.1f} {mn:>+6.1f} {el:>4}{changed}")
