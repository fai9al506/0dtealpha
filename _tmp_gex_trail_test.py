"""Test different trail activation levels for GEX Long (No MESSY, before 14:00, 12pt stop)"""
import os, datetime
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)

def replay(conn, trade, stop_pts, trail_act, trail_gap):
    ts_utc = trade['ts_utc']
    spot = float(trade['spot'])
    is_long = trade['direction'] == 'long'
    stop_lvl = spot - stop_pts if is_long else spot + stop_pts

    prices = conn.execute(text("""
        SELECT ts as ts_utc, spot FROM chain_snapshots
        WHERE ts > :t1 AND ts <= :t2 AND spot IS NOT NULL
        ORDER BY ts ASC
    """), {"t1": ts_utc, "t2": ts_utc + datetime.timedelta(hours=7)}).mappings().all()

    if not prices:
        return trade['outcome_result'], float(trade['outcome_pnl'] or 0), 0

    max_profit = 0.0
    trail_lock = None

    for p in prices:
        price = float(p['spot'])
        profit = (price - spot) if is_long else (spot - price)
        if profit > max_profit:
            max_profit = profit

        # Trail
        if max_profit >= trail_act:
            new_lock = max_profit - trail_gap
            if trail_lock is None or new_lock > trail_lock:
                trail_lock = new_lock
            if profit <= trail_lock:
                return ("WIN" if trail_lock > 0 else "LOSS"), round(trail_lock, 1), round(max_profit, 1)

        # Stop
        if (is_long and price <= stop_lvl) or (not is_long and price >= stop_lvl):
            return "LOSS", round(-stop_pts, 1), round(max_profit, 1)

    # Market close
    last = float(prices[-1]['spot'])
    pnl = (last - spot) if is_long else (spot - last)
    if trail_lock is not None and pnl < trail_lock:
        pnl = trail_lock
    return "EXPIRED", round(pnl, 1), round(max_profit, 1)


with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               ts as ts_utc, direction, grade, paradigm,
               spot, outcome_result, outcome_pnl
        FROM setup_log
        WHERE setup_name = 'GEX Long' AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    # Filter: No MESSY, before 14:00
    filtered = []
    for t in trades:
        par = t['paradigm'] or ''
        if 'MESSY' in par.upper():
            continue
        if t['ts_et'].hour >= 14:
            continue
        filtered.append(t)

    print(f"Filtered trades: {len(filtered)} (no MESSY, before 14:00)")
    print()

    # Test activation levels from 5 to 30, gap 3 to 7
    configs = []
    for act in [5, 7, 8, 10, 12, 15, 18, 20, 25]:
        for gap in [3, 5, 7]:
            configs.append((act, gap))

    # Also test fixed targets for comparison
    print(f"{'Config':<25} {'W':>2} {'L':>2} {'E':>2} {'WR%':>5} {'PnL':>7} {'Avg':>6}  Trade-by-trade PnL")
    print("-" * 110)

    # Fixed 10pt target baseline
    for tgt in [10, 15]:
        wins = losses = exp = 0
        total_pnl = 0
        pnls = []
        for t in filtered:
            # Simple replay with fixed target, no trail
            ts_utc = t['ts_utc']
            spot = float(t['spot'])
            is_long = t['direction'] == 'long'
            stop_lvl = spot - 12

            prices = conn.execute(text("""
                SELECT spot FROM chain_snapshots
                WHERE ts > :t1 AND ts <= :t2 AND spot IS NOT NULL ORDER BY ts ASC
            """), {"t1": ts_utc, "t2": ts_utc + datetime.timedelta(hours=7)}).mappings().all()

            hit = False
            for p in prices:
                price = float(p['spot'])
                profit = price - spot if is_long else spot - price
                if profit >= tgt:
                    wins += 1; total_pnl += tgt; pnls.append(f"+{tgt}"); hit = True; break
                if (is_long and price <= stop_lvl) or (not is_long and price >= stop_lvl):
                    losses += 1; total_pnl -= 12; pnls.append("-12"); hit = True; break
            if not hit:
                last = float(prices[-1]['spot']) if prices else spot
                p = (last - spot) if is_long else (spot - last)
                exp += 1; total_pnl += p; pnls.append(f"{p:+.0f}")

        total = wins + losses + exp
        wr = 100 * wins / total if total else 0
        avg = total_pnl / total if total else 0
        pnl_str = ", ".join(pnls)
        print(f"{'Fixed tgt=' + str(tgt) + ', stop=12':<25} {wins:>2} {losses:>2} {exp:>2} {wr:>4.0f}% {total_pnl:>+7.1f} {avg:>+6.1f}  [{pnl_str}]")

    print()

    # Trail configs
    for act, gap in configs:
        wins = losses = exp = 0
        total_pnl = 0.0
        pnls = []

        for t in filtered:
            res, pnl, mp = replay(conn, t, 12, act, gap)
            total_pnl += pnl
            if res == 'WIN': wins += 1
            elif res == 'LOSS': losses += 1
            else: exp += 1
            pnls.append(f"{pnl:+.0f}")

        total = wins + losses + exp
        wr = 100 * wins / total if total else 0
        avg = total_pnl / total if total else 0
        pnl_str = ", ".join(pnls)
        label = f"act={act}, gap={gap}, stop=12"
        print(f"{label:<25} {wins:>2} {losses:>2} {exp:>2} {wr:>4.0f}% {total_pnl:>+7.1f} {avg:>+6.1f}  [{pnl_str}]")
