"""Check all Feb 19 trades in detail"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

CONFIGS = {
    "DD Exhaustion": {"initial_stop_pts": 12, "trail_activation": 20, "trail_gap": 5},
    "GEX Long": {"initial_stop_pts": 8, "trail_activation": 12, "trail_gap": 5},
    "AG Short": {"initial_stop_pts": 20},
    "BofA Scalp": {},
    "Paradigm Reversal": {"initial_stop_pts": 15, "target_pts": 10},
    "ES Absorption": {"initial_stop_pts": 12, "target_pts": 10},
}

with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT id, ts, setup_name, direction, spot, grade, score,
               outcome_result, outcome_pnl, outcome_stop_level
        FROM setup_log
        WHERE ts::date = '2026-02-19'
        ORDER BY ts ASC
    """)).mappings().all()

    print(f"Feb 19 trades: {len(trades)}")
    print()

    day_total = 0
    for t in trades:
        entry = float(t['spot'])
        is_long = t['direction'].lower() in ('long', 'bullish')
        setup = t['setup_name']
        cfg = CONFIGS.get(setup, {})
        result = t['outcome_result'] or 'OPEN'
        pnl = float(t['outcome_pnl'] or 0)
        if result != 'OPEN':
            day_total += pnl

        # Get price path
        prices = conn.execute(text("""
            SELECT ts, spot FROM playback_snapshots
            WHERE ts >= :start AND ts::date = '2026-02-19'
            ORDER BY ts ASC
        """), {"start": t['ts']}).mappings().all()

        max_fav = 0
        max_adv = 0
        initial_stop_hit = False
        for p in prices:
            price = float(p['spot'])
            profit = (price - entry) if is_long else (entry - price)
            if profit > max_fav:
                max_fav = profit
            if -profit > max_adv:
                max_adv = -profit
            stop_pts = cfg.get('initial_stop_pts')
            if stop_pts and not initial_stop_hit:
                initial_stop = entry - stop_pts if is_long else entry + stop_pts
                if (is_long and price <= initial_stop) or (not is_long and price >= initial_stop):
                    initial_stop_hit = True

        issue = ""
        if initial_stop_hit and result == 'WIN':
            issue = " *** ISSUE ***"

        ts_str = str(t['ts'])[11:19]
        print(f"  #{t['id']:3d} {ts_str} {setup:16s} {t['direction']:5s} @{entry:.1f} "
              f"grade={t['grade']} {result:7s} PNL={pnl:+6.1f} "
              f"max_fav={max_fav:.1f} max_adv={max_adv:.1f} SL_hit={'Y' if initial_stop_hit else 'N'}{issue}")

    print(f"\nFeb 19 resolved PNL total: {day_total:+.1f}")
    print(f"Feb 19 trade count: {len(trades)}")

    # Also show per-setup breakdown for Feb 19
    breakdown = conn.execute(text("""
        SELECT setup_name, outcome_result, COUNT(*) as cnt, SUM(outcome_pnl) as pnl
        FROM setup_log
        WHERE ts::date = '2026-02-19' AND outcome_result IS NOT NULL
        GROUP BY setup_name, outcome_result
        ORDER BY setup_name, outcome_result
    """)).mappings().all()

    print("\nPer-setup breakdown:")
    current = None
    net = 0
    for b in breakdown:
        if b['setup_name'] != current:
            if current:
                print(f"    NET: {net:+.1f}")
            current = b['setup_name']
            net = 0
            print(f"  {current}:")
        p = float(b['pnl'] or 0)
        net += p
        print(f"    {b['outcome_result']}: {b['cnt']} trades, {p:+.1f}")
    if current:
        print(f"    NET: {net:+.1f}")
