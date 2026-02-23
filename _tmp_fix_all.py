"""Fix GEX Long trades #62 and #80: initial stop hit but marked WIN.
Also verify all today's (Feb 20) trades."""
import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

with engine.begin() as conn:
    # === FIX trades #62 and #80 ===
    for tid in [62, 80]:
        t = conn.execute(text("""
            SELECT id, ts, setup_name, direction, spot, outcome_result, outcome_pnl, outcome_stop_level
            FROM setup_log WHERE id = :id
        """), {"id": tid}).mappings().first()

        entry = float(t['spot'])
        is_long = t['direction'].lower() in ('long', 'bullish')
        initial_stop_pts = 8  # GEX Long
        initial_stop = entry - initial_stop_pts if is_long else entry + initial_stop_pts
        pnl = initial_stop - entry if is_long else entry - initial_stop  # = -8

        print(f"FIX #{tid}: {t['setup_name']} {t['direction']} entry={entry:.1f}")
        print(f"  BEFORE: result={t['outcome_result']} PNL={t['outcome_pnl']} stop={t['outcome_stop_level']}")

        conn.execute(text("""
            UPDATE setup_log
            SET outcome_result = 'LOSS', outcome_pnl = :pnl, outcome_stop_level = :stop
            WHERE id = :id
        """), {"id": tid, "pnl": round(pnl, 2), "stop": round(initial_stop, 2)})

        after = conn.execute(text("""
            SELECT outcome_result, outcome_pnl, outcome_stop_level
            FROM setup_log WHERE id = :id
        """), {"id": tid}).mappings().first()
        print(f"  AFTER:  result={after['outcome_result']} PNL={after['outcome_pnl']} stop={after['outcome_stop_level']}")
        print()

    # === CHECK ALL TODAY'S TRADES (Feb 20, 2026) ===
    print("=" * 80)
    print("ALL TODAY'S TRADES (Feb 20, 2026)")
    print("=" * 80)

    today_trades = conn.execute(text("""
        SELECT id, ts, setup_name, direction, spot, grade, score,
               outcome_result, outcome_pnl, outcome_stop_level
        FROM setup_log
        WHERE ts::date = '2026-02-20'
        ORDER BY ts ASC
    """)).mappings().all()

    print(f"Found {len(today_trades)} trades today\n")

    CONFIGS = {
        "DD Exhaustion": {"initial_stop_pts": 12, "target_pts": None, "trail_activation": 20, "trail_gap": 5, "mode": "continuous"},
        "GEX Long": {"initial_stop_pts": 8, "target_pts": None, "trail_activation": 12, "trail_gap": 5, "mode": "rung"},
        "AG Short": {"initial_stop_pts": 20},  # varies, but max 20
        "BofA Scalp": {},
        "Paradigm Reversal": {"initial_stop_pts": 15, "target_pts": 10},
        "ES Absorption": {"initial_stop_pts": 12, "target_pts": 10},
    }

    day_total = 0
    issues_found = 0

    for t in today_trades:
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
            WHERE ts >= :start AND ts::date = '2026-02-20'
            ORDER BY ts ASC
        """), {"start": t['ts']}).mappings().all()

        # Calculate max favorable and max adverse
        max_fav = 0
        max_adv = 0
        initial_stop_hit = None
        for p in prices:
            price = float(p['spot'])
            profit = (price - entry) if is_long else (entry - price)
            if profit > max_fav:
                max_fav = profit
            if -profit > max_adv:
                max_adv = -profit

            # Check initial stop
            stop_pts = cfg.get('initial_stop_pts')
            if stop_pts and not initial_stop_hit:
                initial_stop = entry - stop_pts if is_long else entry + stop_pts
                if is_long and price <= initial_stop:
                    initial_stop_hit = p['ts']
                elif not is_long and price >= initial_stop:
                    initial_stop_hit = p['ts']

        # Check for issues
        issue = ""
        if initial_stop_hit and result == 'WIN':
            issue = " *** ISSUE: SL hit before WIN ***"
            issues_found += 1

        print(f"  #{t['id']:3d} {setup:16s} {t['direction']:5s} entry={entry:.1f} "
              f"grade={t['grade']} result={result:7s} PNL={pnl:+6.1f} "
              f"max_fav={max_fav:.1f} max_adv={max_adv:.1f} "
              f"SL_hit={'YES' if initial_stop_hit else 'no'}{issue}")

    print(f"\nToday's resolved PNL total: {day_total:+.1f}")
    if issues_found:
        print(f"*** {issues_found} ISSUES FOUND ***")
    else:
        print("All today's trades look correct.")

    # === UPDATED GRAND TOTALS ===
    print("\n" + "=" * 80)
    print("UPDATED GRAND TOTALS (after fixes)")
    print("=" * 80)
    all_totals = conn.execute(text("""
        SELECT setup_name,
               COUNT(*) FILTER (WHERE outcome_result = 'WIN') as wins,
               COUNT(*) FILTER (WHERE outcome_result = 'LOSS') as losses,
               COUNT(*) FILTER (WHERE outcome_result = 'EXPIRED') as expired,
               COUNT(*) FILTER (WHERE outcome_result IS NULL) as open,
               SUM(outcome_pnl) FILTER (WHERE outcome_result IS NOT NULL) as net_pnl,
               COUNT(*) as total
        FROM setup_log
        GROUP BY setup_name
        ORDER BY setup_name
    """)).mappings().all()

    grand = 0
    for t in all_totals:
        net = float(t['net_pnl'] or 0)
        grand += net
        resolved = int(t['wins'] or 0) + int(t['losses'] or 0) + int(t['expired'] or 0)
        wr = int(t['wins'] or 0) / resolved * 100 if resolved > 0 else 0
        print(f"  {t['setup_name']:16s}: W={t['wins'] or 0} L={t['losses'] or 0} X={t['expired'] or 0} Open={t['open'] or 0} | "
              f"WR={wr:.0f}% | NET={net:+.1f}")
    print(f"\n  GRAND TOTAL: {grand:+.1f}")
