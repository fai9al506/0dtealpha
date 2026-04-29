"""Overall stats after Mar 26 cleanup."""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["DATABASE_URL"])

with e.connect() as c:
    # Per-setup totals (only resolved trades)
    rows = c.execute(text("""
        SELECT setup_name,
               COUNT(*) as total,
               SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as l,
               SUM(CASE WHEN outcome_result='EXPIRED' THEN 1 ELSE 0 END) as x,
               ROUND(COALESCE(SUM(outcome_pnl),0)::numeric, 1) as pnl,
               ROUND(COALESCE(MAX(outcome_pnl),0)::numeric, 1) as best,
               ROUND(COALESCE(MIN(outcome_pnl),0)::numeric, 1) as worst
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY setup_name
        ORDER BY COALESCE(SUM(outcome_pnl),0) DESC
    """)).mappings().all()

    print("=== Per-Setup (after cleanup) ===\n")
    print(f"{'Setup':22s} | {'Trades':>6s} | {'W':>3s} | {'L':>3s} | {'X':>3s} | {'WR':>5s} | {'PnL':>8s} | {'Best':>6s} | {'Worst':>6s}")
    print("-" * 95)
    grand_t = grand_w = grand_l = grand_x = 0
    grand_pnl = 0
    for r in rows:
        t, w, l, x = int(r['total']), int(r['w']), int(r['l']), int(r['x'])
        pnl = float(r['pnl'])
        wr = f"{100*w/(w+l):.0f}%" if (w+l) > 0 else "n/a"
        print(f"{r['setup_name']:22s} | {t:6d} | {w:3d} | {l:3d} | {x:3d} | {wr:>5s} | {pnl:+8.1f} | {float(r['best']):+6.1f} | {float(r['worst']):+6.1f}")
        grand_t += t; grand_w += w; grand_l += l; grand_x += x; grand_pnl += pnl

    print("-" * 95)
    gwr = f"{100*grand_w/(grand_w+grand_l):.0f}%" if (grand_w+grand_l) > 0 else "n/a"
    print(f"{'GRAND TOTAL':22s} | {grand_t:6d} | {grand_w:3d} | {grand_l:3d} | {grand_x:3d} | {gwr:>5s} | {grand_pnl:+8.1f} |")

    # Unresolved (pending) count
    pending = c.execute(text("""
        SELECT COUNT(*) FROM setup_log WHERE outcome_result IS NULL
    """)).scalar()
    # How many of those are the cleared Mar 26 ones
    cleared = c.execute(text("""
        SELECT COUNT(*) FROM setup_log
        WHERE outcome_result IS NULL AND ts::date = '2026-03-26'
    """)).scalar()
    cleared_m24 = c.execute(text("""
        SELECT COUNT(*) FROM setup_log
        WHERE outcome_result IS NULL AND ts::date = '2026-03-24'
          AND setup_name = 'SB2 Absorption'
    """)).scalar()

    print(f"\nUnresolved: {pending} total ({cleared} cleared from Mar 26, {cleared_m24} cleared from Mar 24 SB2)")

    # Date range
    dr = c.execute(text("""
        SELECT MIN(ts)::date as first, MAX(ts)::date as last,
               COUNT(DISTINCT ts::date) as days
        FROM setup_log WHERE outcome_result IS NOT NULL
    """)).mappings().first()
    print(f"Date range: {dr['first']} → {dr['last']} ({dr['days']} trading days)")
    print(f"Avg PnL/day: {grand_pnl/int(dr['days']):+.1f} pts")
