"""Verify all trading PNL figures from setup_log"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)
with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id,
               ts AT TIME ZONE 'America/New_York' as ts_et,
               setup_name, direction, grade, score,
               ROUND(spot::numeric, 2) as spot,
               outcome_result,
               ROUND(outcome_pnl::numeric, 1) as pnl,
               ROUND(outcome_max_profit::numeric, 1) as max_profit,
               ROUND(outcome_max_loss::numeric, 1) as max_loss,
               outcome_first_event,
               outcome_elapsed_min
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    print(f"Total resolved trades: {len(rows)}")
    print()
    hdr = f"{'#':>3} {'ID':>4} {'Date':>10} {'Time':>5} {'Setup':>18} {'Dir':>5} {'Grd':>5} {'Result':>7} {'PnL':>7} {'MaxP':>6} {'MaxL':>6} {'Event':>7} {'Min':>4}"
    print(hdr)
    print("-" * len(hdr))

    grand_total = 0.0
    setup_totals = {}

    for i, r in enumerate(rows, 1):
        pnl = float(r['pnl']) if r['pnl'] is not None else 0.0
        grand_total += pnl

        sn = r['setup_name']
        if sn not in setup_totals:
            setup_totals[sn] = {'pnl': 0.0, 'wins': 0, 'losses': 0, 'expired': 0, 'total': 0}
        setup_totals[sn]['pnl'] += pnl
        setup_totals[sn]['total'] += 1
        if r['outcome_result'] == 'WIN':
            setup_totals[sn]['wins'] += 1
        elif r['outcome_result'] == 'LOSS':
            setup_totals[sn]['losses'] += 1
        else:
            setup_totals[sn]['expired'] += 1

        mp = float(r['max_profit']) if r['max_profit'] is not None else 0.0
        ml = float(r['max_loss']) if r['max_loss'] is not None else 0.0
        ts = r['ts_et']
        date_str = ts.strftime('%m/%d')
        time_str = ts.strftime('%H:%M')
        em = r['outcome_elapsed_min'] if r['outcome_elapsed_min'] else 0
        evt = str(r['outcome_first_event'] or '')

        print(f"{i:>3} {r['id']:>4} {date_str:>10} {time_str:>5} {sn:>18} {r['direction']:>5} {r['grade']:>5} {r['outcome_result']:>7} {pnl:>+7.1f} {mp:>+6.1f} {ml:>+6.1f} {evt:>7} {em:>4}")

    print("-" * len(hdr))
    print(f"\nGrand Total PnL: {grand_total:+.1f} pts  ({len(rows)} trades)")
    print()

    # Per-setup breakdown
    print("=" * 70)
    print(f"{'Setup':>18} {'Trades':>6} {'Wins':>5} {'Loss':>5} {'Exp':>4} {'WR%':>6} {'PnL':>8} {'Avg':>7}")
    print("-" * 70)
    for sn in sorted(setup_totals.keys(), key=lambda x: setup_totals[x]['pnl'], reverse=True):
        s = setup_totals[sn]
        wr = round(100 * s['wins'] / s['total'], 1) if s['total'] > 0 else 0
        avg = round(s['pnl'] / s['total'], 1) if s['total'] > 0 else 0
        print(f"{sn:>18} {s['total']:>6} {s['wins']:>5} {s['losses']:>5} {s['expired']:>4} {wr:>5.1f}% {s['pnl']:>+8.1f} {avg:>+7.1f}")
    print("-" * 70)
    print(f"{'TOTAL':>18} {len(rows):>6}                        {grand_total:>+8.1f}")
    print()

    # Cross-check: sum PnL directly from DB
    check = conn.execute(text("""
        SELECT ROUND(SUM(outcome_pnl)::numeric, 1) as total_pnl,
               COUNT(*) as count
        FROM setup_log
        WHERE outcome_result IS NOT NULL
    """)).mappings().first()
    print(f"DB SUM(outcome_pnl) = {check['total_pnl']} pts across {check['count']} trades")
    print(f"Python running sum  = {grand_total:+.1f} pts")
    print(f"Match: {'YES' if abs(float(check['total_pnl']) - grand_total) < 0.2 else 'NO - MISMATCH!'}")
