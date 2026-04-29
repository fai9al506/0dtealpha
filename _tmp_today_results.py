"""Today's (Mar 27) trading results."""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["DATABASE_URL"])

with e.connect() as c:
    # All signals today
    rows = c.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as t,
               setup_name, direction, grade, score,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               outcome_elapsed_min, spot, abs_es_price, greek_alignment
        FROM setup_log
        WHERE ts::date = '2026-03-27'
        ORDER BY ts
    """)).mappings().all()

    print(f"=== Mar 27 Trades: {len(rows)} signals ===\n")

    wins = losses = expired = pending = 0
    total_pnl = 0
    for r in rows:
        time = str(r['t'])[11:16]
        res = r['outcome_result'] or 'PENDING'
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        mp = float(r['outcome_max_profit']) if r['outcome_max_profit'] else 0
        align = r['greek_alignment'] if r['greek_alignment'] is not None else '-'
        spot = f"{r['spot']:.1f}" if r['spot'] else '-'

        if res == 'WIN': wins += 1; total_pnl += pnl
        elif res == 'LOSS': losses += 1; total_pnl += pnl
        elif res == 'EXPIRED': expired += 1; total_pnl += pnl
        else: pending += 1

        print(f"id={r['id']:5d} | {time} | {r['setup_name']:20s} | {r['direction'][:1].upper()} | {r['grade']:>3s} | align={align:>2s} | spot={spot:>8s} | {res:7s} | pnl={pnl:+7.1f} | maxP={mp:+7.1f}")

    print(f"\n=== Summary ===")
    print(f"Signals: {len(rows)} | W={wins} L={losses} X={expired} P={pending}")
    print(f"Total PnL: {total_pnl:+.1f} pts")
    if wins + losses > 0:
        print(f"WR: {100*wins/(wins+losses):.0f}%")

    # Check chain_snapshots for data quality
    snap_count = c.execute(text("""
        SELECT COUNT(*) FROM chain_snapshots WHERE ts::date = '2026-03-27'
    """)).scalar()
    last_snap = c.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as t, spot, data_ts
        FROM chain_snapshots WHERE ts::date = '2026-03-27'
        ORDER BY ts DESC LIMIT 1
    """)).mappings().first()
    print(f"\nData quality: {snap_count} snapshots today")
    if last_snap:
        print(f"Last snapshot: {last_snap['t']} spot={last_snap['spot']} data_ts={last_snap['data_ts']}")
    else:
        print("No snapshots yet (pre-market or market closed)")
