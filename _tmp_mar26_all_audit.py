"""Audit ALL Mar 26 trades across all setups."""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["DATABASE_URL"])

with e.connect() as c:
    rows = c.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as signal_ts,
               setup_name, direction, grade, outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss, outcome_elapsed_min, spot
        FROM setup_log
        WHERE ts::date = '2026-03-26' AND outcome_result IS NOT NULL
        ORDER BY ts
    """)).mappings().all()

    print(f"Total Mar 26 trades with outcomes: {len(rows)}\n")

    # TS outage: 10:20-15:55 ET. Trades before 10:18 are clean.
    clean = []
    contaminated = []
    for r in rows:
        time = str(r['signal_ts'])[11:16]
        hr, mn = int(time[:2]), int(time[3:5])
        t_min = hr * 60 + mn
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        mp = float(r['outcome_max_profit']) if r['outcome_max_profit'] else 0

        # Before 10:18 = clean, after = contaminated (TS outage 10:20-15:55)
        is_clean = t_min < 10 * 60 + 18
        bucket = clean if is_clean else contaminated

        flag = 'CLEAN' if is_clean else 'CONTAMINATED'
        bucket.append(r)
        print(f"id={r['id']:5d} | {time} | {r['setup_name']:20s} | {r['direction'][:1].upper()} | {r['outcome_result']:7s} | pnl={pnl:+7.1f} | maxP={mp:+7.1f} | {flag}")

    print(f"\nCLEAN: {len(clean)} trades")
    print(f"CONTAMINATED: {len(contaminated)} trades")

    # Show IDs to null out
    contam_ids = [r['id'] for r in contaminated]
    print(f"\nIDs to clear outcomes: {contam_ids}")
