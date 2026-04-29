"""Clear contaminated outcome data from setup_log.

Two sources of contamination:
1. Mar 26 TS API outage (10:20-15:55 ET) — all trades that were open during outage
2. SB2 abs_details NULL bug — outcome tracker scanned from bar 0 (overnight bars)

Conservative approach: clear outcome fields but keep the signal rows.
Only keep outcomes that are clearly valid (quick losses that resolved before outage).
"""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["DATABASE_URL"])

OUTCOME_COLS = [
    "outcome_result", "outcome_pnl", "outcome_max_profit", "outcome_max_loss",
    "outcome_target_level", "outcome_stop_level", "outcome_first_event", "outcome_elapsed_min"
]

with e.begin() as c:
    # 1. Mar 26: ALL trades except id=1254 (clean LOSS -8 resolved in 1 min before outage)
    mar26 = c.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as t, setup_name, outcome_result, outcome_pnl
        FROM setup_log WHERE ts::date = '2026-03-26' AND outcome_result IS NOT NULL AND id != 1254
        ORDER BY ts
    """)).mappings().all()
    mar26_ids = [r['id'] for r in mar26]
    print(f"Mar 26 contaminated (outage): {len(mar26_ids)} trades")
    for r in mar26:
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        print(f"  id={r['id']} {r['setup_name']:20s} {r['outcome_result']:7s} pnl={pnl:+.1f}")

    # 2. SB2 abs_details NULL bug: Mar 24 trades with inflated maxP (>15) or maxL (< -12)
    sb2_inflated = c.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as t, outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss
        FROM setup_log
        WHERE setup_name = 'SB2 Absorption'
          AND outcome_result IS NOT NULL
          AND ts::date != '2026-03-26'
          AND (outcome_max_profit > 15 OR outcome_max_loss < -12)
        ORDER BY ts
    """)).mappings().all()
    sb2_ids = [r['id'] for r in sb2_inflated]
    print(f"\nSB2 abs_details bug (non-Mar26): {len(sb2_ids)} trades")
    for r in sb2_inflated:
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        mp = float(r['outcome_max_profit']) if r['outcome_max_profit'] else 0
        ml = float(r['outcome_max_loss']) if r['outcome_max_loss'] else 0
        print(f"  id={r['id']} {r['outcome_result']:7s} pnl={pnl:+.1f} maxP={mp:+.1f} maxL={ml:+.1f}")

    all_ids = mar26_ids + sb2_ids
    print(f"\nTotal to clear: {len(all_ids)} trades")

    if all_ids:
        set_clauses = ", ".join(f"{col} = NULL" for col in OUTCOME_COLS)
        c.execute(text(f"""
            UPDATE setup_log SET {set_clauses}
            WHERE id = ANY(:ids)
        """), {"ids": all_ids})
        print(f"\nCLEARED {len(all_ids)} outcome records.")
    else:
        print("Nothing to clear.")

    # Verify
    remaining = c.execute(text("""
        SELECT COUNT(*) FROM setup_log
        WHERE ts::date = '2026-03-26' AND outcome_result IS NOT NULL
    """)).scalar()
    print(f"\nRemaining Mar 26 outcomes: {remaining} (should be 1 = id 1254)")
