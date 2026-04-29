"""Audit all SB2 Absorption outcomes for inflated data, then clean."""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["DATABASE_URL"])

with e.connect() as c:
    # First get column names
    cols = c.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='setup_log' ORDER BY ordinal_position")).fetchall()
    ts_col = 'ts' if any(c[0] == 'ts' for c in cols) else 'created_at'
    print(f"Using timestamp col: {ts_col}")

    rows = c.execute(text(f"""
        SELECT id, {ts_col} AT TIME ZONE 'America/New_York' as signal_ts,
               setup_name, direction, grade, outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
               abs_es_price, abs_details
        FROM setup_log
        WHERE setup_name = 'SB2 Absorption' AND outcome_result IS NOT NULL
        ORDER BY {ts_col}
    """)).mappings().all()

    print(f"Total SB2 trades with outcomes: {len(rows)}\n")
    suspect_ids = []
    for r in rows:
        mp = float(r['outcome_max_profit']) if r['outcome_max_profit'] else 0
        ml = float(r['outcome_max_loss']) if r['outcome_max_loss'] else 0
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        ad = r['abs_details']
        date = str(r['signal_ts'])[:10]
        time = str(r['signal_ts'])[11:16]
        flag = ''
        # SB2 SL=8, T=10(or 12). Any maxP > 15 or |maxL| > 12 or |pnl| > 15 is suspect
        if mp > 15 or abs(ml) > 12 or abs(pnl) > 15:
            flag = ' *** SUSPECT'
            suspect_ids.append(r['id'])
        if ad is None:
            flag += ' [no abs_details]'
        print(f"id={r['id']:5d} | {date} {time} | {r['direction'][:1].upper()} | {r['grade']:>3s} | {r['outcome_result']:7s} | pnl={pnl:+7.1f} | maxP={mp:+7.1f} | maxL={ml:+7.1f} | {r['outcome_elapsed_min'] or 0:3d}m{flag}")

    print(f"\n--- SUSPECT IDs: {suspect_ids} ({len(suspect_ids)} trades) ---")
    print(f"Non-suspect: {len(rows) - len(suspect_ids)} trades")
