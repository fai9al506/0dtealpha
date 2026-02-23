"""Final check: AG Short PnL inconsistency — some WINs get full target, others get 10pt"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)
with engine.begin() as conn:
    # AG Short wins: #21 gets target PnL (16.4), #51 gets 10pt PnL (10.0)
    # Live tracker vs backfill difference?
    # #21 is Feb 4 (backfill), #51 is Feb 5 (backfill), #133+ are Feb 19+ (live)

    # Let's check: which AG wins got full target PnL vs 10pt PnL
    print("=== AG SHORT WIN PNL SOURCE ===")
    print("Key question: live tracker awards full Volland target, backfill sometimes awards 10pt?")
    print()

    ag_wins = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               spot, target, outcome_pnl, outcome_first_event,
               outcome_target_level
        FROM setup_log
        WHERE setup_name = 'AG Short'
          AND outcome_result = 'WIN'
        ORDER BY ts ASC
    """)).mappings().all()

    for r in ag_wins:
        spot = float(r['spot'])
        target = float(r['target']) if r['target'] else 0
        pnl = float(r['outcome_pnl'])
        tgt_dist = abs(spot - target)
        ten_pt = 10.0
        is_ten = abs(pnl - 10.0) < 0.5
        is_full = abs(pnl - tgt_dist) < 1.0
        src = "10pt" if is_ten else ("full_target" if is_full else "OTHER")
        date = r['ts_et'].strftime('%m/%d')
        live = "LIVE" if r['ts_et'].date() >= __import__('datetime').date(2026, 2, 19) else "BACKFILL"
        print(f"  #{r['id']:>3} {date} spot={spot:.1f} target={target:.0f} tgt_dist={tgt_dist:.1f} pnl={pnl:+.1f} = {src} ({live})")

    # Now check: ALL remaining AG Short trades (losses, expired)
    print("\n=== ALL AG SHORT TRADES ===")
    ag_all = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, spot, target, lis, max_plus_gex,
               outcome_result, outcome_pnl,
               outcome_stop_level, outcome_target_level,
               outcome_first_event
        FROM setup_log
        WHERE setup_name = 'AG Short'
          AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    ag_total = 0
    for r in ag_all:
        spot = float(r['spot'])
        pnl = float(r['outcome_pnl'])
        ag_total += pnl
        stop = float(r['outcome_stop_level']) if r['outcome_stop_level'] else 0
        tgt = float(r['outcome_target_level']) if r['outcome_target_level'] else 0
        lis = float(r['lis']) if r['lis'] else 0
        mgex = float(r['max_plus_gex']) if r['max_plus_gex'] else 0

        # For AG Short LOSS: expected pnl = -(stop - spot)
        if r['outcome_result'] == 'LOSS':
            expected = -(stop - spot)  # short: loss when price goes up
            match = abs(pnl - expected) < 1.0
        elif r['outcome_result'] == 'WIN':
            expected = abs(spot - tgt) if tgt else 10.0
            match = abs(pnl - expected) < 1.0 or abs(pnl - 10.0) < 0.5
        else:
            expected = pnl
            match = True

        flag = "" if match else " *** MISMATCH"
        print(f"  #{r['id']:>3} {r['ts_et'].strftime('%m/%d %H:%M')} spot={spot:.1f} lis={lis:.0f} stop={stop:.1f} tgt={tgt:.1f} {r['outcome_result']:>7} pnl={pnl:+.1f} expected~{expected:+.1f}{flag}")
    print(f"  AG Short Total: {ag_total:+.1f}")

    # Check BofA Scalp PnL consistency
    print("\n=== BOFA SCALP DETAIL ===")
    bofa = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, spot,
               bofa_target_level, bofa_stop_level, bofa_max_hold_minutes,
               outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_first_event, outcome_elapsed_min, outcome_max_profit
        FROM setup_log
        WHERE setup_name = 'BofA Scalp'
          AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    bofa_total = 0
    for r in bofa:
        spot = float(r['spot'])
        pnl = float(r['outcome_pnl'])
        bofa_total += pnl
        tgt = float(r['bofa_target_level']) if r['bofa_target_level'] else 0
        stp = float(r['bofa_stop_level']) if r['bofa_stop_level'] else 0
        mp = float(r['outcome_max_profit']) if r['outcome_max_profit'] else 0
        em = r['outcome_elapsed_min'] or 0
        evt = r['outcome_first_event'] or ''

        if r['outcome_result'] == 'WIN':
            expected = abs(tgt - spot)
        elif r['outcome_result'] == 'LOSS':
            expected = -abs(stp - spot)
        else:  # EXPIRED
            expected = pnl  # mark-to-market, can't predict

        match = abs(pnl - expected) < 1.0 if r['outcome_result'] != 'EXPIRED' else True
        flag = "" if match else f" *** expected={expected:+.1f}"

        print(f"  #{r['id']:>3} {r['ts_et'].strftime('%m/%d %H:%M')} {r['direction']:>5} spot={spot:.1f} tgt={tgt:.1f} stop={stp:.1f} {r['outcome_result']:>7} pnl={pnl:+.1f} maxP={mp:+.1f} evt={evt}{flag}")
    print(f"  BofA Total: {bofa_total:+.1f}")

    # Final grand summary
    print("\n=== FINAL VERIFIED TOTALS ===")
    final = conn.execute(text("""
        SELECT setup_name,
               COUNT(*) as cnt,
               SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as w,
               SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as l,
               SUM(CASE WHEN outcome_result NOT IN ('WIN','LOSS') THEN 1 ELSE 0 END) as e,
               ROUND(SUM(outcome_pnl)::numeric, 2) as pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY setup_name
        ORDER BY SUM(outcome_pnl) DESC
    """)).mappings().all()

    grand = 0
    for r in final:
        wr = round(100 * int(r['w']) / int(r['cnt']), 1)
        pnl = float(r['pnl'])
        grand += pnl
        print(f"  {r['setup_name']:>18}: {r['cnt']:>3} trades ({r['w']}W/{r['l']}L/{r['e']}E) WR={wr}% PnL={pnl:+.2f}")
    print(f"  {'GRAND TOTAL':>18}: {sum(int(r['cnt']) for r in final):>3} trades                PnL={grand:+.2f}")
