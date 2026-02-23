"""Check GEX Long stop mismatches and AG Short PnL calculation"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)
with engine.begin() as conn:
    # 1. GEX Long #156 and #162 — stop closer than 8pts but PnL=-8
    # The live tracker uses fixed 8pt stop from _compute_setup_levels()
    # But outcome_stop_level stored is closer. Let's check if the PnL should be -4.2 and -5.3
    print("=== GEX LONG #156 and #162 DEEP DIVE ===")
    for tid in [156, 162]:
        r = conn.execute(text(f"""
            SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
                   direction, grade, spot, lis, target,
                   max_plus_gex, max_minus_gex,
                   outcome_result, outcome_pnl,
                   outcome_target_level, outcome_stop_level,
                   outcome_max_profit, outcome_max_loss,
                   outcome_first_event, outcome_elapsed_min
            FROM setup_log WHERE id = {tid}
        """)).mappings().first()
        if r:
            spot = float(r['spot'])
            stop = float(r['outcome_stop_level']) if r['outcome_stop_level'] else 0
            lis = float(r['lis']) if r['lis'] else 0
            mgex = float(r['max_minus_gex']) if r['max_minus_gex'] else 0
            pnl = float(r['outcome_pnl'])
            print(f"  #{tid}: spot={spot:.1f}, lis={lis:.1f}, max_minus_gex={mgex:.1f}")
            print(f"    stop_level={stop:.1f}, dist={abs(spot-stop):.1f}, pnl={pnl:+.1f}")
            print(f"    _compute_setup_levels for GEX Long: stop = spot - 8 = {spot-8:.1f}")
            print(f"    But stored outcome_stop_level = {stop:.1f} (from backfill, uses max(lis-5, spot-20) with max_minus_gex)")
            print(f"    Live tracker uses _compute_setup_levels (spot-8), backfill uses _calculate_setup_outcome (different logic)")
            print()

    # 2. AG Short: first_event=10pt but PnL varies (10, 16.4, 26.1, etc.)
    # AG Short is NOT a trailing setup - so why different PnLs?
    # Check: does AG Short use target or 10pt for PnL?
    print("=== AG SHORT PNL LOGIC CHECK ===")
    ag_all = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, spot, target, lis,
               outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_first_event
        FROM setup_log
        WHERE setup_name = 'AG Short' AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()
    for r in ag_all:
        spot = float(r['spot']) if r['spot'] else 0
        target = float(r['target']) if r['target'] else 0
        tgt_lvl = float(r['outcome_target_level']) if r['outcome_target_level'] else 0
        pnl = float(r['outcome_pnl'])
        evt = r['outcome_first_event']

        # For short: 10pt level = spot - 10
        ten_pt_pnl = 10.0
        target_pnl = abs(spot - target) if target else 0
        stored_tgt_pnl = abs(spot - tgt_lvl) if tgt_lvl else 0

        note = ""
        if r['outcome_result'] == 'WIN':
            if abs(pnl - 10.0) < 0.1:
                note = "PnL matches 10pt"
            elif abs(pnl - target_pnl) < 0.5:
                note = f"PnL matches full target ({target_pnl:.1f})"
            elif abs(pnl - stored_tgt_pnl) < 0.5:
                note = f"PnL matches stored target ({stored_tgt_pnl:.1f})"
            else:
                note = f"PnL doesn't match 10pt({ten_pt_pnl}) or target({target_pnl:.1f}) or stored_tgt({stored_tgt_pnl:.1f}) *** CHECK"

        print(f"  #{r['id']:>3} {r['ts_et'].strftime('%m/%d %H:%M')} spot={spot:>7.1f} volland_target={target:>7.0f} stored_tgt={tgt_lvl:>7.1f} evt={evt:>6} pnl={pnl:>+7.1f} {note}")

    # 3. Check if the live tracker PnL vs backfill PnL could differ
    # Trades resolved by live tracker have outcome_first_event set differently?
    print("\n=== TRADE RESOLUTION SOURCE CHECK ===")
    print("Trades from Feb 19+ should be live-tracked (live outcome tracker deployed ~Feb 18)")
    print("Trades before Feb 19 were backfilled")

    early = conn.execute(text("""
        SELECT COUNT(*) as cnt,
               SUM(outcome_pnl) as total
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND ts < '2026-02-19'::date
    """)).mappings().first()
    late = conn.execute(text("""
        SELECT COUNT(*) as cnt,
               SUM(outcome_pnl) as total
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND ts >= '2026-02-19'::date
    """)).mappings().first()
    print(f"  Before Feb 19 (backfilled): {early['cnt']} trades, {float(early['total']):+.1f} pts")
    print(f"  Feb 19+ (live-tracked):     {late['cnt']} trades, {float(late['total']):+.1f} pts")
    print(f"  Grand total:                {early['cnt']+late['cnt']} trades, {float(early['total'])+float(late['total']):+.1f} pts")
