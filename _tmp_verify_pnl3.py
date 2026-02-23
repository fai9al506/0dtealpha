"""Check specific anomalies: ES Absorption target mismatch, GEX Long stop mismatch"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)
with engine.begin() as conn:
    # 1. ES Absorption: PnL=+10 but expected negative based on target vs spot
    # This is because ES Absorption uses ES price, not SPX spot
    print("=== ES ABSORPTION DETAIL ===")
    abs_rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, spot, abs_es_price,
               outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level
        FROM setup_log
        WHERE setup_name = 'ES Absorption' AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()
    for r in abs_rows:
        print(f"  #{r['id']} {r['ts_et'].strftime('%m/%d %H:%M')} dir={r['direction']} SPX_spot={r['spot']} ES_price={r['abs_es_price']} result={r['outcome_result']} pnl={r['outcome_pnl']} tgt={r['outcome_target_level']} stop={r['outcome_stop_level']}")

    # 2. GEX Long #13: PnL=+20 but target was only 12pts away
    # Trailing stop: GEX Long uses rung-based trail
    print("\n=== GEX LONG WINS DETAIL ===")
    gex_wins = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, grade, spot, target, lis,
               outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_first_event
        FROM setup_log
        WHERE setup_name = 'GEX Long' AND outcome_result = 'WIN'
        ORDER BY ts ASC
    """)).mappings().all()
    for r in gex_wins:
        print(f"  #{r['id']} {r['ts_et'].strftime('%m/%d %H:%M')} spot={r['spot']} target={r['target']} lis={r['lis']} result={r['outcome_result']} pnl={r['outcome_pnl']} tgt_lvl={r['outcome_target_level']} stop_lvl={r['outcome_stop_level']} maxP={r['outcome_max_profit']} evt={r['outcome_first_event']}")

    # 3. GEX Long #156, #162: LOSS PnL=-8 but expected stop distance is less
    print("\n=== GEX LONG LOSSES WITH STOP MISMATCH ===")
    gex_losses = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, grade, spot, lis, max_plus_gex, max_minus_gex,
               outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_first_event
        FROM setup_log
        WHERE setup_name = 'GEX Long' AND outcome_result = 'LOSS'
        ORDER BY ts ASC
    """)).mappings().all()
    for r in gex_losses:
        spot = float(r['spot']) if r['spot'] else 0
        stop = float(r['outcome_stop_level']) if r['outcome_stop_level'] else 0
        dist = abs(spot - stop) if stop else 0
        pnl = float(r['outcome_pnl'])
        flag = " *** MISMATCH" if abs(abs(pnl) - dist) > 1.0 else ""
        print(f"  #{r['id']} {r['ts_et'].strftime('%m/%d %H:%M')} spot={spot:.1f} lis={r['lis']} stop={stop:.1f} dist={dist:.1f} pnl={pnl:+.1f}{flag}")

    # 4. AG Short #51: PnL=+10 but target was 81.5pts away (hit 10pt first)
    print("\n=== AG SHORT WINS DETAIL ===")
    ag_wins = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, grade, spot, target, lis,
               outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_first_event
        FROM setup_log
        WHERE setup_name = 'AG Short' AND outcome_result = 'WIN'
        ORDER BY ts ASC
    """)).mappings().all()
    for r in ag_wins:
        print(f"  #{r['id']} {r['ts_et'].strftime('%m/%d %H:%M')} spot={r['spot']} target={r['target']} lis={r['lis']} pnl={r['outcome_pnl']} maxP={r['outcome_max_profit']} evt={r['outcome_first_event']}")

    # 5. Check DD Exhaustion trades where max_profit=0 but PnL is positive
    print("\n=== DD TRADES WITH maxP=0 BUT positive PnL ===")
    dd_zero = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, spot,
               outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss,
               outcome_stop_level, outcome_target_level
        FROM setup_log
        WHERE setup_name = 'DD Exhaustion'
          AND outcome_result IS NOT NULL
          AND outcome_max_profit = 0
          AND outcome_pnl > 0
        ORDER BY ts ASC
    """)).mappings().all()
    print(f"  Found {len(dd_zero)} DD trades with maxP=0 and positive PnL:")
    for r in dd_zero:
        print(f"  #{r['id']} {r['ts_et'].strftime('%m/%d %H:%M')} {r['direction']} spot={r['spot']} pnl={r['outcome_pnl']} maxP={r['outcome_max_profit']} stop={r['outcome_stop_level']} tgt={r['outcome_target_level']}")

    # 6. Summary of all DD trades with max_profit=0
    print("\n=== ALL DD TRADES WITH maxP=0 ===")
    dd_all_zero = conn.execute(text("""
        SELECT id, outcome_result, outcome_pnl, outcome_max_profit
        FROM setup_log
        WHERE setup_name = 'DD Exhaustion'
          AND outcome_result IS NOT NULL
          AND (outcome_max_profit = 0 OR outcome_max_profit IS NULL)
        ORDER BY ts ASC
    """)).mappings().all()
    print(f"  {len(dd_all_zero)} DD trades with maxP=0:")
    for r in dd_all_zero:
        print(f"  #{r['id']} {r['outcome_result']} pnl={r['outcome_pnl']}")
