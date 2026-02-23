"""Deep PNL verification - check rounding, per-setup sums, and look for anomalies"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)
with engine.begin() as conn:
    # 1. Check rounding: get raw PNL values with full precision
    rows = conn.execute(text("""
        SELECT id, setup_name, outcome_result, outcome_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    print("=== ROUNDING CHECK ===")
    raw_sum = sum(float(r['outcome_pnl']) for r in rows)
    rounded_sum = sum(round(float(r['outcome_pnl']), 1) for r in rows)
    print(f"Raw sum (full precision): {raw_sum}")
    print(f"Sum of rounded values:    {rounded_sum}")
    print(f"DB-side SUM:")

    db_sum = conn.execute(text("SELECT SUM(outcome_pnl) as s FROM setup_log WHERE outcome_result IS NOT NULL")).mappings().first()
    print(f"  SUM(outcome_pnl) = {db_sum['s']}")

    db_sum_rounded = conn.execute(text("SELECT SUM(ROUND(outcome_pnl::numeric, 1)) as s FROM setup_log WHERE outcome_result IS NOT NULL")).mappings().first()
    print(f"  SUM(ROUND(pnl,1)) = {db_sum_rounded['s']}")
    print()

    # 2. Per-setup breakdown with full precision
    print("=== PER-SETUP BREAKDOWN (full precision) ===")
    setup_rows = conn.execute(text("""
        SELECT setup_name,
               COUNT(*) as cnt,
               SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses,
               SUM(CASE WHEN outcome_result NOT IN ('WIN','LOSS') THEN 1 ELSE 0 END) as other,
               SUM(outcome_pnl) as total_pnl,
               ROUND((100.0 * SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) / COUNT(*))::numeric, 1) as wr
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY setup_name
        ORDER BY SUM(outcome_pnl) DESC
    """)).mappings().all()

    for r in setup_rows:
        print(f"  {r['setup_name']:>18}: {r['cnt']:>3} trades, {r['wins']}W/{r['losses']}L/{r['other']}E, WR={r['wr']}%, PnL={float(r['total_pnl']):+.2f}")
    print()

    # 3. Look for suspicious PNL values
    print("=== ANOMALY CHECK ===")

    # Trades where PnL doesn't match expected target/stop distances
    print("\n--- Trades with unusual PnL for their result type ---")
    anomalies = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               setup_name, direction, grade,
               spot, outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_max_loss, outcome_first_event
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    for r in anomalies:
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        spot = float(r['spot']) if r['spot'] else 0
        tgt = float(r['outcome_target_level']) if r['outcome_target_level'] else None
        stp = float(r['outcome_stop_level']) if r['outcome_stop_level'] else None
        direction = r['direction']
        result = r['outcome_result']
        setup = r['setup_name']
        ts = r['ts_et']

        issues = []

        # Check WIN PnL makes sense
        if result == 'WIN' and tgt and spot:
            if direction in ('long',):
                expected_pnl = tgt - spot
            elif direction in ('short', 'bearish'):
                expected_pnl = spot - tgt
            else:
                expected_pnl = None

            if expected_pnl is not None and abs(pnl - expected_pnl) > 1.0:
                # For trailing setups, PnL won't match target
                if setup not in ('DD Exhaustion', 'GEX Long') or abs(pnl - expected_pnl) > 2.0:
                    issues.append(f"WIN PnL={pnl:+.1f} but expected={expected_pnl:+.1f} (tgt={tgt:.1f} spot={spot:.1f})")

        # Check LOSS PnL makes sense
        if result == 'LOSS' and stp and spot:
            if direction in ('long',):
                expected_pnl = stp - spot
            elif direction in ('short', 'bearish'):
                expected_pnl = spot - stp
            else:
                expected_pnl = None

            if expected_pnl is not None and abs(pnl - expected_pnl) > 1.0:
                issues.append(f"LOSS PnL={pnl:+.1f} but expected={expected_pnl:+.1f} (stop={stp:.1f} spot={spot:.1f})")

        # Check WIN with negative PnL
        if result == 'WIN' and pnl < 0:
            issues.append(f"WIN with negative PnL={pnl:+.1f}")

        # Check LOSS with positive PnL
        if result == 'LOSS' and pnl > 0:
            issues.append(f"LOSS with positive PnL={pnl:+.1f}")

        if issues:
            print(f"  #{r['id']} {ts.strftime('%m/%d %H:%M')} {setup:>18} {direction} {result}: {'; '.join(issues)}")

    print()

    # 4. Check for trades with NULL PnL
    null_pnl = conn.execute(text("""
        SELECT COUNT(*) as cnt FROM setup_log
        WHERE outcome_result IS NOT NULL AND outcome_pnl IS NULL
    """)).mappings().first()
    print(f"Trades with result but NULL PnL: {null_pnl['cnt']}")

    # 5. Check for open trades (no outcome yet)
    open_trades = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               setup_name, direction, grade, spot
        FROM setup_log
        WHERE outcome_result IS NULL
        ORDER BY ts DESC
    """)).mappings().all()
    print(f"Open trades (no outcome yet): {len(open_trades)}")
    for r in open_trades:
        print(f"  #{r['id']} {r['ts_et'].strftime('%m/%d %H:%M')} {r['setup_name']} {r['direction']}")

    # 6. DD Exhaustion detailed check - these use trailing stops
    print("\n=== DD EXHAUSTION DETAILED CHECK ===")
    dd_rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, grade, spot,
               outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_max_loss,
               outcome_first_event, outcome_elapsed_min
        FROM setup_log
        WHERE setup_name = 'DD Exhaustion' AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    dd_total = 0
    for r in dd_rows:
        pnl = float(r['outcome_pnl'])
        dd_total += pnl
        ts = r['ts_et']
        mp = float(r['outcome_max_profit']) if r['outcome_max_profit'] else 0
        print(f"  #{r['id']:>3} {ts.strftime('%m/%d %H:%M')} {r['direction']:>5} {r['grade']:>5} spot={float(r['spot']):>7.1f} {r['outcome_result']:>7} pnl={pnl:>+7.1f} maxP={mp:>+6.1f} stop={float(r['outcome_stop_level']) if r['outcome_stop_level'] else 0:>7.1f} tgt={float(r['outcome_target_level']) if r['outcome_target_level'] else 0:>7.1f} evt={r['outcome_first_event']} {r['outcome_elapsed_min']}m")
    print(f"  DD Total: {dd_total:+.1f}")
