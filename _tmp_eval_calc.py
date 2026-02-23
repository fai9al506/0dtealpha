"""Calculate eval account pass probability with different sizing"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)

with engine.begin() as conn:
    # Get daily PnL in pts
    daily = conn.execute(text("""
        SELECT DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
               COUNT(*) as trades,
               SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as losses,
               ROUND(SUM(outcome_pnl)::numeric, 1) as daily_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY DATE(ts AT TIME ZONE 'America/New_York')
        ORDER BY trade_date ASC
    """)).mappings().all()

    # Get per-trade PnL for intraday drawdown analysis
    trades = conn.execute(text("""
        SELECT DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
               ts AT TIME ZONE 'America/New_York' as ts_et,
               setup_name, outcome_result, outcome_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    daily_pnls = [(str(d['trade_date']), float(d['daily_pnl']), int(d['trades'])) for d in daily]

    # Typical 100K eval rules (E2T / Topstep / Apex style)
    eval_configs = {
        "E2T 100K TCP (estimated)": {
            "profit_target": 6000,
            "daily_loss_limit": 2200,
            "trailing_dd": 3500,
            "max_contracts_mes": 120,  # 12 ES equivalent
        },
        "Topstep 100K": {
            "profit_target": 6000,
            "daily_loss_limit": 2000,
            "trailing_dd": 3000,
            "max_contracts_mes": 100,
        },
        "Apex 100K": {
            "profit_target": 6000,
            "daily_loss_limit": 2500,
            "trailing_dd": 3500,
            "max_contracts_mes": 120,
        },
    }

    MES_PT = 5.0  # $5 per point per MES

    print("=" * 100)
    print("EVAL ACCOUNT PASS CALCULATOR")
    print("=" * 100)

    # Intraday drawdown per day (worst running loss within a day)
    print("\nINTRADAY DRAWDOWN ANALYSIS (worst running loss within each day)")
    print("-" * 80)
    for date_str, _, _ in daily_pnls:
        day_trades = [t for t in trades if str(t['trade_date']) == date_str]
        running = 0
        worst_intraday = 0
        peak_intraday = 0
        for t in day_trades:
            pnl = float(t['outcome_pnl'])
            running += pnl
            if running > peak_intraday:
                peak_intraday = running
            dd = peak_intraday - running
            if dd > worst_intraday:
                worst_intraday = dd
        day_pnl = sum(float(t['outcome_pnl']) for t in day_trades)
        print(f"  {date_str}: day_pnl={day_pnl:>+7.1f} pts, worst_intraday_dd={worst_intraday:>5.1f} pts, trades={len(day_trades)}")

    # Simulate passing for different MES quantities
    print("\n" + "=" * 100)
    print("SIZING ANALYSIS: How many MES to pass $6K target in 10 days")
    print("=" * 100)

    for qty in [5, 8, 10, 12, 15, 20]:
        dollar_per_pt = qty * MES_PT

        print(f"\n--- {qty} MES (${dollar_per_pt:.0f}/pt) ---")

        # Simulate rolling 10-day windows
        cumul_dollars = []
        for i, (date, pnl, ntrades) in enumerate(daily_pnls):
            day_dollars = pnl * dollar_per_pt
            cumul_dollars.append((date, day_dollars, pnl))

        # Check pass/fail across all 10-day windows
        # But we only have 11 days, so just simulate from day 1
        running_pnl = 0
        running_peak = 0
        worst_trail_dd = 0
        passed = False
        pass_day = None
        blown = False
        blow_day = None

        for i, (date, day_dollars, day_pts) in enumerate(cumul_dollars):
            # Check intraday: get worst intraday DD in dollars
            day_trades_list = [t for t in trades if str(t['trade_date']) == date]
            intra_running = 0
            worst_intra_dd_dollars = 0
            for t in day_trades_list:
                pnl = float(t['outcome_pnl']) * dollar_per_pt
                intra_running += pnl
                if intra_running < 0 and abs(intra_running) > worst_intra_dd_dollars:
                    worst_intra_dd_dollars = abs(intra_running)

            running_pnl += day_dollars
            if running_pnl > running_peak:
                running_peak = running_pnl
            trail_dd = running_peak - running_pnl
            if trail_dd > worst_trail_dd:
                worst_trail_dd = trail_dd

            status = ""
            if running_pnl >= 6000 and not passed:
                passed = True
                pass_day = i + 1
                status = " << PASSED"
            if trail_dd >= 3500 and not blown:
                blown = True
                blow_day = i + 1
                status = " << BLOWN (trailing DD)"

            print(f"    Day {i+1:>2} ({date}): {day_dollars:>+8,.0f}  cumul={running_pnl:>+9,.0f}  trail_dd={trail_dd:>6,.0f}  intra_dd={worst_intra_dd_dollars:>6,.0f}{status}")

        print(f"    Result: {'PASSED day ' + str(pass_day) if passed else 'NOT PASSED'} | {'BLOWN day ' + str(blow_day) if blown else 'SURVIVED'} | Worst trail DD: ${worst_trail_dd:,.0f}")

    # Risk per trade analysis
    print("\n" + "=" * 100)
    print("RISK PER TRADE BY SETUP (at different MES sizes)")
    print("=" * 100)

    setup_stops = {
        "DD Exhaustion": 12,
        "AG Short": 20,
        "BofA Scalp": 12,  # approximate
        "GEX Long": 8,
        "ES Absorption": 12,
        "Paradigm Reversal": 15,
    }

    print(f"\n{'Setup':<20} {'Stop':>5} ", end="")
    for qty in [8, 10, 12, 15]:
        print(f"| {qty} MES risk", end="")
    print()
    print("-" * 85)
    for setup, stop in sorted(setup_stops.items()):
        print(f"{setup:<20} {stop:>4}pt ", end="")
        for qty in [8, 10, 12, 15]:
            risk = stop * qty * MES_PT
            print(f"| ${risk:>7,.0f}   ", end="")
        print()

    # What sizing passes fastest while staying safe?
    print("\n" + "=" * 100)
    print("RECOMMENDATION MATRIX")
    print("=" * 100)
    print("""
    Goal: $6,000 profit target in 10 days
    Constraint: ~$2,000-2,500 daily loss limit, ~$3,000-3,500 trailing DD

    At 15 pts/day (conservative avg):
""")
    for qty in [8, 10, 12, 15, 20]:
        daily_dollar = 15 * qty * MES_PT
        days_to_pass = 6000 / daily_dollar if daily_dollar > 0 else 999
        max_loss_per_trade = 20 * qty * MES_PT  # worst stop = AG Short 20pt
        typical_loss = 12 * qty * MES_PT  # typical DD/Abs stop = 12pt
        print(f"    {qty:>2} MES: ${daily_dollar:>5,.0f}/day -> pass in {days_to_pass:.1f} days | worst single trade loss: ${max_loss_per_trade:>5,.0f} | typical loss: ${typical_loss:>5,.0f}")
