import os, sqlalchemy as sa

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    # Break down by option type
    print('=== THEO P&L BY OPTION TYPE ===')
    rows = conn.execute(sa.text("""
        SELECT
            CASE WHEN o.state->>'symbol' LIKE '%P%' THEN 'PUT' ELSE 'CALL' END as opt_type,
            count(*) as cnt,
            sum(((o.state->>'theo_close_price')::numeric - (o.state->>'theo_entry_price')::numeric) * (o.state->>'qty')::int * 100) as theo_pnl
        FROM options_trade_orders o
        WHERE (o.state->>'ts_placed')::date = '2026-03-18'
        GROUP BY 1
        ORDER BY 1
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]} trades, theo P&L = ${float(r[2]):.0f}")

    # The big losers
    print('\n=== BIGGEST THEO LOSERS (< -$30) ===')
    rows = conn.execute(sa.text("""
        SELECT o.setup_log_id,
               o.state->>'setup_name' as setup,
               o.state->>'symbol' as symbol,
               (o.state->>'theo_entry_price')::numeric as theo_entry,
               (o.state->>'theo_close_price')::numeric as theo_exit,
               ((o.state->>'theo_close_price')::numeric - (o.state->>'theo_entry_price')::numeric) * 100 as theo_pnl,
               o.state->>'ts_placed' as placed,
               o.state->>'ts_closed' as closed,
               s.outcome_result, s.outcome_pnl
        FROM options_trade_orders o
        JOIN setup_log s ON s.id = o.setup_log_id
        WHERE (o.state->>'ts_placed')::date = '2026-03-18'
          AND ((o.state->>'theo_close_price')::numeric - (o.state->>'theo_entry_price')::numeric) * 100 < -30
        ORDER BY ((o.state->>'theo_close_price')::numeric - (o.state->>'theo_entry_price')::numeric)
    """)).fetchall()
    for r in rows:
        sid, setup, sym, te, tx, pnl, placed, closed, res, pts = r
        hold_min = ''
        if placed and closed:
            from datetime import datetime
            p = datetime.fromisoformat(placed)
            c = datetime.fromisoformat(closed)
            hold_min = f"{int((c-p).total_seconds()/60)}min"
        print(f"  #{sid} {setup}: {sym} | {float(te):.2f} -> {float(tx):.2f} = ${float(pnl):.0f} | held {hold_min} | setup: {res} {float(pts):+.1f}pts")

    # Winning setups that were losing options
    print('\n=== SETUP WIN but OPTIONS LOSS (theo) ===')
    rows = conn.execute(sa.text("""
        SELECT o.setup_log_id,
               o.state->>'setup_name' as setup,
               s.direction as setup_dir,
               o.state->>'symbol' as symbol,
               (o.state->>'theo_entry_price')::numeric as theo_entry,
               (o.state->>'theo_close_price')::numeric as theo_exit,
               ((o.state->>'theo_close_price')::numeric - (o.state->>'theo_entry_price')::numeric) * 100 as theo_pnl,
               s.outcome_result, s.outcome_pnl
        FROM options_trade_orders o
        JOIN setup_log s ON s.id = o.setup_log_id
        WHERE (o.state->>'ts_placed')::date = '2026-03-18'
          AND s.outcome_result = 'WIN'
          AND ((o.state->>'theo_close_price')::numeric - (o.state->>'theo_entry_price')::numeric) < 0
        ORDER BY ((o.state->>'theo_close_price')::numeric - (o.state->>'theo_entry_price')::numeric)
    """)).fetchall()
    for r in rows:
        sid, setup, sdir, sym, te, tx, pnl, res, pts = r
        print(f"  #{sid} {setup} ({sdir}): {sym} | theo {float(te):.2f}->{float(tx):.2f} = ${float(pnl):.0f} | setup: {res} {float(pts):+.1f}pts")

    # Options hold times vs setup resolution times
    print('\n=== HOLD TIME COMPARISON (options closed AFTER setup resolved?) ===')
    rows = conn.execute(sa.text("""
        SELECT o.setup_log_id,
               o.state->>'setup_name' as setup,
               o.state->>'ts_placed' as opt_placed,
               o.state->>'ts_closed' as opt_closed,
               s.outcome_elapsed_min as setup_elapsed,
               ((o.state->>'theo_close_price')::numeric - (o.state->>'theo_entry_price')::numeric) * 100 as theo_pnl,
               s.outcome_result, s.outcome_pnl
        FROM options_trade_orders o
        JOIN setup_log s ON s.id = o.setup_log_id
        WHERE (o.state->>'ts_placed')::date = '2026-03-18'
        ORDER BY o.setup_log_id
    """)).fetchall()
    for r in rows:
        sid, setup, op, oc, sel, tpnl, sres, spts = r
        if op and oc:
            from datetime import datetime
            p = datetime.fromisoformat(op)
            c = datetime.fromisoformat(oc)
            opt_hold = int((c-p).total_seconds()/60)
            diff = opt_hold - (sel or 0)
            marker = ' <<<' if diff > 15 else ''
            print(f"  #{sid} {setup:<16} opt_hold={opt_hold:>3}min  setup_elapsed={sel or 0:>3}min  diff={diff:>+4}min  theo=${float(tpnl):>+6.0f}  setup={sres} {float(spts):>+6.1f}{marker}")
