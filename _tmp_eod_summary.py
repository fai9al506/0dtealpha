"""End-of-day summary for 2026-05-21."""
import os, json
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"])

with eng.connect() as c:
    # 1) Volland health — last save
    r = c.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' AS et,
               payload->'statistics'->>'paradigm' AS para,
               EXTRACT(EPOCH FROM (now() - ts))::int AS age_s
        FROM volland_snapshots ORDER BY ts DESC LIMIT 1
    """)).fetchone()
    print(f"=== Volland last save: {dict(r._mapping)}\n")

    # 2) All today's real trades with broker P&L
    r = c.execute(text("""
        SELECT sl.id, sl.setup_name, sl.direction, sl.grade, sl.paradigm,
               sl.ts AT TIME ZONE 'America/New_York' AS et,
               rto.state->>'fill_price' AS fill,
               rto.state->>'close_reason' AS close_reason,
               rto.state->>'status' AS status,
               sl.outcome_result, sl.outcome_pnl,
               COALESCE(
                   rto.state->>'stop_fill_price',
                   rto.state->>'target_fill_price',
                   rto.state->>'flatten_fill_price',
                   rto.state->>'close_fill_price'
               ) AS close_fill
        FROM setup_log sl
        JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.ts::date = '2026-05-21'
        ORDER BY sl.ts ASC
    """)).fetchall()

    print(f"=== Today's REAL trades: {len(r)}\n")
    total_sim_pnl = 0.0
    total_broker_pnl = 0.0
    broker_n = 0
    wins = 0
    losses = 0
    open_n = 0
    print(f"{'lid':>5} {'time':<6} {'setup':<20} {'dir':<6} {'grade':<4} {'sim':>6} {'broker':>8} {'status':<10} {'reason':<25}")
    for row in r:
        d = dict(row._mapping)
        fill = float(d["fill"]) if d["fill"] else None
        close_fill = float(d["close_fill"]) if d["close_fill"] else None
        sim_pnl = float(d["outcome_pnl"]) if d["outcome_pnl"] is not None else 0.0
        broker_pnl = None
        if fill and close_fill:
            sign = 1 if d["direction"] in ("long", "bullish") else -1
            broker_pnl = sign * (close_fill - fill)
        total_sim_pnl += sim_pnl
        if broker_pnl is not None:
            total_broker_pnl += broker_pnl
            broker_n += 1
        if d["outcome_result"] == "WIN":
            wins += 1
        elif d["outcome_result"] == "LOSS":
            losses += 1
        if d["status"] != "closed":
            open_n += 1
        b_str = f"{broker_pnl:+8.2f}" if broker_pnl is not None else "    n/a"
        print(f"{d['id']:>5} {d['et'].strftime('%H:%M'):<6} {d['setup_name']:<20} {d['direction']:<6} {d['grade']:<4} {sim_pnl:>+6.1f} {b_str:>8} {d['status']:<10} {(d['close_reason'] or '')[:25]}")

    print(f"\n=== TOTALS ===")
    print(f"  Trades: {len(r)} (wins={wins}, losses={losses}, open={open_n})")
    print(f"  Sim P&L (SPX pts):    {total_sim_pnl:+.1f}")
    print(f"  Broker P&L (MES pts): {total_broker_pnl:+.1f} (from {broker_n} closeable trades)")
    print(f"  Broker $ @ 1 MES:     ${total_broker_pnl * 5:+.2f}")

    # 3) Skip reasons
    r = c.execute(text("""
        SELECT real_trade_skip_reason, COUNT(*) AS n
        FROM setup_log
        WHERE ts::date = '2026-05-21'
          AND real_trade_skip_reason IS NOT NULL
        GROUP BY real_trade_skip_reason ORDER BY n DESC
    """)).fetchall()
    print(f"\n=== Skip reasons today ===")
    for row in r: print(f"  {dict(row._mapping)}")
