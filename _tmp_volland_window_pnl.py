"""Compare today's TSRT P&L during Volland-degraded vs healthy windows."""
import os, json
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"])

# Volland degraded window: 9:21 ET → 12:50 ET = 13:21 UTC → 16:50 UTC
# Volland healthy window: 12:50 ET onwards
with eng.connect() as c:
    print("=== Today's REAL trades — Volland-degraded window vs healthy ===\n")
    r = c.execute(text("""
        SELECT sl.id, sl.setup_name, sl.direction, sl.grade, sl.paradigm,
               sl.ts AT TIME ZONE 'America/New_York' AS et,
               rto.state->>'fill_price' AS fill,
               rto.state->>'close_reason' AS close_reason,
               sl.outcome_result, sl.outcome_pnl,
               -- get close fill from state
               COALESCE(
                   rto.state->>'stop_fill_price',
                   rto.state->>'target_fill_price',
                   rto.state->>'flatten_fill_price',
                   rto.state->>'close_fill_price'
               ) AS close_fill,
               CASE
                 WHEN sl.ts < '2026-05-21 16:50:00+00' THEN 'DEGRADED'
                 ELSE 'HEALTHY'
               END AS volland_state
        FROM setup_log sl
        JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.ts::date = '2026-05-21'
        ORDER BY sl.ts ASC
    """)).fetchall()

    deg_pnl_sim = 0.0
    deg_pnl_real = 0.0
    deg_count = 0
    healthy_pnl_sim = 0.0
    healthy_pnl_real = 0.0
    healthy_count = 0

    print(f"{'lid':>5} {'time':<6} {'setup':<22} {'dir':<6} {'grade':<4} {'paradigm':<14} {'sim P&L':>8} {'broker P&L':>10} {'window':<9}")
    for row in r:
        d = dict(row._mapping)
        fill = float(d["fill"]) if d["fill"] else None
        close_fill = float(d["close_fill"]) if d["close_fill"] else None
        sim_pnl = float(d["outcome_pnl"]) if d["outcome_pnl"] is not None else 0.0
        # broker P&L if we have both fill prices
        broker_pnl = None
        if fill and close_fill:
            sign = 1 if d["direction"] in ("long", "bullish") else -1
            broker_pnl = sign * (close_fill - fill)
        print(f"{d['id']:>5} {d['et'].strftime('%H:%M'):<6} {d['setup_name']:<22} {d['direction']:<6} {d['grade']:<4} {(d['paradigm'] or 'NULL'):<14} {sim_pnl:>+8.1f} {(f'{broker_pnl:+8.2f}' if broker_pnl is not None else '   n/a'):>10} {d['volland_state']:<9}")
        if d["volland_state"] == "DEGRADED":
            deg_pnl_sim += sim_pnl
            if broker_pnl is not None:
                deg_pnl_real += broker_pnl
            deg_count += 1
        else:
            healthy_pnl_sim += sim_pnl
            if broker_pnl is not None:
                healthy_pnl_real += broker_pnl
            healthy_count += 1

    print(f"\n--- TOTALS ---")
    print(f"Volland DEGRADED window (9:21-12:50 ET): {deg_count} trades, sim P&L {deg_pnl_sim:+.1f} pts, broker {deg_pnl_real:+.2f} pts")
    print(f"Volland HEALTHY window (12:50+ ET):     {healthy_count} trades, sim P&L {healthy_pnl_sim:+.1f} pts, broker {healthy_pnl_real:+.2f} pts")

    # Count trades where paradigm was NULL at time of placement (would be stale Greek context)
    print(f"\n--- Trades placed with paradigm=NULL (stale Greek context) ---")
    r = c.execute(text("""
        SELECT sl.id, sl.setup_name, sl.direction, sl.grade,
               sl.ts AT TIME ZONE 'America/New_York' AS et,
               sl.outcome_result, sl.outcome_pnl
        FROM setup_log sl
        JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.ts::date = '2026-05-21'
          AND sl.paradigm IS NULL
        ORDER BY sl.ts ASC
    """)).fetchall()
    null_pnl = 0
    for row in r:
        d = dict(row._mapping)
        sim_pnl = float(d["outcome_pnl"]) if d["outcome_pnl"] is not None else 0.0
        null_pnl += sim_pnl
        print(f"  lid={d['id']} {d['et'].strftime('%H:%M')} {d['setup_name']} {d['direction']} grade={d['grade']} result={d['outcome_result']} pnl={sim_pnl:+.1f}")
    print(f"\nTrades placed with NULL paradigm: {len(r)}, total P&L: {null_pnl:+.1f} pts")
