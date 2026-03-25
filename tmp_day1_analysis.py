#!/usr/bin/env python3
"""Day 1 real-money analysis."""
import sqlalchemy as sa, os, sys

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    # All SC setups from Mar 25
    rows = conn.execute(sa.text("""
        SELECT id, direction, grade, score, spot,
               outcome_result, outcome_pnl, outcome_elapsed_min,
               greek_alignment, paradigm, overvix,
               ts AT TIME ZONE 'US/Eastern' as ts_et
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND ts::date = '2026-03-25'
        ORDER BY id
    """)).fetchall()

    print(f"=== SC Signals Mar 25 ({len(rows)} total) ===")
    for r in rows:
        pnl_str = f"{r.outcome_pnl:+.1f}" if r.outcome_pnl else "--"
        align = r.greek_alignment if r.greek_alignment is not None else 0
        print(f"#{r.id} {r.direction:5s} {r.grade}({r.score:2d}) spot={r.spot:.0f} "
              f"align={align:+d} paradigm={r.paradigm or '?':20s} "
              f"=> {r.outcome_result or 'OPEN':8s} {pnl_str:>6s} {r.outcome_elapsed_min or 0:3.0f}m "
              f"{r.ts_et.strftime('%H:%M')}")

    # SC A+/A/B March 1-24 performance
    rows2 = conn.execute(sa.text("""
        SELECT outcome_result, COUNT(*) as cnt,
               SUM(outcome_pnl) as total_pnl,
               ROUND(AVG(outcome_pnl)::numeric, 1) as avg_pnl
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND ts::date BETWEEN '2026-03-01' AND '2026-03-24'
          AND grade IN ('A+', 'A', 'B')
          AND outcome_result IS NOT NULL
        GROUP BY outcome_result
    """)).fetchall()
    print(f"\n=== SC A+/A/B March 1-24 (reference) ===")
    total_w = total_l = 0
    for r in rows2:
        print(f"  {r.outcome_result}: {r.cnt} trades, total={float(r.total_pnl):+.1f} pts, avg={r.avg_pnl}")
        if r.outcome_result == 'WIN': total_w = r.cnt
        elif r.outcome_result == 'LOSS': total_l = r.cnt
    if total_w + total_l > 0:
        print(f"  WR: {total_w/(total_w+total_l)*100:.1f}%")

    # Daily breakdown
    rows3 = conn.execute(sa.text("""
        SELECT ts::date as d, COUNT(*) as cnt,
               SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as losses,
               SUM(CASE WHEN outcome_result='EXPIRED' THEN 1 ELSE 0 END) as expired,
               SUM(outcome_pnl) as pnl
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND ts::date BETWEEN '2026-03-10' AND '2026-03-25'
          AND grade IN ('A+', 'A', 'B')
          AND outcome_result IS NOT NULL
        GROUP BY ts::date
        ORDER BY d
    """)).fetchall()
    print(f"\n=== SC Daily P&L (A+/A/B, recent) ===")
    for r in rows3:
        wr = r.wins/(r.wins+r.losses)*100 if (r.wins+r.losses)>0 else 0
        flag = " <<<< DAY 1 REAL" if str(r.d) == "2026-03-25" else ""
        print(f"  {r.d}: {r.cnt}t {r.wins}W/{r.losses}L/{r.expired}E ({wr:.0f}%) pnl={float(r.pnl):+.1f}{flag}")

    # Worst SC days in March for context
    print(f"\n=== Worst SC days in March (A+/A/B) ===")
    rows5 = conn.execute(sa.text("""
        SELECT ts::date as d, SUM(outcome_pnl) as pnl,
               COUNT(*) as cnt,
               SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as losses
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND ts::date BETWEEN '2026-03-01' AND '2026-03-25'
          AND grade IN ('A+', 'A', 'B')
          AND outcome_result IS NOT NULL
        GROUP BY ts::date
        HAVING SUM(outcome_pnl) < 0
        ORDER BY SUM(outcome_pnl)
    """)).fetchall()
    for r in rows5:
        flag = " <<<< DAY 1" if str(r.d) == "2026-03-25" else ""
        print(f"  {r.d}: {r.cnt}t {r.wins}W/{r.losses}L pnl={float(r.pnl):+.1f}{flag}")

    # Real trader trade log
    print(f"\n=== Real trader orders (from DB) ===")
    rows7 = conn.execute(sa.text("""
        SELECT setup_log_id, state
        FROM real_trade_orders
        ORDER BY setup_log_id
    """)).fetchall()
    import json
    for r in rows7:
        s = json.loads(r.state) if isinstance(r.state, str) else r.state
        print(f"  log_id={r.setup_log_id} {s.get('setup_name','')} {s.get('direction','')} "
              f"fill={s.get('fill_price','')} status={s.get('status','')} "
              f"pnl={s.get('pnl','')}")

    # Market context
    print(f"\n=== Market context Mar 25 ===")
    rows6 = conn.execute(sa.text("""
        SELECT paradigm, lis,
               ts AT TIME ZONE 'US/Eastern' as ts_et
        FROM volland_snapshots
        WHERE ts::date = '2026-03-25'
          AND paradigm IS NOT NULL
        ORDER BY ts DESC LIMIT 1
    """)).fetchall()
    for r in rows6:
        print(f"  Last paradigm: {r.paradigm}, LIS: {r.lis}")
