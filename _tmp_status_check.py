import os
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"])
with eng.connect() as c:
    # 1) Volland freshness
    r = c.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' AS et,
               payload->'statistics'->>'paradigm' AS para,
               payload->'statistics'->>'lines_in_sand' AS lis,
               payload->'spy_statistics'->>'delta_decay_hedging' AS spy_dd,
               EXTRACT(EPOCH FROM (now() - ts))::int AS age_s
        FROM volland_snapshots ORDER BY ts DESC LIMIT 1
    """)).fetchone()
    print(f"Volland: {dict(r._mapping)}")

    # 2) Open real positions
    r = c.execute(text("""
        SELECT rto.setup_log_id, sl.setup_name, sl.direction, sl.grade,
               rto.state->>'status' AS status, rto.state->>'fill_price' AS fill,
               rto.state->>'current_stop' AS stop,
               rto.state->>'max_favorable' AS mfe,
               rto.created_at AT TIME ZONE 'America/New_York' AS et
        FROM real_trade_orders rto
        JOIN setup_log sl ON sl.id = rto.setup_log_id
        WHERE rto.state->>'status' = 'filled'
        ORDER BY rto.setup_log_id DESC
    """)).fetchall()
    print(f"\nOpen TSRT positions: {len(r)}")
    for row in r:
        print(f"  {dict(row._mapping)}")

    # 3) Today's TSRT P&L (real_trade_orders closed today)
    r = c.execute(text("""
        SELECT COUNT(*) AS trades,
               SUM(CASE WHEN sl.outcome_result='WIN' THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN sl.outcome_result='LOSS' THEN 1 ELSE 0 END) AS losses
        FROM real_trade_orders rto
        JOIN setup_log sl ON sl.id = rto.setup_log_id
        WHERE rto.created_at::date = '2026-05-21'
          AND rto.state->>'status' = 'closed'
    """)).fetchone()
    print(f"\nToday's TSRT trades: {dict(r._mapping)}")

    # 4) Recent skips
    r = c.execute(text("""
        SELECT real_trade_skip_reason, COUNT(*) AS n
        FROM setup_log
        WHERE ts::date = '2026-05-21'
          AND real_trade_skip_reason IS NOT NULL
        GROUP BY real_trade_skip_reason ORDER BY n DESC
    """)).fetchall()
    print("\nToday's skip reasons:")
    for row in r: print(f"  {dict(row._mapping)}")
