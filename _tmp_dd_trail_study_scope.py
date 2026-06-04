"""DD trail study — STEP 1: data scoping (Gate 1).
Counts DD Exhaustion populations, checks fill data availability, era boundaries.
"""
import os, json
from sqlalchemy import create_engine, text

eng = create_engine(os.environ["DATABASE_URL"], connect_args={"options": "-c default_transaction_read_only=on"})

with eng.connect() as c:
    # 1) placed real DD trades (broker truth era, May 18+)
    rows = c.execute(text("""
        SELECT date(sl.ts AT TIME ZONE 'America/New_York') d, count(*),
               count(*) FILTER (WHERE rto.state->>'status'='closed') closed
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.setup_name='DD Exhaustion'
        GROUP BY 1 ORDER BY 1
    """)).fetchall()
    print("=== PLACED real DD trades by date ===")
    tot=0
    for d,n,cl in rows: print(f"  {d}  n={n} closed={cl}"); tot+=n
    print(f"  TOTAL placed: {tot}")

    # 2) sample one state JSONB to see fill fields
    r = c.execute(text("""
        SELECT rto.setup_log_id, rto.state FROM real_trade_orders rto
        JOIN setup_log sl ON sl.id=rto.setup_log_id
        WHERE sl.setup_name='DD Exhaustion' AND rto.state->>'status'='closed'
        ORDER BY rto.setup_log_id DESC LIMIT 2
    """)).fetchall()
    print("\n=== sample closed DD state keys/values ===")
    for lid, st in r:
        s = st if isinstance(st, dict) else json.loads(st)
        keep = {k: v for k, v in s.items() if any(x in k for x in
            ("fill", "price", "pnl", "close", "entry", "stop", "trail", "direction", "qty", "status", "ts", "reason"))}
        print(f"--- lid={lid}")
        for k in sorted(keep): print(f"    {k} = {keep[k]}")

    # 3) all DD long signals (graded, non-LOG) by month — candidate secondary population
    rows = c.execute(text("""
        SELECT to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM') m,
               count(*) FILTER (WHERE lower(sl.direction) IN ('long','bullish')) longs,
               count(*) shorts_and_longs,
               count(*) FILTER (WHERE sl.real_trade_skip_reason IS NULL) no_skip
        FROM setup_log sl
        WHERE sl.setup_name='DD Exhaustion' AND sl.grade IS NOT NULL AND sl.grade != 'LOG'
        GROUP BY 1 ORDER BY 1
    """)).fetchall()
    print("\n=== graded DD signals by month (longs / all / skipNULL) ===")
    for m,l,a,ns in rows: print(f"  {m}  longs={l} all={a} skip_null={ns}")

    # 4) trail params stored on setup_log for DD (any overrides?)
    rows = c.execute(text("""
        SELECT DISTINCT trail_sl, trail_activation, trail_gap, count(*) OVER (PARTITION BY trail_sl, trail_activation, trail_gap)
        FROM setup_log WHERE setup_name='DD Exhaustion' AND ts >= '2026-04-01'
    """)).fetchall()
    print("\n=== distinct DD trail params on setup_log since Apr 1 ===")
    for r2 in rows: print(f"  sl={r2[0]} act={r2[1]} gap={r2[2]} n={r2[3]}")

    # 5) vps_es_range_bars coverage
    rows = c.execute(text("""
        SELECT min(ts_start), max(ts_start), count(*) FROM vps_es_range_bars WHERE range_pts=5
    """)).fetchone()
    print(f"\n=== vps_es_range_bars 5pt coverage: {rows[0]} -> {rows[1]} n={rows[2]}")
