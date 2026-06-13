"""Jun 3 per-setup bleed + Jun 5 breaker timing."""
import os
from collections import defaultdict
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    for day in ("2026-06-03", "2026-06-05"):
        rows = c.execute(text("""
            SELECT (sl.ts AT TIME ZONE 'America/New_York') AS et, sl.setup_name, sl.direction, rto.state
            FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
            WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = :d ORDER BY sl.ts
        """), {"d": day}).fetchall()
        agg = defaultdict(lambda: [0.0, 0])
        cum = 0.0
        last_et = None
        for et, name, direction, st in rows:
            st = st or {}
            fill, exit_p = st.get("fill_price"), (st.get("stop_fill_price") or st.get("close_fill_price"))
            if fill is None or exit_p is None:
                continue
            is_long = (direction or "").lower() in ("long", "bullish")
            usd = ((float(exit_p) - float(fill)) if is_long else (float(fill) - float(exit_p))) * 5.0 * int(st.get("quantity") or 1)
            k = f"{name} {'L' if is_long else 'S'}"
            agg[k][0] += usd
            agg[k][1] += 1
            cum += usd
            last_et = et
        print(f"=== {day}: net ${cum:+.0f}, last entry {last_et} ===")
        for k in sorted(agg, key=lambda x: agg[x][0]):
            print(f"  {k:<22} ${agg[k][0]:+7.0f} ({agg[k][1]}t)")

    # breaker + guard skip timestamps on Jun 5
    print("\n=== Jun 5 risk-control skips (ET) ===")
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') AS et, setup_name, direction, real_trade_skip_reason
        FROM setup_log
        WHERE (ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
          AND real_trade_skip_reason IN ('daily_loss_limit','underwater_stack_block','cap_long_full','dd_short_block')
        ORDER BY ts
    """)).fetchall()
    for r in rows:
        print(f"  {r[0].strftime('%H:%M:%S')}  {r[1]} {r[2]}  -> {r[3]}")
