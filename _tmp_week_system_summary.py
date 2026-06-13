"""Our system's week Jun 1-5: per-day broker P&L, per-setup/direction, + market context."""
import os
from collections import defaultdict
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
DAYS = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"]

with eng.connect() as c:
    # market context per day
    print("=== SPX per day (chain_snapshots) ===")
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date AS d,
               (ARRAY_AGG(spot ORDER BY ts))[1] AS o, MAX(spot) AS h, MIN(spot) AS l,
               (ARRAY_AGG(spot ORDER BY ts DESC))[1] AS cl
        FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date = ANY(:days)
        GROUP BY 1 ORDER BY 1
    """), {"days": DAYS}).fetchall()
    for r in rows:
        vals = [float(x) if x is not None else None for x in r[1:]]
        if any(v is None for v in vals):
            print(f"  {r[0]}  (nulls in spot aggregation: {vals})")
            continue
        o, h, l, cl = vals
        print(f"  {r[0]}  O {o:.0f}  H {h:.0f}  L {l:.0f}  C {cl:.0f}   net {cl-o:+.0f}  range {h-l:.0f}")

    # real trades per day
    rows = c.execute(text("""
        SELECT (sl.ts AT TIME ZONE 'America/New_York')::date AS d,
               (sl.ts AT TIME ZONE 'America/New_York') AS et,
               sl.setup_name, sl.direction, sl.paradigm, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = ANY(:days)
        ORDER BY sl.ts
    """), {"days": DAYS}).fetchall()
    day_tot = defaultdict(float)
    day_n = defaultdict(int)
    day_setup = defaultdict(lambda: defaultdict(lambda: [0.0, 0]))
    for d, et, name, direction, paradigm, st in rows:
        st = st or {}
        fill = st.get("fill_price")
        exit_p = st.get("stop_fill_price") or st.get("close_fill_price")
        if fill is None or exit_p is None:
            continue
        is_long = (direction or "").lower() in ("long", "bullish")
        usd = ((float(exit_p) - float(fill)) if is_long else (float(fill) - float(exit_p))) * 5.0 * int(st.get("quantity") or 1)
        d = str(d)
        day_tot[d] += usd
        day_n[d] += 1
        k = f"{name} {'L' if is_long else 'S'}"
        day_setup[d][k][0] += usd
        day_setup[d][k][1] += 1

    print("\n=== TSRT real per day ===")
    wk = 0.0
    for d in DAYS:
        print(f"  {d}: ${day_tot[d]:+7.0f} ({day_n[d]}t)")
        for k in sorted(day_setup[d], key=lambda x: day_setup[d][x][0]):
            usd, n = day_setup[d][k]
            print(f"      {k:<20} ${usd:+7.0f} ({n}t)")
        wk += day_tot[d]
    print(f"  WEEK TOTAL: ${wk:+.0f}")

    # paradigm timeline per day (volland) - first + most common per day
    print("\n=== Volland paradigm by day (mode + first/last) ===")
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date AS d, paradigm, COUNT(*)
        FROM volland_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date = ANY(:days)
          AND paradigm IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """), {"days": DAYS}).fetchall()
    cur = None
    for r in rows:
        if str(r[0]) != cur:
            cur = str(r[0])
            print(f"  {cur}: ", end="")
        print(f"{r[1]}({r[2]}) ", end="")
    print()
