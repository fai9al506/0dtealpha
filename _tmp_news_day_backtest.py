"""News-day analysis: does economic_events let us avoid days like Fri Jun 5?

1. Friday Jun 5 market characterization (spot path, range, vs other days)
2. TSRT real day P&L per ET day (per-lid broker fills, MCHK-validated method)
3. Tag days by news type from economic_events; compare P&L news vs non-news
"""
import os
from collections import defaultdict
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    # ── 1. Friday market path (SPX spot from chain snapshots) ──
    print("=== Fri Jun 5 SPX spot path (chain_snapshots, 30-min sampling) ===")
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') AS et, spot
        FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
        ORDER BY ts
    """)).fetchall()
    if rows:
        spots = [float(r[1]) for r in rows if r[1]]
        o, hi, lo, cl = spots[0], max(spots), min(spots), spots[-1]
        print(f"  open={o:.0f} high={hi:.0f} low={lo:.0f} close={cl:.0f} "
              f"range={hi-lo:.0f}pts  net={cl-o:+.0f}pts")
        step = max(1, len(rows)//13)
        for r in rows[::step]:
            print(f"    {r[0].strftime('%H:%M')}  {float(r[1]):.0f}")

    # ── daily realized range distribution (last 60 trading days) ──
    print("\n=== Daily SPX realized range, last 60 sessions ===")
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date AS d,
               MAX(spot) - MIN(spot) AS rng,
               (ARRAY_AGG(spot ORDER BY ts))[1] AS o,
               (ARRAY_AGG(spot ORDER BY ts DESC))[1] AS c
        FROM chain_snapshots
        WHERE ts > NOW() - INTERVAL '90 days'
        GROUP BY 1 ORDER BY 1
    """)).fetchall()
    ranges = {str(r[0]): float(r[1]) for r in rows}
    rng_sorted = sorted(ranges.values())
    jun5 = ranges.get('2026-06-05', 0)
    import bisect
    pct = bisect.bisect_left(rng_sorted, jun5) / max(1, len(rng_sorted)) * 100
    print(f"  Jun 5 range = {jun5:.0f} pts -> {pct:.0f}th percentile of last {len(rng_sorted)} sessions")
    print(f"  median range = {rng_sorted[len(rng_sorted)//2]:.0f} pts")
    top5 = sorted(ranges.items(), key=lambda x: -x[1])[:5]
    print("  top-5 range days:", ", ".join(f"{d}({v:.0f})" for d, v in top5))

    # ── 2. real day P&L per ET day from real_trade_orders fills ──
    rows = c.execute(text("""
        SELECT (sl.ts AT TIME ZONE 'America/New_York')::date AS d,
               sl.direction, rto.state
        FROM setup_log sl
        JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        ORDER BY sl.ts
    """)).fetchall()
    day_pnl = defaultdict(float)
    day_n = defaultdict(int)
    for d, direction, state in rows:
        st = state or {}
        fill = st.get("fill_price")
        exit_p = st.get("stop_fill_price") or st.get("close_fill_price")
        if fill is None or exit_p is None:
            continue
        is_long = (direction or "").lower() in ("long", "bullish")
        pts = (float(exit_p) - float(fill)) if is_long else (float(fill) - float(exit_p))
        qty = int(st.get("quantity") or 1)
        day_pnl[str(d)] += pts * 5.0 * qty
        day_n[str(d)] += 1

    # ── 3. news tags per day ──
    ev = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date AS d,
               (ts AT TIME ZONE 'America/New_York')::time AS t,
               title, impact
        FROM economic_events WHERE country = 'USD'
    """)).fetchall()
    tags = defaultdict(set)
    for d, t, title, impact in ev:
        d = str(d)
        if (impact or "").lower() == "high":
            if t.hour == 8:
                tags[d].add("HIGH_830")          # NFP/CPI class, pre-market
            elif 9 <= t.hour < 16:
                tags[d].add("HIGH_INTRADAY")     # ISM, FOMC 14:00, etc.
        tl = (title or "").lower()
        if "non-farm" in tl:
            tags[d].add("NFP")
        if tl.startswith("cpi") or " cpi" in tl:
            tags[d].add("CPI")
        if "fomc statement" in tl or "federal funds rate" in tl:
            tags[d].add("FOMC")

    def bucket(days):
        n = len(days)
        if n == 0:
            return "n=0"
        tot = sum(day_pnl[d] for d in days)
        green = sum(1 for d in days if day_pnl[d] > 0)
        return f"n={n:>3}  total=${tot:+8.0f}  avg=${tot/n:+6.0f}/day  green {green}/{n}"

    all_days = sorted(day_pnl.keys())
    print(f"\n=== Real TSRT day P&L (per-lid fills), {all_days[0]} -> {all_days[-1]} ===")

    for era_name, era_days in [("FULL real era", all_days),
                               ("post-V16 (May 18+)", [d for d in all_days if d >= "2026-05-18"])]:
        print(f"\n--- {era_name} ---")
        nfp = [d for d in era_days if "NFP" in tags[d]]
        cpi = [d for d in era_days if "CPI" in tags[d]]
        fomc = [d for d in era_days if "FOMC" in tags[d]]
        h830 = [d for d in era_days if "HIGH_830" in tags[d]]
        hintra = [d for d in era_days if "HIGH_INTRADAY" in tags[d] and "HIGH_830" not in tags[d]]
        none_ = [d for d in era_days if not ({"HIGH_830", "HIGH_INTRADAY"} & tags[d])]
        print(f"  ALL days          {bucket(era_days)}")
        print(f"  NFP days          {bucket(nfp)}   {nfp}")
        print(f"  CPI days          {bucket(cpi)}   {cpi}")
        print(f"  FOMC days         {bucket(fomc)}   {fomc}")
        print(f"  any HIGH @8:30    {bucket(h830)}")
        print(f"  HIGH intraday only{bucket(hintra)}")
        print(f"  no HIGH news      {bucket(none_)}")

    # worst real days + their tags
    print("\n=== 8 worst real days, with news tags + SPX range ===")
    worst = sorted(all_days, key=lambda d: day_pnl[d])[:8]
    for d in worst:
        print(f"  {d}  ${day_pnl[d]:+7.0f}  ({day_n[d]:>2}t)  range={ranges.get(d, 0):>4.0f}pts  tags={sorted(tags[d]) or '-'}")

    # Friday's skip reasons (did the breaker fire?)
    print("\n=== Jun 5 real_trade_skip_reason counts ===")
    rows = c.execute(text("""
        SELECT real_trade_skip_reason, COUNT(*)
        FROM setup_log
        WHERE (ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
          AND real_trade_skip_reason IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]}")
