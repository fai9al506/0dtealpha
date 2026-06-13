"""Refined news-day analysis: true NFP vs ADP split, per-day table, Jun 5 forensic."""
import os
from collections import defaultdict
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    # all non-farm titled events — map true NFP vs ADP
    print("=== all 'Non-Farm' USD events in DB ===")
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') AS et, title, impact
        FROM economic_events
        WHERE country='USD' AND title ILIKE '%non-farm%'
        ORDER BY ts
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]} | {r[2]:<6} | {r[1]}")

    # real day P&L
    rows = c.execute(text("""
        SELECT (sl.ts AT TIME ZONE 'America/New_York')::date AS d, sl.direction, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        ORDER BY sl.ts
    """)).fetchall()
    day_pnl, day_n = defaultdict(float), defaultdict(int)
    for d, direction, state in rows:
        st = state or {}
        fill, exit_p = st.get("fill_price"), (st.get("stop_fill_price") or st.get("close_fill_price"))
        if fill is None or exit_p is None:
            continue
        is_long = (direction or "").lower() in ("long", "bullish")
        pts = (float(exit_p) - float(fill)) if is_long else (float(fill) - float(exit_p))
        day_pnl[str(d)] += pts * 5.0 * int(st.get("quantity") or 1)
        day_n[str(d)] += 1

    # tags: TRUE_NFP (8:30 'Non-Farm Employment Change', not ADP), ADP, CPI, FOMC, OTHER_HIGH_830
    ev = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date AS d,
               (ts AT TIME ZONE 'America/New_York')::time AS t, title, impact
        FROM economic_events WHERE country='USD'
    """)).fetchall()
    tags = defaultdict(set)
    for d, t, title, impact in ev:
        d, tl, hi = str(d), (title or "").lower(), (impact or "").lower() == "high"
        if "non-farm" in tl and "adp" in tl:
            tags[d].add("ADP")
        elif "non-farm" in tl:
            tags[d].add("NFP")
        if tl.startswith("cpi") or " cpi " in f" {tl} ":
            tags[d].add("CPI")
        if "fomc statement" in tl or "federal funds rate" in tl or "fomc press conference" in tl:
            tags[d].add("FOMC")
        if hi and t.hour == 8 and "non-farm" not in tl:
            tags[d].add("OTHER_HIGH_830")

    print("\n=== per-day table: every real-traded day with a major tag ===")
    all_days = sorted(day_pnl.keys())
    major = {"NFP", "ADP", "CPI", "FOMC", "OTHER_HIGH_830"}
    for d in all_days:
        tg = sorted(tags[d] & major)
        if tg:
            era = "V16" if d >= "2026-05-18" else "pre"
            print(f"  {d} [{era}]  ${day_pnl[d]:+7.0f}  ({day_n[d]:>2}t)  {','.join(tg)}")

    def bucket(days):
        n = len(days)
        if not n:
            return "n=0"
        tot = sum(day_pnl[d] for d in days)
        return f"n={n:>3}  total=${tot:+8.0f}  avg=${tot/n:+6.0f}/day  green {sum(1 for d in days if day_pnl[d]>0)}/{n}"

    print("\n=== corrected buckets (FULL real era) ===")
    for name, sel in [("TRUE NFP", lambda d: "NFP" in tags[d]),
                      ("ADP (no NFP)", lambda d: "ADP" in tags[d] and "NFP" not in tags[d]),
                      ("CPI", lambda d: "CPI" in tags[d]),
                      ("no major tag", lambda d: not (tags[d] & major))]:
        print(f"  {name:<14} {bucket([d for d in all_days if sel(d)])}")

    # ── Jun 5 forensic: per-hour entry P&L + per-setup ──
    print("\n=== Jun 5 trades by entry hour (broker P&L) ===")
    rows = c.execute(text("""
        SELECT (sl.ts AT TIME ZONE 'America/New_York') AS et, sl.setup_name, sl.direction, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
        ORDER BY sl.ts
    """)).fetchall()
    hour_pnl, hour_n = defaultdict(float), defaultdict(int)
    setup_pnl, setup_n = defaultdict(float), defaultdict(int)
    dir_pnl = defaultdict(float)
    for et, name, direction, st in rows:
        st = st or {}
        fill, exit_p = st.get("fill_price"), (st.get("stop_fill_price") or st.get("close_fill_price"))
        if fill is None or exit_p is None:
            continue
        is_long = (direction or "").lower() in ("long", "bullish")
        usd = ((float(exit_p) - float(fill)) if is_long else (float(fill) - float(exit_p))) * 5.0 * int(st.get("quantity") or 1)
        hour_pnl[et.hour] += usd; hour_n[et.hour] += 1
        setup_pnl[name] += usd; setup_n[name] += 1
        dir_pnl["LONG" if is_long else "SHORT"] += usd
    for h in sorted(hour_pnl):
        print(f"  {h:02d}:xx  ${hour_pnl[h]:+7.0f}  ({hour_n[h]}t)")
    print("  per setup:")
    for s in sorted(setup_pnl, key=lambda x: setup_pnl[x]):
        print(f"    {s:<16} ${setup_pnl[s]:+7.0f} ({setup_n[s]}t)")
    print("  per direction:", {k: f"${v:+.0f}" for k, v in dir_pnl.items()})
