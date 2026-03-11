import sqlalchemy as sa
import os
from datetime import date
from collections import defaultdict

engine = sa.create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    # 1. Direction breakdown for +3 alignment (all time)
    print("=== All-time +3 alignment: direction breakdown ===")
    rows = conn.execute(sa.text(
        "SELECT direction, count(*) as n, sum(outcome_pnl) as pnl "
        "FROM setup_log WHERE greek_alignment = 3 AND outcome_result IS NOT NULL "
        "GROUP BY direction ORDER BY n DESC"
    )).fetchall()
    total = sum(int(r.n) for r in rows)
    for r in rows:
        pct = int(r.n) / total * 100
        print("  {:8s}: {:3d} trades ({:.1f}%), {:+.1f} pts".format(r.direction, int(r.n), pct, float(r.pnl or 0)))
    print("  Total: {}".format(total))
    bull_count = sum(int(r.n) for r in rows if r.direction in ("long", "bullish"))
    print("  Bull/Long: {}/{} = {:.1f}%".format(bull_count, total, bull_count/total*100))

    # 2. Direction breakdown for -3 alignment (all time)
    print("")
    print("=== All-time -3 alignment: direction breakdown ===")
    rows2 = conn.execute(sa.text(
        "SELECT direction, count(*) as n, sum(outcome_pnl) as pnl "
        "FROM setup_log WHERE greek_alignment = -3 AND outcome_result IS NOT NULL "
        "GROUP BY direction ORDER BY n DESC"
    )).fetchall()
    total2 = sum(int(r.n) for r in rows2)
    for r in rows2:
        pct = int(r.n) / total2 * 100
        print("  {:8s}: {:3d} trades ({:.1f}%), {:+.1f} pts".format(r.direction, int(r.n), pct, float(r.pnl or 0)))
    bear_count = sum(int(r.n) for r in rows2 if r.direction in ("short", "bearish"))
    print("  Bear/Short: {}/{} = {:.1f}%".format(bear_count, total2, bear_count/total2*100))

    # 3. What are the underlying Greek values for +3 trades?
    print("")
    print("=== +3 trades: Greek regime (charm, vanna, GEX position) ===")
    rows3 = conn.execute(sa.text(
        "SELECT id, ts::date as dt, setup_name, direction, spot, "
        "vanna_all, max_plus_gex, greek_alignment "
        "FROM setup_log WHERE greek_alignment = 3 AND outcome_result IS NOT NULL "
        "ORDER BY ts"
    )).fetchall()
    # Check how many have positive vanna (bullish) vs negative
    pos_vanna = sum(1 for r in rows3 if r.vanna_all is not None and float(r.vanna_all) > 0)
    neg_vanna = sum(1 for r in rows3 if r.vanna_all is not None and float(r.vanna_all) < 0)
    null_vanna = sum(1 for r in rows3 if r.vanna_all is None)
    print("  Vanna positive (bullish): {}".format(pos_vanna))
    print("  Vanna negative (bearish): {}".format(neg_vanna))
    print("  Vanna null: {}".format(null_vanna))

    # Check GEX position (spot below max_plus_gex = bullish)
    below_gex = sum(1 for r in rows3 if r.max_plus_gex and r.spot and float(r.spot) <= float(r.max_plus_gex))
    above_gex = sum(1 for r in rows3 if r.max_plus_gex and r.spot and float(r.spot) > float(r.max_plus_gex))
    null_gex = sum(1 for r in rows3 if not r.max_plus_gex or not r.spot)
    print("  Spot below +GEX (bullish): {}".format(below_gex))
    print("  Spot above +GEX (bearish): {}".format(above_gex))
    print("  GEX null: {}".format(null_gex))

    # 4. Setup breakdown for +3
    print("")
    print("=== +3 by setup name ===")
    rows4 = conn.execute(sa.text(
        "SELECT setup_name, direction, count(*) as n "
        "FROM setup_log WHERE greek_alignment = 3 AND outcome_result IS NOT NULL "
        "GROUP BY setup_name, direction ORDER BY n DESC"
    )).fetchall()
    for r in rows4:
        print("  {:20s} {:8s}: {} trades".format(r.setup_name, r.direction, int(r.n)))

    # 5. Check: do ANY short/bearish setups ever get +3?
    print("")
    print("=== Short/bearish setups that got +3 (should need all Greeks bearish) ===")
    rows5 = conn.execute(sa.text(
        "SELECT id, ts::date as dt, setup_name, direction, vanna_all, spot, max_plus_gex "
        "FROM setup_log WHERE greek_alignment = 3 "
        "AND direction IN ('short', 'bearish') "
        "ORDER BY ts"
    )).fetchall()
    print("  Count: {}".format(len(rows5)))
    for r in rows5:
        v = float(r.vanna_all) if r.vanna_all else None
        gex_pos = "below" if (r.spot and r.max_plus_gex and float(r.spot) <= float(r.max_plus_gex)) else "above"
        print("  #{} {} {:20s} vanna={} spot {} +GEX".format(r.id, r.dt, r.setup_name, v, gex_pos))

    # 6. Check charm values for all +3 trades
    print("")
    print("=== Charm values for +3 long trades (should all be positive) ===")
    # charm is not stored directly in setup_log, but we know from the algo:
    # +3 long requires: charm>0, vanna>0, spot<=max_plus_gex
    # +3 short requires: charm<0, vanna<0, spot>max_plus_gex
    # So +3 can only be short when ALL THREE are bearish
    print("  By design: +3 LONG needs charm>0, vanna>0, spot<+GEX (all bullish)")
    print("  By design: +3 SHORT needs charm<0, vanna<0, spot>+GEX (all bearish)")
    print("  If Greeks are mostly bullish (recent market), shorts CANNOT get +3")
