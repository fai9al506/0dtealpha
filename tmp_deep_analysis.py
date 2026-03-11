import sqlalchemy as sa
import os
from datetime import date, timedelta
from collections import defaultdict

engine = sa.create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:

    # =========================================================
    # PART 1: Greek regime over time — are Greeks EVER bearish?
    # =========================================================
    print("=" * 70)
    print("PART 1: Daily Greek regime — vanna_all values")
    print("=" * 70)
    rows = conn.execute(sa.text("""
        SELECT ts::date as dt,
               avg(vanna_all) as avg_vanna,
               min(vanna_all) as min_vanna,
               max(vanna_all) as max_vanna,
               count(*) as n,
               sum(CASE WHEN vanna_all > 0 THEN 1 ELSE 0 END) as pos_vanna,
               sum(CASE WHEN vanna_all < 0 THEN 1 ELSE 0 END) as neg_vanna
        FROM setup_log
        WHERE vanna_all IS NOT NULL
        GROUP BY ts::date
        ORDER BY ts::date
    """)).fetchall()
    print("Date        | Avg Vanna       | Min/Max                    | Pos/Neg/Total")
    for r in rows:
        print("{} | {:>15.0f} | {:>12.0f} / {:>12.0f} | {}/{}/{}".format(
            r.dt, float(r.avg_vanna), float(r.min_vanna), float(r.max_vanna),
            int(r.pos_vanna), int(r.neg_vanna), int(r.n)))

    # =========================================================
    # PART 2: GEX position — how often is spot above +GEX?
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 2: GEX position — spot vs max_plus_gex")
    print("=" * 70)
    rows2 = conn.execute(sa.text("""
        SELECT ts::date as dt,
               avg(spot) as avg_spot,
               avg(max_plus_gex) as avg_plus_gex,
               sum(CASE WHEN spot <= max_plus_gex THEN 1 ELSE 0 END) as below_gex,
               sum(CASE WHEN spot > max_plus_gex THEN 1 ELSE 0 END) as above_gex,
               count(*) as n
        FROM setup_log
        WHERE spot IS NOT NULL AND max_plus_gex IS NOT NULL
        GROUP BY ts::date
        ORDER BY ts::date
    """)).fetchall()
    print("Date        | Avg Spot | Avg +GEX | Below(bull)/Above(bear)/Total")
    for r in rows2:
        print("{} | {:>8.1f} | {:>8.1f} | {}/{}/{}".format(
            r.dt, float(r.avg_spot), float(r.avg_plus_gex),
            int(r.below_gex), int(r.above_gex), int(r.n)))

    # =========================================================
    # PART 3: Alignment distribution by day + day PnL
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 3: Daily alignment distribution + PnL")
    print("=" * 70)
    rows3 = conn.execute(sa.text("""
        SELECT ts::date as dt,
               sum(CASE WHEN greek_alignment = 3 THEN 1 ELSE 0 END) as a3,
               sum(CASE WHEN greek_alignment = 2 THEN 1 ELSE 0 END) as a2,
               sum(CASE WHEN greek_alignment = 1 THEN 1 ELSE 0 END) as a1,
               sum(CASE WHEN greek_alignment = 0 THEN 1 ELSE 0 END) as a0,
               sum(CASE WHEN greek_alignment = -1 THEN 1 ELSE 0 END) as am1,
               sum(CASE WHEN greek_alignment = -2 THEN 1 ELSE 0 END) as am2,
               sum(CASE WHEN greek_alignment = -3 THEN 1 ELSE 0 END) as am3,
               sum(outcome_pnl) as day_pnl,
               count(*) as n
        FROM setup_log
        WHERE greek_alignment IS NOT NULL AND outcome_result IS NOT NULL
        GROUP BY ts::date
        ORDER BY ts::date
    """)).fetchall()
    print("Date        | +3  +2  +1   0  -1  -2  -3 | DayPnL  | N")
    for r in rows3:
        print("{} | {:>3d} {:>3d} {:>3d} {:>3d} {:>3d} {:>3d} {:>3d} | {:>+7.1f} | {}".format(
            r.dt, int(r.a3), int(r.a2), int(r.a1), int(r.a0),
            int(r.am1), int(r.am2), int(r.am3),
            float(r.day_pnl or 0), int(r.n)))

    # =========================================================
    # PART 4: Top 20 best SHORT trades (by PnL)
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 4: Top 20 best SHORT trades (by PnL)")
    print("=" * 70)
    rows4 = conn.execute(sa.text("""
        SELECT id, ts, setup_name, direction, grade, score, spot, lis, target,
               outcome_result, outcome_pnl, outcome_max_profit,
               vanna_all, spot_vol_beta, greek_alignment,
               paradigm, max_plus_gex, max_minus_gex
        FROM setup_log
        WHERE direction IN ('short', 'bearish')
          AND outcome_result IS NOT NULL
        ORDER BY outcome_pnl DESC
        LIMIT 20
    """)).fetchall()
    for r in rows4:
        align = int(r.greek_alignment) if r.greek_alignment is not None else 0
        print("#{} {} {:20s} {:8s} g={} s={} a={:+d} | {:8s} {:+.1f} (max {:+.1f}) | spot={} lis={} | para={} vanna={}".format(
            r.id, str(r.ts)[:16], r.setup_name, r.direction, r.grade, r.score, align,
            r.outcome_result, float(r.outcome_pnl or 0), float(r.outcome_max_profit or 0),
            r.spot, r.lis,
            r.paradigm,
            "{:.0f}".format(float(r.vanna_all)) if r.vanna_all else "null"))

    # =========================================================
    # PART 5: All BofA Scalp SHORT trades
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 5: All BofA Scalp SHORT trades")
    print("=" * 70)
    rows5 = conn.execute(sa.text("""
        SELECT id, ts, grade, score, spot, lis, paradigm,
               outcome_result, outcome_pnl, outcome_max_profit,
               vanna_all, greek_alignment,
               bofa_stop_level, bofa_target_level, bofa_lis_width
        FROM setup_log
        WHERE setup_name = 'BofA Scalp' AND direction IN ('short', 'bearish')
          AND outcome_result IS NOT NULL
        ORDER BY ts
    """)).fetchall()
    wins = sum(1 for r in rows5 if "WIN" in (r.outcome_result or ""))
    losses = sum(1 for r in rows5 if "LOSS" in (r.outcome_result or ""))
    total_pnl = sum(float(r.outcome_pnl or 0) for r in rows5)
    print("BofA Short: {} trades, {} W / {} L, {:+.1f} pts".format(len(rows5), wins, losses, total_pnl))
    for r in rows5:
        align = int(r.greek_alignment) if r.greek_alignment is not None else 0
        print("  #{} {} g={} s={} a={:+d} | {:8s} {:+.1f} (max {:+.1f}) | spot={} lis={} width={} | para={}".format(
            r.id, str(r.ts)[:16], r.grade, r.score, align,
            r.outcome_result, float(r.outcome_pnl or 0), float(r.outcome_max_profit or 0),
            r.spot, r.lis, r.bofa_lis_width, r.paradigm))

    # =========================================================
    # PART 6: Skew Charm by direction and alignment
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 6: Skew Charm by direction x alignment")
    print("=" * 70)
    rows6 = conn.execute(sa.text("""
        SELECT direction, greek_alignment,
               count(*) as n,
               sum(CASE WHEN outcome_result LIKE :w THEN 1 ELSE 0 END) as wins,
               sum(CASE WHEN outcome_result LIKE :l THEN 1 ELSE 0 END) as losses,
               sum(outcome_pnl) as pnl
        FROM setup_log
        WHERE setup_name = 'Skew Charm' AND outcome_result IS NOT NULL
        GROUP BY direction, greek_alignment
        ORDER BY direction, greek_alignment
    """), {"w": "%WIN%", "l": "%LOSS%"}).fetchall()
    for r in rows6:
        wr = int(r.wins) / (int(r.wins) + int(r.losses)) * 100 if (int(r.wins) + int(r.losses)) else 0
        align = int(r.greek_alignment) if r.greek_alignment is not None else 0
        print("  {:5s} align={:+d}: {:3d} trades, {:+7.1f} pts, {:.0f}% WR ({} W / {} L)".format(
            r.direction, align, int(r.n), float(r.pnl or 0), wr, int(r.wins), int(r.losses)))

    # =========================================================
    # PART 7: Paradigm distribution by day
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 7: Paradigm by day")
    print("=" * 70)
    rows7 = conn.execute(sa.text("""
        SELECT ts::date as dt, paradigm,
               count(*) as n,
               sum(outcome_pnl) as pnl
        FROM setup_log
        WHERE paradigm IS NOT NULL AND outcome_result IS NOT NULL
        GROUP BY ts::date, paradigm
        ORDER BY ts::date, paradigm
    """)).fetchall()
    by_date = defaultdict(dict)
    for r in rows7:
        by_date[str(r.dt)][r.paradigm] = {"n": int(r.n), "pnl": float(r.pnl or 0)}
    for dt in sorted(by_date.keys()):
        parts = []
        for p, d in sorted(by_date[dt].items()):
            parts.append("{}:{}t/{:+.0f}".format(p, d["n"], d["pnl"]))
        print("{} | {}".format(dt, "  ".join(parts)))

    # =========================================================
    # PART 8: Spot-Vol-Beta daily
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 8: Spot-Vol-Beta daily")
    print("=" * 70)
    rows8 = conn.execute(sa.text("""
        SELECT ts::date as dt,
               avg(spot_vol_beta) as avg_svb,
               min(spot_vol_beta) as min_svb,
               max(spot_vol_beta) as max_svb
        FROM setup_log
        WHERE spot_vol_beta IS NOT NULL
        GROUP BY ts::date
        ORDER BY ts::date
    """)).fetchall()
    for r in rows8:
        print("{} | avg={:+.3f} | min={:+.3f} max={:+.3f}".format(
            r.dt, float(r.avg_svb), float(r.min_svb), float(r.max_svb)))

    # =========================================================
    # PART 9: Winning SHORT trades — alignment distribution
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 9: Winning SHORT trades — alignment distribution")
    print("=" * 70)
    rows9 = conn.execute(sa.text("""
        SELECT greek_alignment, count(*) as n, sum(outcome_pnl) as pnl
        FROM setup_log
        WHERE direction IN ('short', 'bearish')
          AND outcome_result LIKE :w
        GROUP BY greek_alignment
        ORDER BY greek_alignment
    """), {"w": "%WIN%"}).fetchall()
    total_wins = sum(int(r.n) for r in rows9)
    for r in rows9:
        align = int(r.greek_alignment) if r.greek_alignment is not None else 0
        pct = int(r.n) / total_wins * 100
        print("  align={:+d}: {:3d} wins ({:.1f}%), {:+.1f} pts".format(
            align, int(r.n), pct, float(r.pnl or 0)))

    # =========================================================
    # PART 10: DD Exhaustion short — alignment vs PnL
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 10: DD Exhaustion SHORT — alignment vs PnL")
    print("=" * 70)
    rows10 = conn.execute(sa.text("""
        SELECT greek_alignment,
               count(*) as n,
               sum(CASE WHEN outcome_result LIKE :w THEN 1 ELSE 0 END) as wins,
               sum(CASE WHEN outcome_result LIKE :l THEN 1 ELSE 0 END) as losses,
               sum(outcome_pnl) as pnl
        FROM setup_log
        WHERE setup_name = 'DD Exhaustion' AND direction = 'short'
          AND outcome_result IS NOT NULL
        GROUP BY greek_alignment
        ORDER BY greek_alignment
    """), {"w": "%WIN%", "l": "%LOSS%"}).fetchall()
    for r in rows10:
        align = int(r.greek_alignment) if r.greek_alignment is not None else 0
        wr = int(r.wins) / (int(r.wins) + int(r.losses)) * 100 if (int(r.wins) + int(r.losses)) else 0
        print("  align={:+d}: {:3d} trades, {:+7.1f} pts, {:.0f}% WR".format(
            align, int(r.n), float(r.pnl or 0), wr))

    # =========================================================
    # PART 11: AG Short — alignment vs PnL
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 11: AG Short — alignment vs PnL")
    print("=" * 70)
    rows11 = conn.execute(sa.text("""
        SELECT greek_alignment,
               count(*) as n,
               sum(CASE WHEN outcome_result LIKE :w THEN 1 ELSE 0 END) as wins,
               sum(CASE WHEN outcome_result LIKE :l THEN 1 ELSE 0 END) as losses,
               sum(outcome_pnl) as pnl
        FROM setup_log
        WHERE setup_name = 'AG Short'
          AND outcome_result IS NOT NULL
        GROUP BY greek_alignment
        ORDER BY greek_alignment
    """), {"w": "%WIN%", "l": "%LOSS%"}).fetchall()
    for r in rows11:
        align = int(r.greek_alignment) if r.greek_alignment is not None else 0
        wr = int(r.wins) / (int(r.wins) + int(r.losses)) * 100 if (int(r.wins) + int(r.losses)) else 0
        print("  align={:+d}: {:3d} trades, {:+7.1f} pts, {:.0f}% WR".format(
            align, int(r.n), float(r.pnl or 0), wr))

    # =========================================================
    # PART 12: How many days had ALL-BULLISH Greeks vs mixed?
    # =========================================================
    print("")
    print("=" * 70)
    print("PART 12: Daily Greek regime classification")
    print("=" * 70)
    rows12 = conn.execute(sa.text("""
        SELECT ts::date as dt,
               bool_and(vanna_all > 0) as all_vanna_pos,
               bool_and(vanna_all < 0) as all_vanna_neg,
               bool_or(vanna_all > 0) AND bool_or(vanna_all < 0) as vanna_mixed,
               avg(vanna_all) as avg_vanna,
               sum(outcome_pnl) as day_pnl,
               count(*) as n
        FROM setup_log
        WHERE vanna_all IS NOT NULL AND outcome_result IS NOT NULL
        GROUP BY ts::date
        ORDER BY ts::date
    """)).fetchall()
    all_bull = 0
    all_bear = 0
    mixed = 0
    for r in rows12:
        if r.all_vanna_pos:
            regime = "ALL-BULL"
            all_bull += 1
        elif r.all_vanna_neg:
            regime = "ALL-BEAR"
            all_bear += 1
        else:
            regime = "MIXED"
            mixed += 1
        print("{} {:9s} avg_vanna={:>12.0f} | {:+7.1f} pts ({} trades)".format(
            r.dt, regime, float(r.avg_vanna), float(r.day_pnl or 0), int(r.n)))
    print("")
    print("All-Bullish days: {}, All-Bearish days: {}, Mixed days: {}".format(all_bull, all_bear, mixed))
