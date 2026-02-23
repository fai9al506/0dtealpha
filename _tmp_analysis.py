"""Comprehensive trading analysis across all 64+ trades"""
import os, json
from collections import defaultdict
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT id, ts, setup_name, direction, grade, score, spot, lis, target,
               paradigm, first_hour, abs_vol_ratio, abs_es_price,
               outcome_result, outcome_pnl, outcome_stop_level,
               outcome_elapsed_min, outcome_max_profit, outcome_max_loss,
               bofa_max_hold_minutes, gap_to_lis, rr_ratio
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    print(f"Total resolved trades: {len(trades)}")
    print()

    # =============================================
    # 1. OVERALL PERFORMANCE BY SETUP
    # =============================================
    print("=" * 80)
    print("1. OVERALL PERFORMANCE BY SETUP")
    print("=" * 80)

    by_setup = defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0, "trades": []})
    for t in trades:
        s = by_setup[t['setup_name']]
        r = t['outcome_result']
        s["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        s["pnl"] += float(t['outcome_pnl'] or 0)
        s["trades"].append(t)

    grand = 0
    for name, d in sorted(by_setup.items()):
        total = d["W"] + d["L"] + d["X"]
        wr = d["W"] / total * 100 if total else 0
        grand += d["pnl"]
        avg_win = sum(float(t['outcome_pnl']) for t in d["trades"] if t['outcome_result'] == 'WIN') / d["W"] if d["W"] else 0
        avg_loss = sum(float(t['outcome_pnl']) for t in d["trades"] if t['outcome_result'] == 'LOSS') / d["L"] if d["L"] else 0
        print(f"\n  {name}: {total} trades, WR={wr:.0f}%, NET={d['pnl']:+.1f}")
        print(f"    W={d['W']} L={d['L']} X={d['X']} | avg_win={avg_win:+.1f} avg_loss={avg_loss:+.1f}")
    print(f"\n  GRAND TOTAL: {grand:+.1f}")

    # =============================================
    # 2. WIN RATE BY GRADE
    # =============================================
    print("\n" + "=" * 80)
    print("2. WIN RATE BY GRADE (across all setups)")
    print("=" * 80)

    by_grade = defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0})
    for t in trades:
        g = t['grade'] or 'N/A'
        d = by_grade[g]
        r = t['outcome_result']
        d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        d["pnl"] += float(t['outcome_pnl'] or 0)

    for grade in sorted(by_grade.keys()):
        d = by_grade[grade]
        total = d["W"] + d["L"] + d["X"]
        wr = d["W"] / total * 100 if total else 0
        print(f"  Grade {grade:12s}: {total:2d} trades, WR={wr:5.1f}%, NET={d['pnl']:+7.1f}")

    # =============================================
    # 3. WIN RATE BY GRADE PER SETUP
    # =============================================
    print("\n" + "=" * 80)
    print("3. WIN RATE BY GRADE PER SETUP")
    print("=" * 80)

    by_setup_grade = defaultdict(lambda: defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0}))
    for t in trades:
        d = by_setup_grade[t['setup_name']][t['grade'] or 'N/A']
        r = t['outcome_result']
        d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        d["pnl"] += float(t['outcome_pnl'] or 0)

    for setup in sorted(by_setup_grade.keys()):
        print(f"\n  {setup}:")
        for grade in sorted(by_setup_grade[setup].keys()):
            d = by_setup_grade[setup][grade]
            total = d["W"] + d["L"] + d["X"]
            wr = d["W"] / total * 100 if total else 0
            print(f"    Grade {grade:12s}: {total:2d} trades, WR={wr:5.1f}%, NET={d['pnl']:+7.1f}")

    # =============================================
    # 4. TIME OF DAY ANALYSIS
    # =============================================
    print("\n" + "=" * 80)
    print("4. TIME OF DAY ANALYSIS")
    print("=" * 80)

    by_hour = defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0})
    for t in trades:
        ts = t['ts']
        # Convert to ET (subtract 5 hours from UTC as approximation)
        hour_utc = ts.hour
        hour_et = (hour_utc - 5) % 24
        d = by_hour[hour_et]
        r = t['outcome_result']
        d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        d["pnl"] += float(t['outcome_pnl'] or 0)

    for hour in sorted(by_hour.keys()):
        d = by_hour[hour]
        total = d["W"] + d["L"] + d["X"]
        wr = d["W"] / total * 100 if total else 0
        print(f"  {hour:2d}:00 ET: {total:2d} trades, WR={wr:5.1f}%, NET={d['pnl']:+7.1f}")

    # =============================================
    # 5. TIME OF DAY BY SETUP
    # =============================================
    print("\n" + "=" * 80)
    print("5. TIME OF DAY BY SETUP (key setups)")
    print("=" * 80)

    by_setup_hour = defaultdict(lambda: defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0}))
    for t in trades:
        hour_et = (t['ts'].hour - 5) % 24
        d = by_setup_hour[t['setup_name']][hour_et]
        r = t['outcome_result']
        d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        d["pnl"] += float(t['outcome_pnl'] or 0)

    for setup in sorted(by_setup_hour.keys()):
        print(f"\n  {setup}:")
        for hour in sorted(by_setup_hour[setup].keys()):
            d = by_setup_hour[setup][hour]
            total = d["W"] + d["L"] + d["X"]
            wr = d["W"] / total * 100 if total else 0
            print(f"    {hour:2d}:00 ET: {total:2d} trades, WR={wr:5.1f}%, NET={d['pnl']:+7.1f}")

    # =============================================
    # 6. PARADIGM ANALYSIS (BofA vs others)
    # =============================================
    print("\n" + "=" * 80)
    print("6. PARADIGM ANALYSIS")
    print("=" * 80)

    by_paradigm = defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0, "count": 0})
    for t in trades:
        p = (t['paradigm'] or 'N/A').upper()
        d = by_paradigm[p]
        d["count"] += 1
        r = t['outcome_result']
        d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        d["pnl"] += float(t['outcome_pnl'] or 0)

    for para in sorted(by_paradigm.keys()):
        d = by_paradigm[para]
        total = d["count"]
        wr = d["W"] / total * 100 if total else 0
        print(f"  {para:20s}: {total:2d} trades, WR={wr:5.1f}%, NET={d['pnl']:+7.1f}")

    # =============================================
    # 7. DIRECTION ANALYSIS
    # =============================================
    print("\n" + "=" * 80)
    print("7. DIRECTION ANALYSIS")
    print("=" * 80)

    by_dir = defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0})
    for t in trades:
        d = by_dir[t['direction']]
        r = t['outcome_result']
        d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        d["pnl"] += float(t['outcome_pnl'] or 0)

    for direction in sorted(by_dir.keys()):
        d = by_dir[direction]
        total = d["W"] + d["L"] + d["X"]
        wr = d["W"] / total * 100 if total else 0
        print(f"  {direction:10s}: {total:2d} trades, WR={wr:5.1f}%, NET={d['pnl']:+7.1f}")

    # =============================================
    # 8. DAILY P&L
    # =============================================
    print("\n" + "=" * 80)
    print("8. DAILY P&L")
    print("=" * 80)

    by_date = defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0, "count": 0})
    for t in trades:
        d_str = str(t['ts'])[:10]
        d = by_date[d_str]
        d["count"] += 1
        r = t['outcome_result']
        d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        d["pnl"] += float(t['outcome_pnl'] or 0)

    running = 0
    for date in sorted(by_date.keys()):
        d = by_date[date]
        running += d["pnl"]
        wr = d["W"] / d["count"] * 100 if d["count"] else 0
        print(f"  {date}: {d['count']:2d} trades, WR={wr:5.1f}%, day={d['pnl']:+7.1f}, cumulative={running:+7.1f}")

    # =============================================
    # 9. SCORE ANALYSIS (do higher scores win more?)
    # =============================================
    print("\n" + "=" * 80)
    print("9. SCORE BRACKETS (does score predict outcome?)")
    print("=" * 80)

    score_brackets = [(0, 30, "0-30"), (30, 50, "30-50"), (50, 70, "50-70"), (70, 90, "70-90"), (90, 101, "90-100")]
    by_score = defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0})
    for t in trades:
        sc = float(t['score'] or 0)
        for lo, hi, label in score_brackets:
            if lo <= sc < hi:
                d = by_score[label]
                r = t['outcome_result']
                d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
                d["pnl"] += float(t['outcome_pnl'] or 0)
                break

    for label in ["0-30", "30-50", "50-70", "70-90", "90-100"]:
        d = by_score.get(label, {"W": 0, "L": 0, "X": 0, "pnl": 0})
        total = d["W"] + d["L"] + d["X"]
        if total == 0:
            continue
        wr = d["W"] / total * 100 if total else 0
        print(f"  Score {label:6s}: {total:2d} trades, WR={wr:5.1f}%, NET={d['pnl']:+7.1f}")

    # =============================================
    # 10. WORST TRADES (learning opportunities)
    # =============================================
    print("\n" + "=" * 80)
    print("10. WORST TRADES (biggest losses)")
    print("=" * 80)

    sorted_by_pnl = sorted(trades, key=lambda t: float(t['outcome_pnl'] or 0))
    for t in sorted_by_pnl[:10]:
        hour_et = (t['ts'].hour - 5) % 24
        print(f"  #{t['id']:3d} {str(t['ts'])[:10]} {hour_et:2d}:{t['ts'].minute:02d} {t['setup_name']:16s} {t['direction']:5s} "
              f"grade={t['grade']:6s} score={float(t['score'] or 0):5.1f} "
              f"PNL={float(t['outcome_pnl']):+6.1f} paradigm={t['paradigm'] or 'N/A'}")

    # =============================================
    # 11. BEST TRADES
    # =============================================
    print("\n" + "=" * 80)
    print("11. BEST TRADES (biggest wins)")
    print("=" * 80)

    for t in sorted_by_pnl[-10:]:
        hour_et = (t['ts'].hour - 5) % 24
        print(f"  #{t['id']:3d} {str(t['ts'])[:10]} {hour_et:2d}:{t['ts'].minute:02d} {t['setup_name']:16s} {t['direction']:5s} "
              f"grade={t['grade']:6s} score={float(t['score'] or 0):5.1f} "
              f"PNL={float(t['outcome_pnl']):+6.1f} paradigm={t['paradigm'] or 'N/A'}")

    # =============================================
    # 12. FIRST HOUR FLAG ANALYSIS
    # =============================================
    print("\n" + "=" * 80)
    print("12. FIRST HOUR FLAG (first_hour=True vs False)")
    print("=" * 80)

    by_fh = defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0})
    for t in trades:
        fh = "first_hour" if t['first_hour'] else "after_first_hour"
        d = by_fh[fh]
        r = t['outcome_result']
        d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        d["pnl"] += float(t['outcome_pnl'] or 0)

    for label in ["first_hour", "after_first_hour"]:
        d = by_fh.get(label, {"W": 0, "L": 0, "X": 0, "pnl": 0})
        total = d["W"] + d["L"] + d["X"]
        if total == 0:
            continue
        wr = d["W"] / total * 100 if total else 0
        print(f"  {label:20s}: {total:2d} trades, WR={wr:5.1f}%, NET={d['pnl']:+7.1f}")

    # =============================================
    # 13. DD EXHAUSTION DETAILED BREAKDOWN
    # =============================================
    print("\n" + "=" * 80)
    print("13. DD EXHAUSTION DETAILED ANALYSIS")
    print("=" * 80)

    dd_trades = [t for t in trades if t['setup_name'] == 'DD Exhaustion']
    print(f"  Total DD trades: {len(dd_trades)}")

    # By elapsed time
    dd_elapsed = defaultdict(lambda: {"W": 0, "L": 0, "X": 0, "pnl": 0})
    for t in dd_trades:
        elapsed = t['outcome_elapsed_min'] or 0
        if elapsed <= 30:
            bucket = "0-30min"
        elif elapsed <= 60:
            bucket = "30-60min"
        elif elapsed <= 120:
            bucket = "60-120min"
        else:
            bucket = "120+min"
        d = dd_elapsed[bucket]
        r = t['outcome_result']
        d["W" if r == "WIN" else "L" if r == "LOSS" else "X"] += 1
        d["pnl"] += float(t['outcome_pnl'] or 0)

    print("\n  By elapsed time:")
    for bucket in ["0-30min", "30-60min", "60-120min", "120+min"]:
        d = dd_elapsed.get(bucket, {"W": 0, "L": 0, "X": 0, "pnl": 0})
        total = d["W"] + d["L"] + d["X"]
        if total == 0:
            continue
        wr = d["W"] / total * 100 if total else 0
        print(f"    {bucket:10s}: {total:2d} trades, WR={wr:5.1f}%, NET={d['pnl']:+7.1f}")

    # DD individual trades
    print("\n  All DD trades:")
    for t in dd_trades:
        hour_et = (t['ts'].hour - 5) % 24
        print(f"    #{t['id']:3d} {str(t['ts'])[:10]} {hour_et:2d}:{t['ts'].minute:02d} {t['direction']:5s} "
              f"grade={t['grade']:6s} {t['outcome_result']:7s} PNL={float(t['outcome_pnl']):+6.1f} "
              f"elapsed={t['outcome_elapsed_min'] or '?'}min")

    # =============================================
    # 14. AG SHORT DETAILED BREAKDOWN
    # =============================================
    print("\n" + "=" * 80)
    print("14. AG SHORT DETAILED ANALYSIS")
    print("=" * 80)

    ag_trades = [t for t in trades if t['setup_name'] == 'AG Short']
    print(f"  Total AG trades: {len(ag_trades)}")

    # Gap to LIS analysis for AG
    print("\n  All AG trades:")
    for t in ag_trades:
        hour_et = (t['ts'].hour - 5) % 24
        gap = t['gap_to_lis']
        rr = t['rr_ratio']
        print(f"    #{t['id']:3d} {str(t['ts'])[:10]} {hour_et:2d}:{t['ts'].minute:02d} {t['direction']:5s} "
              f"grade={t['grade']:6s} score={float(t['score'] or 0):5.1f} "
              f"{t['outcome_result']:7s} PNL={float(t['outcome_pnl']):+6.1f} "
              f"gap={gap} rr={rr}")

    # =============================================
    # 15. GEX LONG DETAILED BREAKDOWN
    # =============================================
    print("\n" + "=" * 80)
    print("15. GEX LONG DETAILED ANALYSIS")
    print("=" * 80)

    gex_trades = [t for t in trades if t['setup_name'] == 'GEX Long']
    print(f"  Total GEX trades: {len(gex_trades)}")
    print("\n  All GEX trades:")
    for t in gex_trades:
        hour_et = (t['ts'].hour - 5) % 24
        print(f"    #{t['id']:3d} {str(t['ts'])[:10]} {hour_et:2d}:{t['ts'].minute:02d} {t['direction']:5s} "
              f"grade={t['grade']:6s} score={float(t['score'] or 0):5.1f} "
              f"{t['outcome_result']:7s} PNL={float(t['outcome_pnl']):+6.1f} "
              f"elapsed={t['outcome_elapsed_min'] or '?'}min")
