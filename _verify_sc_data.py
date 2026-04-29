import os, csv
from sqlalchemy import create_engine, text

db = os.getenv('DATABASE_URL', '').replace('postgres://', 'postgresql://')
engine = create_engine(db)

with engine.connect() as conn:
    # 1. Mar 26 contaminated SC trades
    r = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND (ts AT TIME ZONE 'America/New_York')::date = '2026-03-26'
        ORDER BY id
    """)).fetchall()
    print(f"Mar 26 SC trades: {len(r)}")
    for row in r:
        print(f"  id={row[0]} outcome={row[2]} pnl={row[3]} mfe={row[4]}")

    # 2. SL actually used per trade (from outcome_stop_level)
    r2 = conn.execute(text("""
        SELECT id, (ts AT TIME ZONE 'America/New_York')::date as d,
               direction, spot, outcome_stop_level,
               CASE WHEN direction = 'long' THEN ROUND((spot - outcome_stop_level)::numeric, 1)
                    ELSE ROUND((outcome_stop_level - spot)::numeric, 1) END as sl_used,
               outcome_result, outcome_pnl
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND outcome_result IN ('WIN','LOSS','EXPIRED')
          AND outcome_stop_level IS NOT NULL
        ORDER BY id
    """)).fetchall()

    # Group by SL used
    sl_counts = {}
    for row in r2:
        sl = row[5]
        if sl not in sl_counts:
            sl_counts[sl] = {'n': 0, 'dates': set()}
        sl_counts[sl]['n'] += 1
        sl_counts[sl]['dates'].add(str(row[1]))

    print(f"\nSL values used across all SC trades:")
    for sl in sorted(sl_counts.keys()):
        dates = sorted(sl_counts[sl]['dates'])
        print(f"  SL={sl}: {sl_counts[sl]['n']} trades, dates: {dates[0]} to {dates[-1]}")

    # 3. SC A+/A/B split by SL era
    r3 = conn.execute(text("""
        SELECT
            CASE WHEN (ts AT TIME ZONE 'America/New_York')::date < '2026-03-18' THEN 'pre_mar18 (SL=20)'
                 ELSE 'post_mar18 (SL=14)' END as period,
            COUNT(*) as n,
            COUNT(*) FILTER (WHERE outcome_result = 'WIN') as wins,
            COUNT(*) FILTER (WHERE outcome_result = 'LOSS') as losses,
            COUNT(*) FILTER (WHERE outcome_result = 'EXPIRED') as expired,
            ROUND(SUM(outcome_pnl)::numeric, 1) as total_pnl,
            ROUND(AVG(outcome_pnl) FILTER (WHERE outcome_result = 'LOSS')::numeric, 1) as avg_loss,
            ROUND(AVG(outcome_max_profit) FILTER (WHERE outcome_result = 'WIN')::numeric, 1) as avg_mfe_w,
            ROUND(AVG(outcome_max_profit) FILTER (WHERE outcome_result = 'LOSS')::numeric, 1) as avg_mfe_l
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND outcome_result IN ('WIN','LOSS','EXPIRED')
          AND grade IN ('A+', 'A', 'B')
        GROUP BY period
        ORDER BY period
    """)).fetchall()
    print(f"\nSC A+/A/B by SL era:")
    for row in r3:
        wr = row[2] / row[1] * 100 if row[1] else 0
        print(f"  {row[0]}: {row[1]}t, {row[2]}W/{row[3]}L/{row[4]}E, WR {wr:.0f}%, PnL={row[5]}, avgLoss={row[6]}, avgMFE(W)={row[7]}, avgMFE(L)={row[8]}")

    # 4. High MFE outliers (>50 pts) - check for stale data
    r4 = conn.execute(text("""
        SELECT id, (ts AT TIME ZONE 'America/New_York') as ts_et,
               direction, spot, outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss, outcome_elapsed_min, grade
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND outcome_result IN ('WIN','LOSS','EXPIRED')
          AND outcome_max_profit > 50
        ORDER BY outcome_max_profit DESC
    """)).fetchall()
    print(f"\nSC trades with MFE > 50 (potential stale data):")
    for row in r4:
        ts_str = row[1].strftime('%m-%d %H:%M') if row[1] else '?'
        print(f"  id={row[0]} {ts_str} {row[2]} spot={row[3]:.1f} {row[4]}({row[5]:+.1f}) MFE={row[6]:.1f} MAE={row[7]:.1f} {row[8]}min grade={row[9]}")

    # 5. Check loss PnL distribution (should be exactly -14 or -20)
    r5 = conn.execute(text("""
        SELECT outcome_pnl, COUNT(*) as n
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND outcome_result = 'LOSS'
          AND grade IN ('A+', 'A', 'B')
        GROUP BY outcome_pnl
        ORDER BY outcome_pnl
    """)).fetchall()
    print(f"\nSC A+/A/B LOSS PnL distribution (should be -14 or -20):")
    for row in r5:
        print(f"  PnL={row[0]:+.1f}: {row[1]} trades")

    # 6. POST-Mar 18 only (SL=14 era, what we'd deploy with)
    r6 = conn.execute(text("""
        SELECT
            COUNT(*) as n,
            COUNT(*) FILTER (WHERE outcome_result = 'WIN') as wins,
            COUNT(*) FILTER (WHERE outcome_result = 'LOSS') as losses,
            COUNT(*) FILTER (WHERE outcome_result = 'EXPIRED') as expired,
            ROUND(SUM(outcome_pnl)::numeric, 1) as total_pnl,
            ROUND(AVG(outcome_pnl) FILTER (WHERE outcome_result = 'WIN')::numeric, 1) as avg_win,
            ROUND(AVG(outcome_pnl) FILTER (WHERE outcome_result = 'LOSS')::numeric, 1) as avg_loss,
            ROUND(AVG(outcome_max_profit) FILTER (WHERE outcome_result = 'WIN')::numeric, 1) as avg_mfe_w,
            ROUND(AVG(outcome_max_profit) FILTER (WHERE outcome_result = 'LOSS')::numeric, 1) as avg_mfe_l,
            COUNT(*) FILTER (WHERE outcome_result = 'LOSS' AND outcome_max_profit >= 10) as loss_touched_act
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
          AND outcome_result IN ('WIN','LOSS','EXPIRED')
          AND grade IN ('A+', 'A', 'B')
          AND (ts AT TIME ZONE 'America/New_York')::date >= '2026-03-18'
    """)).fetchall()
    row = r6[0]
    wr = row[1] / row[0] * 100 if row[0] else 0
    print(f"\nPOST-MAR 18 ONLY (SL=14 era, current live config):")
    print(f"  {row[0]}t: {row[1]}W/{row[2]}L/{row[3]}E, WR {wr:.0f}%")
    print(f"  PnL: {row[4]}, AvgWin: {row[5]}, AvgLoss: {row[6]}")
    print(f"  AvgMFE(W): {row[7]}, AvgMFE(L): {row[8]}")
    print(f"  Losers that touched activation (MFE>=10): {row[9]}")

    # 7. V12 filter on post-Mar 18
    r7 = conn.execute(text("""
        WITH sc AS (
            SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
                   direction, grade, spot, paradigm, greek_alignment,
                   outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss
            FROM setup_log
            WHERE setup_name = 'Skew Charm'
              AND outcome_result IN ('WIN','LOSS','EXPIRED')
              AND grade IN ('A+', 'A', 'B')
              AND (ts AT TIME ZONE 'America/New_York')::date >= '2026-03-18'
        )
        SELECT id, ts_et, direction, grade, spot, paradigm, greek_alignment,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss
        FROM sc
        WHERE NOT (EXTRACT(HOUR FROM ts_et) = 14 AND EXTRACT(MINUTE FROM ts_et) >= 30
                   AND EXTRACT(HOUR FROM ts_et) < 15)
          AND NOT (EXTRACT(HOUR FROM ts_et) >= 15 AND EXTRACT(MINUTE FROM ts_et) >= 30)
          AND NOT (direction = 'short' AND paradigm = 'GEX-LIS')
          AND (direction = 'short'
               OR (direction = 'long' AND COALESCE(greek_alignment, 0) >= 2))
        ORDER BY id
    """)).fetchall()

    wins = sum(1 for r in r7 if r[7] == 'WIN')
    losses = sum(1 for r in r7 if r[7] == 'LOSS')
    expired = sum(1 for r in r7 if r[7] == 'EXPIRED')
    total_pnl = sum(r[8] for r in r7 if r[8])
    avg_win = sum(r[8] for r in r7 if r[7] == 'WIN') / wins if wins else 0
    avg_loss = sum(r[8] for r in r7 if r[7] == 'LOSS') / losses if losses else 0
    avg_mfe_w = sum(r[9] for r in r7 if r[7] == 'WIN' and r[9]) / wins if wins else 0
    avg_mfe_l = sum(r[9] for r in r7 if r[7] == 'LOSS' and r[9] is not None) / losses if losses else 0
    loss_touched = sum(1 for r in r7 if r[7] == 'LOSS' and r[9] and r[9] >= 10)

    print(f"\nPOST-MAR 18 + V12 FILTER (the REAL dataset for deployment):")
    print(f"  {len(r7)}t: {wins}W/{losses}L/{expired}E, WR {wins/len(r7)*100:.0f}%")
    print(f"  PnL: {total_pnl:+.1f}, AvgWin: {avg_win:+.1f}, AvgLoss: {avg_loss:+.1f}")
    print(f"  AvgMFE(winners): {avg_mfe_w:.1f}")
    print(f"  AvgMFE(losers): {avg_mfe_l:.1f}")
    print(f"  Losers that touched activation (MFE>=10): {loss_touched}/{losses}")
    print(f"  Capture: {avg_win/avg_mfe_w*100:.0f}%" if avg_mfe_w else "  N/A")

    # Gap improvement for this clean dataset
    win_trades = [(r[8], r[9]) for r in r7 if r[7] == 'WIN' and r[9] and r[9] >= 10]
    loss_total = sum(r[8] for r in r7 if r[7] != 'WIN')

    print(f"\n  GAP SENSITIVITY (post-Mar 18 + V12, {len(win_trades)} trail-active wins):")
    for new_gap in [3, 4, 5, 6, 7, 8]:
        gap_delta = 8 - new_gap
        new_win_sum = sum(min(pnl + gap_delta, mfe) for pnl, mfe in win_trades)
        new_total = new_win_sum + loss_total
        new_avg_w = new_win_sum / len(win_trades) if win_trades else 0
        new_cap = new_avg_w / avg_mfe_w * 100 if avg_mfe_w else 0
        tag = " <-- CURRENT" if new_gap == 8 else ""
        print(f"    GAP={new_gap}: PnL {new_total:>+8.1f} (delta {new_total - total_pnl:>+7.1f})  AvgWin {new_avg_w:+.1f}  Cap {new_cap:.0f}%{tag}")

    # MFE distribution for this clean dataset
    print(f"\n  MFE DISTRIBUTION (post-Mar 18 + V12 winners):")
    w_list = [(r[8], r[9]) for r in r7 if r[7] == 'WIN' and r[9]]
    for lo, hi in [(10, 15), (15, 20), (20, 30), (30, 50), (50, 200)]:
        b = [(p, m) for p, m in w_list if lo <= m < hi]
        if b:
            ap = sum(p for p, m in b) / len(b)
            am = sum(m for p, m in b) / len(b)
            print(f"    MFE {lo:>2}-{hi:<3}: {len(b):>2} wins, avgPnL {ap:+.1f}, avgMFE {am:.1f}, cap {ap/am*100:.0f}%")
