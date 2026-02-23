"""Deep analysis of GEX Long setup — why 17.6% WR?"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)
with engine.begin() as conn:
    # 1. All GEX Long trades with full context
    print("=" * 130)
    print("ALL GEX LONG TRADES — FULL CONTEXT")
    print("=" * 130)
    rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               direction, grade, score,
               spot, lis, target,
               max_plus_gex, max_minus_gex,
               gap_to_lis, upside, rr_ratio,
               support_score, upside_score, floor_cluster_score, target_cluster_score, rr_score,
               paradigm, first_hour,
               outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_max_loss,
               outcome_first_event, outcome_elapsed_min
        FROM setup_log
        WHERE setup_name = 'GEX Long'
          AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    print(f"\n{'#':>3} {'ID':>4} {'Date':>5} {'Time':>5} {'Grd':>5} {'Scr':>3} {'Paradigm':>12} {'Spot':>7} {'LIS':>7} {'Tgt':>7} {'+GEX':>7} {'-GEX':>7} {'Gap':>5} {'Up':>5} {'R:R':>4} {'1H':>3} | {'Res':>7} {'PnL':>6} {'MaxP':>6} {'MaxL':>6} {'Min':>4}")
    print("-" * 130)

    for r in rows:
        spot = float(r['spot']) if r['spot'] else 0
        lis = float(r['lis']) if r['lis'] else 0
        tgt = float(r['target']) if r['target'] else 0
        pgex = float(r['max_plus_gex']) if r['max_plus_gex'] else 0
        mgex = float(r['max_minus_gex']) if r['max_minus_gex'] else 0
        gap = float(r['gap_to_lis']) if r['gap_to_lis'] else 0
        up = float(r['upside']) if r['upside'] else 0
        rr = float(r['rr_ratio']) if r['rr_ratio'] else 0
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        mp = float(r['outcome_max_profit']) if r['outcome_max_profit'] else 0
        ml = float(r['outcome_max_loss']) if r['outcome_max_loss'] else 0
        em = r['outcome_elapsed_min'] or 0
        fh = 'Y' if r['first_hour'] else 'N'
        par = (r['paradigm'] or '')[:12]
        ts = r['ts_et']

        # Key: is +GEX above spot and -GEX below spot?
        gex_aligned = pgex > spot and mgex < spot

        print(f"{r['id']:>4} {ts.strftime('%m/%d'):>5} {ts.strftime('%H:%M'):>5} {r['grade']:>5} {r['score']:>3.0f} {par:>12} {spot:>7.1f} {lis:>7.0f} {tgt:>7.0f} {pgex:>7.0f} {mgex:>7.0f} {gap:>+5.1f} {up:>5.1f} {rr:>4.1f} {fh:>3} | {r['outcome_result']:>7} {pnl:>+6.1f} {mp:>+6.1f} {ml:>+6.1f} {em:>4}")

    # 2. GEX alignment analysis
    print("\n" + "=" * 130)
    print("GEX ALIGNMENT ANALYSIS: Is +GEX above spot AND -GEX below spot?")
    print("=" * 130)

    aligned_w, aligned_l, aligned_e = 0, 0, 0
    misaligned_w, misaligned_l, misaligned_e = 0, 0, 0
    aligned_pnl, misaligned_pnl = 0.0, 0.0

    for r in rows:
        spot = float(r['spot']) if r['spot'] else 0
        pgex = float(r['max_plus_gex']) if r['max_plus_gex'] else 0
        mgex = float(r['max_minus_gex']) if r['max_minus_gex'] else 0
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        result = r['outcome_result']

        aligned = pgex > spot and mgex < spot

        pgex_pos = "ABOVE" if pgex > spot else "BELOW" if pgex < spot else "AT"
        mgex_pos = "BELOW" if mgex < spot else "ABOVE" if mgex > spot else "AT"
        pgex_dist = pgex - spot
        mgex_dist = spot - mgex

        status = "ALIGNED" if aligned else "MISALIGNED"

        print(f"  #{r['id']:>3} {status:>10} | +GEX {pgex_pos} spot by {pgex_dist:>+6.1f} | -GEX {mgex_pos} spot by {mgex_dist:>+6.1f} | {result:>7} {pnl:>+6.1f}")

        if aligned:
            aligned_pnl += pnl
            if result == 'WIN': aligned_w += 1
            elif result == 'LOSS': aligned_l += 1
            else: aligned_e += 1
        else:
            misaligned_pnl += pnl
            if result == 'WIN': misaligned_w += 1
            elif result == 'LOSS': misaligned_l += 1
            else: misaligned_e += 1

    at = aligned_w + aligned_l + aligned_e
    mt = misaligned_w + misaligned_l + misaligned_e
    print(f"\n  ALIGNED   (+GEX above, -GEX below): {at} trades, {aligned_w}W/{aligned_l}L/{aligned_e}E, WR={100*aligned_w/at:.0f}%, PnL={aligned_pnl:+.1f}")
    print(f"  MISALIGNED:                          {mt} trades, {misaligned_w}W/{misaligned_l}L/{misaligned_e}E, WR={100*misaligned_w/mt:.0f}% PnL={misaligned_pnl:+.1f}")

    # 3. Paradigm analysis
    print("\n" + "=" * 130)
    print("PARADIGM AT SIGNAL TIME")
    print("=" * 130)
    paradigm_stats = {}
    for r in rows:
        par = r['paradigm'] or 'unknown'
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        result = r['outcome_result']
        if par not in paradigm_stats:
            paradigm_stats[par] = {'w': 0, 'l': 0, 'e': 0, 'pnl': 0.0}
        paradigm_stats[par]['pnl'] += pnl
        if result == 'WIN': paradigm_stats[par]['w'] += 1
        elif result == 'LOSS': paradigm_stats[par]['l'] += 1
        else: paradigm_stats[par]['e'] += 1

    for par, s in sorted(paradigm_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
        total = s['w'] + s['l'] + s['e']
        wr = 100 * s['w'] / total if total > 0 else 0
        print(f"  {par:>15}: {total} trades, {s['w']}W/{s['l']}L/{s['e']}E, WR={wr:.0f}%, PnL={s['pnl']:+.1f}")

    # 4. Spot vs LIS gap analysis
    print("\n" + "=" * 130)
    print("SPOT vs LIS GAP ANALYSIS (how far from support when signal fires)")
    print("=" * 130)
    for r in rows:
        spot = float(r['spot']) if r['spot'] else 0
        lis = float(r['lis']) if r['lis'] else 0
        gap = spot - lis  # positive = above LIS
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        result = r['outcome_result']
        ts = r['ts_et']
        print(f"  #{r['id']:>3} {ts.strftime('%m/%d %H:%M')} spot={spot:.1f} LIS={lis:.0f} gap={gap:>+6.1f} (spot {'above' if gap > 0 else 'below'} LIS) | {result:>7} {pnl:>+6.1f}")

    # 5. Score component breakdown
    print("\n" + "=" * 130)
    print("SCORE COMPONENTS (support / upside / floor_cluster / target_cluster / rr)")
    print("=" * 130)
    for r in rows:
        ss = r['support_score'] or 0
        us = r['upside_score'] or 0
        fs = r['floor_cluster_score'] or 0
        ts_score = r['target_cluster_score'] or 0
        rs = r['rr_score'] or 0
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        result = r['outcome_result']
        print(f"  #{r['id']:>3} support={ss:>3} upside={us:>3} floor={fs:>3} target={ts_score:>3} rr={rs:>3} total={r['score']:>3.0f} | {result:>7} {pnl:>+6.1f}")

    # 6. Time of day
    print("\n" + "=" * 130)
    print("TIME OF DAY ANALYSIS")
    print("=" * 130)
    for r in rows:
        ts = r['ts_et']
        hour = ts.hour + ts.minute / 60
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        result = r['outcome_result']
        period = "MORNING (9:30-11)" if hour < 11 else ("MIDDAY (11-14)" if hour < 14 else "AFTERNOON (14-16)")
        print(f"  #{r['id']:>3} {ts.strftime('%H:%M')} {period:>20} | {result:>7} {pnl:>+6.1f}")

    morning = [(r, float(r['outcome_pnl'])) for r in rows if r['ts_et'].hour + r['ts_et'].minute/60 < 11]
    midday = [(r, float(r['outcome_pnl'])) for r in rows if 11 <= r['ts_et'].hour + r['ts_et'].minute/60 < 14]
    afternoon = [(r, float(r['outcome_pnl'])) for r in rows if r['ts_et'].hour + r['ts_et'].minute/60 >= 14]

    for label, group in [("MORNING", morning), ("MIDDAY", midday), ("AFTERNOON", afternoon)]:
        if group:
            wins = sum(1 for r, p in group if r['outcome_result'] == 'WIN')
            total = len(group)
            pnl_sum = sum(p for _, p in group)
            print(f"  {label}: {total} trades, {wins}W, WR={100*wins/total:.0f}%, PnL={pnl_sum:+.1f}")

    # 7. Check Volland data at signal time for each trade
    print("\n" + "=" * 130)
    print("VOLLAND SNAPSHOT AT SIGNAL TIME (DD hedging, charm, paradigm)")
    print("=" * 130)
    for r in rows:
        ts_utc = r['ts_et']  # already ET
        # Query volland_snapshots near signal time
        vol = conn.execute(text("""
            SELECT ts,
                   payload->>'paradigm' as paradigm,
                   payload->>'lis' as lis,
                   payload->>'ddHedging' as dd_hedging,
                   payload->>'aggregatedCharm' as charm,
                   payload->>'target' as target
            FROM volland_snapshots
            WHERE ts BETWEEN (:ts::timestamptz - interval '3 minutes') AND (:ts::timestamptz + interval '3 minutes')
              AND payload->>'paradigm' IS NOT NULL
            ORDER BY ts ASC
            LIMIT 1
        """), {"ts": r['ts_et'].strftime('%Y-%m-%d %H:%M:%S-05:00')}).mappings().first()

        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        if vol:
            dd = vol['dd_hedging'] or 'N/A'
            charm = vol['charm'] or 'N/A'
            par = vol['paradigm'] or 'N/A'
            print(f"  #{r['id']:>3} {ts_utc.strftime('%m/%d %H:%M')} paradigm={par:>12} DD={dd:>15} charm={charm:>12} | {r['outcome_result']:>7} {pnl:>+6.1f}")
        else:
            print(f"  #{r['id']:>3} {ts_utc.strftime('%m/%d %H:%M')} [no volland snapshot found near signal time] | {r['outcome_result']:>7} {pnl:>+6.1f}")

    # 8. Max profit analysis — how close did losers get?
    print("\n" + "=" * 130)
    print("MAX PROFIT ON LOSING TRADES — how close did they get before reversing?")
    print("=" * 130)
    for r in rows:
        if r['outcome_result'] != 'LOSS':
            continue
        spot = float(r['spot']) if r['spot'] else 0
        mp = float(r['outcome_max_profit']) if r['outcome_max_profit'] else 0
        ml = float(r['outcome_max_loss']) if r['outcome_max_loss'] else 0
        tgt = float(r['target']) if r['target'] else 0
        tgt_dist = tgt - spot if tgt else 0
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        em = r['outcome_elapsed_min'] or 0
        print(f"  #{r['id']:>3} {r['ts_et'].strftime('%m/%d %H:%M')} spot={spot:.1f} target_dist={tgt_dist:.1f} maxProfit={mp:>+6.1f} maxLoss={ml:>+6.1f} stopped_at={pnl:+.1f} after {em}min")
