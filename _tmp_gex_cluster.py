"""Check GEX Long trades: floor cluster, support proximity, and all scoring components"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT id, ts, spot, lis, target, max_plus_gex, max_minus_gex,
               gap_to_lis, rr_ratio, grade, score, paradigm,
               support_score, upside_score, floor_cluster_score,
               target_cluster_score, rr_score,
               outcome_result, outcome_pnl, outcome_elapsed_min
        FROM setup_log
        WHERE setup_name = 'GEX Long'
        ORDER BY ts ASC
    """)).mappings().all()

    print(f"All GEX Long trades ({len(trades)}):\n")
    print(f"{'#':>4s} {'Date':10s} {'Result':7s} {'PNL':>6s} {'Grade':6s} {'Score':>5s} "
          f"{'Gap':>5s} {'Spot':>7s} {'LIS':>7s} {'-GEX':>7s} {'Floor':>7s} "
          f"{'Sup':>4s} {'Up':>4s} {'Flr':>4s} {'Tgt':>4s} {'RR':>4s} {'Paradigm':12s}")
    print("-" * 120)

    for t in trades:
        spot = float(t['spot'])
        lis = float(t['lis']) if t['lis'] else 0
        minus_gex = float(t['max_minus_gex']) if t['max_minus_gex'] else 0
        floor_dist = abs(lis - minus_gex) if lis and minus_gex else None
        gap = float(t['gap_to_lis']) if t['gap_to_lis'] else 0

        result = t['outcome_result'] or 'OPEN'
        pnl = float(t['outcome_pnl'] or 0)

        print(f"#{t['id']:3d} {str(t['ts'])[:10]} {result:7s} {pnl:+6.1f} {t['grade']:6s} {float(t['score'] or 0):5.1f} "
              f"{gap:5.1f} {spot:7.1f} {lis:7.1f} {minus_gex:7.1f} "
              f"{floor_dist:7.1f} "
              f"{t['support_score'] or 0:4d} {t['upside_score'] or 0:4d} {t['floor_cluster_score'] or 0:4d} "
              f"{t['target_cluster_score'] or 0:4d} {t['rr_score'] or 0:4d} {t['paradigm'] or '':12s}")

    # Summary: floor cluster score by outcome
    print("\n\nFloor cluster analysis:")
    print(f"{'Result':7s} {'Count':>5s} {'Avg FlrScore':>12s} {'Avg FlrDist':>11s} {'Avg Gap':>8s}")
    from collections import defaultdict
    by_result = defaultdict(list)
    for t in trades:
        by_result[t['outcome_result'] or 'OPEN'].append(t)

    for result in ['WIN', 'LOSS', 'EXPIRED']:
        group = by_result.get(result, [])
        if not group:
            continue
        avg_flr = sum(t['floor_cluster_score'] or 0 for t in group) / len(group)
        avg_dist = sum(abs(float(t['lis'] or 0) - float(t['max_minus_gex'] or 0)) for t in group) / len(group)
        avg_gap = sum(float(t['gap_to_lis'] or 0) for t in group) / len(group)
        print(f"{result:7s} {len(group):5d} {avg_flr:12.1f} {avg_dist:11.1f} {avg_gap:8.1f}")
