"""Check ALL trail-based trades (DD Exhaustion, GEX Long) for missed initial stops.
A missed initial stop = price breached initial SL before trail activated, but outcome says WIN."""
import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

TRAIL_CONFIGS = {
    "DD Exhaustion": {"initial_stop_pts": 12, "trail_activation": 20, "trail_gap": 5, "mode": "continuous"},
    "GEX Long": {"initial_stop_pts": 8, "trail_activation": 12, "trail_gap": 5, "mode": "rung"},
}

with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT id, ts, setup_name, direction, spot, outcome_result, outcome_pnl, outcome_stop_level
        FROM setup_log
        WHERE setup_name IN ('DD Exhaustion', 'GEX Long')
          AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    print(f"Checking {len(trades)} trail-based trades for missed initial stops...\n")
    issues = []

    for t in trades:
        cfg = TRAIL_CONFIGS[t['setup_name']]
        entry = float(t['spot'])
        is_long = t['direction'].lower() in ('long', 'bullish')
        initial_stop = entry - cfg['initial_stop_pts'] if is_long else entry + cfg['initial_stop_pts']

        # Get price path
        trade_date = t['ts'].strftime('%Y-%m-%d')
        prices = conn.execute(text("""
            SELECT ts, spot FROM playback_snapshots
            WHERE ts >= :start AND ts::date = :tdate
            ORDER BY ts ASC
        """), {"start": t['ts'], "tdate": trade_date}).mappings().all()

        # Check if initial stop was hit in price path
        initial_stop_hit_ts = None
        max_fav = 0
        trail_activated = False
        for p in prices:
            price = float(p['spot'])
            profit = (price - entry) if is_long else (entry - price)
            if profit > max_fav:
                max_fav = profit
            if max_fav >= cfg['trail_activation']:
                trail_activated = True

            # Check stop hit
            if is_long and price <= initial_stop:
                initial_stop_hit_ts = p['ts']
                break
            elif not is_long and price >= initial_stop:
                initial_stop_hit_ts = p['ts']
                break

        # Flag issue: initial stop was hit BUT outcome is WIN or trail-based exit
        status = "OK"
        if initial_stop_hit_ts and t['outcome_result'] == 'WIN':
            status = "*** WRONG: initial stop hit but marked WIN ***"
            issues.append(t['id'])
        elif initial_stop_hit_ts and t['outcome_result'] == 'LOSS' and abs(float(t['outcome_pnl'] or 0)) < cfg['initial_stop_pts'] - 1:
            status = "*** SUSPICIOUS: stop hit but PNL too small ***"
            issues.append(t['id'])
        elif initial_stop_hit_ts and t['outcome_result'] == 'EXPIRED':
            status = "*** WRONG: initial stop hit but marked EXPIRED ***"
            issues.append(t['id'])

        marker = " <---" if t['id'] in issues else ""
        print(f"  #{t['id']:3d} {t['setup_name']:16s} {t['direction']:5s} entry={entry:.1f} "
              f"SL={initial_stop:.1f} result={t['outcome_result']:7s} PNL={float(t['outcome_pnl'] or 0):+6.1f} "
              f"max_fav={max_fav:.1f} SL_hit={'YES@'+str(initial_stop_hit_ts)[:19] if initial_stop_hit_ts else 'no':>25s} "
              f"{status}{marker}")

    if issues:
        print(f"\n*** {len(issues)} ISSUES FOUND: trade IDs {issues}")
    else:
        print(f"\nAll trades OK - no missed initial stops detected.")
