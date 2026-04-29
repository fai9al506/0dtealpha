import os
from sqlalchemy import create_engine, text

db = os.getenv('DATABASE_URL', '').replace('postgres://', 'postgresql://')
engine = create_engine(db)

with engine.connect() as conn:
    # 1. setup_log columns
    cols = conn.execute(text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'setup_log'
        ORDER BY ordinal_position
    """)).fetchall()
    print("SETUP_LOG COLUMNS:")
    for c in cols:
        print(f"  {c[0]:30s} {c[1]}")

    # 2. chain_snapshots stats
    r = conn.execute(text("""
        SELECT COUNT(*), MIN(ts), MAX(ts), COUNT(DISTINCT ts::date) as days
        FROM chain_snapshots
    """)).fetchone()
    print(f"\nCHAIN_SNAPSHOTS: {r[0]} rows, {r[3]} days")

    # Check interval on a specific day
    r2 = conn.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as ts_et, spot
        FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date = '2026-03-20'
        ORDER BY ts
        LIMIT 10
    """)).fetchall()
    print(f"\nSample Mar 20 (first 10 rows):")
    for row in r2:
        print(f"  {row[0].strftime('%H:%M:%S')} spot={row[1]}")

    # Check interval between snapshots
    r2b = conn.execute(text("""
        SELECT EXTRACT(EPOCH FROM (LEAD(ts) OVER (ORDER BY ts) - ts)) as gap_sec
        FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date = '2026-03-20'
        ORDER BY ts
        LIMIT 50
    """)).fetchall()
    gaps = [row[0] for row in r2b if row[0] is not None and row[0] < 600]
    if gaps:
        print(f"  Avg interval: {sum(gaps)/len(gaps):.0f}s, min: {min(gaps):.0f}s, max: {max(gaps):.0f}s")

    # 3. Does chain_snapshots have HIGH/LOW for each snapshot?
    snap_cols = conn.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'chain_snapshots'
        ORDER BY ordinal_position
    """)).fetchall()
    print(f"\nCHAIN_SNAPSHOTS COLUMNS: {[c[0] for c in snap_cols]}")

    # 4. Can we get price path for a trade?
    trade = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, spot, direction
        FROM setup_log
        WHERE setup_name = 'Skew Charm' AND outcome_result = 'WIN'
          AND (ts AT TIME ZONE 'America/New_York')::date >= '2026-03-20'
        ORDER BY id LIMIT 1
    """)).fetchone()

    if trade:
        print(f"\nTRADE #{trade[0]}: entry {trade[1].strftime('%Y-%m-%d %H:%M')} spot={trade[2]} {trade[3]}")
        path = conn.execute(text("""
            SELECT ts AT TIME ZONE 'America/New_York' as ts_et, spot
            FROM chain_snapshots
            WHERE ts >= :t AND ts <= :t + interval '60 minutes'
            ORDER BY ts
        """), {"t": trade[1]}).fetchall()
        print(f"  Price path (next 60 min): {len(path)} snapshots")
        for p in path[:10]:
            delta = p[1] - trade[2]
            print(f"    {p[0].strftime('%H:%M:%S')} spot={p[1]:.1f} (delta={delta:+.1f})")
        if len(path) > 10:
            print(f"    ... {len(path)-10} more")

    # 5. Key diagnosis: what's the actual data density?
    print(f"\nDATA DENSITY PER DAY (Mar 18-25, SL=14 era):")
    density = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date as d,
               COUNT(*) as snapshots,
               MIN(spot) as low,
               MAX(spot) as high,
               MAX(spot) - MIN(spot) as range
        FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-03-18' AND '2026-03-25'
        GROUP BY d ORDER BY d
    """)).fetchall()
    for row in density:
        print(f"  {row[0]}: {row[1]} snapshots, range {row[4]:.1f} pts ({row[2]:.1f} to {row[3]:.1f})")
