"""Explore DD per-strike data shape and availability."""
from sqlalchemy import create_engine, text
from collections import defaultdict

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)

with engine.connect() as conn:
    # 1. Basic stats
    r = conn.execute(text("""
        SELECT COUNT(*) as cnt,
               MIN(ts_utc) as first_ts, MAX(ts_utc) as last_ts,
               COUNT(DISTINCT ts_utc::date) as n_dates
        FROM volland_exposure_points
        WHERE greek = 'deltaDecay'
    """)).fetchone()
    print(f"DD per-strike rows: {r.cnt}")
    print(f"Date range: {r.first_ts} to {r.last_ts}")
    print(f"Trading days: {r.n_dates}")

    # 2. expiration_option values
    r2 = conn.execute(text("""
        SELECT expiration_option, COUNT(*) as cnt
        FROM volland_exposure_points
        WHERE greek = 'deltaDecay'
        GROUP BY expiration_option
    """)).fetchall()
    print(f"\nExpiration options:")
    for row in r2:
        print(f"  {row.expiration_option}: {row.cnt} rows")

    # 3. Snapshots per day
    r3 = conn.execute(text("""
        SELECT ts_utc::date as d, COUNT(DISTINCT ts_utc) as snaps,
               COUNT(*) as rows, AVG(current_price::numeric) as avg_spot
        FROM volland_exposure_points
        WHERE greek = 'deltaDecay'
        GROUP BY ts_utc::date
        ORDER BY d
    """)).fetchall()
    print(f"\nSnapshots per day:")
    print(f"  {'Date':>12} {'Snaps':>6} {'Rows':>7} {'AvgSpot':>8}")
    for row in r3:
        print(f"  {str(row.d):>12} {row.snaps:>6} {row.rows:>7} {float(row.avg_spot):>8.1f}")

    # 4. Sample snapshot: what does a single DD snapshot look like?
    r4 = conn.execute(text("""
        SELECT ts_utc, strike::numeric as strike, value::numeric as val,
               current_price::numeric as spot
        FROM volland_exposure_points
        WHERE greek = 'deltaDecay'
          AND ts_utc = (SELECT MAX(ts_utc) FROM volland_exposure_points WHERE greek = 'deltaDecay' AND ts_utc::date = '2026-03-19')
        ORDER BY strike
    """)).fetchall()
    print(f"\nSample DD snapshot (latest on Mar 19, {len(r4)} strikes):")
    if r4:
        spot = float(r4[0].spot) if r4[0].spot else 0
        print(f"  Spot: {spot:.1f}")
        print(f"  {'Strike':>8} {'DD Value':>15} {'Dist':>6} {'Sign':>5}")
        for row in r4:
            s = float(row.strike)
            v = float(row.val)
            dist = s - spot
            sign = "+" if v > 0 else "-"
            # Only show strikes near spot
            if abs(dist) <= 50:
                print(f"  {s:>8.0f} {v:>15.0f} {dist:>+6.0f} {sign:>5}")

    # 5. Compare with charm at same time
    if r4:
        ts = r4[0].ts_utc
        r5 = conn.execute(text("""
            SELECT strike::numeric as strike, value::numeric as val
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND (expiration_option IS NULL OR expiration_option = 'TODAY')
              AND ts_utc BETWEEN :ts - INTERVAL '3 minutes' AND :ts + INTERVAL '3 minutes'
            ORDER BY strike
        """), {'ts': ts}).fetchall()
        print(f"\n  Charm at same time ({len(r5)} strikes):")
        print(f"  {'Strike':>8} {'Charm':>15} {'DD':>15} {'Same Sign?':>11}")
        charm_by_strike = {float(r.strike): float(r.val) for r in r5}
        dd_by_strike = {float(r.strike): float(r.val) for r in r4}
        for s in sorted(set(charm_by_strike.keys()) & set(dd_by_strike.keys())):
            if abs(s - spot) <= 30:
                c = charm_by_strike[s]
                d = dd_by_strike[s]
                same = "YES" if (c > 0) == (d > 0) else "NO"
                print(f"  {s:>8.0f} {c:>15.0f} {d:>15.0f} {same:>11}")

    # 6. DD value distribution
    r6 = conn.execute(text("""
        SELECT
            percentile_cont(0.25) WITHIN GROUP (ORDER BY ABS(value::numeric)) as p25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY ABS(value::numeric)) as p50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY ABS(value::numeric)) as p75,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY ABS(value::numeric)) as p95,
            MAX(ABS(value::numeric)) as max_val
        FROM volland_exposure_points
        WHERE greek = 'deltaDecay'
    """)).fetchone()
    print(f"\nDD value distribution (absolute):")
    print(f"  P25: {float(r6.p25)/1e6:.1f}M")
    print(f"  P50: {float(r6.p50)/1e6:.1f}M")
    print(f"  P75: {float(r6.p75)/1e6:.1f}M")
    print(f"  P95: {float(r6.p95)/1e6:.1f}M")
    print(f"  Max: {float(r6.max_val)/1e6:.1f}M")

    # 7. Find DD "neutral zone" — where does net DD cross zero?
    print(f"\n=== DD Neutral Zone Analysis (EOD target) ===")
    # For several snapshots near EOD (15:00-15:30), find where DD crosses zero
    r7 = conn.execute(text("""
        WITH eod_snaps AS (
            SELECT DISTINCT ON (ts_utc::date) ts_utc, ts_utc::date as d
            FROM volland_exposure_points
            WHERE greek = 'deltaDecay'
              AND EXTRACT(HOUR FROM ts_utc AT TIME ZONE 'America/New_York') >= 15
              AND EXTRACT(HOUR FROM ts_utc AT TIME ZONE 'America/New_York') < 16
            ORDER BY ts_utc::date, ts_utc DESC
        )
        SELECT e.d, e.ts_utc, v.strike::numeric as strike, v.value::numeric as val,
               v.current_price::numeric as spot
        FROM eod_snaps e
        JOIN volland_exposure_points v ON v.ts_utc = e.ts_utc AND v.greek = 'deltaDecay'
        ORDER BY e.d, v.strike::numeric
    """)).fetchall()

    # Group by date, find zero-crossing strike
    by_date = defaultdict(list)
    for r in r7:
        by_date[str(r.d)].append((float(r.strike), float(r.val), float(r.spot) if r.spot else 0))

    print(f"\n  {'Date':>12} {'Spot':>8} {'DD Neutral':>11} {'Dist':>6}")
    for d in sorted(by_date.keys()):
        strikes = by_date[d]
        spot = strikes[0][2]
        # Find where DD crosses zero (sign change between adjacent strikes)
        neutral = None
        for i in range(len(strikes)-1):
            s1, v1, _ = strikes[i]
            s2, v2, _ = strikes[i+1]
            if v1 * v2 < 0:  # sign change
                # Interpolate
                frac = abs(v1) / (abs(v1) + abs(v2))
                neutral = s1 + frac * (s2 - s1)
                break
        if neutral:
            print(f"  {d:>12} {spot:>8.1f} {neutral:>11.1f} {neutral-spot:>+6.1f}")
        else:
            print(f"  {d:>12} {spot:>8.1f} {'---':>11} {'---':>6}")
