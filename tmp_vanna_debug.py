"""Debug: Why is vanna exposure_points data missing for GEX-LIS trades?"""
import sqlalchemy as sa
import os

e = sa.create_engine(os.environ['DATABASE_URL'])

with e.connect() as c:
    # 1. Does vanna data exist at all today?
    print("=== VANNA DATA TODAY (Mar 20) ===")
    count = c.execute(sa.text("""
        SELECT COUNT(*), COUNT(DISTINCT ts_utc), MIN(ts_utc), MAX(ts_utc)
        FROM volland_exposure_points
        WHERE greek = 'vanna' AND ts_utc::date = '2026-03-20'
    """)).fetchone()
    print(f"  Total rows: {count[0]}, Distinct timestamps: {count[1]}")
    print(f"  First: {count[2]}, Last: {count[3]}")

    # 2. What expiration_options exist for vanna today?
    opts = c.execute(sa.text("""
        SELECT expiration_option, COUNT(*) as cnt
        FROM volland_exposure_points
        WHERE greek = 'vanna' AND ts_utc::date = '2026-03-20'
        GROUP BY expiration_option
    """)).fetchall()
    print(f"\n  Vanna expiration options today:")
    for o in opts:
        print(f"    {o[0]}: {o[1]} rows")

    # 3. Check timestamps around trade #995 (13:45 ET = 17:45 UTC)
    print("\n=== TRADE #995 TIMING ===")
    trade = c.execute(sa.text("SELECT ts FROM setup_log WHERE id = 995")).fetchone()
    print(f"  Trade #995 ts: {trade[0]}")

    # What volland timestamps exist near that time?
    nearby = c.execute(sa.text("""
        SELECT DISTINCT ts_utc
        FROM volland_exposure_points
        WHERE greek = 'vanna'
          AND ts_utc::date = '2026-03-20'
          AND ts_utc <= :trade_ts
        ORDER BY ts_utc DESC LIMIT 5
    """), {"trade_ts": trade[0]}).fetchall()
    print(f"  Volland vanna timestamps before trade:")
    for n in nearby:
        print(f"    {n[0]}")

    # 4. TIMEZONE CHECK — is setup_log.ts in UTC or ET?
    print("\n=== TIMEZONE CHECK ===")
    trade_ts = c.execute(sa.text("""
        SELECT ts, ts AT TIME ZONE 'US/Eastern' as ts_et,
               ts AT TIME ZONE 'UTC' as ts_utc
        FROM setup_log WHERE id = 995
    """)).fetchone()
    print(f"  setup_log.ts raw: {trade_ts[0]}")
    print(f"  AT TIME ZONE ET:  {trade_ts[1]}")
    print(f"  AT TIME ZONE UTC: {trade_ts[2]}")

    vol_ts = c.execute(sa.text("""
        SELECT ts_utc, ts_utc AT TIME ZONE 'US/Eastern' as ts_et
        FROM volland_exposure_points
        WHERE greek = 'vanna' AND ts_utc::date = '2026-03-20'
        ORDER BY ts_utc DESC LIMIT 1
    """)).fetchone()
    if vol_ts:
        print(f"\n  volland ts_utc raw: {vol_ts[0]}")
        print(f"  AT TIME ZONE ET:   {vol_ts[1]}")

    # 5. The original query was: ts_utc <= :trade_ts AND ts_utc::date = :trade_date
    # If setup_log.ts is UTC and volland ts_utc is also UTC, dates might not match!
    print("\n=== DATE MISMATCH CHECK ===")
    print(f"  Trade ts date cast: {trade_ts[0]}")
    # setup_log ts::date — what date does it give?
    d = c.execute(sa.text("SELECT ts::date FROM setup_log WHERE id = 995")).fetchone()
    print(f"  setup_log ts::date for #995: {d[0]}")

    # volland ts_utc dates today
    vd = c.execute(sa.text("""
        SELECT DISTINCT ts_utc::date
        FROM volland_exposure_points
        WHERE greek = 'vanna'
          AND ts_utc >= '2026-03-20' AND ts_utc < '2026-03-21'
    """)).fetchall()
    print(f"  Volland dates for Mar 20 vanna: {[x[0] for x in vd]}")

    # 6. Try the query WITHOUT date filter
    print("\n=== QUERY WITHOUT DATE FILTER ===")
    for trade_id in [995, 943, 670, 337]:
        t = c.execute(sa.text("SELECT ts, ts::date FROM setup_log WHERE id = :id"), {"id": trade_id}).fetchone()
        if not t:
            continue
        # With date filter (original)
        with_date = c.execute(sa.text("""
            SELECT COUNT(DISTINCT ts_utc)
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND ts_utc <= :ts AND ts_utc::date = :d
        """), {"ts": t[0], "d": t[1]}).scalar()
        # Without date filter
        without_date = c.execute(sa.text("""
            SELECT COUNT(DISTINCT ts_utc)
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND ts_utc <= :ts
              AND ts_utc >= :ts - interval '1 day'
        """), {"ts": t[0]}).scalar()
        print(f"  #{trade_id} ts={t[0]} date={t[1]}: with_date_filter={with_date}, without_date_filter={without_date}")

    # 7. Check what SUM we get without date filter for #995
    print("\n=== VANNA SUM WITHOUT DATE FILTER FOR #995 ===")
    t995 = c.execute(sa.text("SELECT ts FROM setup_log WHERE id = 995")).scalar()
    sums = c.execute(sa.text("""
        SELECT
            expiration_option,
            SUM(value) as total,
            COUNT(*) as strikes
        FROM volland_exposure_points
        WHERE greek = 'vanna'
          AND ts_utc = (
            SELECT MAX(ts_utc) FROM volland_exposure_points
            WHERE greek = 'vanna' AND ts_utc <= :ts
          )
        GROUP BY expiration_option
    """), {"ts": t995}).fetchall()
    for s in sums:
        print(f"  {s[0]:25s}: sum={s[1]/1e6:+.0f}M ({s[2]} strikes)")
