"""Backfill Greek context columns on all historical setup_log rows."""
import os, sys, json
sys.stdout.reconfigure(encoding='utf-8')
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])

# Step 0: Create columns if they don't exist yet (Railway may not have deployed)
with engine.begin() as conn:
    for col, dtype in [
        ("vanna_all", "DOUBLE PRECISION"),
        ("vanna_weekly", "DOUBLE PRECISION"),
        ("vanna_monthly", "DOUBLE PRECISION"),
        ("spot_vol_beta", "DOUBLE PRECISION"),
        ("greek_alignment", "INTEGER"),
    ]:
        conn.execute(text(f"""
        DO $$ BEGIN
            ALTER TABLE setup_log ADD COLUMN {col} {dtype};
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
        """))
    print("Columns ready")

# Pull all setup_log rows missing Greek data
with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               setup_name, direction, spot, max_plus_gex
        FROM setup_log
        WHERE vanna_all IS NULL
        ORDER BY id
    """)).mappings().all()

print(f"Backfilling {len(rows)} rows...")

updated = 0
for i, row in enumerate(rows):
    trade_ts = row["ts_et"]
    tid = row["id"]

    with engine.begin() as conn:
        # Vanna ALL
        vanna_all = None
        r = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'ALL'
              AND ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE greek = 'vanna' AND expiration_option = 'ALL'
                  AND ts_utc <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
              )
        """), {"ts": trade_ts}).mappings().first()
        if r and r["total"] is not None:
            vanna_all = float(r["total"])

        # Vanna THIS_WEEK
        vanna_weekly = None
        r = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'THIS_WEEK'
              AND ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE greek = 'vanna' AND expiration_option = 'THIS_WEEK'
                  AND ts_utc <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
              )
        """), {"ts": trade_ts}).mappings().first()
        if r and r["total"] is not None:
            vanna_weekly = float(r["total"])

        # Vanna THIRTY_NEXT_DAYS
        vanna_monthly = None
        r = conn.execute(text("""
            SELECT SUM(value::numeric)::float as total
            FROM volland_exposure_points
            WHERE greek = 'vanna' AND expiration_option = 'THIRTY_NEXT_DAYS'
              AND ts_utc = (
                SELECT MAX(ts_utc) FROM volland_exposure_points
                WHERE greek = 'vanna' AND expiration_option = 'THIRTY_NEXT_DAYS'
                  AND ts_utc <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
              )
        """), {"ts": trade_ts}).mappings().first()
        if r and r["total"] is not None:
            vanna_monthly = float(r["total"])

        # SVB + charm
        svb_correlation = None
        agg_charm = None
        snap = conn.execute(text("""
            SELECT payload FROM volland_snapshots
            WHERE payload->>'error_event' IS NULL
              AND payload->'statistics'->'spot_vol_beta' IS NOT NULL
              AND ts <= (:ts AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC')
            ORDER BY ts DESC LIMIT 1
        """), {"ts": trade_ts}).mappings().first()
        if snap:
            payload = snap["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(payload, dict):
                stats = payload.get("statistics", {})
                svb = stats.get("spot_vol_beta", {})
                if isinstance(svb, dict) and svb.get("correlation") is not None:
                    try:
                        svb_correlation = float(svb["correlation"])
                    except (ValueError, TypeError):
                        pass
                charm_val = stats.get("aggregatedCharm")
                if charm_val is not None:
                    try:
                        agg_charm = float(charm_val)
                    except (ValueError, TypeError):
                        pass

        # Greek alignment
        direction = row["direction"]
        is_long = direction in ("long", "bullish")
        alignment = 0
        if agg_charm is not None:
            alignment += 1 if (agg_charm > 0) == is_long else -1
        if vanna_all is not None:
            alignment += 1 if (vanna_all > 0) == is_long else -1
        spot = row["spot"]
        max_plus_gex = row["max_plus_gex"]
        if spot and max_plus_gex:
            gex_bullish = spot <= max_plus_gex
            alignment += 1 if gex_bullish == is_long else -1

        # UPDATE
        conn.execute(text("""
            UPDATE setup_log
            SET vanna_all = :vanna_all,
                vanna_weekly = :vanna_weekly,
                vanna_monthly = :vanna_monthly,
                spot_vol_beta = :svb,
                greek_alignment = :alignment
            WHERE id = :id
        """), {
            "vanna_all": vanna_all,
            "vanna_weekly": vanna_weekly,
            "vanna_monthly": vanna_monthly,
            "svb": svb_correlation,
            "alignment": alignment,
            "id": tid,
        })
        updated += 1

    if (i + 1) % 50 == 0:
        print(f"  ...{i+1}/{len(rows)}", flush=True)

print(f"\nBackfilled {updated} rows")

# Verify
with engine.begin() as conn:
    check = conn.execute(text("""
        SELECT
            COUNT(*) as total,
            COUNT(vanna_all) as has_vanna,
            COUNT(spot_vol_beta) as has_svb,
            COUNT(greek_alignment) as has_align,
            MIN(greek_alignment) as min_align,
            MAX(greek_alignment) as max_align
        FROM setup_log
    """)).mappings().first()
    print(f"\nVerification:")
    print(f"  Total rows: {check['total']}")
    print(f"  Has vanna_all: {check['has_vanna']}")
    print(f"  Has SVB: {check['has_svb']}")
    print(f"  Has alignment: {check['has_align']}")
    print(f"  Alignment range: {check['min_align']} to {check['max_align']}")

    # Sample
    sample = conn.execute(text("""
        SELECT id, setup_name, direction, vanna_all, vanna_weekly, vanna_monthly,
               spot_vol_beta, greek_alignment
        FROM setup_log
        ORDER BY id DESC LIMIT 5
    """)).mappings().all()
    print(f"\nLatest 5 rows:")
    for s in sample:
        if s['vanna_all'] is not None:
            print(f"  #{s['id']} {s['setup_name']:<18} {s['direction']:<6} "
                  f"vA={s['vanna_all']:>10.0f}  vW={s['vanna_weekly'] or 0:>10.0f}  "
                  f"vM={s['vanna_monthly'] or 0:>10.0f}  "
                  f"SVB={s['spot_vol_beta'] or 0:>+.3f}  align={s['greek_alignment']:>+d}")
        else:
            print(f"  #{s['id']} {s['setup_name']:<18} -- no volland data at signal time")
