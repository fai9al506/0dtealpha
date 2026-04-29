"""VIX Direction Study: Should we modify V12-fix VIX gate based on VIX direction?"""
import psycopg2

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

def run_query(conn, label, sql):
    print(f"\n{'='*120}")
    print(f"  QUERY {label}")
    print(f"{'='*120}")
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()

    # Calculate column widths
    widths = [len(c) for c in cols]
    for r in rows:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(str(v)))

    # Print
    header = " | ".join(c.ljust(widths[i]) for i, c in enumerate(cols))
    print(header)
    print("-" * len(header))
    for r in rows:
        print(" | ".join(str(v).ljust(widths[i]) for i, v in enumerate(r)))
    print(f"\nTotal rows: {len(rows)}")
    return rows

def main():
    conn = psycopg2.connect(DB)

    # ============================================================
    # QUERY 1: Daily VIX open/close and spot range for VIX>22 days
    # ============================================================
    run_query(conn, "1: Daily VIX open/close for high-VIX days (VIX open > 22)", """
    WITH daily_vix AS (
        SELECT
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            MIN(vix) as vix_low,
            MAX(vix) as vix_high,
            (array_agg(vix ORDER BY ts))[1] as vix_open,
            (array_agg(vix ORDER BY ts DESC))[1] as vix_close,
            (array_agg(spot ORDER BY ts))[1] as spot_open,
            (array_agg(spot ORDER BY ts DESC))[1] as spot_close
        FROM chain_snapshots
        WHERE vix IS NOT NULL AND vix > 0
        GROUP BY trade_date
    )
    SELECT trade_date,
           ROUND(vix_open::numeric, 2) as vix_open,
           ROUND(vix_close::numeric, 2) as vix_close,
           ROUND(vix_low::numeric, 2) as vix_low,
           ROUND(vix_high::numeric, 2) as vix_high,
           ROUND((vix_close - vix_open)::numeric, 2) as vix_change,
           ROUND(spot_open::numeric, 1) as spot_open,
           ROUND(spot_close::numeric, 1) as spot_close,
           ROUND((spot_close - spot_open)::numeric, 1) as spot_change
    FROM daily_vix
    WHERE vix_open > 22
    ORDER BY trade_date
    """)

    # ============================================================
    # QUERY 2: All non-SC longs blocked by VIX>22 gate, by VIX direction
    # ============================================================
    run_query(conn, "2: Non-SC longs (alignment>=2, VIX>22) grouped by VIX direction", """
    WITH daily_vix AS (
        SELECT
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            (array_agg(vix ORDER BY ts))[1] as vix_open,
            (array_agg(vix ORDER BY ts DESC))[1] as vix_close
        FROM chain_snapshots
        WHERE vix IS NOT NULL AND vix > 0
        GROUP BY trade_date
    )
    SELECT
        CASE WHEN dv.vix_close < dv.vix_open - 1 THEN 'VIX_falling_1pt+'
             WHEN dv.vix_close < dv.vix_open THEN 'VIX_falling_small'
             WHEN dv.vix_close > dv.vix_open + 1 THEN 'VIX_rising_1pt+'
             ELSE 'VIX_rising_small'
        END as vix_direction,
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE s.outcome_result='WIN') as wins,
        COUNT(*) FILTER (WHERE s.outcome_result='LOSS') as losses,
        COUNT(*) FILTER (WHERE s.outcome_result='EXPIRED') as expired,
        ROUND(SUM(s.outcome_pnl)::numeric, 1) as pnl,
        ROUND(100.0 * COUNT(*) FILTER (WHERE s.outcome_result='WIN') /
              NULLIF(COUNT(*) FILTER (WHERE s.outcome_result IN ('WIN','LOSS')), 0), 1) as wr
    FROM setup_log s
    JOIN daily_vix dv ON (s.ts AT TIME ZONE 'America/New_York')::date = dv.trade_date
    WHERE s.outcome_result IS NOT NULL
    AND s.direction IN ('long', 'bullish')
    AND s.setup_name NOT IN ('Skew Charm')
    AND s.greek_alignment >= 2
    AND dv.vix_open > 22
    GROUP BY vix_direction
    ORDER BY vix_direction
    """)

    # ============================================================
    # QUERY 3: Same but broken down by setup name
    # ============================================================
    run_query(conn, "3: Non-SC longs (alignment>=2, VIX>22) by setup + VIX direction", """
    WITH daily_vix AS (
        SELECT
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            (array_agg(vix ORDER BY ts))[1] as vix_open,
            (array_agg(vix ORDER BY ts DESC))[1] as vix_close
        FROM chain_snapshots
        WHERE vix IS NOT NULL AND vix > 0
        GROUP BY trade_date
    )
    SELECT
        s.setup_name,
        CASE WHEN dv.vix_close < dv.vix_open - 1 THEN 'VIX_falling'
             ELSE 'VIX_rising_flat'
        END as vix_dir,
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE s.outcome_result='WIN') as wins,
        COUNT(*) FILTER (WHERE s.outcome_result='LOSS') as losses,
        COUNT(*) FILTER (WHERE s.outcome_result='EXPIRED') as expired,
        ROUND(SUM(s.outcome_pnl)::numeric, 1) as pnl,
        ROUND(100.0 * COUNT(*) FILTER (WHERE s.outcome_result='WIN') /
              NULLIF(COUNT(*) FILTER (WHERE s.outcome_result IN ('WIN','LOSS')), 0), 1) as wr
    FROM setup_log s
    JOIN daily_vix dv ON (s.ts AT TIME ZONE 'America/New_York')::date = dv.trade_date
    WHERE s.outcome_result IS NOT NULL
    AND s.direction IN ('long', 'bullish')
    AND s.setup_name NOT IN ('Skew Charm')
    AND s.greek_alignment >= 2
    AND dv.vix_open > 22
    GROUP BY s.setup_name, vix_dir
    ORDER BY s.setup_name, vix_dir
    """)

    # ============================================================
    # QUERY 4: VIX falling 2+ pts — individual trade details
    # ============================================================
    run_query(conn, "4: Non-SC longs (alignment>=2) on VIX crush days (dropped 2+ pts)", """
    WITH daily_vix AS (
        SELECT
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            (array_agg(vix ORDER BY ts))[1] as vix_open,
            (array_agg(vix ORDER BY ts DESC))[1] as vix_close
        FROM chain_snapshots
        WHERE vix IS NOT NULL AND vix > 0
        GROUP BY trade_date
    )
    SELECT
        (s.ts AT TIME ZONE 'America/New_York')::date as trade_date,
        s.setup_name, s.direction, s.grade, s.greek_alignment as alignment, s.paradigm,
        s.outcome_result as outcome, s.outcome_pnl,
        ROUND(dv.vix_open::numeric, 2) as vix_open,
        ROUND(dv.vix_close::numeric, 2) as vix_close,
        ROUND((dv.vix_close - dv.vix_open)::numeric, 2) as vix_change
    FROM setup_log s
    JOIN daily_vix dv ON (s.ts AT TIME ZONE 'America/New_York')::date = dv.trade_date
    WHERE s.outcome_result IS NOT NULL
    AND s.direction IN ('long', 'bullish')
    AND s.setup_name NOT IN ('Skew Charm')
    AND s.greek_alignment >= 2
    AND dv.vix_open > 22
    AND dv.vix_close < dv.vix_open - 2
    ORDER BY (s.ts AT TIME ZONE 'America/New_York')::date, s.ts
    """)

    # ============================================================
    # QUERY 5: SC longs on VIX>22 days — crush vs rise (reference)
    # ============================================================
    run_query(conn, "5: SC longs on VIX>22 days — VIX crush vs rise (reference)", """
    WITH daily_vix AS (
        SELECT
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            (array_agg(vix ORDER BY ts))[1] as vix_open,
            (array_agg(vix ORDER BY ts DESC))[1] as vix_close
        FROM chain_snapshots
        WHERE vix IS NOT NULL AND vix > 0
        GROUP BY trade_date
    )
    SELECT
        CASE WHEN dv.vix_close < dv.vix_open - 1 THEN 'VIX_falling'
             ELSE 'VIX_rising_flat'
        END as vix_dir,
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE s.outcome_result='WIN') as wins,
        COUNT(*) FILTER (WHERE s.outcome_result='LOSS') as losses,
        COUNT(*) FILTER (WHERE s.outcome_result='EXPIRED') as expired,
        ROUND(SUM(s.outcome_pnl)::numeric, 1) as pnl,
        ROUND(100.0 * COUNT(*) FILTER (WHERE s.outcome_result='WIN') /
              NULLIF(COUNT(*) FILTER (WHERE s.outcome_result IN ('WIN','LOSS')), 0), 1) as wr
    FROM setup_log s
    JOIN daily_vix dv ON (s.ts AT TIME ZONE 'America/New_York')::date = dv.trade_date
    WHERE s.outcome_result IS NOT NULL
    AND s.setup_name = 'Skew Charm'
    AND s.direction IN ('long', 'bullish')
    AND dv.vix_open > 22
    GROUP BY vix_dir
    ORDER BY vix_dir
    """)

    # ============================================================
    # QUERY 6: VIX>22 day counts
    # ============================================================
    run_query(conn, "6: High-VIX day distribution", """
    WITH daily_vix AS (
        SELECT
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            (array_agg(vix ORDER BY ts))[1] as vix_open,
            (array_agg(vix ORDER BY ts DESC))[1] as vix_close
        FROM chain_snapshots
        WHERE vix IS NOT NULL AND vix > 0
        GROUP BY trade_date
    )
    SELECT
        COUNT(*) as total_high_vix_days,
        COUNT(*) FILTER (WHERE vix_close < vix_open - 1) as vix_falling_1pt,
        COUNT(*) FILTER (WHERE vix_close < vix_open - 2) as vix_falling_2pt,
        COUNT(*) FILTER (WHERE vix_close > vix_open + 1) as vix_rising_1pt,
        COUNT(*) FILTER (WHERE vix_close BETWEEN vix_open - 1 AND vix_open + 1) as vix_flat
    FROM daily_vix
    WHERE vix_open > 22
    """)

    # ============================================================
    # QUERY 7: Running P&L by date for VIX-falling non-SC longs
    # ============================================================
    run_query(conn, "7: Running P&L for VIX-falling non-SC longs (MaxDD calc)", """
    WITH daily_vix AS (
        SELECT
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            (array_agg(vix ORDER BY ts))[1] as vix_open,
            (array_agg(vix ORDER BY ts DESC))[1] as vix_close
        FROM chain_snapshots
        WHERE vix IS NOT NULL AND vix > 0
        GROUP BY trade_date
    )
    SELECT
        (s.ts AT TIME ZONE 'America/New_York')::date as trade_date,
        ROUND(SUM(s.outcome_pnl)::numeric, 1) as daily_pnl,
        ROUND(SUM(SUM(s.outcome_pnl)) OVER (ORDER BY (s.ts AT TIME ZONE 'America/New_York')::date)::numeric, 1) as running_pnl
    FROM setup_log s
    JOIN daily_vix dv ON (s.ts AT TIME ZONE 'America/New_York')::date = dv.trade_date
    WHERE s.outcome_result IS NOT NULL
    AND s.direction IN ('long', 'bullish')
    AND s.setup_name NOT IN ('Skew Charm')
    AND s.greek_alignment >= 2
    AND dv.vix_open > 22
    AND dv.vix_close < dv.vix_open - 1
    GROUP BY (s.ts AT TIME ZONE 'America/New_York')::date
    ORDER BY (s.ts AT TIME ZONE 'America/New_York')::date
    """)

    conn.close()
    print("\n\nDone.")

if __name__ == "__main__":
    main()
