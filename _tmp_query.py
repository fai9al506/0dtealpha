import psycopg2, os, sys, traceback

try:
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    # First 5 bars to see CVD starting point
    print("=== First 5 bars (session start) ===")
    cur.execute("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
           bar_volume, bar_delta, bar_buy_volume, bar_sell_volume,
           cumulative_delta, cvd_close,
           ts_start AT TIME ZONE 'America/New_York' as start_et,
           ts_end AT TIME ZONE 'America/New_York' as end_et
    FROM es_range_bars
    WHERE trade_date = '2026-02-19'
      AND range_pts = 5.0
    ORDER BY bar_idx ASC
    LIMIT 5
    """)
    for r in cur.fetchall():
        start_str = r[11].strftime('%H:%M:%S') if r[11] else '?'
        end_str = r[12].strftime('%H:%M:%S') if r[12] else '?'
        print(f"#{r[0]:>3} | {start_str}-{end_str} | O={r[1]:.2f} H={r[2]:.2f} L={r[3]:.2f} C={r[4]:.2f} | "
              f"vol={r[5]:>6} d={r[6]:>+6} buy={r[7]:>6} sell={r[8]:>6} | "
              f"cum_delta={r[9]:>+8} cvd_close={r[10]:>+8}")
    sys.stdout.flush()

    # Bars 125-155 around the area of interest
    print("\n=== Bars 125-155 (around 10:28 area) ===")
    cur.execute("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
           bar_volume, bar_delta, bar_buy_volume, bar_sell_volume,
           cumulative_delta, cvd_close,
           ts_start AT TIME ZONE 'America/New_York' as start_et,
           ts_end AT TIME ZONE 'America/New_York' as end_et
    FROM es_range_bars
    WHERE trade_date = '2026-02-19'
      AND range_pts = 5.0
      AND bar_idx BETWEEN 125 AND 155
    ORDER BY bar_idx ASC
    """)
    for r in cur.fetchall():
        start_str = r[11].strftime('%H:%M:%S') if r[11] else '?'
        end_str = r[12].strftime('%H:%M:%S') if r[12] else '?'
        print(f"#{r[0]:>3} | {start_str}-{end_str} | O={r[1]:.2f} H={r[2]:.2f} L={r[3]:.2f} C={r[4]:.2f} | "
              f"vol={r[5]:>6} d={r[6]:>+6} buy={r[7]:>6} sell={r[8]:>6} | "
              f"cum_delta={r[9]:>+8} cvd_close={r[10]:>+8}")
    sys.stdout.flush()

    # Delta accumulation check
    print("\n=== Delta accumulation check (first 10 bars) ===")
    cur.execute("""
    SELECT bar_idx, bar_delta, cumulative_delta, cvd_close,
           bar_buy_volume, bar_sell_volume, bar_volume
    FROM es_range_bars
    WHERE trade_date = '2026-02-19'
      AND range_pts = 5.0
    ORDER BY bar_idx ASC
    LIMIT 10
    """)
    running = 0
    for r in cur.fetchall():
        running += r[1]
        print(f"#{r[0]:>3} | bar_delta={r[1]:>+6} | cum_delta(db)={r[2]:>+8} | cvd_close(db)={r[3]:>+8} | "
              f"running_sum={running:>+8} | buy={r[4]:>6} sell={r[5]:>6} vol={r[6]:>6}")
    sys.stdout.flush()

    # Key comparison: swing bar vs trigger bar
    print("\n=== Key bars: #131 (swing) vs #145 (trigger) ===")
    cur.execute("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
           bar_volume, bar_delta, bar_buy_volume, bar_sell_volume,
           cumulative_delta, cvd_close,
           ts_start AT TIME ZONE 'America/New_York' as start_et,
           ts_end AT TIME ZONE 'America/New_York' as end_et
    FROM es_range_bars
    WHERE trade_date = '2026-02-19'
      AND range_pts = 5.0
      AND bar_idx IN (131, 145)
    ORDER BY bar_idx ASC
    """)
    for r in cur.fetchall():
        start_str = r[11].strftime('%H:%M:%S') if r[11] else '?'
        end_str = r[12].strftime('%H:%M:%S') if r[12] else '?'
        buy_pct = (r[7] / r[5] * 100) if r[5] > 0 else 0
        print(f"#{r[0]:>3} | {start_str}-{end_str} | O={r[1]:.2f} H={r[2]:.2f} L={r[3]:.2f} C={r[4]:.2f}")
        print(f"      vol={r[5]:>6} d={r[6]:>+6} buy={r[7]:>6}({buy_pct:.0f}%) sell={r[8]:>6} | "
              f"cum_delta={r[9]:>+8} cvd_close={r[10]:>+8}")
    sys.stdout.flush()

    conn.close()
except Exception as e:
    traceback.print_exc()
    sys.exit(1)
