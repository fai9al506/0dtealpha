import os, psycopg2

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

print("=== DATABASE SIZE ===")
cur.execute("SELECT pg_size_pretty(pg_database_size(current_database())), pg_database_size(current_database())")
r = cur.fetchone()
print(f"Total: {r[0]} ({r[1]:,} bytes)")
gb = r[1] / (1024**3)
print(f"       {gb:.2f} GB")

print("\n=== TABLE SIZES (desc) ===")
cur.execute("""
    SELECT tablename,
           pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)),
           pg_total_relation_size(schemaname||'.'||tablename),
           pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)),
           pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename))
    FROM pg_tables WHERE schemaname='public'
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]:35s} total={r[1]:>10s}  data={r[3]:>10s}  idx={r[4]:>10s}")

print("\n=== ROW COUNTS ===")
tables = ['es_range_bars','chain_snapshots','volland_exposure_points','es_delta_bars',
          'volland_snapshots','es_delta_snapshots','setup_log','setup_cooldowns','auto_trade_orders']
for t in tables:
    try:
        cur.execute(f"SELECT count(*) FROM {t}")
        print(f"  {t:35s} {cur.fetchone()[0]:>10,}")
    except Exception as e:
        conn.rollback()
        print(f"  {t:35s} ERROR: {e}")

print("\n=== DAILY GROWTH (last 5 trading days) ===")
growth_items = [
    ('es_range_bars', 'trade_date', "AND source='rithmic'"),
    ('chain_snapshots', "ts::date", ""),
    ('volland_exposure_points', "ts::date", ""),
    ('es_delta_bars', "ts::date", ""),
    ('volland_snapshots', "ts::date", ""),
]
daily_bytes = {}
for t, tc, extra in growth_items:
    cur.execute(f"SELECT {tc} as d, count(*) FROM {t} WHERE 1=1 {extra} GROUP BY d ORDER BY d DESC LIMIT 5")
    rows = cur.fetchall()
    if rows:
        avg = sum(r[1] for r in rows) / len(rows)
        print(f"  {t:35s} ~{avg:,.0f} rows/day")

print("\n=== DATE RANGES ===")
cur.execute("SELECT min(ts::date), max(ts::date), count(DISTINCT ts::date) FROM chain_snapshots")
r = cur.fetchone()
print(f"  chain_snapshots:          {r[0]} to {r[1]} ({r[2]} days)")
cur.execute("SELECT min(trade_date), max(trade_date), count(DISTINCT trade_date) FROM es_range_bars WHERE source='rithmic'")
r = cur.fetchone()
print(f"  es_range_bars (rithmic):  {r[0]} to {r[1]} ({r[2]} days)")
cur.execute("SELECT min(ts::date), max(ts::date), count(DISTINCT ts::date) FROM volland_snapshots")
r = cur.fetchone()
print(f"  volland_snapshots:        {r[0]} to {r[1]} ({r[2]} days)")

# Estimate growth per day in MB
print("\n=== GROWTH ESTIMATE ===")
cur.execute("""
    SELECT ts::date as d, pg_size_pretty(sum(pg_column_size(t.*)))
    FROM chain_snapshots t
    WHERE ts::date >= (current_date - 5)
    GROUP BY d ORDER BY d DESC LIMIT 3
""")
# Alternative: estimate from row count * avg row size
for t in ['chain_snapshots', 'volland_exposure_points', 'es_range_bars', 'es_delta_bars', 'volland_snapshots']:
    cur.execute(f"SELECT pg_total_relation_size('public.{t}'), count(*) FROM {t}")
    r = cur.fetchone()
    if r[1] > 0:
        avg_row = r[0] / r[1]
        print(f"  {t:35s} avg row = {avg_row:.0f} bytes")

conn.close()
