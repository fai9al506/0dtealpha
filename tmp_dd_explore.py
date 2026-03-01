"""
Step 1: Explore Delta Decay data in the database
"""
import json, os, psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

out = []
def p(s=""):
    out.append(str(s))

# 1. Table schema
p("=== volland_exposure_points schema ===")
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'volland_exposure_points'
    ORDER BY ordinal_position
""")
for row in cur.fetchall():
    p(f"  {row[0]:30} {row[1]}")

# 2. Distinct greek types
p("\n=== Distinct greek types ===")
cur.execute("SELECT DISTINCT greek FROM volland_exposure_points ORDER BY 1")
for row in cur.fetchall():
    p(f"  {row[0]}")

# 3. DD date range and count
p("\n=== DD data range ===")
cur.execute("""
    SELECT MIN(ts_utc), MAX(ts_utc), COUNT(*), COUNT(DISTINCT ts_utc::date)
    FROM volland_exposure_points
    WHERE greek = 'deltaDecay'
""")
row = cur.fetchone()
p(f"  From: {row[0]}  To: {row[1]}  Total rows: {row[2]}  Days: {row[3]}")

# 4. DD points per snapshot
p("\n=== DD points per snapshot (recent) ===")
cur.execute("""
    SELECT ts_utc, COUNT(*) as pts, MIN(strike) as min_s, MAX(strike) as max_s,
           MIN(value) as min_v, MAX(value) as max_v, SUM(value) as total_v,
           MIN(current_price) as spot
    FROM volland_exposure_points
    WHERE greek = 'deltaDecay'
    GROUP BY ts_utc
    ORDER BY ts_utc DESC
    LIMIT 10
""")
for row in cur.fetchall():
    p(f"  ts={row[0]}  pts={row[1]}  strikes={float(row[2]):.0f}-{float(row[3]):.0f}  "
      f"val={float(row[4]):.0f} to {float(row[5]):.0f}  total={float(row[6]):.0f}  spot={float(row[7]):.1f}")

# 5. Sample DD exposure points near spot
p("\n=== DD values near spot (latest snapshot) ===")
cur.execute("""
    WITH latest AS (
        SELECT MAX(ts_utc) as ts FROM volland_exposure_points WHERE greek = 'deltaDecay'
    )
    SELECT e.strike, e.value, e.current_price
    FROM volland_exposure_points e, latest l
    WHERE e.ts_utc = l.ts AND e.greek = 'deltaDecay'
    AND e.strike BETWEEN e.current_price - 50 AND e.current_price + 50
    ORDER BY e.strike
""")
for row in cur.fetchall():
    p(f"  Strike {float(row[0]):8.0f}  DD={float(row[1]):12.0f}  Spot={float(row[2]):.1f}")

# 6. Volland snapshots - DD in statistics
p("\n=== DD in volland_snapshots statistics (recent) ===")
cur.execute("""
    SELECT ts,
           payload->'statistics'->>'delta_decay_hedging' as dd_hedge,
           payload->'statistics'->>'aggregatedDeltaDecay' as dd_raw,
           payload->'statistics'->>'aggregatedCharm' as charm,
           payload->'statistics'->>'paradigm' as paradigm,
           payload->>'current_price' as spot
    FROM volland_snapshots
    WHERE payload->'statistics' IS NOT NULL
      AND payload->'statistics'->>'aggregatedDeltaDecay' IS NOT NULL
    ORDER BY ts DESC
    LIMIT 15
""")
for row in cur.fetchall():
    p(f"  ts={row[0]}  DD={row[1]}  raw={row[2]}  charm={row[3]}  para={row[4]}  spot={row[5]}")

# 7. How many snapshots have DD data?
p("\n=== Snapshot count with DD data ===")
cur.execute("""
    SELECT COUNT(*), COUNT(DISTINCT ts::date)
    FROM volland_snapshots
    WHERE payload->'statistics'->>'aggregatedDeltaDecay' IS NOT NULL
""")
row = cur.fetchone()
p(f"  Snapshots with DD: {row[0]}  Days: {row[1]}")

# 8. DD data alongside spot price changes
p("\n=== DD value evolution (first 3 snapshots per day, last 5 days) ===")
cur.execute("""
    WITH ranked AS (
        SELECT ts, ts::date as trade_date,
               payload->'statistics'->>'aggregatedDeltaDecay' as dd_raw,
               payload->'statistics'->>'aggregatedCharm' as charm,
               payload->'statistics'->>'paradigm' as paradigm,
               payload->>'current_price' as spot,
               ROW_NUMBER() OVER (PARTITION BY ts::date ORDER BY ts) as rn
        FROM volland_snapshots
        WHERE payload->'statistics'->>'aggregatedDeltaDecay' IS NOT NULL
    )
    SELECT trade_date, ts, dd_raw, charm, paradigm, spot
    FROM ranked
    WHERE rn <= 3
    ORDER BY trade_date DESC, ts
    LIMIT 30
""")
cur_date = None
for row in cur.fetchall():
    if row[0] != cur_date:
        cur_date = row[0]
        p(f"\n  --- {cur_date} ---")
    p(f"    {row[1]}  DD={row[2]}  Charm={row[3]}  Para={row[4]}  Spot={row[5]}")

cur.close()
conn.close()

with open("tmp_dd_explore_output.txt", "w") as f:
    f.write("\n".join(out))
print(f"Done. {len(out)} lines -> tmp_dd_explore_output.txt")
