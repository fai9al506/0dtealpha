"""Verify v2 — correct column names."""
import os, psycopg2, json
c = psycopg2.connect(os.environ['DATABASE_URL']); cur = c.cursor()

# Sample chain row to see what 'rows' / 'columns' looks like (volumes per strike?)
print("="*100)
print("1. chain_snapshots rows structure (do we have volume per strike?)")
print("="*100)
cur.execute("""
SELECT columns, jsonb_array_length(rows) FROM chain_snapshots
WHERE (ts AT TIME ZONE 'America/New_York')::date='2026-05-29'
ORDER BY ts LIMIT 1
""")
r = cur.fetchone()
if r:
    cols = r[0]
    print(f"columns array: {cols}")
    print(f"# of rows: {r[1]}")

cur.execute("""
SELECT rows->0, rows->1 FROM chain_snapshots
WHERE (ts AT TIME ZONE 'America/New_York')::date='2026-05-29'
ORDER BY ts LIMIT 1
""")
r = cur.fetchone()
if r:
    print(f"sample row 0: {r[0]}")
    print(f"sample row 1: {r[1]}")

# 2. Volland exposure points - charm at 09:30-09:40 today
print()
print("="*100)
print("2. Volland exposure points 09:30 - 09:40 ET today (charm + gamma)")
print("="*100)
cur.execute("""
SELECT (ts_utc AT TIME ZONE 'America/New_York') AS et,
       greek, strike, value
FROM volland_exposure_points
WHERE (ts_utc AT TIME ZONE 'America/New_York')::date='2026-05-29'
  AND (ts_utc AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '09:40'
  AND greek IN ('charm','gamma','gammaToday','gamma_today','gex')
  AND strike BETWEEN 7570 AND 7640
ORDER BY ts_utc, greek, strike
""")
prev_ts = None
for r in cur.fetchall():
    if r[0] != prev_ts:
        print(f"\n  {r[0].strftime('%H:%M:%S')}:")
        prev_ts = r[0]
    print(f"     {r[1]:<14} K={r[2]:.0f}  val={r[3]:>+12.2f}M")

# 3. Distinct greek types we have
print()
print("="*100)
print("3. Distinct greek types in volland_exposure_points")
print("="*100)
cur.execute("SELECT DISTINCT greek FROM volland_exposure_points WHERE ts_utc > NOW() - INTERVAL '1 day'")
for (g,) in cur.fetchall(): print(f"  {g}")
