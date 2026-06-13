"""Verify user's 9:32 ET observation:
  - +GEX magnet at 7600
  - Charm offset at 7600 (+) and 7605 (-)
  - Put volume dominant 9:30-9:32
  - Call volume on 7630/7635 ramped 9:32-9:38
  - Spot moved 7585 -> 7600 by 9:58
"""
import os, psycopg2, json
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ['DATABASE_URL']); cur = c.cursor()

# 1. Inspect chain_snapshots schema (need volume per strike)
print("="*100)
print("1. chain_snapshots schema sample")
print("="*100)
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='chain_snapshots' ORDER BY ordinal_position")
cols = [r[0] for r in cur.fetchall()]
print(f"columns: {cols}")

# 2. Spot trajectory 9:30 -> 10:00 today
print()
print("="*100)
print("2. SPX spot trajectory 09:30 - 10:00 ET today (2026-05-29)")
print("="*100)
cur.execute("""
SELECT (ts AT TIME ZONE 'America/New_York') AS et,
       spot
FROM chain_snapshots
WHERE (ts AT TIME ZONE 'America/New_York')::date = '2026-05-29'
  AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '10:00'
GROUP BY ts, spot ORDER BY ts
LIMIT 50
""")
spot_path = cur.fetchall()
for r in spot_path[:30]: print(f"   {r[0].strftime('%H:%M:%S')} spot={r[1]}")

# 3. Volland exposure points - charm + GEX at 09:30-09:40 today
print()
print("="*100)
print("3. Volland exposure points at 09:30 - 09:40 ET today")
print("="*100)
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='volland_exposure_points' ORDER BY ordinal_position")
print(f"vep columns: {[r[0] for r in cur.fetchall()]}")

cur.execute("""
SELECT (snapshot_ts AT TIME ZONE 'America/New_York') AS et,
       exposure_type, strike, value
FROM volland_exposure_points
WHERE (snapshot_ts AT TIME ZONE 'America/New_York')::date='2026-05-29'
  AND (snapshot_ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '09:40'
  AND exposure_type IN ('charm','gamma','gamma_today')
  AND strike BETWEEN 7580 AND 7640
ORDER BY snapshot_ts, exposure_type, strike
""")
prev_ts = None
for r in cur.fetchall():
    if r[0] != prev_ts:
        print(f"\n  {r[0].strftime('%H:%M:%S')}:")
        prev_ts = r[0]
    print(f"     {r[1]:<12} K={r[2]} value={r[3]}")
