"""Check Volland exposure data availability for pin detection"""
import os, sys, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# How much historical exposure data do we have?
r = c.execute(text("""
    SELECT greek, expiration_option,
           MIN(ts_utc::date) as first_date,
           MAX(ts_utc::date) as last_date,
           COUNT(DISTINCT ts_utc::date) as days,
           COUNT(*) as total_rows
    FROM volland_exposure_points
    GROUP BY greek, expiration_option
    ORDER BY greek, expiration_option
""")).fetchall()
print("Exposure data coverage:")
print("%-12s %-20s %-12s %-12s %5s %8s" % ("Greek", "Expiration", "First", "Last", "Days", "Rows"))
print("-" * 75)
for row in r:
    print("%-12s %-20s %-12s %-12s %5d %8d" % (row[0], row[1] or 'None', row[2], row[3], row[4], row[5]))

# For a specific trade time, can we find nearby exposure snapshot?
# Check Mar 10 14:53 ET (the losing DD trade)
print("\n--- Exposure around Mar 10 14:53 ET (trade #665) ---")
# Convert to UTC: ET + 5 = UTC (during EDT it's +4)
r2 = c.execute(text("""
    SELECT ts_utc, greek, expiration_option, current_price,
           COUNT(*) as points
    FROM volland_exposure_points
    WHERE ts_utc BETWEEN '2026-03-10 18:40:00+00' AND '2026-03-10 19:00:00+00'
    GROUP BY ts_utc, greek, expiration_option, current_price
    ORDER BY ts_utc DESC, greek, expiration_option
""")).fetchall()
if r2:
    print("Snapshots found:")
    for row in r2:
        print("  %s  %s/%s  spot=%s  points=%d" % (row[0], row[1], row[2], row[3], row[4]))
else:
    print("No exposure snapshots in that window")

# Try wider window
print("\n--- Wider: any exposure on Mar 10 afternoon? ---")
r3 = c.execute(text("""
    SELECT ts_utc, greek, expiration_option, current_price,
           COUNT(*) as points
    FROM volland_exposure_points
    WHERE ts_utc::date = '2026-03-10'
      AND EXTRACT(HOUR FROM ts_utc) >= 18
    GROUP BY ts_utc, greek, expiration_option, current_price
    ORDER BY ts_utc DESC, greek, expiration_option
    LIMIT 30
""")).fetchall()
if r3:
    for row in r3:
        print("  %s  %s/%s  spot=%s  points=%d" % (row[0], row[1], row[2], row[3], row[4]))
else:
    print("No afternoon exposure data")

# Check what's in volland_snapshots payload for GEX/LIS info
print("\n--- volland_snapshots payload keys (latest) ---")
r4 = c.execute(text("""
    SELECT ts, jsonb_object_keys(payload) as key
    FROM volland_snapshots
    WHERE ts::date = '2026-03-10'
    ORDER BY ts DESC
    LIMIT 1
""")).fetchall()
# Get full payload
r5 = c.execute(text("""
    SELECT ts, payload
    FROM volland_snapshots
    WHERE ts::date = '2026-03-10'
    ORDER BY ts DESC
    LIMIT 1
""")).fetchone()
if r5:
    p = r5[1] if isinstance(r5[1], dict) else json.loads(r5[1])
    print("  ts:", r5[0])
    for k, v in sorted(p.items()):
        val_str = str(v)[:80] if v else 'None'
        print("  %s: %s" % (k, val_str))

c.close()
