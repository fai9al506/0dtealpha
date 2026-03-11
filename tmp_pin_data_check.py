"""Check what Volland exposure data we have for pin detection"""
import os, sys
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Check greek types and expiration options
r = c.execute(text("""
    SELECT DISTINCT greek, expiration_option
    FROM volland_exposure_points
    ORDER BY greek, expiration_option
""")).fetchall()
print("Greek types in exposure_points:")
for row in r:
    print("  greek=%s  expiration=%s" % (row[0], row[1]))

# Sample: get latest snapshot exposure around current price
print("\n--- Latest charm exposure (today-only) around spot ---")
r2 = c.execute(text("""
    SELECT strike, value, current_price, greek, expiration_option
    FROM volland_exposure_points
    WHERE greek = 'charm' AND expiration_option = 'today'
      AND ts_utc > NOW() - INTERVAL '2 hours'
    ORDER BY ts_utc DESC, strike
    LIMIT 30
""")).fetchall()
for row in r2:
    print("  strike=%s  value=%s  spot=%s" % (row[0], row[1], row[2]))

# Sample: get latest vanna exposure around spot
print("\n--- Latest vanna exposure (all) around spot ---")
r3 = c.execute(text("""
    SELECT strike, value, current_price, greek, expiration_option
    FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'all'
      AND ts_utc > NOW() - INTERVAL '2 hours'
    ORDER BY ts_utc DESC, strike
    LIMIT 30
""")).fetchall()
for row in r3:
    print("  strike=%s  value=%s  spot=%s" % (row[0], row[1], row[2]))

# Check volland_snapshots for GEX data
print("\n--- volland_snapshots columns ---")
r4 = c.execute(text("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name='volland_snapshots'
    ORDER BY ordinal_position
""")).fetchall()
for row in r4:
    print("  %s: %s" % (row[0], row[1]))

# Check chain_snapshots for GEX
print("\n--- Sample GEX from chain_snapshots (latest) ---")
r5 = c.execute(text("""
    SELECT ts, spot, columns,
           jsonb_array_length(rows) as num_rows
    FROM chain_snapshots
    ORDER BY ts DESC LIMIT 1
""")).fetchone()
if r5:
    print("  ts=%s spot=%s rows=%s" % (r5[0], r5[1], r5[3]))
    # Check if columns include GEX
    import json
    cols = json.loads(r5[2]) if isinstance(r5[2], str) else r5[2]
    print("  columns:", cols)

c.close()
