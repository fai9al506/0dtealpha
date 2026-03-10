"""Deep analysis of March 6 auto-trade behavior."""
import psycopg2, os, json, requests

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# 1. What setups were the auto-trader configured for?
# Check if auto_trade_orders has ANY recent records
cur.execute("SELECT setup_log_id, state->>'setup_name', state->>'status', state->>'created_at' FROM auto_trade_orders ORDER BY setup_log_id DESC LIMIT 20")
print("=== RECENT auto_trade_orders ===", flush=True)
for r in cur.fetchall():
    print(r, flush=True)

# 2. Options trade orders - check all
print("\n=== ALL options_trade_orders ===", flush=True)
cur.execute("SELECT setup_log_id, state->>'created_at', state->>'setup_name', state->>'direction', state->>'status', state->>'symbol', state->>'entry_price', state->>'exit_price', state->>'pnl_dollars', state->>'close_reason' FROM options_trade_orders ORDER BY setup_log_id DESC LIMIT 30")
for r in cur.fetchall():
    print(r, flush=True)

# 3. Greek filter results for March 6
print("\n=== GREEK FILTER IMPACT (March 6) ===", flush=True)
cur.execute("""
SELECT setup_name, direction, greek_alignment,
       count(*) as trades,
       count(*) filter (where outcome_result='WIN') as wins,
       count(*) filter (where outcome_result='LOSS') as losses,
       round(sum(outcome_pnl)::numeric, 1) as pnl
FROM setup_log
WHERE ts::date = '2026-03-06' AND outcome_result IS NOT NULL
GROUP BY setup_name, direction, greek_alignment
ORDER BY setup_name, direction, greek_alignment
""")
for r in cur.fetchall():
    print(r, flush=True)

# 4. Hourly breakdown
print("\n=== HOURLY BREAKDOWN (ET) ===", flush=True)
cur.execute("""
SELECT extract(hour from ts AT TIME ZONE 'EST') as hour_et,
       count(*) as trades,
       count(*) filter (where outcome_result='WIN') as wins,
       count(*) filter (where outcome_result='LOSS') as losses,
       round(sum(outcome_pnl)::numeric, 1) as pnl
FROM setup_log
WHERE ts::date = '2026-03-06' AND outcome_result IS NOT NULL
GROUP BY hour_et
ORDER BY hour_et
""")
for r in cur.fetchall():
    print(r, flush=True)

# 5. ES price range on March 6 (from range bars)
print("\n=== ES PRICE RANGE (March 6) ===", flush=True)
cur.execute("""
SELECT min(low) as es_low, max(high) as es_high,
       min(low) filter (where ts_start::date = '2026-03-06') as mar6_low,
       max(high) filter (where ts_start::date = '2026-03-06') as mar6_high,
       count(*) as bars
FROM es_range_bars
WHERE ts_start::date = '2026-03-06' AND source = 'rithmic'
""")
print(cur.fetchone(), flush=True)

# 6. Volland data for March 6 - paradigm changes
print("\n=== PARADIGM SHIFTS (March 6) ===", flush=True)
cur.execute("""
SELECT ts, statistics->>'paradigm' as paradigm,
       statistics->>'lis' as lis,
       statistics->>'aggregatedCharm' as charm
FROM volland_snapshots
WHERE ts::date = '2026-03-06' AND statistics IS NOT NULL
  AND statistics->>'paradigm' IS NOT NULL
ORDER BY ts
""")
rows = cur.fetchall()
prev_paradigm = None
for r in rows:
    if r[1] != prev_paradigm:
        print(f"  {str(r[0])[:19]}  Paradigm: {r[1]}  LIS: {r[2]}  Charm: {r[3]}", flush=True)
        prev_paradigm = r[1]

print(f"Total volland snapshots: {len(rows)}", flush=True)

# 7. SPX price action from setup_log spots
print("\n=== SPX PRICE TIMELINE (from setup spots) ===", flush=True)
cur.execute("""
SELECT ts AT TIME ZONE 'EST' as ts_et, round(spot::numeric, 1) as spot
FROM setup_log
WHERE ts::date = '2026-03-06'
ORDER BY ts
""")
rows = cur.fetchall()
if rows:
    first_spot = float(rows[0][1])
    last_spot = float(rows[-1][1])
    max_spot = max(float(r[1]) for r in rows)
    min_spot = min(float(r[1]) for r in rows)
    print(f"Open area: {first_spot}, Close area: {last_spot}", flush=True)
    print(f"High: {max_spot}, Low: {min_spot}", flush=True)
    print(f"Range: {max_spot - min_spot:.1f} pts", flush=True)

conn.close()
