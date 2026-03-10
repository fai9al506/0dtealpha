import psycopg2, os, json

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Check all auto_trade_orders (maybe they're cleaned up?)
cur.execute("SELECT count(*) FROM auto_trade_orders")
print(f"Total auto_trade_orders rows: {cur.fetchone()[0]}", flush=True)

cur.execute("SELECT setup_log_id, state->>'created_at', state->>'setup_name', state->>'status' FROM auto_trade_orders ORDER BY state->>'created_at' DESC LIMIT 10")
print("Latest auto_trade_orders:", flush=True)
for r in cur.fetchall():
    print(r, flush=True)

# Check all options_trade_orders
print("---", flush=True)
cur.execute("SELECT count(*) FROM options_trade_orders")
print(f"Total options_trade_orders rows: {cur.fetchone()[0]}", flush=True)

cur.execute("SELECT setup_log_id, state->>'created_at', state->>'setup_name', state->>'status' FROM options_trade_orders ORDER BY state->>'created_at' DESC LIMIT 10")
print("Latest options_trade_orders:", flush=True)
for r in cur.fetchall():
    print(r, flush=True)

# Check if there's a way to see which setup_log entries were auto-traded
# Look at the setup_log for March 6 - check grade to see which were tradeable
print("---TRADEABLE SIGNALS (non-LOG grade, March 6)---", flush=True)
cur.execute("""
SELECT id, setup_name, direction, grade, outcome_result,
       round(outcome_pnl::numeric, 1) as pnl,
       round(spot::numeric, 1) as spot,
       ts AT TIME ZONE 'America/New_York' as ts_et,
       greek_alignment, paradigm
FROM setup_log
WHERE ts::date = '2026-03-06' AND grade NOT IN ('LOG')
ORDER BY ts
""")
for r in cur.fetchall():
    print(r, flush=True)

# Count tradeable vs log
cur.execute("""
SELECT
    count(*) filter (where grade != 'LOG') as tradeable,
    count(*) filter (where grade = 'LOG') as log_only,
    round(sum(outcome_pnl) filter (where grade != 'LOG')::numeric, 1) as tradeable_pnl,
    round(sum(outcome_pnl) filter (where grade = 'LOG')::numeric, 1) as log_pnl
FROM setup_log
WHERE ts::date = '2026-03-06' AND outcome_result IS NOT NULL
""")
print("---TRADEABLE vs LOG---", flush=True)
print(cur.fetchone(), flush=True)

conn.close()
