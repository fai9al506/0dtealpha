import psycopg2, os, json

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Per-setup totals
cur.execute("""
SELECT setup_name, count(*) as trades,
       count(*) filter (where outcome_result='WIN') as wins,
       count(*) filter (where outcome_result='LOSS') as losses,
       count(*) filter (where outcome_result='EXPIRED') as expired,
       round(coalesce(sum(outcome_pnl),0)::numeric, 1) as total_pnl,
       round(coalesce(avg(outcome_pnl),0)::numeric, 1) as avg_pnl
FROM setup_log
WHERE ts::date = '2026-03-06' AND outcome_result IS NOT NULL
GROUP BY setup_name
ORDER BY total_pnl DESC
""")
print("---PER SETUP---", flush=True)
for r in cur.fetchall():
    print(r, flush=True)

# Grand total
cur.execute("""
SELECT count(*) as trades,
       count(*) filter (where outcome_result='WIN') as wins,
       count(*) filter (where outcome_result='LOSS') as losses,
       count(*) filter (where outcome_result='EXPIRED') as expired,
       round(coalesce(sum(outcome_pnl),0)::numeric, 1) as total_pnl
FROM setup_log
WHERE ts::date = '2026-03-06' AND outcome_result IS NOT NULL
""")
print("---GRAND TOTAL---", flush=True)
print(cur.fetchone(), flush=True)

# All individual trades
cur.execute("""
SELECT id, setup_name, direction, round(spot::numeric, 1) as spot, grade,
       round(score::numeric, 2) as score, outcome_result, round(outcome_pnl::numeric, 1) as pnl_pts,
       ts, outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
       greek_alignment, spot_vol_beta, paradigm,
       outcome_target_level, outcome_stop_level,
       comments
FROM setup_log
WHERE ts::date = '2026-03-06'
ORDER BY ts
""")
cols = [d[0] for d in cur.description]
print("---ALL TRADES---", flush=True)
for r in cur.fetchall():
    d = dict(zip(cols, [str(v) for v in r]))
    print(json.dumps(d), flush=True)

# Auto-trade orders
print("---AUTO TRADE ORDERS---", flush=True)
cur.execute("""
SELECT setup_log_id, state
FROM auto_trade_orders
WHERE (state->>'created_at')::date = '2026-03-06'
ORDER BY state->>'created_at'
""")
rows = cur.fetchall()
print(f"SIM futures trades: {len(rows)}", flush=True)
total_futures_pnl = 0
for r in rows:
    s = r[1]
    pnl = s.get('pnl_pts') or s.get('realized_pnl_pts') or 0
    total_futures_pnl += float(pnl) if pnl else 0
    print(json.dumps({
        'id': r[0], 'setup': s.get('setup_name'), 'dir': s.get('direction'),
        'entry': s.get('entry_price'), 'status': s.get('status'),
        'pnl': pnl, 't1': s.get('t1_filled'), 't2': s.get('t2_filled'),
        'created': s.get('created_at'), 'closed': s.get('closed_at'),
        'reason': s.get('close_reason'), 'qty': s.get('qty'),
        'stop': s.get('stop_price'), 'target': s.get('target_price')
    }), flush=True)
print(f"Total futures PnL pts: {total_futures_pnl}", flush=True)

# Options trades
print("---OPTIONS TRADES---", flush=True)
try:
    cur.execute("""
    SELECT setup_log_id, state
    FROM options_trade_orders
    WHERE (state->>'created_at')::date = '2026-03-06'
    ORDER BY state->>'created_at'
    """)
    rows = cur.fetchall()
    print(f"Options trades: {len(rows)}", flush=True)
    for r in rows:
        s = r[1]
        print(json.dumps(s, default=str), flush=True)
except Exception as e:
    print(f"Options error: {e}", flush=True)

# Breakdown by direction and time
cur.execute("""
SELECT setup_name, direction,
       to_char(ts AT TIME ZONE 'US/Eastern', 'HH24:MI') as time_et,
       round(spot::numeric, 1) as spot,
       outcome_result, round(outcome_pnl::numeric, 1) as pnl,
       greek_alignment, paradigm, grade
FROM setup_log
WHERE ts::date = '2026-03-06'
ORDER BY ts
""")
print("---TIMELINE---", flush=True)
for r in cur.fetchall():
    print(r, flush=True)

conn.close()
